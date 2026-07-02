use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, Offset, Timelike};
use pyo3::exceptions::{
    PyImportError, PyIndexError, PyKeyError, PyRuntimeError, PyTypeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBool, PyCapsule, PyDate, PyDateTime, PyDict, PyIterator, PyList, PyTime, PyTuple,
    PyType, PyTzInfo,
};
use pyo3::IntoPyObjectExt;
use pyo3_arrow::ffi::{
    to_array_pycapsules, to_schema_pycapsule, to_stream_pycapsule, ArrayIterator,
};
use pyo3_arrow::{PyChunkedArray, PyRecordBatch, PyTable};
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::derive::*;
use xbbg_arrow::{
    build_array, cell_from_array, cell_has_value, cell_to_string, ArrowCoreError, CellValue,
    ColumnData, SortDirection, TableData,
};
use xbbg_async::engine::state::{
    METADATA_KEY_EID_DATA, METADATA_KEY_FIELD_EXCEPTIONS, METADATA_KEY_SECURITY_ERRORS,
};

fn core_err_to_py(err: ArrowCoreError) -> PyErr {
    match err {
        ArrowCoreError::UnknownColumn(name) => {
            PyKeyError::new_err(format!("unknown column: {name}"))
        }
        ArrowCoreError::ColumnIndexOutOfRange => PyIndexError::new_err("column index out of range"),
        ArrowCoreError::RowIndexOutOfRange => PyIndexError::new_err("column index out of range"),
        ArrowCoreError::InvalidSortDirection { column, direction } => PyValueError::new_err(
            format!("unsupported sort direction for {column}: {direction}"),
        ),
        other => PyValueError::new_err(other.to_string()),
    }
}

fn map_core<T>(result: xbbg_arrow::Result<T>) -> PyResult<T> {
    result.map_err(core_err_to_py)
}

fn serde_json_value_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(value) => Ok(PyBool::new(py, *value).to_owned().into_any().unbind()),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            } else if let Some(value) = value.as_u64() {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            } else if let Some(value) = value.as_f64() {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            } else {
                Err(PyValueError::new_err("unsupported JSON number"))
            }
        }
        serde_json::Value::String(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        serde_json::Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(serde_json_value_to_py(py, value)?)?;
            }
            Ok(list.into_any().unbind())
        }
        serde_json::Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, serde_json_value_to_py(py, value)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

fn parse_metadata_json(
    py: Python<'_>,
    metadata: &HashMap<String, String>,
    key: &str,
) -> PyResult<Py<PyAny>> {
    let Some(raw) = metadata.get(key) else {
        return Ok(py.None());
    };
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|err| PyValueError::new_err(format!("invalid JSON in {key}: {err}")))?;
    serde_json_value_to_py(py, &value)
}

fn ensure_optional_module(
    py: Python<'_>,
    module: &str,
    install: &str,
    context: &str,
) -> PyResult<()> {
    py.import(module).map(|_| ()).map_err(|err| {
        PyImportError::new_err(format!(
            "{context} requires the '{module}' package. Install it with `{install}`. Original import error: {err}"
        ))
    })
}

fn py_value_to_cell(value: &Bound<'_, PyAny>) -> PyResult<CellValue> {
    if value.is_none() {
        return Ok(CellValue::Null);
    }
    if let Ok(v) = value.extract::<bool>() {
        return Ok(CellValue::Bool(v));
    }
    if let Ok(v) = value.extract::<i64>() {
        return Ok(CellValue::Int(v));
    }
    if let Ok(v) = value.extract::<f64>() {
        return Ok(CellValue::Float(v));
    }
    if let Ok(v) = value.extract::<String>() {
        return Ok(CellValue::Text(v));
    }
    if let Ok(isoformat) = value.getattr("isoformat") {
        if let Ok(text) = isoformat.call0()?.extract::<String>() {
            if text.len() == 10 {
                if let Ok(date) = NaiveDate::parse_from_str(&text, "%Y-%m-%d") {
                    return Ok(CellValue::Date(date));
                }
            }
            return Ok(CellValue::Text(text));
        }
    }
    Ok(CellValue::Text(value.str()?.to_string()))
}

fn rows_to_batch(rows: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let iterator = PyIterator::from_object(rows)?;
    let mut column_names: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut raw_rows: Vec<HashMap<String, CellValue>> = Vec::new();

    for item in iterator {
        let item = item?;
        let dict = item.cast::<PyDict>().map_err(|_| {
            PyTypeError::new_err("ArrowTable.from_pylist expects an iterable of dict rows")
        })?;
        let mut row = HashMap::new();
        for (key, value) in dict.iter() {
            let name = key.extract::<String>()?;
            if seen.insert(name.clone()) {
                column_names.push(name.clone());
            }
            row.insert(name, py_value_to_cell(&value)?);
        }
        raw_rows.push(row);
    }

    if raw_rows.is_empty() {
        return RecordBatch::try_new(Arc::new(Schema::empty()), vec![])
            .map_err(|e| PyValueError::new_err(e.to_string()));
    }

    let mut fields = Vec::with_capacity(column_names.len());
    let mut arrays = Vec::with_capacity(column_names.len());
    for name in &column_names {
        let cells = raw_rows
            .iter()
            .map(|row| row.get(name).cloned().unwrap_or(CellValue::Null))
            .collect::<Vec<_>>();
        let (field, array) = build_array(name, &cells);
        fields.push(field);
        arrays.push(array);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

fn py_sequence_to_cells(values: &Bound<'_, PyAny>) -> PyResult<Vec<CellValue>> {
    let iterator = PyIterator::from_object(values)?;
    iterator
        .map(|item| item.and_then(|value| py_value_to_cell(&value)))
        .collect()
}

fn normalize_index(index: isize, len: usize) -> PyResult<usize> {
    let len = len as isize;
    let idx = if index < 0 { len + index } else { index };
    if !(0..len).contains(&idx) {
        return Err(PyIndexError::new_err("column index out of range"));
    }
    Ok(idx as usize)
}

fn column_index_arg(value: &Bound<'_, PyAny>, schema: &SchemaRef) -> PyResult<usize> {
    if let Ok(index) = value.extract::<isize>() {
        return normalize_index(index, schema.fields().len());
    }
    if let Ok(name) = value.extract::<String>() {
        return schema
            .index_of(&name)
            .map_err(|_| PyKeyError::new_err(format!("unknown column: {name}")));
    }
    Err(PyTypeError::new_err(
        "column selector must be an int or str",
    ))
}

fn column_indices_arg(value: &Bound<'_, PyAny>, schema: &SchemaRef) -> PyResult<Vec<usize>> {
    if value.extract::<String>().is_ok() || value.extract::<isize>().is_ok() {
        return Ok(vec![column_index_arg(value, schema)?]);
    }
    let iterator = PyIterator::from_object(value).map_err(|_| {
        PyTypeError::new_err("columns must be a sequence of column names or indices")
    })?;
    iterator
        .map(|item| item.and_then(|column| column_index_arg(&column, schema)))
        .collect()
}

fn date32_to_py(py: Python<'_>, days: i32) -> PyResult<Py<PyAny>> {
    let Some(date) = xbbg_arrow::scalar::date_from_days(days) else {
        return Ok(py.None());
    };
    Ok(
        PyDate::new(py, date.year(), date.month() as u8, date.day() as u8)?
            .into_any()
            .unbind(),
    )
}

fn timestamp_to_py(py: Python<'_>, micros: i64, timezone: Option<&str>) -> PyResult<Py<PyAny>> {
    let Some(dt) = DateTime::from_timestamp_micros(micros) else {
        return Ok(py.None());
    };

    let utc = PyTzInfo::utc(py)?;
    let utc_datetime = PyDateTime::new(
        py,
        dt.year(),
        dt.month() as u8,
        dt.day() as u8,
        dt.hour() as u8,
        dt.minute() as u8,
        dt.second() as u8,
        dt.timestamp_subsec_micros(),
        Some(&utc),
    )?;

    let Some(tz_name) = timezone.filter(|tz| !tz.eq_ignore_ascii_case("UTC")) else {
        return Ok(utc_datetime.into_any().unbind());
    };

    if let Ok(zoneinfo) = py
        .import("zoneinfo")
        .and_then(|module| module.getattr("ZoneInfo"))
        .and_then(|zoneinfo| zoneinfo.call1((tz_name,)))
    {
        if let Ok(converted) = utc_datetime.call_method1("astimezone", (zoneinfo,)) {
            return Ok(converted.unbind());
        }
    }

    let tz = tz_name
        .parse::<chrono_tz::Tz>()
        .map_err(|_| PyValueError::new_err(format!("unknown timestamp timezone: {tz_name}")))?;
    let local = dt.with_timezone(&tz);
    let offset_seconds = local.offset().fix().local_minus_utc();
    let datetime = py.import("datetime")?;
    let timedelta = datetime.getattr("timedelta")?.call1((0, offset_seconds))?;
    let fixed_tz = datetime.getattr("timezone")?.call1((timedelta, tz_name))?;

    Ok(datetime
        .getattr("datetime")?
        .call1((
            local.year(),
            local.month(),
            local.day(),
            local.hour(),
            local.minute(),
            local.second(),
            local.timestamp_subsec_micros(),
            fixed_tz,
        ))?
        .unbind())
}

fn time64_micros_to_py(py: Python<'_>, micros: i64) -> PyResult<Py<PyAny>> {
    if !(0..86_400_000_000).contains(&micros) {
        return Ok(py.None());
    }
    let seconds = micros / 1_000_000;
    let microsecond = (micros % 1_000_000) as u32;
    let hour = (seconds / 3_600) as u8;
    let minute = ((seconds % 3_600) / 60) as u8;
    let second = (seconds % 60) as u8;
    Ok(PyTime::new(py, hour, minute, second, microsecond, None)?
        .into_any()
        .unbind())
}

fn scalar_to_py(py: Python<'_>, array: &dyn Array, row: usize) -> PyResult<Py<PyAny>> {
    if array.is_null(row) {
        return Ok(py.None());
    }
    match array.data_type() {
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("BooleanArray")
            .value(row)
            .into_py_any(py),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array")
            .value(row)
            .into_py_any(py),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array")
            .value(row)
            .into_py_any(py),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("UInt32Array")
            .value(row)
            .into_py_any(py),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64Array")
            .value(row)
            .into_py_any(py),
        DataType::Float32 => (array
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32Array")
            .value(row) as f64)
            .into_py_any(py),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array")
            .value(row)
            .into_py_any(py),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray")
            .value(row)
            .into_py_any(py),
        DataType::Date32 => date32_to_py(
            py,
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Date32Array")
                .value(row),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => timestamp_to_py(
            py,
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("TimestampMicrosecondArray")
                .value(row),
            timezone.as_deref(),
        ),
        DataType::Time64(TimeUnit::Microsecond) => time64_micros_to_py(
            py,
            array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .expect("Time64MicrosecondArray")
                .value(row),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => timestamp_to_py(
            py,
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("TimestampMillisecondArray")
                .value(row)
                * 1_000,
            timezone.as_deref(),
        ),
        // Unsupported dtype: surface None (absent) instead of Debug-dumping
        // the entire array into every cell.
        _ => Ok(py.None()),
    }
}

fn batch_to_pylist(py: Python<'_>, batch: &RecordBatch) -> PyResult<Vec<Py<PyAny>>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let dict = PyDict::new(py);
        for (column_idx, field) in batch.schema().fields().iter().enumerate() {
            dict.set_item(
                field.name(),
                scalar_to_py(py, batch.column(column_idx).as_ref(), row_idx)?,
            )?;
        }
        rows.push(dict.into_any().unbind());
    }
    Ok(rows)
}

fn table_to_pylist(py: Python<'_>, table: &TableData) -> PyResult<Vec<Py<PyAny>>> {
    let mut rows = Vec::new();
    for batch in &table.batches {
        rows.extend(batch_to_pylist(py, batch)?);
    }
    Ok(rows)
}

fn column_to_pylist(py: Python<'_>, column: &ColumnData) -> PyResult<Vec<Py<PyAny>>> {
    let mut values = Vec::with_capacity(column.len());
    for chunk in &column.chunks {
        for row in 0..chunk.len() {
            values.push(scalar_to_py(py, chunk.as_ref(), row)?);
        }
    }
    Ok(values)
}

fn period_label(array: &dyn Array, row: usize, periodicity: Option<&str>) -> String {
    let Some(parsed) = xbbg_arrow::scalar::date_from_array(array, row) else {
        return cell_to_string(&cell_from_array(array, row)).unwrap_or_default();
    };
    match periodicity.unwrap_or("DAILY").to_ascii_uppercase().as_str() {
        "YEARLY" | "Y" => format!("{:04}", parsed.year()),
        "SEMI_ANNUALLY" | "SEMIANNUALLY" | "S" => {
            let half = if parsed.month() <= 6 { 1 } else { 2 };
            format!("{:04}H{}", parsed.year(), half)
        }
        "QUARTERLY" | "Q" => {
            let quarter = ((parsed.month() - 1) / 3) + 1;
            format!("{:04}Q{}", parsed.year(), quarter)
        }
        "MONTHLY" | "M" => format!("{:04}-{:02}", parsed.year(), parsed.month()),
        "WEEKLY" | "W" => {
            let iso = parsed.iso_week();
            format!("{:04}-W{:02}", iso.year(), iso.week())
        }
        _ => parsed.to_string(),
    }
}

fn parse_bqr_path(path: &str) -> Option<(usize, String)> {
    let start = path.find("tickData[")? + "tickData[".len();
    let rest = &path[start..];
    let end = rest.find(']')?;
    let idx = rest[..end].parse::<usize>().ok()?;
    let after = rest.get(end + 1..)?;
    let field_start = after.strip_prefix('.')?;
    let field = field_start
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>();
    if field.is_empty() {
        None
    } else {
        Some((idx, field))
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct ArrowField {
    field: FieldRef,
}

#[pymethods]
impl ArrowField {
    #[getter]
    fn name(&self) -> String {
        self.field.name().clone()
    }

    #[getter]
    fn data_type(&self) -> String {
        self.field.data_type().to_string()
    }

    #[getter]
    fn nullable(&self) -> bool {
        self.field.is_nullable()
    }

    fn __repr__(&self) -> String {
        format!(
            "xbbg.ArrowField(name={:?}, data_type={:?}, nullable={})",
            self.field.name(),
            self.field.data_type(),
            self.field.is_nullable()
        )
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct ArrowSchema {
    schema: SchemaRef,
}

#[pymethods]
impl ArrowSchema {
    #[getter]
    fn names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    #[getter]
    fn fields(&self) -> Vec<ArrowField> {
        self.schema
            .fields()
            .iter()
            .map(|field| ArrowField {
                field: field.clone(),
            })
            .collect()
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.schema.as_ref())
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow schema export failed: {e}")))
    }

    fn __repr__(&self) -> String {
        format!("xbbg.ArrowSchema(names={:?})", self.names())
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct ArrowColumn {
    data: ColumnData,
}

impl ArrowColumn {
    fn new(data: ColumnData) -> Self {
        Self { data }
    }
}

#[pymethods]
impl ArrowColumn {
    #[getter]
    fn name(&self) -> String {
        self.data.name.clone()
    }

    #[getter]
    fn field(&self) -> ArrowField {
        ArrowField {
            field: self.data.field.clone(),
        }
    }

    #[getter]
    fn data_type(&self) -> String {
        self.data.field.data_type().to_string()
    }

    #[getter]
    fn null_count(&self) -> usize {
        self.data.null_count()
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.data.nbytes()
    }

    #[pyo3(signature = (offset=0, length=None))]
    fn slice(&self, offset: usize, length: Option<usize>) -> Self {
        Self::new(self.data.slice(offset, length))
    }

    fn to_pylist(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = PyList::empty(py);
        for value in column_to_pylist(py, &self.data)? {
            values.append(value.bind(py))?;
        }
        Ok(values.into_any().unbind())
    }

    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ensure_optional_module(
            py,
            "pyarrow",
            "pip install xbbg[pyarrow]",
            "ArrowColumn.to_pyarrow()",
        )?;
        PyChunkedArray::try_new(self.data.chunks.clone(), self.data.field.clone())?.into_pyarrow(py)
    }

    fn __len__(&self) -> usize {
        self.data.len()
    }

    fn __getitem__(&self, py: Python<'_>, index: isize) -> PyResult<Py<PyAny>> {
        let idx = normalize_index(index, self.data.len())?;
        let (chunk, row) = map_core(self.data.chunk_for_index(idx))?;
        scalar_to_py(py, chunk.as_ref(), row)
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = self.to_pylist(py)?;
        Ok(values.bind(py).call_method0("__iter__")?.unbind())
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        let values = self.to_pylist(py)?;
        values
            .bind(py)
            .rich_compare(other, pyo3::basic::CompareOp::Eq)?
            .is_truthy()
    }

    fn __ne__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        let values = self.to_pylist(py)?;
        values
            .bind(py)
            .rich_compare(other, pyo3::basic::CompareOp::Ne)?
            .is_truthy()
    }

    fn __repr__(&self) -> String {
        format!(
            "xbbg.ArrowColumn(name={:?}, len={}, data_type={:?})",
            self.data.name,
            self.data.len(),
            self.data.field.data_type()
        )
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct ArrowRecordBatch {
    batch: RecordBatch,
}

impl ArrowRecordBatch {
    pub fn new(batch: RecordBatch) -> Self {
        Self { batch }
    }

    pub(crate) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    fn table_data(&self) -> PyResult<TableData> {
        map_core(TableData::try_new(vec![self.batch.clone()]))
    }

    fn to_array_pycapsules<'py>(
        py: Python<'py>,
        record_batch: RecordBatch,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let field = Field::new_struct("", record_batch.schema_ref().fields().clone(), false)
            .with_metadata(record_batch.schema_ref().metadata().clone());
        let array: ArrayRef = Arc::new(StructArray::from(record_batch));
        to_array_pycapsules(py, field.into(), array.as_ref(), requested_schema)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow array export failed: {e}")))
    }
}

#[pymethods]
impl ArrowRecordBatch {
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    #[getter]
    fn shape(&self) -> (usize, usize) {
        (self.batch.num_rows(), self.batch.num_columns())
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.batch
            .columns()
            .iter()
            .map(|array| array.get_buffer_memory_size())
            .sum()
    }

    #[getter]
    fn columns(&self) -> PyResult<Vec<ArrowColumn>> {
        let table = self.table_data()?;
        (0..table.num_columns())
            .map(|idx| map_core(table.column_by_index(idx)).map(ArrowColumn::new))
            .collect()
    }

    #[getter]
    fn schema(&self) -> ArrowSchema {
        ArrowSchema {
            schema: self.batch.schema(),
        }
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        Self::to_array_pycapsules(py, self.batch.clone(), requested_schema)
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.batch.schema_ref().as_ref())
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow schema export failed: {e}")))
    }

    fn to_pylist(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        batch_to_pylist(py, &self.batch)
    }

    fn to_table(&self) -> PyResult<ArrowTable> {
        ArrowTable::try_new(vec![self.batch.clone()])
    }

    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ensure_optional_module(
            py,
            "pyarrow",
            "pip install xbbg[pyarrow]",
            "ArrowRecordBatch.to_pyarrow()",
        )?;
        PyRecordBatch::new(self.batch.clone()).into_pyarrow(py)
    }

    fn column(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        let idx = column_index_arg(column, self.batch.schema_ref())?;
        map_core(self.table_data()?.column_by_index(idx)).map(ArrowColumn::new)
    }

    fn get_column(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        self.column(column)
    }

    fn __getitem__(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        self.column(column)
    }

    fn select(&self, columns: &Bound<'_, PyAny>) -> PyResult<Self> {
        let indices = column_indices_arg(columns, self.batch.schema_ref())?;
        let table = map_core(self.table_data()?.select_indices(&indices))?;
        let batch = table
            .batches
            .into_iter()
            .next()
            .unwrap_or_else(|| RecordBatch::new_empty(self.batch.schema()));
        Ok(Self::new(batch))
    }

    #[pyo3(signature = (offset=0, length=None))]
    fn slice(&self, offset: usize, length: Option<usize>) -> Self {
        if offset >= self.batch.num_rows() || length == Some(0) {
            return Self::new(RecordBatch::new_empty(self.batch.schema()));
        }
        let take = length
            .unwrap_or(self.batch.num_rows() - offset)
            .min(self.batch.num_rows() - offset);
        Self::new(self.batch.slice(offset, take))
    }

    fn __len__(&self) -> usize {
        self.batch.num_rows()
    }

    fn __repr__(&self) -> String {
        format!(
            "xbbg.ArrowRecordBatch(num_rows={}, num_columns={}, columns={:?})",
            self.batch.num_rows(),
            self.batch.num_columns(),
            self.column_names()
        )
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct ArrowTable {
    data: TableData,
}

impl ArrowTable {
    pub fn try_new(batches: Vec<RecordBatch>) -> PyResult<Self> {
        map_core(TableData::try_new(batches)).map(Self::from_data)
    }

    fn from_data(data: TableData) -> Self {
        Self { data }
    }

    fn to_stream_pycapsule<'py>(
        py: Python<'py>,
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let fields = schema.fields();
        let array_reader = batches.into_iter().map(|batch| {
            Ok::<ArrayRef, arrow_schema::ArrowError>(Arc::new(StructArray::from(batch)) as ArrayRef)
        });
        let field =
            Field::new_struct("", fields.clone(), false).with_metadata(schema.metadata().clone());
        let array_reader = Box::new(ArrayIterator::new(array_reader, field.into()));
        to_stream_pycapsule(py, array_reader, requested_schema)
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow stream export failed: {e}")))
    }
}

#[pymethods]
impl ArrowTable {
    #[classmethod]
    #[pyo3(signature = (rows, schema=None))]
    fn from_pylist(
        _cls: &Bound<'_, PyType>,
        rows: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        if schema.is_some() {
            return Err(PyValueError::new_err(
                "ArrowTable.from_pylist schema override is not implemented; pass dict rows with desired keys",
            ));
        }
        Self::try_new(vec![rows_to_batch(rows)?])
    }

    #[classmethod]
    fn empty(_cls: &Bound<'_, PyType>, schema_or_columns: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(names) = schema_or_columns.extract::<Vec<String>>() {
            return map_core(TableData::empty_from_columns(names)).map(Self::from_data);
        }
        if let Ok(schema) = schema_or_columns.extract::<PyRef<'_, ArrowSchema>>() {
            return Self::try_new(vec![RecordBatch::new_empty(schema.schema.clone())]);
        }
        Err(PyTypeError::new_err(
            "ArrowTable.empty expects a sequence of column names or ArrowSchema",
        ))
    }

    #[classmethod]
    fn from_batches(_cls: &Bound<'_, PyType>, batches: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut rust_batches = Vec::new();
        for item in PyIterator::from_object(batches)? {
            let item = item?;
            let batch = item.extract::<PyRef<'_, ArrowRecordBatch>>()?;
            rust_batches.push(batch.batch.clone());
        }
        Self::try_new(rust_batches)
    }

    #[classmethod]
    fn concat_tables(_cls: &Bound<'_, PyType>, tables: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut rust_tables = Vec::new();
        for item in PyIterator::from_object(tables)? {
            let item = item?;
            let table = item.extract::<PyRef<'_, ArrowTable>>()?;
            rust_tables.push(table.data.clone());
        }
        map_core(TableData::concat_tables(&rust_tables)).map(Self::from_data)
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.data.column_names()
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.data.num_rows()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.data.num_columns()
    }

    #[getter]
    fn shape(&self) -> (usize, usize) {
        self.data.shape()
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.data.nbytes()
    }

    #[getter]
    fn chunk_lengths(&self) -> Vec<usize> {
        self.data.chunk_lengths()
    }

    #[getter]
    fn columns(&self) -> PyResult<Vec<ArrowColumn>> {
        (0..self.data.num_columns())
            .map(|idx| map_core(self.data.column_by_index(idx)).map(ArrowColumn::new))
            .collect()
    }

    #[getter]
    fn schema(&self) -> ArrowSchema {
        ArrowSchema {
            schema: self.data.schema.clone(),
        }
    }

    #[getter]
    fn metadata(&self) -> HashMap<String, String> {
        self.data.schema.metadata().clone()
    }

    #[getter]
    fn eid_data(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_metadata_json(py, self.data.schema.metadata(), METADATA_KEY_EID_DATA)
    }

    #[getter]
    fn security_errors(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_metadata_json(py, self.data.schema.metadata(), METADATA_KEY_SECURITY_ERRORS)
    }

    #[getter]
    fn field_exceptions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        parse_metadata_json(py, self.data.schema.metadata(), METADATA_KEY_FIELD_EXCEPTIONS)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        Self::to_stream_pycapsule(
            py,
            self.data.batches.clone(),
            self.data.schema.clone(),
            requested_schema,
        )
    }

    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.data.schema.as_ref())
            .map_err(|e| PyRuntimeError::new_err(format!("Arrow schema export failed: {e}")))
    }

    fn to_batches(&self, py: Python<'_>) -> PyResult<Vec<Py<ArrowRecordBatch>>> {
        self.data
            .batches
            .iter()
            .cloned()
            .map(|batch| Py::new(py, ArrowRecordBatch::new(batch)))
            .collect()
    }

    fn to_pylist(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        table_to_pylist(py, &self.data)
    }

    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ensure_optional_module(
            py,
            "pyarrow",
            "pip install xbbg[pyarrow]",
            "ArrowTable.to_pyarrow()",
        )?;
        PyTable::try_new(self.data.batches.clone(), self.data.schema.clone())?.into_pyarrow(py)
    }

    fn to_pandas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ensure_optional_module(
            py,
            "pandas",
            "pip install xbbg[pandas]",
            "ArrowTable.to_pandas()",
        )?;
        let pd = py.import("pandas")?;
        let dataframe = pd.getattr("DataFrame")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("columns", self.column_names())?;
        dataframe.call_method("from_records", (self.to_pylist(py)?,), Some(&kwargs))
    }

    fn to_polars<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ensure_optional_module(
            py,
            "polars",
            "pip install xbbg[polars]",
            "ArrowTable.to_polars()",
        )?;
        let polars = py.import("polars")?;
        let table = Py::new(py, self.clone())?;
        match polars.call_method1("from_arrow", (table,)) {
            Ok(frame) => Ok(frame),
            Err(err) if err.to_string().contains("pyarrow") => {
                let dataframe = polars.getattr("DataFrame")?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("schema", self.column_names())?;
                dataframe.call((self.to_pylist(py)?,), Some(&kwargs))
            }
            Err(err) => Err(err),
        }
    }

    fn column(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        let idx = column_index_arg(column, &self.data.schema)?;
        map_core(self.data.column_by_index(idx)).map(ArrowColumn::new)
    }

    fn get_column(&self, column: &Bound<'_, PyAny>) -> PyResult<ArrowColumn> {
        self.column(column)
    }

    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if key.cast::<PyList>().is_ok() || key.cast::<PyTuple>().is_ok() {
            return Py::new(py, self.select(key)?).map(|obj| obj.into_any());
        }
        Py::new(py, self.column(key)?).map(|obj| obj.into_any())
    }

    fn __len__(&self) -> usize {
        self.num_rows()
    }

    fn select(&self, columns: &Bound<'_, PyAny>) -> PyResult<Self> {
        let indices = column_indices_arg(columns, &self.data.schema)?;
        map_core(self.data.select_indices(&indices)).map(Self::from_data)
    }

    fn select_columns(&self, names: Vec<String>) -> PyResult<Self> {
        map_core(self.data.select_names(&names)).map(Self::from_data)
    }

    fn drop_columns(&self, names: Vec<String>) -> PyResult<Self> {
        map_core(self.data.drop_columns(&names)).map(Self::from_data)
    }

    fn rename_columns(&self, mapping: HashMap<String, String>) -> PyResult<Self> {
        map_core(self.data.rename_columns(&mapping)).map(Self::from_data)
    }

    fn rename(&self, mapping: HashMap<String, String>) -> PyResult<Self> {
        self.rename_columns(mapping)
    }

    #[pyo3(signature = (offset=0, length=None))]
    fn slice(&self, offset: usize, length: Option<usize>) -> PyResult<Self> {
        map_core(self.data.slice(offset, length)).map(Self::from_data)
    }

    fn tail(&self, n: usize) -> PyResult<Self> {
        map_core(self.data.tail(n)).map(Self::from_data)
    }

    fn sort_by(&self, sort_keys: Vec<(String, String)>) -> PyResult<Self> {
        let keys = sort_keys
            .into_iter()
            .map(|(name, direction)| {
                SortDirection::from_str(&direction)
                    .map(|direction| (name.clone(), direction))
                    .map_err(|_| {
                        PyValueError::new_err(format!(
                            "unsupported sort direction for {name}: {direction}"
                        ))
                    })
            })
            .collect::<PyResult<Vec<_>>>()?;
        map_core(self.data.sort_by(&keys)).map(Self::from_data)
    }

    fn filter_eq(&self, column: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let needle = py_value_to_cell(value)?;
        map_core(self.data.filter_eq(column, &needle)).map(Self::from_data)
    }

    fn add_column(&self, index: usize, name: &str, values: &Bound<'_, PyAny>) -> PyResult<Self> {
        let cells = py_sequence_to_cells(values)?;
        map_core(self.data.add_column(index, name, &cells)).map(Self::from_data)
    }

    fn set_column(&self, index: usize, name: &str, values: &Bound<'_, PyAny>) -> PyResult<Self> {
        let cells = py_sequence_to_cells(values)?;
        map_core(self.data.set_column(index, name, &cells)).map(Self::from_data)
    }

    fn head(&self, n: usize) -> PyResult<Self> {
        map_core(self.data.head(n)).map(Self::from_data)
    }

    fn has_any_value(&self, names: Vec<String>) -> bool {
        for name in names {
            let Ok(idx) = self.data.schema.index_of(&name) else {
                continue;
            };
            for batch in &self.data.batches {
                let array = batch.column(idx);
                for row in 0..array.len() {
                    if cell_has_value(&cell_from_array(array.as_ref(), row)) {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn apply_historical_presentation(
        &self,
        show_date: Option<bool>,
        date_format: Option<String>,
        sort: Option<String>,
        periodicity: Option<String>,
    ) -> PyResult<Self> {
        let mut result = self.clone();

        if let Some(sort) = sort {
            if result.data.schema.index_of("date").is_ok() {
                let date_order = if sort.eq_ignore_ascii_case("DESCENDING") {
                    "descending"
                } else {
                    "ascending"
                };
                let mut sort_keys = Vec::new();
                if result.data.schema.index_of("ticker").is_ok() {
                    sort_keys.push(("ticker".to_string(), "ascending".to_string()));
                }
                sort_keys.push(("date".to_string(), date_order.to_string()));
                if result.data.schema.index_of("field").is_ok() {
                    sort_keys.push(("field".to_string(), "ascending".to_string()));
                }
                result = result.sort_by(sort_keys)?;
            }
        }

        if let Some(date_format) = date_format {
            let normalized = date_format.to_ascii_uppercase();
            if matches!(normalized.as_str(), "PERIODIC" | "BOTH") {
                if let Ok(date_idx) = result.data.schema.index_of("date") {
                    let mut period_cells = Vec::with_capacity(result.num_rows());
                    for batch in &result.data.batches {
                        let date_array = batch.column(date_idx);
                        for row in 0..date_array.len() {
                            period_cells.push(CellValue::Text(period_label(
                                date_array.as_ref(),
                                row,
                                periodicity.as_deref(),
                            )));
                        }
                    }
                    let data = if normalized == "PERIODIC" {
                        result
                            .data
                            .with_cells_column(date_idx, "period", &period_cells, true, true)
                    } else {
                        result.data.with_cells_column(
                            date_idx + 1,
                            "period",
                            &period_cells,
                            false,
                            true,
                        )
                    };
                    result = Self::from_data(map_core(data)?);
                }
            }
        }

        if show_date == Some(false) {
            result = result.drop_columns(vec!["date".to_string(), "period".to_string()])?;
        }

        Ok(result)
    }

    fn reshape_bqr_generic(&self, ticker: &str) -> PyResult<Self> {
        let Ok(path_idx) = self.data.schema.index_of("path") else {
            return map_core(TableData::empty_from_columns(vec![
                "ticker".to_string(),
                "time".to_string(),
                "type".to_string(),
                "value".to_string(),
                "size".to_string(),
            ]))
            .map(Self::from_data);
        };
        let value_str_idx = self.data.schema.index_of("value_str").ok();
        let value_num_idx = self.data.schema.index_of("value_num").ok();
        let batch = map_core(self.data.combined_batch())?;
        let mut fields = BTreeSet::new();
        let mut records: BTreeMap<usize, HashMap<String, CellValue>> = BTreeMap::new();

        for row in 0..batch.num_rows() {
            let Some(path) = cell_to_string(&cell_from_array(batch.column(path_idx).as_ref(), row))
            else {
                continue;
            };
            let Some((idx, field)) = parse_bqr_path(&path) else {
                continue;
            };
            fields.insert(field.clone());
            let value_str = value_str_idx
                .map(|idx| cell_from_array(batch.column(idx).as_ref(), row))
                .unwrap_or(CellValue::Null);
            let value_num = value_num_idx
                .map(|idx| cell_from_array(batch.column(idx).as_ref(), row))
                .unwrap_or(CellValue::Null);
            let value = if cell_has_value(&value_str) {
                value_str
            } else {
                value_num
            };
            records.entry(idx).or_default().insert(field, value);
        }

        if records.is_empty() {
            return map_core(TableData::empty_from_columns(vec![
                "ticker".to_string(),
                "time".to_string(),
                "type".to_string(),
                "value".to_string(),
                "size".to_string(),
            ]))
            .map(Self::from_data);
        }

        let priority = ["ticker", "time", "type", "value", "size"];
        let mut columns = vec!["ticker".to_string()];
        for name in priority.iter().skip(1) {
            if fields.contains(*name) {
                columns.push((*name).to_string());
            }
        }
        for name in fields {
            if !priority.contains(&name.as_str()) {
                columns.push(name);
            }
        }

        let mut out_fields = Vec::with_capacity(columns.len());
        let mut out_arrays = Vec::with_capacity(columns.len());
        for column in &columns {
            let cells = records
                .values()
                .map(|record| {
                    if column == "ticker" {
                        CellValue::Text(ticker.to_string())
                    } else {
                        record.get(column).cloned().unwrap_or(CellValue::Null)
                    }
                })
                .collect::<Vec<_>>();
            let (field, array) = build_array(column, &cells);
            out_fields.push(field);
            out_arrays.push(array);
        }


        let batch = RecordBatch::try_new(Arc::new(Schema::new(out_fields)), out_arrays)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Self::try_new(vec![batch])
    }

    fn __repr__(&self) -> String {
        format!(
            "xbbg.ArrowTable(num_rows={}, num_columns={}, columns={:?})",
            self.num_rows(),
            self.num_columns(),
            self.column_names()
        )
    }
}

pub(crate) fn record_batch_to_arrow_record_batch(
    py: Python<'_>,
    batch: RecordBatch,
) -> PyResult<Py<PyAny>> {
    Py::new(py, ArrowRecordBatch::new(batch)).map(|obj| obj.into_any())
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ArrowField>()?;
    m.add_class::<ArrowSchema>()?;
    m.add_class::<ArrowColumn>()?;
    m.add_class::<ArrowRecordBatch>()?;
    m.add_class::<ArrowTable>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_to_py_preserves_timestamp_timezone_metadata() {
        Python::initialize();
        Python::attach(|py| {
            let array = TimestampMicrosecondArray::from(vec![0]).with_timezone("Asia/Hong_Kong");
            let value = scalar_to_py(py, &array, 0).expect("timestamp");
            let iso: String = value
                .bind(py)
                .call_method0("isoformat")
                .expect("isoformat")
                .extract()
                .expect("iso string");

            assert_eq!(iso, "1970-01-01T08:00:00+08:00");
        });
    }

    #[test]
    fn scalar_to_py_preserves_utc_timestamp_default() {
        Python::initialize();
        Python::attach(|py| {
            let array = TimestampMicrosecondArray::from(vec![0]);
            let value = scalar_to_py(py, &array, 0).expect("timestamp");
            let iso: String = value
                .bind(py)
                .call_method0("isoformat")
                .expect("isoformat")
                .extract()
                .expect("iso string");

            assert_eq!(iso, "1970-01-01T00:00:00+00:00");
        });
    }

    #[test]
    fn arrow_table_metadata_getters_parse_xbbg_json() {
        Python::initialize();
        Python::attach(|py| {
            let schema = Schema::new(vec![Field::new("ticker", DataType::Utf8, false)])
                .with_metadata(HashMap::from([
                    (
                        METADATA_KEY_EID_DATA.to_string(),
                        r#"{"IBM US Equity":[101,202]}"#.to_string(),
                    ),
                    (
                        METADATA_KEY_SECURITY_ERRORS.to_string(),
                        r#"{"BAD Ticker":{"category":"BAD_SEC","code":1,"subcategory":"INVALID","message":"bad security"}}"#.to_string(),
                    ),
                    (
                        METADATA_KEY_FIELD_EXCEPTIONS.to_string(),
                        r#"{"IBM US Equity":[{"field":"PX_BAD","category":"BAD_FLD","code":2,"subcategory":"INVALID","message":"bad field"}]}"#.to_string(),
                    ),
                ]));
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![Arc::new(StringArray::from(vec!["IBM US Equity"]))],
            )
            .expect("record batch");
            let table = ArrowTable::try_new(vec![batch]).expect("table");

            assert!(table.metadata().contains_key(METADATA_KEY_EID_DATA));
            let eid_data = table.eid_data(py).expect("eid data");
            let eid_dict = eid_data.bind(py);
            let eids: Vec<i32> = eid_dict
                .get_item("IBM US Equity")
                .expect("ticker eids")
                .extract()
                .expect("eids");
            assert_eq!(eids, vec![101, 202]);

            let security_errors = table.security_errors(py).expect("security errors");
            assert!(security_errors
                .bind(py)
                .get_item("BAD Ticker")
                .expect("security error")
                .is_instance_of::<PyDict>());

            let field_exceptions = table.field_exceptions(py).expect("field exceptions");
            assert!(field_exceptions
                .bind(py)
                .get_item("IBM US Equity")
                .expect("field exception")
                .is_instance_of::<PyList>());
        });
    }
}
