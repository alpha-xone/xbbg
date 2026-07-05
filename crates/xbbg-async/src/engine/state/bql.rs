//! BQL (Bloomberg Query Language) state with Arrow builders.
//!
//! BQL responses contain structured result data that we extract directly
//! from Bloomberg Elements without JSON intermediate serialization.
//!
//! Note: BQL can return complex nested structures. We flatten them into
//! a tabular format with id column + value columns per field.

use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    sync::Arc,
};
use tokio::sync::oneshot;

use super::typed_builder::ColumnSet;
use super::value_utils::top_level_response_error;
use xbbg_core::{BlpError, Message};

const BQL_TYPED_JSON_MAX_BYTES: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BqlColumnKind {
    Numeric,
    String,
    Infer,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BqlJsonResponse<'a> {
    #[serde(default, borrow)]
    client_context: Option<BqlClientContext<'a>>,
    #[serde(default, borrow)]
    response_exceptions: Option<Vec<BqlException<'a>>>,
    #[serde(default, borrow)]
    results: Option<BTreeMap<String, BqlJsonField<'a>>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BqlClientContext<'a> {
    #[serde(default, borrow)]
    client_request_id: Option<Cow<'a, str>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BqlJsonField<'a> {
    #[serde(default, borrow)]
    id_column: Option<BqlJsonColumn<'a>>,
    #[serde(default, borrow)]
    values_column: Option<BqlJsonColumn<'a>>,
    #[serde(default, borrow)]
    secondary_columns: Vec<BqlJsonColumn<'a>>,
    #[serde(default, borrow)]
    response_exceptions: Option<Vec<BqlException<'a>>>,
}

#[derive(Debug, Deserialize)]
struct BqlJsonColumn<'a> {
    #[serde(default, borrow)]
    name: Option<Cow<'a, str>>,
    #[serde(default, rename = "type", borrow)]
    data_type: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    values: Vec<BqlCell<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BqlException<'a> {
    #[serde(default, borrow)]
    message: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    node_name: Option<Cow<'a, str>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BqlCell<'a> {
    String(#[serde(borrow)] Cow<'a, str>),
    Number(f64),
    Bool(bool),
    Null,
    Other(Box<JsonValue>),
}

impl BqlCell<'_> {
    fn append_as_string(&self, builder: &mut StringBuilder) {
        match self {
            Self::String(s) => builder.append_value(s.as_ref()),
            Self::Null => builder.append_null(),
            Self::Number(n) => builder.append_value(n.to_string()),
            Self::Bool(b) => builder.append_value(b.to_string()),
            Self::Other(value) => builder.append_value(value.to_string()),
        }
    }

    fn append_as_id(&self, builder: &mut StringBuilder) {
        match self {
            Self::Null => builder.append_value(""),
            other => other.append_as_string(builder),
        }
    }
}

/// State for a BQL request.
pub struct BqlState {
    /// Column set for building the output
    columns: ColumnSet,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    /// Accumulated JSON string (for JSON-encoded responses)
    json_buffer: Option<String>,
}

impl BqlState {
    /// Create a new BQL state.
    pub fn new(reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            columns: ColumnSet::new(),
            reply,
            json_buffer: None,
        }
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        self.process_message(msg);
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/bqlsvc", "sendQuery") {
            let _ = self.reply.send(Err(error));
            return;
        }

        self.process_message(msg);

        // If we accumulated JSON, try to parse it
        let result = if let Some(json_str) = self.json_buffer.take() {
            self.parse_bql_json(&json_str)
        } else {
            self.columns.finish()
        };

        if let Ok(ref batch) = result {
            xbbg_log::debug!(
                rows = batch.num_rows(),
                cols = batch.num_columns(),
                "bql finish"
            );
        }
        let _ = self.reply.send(result);
    }

    /// Process a BQL response message using Element API.
    ///
    /// BQL response structure:
    /// ```text
    /// beqlData {
    ///   results[] {
    ///     ... varies by query
    ///   }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();

        // Try different BQL response structures
        // Structure 1: beqlData -> results
        if let Some(beql_data) = root.get_by_str("beqlData") {
            if let Some(results) = beql_data.get_by_str("results") {
                // Check if first result is a JSON string
                if !results.is_empty() {
                    if let Some(first) = results.get_element(0) {
                        if let Some(xbbg_core::Value::String(s)) = first.get_value(0) {
                            // This is a JSON-encoded response
                            if s.starts_with('{') {
                                self.json_buffer = Some(s.to_string());
                                return;
                            }
                        }
                    }
                }
                self.extract_results(&results);
                return;
            }

            // Check for direct JSON string in beqlData
            if let Some(xbbg_core::Value::String(s)) = beql_data.get_value(0) {
                if s.starts_with('{') {
                    self.json_buffer = Some(s.to_string());
                    return;
                }
            }
        }

        // Structure 2: Direct results array
        if let Some(results) = root.get_by_str("results") {
            self.extract_results(&results);
            return;
        }

        // Structure 3: Check if root contains a JSON string value
        if let Some(xbbg_core::Value::String(s)) = root.get_value(0) {
            if s.starts_with('{') {
                self.json_buffer = Some(s.to_string());
                return;
            }
        }

        // Structure 4: Flatten the entire response (fallback)
        self.flatten_element("", &root);
    }

    /// Parse BQL JSON response into a proper table.
    ///
    /// Bloomberg BQL JSON response structure:
    /// ```json
    /// {
    ///   "clientContext": { "clientRequestId": "...", ... },
    ///   "responseExceptions": null | [{ "message", "messageCategory",
    ///       "messageSubcategory", "nodeName", "type" }],
    ///   "results": null | {
    ///     "field_name": {
    ///       "idColumn": { "name": "ID", "type": "STRING", "values": [...] },
    ///       "valuesColumn": { "name": "VALUE", "type": "DOUBLE"|..., "values": [...] },
    ///       "secondaryColumns": [{ "name": "DATE"|"CURRENCY", "values": [...] }],
    ///       "responseExceptions": [],
    ///       "partialErrorMap": { "errorIterator": null | [...] }
    ///     }
    ///   }
    /// }
    /// ```
    fn parse_bql_json(&self, json_str: &str) -> Result<RecordBatch, BlpError> {
        if json_str.len() <= BQL_TYPED_JSON_MAX_BYTES {
            self.parse_bql_json_typed(json_str)
        } else {
            self.parse_bql_json_value(json_str)
        }
    }

    fn parse_bql_json_typed(&self, json_str: &str) -> Result<RecordBatch, BlpError> {
        let response: BqlJsonResponse<'_> =
            serde_json::from_str(json_str).map_err(|e| BlpError::Internal {
                detail: format!("Failed to parse BQL JSON: {}", e),
            })?;

        let request_id = response
            .client_context
            .as_ref()
            .and_then(|c| c.client_request_id.as_deref())
            .map(str::to_string);

        // Collect top-level responseExceptions (syntax errors, invalid fields, etc.)
        let top_exceptions =
            Self::extract_exception_messages(response.response_exceptions.as_deref());

        // Route on results: object → parse fields, null/missing/empty → empty or error.
        let Some(results_obj) = response.results.as_ref().filter(|obj| !obj.is_empty()) else {
            if !top_exceptions.is_empty() {
                return Err(BlpError::RequestFailure {
                    service: "//blp/bqlsvc".into(),
                    operation: Some("sendQuery".into()),
                    cid: None,
                    label: None,
                    request_id,
                    source: Some(top_exceptions.join("; ").into()),
                });
            }
            return Self::empty_batch();
        };

        // Results present — log any partial exceptions as warnings but continue.
        if !top_exceptions.is_empty() {
            xbbg_log::warn!(
                exceptions = top_exceptions.join("; ").as_str(),
                "BQL response has partial exceptions but results are present"
            );
        }

        // Collect field names and determine row count from the first field.
        // Keep slices into the typed parsed JSON instead of materializing cloned
        // cell vectors; the parsed tree lives until Arrow arrays have been built.
        let mut id_values: &[BqlCell<'_>] = &[];
        type FieldCol<'a> = (String, &'a [BqlCell<'a>], Option<&'a str>);
        let mut field_columns: Vec<FieldCol<'_>> = Vec::new();
        let mut field_column_names: HashSet<String> = HashSet::new();
        for (field_name, field_data) in results_obj {
            // Extract idColumn values (only need to do this once).
            if id_values.is_empty() {
                if let Some(id_col) = &field_data.id_column {
                    id_values = id_col.values.as_slice();
                }
            }

            // Extract secondaryColumns (e.g. DATE, CURRENCY) — time-series and
            // multi-axis BQL queries return per-field auxiliary dimensions here.
            // Emit each distinct column once, placed before the primary value
            // column so the natural column order is ticker, DATE, <field>.
            for sec_col in &field_data.secondary_columns {
                let Some(col_name) = sec_col.name.as_deref() else {
                    continue;
                };
                let col_name_lower = col_name.to_lowercase();
                if !field_column_names.insert(col_name_lower.clone()) {
                    continue;
                }
                field_columns.push((
                    col_name_lower,
                    sec_col.values.as_slice(),
                    sec_col.data_type.as_deref(),
                ));
            }

            // Extract valuesColumn values and type hint.
            let (values, val_type) = field_data
                .values_column
                .as_ref()
                .map(|col| (col.values.as_slice(), col.data_type.as_deref()))
                .unwrap_or((&[][..], None));

            // Warn about per-field partial errors.
            let field_exceptions =
                Self::extract_exception_messages(field_data.response_exceptions.as_deref());
            if !field_exceptions.is_empty() {
                xbbg_log::warn!(
                    field = field_name.as_str(),
                    exceptions = field_exceptions.join("; ").as_str(),
                    "BQL field has partial errors"
                );
            }

            field_columns.push((field_name.to_string(), values, val_type));
            field_column_names.insert(field_name.to_string());
        }

        // Build Arrow arrays.
        // Use "ticker" for the id column to avoid conflicts with user-requested "id" field.
        let row_count = id_values.len();
        let mut id_builder = Self::string_builder(row_count);
        for value in id_values {
            value.append_as_id(&mut id_builder);
        }

        let mut fields = vec![Field::new("ticker", DataType::Utf8, true)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(id_builder.finish())];

        for (name, values, type_hint) in &field_columns {
            Self::append_bql_cell_column(
                name.as_str(),
                values,
                *type_hint,
                row_count,
                &mut fields,
                &mut arrays,
            );
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("Failed to create RecordBatch: {}", e),
        })
    }

    fn parse_bql_json_value(&self, json_str: &str) -> Result<RecordBatch, BlpError> {
        let json: JsonValue = serde_json::from_str(json_str).map_err(|e| BlpError::Internal {
            detail: format!("Failed to parse BQL JSON: {}", e),
        })?;

        let request_id = json
            .get("clientContext")
            .and_then(|c| c.get("clientRequestId"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let top_exceptions = Self::extract_exception_messages_value(&json);

        let results_obj = match json.get("results") {
            Some(JsonValue::Object(obj)) if !obj.is_empty() => obj,
            Some(JsonValue::Object(_)) | Some(JsonValue::Null) | None => {
                if !top_exceptions.is_empty() {
                    return Err(BlpError::RequestFailure {
                        service: "//blp/bqlsvc".into(),
                        operation: Some("sendQuery".into()),
                        cid: None,
                        label: None,
                        request_id,
                        source: Some(top_exceptions.join("; ").into()),
                    });
                }
                return Self::empty_batch();
            }
            Some(other) => {
                return Err(BlpError::Internal {
                    detail: format!("BQL 'results' has unexpected type: {other}"),
                });
            }
        };

        if !top_exceptions.is_empty() {
            xbbg_log::warn!(
                exceptions = top_exceptions.join("; ").as_str(),
                "BQL response has partial exceptions but results are present"
            );
        }

        let field_names: Vec<&String> = results_obj.keys().collect();
        let mut id_values: &[JsonValue] = &[];
        type FieldCol<'a> = (String, &'a [JsonValue], Option<&'a str>);
        let mut field_columns: Vec<FieldCol<'_>> = Vec::new();
        let mut field_column_names: HashSet<String> = HashSet::new();
        for field_name in &field_names {
            let field_data = &results_obj[*field_name];

            if id_values.is_empty() {
                if let Some(id_col) = field_data.get("idColumn") {
                    if let Some(values) = id_col.get("values") {
                        if let Some(arr) = values.as_array() {
                            id_values = arr.as_slice();
                        }
                    }
                }
            }

            if let Some(JsonValue::Array(sec_cols)) = field_data.get("secondaryColumns") {
                for sec_col in sec_cols {
                    let Some(col_name) = sec_col.get("name").and_then(|n| n.as_str()) else {
                        continue;
                    };
                    let Some(JsonValue::Array(col_vals)) = sec_col.get("values") else {
                        continue;
                    };
                    let col_name_lower = col_name.to_lowercase();
                    if !field_column_names.insert(col_name_lower.clone()) {
                        continue;
                    }
                    let sec_type = sec_col.get("type").and_then(|t| t.as_str());
                    field_columns.push((col_name_lower, col_vals.as_slice(), sec_type));
                }
            }

            let mut values: &[JsonValue] = &[];
            let mut val_type: Option<&str> = None;
            if let Some(val_col) = field_data.get("valuesColumn") {
                val_type = val_col.get("type").and_then(|t| t.as_str());
                if let Some(vals) = val_col.get("values") {
                    if let Some(arr) = vals.as_array() {
                        values = arr.as_slice();
                    }
                }
            }

            let field_exceptions = Self::extract_exception_messages_value(field_data);
            if !field_exceptions.is_empty() {
                xbbg_log::warn!(
                    field = field_name.as_str(),
                    exceptions = field_exceptions.join("; ").as_str(),
                    "BQL field has partial errors"
                );
            }

            field_columns.push((field_name.to_string(), values, val_type));
            field_column_names.insert(field_name.to_string());
        }

        let row_count = id_values.len();
        let mut id_builder = Self::string_builder(row_count);
        for value in id_values {
            match value {
                JsonValue::String(s) => id_builder.append_value(s),
                JsonValue::Null => id_builder.append_value(""),
                other => id_builder.append_value(other.to_string()),
            }
        }

        let mut fields = vec![Field::new("ticker", DataType::Utf8, true)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(id_builder.finish())];

        for (name, values, type_hint) in &field_columns {
            Self::append_json_value_column(
                name.as_str(),
                values,
                *type_hint,
                row_count,
                &mut fields,
                &mut arrays,
            );
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| BlpError::Internal {
            detail: format!("Failed to create RecordBatch: {}", e),
        })
    }

    /// Parse a cached/generated BQL JSON payload for benchmark-only replay.
    ///
    /// This is intentionally hidden behind `bench-internals` so production builds
    /// do not expose benchmark hooks or carry profiling behavior in public APIs.
    #[cfg(feature = "bench-internals")]
    pub fn parse_bql_json_for_bench(&self, json_str: &str) -> Result<RecordBatch, BlpError> {
        self.parse_bql_json(json_str)
    }

    fn column_kind(type_hint: Option<&str>) -> BqlColumnKind {
        match type_hint {
            Some(t)
                if t.eq_ignore_ascii_case("DOUBLE")
                    || t.eq_ignore_ascii_case("FLOAT")
                    || t.eq_ignore_ascii_case("INT32")
                    || t.eq_ignore_ascii_case("INT64")
                    || t.eq_ignore_ascii_case("INTEGER") =>
            {
                BqlColumnKind::Numeric
            }
            Some(t)
                if t.eq_ignore_ascii_case("STRING")
                    || t.eq_ignore_ascii_case("DATE")
                    || t.eq_ignore_ascii_case("DATETIME") =>
            {
                BqlColumnKind::String
            }
            _ => BqlColumnKind::Infer,
        }
    }

    fn append_bql_cell_column(
        name: &str,
        values: &[BqlCell<'_>],
        type_hint: Option<&str>,
        row_count: usize,
        fields: &mut Vec<Field>,
        arrays: &mut Vec<ArrayRef>,
    ) {
        match Self::column_kind(type_hint) {
            BqlColumnKind::Numeric => {
                let mut builder = Self::float_builder(row_count);
                for row_idx in 0..row_count {
                    match values.get(row_idx) {
                        Some(BqlCell::Number(n)) => builder.append_value(*n),
                        Some(BqlCell::String(s)) => {
                            if let Ok(f) = s.parse::<f64>() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                fields.push(Field::new(name, DataType::Float64, true));
                arrays.push(Arc::new(builder.finish()));
            }
            BqlColumnKind::String => {
                let mut builder = Self::string_builder(row_count);
                for row_idx in 0..row_count {
                    match values.get(row_idx) {
                        Some(value) => value.append_as_string(&mut builder),
                        None => builder.append_null(),
                    }
                }
                fields.push(Field::new(name, DataType::Utf8, true));
                arrays.push(Arc::new(builder.finish()));
            }
            BqlColumnKind::Infer => {
                Self::append_inferred_bql_cell_column(name, values, row_count, fields, arrays);
            }
        }
    }

    fn append_inferred_bql_cell_column(
        name: &str,
        values: &[BqlCell<'_>],
        row_count: usize,
        fields: &mut Vec<Field>,
        arrays: &mut Vec<ArrayRef>,
    ) {
        let mut numeric_values: Vec<Option<f64>> = Vec::with_capacity(row_count);
        let mut string_builder: Option<StringBuilder> = None;

        for row_idx in 0..row_count {
            if let Some(builder) = string_builder.as_mut() {
                match values.get(row_idx) {
                    Some(value) => value.append_as_string(builder),
                    None => builder.append_null(),
                }
                continue;
            }

            match values.get(row_idx) {
                Some(BqlCell::Number(n)) => numeric_values.push(Some(*n)),
                Some(BqlCell::Null) | None => numeric_values.push(None),
                Some(value) => {
                    let mut builder = Self::string_builder(row_count);
                    for numeric in &numeric_values {
                        match numeric {
                            Some(n) => builder.append_value(n.to_string()),
                            None => builder.append_null(),
                        }
                    }
                    value.append_as_string(&mut builder);
                    string_builder = Some(builder);
                }
            }
        }

        if let Some(mut builder) = string_builder {
            fields.push(Field::new(name, DataType::Utf8, true));
            arrays.push(Arc::new(builder.finish()));
            return;
        }

        let mut builder = Self::float_builder(row_count);
        for numeric in numeric_values {
            match numeric {
                Some(n) => builder.append_value(n),
                None => builder.append_null(),
            }
        }
        fields.push(Field::new(name, DataType::Float64, true));
        arrays.push(Arc::new(builder.finish()));
    }

    fn append_json_value_column(
        name: &str,
        values: &[JsonValue],
        type_hint: Option<&str>,
        row_count: usize,
        fields: &mut Vec<Field>,
        arrays: &mut Vec<ArrayRef>,
    ) {
        match Self::column_kind(type_hint) {
            BqlColumnKind::Numeric => {
                let mut builder = Self::float_builder(row_count);
                for row_idx in 0..row_count {
                    match values.get(row_idx) {
                        Some(JsonValue::Number(n)) => {
                            builder.append_value(n.as_f64().unwrap_or(f64::NAN));
                        }
                        Some(JsonValue::String(s)) => {
                            if let Ok(f) = s.parse::<f64>() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                fields.push(Field::new(name, DataType::Float64, true));
                arrays.push(Arc::new(builder.finish()));
            }
            BqlColumnKind::String => {
                let mut builder = Self::string_builder(row_count);
                for row_idx in 0..row_count {
                    match values.get(row_idx) {
                        Some(value) => Self::append_json_as_string(value, &mut builder),
                        None => builder.append_null(),
                    }
                }
                fields.push(Field::new(name, DataType::Utf8, true));
                arrays.push(Arc::new(builder.finish()));
            }
            BqlColumnKind::Infer => {
                Self::append_inferred_json_value_column(name, values, row_count, fields, arrays);
            }
        }
    }

    fn append_inferred_json_value_column(
        name: &str,
        values: &[JsonValue],
        row_count: usize,
        fields: &mut Vec<Field>,
        arrays: &mut Vec<ArrayRef>,
    ) {
        let mut numeric_values: Vec<Option<f64>> = Vec::with_capacity(row_count);
        let mut string_builder: Option<StringBuilder> = None;

        for row_idx in 0..row_count {
            if let Some(builder) = string_builder.as_mut() {
                match values.get(row_idx) {
                    Some(value) => Self::append_json_as_string(value, builder),
                    None => builder.append_null(),
                }
                continue;
            }

            match values.get(row_idx) {
                Some(JsonValue::Number(n)) => {
                    numeric_values.push(Some(n.as_f64().unwrap_or(f64::NAN)));
                }
                Some(JsonValue::Null) | None => numeric_values.push(None),
                Some(value) => {
                    let mut builder = Self::string_builder(row_count);
                    for numeric in &numeric_values {
                        match numeric {
                            Some(n) => builder.append_value(n.to_string()),
                            None => builder.append_null(),
                        }
                    }
                    Self::append_json_as_string(value, &mut builder);
                    string_builder = Some(builder);
                }
            }
        }

        if let Some(mut builder) = string_builder {
            fields.push(Field::new(name, DataType::Utf8, true));
            arrays.push(Arc::new(builder.finish()));
            return;
        }

        let mut builder = Self::float_builder(row_count);
        for numeric in numeric_values {
            match numeric {
                Some(n) => builder.append_value(n),
                None => builder.append_null(),
            }
        }
        fields.push(Field::new(name, DataType::Float64, true));
        arrays.push(Arc::new(builder.finish()));
    }

    fn append_json_as_string(value: &JsonValue, builder: &mut StringBuilder) {
        match value {
            JsonValue::String(s) => builder.append_value(s),
            JsonValue::Null => builder.append_null(),
            other => builder.append_value(other.to_string()),
        }
    }

    fn string_builder(row_count: usize) -> StringBuilder {
        if row_count <= 1 {
            StringBuilder::new()
        } else {
            StringBuilder::with_capacity(row_count, row_count.saturating_mul(16).max(1))
        }
    }

    fn float_builder(row_count: usize) -> Float64Builder {
        if row_count <= 1 {
            Float64Builder::new()
        } else {
            Float64Builder::with_capacity(row_count)
        }
    }

    /// Extract human-readable messages from a `responseExceptions` array.
    /// Works for both the top-level response and per-field exception arrays.
    fn extract_exception_messages(exceptions: Option<&[BqlException<'_>]>) -> Vec<String> {
        exceptions
            .unwrap_or_default()
            .iter()
            .filter_map(|exception| {
                exception.message.as_ref().map(|msg| {
                    if let Some(node) = exception.node_name.as_deref() {
                        format!("{msg} (in {node})")
                    } else {
                        msg.to_string()
                    }
                })
            })
            .collect()
    }

    fn extract_exception_messages_value(json: &JsonValue) -> Vec<String> {
        let Some(JsonValue::Array(exceptions)) = json.get("responseExceptions") else {
            return Vec::new();
        };
        exceptions
            .iter()
            .filter_map(|e| {
                e.get("message").and_then(|m| m.as_str()).map(|msg| {
                    if let Some(node) = e.get("nodeName").and_then(|n| n.as_str()) {
                        format!("{msg} (in {node})")
                    } else {
                        msg.to_string()
                    }
                })
            })
            .collect()
    }

    /// Return an empty single-column batch (no results).
    fn empty_batch() -> Result<RecordBatch, BlpError> {
        let schema = Schema::new(vec![Field::new("ticker", DataType::Utf8, true)]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .map_err(|e| BlpError::Internal {
            detail: format!("Failed to create empty batch: {}", e),
        })
    }

    /// Extract results from a BQL results element (legacy Element-API fallback).
    /// Note: secondaryColumns (DATE, CURRENCY) are only available in the JSON
    /// path — this path does not support them.
    fn extract_results(&mut self, results: &xbbg_core::Element) {
        xbbg_log::warn!(
            "BQL response routed to Element-API path — secondaryColumns will be missing"
        );
        let n = results.len();
        for i in 0..n {
            if let Some(row) = results.get_element(i) {
                // Each result row - extract all fields
                let num_children = row.num_children();
                for j in 0..num_children {
                    if let Some(child) = row.get_at(j) {
                        let name_str = child.name_str();
                        if let Some(value) = child.get_value(0) {
                            self.columns.append(name_str, value);
                        } else {
                            self.columns.append_null(name_str);
                        }
                    }
                }
                self.columns.end_row();
            }
        }
    }

    /// Flatten an element into path-value pairs (fallback for complex structures).
    fn flatten_element(&mut self, path: &str, element: &xbbg_core::Element) {
        let datatype = element.datatype();

        // For complex types, recurse into children
        if datatype.is_complex() {
            // If it's an array/sequence with values
            if element.is_array() {
                let n = element.len();
                for i in 0..n {
                    if let Some(child) = element.get_element(i) {
                        let child_path = if path.is_empty() {
                            format!("[{i}]")
                        } else {
                            format!("{path}[{i}]")
                        };
                        self.flatten_element(&child_path, &child);
                    }
                }
            } else {
                // Iterate named children
                let n = element.num_children();
                for i in 0..n {
                    if let Some(child) = element.get_at(i) {
                        let name = child.name_str();
                        let child_path = if path.is_empty() {
                            name.to_string()
                        } else {
                            format!("{}.{}", path, name)
                        };
                        self.flatten_element(&child_path, &child);
                    }
                }
            }
        } else {
            // Leaf value - add to columns
            if let Some(value) = element.get_value(0) {
                self.columns.append_str("path", path);

                // Convert value to string for generic representation
                let value_str = match &value {
                    xbbg_core::Value::String(s) | xbbg_core::Value::Enum(s) => s.to_string(),
                    xbbg_core::Value::Float64(f) => f.to_string(),
                    xbbg_core::Value::Int64(i) => i.to_string(),
                    xbbg_core::Value::Int32(i) => i.to_string(),
                    xbbg_core::Value::Bool(b) => b.to_string(),
                    xbbg_core::Value::Null => String::new(),
                    _ => format!("{:?}", value),
                };
                self.columns.append_str("value", &value_str);
                self.columns.end_row();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, StringArray};

    fn make_state() -> BqlState {
        let (tx, _rx) = oneshot::channel();
        BqlState::new(tx)
    }

    #[test]
    fn parse_bql_json_extracts_secondary_columns() {
        let json = r#"{
            "clientContext": { "clientRequestId": "abc" },
            "responseExceptions": null,
            "results": {
                "px_last": {
                    "idColumn": {
                        "name": "ID",
                        "type": "STRING",
                        "values": ["AAPL US Equity", "AAPL US Equity", "AAPL US Equity"]
                    },
                    "valuesColumn": {
                        "name": "VALUE",
                        "type": "DOUBLE",
                        "values": [150.1, 151.2, 152.3]
                    },
                    "secondaryColumns": [
                        {
                            "name": "DATE",
                            "type": "DATE",
                            "values": ["2026-04-10", "2026-04-11", "2026-04-14"]
                        },
                        {
                            "name": "CURRENCY",
                            "type": "STRING",
                            "values": ["USD", "USD", "USD"]
                        }
                    ],
                    "responseExceptions": [],
                    "partialErrorMap": { "errorIterator": null }
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["ticker", "date", "currency", "px_last"]);
        assert_eq!(batch.num_rows(), 3);

        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("date column is utf8");
        assert_eq!(dates.value(0), "2026-04-10");
        assert_eq!(dates.value(2), "2026-04-14");

        let px = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("px_last column is f64");
        assert_eq!(px.value(0), 150.1);
    }

    #[test]
    fn parse_bql_json_dedupes_secondary_columns_across_fields() {
        let json = r#"{
            "results": {
                "px_last": {
                    "idColumn": { "values": ["T"] },
                    "valuesColumn": { "values": [1.0] },
                    "secondaryColumns": [
                        { "name": "DATE", "values": ["2026-04-10"] }
                    ]
                },
                "px_open": {
                    "idColumn": { "values": ["T"] },
                    "valuesColumn": { "values": [0.9] },
                    "secondaryColumns": [
                        { "name": "DATE", "values": ["2026-04-10"] }
                    ]
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // DATE should appear exactly once, primary fields follow in insertion order
        let date_count = names.iter().filter(|n| **n == "date").count();
        assert_eq!(date_count, 1);
        assert!(names.contains(&"px_last"));
        assert!(names.contains(&"px_open"));
    }

    #[test]
    fn parse_bql_json_mismatched_field_lengths_truncates() {
        let json = r#"{
            "results": {
                "field_a": {
                    "idColumn": { "values": ["X", "Y"] },
                    "valuesColumn": { "type": "DOUBLE", "values": [1.0, 2.0] }
                },
                "field_b": {
                    "idColumn": { "values": ["X", "Y", "Z", "W"] },
                    "valuesColumn": { "type": "DOUBLE", "values": [10.0, 20.0, 30.0, 40.0] }
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
        let col_b = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("field_b is f64");
        assert_eq!(col_b.value(0), 10.0);
        assert_eq!(col_b.value(1), 20.0);
    }

    #[test]
    fn parse_bql_json_uses_type_hint_over_value_sniffing() {
        let json = r#"{
            "results": {
                "sector": {
                    "idColumn": { "values": ["AAPL"] },
                    "valuesColumn": { "type": "STRING", "values": ["Technology"] }
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("sector is utf8 via type hint");
        assert_eq!(col.value(0), "Technology");
    }

    fn large_bql_json_with_padding() -> String {
        let padding = "x".repeat(BQL_TYPED_JSON_MAX_BYTES);
        format!(
            r#"{{
            "clientContext": {{ "clientRequestId": "large-request" }},
            "responseExceptions": [{{ "message": "partial top-level warning", "nodeName": "query" }}],
            "padding": "{padding}",
            "results": {{
                "px_last": {{
                    "idColumn": {{
                        "name": "ID",
                        "type": "STRING",
                        "values": ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"]
                    }},
                    "valuesColumn": {{
                        "name": "VALUE",
                        "type": "DOUBLE",
                        "values": [150.1, null, "bad"]
                    }},
                    "secondaryColumns": [
                        {{
                            "name": "DATE",
                            "type": "DATE",
                            "values": ["2026-04-10", "2026-04-11", "2026-04-12"]
                        }},
                        {{
                            "name": "ASOF",
                            "type": "DATETIME",
                            "values": ["2026-04-10T12:30:00", "2026-04-11T12:30:00", "2026-04-12T12:30:00"]
                        }},
                        {{
                            "name": "CONFIDENCE",
                            "type": "STRING",
                            "values": ["0.95", null, "0.75"]
                        }}
                    ],
                    "responseExceptions": [{{ "message": "field warning", "nodeName": "px_last" }}]
                }},
                "rating": {{
                    "idColumn": {{ "type": "STRING", "values": ["AAPL US Equity", "MSFT US Equity", "IBM US Equity"] }},
                    "valuesColumn": {{ "type": "STRING", "values": ["1", "2", "3"] }},
                    "secondaryColumns": []
                }},
                "volume": {{
                    "idColumn": {{ "type": "STRING", "values": ["AAPL US Equity", "MSFT US Equity", "IBM US Equity", "TSLA US Equity"] }},
                    "valuesColumn": {{ "type": "INT64", "values": [1000, 2000, 3000, 4000] }},
                    "secondaryColumns": [
                        {{ "name": "DATE", "type": "DATE", "values": ["2026-04-10", "2026-04-11", "2026-04-12", "2026-04-13"] }}
                    ]
                }}
            }}
        }}"#
        )
    }

    #[test]
    fn parse_bql_json_large_payload_value_path_preserves_schema_and_values() {
        let json = large_bql_json_with_padding();
        assert!(json.len() > BQL_TYPED_JSON_MAX_BYTES);

        let batch = make_state().parse_bql_json(&json).expect("parse ok");
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "ticker",
                "date",
                "asof",
                "confidence",
                "px_last",
                "rating",
                "volume",
            ]
        );
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 7);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
        assert_eq!(schema.field(5).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(6).data_type(), &DataType::Float64);

        let ticker = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("ticker is utf8");
        assert_eq!(ticker.value(0), "AAPL US Equity");
        assert_eq!(ticker.value(2), "IBM US Equity");

        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("date is utf8");
        assert_eq!(dates.value(0), "2026-04-10");
        assert_eq!(dates.value(2), "2026-04-12");

        let asof = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("datetime is utf8");
        assert_eq!(asof.value(1), "2026-04-11T12:30:00");

        let confidence = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("confidence is utf8");
        assert_eq!(confidence.value(0), "0.95");
        assert!(confidence.is_null(1));

        let px_last = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("px_last is f64");
        assert_eq!(px_last.value(0), 150.1);
        assert!(px_last.is_null(1));
        assert!(px_last.is_null(2));

        let rating = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("rating remains utf8 via type hint");
        assert_eq!(rating.value(0), "1");
        assert_eq!(rating.value(2), "3");

        let volume = batch
            .column(6)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("volume is f64 via type hint");
        assert_eq!(volume.value(0), 1000.0);
        assert_eq!(volume.value(2), 3000.0);
    }

    #[test]
    fn parse_bql_json_keeps_date_and_datetime_as_utf8_for_compatibility() {
        let json = r#"{
            "results": {
                "event_count": {
                    "idColumn": { "type": "STRING", "values": ["AAPL US Equity"] },
                    "valuesColumn": { "type": "INT32", "values": [2] },
                    "secondaryColumns": [
                        { "name": "DATE", "type": "DATE", "values": ["2026-04-10"] },
                        { "name": "EVENT_TIME", "type": "DATETIME", "values": ["2026-04-10T09:30:00"] }
                    ]
                }
            }
        }"#;

        let batch = make_state().parse_bql_json(json).expect("parse ok");
        let schema = batch.schema();
        assert_eq!(schema.field(1).name(), "date");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), "event_time");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);

        let date = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("date remains utf8");
        let event_time = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("datetime remains utf8");
        assert_eq!(date.value(0), "2026-04-10");
        assert_eq!(event_time.value(0), "2026-04-10T09:30:00");
    }
}
