//! Historical data (bdh) state with Arrow builders.
//!
//! Extracts HistoricalDataResponse messages directly from Bloomberg Elements
//! without JSON intermediate serialization.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::refdata::{LongMode, OutputFormat};
use super::typed_builder::{ArrowType, ColumnSet};
use super::value_utils::{
    append_long_value_row, common_value_type, get_value_cached_datatype, top_level_response_error,
    FieldExceptionMeta, LongStringColumns, ResponseMetadata, SecurityErrorMeta, TypedLongColumns,
    WideColumns,
};
use xbbg_core::{BlpError, DataType as BlpDataType, Message, Name, Value};

struct HistDataElementNames {
    security_data: Name,
    security: Name,
    security_error: Name,
    field_data: Name,
    date: Name,
    eid_data: Name,
    field_exceptions: Name,
    field_id: Name,
    error_info: Name,
    category: Name,
    code: Name,
    subcategory: Name,
    message: Name,
}

impl HistDataElementNames {
    fn new() -> Self {
        Self {
            security_data: Name::get_or_intern("securityData"),
            security: Name::get_or_intern("security"),
            security_error: Name::get_or_intern("securityError"),
            field_data: Name::get_or_intern("fieldData"),
            date: Name::get_or_intern("date"),
            eid_data: Name::get_or_intern("eidData"),
            field_exceptions: Name::get_or_intern("fieldExceptions"),
            field_id: Name::get_or_intern("fieldId"),
            error_info: Name::get_or_intern("errorInfo"),
            category: Name::get_or_intern("category"),
            code: Name::get_or_intern("code"),
            subcategory: Name::get_or_intern("subcategory"),
            message: Name::get_or_intern("message"),
        }
    }
}

/// State for a historical data request (bdh).
pub struct HistDataState {
    /// Field names as strings
    field_names: Vec<String>,
    /// Pre-interned Bloomberg field names for hot lookups
    field_lookup_names: Vec<Name>,
    /// Observed Bloomberg data types for requested fields, learned from returned Elements
    field_value_datatypes: Vec<Option<BlpDataType>>,
    /// Observed Bloomberg data type for the structural date field
    date_datatype: Option<BlpDataType>,
    /// Pre-interned structural names for response traversal
    names: HistDataElementNames,
    /// Field type hints (field name -> arrow type)
    field_types: HashMap<String, ArrowType>,
    /// Output format
    format: OutputFormat,
    /// Long format mode (only used when format == Long)
    long_mode: LongMode,
    /// Column set for building the output
    columns: ColumnSet,
    /// Fixed long-format builders for the common string-value output path
    long_columns: Option<LongStringColumns>,
    /// Fixed typed-long builders for direct multi-value output
    typed_long_columns: Option<TypedLongColumns>,
    /// Fixed wide-format builders for requested field columns
    wide_columns: Option<WideColumns>,
    /// Security identifiers that returned securityError.
    failed_securities: Vec<String>,
    /// Response-level diagnostics (eidData / securityError / fieldExceptions)
    /// attached to the result batch as schema metadata.
    response_meta: ResponseMetadata,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl HistDataState {
    /// Create a new histdata state with Long format (default).
    pub fn new(fields: Vec<String>, reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self::with_format(fields, OutputFormat::Long, LongMode::String, None, reply)
    }

    /// Create a new histdata state with optional field type overrides (defaults to Long format).
    pub fn with_types(
        fields: Vec<String>,
        field_types: Option<HashMap<String, String>>,
        reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    ) -> Self {
        Self::with_format(
            fields,
            OutputFormat::Long,
            LongMode::String,
            field_types,
            reply,
        )
    }

    /// Create a new histdata state with specified format.
    pub fn with_format(
        fields: Vec<String>,
        format: OutputFormat,
        long_mode: LongMode,
        field_types: Option<HashMap<String, String>>,
        reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
    ) -> Self {
        // Convert string types to ArrowType, defaulting to Float64 for historical data
        let arrow_types: HashMap<String, ArrowType> = field_types
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, ArrowType::parse(&v)))
            .collect();
        let field_lookup_names: Vec<Name> = fields
            .iter()
            .map(|field| Name::get_or_intern(field))
            .collect();
        let field_value_datatypes = vec![None; field_lookup_names.len()];

        // Fixed long output modes bypass ColumnSet entirely; keep ColumnSet hints only
        // for dynamic wide/metadata paths that actually append through ColumnSet.
        let long_value_type = (format == OutputFormat::Long && long_mode == LongMode::String)
            .then(|| common_value_type(&fields, &arrow_types));
        let wide_columns =
            (format == OutputFormat::Wide).then(|| WideColumns::histdata(&fields, &arrow_types));
        let mut columns = ColumnSet::new();
        if long_value_type.is_none() && wide_columns.is_none() && long_mode != LongMode::Typed {
            for (name, arrow_type) in &arrow_types {
                columns.set_type_hint(name, *arrow_type);
            }
        }

        Self {
            field_names: fields,
            field_lookup_names,
            field_value_datatypes,
            date_datatype: None,
            names: HistDataElementNames::new(),
            field_types: arrow_types,
            format,
            long_mode,
            columns,
            long_columns: long_value_type.map(LongStringColumns::histdata),
            typed_long_columns: (format == OutputFormat::Long && long_mode == LongMode::Typed)
                .then(TypedLongColumns::histdata),
            wide_columns,
            failed_securities: Vec::new(),
            response_meta: ResponseMetadata::default(),
            reply,
        }
    }

    /// Process a PARTIAL_RESPONSE message.
    pub fn on_partial(&mut self, msg: &Message) {
        self.process_message(msg);
    }

    /// Process the final RESPONSE message and send the result via reply channel.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "HistoricalDataRequest")
        {
            let _ = self.reply.send(Err(error));
            return;
        }

        self.process_message(msg);

        let row_count = match self.format {
            OutputFormat::Long => self.long_columns.as_ref().map_or_else(
                || {
                    self.typed_long_columns
                        .as_ref()
                        .map_or_else(|| self.columns.row_count(), TypedLongColumns::row_count)
                },
                LongStringColumns::row_count,
            ),
            OutputFormat::Wide => self
                .wide_columns
                .as_ref()
                .map_or_else(|| self.columns.row_count(), WideColumns::row_count),
        };
        if row_count == 0 && !self.failed_securities.is_empty() {
            let detail = format!(
                "All securities failed: {}",
                self.failed_securities.join(", ")
            );
            let _ = self.reply.send(Err(BlpError::RequestFailure {
                service: "//blp/refdata".to_string(),
                operation: Some("HistoricalDataRequest".to_string()),
                cid: None,
                label: Some(detail),
                request_id: None,
                source: None,
            }));
            return;
        }
        let reply = self.reply;
        let response_meta = std::mem::take(&mut self.response_meta);
        let result = match self.format {
            OutputFormat::Long => match self.long_mode {
                LongMode::String => {
                    if let Some(long_columns) = self.long_columns.take() {
                        long_columns.finish_histdata()
                    } else {
                        self.columns
                            .finish_with_order(&["ticker", "date", "field", "value"])
                    }
                }
                LongMode::WithMetadata => self
                    .columns
                    .finish_with_order(&["ticker", "date", "field", "value", "dtype"]),
                LongMode::Typed => self
                    .typed_long_columns
                    .take()
                    .unwrap_or_else(TypedLongColumns::histdata)
                    .finish(),
            },
            OutputFormat::Wide => {
                if let Some(wide_columns) = self.wide_columns.take() {
                    wide_columns.finish_histdata()
                } else {
                    let mut order = vec!["ticker", "date"];
                    order.extend(self.field_names.iter().map(|s| s.as_str()));
                    self.columns.finish_with_order(&order)
                }
            }
        };
        let result = result.map(|batch| response_meta.attach(batch));
        if let Ok(batch) = &result {
            xbbg_log::debug!(
                rows = batch.num_rows(),
                cols = batch.num_columns(),
                "histdata finish"
            );
        }
        let _ = reply.send(result);
    }

    /// Process a HistoricalDataResponse message using Element API.
    ///
    /// Bloomberg structure:
    /// ```text
    /// HistoricalDataResponse {
    ///   securityData {
    ///     security: "AAPL US Equity"
    ///     fieldData[] {
    ///       date: 2024-01-15
    ///       PX_LAST: 150.0
    ///       VOLUME: 1000000
    ///       ...
    ///     }
    ///     fieldExceptions[]? { ... }
    ///     securityError? { ... }
    ///   }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();

        // Get securityData (note: singular in HistoricalDataResponse)
        let Some(security_data) = root.get(&self.names.security_data) else {
            trace!("No securityData in message");
            return;
        };

        // Get ticker
        let ticker = security_data
            .get(&self.names.security)
            .and_then(|e| e.get_str(0))
            .unwrap_or("");

        // eidData rides alongside fieldData when returnEids was requested.
        if let Some(eids) = security_data.get(&self.names.eid_data) {
            self.response_meta.record_eid_data(ticker, &eids);
        }

        // Check for security error
        if let Some(security_error) = security_data.get(&self.names.security_error) {
            let read_str = |name: &Name| {
                security_error
                    .get(name)
                    .and_then(|e| e.get_str(0))
                    .map(str::to_string)
                    .unwrap_or_default()
            };
            let error = SecurityErrorMeta {
                category: read_str(&self.names.category),
                code: security_error
                    .get(&self.names.code)
                    .and_then(|e| e.get_i32(0))
                    .unwrap_or_default(),
                subcategory: read_str(&self.names.subcategory),
                message: read_str(&self.names.message),
            };
            xbbg_log::warn!(
                ticker = ticker,
                category = error.category.as_str(),
                code = error.code,
                message = error.message.as_str(),
                "HistoricalData securityError; skipping security"
            );
            self.failed_securities.push(ticker.to_string());
            self.response_meta.record_security_error(ticker, error);
            return;
        }

        // Collect per-field exceptions (invalid fields, entitlement misses).
        if let Some(field_exceptions) = security_data.get(&self.names.field_exceptions) {
            for exc in field_exceptions.values() {
                let field = exc
                    .get(&self.names.field_id)
                    .and_then(|e| e.get_str(0))
                    .unwrap_or("?");
                let err_info = exc.get(&self.names.error_info);
                let read_err = |name: &Name| {
                    err_info
                        .as_ref()
                        .and_then(|e| e.get(name))
                        .and_then(|e| e.get_str(0))
                        .map(str::to_string)
                        .unwrap_or_default()
                };
                self.response_meta.record_field_exception(
                    ticker,
                    FieldExceptionMeta {
                        field: field.to_string(),
                        category: read_err(&self.names.category),
                        code: err_info
                            .as_ref()
                            .and_then(|e| e.get(&self.names.code))
                            .and_then(|e| e.get_i32(0))
                            .unwrap_or_default(),
                        subcategory: read_err(&self.names.subcategory),
                        message: read_err(&self.names.message),
                    },
                );
            }
        }

        // Get fieldData array
        let Some(field_data) = security_data.get(&self.names.field_data) else {
            trace!(ticker = ticker, "No fieldData for security");
            return;
        };
        if self.format == OutputFormat::Long && self.long_mode == LongMode::Typed {
            if let Some(columns) = self.typed_long_columns.as_mut() {
                columns.reserve_if_empty(field_data.len().saturating_mul(self.field_names.len()));
            }
        }

        // Iterate through each row (each date)
        for row in field_data.values() {
            // Get date value for this row
            let date_value = row
                .get(&self.names.date)
                .and_then(|element| get_value_cached_datatype(&element, &mut self.date_datatype));

            match self.format {
                OutputFormat::Long => {
                    self.process_long_format(ticker, &date_value, &row);
                }
                OutputFormat::Wide => {
                    self.process_wide_format(ticker, &date_value, &row);
                }
            }
        }
    }

    /// Process row in long format (one row per field).
    fn process_long_format(
        &mut self,
        ticker: &str,
        date_value: &Option<Value>,
        row: &xbbg_core::Element,
    ) {
        if let Some(long_columns) = self.long_columns.as_mut() {
            for ((field_name, field_lookup_name), field_datatype) in self
                .field_names
                .iter()
                .zip(&self.field_lookup_names)
                .zip(self.field_value_datatypes.iter_mut())
            {
                let value = row
                    .get(field_lookup_name)
                    .and_then(|element| get_value_cached_datatype(&element, field_datatype));
                long_columns.append_histdata_row(ticker, date_value.clone(), field_name, value);
            }
            return;
        }
        if let Some(typed_columns) = self.typed_long_columns.as_mut() {
            for (field_name, field_lookup_name) in
                self.field_names.iter().zip(&self.field_lookup_names)
            {
                let value = row
                    .get(field_lookup_name)
                    .and_then(|element| element.get_value(0));
                typed_columns.append_row(ticker, date_value.as_ref(), field_name, value);
            }
            return;
        }

        let long_mode = self.long_mode;
        let field_names = &self.field_names;
        let field_lookup_names = &self.field_lookup_names;
        let field_types = &self.field_types;
        let columns = &mut self.columns;

        for (field_name, field_lookup_name) in field_names.iter().zip(field_lookup_names) {
            // Get the field value
            let value = row.get(field_lookup_name).and_then(|e| e.get_value(0));
            let dtype = value
                .as_ref()
                .map(|v| dtype_from_hints(field_types, field_name, v));

            append_long_value_row(columns, long_mode, field_name, value, dtype, |columns| {
                columns.append_str("ticker", ticker);
                if let Some(date_value) = date_value {
                    columns.append("date", date_value.clone());
                } else {
                    columns.append_null("date");
                }
            });
        }
    }

    /// Process row in wide format (one row per date with all fields as columns).
    fn process_wide_format(
        &mut self,
        ticker: &str,
        date_value: &Option<Value>,
        row: &xbbg_core::Element,
    ) {
        if let Some(wide_columns) = self.wide_columns.as_mut() {
            wide_columns.append_histdata_row(
                ticker,
                date_value.clone(),
                &self.field_lookup_names,
                &mut self.field_value_datatypes,
                |field_lookup_name, field_datatype| {
                    row.get(field_lookup_name)
                        .and_then(|element| get_value_cached_datatype(&element, field_datatype))
                },
            );
        }
    }
}

fn dtype_from_hints(
    field_types: &HashMap<String, ArrowType>,
    field_name: &str,
    value: &Value<'_>,
) -> &'static str {
    // Use type hint if available
    if let Some(hint) = field_types.get(field_name) {
        return hint.type_name();
    }
    // Otherwise infer from value
    ArrowType::from_value(value).type_name()
}
