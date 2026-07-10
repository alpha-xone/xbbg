//! Streaming historical data (bdh) state with Arrow builders.
//!
//! Unlike HistDataState, this state yields chunks immediately via a channel
//! instead of accumulating all data until the final response.
//!
//! Extracts directly from Bloomberg Elements without JSON intermediate.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Date32Builder, Float64Builder, StringBuilder};
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use tokio::sync::mpsc;
use xbbg_log::trace;

use super::value_utils::{top_level_response_error, ResponseMetadata};
use xbbg_core::{BlpError, Message};

/// Streaming state for a historical data request (bdh).
pub struct HistDataStreamState {
    /// Field names as strings
    field_names: Vec<String>,
    /// Stream channel for sending chunks
    stream: mpsc::Sender<Result<RecordBatch, BlpError>>,
    /// Schema (cached after first build)
    schema: Option<Arc<Schema>>,
}

impl HistDataStreamState {
    /// Create a new streaming histdata state.
    pub fn new(fields: Vec<String>, stream: mpsc::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self::with_types(fields, None, stream)
    }

    /// Create a new streaming histdata state with optional field type overrides.
    pub fn with_types(
        fields: Vec<String>,
        _field_types: Option<HashMap<String, String>>,
        stream: mpsc::Sender<Result<RecordBatch, BlpError>>,
    ) -> Self {
        // Note: field_types is currently unused - streaming uses dynamic schema
        Self {
            field_names: fields,
            stream,
            schema: None,
        }
    }

    /// Process a PARTIAL_RESPONSE message and yield a chunk.
    pub fn on_partial(&mut self, msg: &Message) {
        if let Some(batch) = self.process_message(msg) {
            // Non-blocking send - if channel is full, drop the batch
            let _ = self.stream.try_send(Ok(batch));
        }
    }

    /// Process the final RESPONSE message and close the stream.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "HistoricalDataRequest")
        {
            let _ = self.stream.try_send(Err(error));
            return;
        }

        if let Some(batch) = self.process_message(msg) {
            let _ = self.stream.try_send(Ok(batch));
        }
        // Stream closes automatically when self is dropped
    }

    /// Fail the stream with an error.
    pub fn fail(self, error: BlpError) {
        let _ = self.stream.try_send(Err(error));
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
    ///     }
    ///   }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) -> Option<RecordBatch> {
        let root = msg.elements();

        // Get securityData (singular in HistoricalDataResponse)
        let security_data = root.get_by_str("securityData")?;

        // Get ticker
        let ticker = security_data
            .get_by_str("security")
            .and_then(|e| e.get_str(0))
            .unwrap_or("");

        let mut response_meta = ResponseMetadata::default();
        let has_eids = if let Some(eids) = security_data.get_by_str("eidData") {
            response_meta.record_eid_data(ticker, &eids);
            true
        } else {
            false
        };

        // A metadata-only response must still surface its entitlement IDs.
        if security_data.get_by_str("securityError").is_some() && !has_eids {
            trace!(ticker = ticker, "Security has error, skipping");
            return None;
        }

        let field_data = security_data.get_by_str("fieldData");
        let n = field_data.as_ref().map_or(0, |data| data.len());
        if n == 0 && !has_eids {
            return None;
        }

        // Create builders for this chunk
        let mut ticker_builder = StringBuilder::with_capacity(n, ticker.len().saturating_mul(n));
        let mut date_builder = Date32Builder::with_capacity(n);
        let mut field_builders: Vec<Float64Builder> = self
            .field_names
            .iter()
            .map(|_| Float64Builder::with_capacity(n))
            .collect();

        for i in 0..n {
            let field_data = field_data
                .as_ref()
                .expect("nonzero length requires fieldData");
            let Some(row) = field_data.get_element(i) else {
                continue;
            };

            ticker_builder.append_value(ticker);

            // Get date
            if let Some(date_elem) = row.get_by_str("date") {
                if let Some(days) = date_elem.get_date32(0) {
                    date_builder.append_value(days);
                } else {
                    date_builder.append_null();
                }
            } else {
                date_builder.append_null();
            }

            // Get each field value
            for (j, field_name) in self.field_names.iter().enumerate() {
                if let Some(field_elem) = row.get_by_str(field_name) {
                    if let Some(value) = field_elem.get_value(0) {
                        match value.as_f64() {
                            Some(f) => field_builders[j].append_value(f),
                            None => field_builders[j].append_null(),
                        }
                    } else {
                        field_builders[j].append_null();
                    }
                } else {
                    field_builders[j].append_null();
                }
            }
        }

        // Build the schema if not cached
        let schema = self.schema.get_or_insert_with(|| {
            let mut fields = vec![
                Field::new("ticker", DataType::Utf8, false),
                Field::new("date", DataType::Date32, true),
            ];
            for name in &self.field_names {
                fields.push(Field::new(name.as_str(), DataType::Float64, true));
            }
            Arc::new(Schema::new(fields))
        });

        // Build columns
        let ticker_array = ticker_builder.finish();
        let date_array = date_builder.finish();
        let field_arrays: Vec<ArrayRef> = field_builders
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as ArrayRef)
            .collect();

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(2 + field_arrays.len());
        columns.push(Arc::new(ticker_array));
        columns.push(Arc::new(date_array));
        columns.extend(field_arrays);

        let batch = RecordBatch::try_new(schema.clone(), columns).ok()?;
        Some(response_meta.attach(batch))
    }
}
