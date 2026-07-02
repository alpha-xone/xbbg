//! Bulk data (bds) state with Arrow builders.
//!
//! Extracts BulkDataResponse messages directly from Bloomberg Elements
//! without JSON intermediate serialization.

use arrow_array::RecordBatch;
use std::collections::HashSet;
use tokio::sync::oneshot;
use xbbg_log::trace;

use super::typed_builder::ColumnSet;
use super::value_utils::{
    arrow_type_for_element, should_emit_scalar_field, top_level_response_error, ResponseMetadata,
    SecurityErrorMeta,
};
use xbbg_core::{BlpError, Element, Message};

/// State for a bulk data request (bds).
pub struct BulkDataState {
    /// Field name as string (the bulk field to extract)
    field_name: String,
    /// Column set for building the output.
    columns: ColumnSet,
    /// Discovered scalar sub-field names, in first-seen order across all rows.
    subfield_names: Vec<String>,
    /// Membership set for O(1) duplicate checks while preserving `subfield_names` order.
    subfield_name_set: HashSet<String>,
    /// Response-level diagnostics (eidData / securityError) attached to the
    /// result batch as schema metadata.
    response_meta: ResponseMetadata,
    /// Reply channel
    pub reply: oneshot::Sender<Result<RecordBatch, BlpError>>,
}

impl BulkDataState {
    /// Create a new bulkdata state.
    pub fn new(field: String, reply: oneshot::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            field_name: field,
            columns: ColumnSet::new(),
            subfield_names: Vec::new(),
            subfield_name_set: HashSet::new(),
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
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "ReferenceDataRequest")
        {
            let _ = self.reply.send(Err(error));
            return;
        }

        self.process_message(msg);
        let reply = self.reply;
        let response_meta = std::mem::take(&mut self.response_meta);
        // Include "field" column to identify which bulk field was queried
        let mut order = vec!["ticker", "field"];
        order.extend(self.subfield_names.iter().map(|s| s.as_str()));
        let result = self
            .columns
            .finish_with_order(&order)
            .map(|batch| response_meta.attach(batch));
        if let Ok(batch) = &result {
            xbbg_log::debug!(
                rows = batch.num_rows(),
                cols = batch.num_columns(),
                "bulkdata finish"
            );
        }
        let _ = reply.send(result);
    }

    /// Process a BulkDataResponse message using Element API.
    ///
    /// Bloomberg structure (for bds - similar to refdata but with array fields):
    /// ```text
    /// ReferenceDataResponse {
    ///   securityData[] {
    ///     security: "AAPL US Equity"
    ///     fieldData {
    ///       DVD_HIST[] {           // <-- bulk field is an array
    ///         Declared Date: "2024-01-15"
    ///         Amount: 0.24
    ///         ...
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) {
        let root = msg.elements();

        // Get securityData array
        let Some(security_data) = root.get_by_str("securityData") else {
            trace!("No securityData in message");
            return;
        };

        // Iterate through each security
        let n = security_data.len();
        for i in 0..n {
            let Some(sec) = security_data.get_element(i) else {
                continue;
            };

            // Get ticker
            let ticker = sec
                .get_by_str("security")
                .and_then(|e| e.get_str(0))
                .unwrap_or("");

            // eidData rides alongside fieldData when returnEids was requested.
            if let Some(eids) = sec.get_by_str("eidData") {
                self.response_meta.record_eid_data(ticker, &eids);
            }

            // Check for security error
            if let Some(security_error) = sec.get_by_str("securityError") {
                let read_str = |name: &str| {
                    security_error
                        .get_by_str(name)
                        .and_then(|e| e.get_str(0))
                        .map(str::to_string)
                        .unwrap_or_default()
                };
                let error = SecurityErrorMeta {
                    category: read_str("category"),
                    code: security_error
                        .get_by_str("code")
                        .and_then(|e| e.get_i32(0))
                        .unwrap_or_default(),
                    subcategory: read_str("subcategory"),
                    message: read_str("message"),
                };
                xbbg_log::warn!(
                    ticker = ticker,
                    category = error.category.as_str(),
                    code = error.code,
                    message = error.message.as_str(),
                    "BulkData securityError; skipping security"
                );
                self.response_meta.record_security_error(ticker, error);
                continue;
            }

            // Get fieldData
            let Some(field_data) = sec.get_by_str("fieldData") else {
                trace!(ticker = ticker, "No fieldData for security");
                continue;
            };

            // Get the bulk field (which is an array)
            let Some(bulk_field) = field_data.get_by_str(&self.field_name) else {
                trace!(ticker = ticker, field = %self.field_name, "Bulk field not found");
                continue;
            };

            // Iterate through the array of rows
            let row_count = bulk_field.len();
            for j in 0..row_count {
                let Some(row) = bulk_field.get_element(j) else {
                    continue;
                };

                self.columns.append_str("ticker", ticker);
                self.columns.append_str("field", &self.field_name);

                self.discover_subfields(&row);

                // Extract sub-field values in the full discovered order. ColumnSet
                // pads missing late columns for earlier rows and appends nulls for
                // fields absent from this row.
                let subfield_names = &self.subfield_names;
                let columns = &mut self.columns;
                for subfield_name in subfield_names {
                    Self::append_subfield(columns, &row, subfield_name);
                }

                self.columns.end_row();
            }
        }
    }

    fn discover_subfields(&mut self, row: &Element<'_>) {
        for child in row.children() {
            if !should_emit_scalar_field(&child) {
                continue;
            }

            let name = child.name().as_str().to_string();
            if !self.subfield_name_set.insert(name.clone()) {
                continue;
            }

            self.columns
                .set_type_hint(&name, arrow_type_for_element(&child));
            self.subfield_names.push(name);
        }
    }

    fn append_subfield(columns: &mut ColumnSet, row: &Element<'_>, subfield_name: &str) {
        if let Some(subfield_elem) = row.get_by_str(subfield_name) {
            if let Some(value) = subfield_elem.get_value(0) {
                columns.append(subfield_name, value);
            } else {
                columns.append_null(subfield_name);
            }
        } else {
            columns.append_null(subfield_name);
        }
    }
}
