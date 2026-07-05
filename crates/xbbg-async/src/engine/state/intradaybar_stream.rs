//! Streaming intraday bar (bdib) state with Arrow builders.
//!
//! Unlike IntradayBarState, this state yields chunks immediately via a channel
//! instead of accumulating all data until the final response.
//!
//! Extracts directly from Bloomberg Elements without JSON intermediate.

use std::sync::Arc;

use arrow_array::builder::{
    Float64Builder, Int32Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use tokio::sync::mpsc;

use super::value_utils::top_level_response_error;
use xbbg_core::{BlpError, Message};

/// Streaming state for an intraday bar request (bdib).
pub struct IntradayBarStreamState {
    /// Ticker for this request
    ticker: String,
    /// Stream channel for sending chunks
    stream: mpsc::Sender<Result<RecordBatch, BlpError>>,
    /// Schema (cached after first build)
    schema: Option<Arc<Schema>>,
}

impl IntradayBarStreamState {
    /// Create a new streaming intraday bar state.
    pub fn new(ticker: String, stream: mpsc::Sender<Result<RecordBatch, BlpError>>) -> Self {
        Self {
            ticker,
            stream,
            schema: None,
        }
    }

    /// Process a PARTIAL_RESPONSE message and yield a chunk.
    pub fn on_partial(&mut self, msg: &Message) {
        if let Some(batch) = self.process_message(msg) {
            let _ = self.stream.try_send(Ok(batch));
        }
    }

    /// Process the final RESPONSE message and close the stream.
    pub fn finish(mut self, msg: &Message) {
        if let Some(error) = top_level_response_error(msg, "//blp/refdata", "IntradayBarRequest") {
            let _ = self.stream.try_send(Err(error));
            return;
        }

        if let Some(batch) = self.process_message(msg) {
            let _ = self.stream.try_send(Ok(batch));
        }
    }

    /// Fail the stream with an error.
    pub fn fail(self, error: BlpError) {
        let _ = self.stream.try_send(Err(error));
    }

    /// Process an IntradayBarResponse message using Element API.
    ///
    /// Bloomberg structure:
    /// ```text
    /// IntradayBarResponse {
    ///   barData {
    ///     barTickData[] {
    ///       time: 2024-01-15T09:30:00
    ///       open: 150.0
    ///       high: 151.0
    ///       low: 149.5
    ///       close: 150.5
    ///       volume: 1000000
    ///       numEvents: 500
    ///       value: 150500000.0
    ///     }
    ///   }
    /// }
    /// ```
    fn process_message(&mut self, msg: &Message) -> Option<RecordBatch> {
        let root = msg.elements();

        // Get barData
        let bar_data = root.get_by_str("barData")?;

        // Get barTickData array
        let bar_tick_data = bar_data.get_by_str("barTickData")?;
        let n = bar_tick_data.len();
        if n == 0 {
            return None;
        }

        // Create builders
        let mut ticker_builder =
            StringBuilder::with_capacity(n, self.ticker.len().saturating_mul(n));
        let mut time_builder = TimestampMicrosecondBuilder::with_capacity(n);
        let mut open_builder = Float64Builder::with_capacity(n);
        let mut high_builder = Float64Builder::with_capacity(n);
        let mut low_builder = Float64Builder::with_capacity(n);
        let mut close_builder = Float64Builder::with_capacity(n);
        let mut volume_builder = Float64Builder::with_capacity(n);
        let mut num_events_builder = Int32Builder::with_capacity(n);
        let mut value_builder = Float64Builder::with_capacity(n);

        for i in 0..n {
            let Some(bar) = bar_tick_data.get_element(i) else {
                continue;
            };

            ticker_builder.append_value(&self.ticker);

            // Get time - native datetime extraction
            if let Some(time_elem) = bar.get_by_str("time") {
                if let Some(micros) = time_elem.get_timestamp_us(0) {
                    time_builder.append_value(micros);
                } else {
                    time_builder.append_null();
                }
            } else {
                time_builder.append_null();
            }

            // OHLC + Volume
            append_f64_field(&bar, "open", &mut open_builder);
            append_f64_field(&bar, "high", &mut high_builder);
            append_f64_field(&bar, "low", &mut low_builder);
            append_f64_field(&bar, "close", &mut close_builder);
            append_f64_field(&bar, "volume", &mut volume_builder);

            // numEvents
            if let Some(elem) = bar.get_by_str("numEvents") {
                if let Some(n) = elem.get_i32(0) {
                    num_events_builder.append_value(n);
                } else if let Some(n) = elem.get_i64(0) {
                    num_events_builder.append_value(n as i32);
                } else {
                    num_events_builder.append_null();
                }
            } else {
                num_events_builder.append_null();
            }

            append_f64_field(&bar, "value", &mut value_builder);
        }

        // Build schema if not cached
        let schema = self.schema.get_or_insert_with(|| {
            Arc::new(Schema::new(vec![
                Field::new("ticker", DataType::Utf8, false),
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new("open", DataType::Float64, true),
                Field::new("high", DataType::Float64, true),
                Field::new("low", DataType::Float64, true),
                Field::new("close", DataType::Float64, true),
                Field::new("volume", DataType::Float64, true),
                Field::new("numEvents", DataType::Int32, true),
                Field::new("value", DataType::Float64, true),
            ]))
        });

        let columns: Vec<ArrayRef> = vec![
            Arc::new(ticker_builder.finish()),
            Arc::new(time_builder.finish().with_timezone("UTC")),
            Arc::new(open_builder.finish()),
            Arc::new(high_builder.finish()),
            Arc::new(low_builder.finish()),
            Arc::new(close_builder.finish()),
            Arc::new(volume_builder.finish()),
            Arc::new(num_events_builder.finish()),
            Arc::new(value_builder.finish()),
        ];

        RecordBatch::try_new(schema.clone(), columns).ok()
    }
}

/// Helper to append an f64 field value to a builder.
fn append_f64_field(elem: &xbbg_core::Element<'_>, field: &str, builder: &mut Float64Builder) {
    if let Some(field_elem) = elem.get_by_str(field) {
        if let Some(v) = field_elem.get_f64(0) {
            builder.append_value(v);
        } else {
            builder.append_null();
        }
    } else {
        builder.append_null();
    }
}
