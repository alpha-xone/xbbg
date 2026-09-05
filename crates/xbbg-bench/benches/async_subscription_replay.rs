//! Synthetic subscription replay benchmark for the xbbg-async Arrow builder path.
//!
//! This benchmark is fully offline: it does not create a Bloomberg session or
//! traverse live SDK events. It replays synthetic
//! subscription-shaped rows through a minimal local harness that mirrors the
//! `SubscriptionState` timestamp/topic columns, requested-field appends,
//! dynamic late-field growth with null backfill, sparse missing values, mixed
//! field types, and final Arrow `RecordBatch` construction.
//!
//! Run after wiring the bench target:
//!   SUB_REPLAY_ROWS=100000 SUB_REPLAY_FLUSH=1024 SUB_REPLAY_ITERATIONS=5 \
//!     cargo bench --package xbbg-bench --bench async_subscription_replay

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_array::builder::{StringBuilder, TimestampMicrosecondBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use xbbg_async::engine::state::typed_builder::{ArrowType, TypedBuilder};
use xbbg_bench::write_json;
use xbbg_core::Value;

const DEFAULT_ROWS: usize = 100_000;
const DEFAULT_FLUSH: usize = 1_024;
const DEFAULT_ITERATIONS: usize = 10;
const DEFAULT_WARMUP: usize = 2;
const TOPICS: [&str; 8] = [
    "IBM US Equity",
    "MSFT US Equity",
    "AAPL US Equity",
    "NVDA US Equity",
    "ES1 Index",
    "TY1 Comdty",
    "EURUSD Curncy",
    "SPX Index",
];
const REQUESTED_FIELDS: [&str; 8] = [
    "LAST_PRICE",
    "BID",
    "ASK",
    "BID_SIZE",
    "ASK_SIZE",
    "IS_DELAYED_STREAM",
    "TRADE_TIME",
    "CONDITION_CODE",
];
const LATE_FIELDS: [&str; 4] = [
    "RT_PX_CHG_NET_1D",
    "VOLUME",
    "MKTDATA_EVENT_TYPE",
    "MKTDATA_EVENT_SUBTYPE",
];

#[derive(Debug)]
struct BenchConfig {
    rows: usize,
    flush_threshold: usize,
    iterations: usize,
    warmup_iterations: usize,
}

#[derive(Debug)]
struct IterationResult {
    iteration: usize,
    rows: usize,
    batches: usize,
    columns: usize,
    elapsed_us: u128,
    rows_per_sec: f64,
    batches_per_sec: f64,
}

struct ReplayState {
    field_strings: Vec<String>,
    field_indices: HashMap<String, usize>,
    timestamp_builder: TimestampMicrosecondBuilder,
    topic_builder: StringBuilder,
    field_builders: Vec<Option<TypedBuilder>>,
    pending_count: usize,
    flush_threshold: usize,
    cached_schema: Option<Arc<Schema>>,
    batches: usize,
    total_columns: usize,
}

impl ReplayState {
    fn new(fields: &[&str], flush_threshold: usize) -> Self {
        let mut field_strings = Vec::with_capacity(fields.len() + LATE_FIELDS.len());
        let mut field_indices = HashMap::with_capacity(fields.len() + LATE_FIELDS.len());

        for field in fields {
            let idx = field_strings.len();
            field_indices.insert((*field).to_string(), idx);
            field_strings.push((*field).to_string());
        }

        let field_builders = field_strings.iter().map(|_| None).collect();

        Self {
            field_strings,
            field_indices,
            timestamp_builder: TimestampMicrosecondBuilder::new(),
            topic_builder: StringBuilder::new(),
            field_builders,
            pending_count: 0,
            flush_threshold,
            cached_schema: None,
            batches: 0,
            total_columns: 0,
        }
    }

    fn append_row(&mut self, row: usize) {
        let timestamp = 1_700_000_000_000_000_i64 + row as i64 * 250;
        let topic = TOPICS[row % TOPICS.len()];

        self.timestamp_builder.append_value(timestamp);
        self.topic_builder.append_value(topic);

        for field in REQUESTED_FIELDS {
            match synthetic_value(field, row) {
                Some(value) => self.append_value(field, value),
                None => self.append_missing(field),
            }
        }

        // Introduce late all-fields/captured columns only after warm-up rows so
        // the replay exercises dynamic field growth and current-batch null backfill.
        if row >= self.flush_threshold / 2 {
            for field in LATE_FIELDS {
                match synthetic_value(field, row) {
                    Some(value) => self.append_value(field, value),
                    None => self.append_missing(field),
                }
            }
        }

        self.pending_count += 1;
        if self.pending_count >= self.flush_threshold {
            self.finish_pending_batch();
        }
    }

    fn finish(&mut self) {
        self.finish_pending_batch();
    }

    fn append_value(&mut self, field: &str, value: Value<'_>) {
        let idx = self.ensure_field(field);

        if let Some(builder) = self.field_builders[idx].as_mut() {
            builder.append_value(Some(value));
            return;
        }

        if matches!(value, Value::Null) {
            return;
        }

        let mut builder = TypedBuilder::new(ArrowType::from_value(&value));
        for _ in 0..self.pending_count {
            builder.append_null();
        }
        builder.append_value(Some(value));
        self.field_builders[idx] = Some(builder);
        self.cached_schema = None;
    }

    fn append_missing(&mut self, field: &str) {
        let idx = self.ensure_field(field);
        if let Some(builder) = self.field_builders[idx].as_mut() {
            builder.append_null();
        }
    }

    fn ensure_field(&mut self, field: &str) -> usize {
        if let Some(&idx) = self.field_indices.get(field) {
            return idx;
        }

        let idx = self.field_strings.len();
        self.field_strings.push(field.to_string());
        self.field_indices.insert(field.to_string(), idx);
        self.field_builders.push(None);
        self.cached_schema = None;
        idx
    }

    fn finish_pending_batch(&mut self) {
        if self.pending_count == 0 {
            return;
        }

        let row_count = self.pending_count;
        let timestamp_array = self.timestamp_builder.finish().with_timezone("UTC");
        let topic_array = self.topic_builder.finish();
        let field_arrays: Vec<ArrayRef> = self
            .field_builders
            .iter_mut()
            .map(|builder_opt| {
                if let Some(builder) = builder_opt {
                    builder.finish()
                } else {
                    let mut builder = StringBuilder::new();
                    for _ in 0..row_count {
                        builder.append_null();
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
            })
            .collect();

        let schema = self.get_or_build_schema();
        let mut columns: Vec<ArrayRef> = vec![Arc::new(timestamp_array), Arc::new(topic_array)];
        columns.extend(field_arrays);

        let batch =
            RecordBatch::try_new(schema, columns).expect("synthetic replay batch schema mismatch");
        self.total_columns += batch.num_columns();
        self.batches += 1;
        self.pending_count = 0;

        std::hint::black_box(batch);
    }

    fn get_or_build_schema(&mut self) -> Arc<Schema> {
        if let Some(schema) = &self.cached_schema {
            return schema.clone();
        }

        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("topic", DataType::Utf8, false),
        ];

        for (idx, name) in self.field_strings.iter().enumerate() {
            let data_type = self.field_builders[idx]
                .as_ref()
                .map(TypedBuilder::data_type)
                .unwrap_or(DataType::Utf8);
            fields.push(Field::new(name.as_str(), data_type, true));
        }

        let schema = Arc::new(Schema::new(fields));
        self.cached_schema = Some(schema.clone());
        schema
    }
}

fn synthetic_value(field: &str, row: usize) -> Option<Value<'static>> {
    match field {
        "LAST_PRICE" => (!row.is_multiple_of(17))
            .then_some(Value::Float64(100.0 + (row % 10_000) as f64 * 0.01)),
        "BID" => (!row.is_multiple_of(11))
            .then_some(Value::Float64(99.95 + (row % 10_000) as f64 * 0.01)),
        "ASK" => (!row.is_multiple_of(13))
            .then_some(Value::Float64(100.05 + (row % 10_000) as f64 * 0.01)),
        "BID_SIZE" => (!row.is_multiple_of(5)).then_some(Value::Int32((row % 1_000) as i32 + 1)),
        "ASK_SIZE" => (!row.is_multiple_of(7)).then_some(Value::Int64((row % 2_000) as i64 + 1)),
        "IS_DELAYED_STREAM" => Some(Value::Bool(row.is_multiple_of(19))),
        "TRADE_TIME" => (!row.is_multiple_of(23)).then_some(Value::TimestampMicros(
            1_700_000_000_000_000_i64 + row as i64 * 250,
        )),
        "CONDITION_CODE" => match row % 9 {
            0 => None,
            1 => Some(Value::String("OPEN")),
            2 => Some(Value::String("CLOSE")),
            _ => Some(Value::String("REGULAR")),
        },
        "RT_PX_CHG_NET_1D" => {
            (!row.is_multiple_of(3)).then_some(Value::Float64((row as f64 % 200.0) - 100.0))
        }
        "VOLUME" => (!row.is_multiple_of(4)).then_some(Value::Int64(1_000_000 + row as i64 * 10)),
        "MKTDATA_EVENT_TYPE" => (!row.is_multiple_of(6)).then_some(Value::String("TRADE")),
        "MKTDATA_EVENT_SUBTYPE" => (!row.is_multiple_of(10)).then_some(Value::String("SUMMARY")),
        _ => None,
    }
}

fn run_iteration(iteration: usize, config: &BenchConfig) -> IterationResult {
    let mut replay = ReplayState::new(&REQUESTED_FIELDS, config.flush_threshold);
    let started = Instant::now();

    for row in 0..config.rows {
        replay.append_row(row);
    }
    replay.finish();

    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let rows_per_sec = config.rows as f64 / elapsed_secs;
    let batches_per_sec = replay.batches as f64 / elapsed_secs;

    IterationResult {
        iteration,
        rows: config.rows,
        batches: replay.batches,
        columns: replay.total_columns,
        elapsed_us: elapsed.as_micros(),
        rows_per_sec,
        batches_per_sec,
    }
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn percentile_elapsed_us(results: &[IterationResult], percentile: f64) -> f64 {
    let mut values: Vec<u128> = results.iter().map(|result| result.elapsed_us).collect();
    values.sort_unstable();
    let idx = (((values.len() - 1) as f64) * percentile / 100.0).round() as usize;
    values[idx] as f64
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_secs()
}

fn write_results(config: &BenchConfig, timestamp: u64, results: &[IterationResult]) {
    let best_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .fold(0.0, f64::max);
    let avg_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .sum::<f64>()
        / results.len() as f64;
    let mean_elapsed_us =
        results.iter().map(|result| result.elapsed_us).sum::<u128>() as f64 / results.len() as f64;
    let min_elapsed_us = results
        .iter()
        .map(|result| result.elapsed_us)
        .min()
        .unwrap_or_default() as f64;
    let p50_elapsed_us = percentile_elapsed_us(results, 50.0);
    let input_descriptor = format!(
        "rows={};flush_threshold={};iterations={};warmup={};requested_fields={};late_fields={}",
        config.rows,
        config.flush_threshold,
        config.iterations,
        config.warmup_iterations,
        REQUESTED_FIELDS.len(),
        LATE_FIELDS.len(),
    );
    let provenance = xbbg_bench::benchmark_provenance_json(&input_descriptor);

    let iterations_json = results
        .iter()
        .map(|result| {
            format!(
                r#"    {{
      "iteration": {},
      "rows": {},
      "batches": {},
      "columns_finalized": {},
      "elapsed_us": {},
      "rows_per_sec": {:.2},
      "batches_per_sec": {:.2}
    }}"#,
                result.iteration,
                result.rows,
                result.batches,
                result.columns,
                result.elapsed_us,
                result.rows_per_sec,
                result.batches_per_sec
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let json = format!(
        r#"{{
  "schema_version": 2,
  "timestamp": {},
  "crate": "xbbg-async",
  "benchmark_type": "synthetic_subscription_replay",
  "offline": true,
  "uses_bloomberg_session": false,
  "coverage": "synthetic Arrow builder state only; no bounded transport queue, Bloomberg SDK event, or network coverage",
  "timing_scope": "warm uninstrumented row append and RecordBatch finalization",
  "percentile_policy": "p50 reported; p95 requires at least 20 samples and p99 at least 100",
  "provenance": {},
  "config": {{
    "rows": {},
    "flush_threshold": {},
    "iterations": {},
    "warmup_iterations": {},
    "requested_fields": {},
    "late_fields": {}
  }},
  "summary": {{
    "sample_count": {},
    "mean_elapsed_us": {:.2},
    "min_elapsed_us": {:.2},
    "p50_elapsed_us": {:.2},
    "avg_rows_per_sec": {:.2},
    "best_rows_per_sec": {:.2}
  }},
  "iterations": [
{}
  ]
}}"#,
        timestamp,
        provenance,
        config.rows,
        config.flush_threshold,
        config.iterations,
        config.warmup_iterations,
        REQUESTED_FIELDS.len(),
        LATE_FIELDS.len(),
        results.len(),
        mean_elapsed_us,
        min_elapsed_us,
        p50_elapsed_us,
        avg_rows_per_sec,
        best_rows_per_sec,
        iterations_json
    );

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benchmarks/results");
    let timestamped = dir.join(format!("async_subscription_replay_{timestamp}.json"));
    let latest = dir.join("async_subscription_replay_latest.json");
    write_json(&timestamped, &json);
    write_json(&latest, &json);
}

fn print_results(config: &BenchConfig, results: &[IterationResult]) {
    println!("\n{:=<88}", "");
    println!("  xbbg-async Synthetic Subscription Replay Benchmark");
    println!("{:=<88}\n", "");
    println!(
        "  rows={} flush={} warmup={} iterations={} requested_fields={} late_fields={}",
        config.rows,
        config.flush_threshold,
        config.warmup_iterations,
        config.iterations,
        REQUESTED_FIELDS.len(),
        LATE_FIELDS.len()
    );
    println!(
        "  {:>9} {:>12} {:>9} {:>12} {:>14} {:>14}",
        "Iteration", "Rows", "Batches", "Columns", "Rows/sec", "Elapsed (us)"
    );
    println!("  {:-<84}", "");

    for result in results {
        println!(
            "  {:>9} {:>12} {:>9} {:>12} {:>14.0} {:>14}",
            result.iteration,
            result.rows,
            result.batches,
            result.columns,
            result.rows_per_sec,
            result.elapsed_us
        );
    }

    let avg_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .sum::<f64>()
        / results.len() as f64;
    let min_elapsed_us = results
        .iter()
        .map(|result| result.elapsed_us)
        .min()
        .unwrap_or_default();
    let p50_elapsed_us = percentile_elapsed_us(results, 50.0);
    println!("\n  Average rows/sec: {:.0}", avg_rows_per_sec);
    println!(
        "  Elapsed us min/mean/p50: {}/{:.2}/{:.2}",
        min_elapsed_us,
        results.iter().map(|result| result.elapsed_us).sum::<u128>() as f64 / results.len() as f64,
        p50_elapsed_us
    );
    println!("{:=<88}\n", "");
}

fn main() {
    let config = BenchConfig {
        rows: parse_env_usize("SUB_REPLAY_ROWS", DEFAULT_ROWS),
        flush_threshold: parse_env_usize("SUB_REPLAY_FLUSH", DEFAULT_FLUSH),
        iterations: parse_env_usize("SUB_REPLAY_ITERATIONS", DEFAULT_ITERATIONS),
        warmup_iterations: parse_env_usize("BENCH_WARMUP", DEFAULT_WARMUP),
    };

    for _ in 0..config.warmup_iterations {
        std::hint::black_box(run_iteration(0, &config));
    }

    let mut results = Vec::with_capacity(config.iterations);
    for iteration in 1..=config.iterations {
        let result = run_iteration(iteration, &config);
        std::hint::black_box(&result);
        results.push(result);
    }

    print_results(&config, &results);
    write_results(&config, unix_timestamp(), &results);
}
