//! Cached real Bloomberg subscription message -> xbbg-async SubscriptionState -> Arrow benchmark.
//!
//! This benchmark makes one bounded live subscription capture, keeps the returned
//! Bloomberg `Event`s in memory, then replays those cached events many times
//! through the real `SubscriptionState::on_message` path. It bridges the existing
//! pure `xbbg-core` parsing and synthetic Arrow replay benchmarks without
//! hammering Bloomberg data limits.
//!
//! It does not change production hot paths.
//!
//! Run:
//!   CACHED_SUB_TICKER="XBTUSD Curncy" CACHED_SUB_CAPTURE_MESSAGES=25 \
//!     CACHED_SUB_REPLAY_LOOPS=1000 cargo bench -p xbbg-bench --bench cached_subscription_arrow

use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use tokio::sync::mpsc;
use xbbg_async::engine::state::{subscription_update_to_record_batch, SubscriptionState};
use xbbg_async::engine::OverflowPolicy;
use xbbg_bench::write_json;
use xbbg_core::{CorrelationId, Event, EventType, Session, SessionOptions, SubscriptionList};

const DEFAULT_TICKER: &str = "XBTUSD Curncy";
const DEFAULT_FIELDS: &str = "LAST_PRICE,BID,ASK";
const DEFAULT_CAPTURE_MESSAGES: usize = 25;
const DEFAULT_CAPTURE_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_REPLAY_LOOPS: usize = 1_000;
const DEFAULT_FLUSH_THRESHOLD: usize = 1_024;
const DEFAULT_CHANNEL_CAPACITY: usize = 16_384;
const DEFAULT_ITERATIONS: usize = 5;
const LIVE_ENABLE_ENV: &str = "CACHED_SUB_ENABLE_LIVE";

#[derive(Debug)]
struct BenchConfig {
    ticker: String,
    fields: Vec<String>,
    capture_messages: usize,
    capture_timeout_ms: u64,
    replay_loops: usize,
    flush_threshold: usize,
    channel_capacity: usize,
    iterations: usize,
    capture_all_fields: bool,
}

struct CaptureResult {
    events: Vec<Event>,
    messages: usize,
    capture_elapsed_ms: u128,
}

#[derive(Debug)]
struct ReplayResult {
    iteration: usize,
    input_events: usize,
    input_messages: usize,
    replay_loops: usize,
    rows_emitted: usize,
    batches_emitted: usize,
    total_columns_emitted: usize,
    elapsed_us: u128,
    messages_per_sec: f64,
    rows_per_sec: f64,
    cells_per_sec: f64,
    avg_columns_per_batch: f64,
    ns_per_message: f64,
    effective_channel_capacity: usize,
    dropped_batches: u64,
}

fn main() {
    let config = BenchConfig::from_env();

    println!("xbbg cached subscription -> Arrow benchmark");
    println!("============================================\n");
    println!(
        "capture ticker={} fields={:?} target_messages={} timeout_ms={} all_fields={}",
        config.ticker,
        config.fields,
        config.capture_messages,
        config.capture_timeout_ms,
        config.capture_all_fields
    );
    println!(
        "replay loops={} flush={} iterations={} channel_capacity={}\n",
        config.replay_loops, config.flush_threshold, config.iterations, config.channel_capacity
    );

    if !live_capture_enabled() {
        println!(
            "Skipping cached subscription benchmark: set {LIVE_ENABLE_ENV}=1 to enable the live Bloomberg capture."
        );
        return;
    }

    let session = match setup_subscription_session() {
        Ok(session) => session,
        Err(err) => {
            println!(
                "Skipping cached subscription benchmark: Bloomberg session unavailable ({err})."
            );
            return;
        }
    };
    let capture = match capture_subscription_events(&session, &config) {
        Ok(capture) => capture,
        Err(err) => {
            session.stop();
            println!("Skipping cached subscription benchmark: capture unavailable ({err}).");
            return;
        }
    };
    session.stop();

    if capture.messages == 0 {
        println!(
            "Skipping cached subscription benchmark: captured zero subscription messages for {}; ensure Bloomberg is running and ticker is active.",
            config.ticker
        );
        return;
    }

    println!(
        "captured {} events / {} messages in {}ms\n",
        capture.events.len(),
        capture.messages,
        capture.capture_elapsed_ms
    );

    let mut results = Vec::with_capacity(config.iterations);
    for iteration in 1..=config.iterations {
        let result = replay_cached_events(iteration, &config, &capture.events);
        std::hint::black_box(&result);
        results.push(result);
    }

    print_results(&results);
    write_results(&config, &capture, &results);
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            ticker: std::env::var("CACHED_SUB_TICKER")
                .unwrap_or_else(|_| DEFAULT_TICKER.to_string()),
            fields: parse_fields(
                &std::env::var("CACHED_SUB_FIELDS").unwrap_or_else(|_| DEFAULT_FIELDS.to_string()),
            ),
            capture_messages: env_usize("CACHED_SUB_CAPTURE_MESSAGES", DEFAULT_CAPTURE_MESSAGES),
            capture_timeout_ms: env_u64(
                "CACHED_SUB_CAPTURE_TIMEOUT_MS",
                DEFAULT_CAPTURE_TIMEOUT_MS,
            ),
            replay_loops: env_usize("CACHED_SUB_REPLAY_LOOPS", DEFAULT_REPLAY_LOOPS),
            flush_threshold: env_usize("CACHED_SUB_FLUSH", DEFAULT_FLUSH_THRESHOLD),
            channel_capacity: env_usize("CACHED_SUB_CHANNEL_CAPACITY", DEFAULT_CHANNEL_CAPACITY),
            iterations: env_usize("CACHED_SUB_ITERATIONS", DEFAULT_ITERATIONS),
            capture_all_fields: env_bool("CACHED_SUB_ALL_FIELDS", false),
        }
    }
}

fn parse_fields(raw: &str) -> Vec<String> {
    let fields: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    if fields.is_empty() {
        DEFAULT_FIELDS.split(',').map(ToOwned::to_owned).collect()
    } else {
        fields
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}
fn live_capture_enabled() -> bool {
    env_bool(LIVE_ENABLE_ENV, false)
        || env_bool("XBBG_BENCH_LIVE", false)
        || env_bool("XBBG_LIVE_BENCHMARKS", false)
}

fn setup_subscription_session() -> Result<Session, String> {
    let host = std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("BLP_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8194);

    let mut options = SessionOptions::new()
        .map_err(|err| format!("failed to create session options: {err:?}"))?;
    options
        .set_server_host(&host)
        .map_err(|err| format!("failed to set host: {err:?}"))?;
    options.set_server_port(port);
    options.set_record_subscription_receive_times(true);

    let session =
        Session::new(&options).map_err(|err| format!("failed to create session: {err:?}"))?;
    session
        .start_and_wait(30_000)
        .map_err(|err| format!("failed to start subscription benchmark session: {err:?}"))?;
    Ok(session)
}

fn capture_subscription_events(
    session: &xbbg_core::Session,
    config: &BenchConfig,
) -> Result<CaptureResult, String> {
    let mut subscription_list = SubscriptionList::new();
    let cid = CorrelationId::new_int(1);
    let field_refs: Vec<&str> = config.fields.iter().map(String::as_str).collect();
    subscription_list
        .add(&config.ticker, &field_refs, "", &cid)
        .map_err(|err| format!("failed to add subscription: {err:?}"))?;

    session
        .subscribe(&subscription_list, None)
        .map_err(|err| format!("failed to subscribe: {err:?}"))?;

    let started = Instant::now();
    let deadline = started + Duration::from_millis(config.capture_timeout_ms);
    let mut events = Vec::new();
    let mut messages = 0usize;

    while messages < config.capture_messages {
        let timeout_ms = deadline
            .saturating_duration_since(Instant::now())
            .as_millis() as u32;
        if timeout_ms == 0 {
            break;
        }

        let Ok(event) = session.next_event(Some(timeout_ms)) else {
            continue;
        };

        if event.event_type() == EventType::SubscriptionData {
            let event_messages = event.messages().count();
            if event_messages > 0 {
                messages += event_messages;
                events.push(event);
            }
        }
    }

    session
        .unsubscribe(&subscription_list)
        .map_err(|err| format!("failed to unsubscribe after capture: {err:?}"))?;

    Ok(CaptureResult {
        events,
        messages,
        capture_elapsed_ms: started.elapsed().as_millis(),
    })
}

fn replay_cached_events(iteration: usize, config: &BenchConfig, events: &[Event]) -> ReplayResult {
    let cached_messages = count_cached_messages(events);
    let expected_batches = expected_batches(
        cached_messages.saturating_mul(config.replay_loops),
        config.flush_threshold,
    );
    let effective_channel_capacity = config
        .channel_capacity
        .max(expected_batches.saturating_add(1));
    let (tx, mut rx) = mpsc::channel(effective_channel_capacity);
    let mut state = SubscriptionState::with_policy(
        config.ticker.clone(),
        config.fields.clone(),
        tx,
        config.flush_threshold,
        OverflowPolicy::DropNewest,
        config.capture_all_fields,
    );

    let started = Instant::now();
    let mut input_messages = 0usize;

    for _ in 0..config.replay_loops {
        for event in events {
            for message in event.messages() {
                state.on_message(&message);
                input_messages += 1;
            }
        }
    }

    state.flush();
    let dropped_batches = state.dropped_batches;
    drop(state);

    let mut rows_emitted = 0usize;
    let mut batches_emitted = 0usize;
    let mut total_columns_emitted = 0usize;
    let mut total_cells_emitted = 0usize;
    while let Ok(result) = rx.try_recv() {
        let update = result.expect("SubscriptionState should not emit errors in replay");
        let batch: RecordBatch = subscription_update_to_record_batch(&update)
            .expect("SubscriptionUpdate should adapt to RecordBatch in replay");
        rows_emitted += batch.num_rows();
        batches_emitted += 1;
        total_columns_emitted += batch.num_columns();
        total_cells_emitted += batch.num_rows().saturating_mul(batch.num_columns());
        std::hint::black_box(batch);
    }

    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    if dropped_batches != 0 {
        panic!("cached subscription benchmark dropped {dropped_batches} batches; increase CACHED_SUB_CHANNEL_CAPACITY or flush less often");
    }

    ReplayResult {
        iteration,
        input_events: events.len(),
        input_messages,
        replay_loops: config.replay_loops,
        rows_emitted,
        batches_emitted,
        total_columns_emitted,
        elapsed_us: elapsed.as_micros(),
        messages_per_sec: input_messages as f64 / elapsed_secs,
        rows_per_sec: rows_emitted as f64 / elapsed_secs,
        cells_per_sec: total_cells_emitted as f64 / elapsed_secs,
        avg_columns_per_batch: average_columns(total_columns_emitted, batches_emitted),
        ns_per_message: elapsed.as_nanos() as f64 / input_messages.max(1) as f64,
        effective_channel_capacity,
        dropped_batches,
    }
}

fn count_cached_messages(events: &[Event]) -> usize {
    events.iter().map(|event| event.messages().count()).sum()
}

fn expected_batches(messages: usize, flush_threshold: usize) -> usize {
    messages.saturating_add(flush_threshold.saturating_sub(1)) / flush_threshold
}

fn average_columns(total_columns: usize, batches: usize) -> f64 {
    if batches == 0 {
        0.0
    } else {
        total_columns as f64 / batches as f64
    }
}

fn print_results(results: &[ReplayResult]) {
    println!("{:=<112}", "");
    println!("  cached real subscription Message -> SubscriptionState -> Arrow");
    println!("{:=<112}\n", "");
    println!(
        "  {:>9} {:>12} {:>12} {:>9} {:>9} {:>10} {:>14} {:>12}",
        "Iteration", "Messages", "Rows", "Batches", "Cols/B", "Elapsed", "Rows/sec", "ns/msg"
    );
    println!("  {:-<108}", "");

    for result in results {
        println!(
            "  {:>9} {:>12} {:>12} {:>9} {:>9.2} {:>9}us {:>14.0} {:>12.1}",
            result.iteration,
            result.input_messages,
            result.rows_emitted,
            result.batches_emitted,
            result.avg_columns_per_batch,
            result.elapsed_us,
            result.rows_per_sec,
            result.ns_per_message,
        );
    }

    let avg_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .sum::<f64>()
        / results.len() as f64;
    let avg_ns_per_message = results
        .iter()
        .map(|result| result.ns_per_message)
        .sum::<f64>()
        / results.len() as f64;
    println!("\n  Average rows/sec: {:.0}", avg_rows_per_sec);
    println!("  Average ns/message: {:.1}", avg_ns_per_message);
    let effective_channel_capacity = results
        .first()
        .map(|result| result.effective_channel_capacity)
        .unwrap_or_default();
    let dropped_batches: u64 = results.iter().map(|result| result.dropped_batches).sum();
    println!("  Effective channel capacity: {effective_channel_capacity}");
    println!("  Dropped batches: {dropped_batches}");
    println!("{:=<112}\n", "");
}

fn write_results(config: &BenchConfig, capture: &CaptureResult, results: &[ReplayResult]) {
    let timestamp = unix_timestamp();
    let avg_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .sum::<f64>()
        / results.len() as f64;
    let best_rows_per_sec = results
        .iter()
        .map(|result| result.rows_per_sec)
        .fold(0.0, f64::max);
    let avg_ns_per_message = results
        .iter()
        .map(|result| result.ns_per_message)
        .sum::<f64>()
        / results.len() as f64;

    let mut json = String::new();
    writeln!(&mut json, "{{").unwrap();
    writeln!(&mut json, "  \"timestamp\": {timestamp},").unwrap();
    writeln!(&mut json, "  \"crate\": \"xbbg-async\",").unwrap();
    writeln!(
        &mut json,
        "  \"benchmark_type\": \"cached_subscription_arrow\","
    )
    .unwrap();
    writeln!(&mut json, "  \"uses_bloomberg_session\": true,").unwrap();
    let build_mode = xbbg_bench::build_mode();
    writeln!(
        &mut json,
        "  \"target_cpu\": {{ \"native\": {} }},",
        build_mode.target_cpu_native
    )
    .unwrap();
    writeln!(&mut json, "  \"debug_build\": {},", build_mode.debug_build).unwrap();
    writeln!(&mut json, "  \"config\": {{").unwrap();
    writeln!(
        &mut json,
        "    \"ticker\": \"{}\",",
        json_escape(&config.ticker)
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"fields\": [{}],",
        json_string_array(&config.fields)
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"capture_messages\": {},",
        config.capture_messages
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"capture_timeout_ms\": {},",
        config.capture_timeout_ms
    )
    .unwrap();
    writeln!(&mut json, "    \"replay_loops\": {},", config.replay_loops).unwrap();
    writeln!(
        &mut json,
        "    \"flush_threshold\": {},",
        config.flush_threshold
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"channel_capacity\": {},",
        config.channel_capacity
    )
    .unwrap();
    writeln!(&mut json, "    \"iterations\": {},", config.iterations).unwrap();
    writeln!(
        &mut json,
        "    \"capture_all_fields\": {}",
        config.capture_all_fields
    )
    .unwrap();
    writeln!(&mut json, "  }},").unwrap();
    writeln!(&mut json, "  \"capture\": {{").unwrap();
    writeln!(&mut json, "    \"events\": {},", capture.events.len()).unwrap();
    writeln!(&mut json, "    \"messages\": {},", capture.messages).unwrap();
    writeln!(
        &mut json,
        "    \"elapsed_ms\": {}",
        capture.capture_elapsed_ms
    )
    .unwrap();
    writeln!(&mut json, "  }},").unwrap();
    writeln!(&mut json, "  \"summary\": {{").unwrap();
    writeln!(
        &mut json,
        "    \"avg_rows_per_sec\": {:.2},",
        avg_rows_per_sec
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"best_rows_per_sec\": {:.2},",
        best_rows_per_sec
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"avg_ns_per_message\": {:.2},",
        avg_ns_per_message
    )
    .unwrap();
    let max_effective_channel_capacity = results
        .iter()
        .map(|result| result.effective_channel_capacity)
        .max()
        .unwrap_or_default();
    let total_dropped_batches: u64 = results.iter().map(|result| result.dropped_batches).sum();
    writeln!(
        &mut json,
        "    \"effective_channel_capacity\": {max_effective_channel_capacity},"
    )
    .unwrap();
    writeln!(
        &mut json,
        "    \"dropped_batches\": {total_dropped_batches}"
    )
    .unwrap();
    writeln!(&mut json, "  }},").unwrap();
    writeln!(&mut json, "  \"iterations\": [").unwrap();

    for (idx, result) in results.iter().enumerate() {
        let comma = if idx + 1 == results.len() { "" } else { "," };
        writeln!(&mut json, "    {{").unwrap();
        writeln!(&mut json, "      \"iteration\": {},", result.iteration).unwrap();
        writeln!(
            &mut json,
            "      \"input_events\": {},",
            result.input_events
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"input_messages\": {},",
            result.input_messages
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"replay_loops\": {},",
            result.replay_loops
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"rows_emitted\": {},",
            result.rows_emitted
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"batches_emitted\": {},",
            result.batches_emitted
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"total_columns_emitted\": {},",
            result.total_columns_emitted
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"avg_columns_per_batch\": {:.3},",
            result.avg_columns_per_batch
        )
        .unwrap();
        writeln!(&mut json, "      \"elapsed_us\": {},", result.elapsed_us).unwrap();
        writeln!(
            &mut json,
            "      \"messages_per_sec\": {:.2},",
            result.messages_per_sec
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"rows_per_sec\": {:.2},",
            result.rows_per_sec
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"cells_per_sec\": {:.2},",
            result.cells_per_sec
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"ns_per_message\": {:.2},",
            result.ns_per_message
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"effective_channel_capacity\": {},",
            result.effective_channel_capacity
        )
        .unwrap();
        writeln!(
            &mut json,
            "      \"dropped_batches\": {}",
            result.dropped_batches
        )
        .unwrap();
        writeln!(&mut json, "    }}{comma}").unwrap();
    }

    writeln!(&mut json, "  ]").unwrap();
    writeln!(&mut json, "}}").unwrap();

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benchmarks/results");
    let timestamped = dir.join(format!("cached_subscription_arrow_{timestamp}.json"));
    let latest = dir.join("cached_subscription_arrow_latest.json");
    write_json(&timestamped, &json);
    write_json(&latest, &json);
}

fn json_string_array(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            ch if ch.is_control() => write!(&mut escaped, "\\u{:04x}", ch as u32).unwrap(),
            ch => escaped.push(ch),
        }
    }
    escaped
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_secs()
}
