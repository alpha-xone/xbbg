//! Shared benchmark helpers for xbbg.
//!
//! Provides reusable session setup, field name interning, and result
//! writing utilities used across all benchmark binaries.

use xbbg_core::{EventType, Name, Session, SessionOptions};

// ---------------------------------------------------------------------------
// Session helpers
// ---------------------------------------------------------------------------

/// Create and start a Bloomberg session, waiting for `SessionStarted`.
///
/// Reads `BLP_HOST` (default `127.0.0.1`) and `BLP_PORT` (default `8194`)
/// from the environment.
pub fn setup_session() -> Session {
    let host = std::env::var("BLP_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("BLP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8194);

    let mut opts = SessionOptions::new().expect("failed to create session options");
    opts.set_server_host(&host).expect("failed to set host");
    opts.set_server_port(port);

    let sess = Session::new(&opts).expect("failed to create session");
    sess.start().expect("failed to start session");

    // Wait for SessionStarted
    loop {
        if let Ok(ev) = sess.next_event(Some(5000)) {
            if ev.event_type() == EventType::SessionStatus {
                break;
            }
        }
    }

    sess
}

/// Open a Bloomberg service and wait for `ServiceStatus`.
pub fn open_service(sess: &Session, uri: &str) {
    sess.open_service(uri)
        .unwrap_or_else(|e| panic!("failed to open service {uri}: {e}"));
    loop {
        if let Ok(ev) = sess.next_event(Some(5000)) {
            if ev.event_type() == EventType::ServiceStatus {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Pre-interned field names
// ---------------------------------------------------------------------------

/// Commonly used Bloomberg field names, pre-interned for benchmarks.
pub struct FieldNames {
    pub securities: Name,
    pub fields: Name,
    pub security_data: Name,
    pub field_data: Name,
    pub security: Name,
    pub px_last: Name,
    pub px_open: Name,
    pub px_high: Name,
    pub px_low: Name,
    pub volume: Name,
    pub cur_mkt_cap: Name,
    pub eqy_weighted_avg_px: Name,
    pub px_bid: Name,
    pub px_ask: Name,
    pub last_trade: Name,
    pub last_price: Name,
    pub bid: Name,
    pub ask: Name,
}

impl FieldNames {
    pub fn new() -> Self {
        Self {
            securities: Name::get_or_intern("securities"),
            fields: Name::get_or_intern("fields"),
            security_data: Name::get_or_intern("securityData"),
            field_data: Name::get_or_intern("fieldData"),
            security: Name::get_or_intern("security"),
            px_last: Name::get_or_intern("PX_LAST"),
            px_open: Name::get_or_intern("PX_OPEN"),
            px_high: Name::get_or_intern("PX_HIGH"),
            px_low: Name::get_or_intern("PX_LOW"),
            volume: Name::get_or_intern("VOLUME"),
            cur_mkt_cap: Name::get_or_intern("CUR_MKT_CAP"),
            eqy_weighted_avg_px: Name::get_or_intern("EQY_WEIGHTED_AVG_PX"),
            px_bid: Name::get_or_intern("PX_BID"),
            px_ask: Name::get_or_intern("PX_ASK"),
            last_trade: Name::get_or_intern("LAST_TRADE"),
            last_price: Name::get_or_intern("LAST_PRICE"),
            bid: Name::get_or_intern("BID"),
            ask: Name::get_or_intern("ASK"),
        }
    }
}

impl Default for FieldNames {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Result writing
// ---------------------------------------------------------------------------

/// Write benchmark results to a JSON file.
///
/// Creates the parent directory if needed.
pub fn write_json(path: &std::path::Path, json: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("failed to create results directory");
    }
    std::fs::write(path, json).expect("failed to write results");
    println!("Results written to: {}", path.display());
}

/// Runtime build-mode metadata stamped into benchmark result files.
#[derive(Clone, Copy, Debug)]
pub struct BuildMode {
    /// Whether `RUSTFLAGS` contains `target-cpu=native`.
    pub target_cpu_native: bool,
    /// Whether this benchmark binary was compiled with debug assertions.
    pub debug_build: bool,
}

/// Read build-mode metadata at runtime.
pub fn build_mode() -> BuildMode {
    let rustflags = std::env::var("RUSTFLAGS").unwrap_or_default();
    BuildMode {
        target_cpu_native: rustflags.contains("target-cpu=native"),
        debug_build: cfg!(debug_assertions),
    }
}


/// Parse iteration count from env var, with default.
pub fn env_iterations(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
