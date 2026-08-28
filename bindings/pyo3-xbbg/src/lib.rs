//! PyO3 bindings for xbbg Bloomberg engine.
//!
//! This module provides Python bindings for the Rust xbbg Engine,
//! exposing a generic `request()` method that accepts parameters from Python.
//!
//! # GIL Handling
//!
//! The async API releases the GIL during Bloomberg SDK operations:
//! - `future_into_py` schedules work on tokio (no GIL held)
//! - GIL is only acquired via `Python::try_attach()` for final Arrow conversion
//! - `py.detach()` releases GIL during blocking `Engine::start()`
//!
//! # Exception Mapping
//!
//! Rust errors are mapped to Python exceptions:
//! - `BlpError::SessionStart` → `BlpSessionError`
//! - `BlpError::OpenService` → `BlpSessionError`
//! - `BlpError::RequestFailure` → `BlpRequestError`
//! - `BlpError::Timeout` → `BlpTimeoutError`
//! - `BlpError::InvalidArgument` → `BlpValidationError`
//! - Other errors → `BlpInternalError`
//!
//! # Logging
//!
//! Rust tracing events are output to stderr via a non-blocking writer.
//! The log level is controlled from Python without any GIL acquisition:
//!
//! ```python
//! import xbbg
//! xbbg.set_log_level("debug")   # sets atomic level, no GIL on log path
//! xbbg.set_log_level("warn")    # default — quiet for end users
//! ```
//!
//! For per-crate control, set `RUST_LOG` before importing xbbg:
//!
//! ```bash
//! RUST_LOG=xbbg_core=trace,xbbg_async=debug python my_script.py
//! ```

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDate, Timelike};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDate, PyDateTime, PyDict, PyTime, PyTzInfo};
use pyo3_async_runtimes::tokio::future_into_py;
#[cfg(feature = "stub-gen")]
use pyo3_stub_gen::{define_stub_info_gatherer, derive::*};
use tokio::sync::{watch, Mutex};
use xbbg_log::{debug, info, warn};

use xbbg_async::engine::state::{
    subscription_update_to_record_batch, SubscriptionMetrics, SubscriptionUpdate, UpdateValue,
};
use xbbg_async::engine::{
    AdminStatusInfo, Engine, EngineConfig, RetryPolicy, ServerAddr, ServiceStatusInfo,
    SessionStatusInfo, Socks5Proxy, SubscriptionCommandHandle, SubscriptionEventInfo,
    SubscriptionFailureInfo, TlsConfig, TopicStatusInfo, Transport,
};
use xbbg_async::{BlpAsyncError, OverflowPolicy, ValidationMode};
use xbbg_core::{AuthConfig, BlpError};
use xbbg_ext::{ExchangeInfo, MarketInfo, MarketTiming};

mod ext;
mod markets;
mod native_arrow;
mod recipes;
mod request;

use request::dict_to_request_params;

type StreamBatchResult = Result<SubscriptionUpdate, BlpError>;
type StreamSender = tokio::sync::mpsc::Sender<StreamBatchResult>;
type StreamReceiver = tokio::sync::mpsc::Receiver<StreamBatchResult>;
type SharedStreamReceiver = Arc<Mutex<Option<StreamReceiver>>>;
type SubscriptionMetricsMap = HashMap<usize, Arc<SubscriptionMetrics>>;
type SubscriptionEventTuple = (i64, String, String, String, Option<String>, Option<String>);

async fn wait_for_subscription_close(close_rx: &mut watch::Receiver<bool>) {
    if *close_rx.borrow() {
        return;
    }

    while close_rx.changed().await.is_ok() {
        if *close_rx.borrow() {
            return;
        }
    }
}

/// Shutdown-safe wrapper for [`future_into_py`].
///
/// Wraps any async future with interpreter-shutdown detection. If the engine
/// signals shutdown before the future completes, or if the interpreter has been
/// finalized by the time the future returns, the future suspends indefinitely
/// instead of delivering a result. This prevents `future_into_py` from calling
/// `Python::attach()` on a dead interpreter. (Issue #270)
///
/// Callers use `Python::attach()` naturally inside their closures — the guard
/// is applied automatically at the boundary.
fn shutdown_safe_future<'py, F>(
    py: Python<'py>,
    engine: &Arc<Engine>,
    fut: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: std::future::Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
{
    let mut shutdown_rx = engine.shutdown_receiver();
    future_into_py(py, async move {
        tokio::select! {
            result = fut => {
                // Future completed. Verify interpreter is still alive before
                // returning — returning triggers future_into_py's internal
                // Python::attach() for result delivery.
                if Python::try_attach(|_| ()).is_some() {
                    result
                } else {
                    std::future::pending().await
                }
            }
            _ = shutdown_rx.changed() => std::future::pending().await,
        }
    })
}

/// Defense-in-depth for `future_into_py` closures that lack an `Arc<Engine>`.
///
/// Attempts `Python::attach` to run `f`. If the interpreter has already been
/// finalized, suspends the future forever instead of panicking — the tokio
/// runtime drops the suspended future during process exit.
///
/// Used by `PySubscription` methods that cannot use [`shutdown_safe_future`]
/// because they don't hold an `Arc<Engine>`.
async fn try_attach_or_suspend<F, T>(f: F) -> PyResult<T>
where
    F: FnOnce(Python<'_>) -> PyResult<T> + Send,
    T: Send,
{
    match Python::try_attach(f) {
        Some(result) => result,
        None => std::future::pending().await,
    }
}

fn subscription_metrics_totals(
    metrics: &SubscriptionMetricsMap,
) -> (u64, u64, u64, bool, u64, u64, u64) {
    let messages_received = metrics
        .values()
        .map(|m| m.messages_received.load(Ordering::Relaxed))
        .sum();
    let dropped_batches = metrics
        .values()
        .map(|m| m.dropped_batches.load(Ordering::Relaxed))
        .sum();
    let batches_sent = metrics
        .values()
        .map(|m| m.batches_sent.load(Ordering::Relaxed))
        .sum();
    let slow_consumer = metrics
        .values()
        .any(|m| m.slow_consumer.load(Ordering::Relaxed));
    let data_loss_events = metrics
        .values()
        .map(|m| m.data_loss_events.load(Ordering::Relaxed))
        .sum();
    let last_message_us = metrics
        .values()
        .map(|m| m.last_message_us.load(Ordering::Relaxed))
        .max()
        .unwrap_or(0);
    let last_data_loss_us = metrics
        .values()
        .map(|m| m.last_data_loss_us.load(Ordering::Relaxed))
        .max()
        .unwrap_or(0);

    (
        messages_received,
        dropped_batches,
        batches_sent,
        slow_consumer,
        data_loss_events,
        last_message_us,
        last_data_loss_us,
    )
}

// =============================================================================
// Python Exception Hierarchy (mirrors py-xbbg/src/xbbg/exceptions.py)
// =============================================================================

pyo3::create_exception!(xbbg._core, BlpErrorBase, pyo3::exceptions::PyException);
pyo3::create_exception!(xbbg._core, BlpSessionError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpRequestError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpLimitError, BlpRequestError);
pyo3::create_exception!(xbbg._core, BlpSecurityError, BlpRequestError);
pyo3::create_exception!(xbbg._core, BlpFieldError, BlpRequestError);
pyo3::create_exception!(xbbg._core, BlpValidationError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpTimeoutError, BlpErrorBase);
pyo3::create_exception!(xbbg._core, BlpInternalError, BlpErrorBase);

/// Convert BlpError to appropriate Python exception.
///
/// Maps each BlpError variant to the corresponding Python exception class,
/// preserving all structured error context (service, operation, cid, etc.).
fn request_failure_is_limit(label: Option<&str>) -> bool {
    label.is_some_and(|label| {
        label.contains("category=LIMIT")
            || label.contains("DAILY_CAPACITY_REACHED")
            || label.contains("subcategory=DAILY_CAPACITY_REACHED")
    })
}

fn blp_error_to_pyerr(e: BlpError) -> PyErr {
    match e {
        BlpError::SessionStart { source, label } => {
            let msg = format_error_msg("Session start failed", label.as_deref(), source.as_deref());
            BlpSessionError::new_err(msg)
        }
        BlpError::OpenService {
            service,
            source,
            label,
        } => {
            let msg = format!(
                "Failed to open service '{}': {}",
                service,
                format_error_msg("", label.as_deref(), source.as_deref())
            );
            BlpSessionError::new_err(msg)
        }
        BlpError::RequestFailure {
            service,
            operation,
            cid,
            label,
            request_id,
            source,
        } => {
            let mut msg = format!("Request failed on {}", service);
            if let Some(op) = &operation {
                msg.push_str(&format!("::{}", op));
            }
            if let Some(c) = &cid {
                msg.push_str(&format!(" (cid={})", c));
            }
            if let Some(rid) = &request_id {
                msg.push_str(&format!(" [request_id={}]", rid));
            }
            if let Some(l) = &label {
                msg.push_str(&format!(" - {}", l));
            }
            if let Some(s) = &source {
                msg.push_str(&format!(": {}", s));
            }
            if request_failure_is_limit(label.as_deref()) {
                BlpLimitError::new_err(msg)
            } else {
                BlpRequestError::new_err(msg)
            }
        }
        BlpError::InvalidArgument { detail } => {
            BlpValidationError::new_err(format!("Invalid argument: {}", detail))
        }
        BlpError::Timeout => BlpTimeoutError::new_err("Request timed out"),
        BlpError::TemplateTerminated { cid } => {
            let msg = match cid {
                Some(c) => format!("Request template terminated (cid={})", c),
                None => "Request template terminated".to_string(),
            };
            BlpRequestError::new_err(msg)
        }
        BlpError::SubscriptionFailure { cid, label } => {
            let mut msg = "Subscription failed".to_string();
            if let Some(c) = &cid {
                msg.push_str(&format!(" (cid={})", c));
            }
            if let Some(l) = &label {
                msg.push_str(&format!(": {}", l));
            }
            BlpRequestError::new_err(msg)
        }
        BlpError::Internal { detail } => {
            BlpInternalError::new_err(format!("Internal error: {}", detail))
        }
        BlpError::SchemaOperationNotFound { service, operation } => {
            BlpValidationError::new_err(format!("Operation not found: {}::{}", service, operation))
        }
        BlpError::SchemaElementNotFound { parent, name } => {
            BlpValidationError::new_err(format!("Schema element not found: {}.{}", parent, name))
        }
        BlpError::SchemaTypeMismatch {
            element,
            expected,
            found,
        } => BlpValidationError::new_err(format!(
            "Schema type mismatch at {}: expected {:?}, found {:?}",
            element, expected, found
        )),
        BlpError::SchemaUnsupported { element, detail } => BlpValidationError::new_err(format!(
            "Unsupported schema construct at {}: {}",
            element, detail
        )),
        BlpError::Validation { message, errors } => {
            // Build detailed error message with suggestions
            let details: Vec<String> = errors
                .iter()
                .map(|e| {
                    if let Some(ref suggestion) = e.suggestion {
                        format!("{} (did you mean '{}'?)", e, suggestion)
                    } else {
                        e.to_string()
                    }
                })
                .collect();
            let msg = if details.is_empty() {
                message
            } else {
                format!("{}: {}", message, details.join("; "))
            };
            BlpValidationError::new_err(msg)
        }
    }
}

/// Convert BlpAsyncError to appropriate Python exception.
fn blp_async_error_to_pyerr(e: BlpAsyncError) -> PyErr {
    match e {
        // Route structured BlpError through the full exception mapper
        BlpAsyncError::Blp(blp_err) => blp_error_to_pyerr(blp_err),
        // Explicit BlpError (not From trait)
        BlpAsyncError::BlpError(blp_err) => blp_error_to_pyerr(blp_err),

        BlpAsyncError::Internal(msg) => BlpInternalError::new_err(msg),

        BlpAsyncError::ConfigError { detail } => {
            BlpValidationError::new_err(format!("Configuration error: {}", detail))
        }
        BlpAsyncError::ChannelClosed => BlpInternalError::new_err("Channel closed unexpectedly"),
        BlpAsyncError::StreamFull => {
            BlpInternalError::new_err("Stream buffer full - consumer too slow")
        }
        BlpAsyncError::Cancelled => BlpRequestError::new_err("Request was cancelled"),
        BlpAsyncError::Timeout => BlpTimeoutError::new_err("Request timed out"),
        BlpAsyncError::SessionLost {
            worker_id,
            in_flight_count,
        } => BlpSessionError::new_err(format!(
            "session lost on worker {} ({} in-flight requests failed)",
            worker_id, in_flight_count,
        )),
        BlpAsyncError::AllWorkersDown { pool_size } => BlpSessionError::new_err(format!(
            "all {} request workers are dead — no healthy worker available",
            pool_size,
        )),
    }
}

/// Helper to format error messages with optional label and source.
fn format_error_msg(
    base: &str,
    label: Option<&str>,
    source: Option<&(dyn std::error::Error + Send + Sync)>,
) -> String {
    let mut msg = base.to_string();
    if let Some(l) = label {
        if !msg.is_empty() {
            msg.push_str(": ");
        }
        msg.push_str(l);
    }
    if let Some(s) = source {
        if !msg.is_empty() {
            msg.push_str(" - ");
        }
        msg.push_str(&s.to_string());
    }
    if msg.is_empty() {
        "Unknown error".to_string()
    } else {
        msg
    }
}

/// Python configuration for the xbbg Engine.
///
/// All settings have sensible defaults - you only need to specify what you want to change.
///
/// The defaults are derived from `EngineConfig::default()` in xbbg-async, so they
/// stay in sync automatically.
#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct PyEngineConfig {
    /// Bloomberg server host (default: "localhost")
    #[pyo3(get, set)]
    pub host: String,
    /// Bloomberg server port (default: 8194)
    #[pyo3(get, set)]
    pub port: u16,
    /// Multiple servers for failover: list of (host, port) tuples. Overrides host/port when set.
    #[pyo3(get, set)]
    pub servers: Vec<(String, u16)>,
    /// ZFP remote: "8194" or "8196". Default: None (direct transport).
    #[pyo3(get, set)]
    pub zfp_remote: Option<String>,
    /// Number of pre-warmed request workers (default: 2)
    #[pyo3(get, set)]
    pub request_pool_size: usize,
    /// Number of pre-warmed subscription sessions (default: 1)
    #[pyo3(get, set)]
    pub subscription_pool_size: usize,
    /// Enable request sharding for eligible multi-security reference/history requests.
    #[pyo3(get, set)]
    pub shard_requests: bool,
    /// Minimum securities before request sharding applies (default: 20).
    #[pyo3(get, set)]
    pub shard_threshold: usize,
    /// Maximum securities per sharded request (default: 16).
    #[pyo3(get, set)]
    pub shard_chunk_size: usize,
    /// Maximum concurrent shard requests per user request (default: 4).
    #[pyo3(get, set)]
    pub shard_max_concurrent: usize,
    /// Validation mode: "disabled" (default), "lenient", or "strict".
    /// Also accepts "off" and "none" as aliases for "disabled".
    #[pyo3(get, set)]
    pub validation_mode: String,
    /// Number of ticks to buffer before flushing to Python (default: 1)
    #[pyo3(get, set)]
    pub subscription_flush_threshold: usize,
    /// Bloomberg SDK event queue size (default: 10000)
    #[pyo3(get, set)]
    pub max_event_queue_size: usize,
    /// Internal command channel capacity (default: 256)
    #[pyo3(get, set)]
    pub command_queue_size: usize,
    /// Subscription stream backpressure capacity (default: 256)
    #[pyo3(get, set)]
    pub subscription_stream_capacity: usize,
    /// Overflow policy for slow consumers: "drop_newest" (default) or "block".
    /// Also accepts "dropnewest" as an alias for "drop_newest".
    #[pyo3(get, set)]
    pub overflow_policy: String,
    /// Services to pre-warm on startup (default: ["//blp/refdata", "//blp/apiflds"])
    #[pyo3(get, set)]
    pub warmup_services: Vec<String>,
    /// Custom path for field cache JSON file (default: ~/.xbbg/field_cache.json)
    /// Set to None to use the default path.
    #[pyo3(get, set)]
    pub field_cache_path: Option<String>,
    /// Optional auth method: "none", "user", "app", "userapp", "dir", "manual", or "token".
    /// Default: None (no auth). Also accepts "directory" as an alias for "dir" and the empty
    /// string as "none".
    #[pyo3(get, set)]
    pub auth_method: Option<String>,
    /// Bloomberg application name for app/userapp/manual auth.
    #[pyo3(get, set)]
    pub app_name: Option<String>,
    /// Active Directory property for dir auth.
    #[pyo3(get, set)]
    pub dir_property: Option<String>,
    /// Manual Bloomberg user id for manual auth.
    #[pyo3(get, set)]
    pub user_id: Option<String>,
    /// Manual Bloomberg ip address for manual auth.
    #[pyo3(get, set)]
    pub ip_address: Option<String>,
    #[pyo3(get, set)]
    pub token: Option<String>,
    #[pyo3(get, set)]
    pub tls_client_credentials: Option<String>,
    #[pyo3(get, set)]
    pub tls_client_credentials_password: Option<String>,
    #[pyo3(get, set)]
    pub tls_trust_material: Option<String>,
    #[pyo3(get, set)]
    pub tls_handshake_timeout_ms: Option<i32>,
    #[pyo3(get, set)]
    pub tls_crl_fetch_timeout_ms: Option<i32>,
    #[pyo3(get, set)]
    pub num_start_attempts: usize,
    /// Whether Bloomberg should auto-restart the session on disconnect (default: True).
    #[pyo3(get, set)]
    pub auto_restart_on_disconnection: bool,
    #[pyo3(get, set)]
    pub retry_max_retries: u32,
    #[pyo3(get, set)]
    pub retry_initial_delay_ms: u64,
    #[pyo3(get, set)]
    pub retry_backoff_factor: f64,
    #[pyo3(get, set)]
    pub retry_max_delay_ms: u64,
    /// Hard per-request timeout in ms; 0 disables. Default: 60_000.
    #[pyo3(get, set)]
    pub request_timeout_ms: u64,
    /// Warn threshold for a subscription's streams staying deactivated, in ms;
    /// 0 disables. Default: 30_000.
    #[pyo3(get, set)]
    pub streams_deactivated_warn_ms: u64,
    /// Enable BLPAPI keep-alive pings. SDK default: True.
    #[pyo3(get, set)]
    pub keep_alive_enabled: bool,
    /// Milliseconds of inactivity before keep-alive ping is sent. None = SDK default (20_000).
    #[pyo3(get, set)]
    pub keep_alive_inactivity_ms: Option<i32>,
    /// Milliseconds to wait for keep-alive response before declaring the connection dead.
    /// None = SDK default (10_000).
    #[pyo3(get, set)]
    pub keep_alive_response_timeout_ms: Option<i32>,
    /// Slow-consumer hi water mark as fraction of max_event_queue_size. None = SDK default (0.75).
    #[pyo3(get, set)]
    pub slow_consumer_hi_water_mark: Option<f32>,
    /// Slow-consumer lo water mark as fraction of max_event_queue_size. None = SDK default (0.5).
    #[pyo3(get, set)]
    pub slow_consumer_lo_water_mark: Option<f32>,
    /// Bloomberg SDK log level: "off" (default), "fatal", "error", "warn", "info", "debug",
    /// or "trace". Also accepts "warning" as an alias for "warn".
    #[pyo3(get, set)]
    pub sdk_log_level: String,
    /// SOCKS5 proxy hostname for Bloomberg connections.
    #[pyo3(get, set)]
    pub socks5_host: Option<String>,
    /// SOCKS5 proxy port (required when socks5_host is set).
    #[pyo3(get, set)]
    pub socks5_port: Option<u16>,
}
#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl PyEngineConfig {
    /// Create a new configuration with defaults.
    ///
    /// All defaults are derived from the Rust EngineConfig to stay in sync.
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let defaults = EngineConfig::default();
        let (default_host, default_port) = default_direct_host_port(&defaults);
        let mut config = Self {
            host: default_host,
            port: default_port,
            servers: Vec::new(),
            zfp_remote: None,
            request_pool_size: defaults.request_pool_size,
            subscription_pool_size: defaults.subscription_pool_size,
            shard_requests: defaults.shard_requests,
            shard_threshold: defaults.shard_threshold,
            shard_chunk_size: defaults.shard_chunk_size,
            shard_max_concurrent: defaults.shard_max_concurrent,
            validation_mode: defaults.validation_mode.to_string(),
            subscription_flush_threshold: defaults.subscription_flush_threshold,
            max_event_queue_size: defaults.max_event_queue_size,
            command_queue_size: defaults.command_queue_size,
            subscription_stream_capacity: defaults.subscription_stream_capacity,
            overflow_policy: defaults.overflow_policy.to_string(),
            warmup_services: defaults.warmup_services,
            field_cache_path: None,
            auth_method: None,
            app_name: None,
            dir_property: None,
            user_id: None,
            ip_address: None,
            token: None,
            tls_client_credentials: None,
            tls_client_credentials_password: None,
            tls_trust_material: None,
            tls_handshake_timeout_ms: None,
            tls_crl_fetch_timeout_ms: None,
            num_start_attempts: defaults.num_start_attempts,
            auto_restart_on_disconnection: defaults.auto_restart_on_disconnection,
            retry_max_retries: 0,
            retry_initial_delay_ms: 1000,
            retry_backoff_factor: 2.0,
            retry_max_delay_ms: 30_000,
            request_timeout_ms: defaults.request_timeout_ms,
            streams_deactivated_warn_ms: defaults.streams_deactivated_warn_ms,
            keep_alive_enabled: defaults.keep_alive_enabled,
            keep_alive_inactivity_ms: defaults.keep_alive_inactivity_ms,
            keep_alive_response_timeout_ms: defaults.keep_alive_response_timeout_ms,
            slow_consumer_hi_water_mark: defaults.slow_consumer_hi_water_mark,
            slow_consumer_lo_water_mark: defaults.slow_consumer_lo_water_mark,
            sdk_log_level: "off".to_string(),
            socks5_host: None,
            socks5_port: None,
        };

        if let Some(kw) = kwargs {
            if let Some(v) = kw.get_item("host")? {
                config.host = v.extract()?;
            }
            if let Some(v) = kw.get_item("port")? {
                config.port = v.extract()?;
            }
            if let Some(v) = kw.get_item("servers")? {
                config.servers = v.extract()?;
            }
            if let Some(v) = kw.get_item("zfp_remote")? {
                config.zfp_remote = v.extract()?;
            }
            if let Some(v) = kw.get_item("request_pool_size")? {
                config.request_pool_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("subscription_pool_size")? {
                config.subscription_pool_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("shard_requests")? {
                config.shard_requests = v.extract()?;
            }
            if let Some(v) = kw.get_item("shard_threshold")? {
                config.shard_threshold = v.extract()?;
            }
            if let Some(v) = kw.get_item("shard_chunk_size")? {
                config.shard_chunk_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("shard_max_concurrent")? {
                config.shard_max_concurrent = v.extract()?;
            }
            if let Some(v) = kw.get_item("validation_mode")? {
                config.validation_mode = v.extract()?;
            }
            if let Some(v) = kw.get_item("subscription_flush_threshold")? {
                config.subscription_flush_threshold = v.extract()?;
            }
            if let Some(v) = kw.get_item("max_event_queue_size")? {
                config.max_event_queue_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("command_queue_size")? {
                config.command_queue_size = v.extract()?;
            }
            if let Some(v) = kw.get_item("subscription_stream_capacity")? {
                config.subscription_stream_capacity = v.extract()?;
            }
            if let Some(v) = kw.get_item("overflow_policy")? {
                config.overflow_policy = v.extract()?;
            }
            if let Some(v) = kw.get_item("warmup_services")? {
                config.warmup_services = v.extract()?;
            }
            if let Some(v) = kw.get_item("field_cache_path")? {
                config.field_cache_path = v.extract()?;
            }
            if let Some(v) = kw.get_item("auth_method")? {
                config.auth_method = v.extract()?;
            }
            if let Some(v) = kw.get_item("app_name")? {
                config.app_name = v.extract()?;
            }
            if let Some(v) = kw.get_item("dir_property")? {
                config.dir_property = v.extract()?;
            }
            if let Some(v) = kw.get_item("user_id")? {
                config.user_id = v.extract()?;
            }
            if let Some(v) = kw.get_item("ip_address")? {
                config.ip_address = v.extract()?;
            }
            if let Some(v) = kw.get_item("token")? {
                config.token = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_client_credentials")? {
                config.tls_client_credentials = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_client_credentials_password")? {
                config.tls_client_credentials_password = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_trust_material")? {
                config.tls_trust_material = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_handshake_timeout_ms")? {
                config.tls_handshake_timeout_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("tls_crl_fetch_timeout_ms")? {
                config.tls_crl_fetch_timeout_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("num_start_attempts")? {
                config.num_start_attempts = v.extract()?;
            }
            if let Some(v) = kw.get_item("auto_restart_on_disconnection")? {
                config.auto_restart_on_disconnection = v.extract()?;
            }
            if let Some(v) = kw.get_item("retry_max_retries")? {
                config.retry_max_retries = v.extract()?;
            }
            if let Some(v) = kw.get_item("retry_initial_delay_ms")? {
                config.retry_initial_delay_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("retry_backoff_factor")? {
                config.retry_backoff_factor = v.extract()?;
            }
            if let Some(v) = kw.get_item("retry_max_delay_ms")? {
                config.retry_max_delay_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("request_timeout_ms")? {
                config.request_timeout_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("streams_deactivated_warn_ms")? {
                config.streams_deactivated_warn_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("keep_alive_enabled")? {
                config.keep_alive_enabled = v.extract()?;
            }
            if let Some(v) = kw.get_item("keep_alive_inactivity_ms")? {
                config.keep_alive_inactivity_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("keep_alive_response_timeout_ms")? {
                config.keep_alive_response_timeout_ms = v.extract()?;
            }
            if let Some(v) = kw.get_item("slow_consumer_hi_water_mark")? {
                config.slow_consumer_hi_water_mark = v.extract()?;
            }
            if let Some(v) = kw.get_item("slow_consumer_lo_water_mark")? {
                config.slow_consumer_lo_water_mark = v.extract()?;
            }
            if let Some(v) = kw.get_item("sdk_log_level")? {
                config.sdk_log_level = v.extract()?;
            }
            if let Some(v) = kw.get_item("socks5_host")? {
                config.socks5_host = v.extract()?;
            }
            if let Some(v) = kw.get_item("socks5_port")? {
                config.socks5_port = v.extract()?;
            }
        }

        Ok(config)
    }

    fn __repr__(&self) -> String {
        let fcp_display = self.field_cache_path.as_deref().unwrap_or("default");
        let auth_method = self.auth_method.as_deref().unwrap_or("none");
        format!(
            "EngineConfig(host='{}', port={}, request_pool_size={}, subscription_pool_size={}, \
             shard_requests={}, shard_threshold={}, shard_chunk_size={}, shard_max_concurrent={}, \
             validation_mode='{}', overflow_policy='{}', auth_method='{}', field_cache_path='{}', warmup_services={:?})",
            self.host,
            self.port,
            self.request_pool_size,
            self.subscription_pool_size,
            self.shard_requests,
            self.shard_threshold,
            self.shard_chunk_size,
            self.shard_max_concurrent,
            self.validation_mode,
            self.overflow_policy,
            auth_method,
            fcp_display,
            self.warmup_services
        )
    }
}

fn require_auth_value(value: &Option<String>, field: &str, method: &str) -> PyResult<String> {
    value
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            PyValueError::new_err(format!("{field} is required for auth_method='{method}'"))
        })
}

/// Reject negative millisecond fields (mirrors napi's
/// `require_non_negative_duration`); `None` keeps the engine/SDK default.
fn require_non_negative_ms(value: Option<i32>, field: &str) -> PyResult<()> {
    match value {
        Some(v) if v < 0 => Err(PyValueError::new_err(format!(
            "{field} must be non-negative"
        ))),
        _ => Ok(()),
    }
}

/// Reject negative TLS timeout fields (mirrors napi's
/// `require_non_negative_timeout` wording).
fn require_non_negative_tls_timeout(value: Option<i32>, field: &str) -> PyResult<()> {
    match value {
        Some(v) if v < 0 => Err(PyValueError::new_err(format!(
            "{field} must be a non-negative integer number of milliseconds"
        ))),
        _ => Ok(()),
    }
}

/// Reject out-of-range slow-consumer watermarks. `inclusive_high` matches
/// napi: hi accepts 1.0, lo does not.
fn require_watermark_range(value: Option<f32>, field: &str, inclusive_high: bool) -> PyResult<()> {
    let Some(v) = value else {
        return Ok(());
    };
    let in_range = if inclusive_high {
        (0.0..=1.0).contains(&v)
    } else {
        (0.0..1.0).contains(&v)
    };
    if in_range {
        Ok(())
    } else {
        let range = if inclusive_high {
            "0.0..=1.0"
        } else {
            "0.0..1.0"
        };
        Err(PyValueError::new_err(format!("{field} must be in {range}")))
    }
}

fn build_auth_config(py_config: &PyEngineConfig) -> PyResult<Option<AuthConfig>> {
    let method = match py_config.auth_method.as_deref() {
        None => {
            if py_config.app_name.is_some()
                || py_config.dir_property.is_some()
                || py_config.user_id.is_some()
                || py_config.ip_address.is_some()
                || py_config.token.is_some()
            {
                return Err(PyValueError::new_err(
                    "auth_method is required when auth-specific fields are provided",
                ));
            }
            return Ok(None);
        }
        Some(method) => method.trim().to_ascii_lowercase(),
    };

    let auth = match method.as_str() {
        "" | "none" => None,
        "user" => Some(AuthConfig::User),
        "app" => Some(AuthConfig::App {
            app_name: require_auth_value(&py_config.app_name, "app_name", &method)?,
        }),
        "userapp" => Some(AuthConfig::UserApp {
            app_name: require_auth_value(&py_config.app_name, "app_name", &method)?,
        }),
        "dir" | "directory" => Some(AuthConfig::Directory {
            property_name: require_auth_value(&py_config.dir_property, "dir_property", &method)?,
        }),
        "manual" => Some(AuthConfig::Manual {
            app_name: require_auth_value(&py_config.app_name, "app_name", &method)?,
            user_id: require_auth_value(&py_config.user_id, "user_id", &method)?,
            ip_address: require_auth_value(&py_config.ip_address, "ip_address", &method)?,
        }),
        "token" => Some(AuthConfig::Token {
            token: require_auth_value(&py_config.token, "token", &method)?,
        }),
        other => {
            return Err(PyValueError::new_err(format!(
                "Invalid auth_method: {other}. Must be one of ['none', 'user', 'app', 'userapp', 'dir', 'directory', 'manual', 'token']",
            )));
        }
    };

    Ok(auth)
}

/// Expose `(host, port)` of the first server in the default `Transport::Direct`,
/// so `PyEngineConfig`'s Python-visible defaults stay in lockstep with the
/// Rust-side default.
fn default_direct_host_port(defaults: &EngineConfig) -> (String, u16) {
    match &defaults.transport {
        Transport::Direct(servers) => servers
            .first()
            .map(|s| (s.host.clone(), s.port))
            .unwrap_or_else(|| ("localhost".to_string(), 8194)),
        Transport::Zfp(_) => ("localhost".to_string(), 8194),
    }
}

fn resolve_transport(py: &PyEngineConfig) -> PyResult<Transport> {
    let zfp = py
        .zfp_remote
        .as_deref()
        .map(|s| s.parse::<xbbg_core::zfp::ZfpRemote>())
        .transpose()
        .map_err(PyValueError::new_err)?;

    let socks5 = match (py.socks5_host.as_deref(), py.socks5_port) {
        (Some(host), Some(port)) => Some(Socks5Proxy {
            host: host.to_string(),
            port,
        }),
        (Some(_), None) => {
            return Err(PyValueError::new_err("socks5_host set without socks5_port"));
        }
        (None, Some(_)) => {
            return Err(PyValueError::new_err("socks5_port set without socks5_host"));
        }
        (None, None) => None,
    };

    let explicit_servers = !py.servers.is_empty();
    let explicit_hostport = py.host != "localhost" || py.port != 8194;

    if let Some(remote) = zfp {
        if explicit_servers || explicit_hostport {
            return Err(PyValueError::new_err(
                "zfp_remote cannot be combined with host/port/servers — \
                 ZFP supplies Bloomberg endpoints via the leased-line path",
            ));
        }
        if socks5.is_some() {
            return Err(PyValueError::new_err(
                "zfp_remote cannot be combined with socks5_host/socks5_port",
            ));
        }
        return Ok(Transport::Zfp(remote));
    }

    let raw = if explicit_servers {
        py.servers.clone()
    } else {
        vec![(py.host.clone(), py.port)]
    };
    let servers = raw
        .into_iter()
        .map(|(host, port)| ServerAddr {
            host,
            port,
            proxy: socks5.clone(),
        })
        .collect();
    Ok(Transport::Direct(servers))
}

fn resolve_tls(py: &PyEngineConfig) -> PyResult<Option<TlsConfig>> {
    require_non_negative_tls_timeout(py.tls_handshake_timeout_ms, "tls_handshake_timeout_ms")?;
    require_non_negative_tls_timeout(py.tls_crl_fetch_timeout_ms, "tls_crl_fetch_timeout_ms")?;
    match (
        py.tls_client_credentials.as_deref(),
        py.tls_trust_material.as_deref(),
    ) {
        (None, None) => Ok(None),
        (Some(creds), Some(trust)) => Ok(Some(TlsConfig {
            client_credentials: creds.to_string(),
            client_credentials_password: py
                .tls_client_credentials_password
                .clone()
                .unwrap_or_default(),
            trust_material: trust.to_string(),
            handshake_timeout_ms: py.tls_handshake_timeout_ms,
            crl_fetch_timeout_ms: py.tls_crl_fetch_timeout_ms,
        })),
        (Some(_), None) => Err(PyValueError::new_err(
            "tls_client_credentials set without tls_trust_material",
        )),
        (None, Some(_)) => Err(PyValueError::new_err(
            "tls_trust_material set without tls_client_credentials",
        )),
    }
}

impl TryFrom<&PyEngineConfig> for EngineConfig {
    type Error = PyErr;

    fn try_from(py_config: &PyEngineConfig) -> Result<Self, Self::Error> {
        let validation_mode: ValidationMode = py_config
            .validation_mode
            .parse()
            .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))?;

        let overflow_policy: OverflowPolicy = py_config
            .overflow_policy
            .parse()
            .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))?;

        if py_config.subscription_stream_capacity == 0 {
            return Err(PyValueError::new_err(
                "subscription_stream_capacity must be greater than zero",
            ));
        }
        require_non_negative_ms(
            py_config.keep_alive_inactivity_ms,
            "keep_alive_inactivity_ms",
        )?;
        require_non_negative_ms(
            py_config.keep_alive_response_timeout_ms,
            "keep_alive_response_timeout_ms",
        )?;
        require_watermark_range(
            py_config.slow_consumer_hi_water_mark,
            "slow_consumer_hi_water_mark",
            true,
        )?;
        require_watermark_range(
            py_config.slow_consumer_lo_water_mark,
            "slow_consumer_lo_water_mark",
            false,
        )?;
        let auth = build_auth_config(py_config)?;
        let transport = resolve_transport(py_config)?;
        let tls = resolve_tls(py_config)?;

        Ok(EngineConfig {
            transport,
            tls,
            request_pool_size: py_config.request_pool_size,
            subscription_pool_size: py_config.subscription_pool_size,
            shard_requests: py_config.shard_requests,
            shard_threshold: py_config.shard_threshold,
            shard_chunk_size: py_config.shard_chunk_size,
            shard_max_concurrent: py_config.shard_max_concurrent,
            validation_mode,
            subscription_flush_threshold: py_config.subscription_flush_threshold,
            max_event_queue_size: py_config.max_event_queue_size,
            command_queue_size: py_config.command_queue_size,
            subscription_stream_capacity: py_config.subscription_stream_capacity,
            overflow_policy,
            warmup_services: py_config.warmup_services.clone(),
            field_cache_path: py_config
                .field_cache_path
                .as_ref()
                .map(std::path::PathBuf::from),
            auth,
            num_start_attempts: py_config.num_start_attempts,
            auto_restart_on_disconnection: py_config.auto_restart_on_disconnection,
            retry_policy: RetryPolicy {
                max_retries: py_config.retry_max_retries,
                initial_delay_ms: py_config.retry_initial_delay_ms,
                backoff_factor: py_config.retry_backoff_factor,
                max_delay_ms: py_config.retry_max_delay_ms,
            },
            request_timeout_ms: py_config.request_timeout_ms,
            streams_deactivated_warn_ms: py_config.streams_deactivated_warn_ms,
            keep_alive_enabled: py_config.keep_alive_enabled,
            keep_alive_inactivity_ms: py_config.keep_alive_inactivity_ms,
            keep_alive_response_timeout_ms: py_config.keep_alive_response_timeout_ms,
            slow_consumer_hi_water_mark: py_config.slow_consumer_hi_water_mark,
            slow_consumer_lo_water_mark: py_config.slow_consumer_lo_water_mark,
            sdk_log_level: py_config
                .sdk_log_level
                .parse()
                .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))?,
        })
    }
}

/// Result of a Bloomberg entitlement check.
#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass(module = "xbbg._core", frozen, skip_from_py_object)]
#[derive(Clone)]
struct EntitlementReport {
    #[pyo3(get)]
    entitled: bool,
    #[pyo3(get)]
    failed_eids: Vec<i32>,
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl EntitlementReport {
    fn __repr__(&self) -> String {
        format!(
            "EntitlementReport(entitled={}, failed_eids={:?})",
            self.entitled, self.failed_eids
        )
    }
}

/// Python wrapper for the xbbg Engine.
#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass]
struct PyEngine {
    engine: Arc<Engine>,
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl PyEngine {
    /// Create a new Engine with optional host/port configuration.
    ///
    /// This blocks while connecting to Bloomberg. GIL is released during connection.
    /// For more configuration options, use `Engine.with_config()`.
    #[new]
    #[pyo3(signature = (host="localhost", port=8194))]
    fn new(py: Python<'_>, host: &str, port: u16) -> PyResult<Self> {
        info!(
            host = host,
            port = port,
            "PyEngine: connecting to Bloomberg"
        );

        let config = EngineConfig {
            transport: Transport::Direct(vec![ServerAddr::new(host, port)]),
            ..Default::default()
        };

        Self::start_engine(py, config)
    }

    /// Create a new Engine with full configuration.
    ///
    /// This blocks while connecting to Bloomberg. GIL is released during connection.
    ///
    /// Example:
    /// ```python
    /// config = EngineConfig(
    ///     host="localhost",
    ///     port=8194,
    ///     request_pool_size=4,
    ///     subscription_pool_size=8,
    ///     overflow_policy="drop_newest",
    /// )
    /// engine = Engine.with_config(config)
    /// ```
    #[staticmethod]
    fn with_config(py: Python<'_>, config: &PyEngineConfig) -> PyResult<Self> {
        info!(
            host = %config.host,
            port = config.port,
            request_pool_size = config.request_pool_size,
            subscription_pool_size = config.subscription_pool_size,
            "PyEngine: connecting with custom config"
        );

        let rust_config: EngineConfig = config.try_into()?;

        Self::start_engine(py, rust_config)
    }

    // =========================================================================
    // Generic Request API
    // =========================================================================

    /// Generic async Bloomberg request.
    ///
    /// Accepts a dictionary of parameters and returns an xbbg ArrowRecordBatch.
    ///
    /// Required keys:
    /// - service: Bloomberg service URI (e.g., "//blp/refdata")
    /// - operation: Request operation name (e.g., "ReferenceDataRequest")
    ///   Use "" / Operation.RAW_REQUEST together with request_operation for raw mode.
    ///
    /// Optional keys:
    /// - extractor: Extractor type hint (e.g., "refdata", "histdata", "intraday_bar")
    ///   If omitted, Rust resolves a default from `operation`.
    /// - request_operation: Actual Bloomberg operation name when operation=""
    ///
    /// Optional keys (depend on request type):
    /// - securities: List of security identifiers
    /// - security: Single security identifier
    /// - fields: List of field names
    /// - overrides: List of (name, value) tuples
    /// - start_date, end_date: For historical requests
    /// - start_datetime, end_datetime: For intraday requests
    /// - event_type: For intraday bars (TRADE, BID, ASK)
    /// - interval: Bar interval in minutes
    /// - options: Additional Bloomberg options
    #[pyo3(signature = (params))]
    fn request<'py>(
        &self,
        py: Python<'py>,
        params: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        // Extract and convert params to Rust struct
        let rust_params = dict_to_request_params(params)?;

        debug!(
            service = %rust_params.service,
            operation = %rust_params.operation,
            extractor = ?rust_params.extractor,
            securities = ?rust_params.securities,
            fields = ?rust_params.fields,
            "PyEngine: sending request"
        );

        shutdown_safe_future(py, &self.engine, async move {
            let batch = engine.request(rust_params).await.map_err(|e| {
                warn!(error = %e, "PyEngine: request failed");
                blp_async_error_to_pyerr(e)
            })?;

            debug!(num_rows = batch.num_rows(), "PyEngine: request completed");

            Python::attach(|py| native_arrow::record_batch_to_arrow_record_batch(py, batch))
        })
    }

    /// Return the seat type for the lazily authorized identity.
    ///
    /// Authorization is performed on first use: the configured auth identity is used when
    /// EngineConfig has auth settings, otherwise the Desktop terminal OS-logon user is used.
    /// First use may take a moment and authorization timeout failures are retryable.
    fn seat_type<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, &self.engine, async move {
            let seat_type = engine.seat_type().await.map_err(blp_async_error_to_pyerr)?;
            Python::attach(|py| Ok(seat_type.as_str().into_pyobject(py)?.into_any().unbind()))
        })
    }

    /// Check EID entitlements for the lazily authorized identity.
    ///
    /// Authorization is performed on first use: the configured auth identity is used when
    /// EngineConfig has auth settings, otherwise the Desktop terminal OS-logon user is used.
    /// First use may take a moment and authorization timeout failures are retryable.
    #[pyo3(signature = (service, eids))]
    fn check_entitlements<'py>(
        &self,
        py: Python<'py>,
        service: String,
        eids: Vec<i32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, &self.engine, async move {
            let report = engine
                .check_entitlements(&service, &eids)
                .await
                .map_err(blp_async_error_to_pyerr)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    EntitlementReport {
                        entitled: report.entitled,
                        failed_eids: report.failed_eids,
                    },
                )
                .map(|obj| obj.into_any())
            })
        })
    }

    /// Return whether the lazily authorized identity is authorized for a service.
    ///
    /// Authorization is performed on first use: the configured auth identity is used when
    /// EngineConfig has auth settings, otherwise the Desktop terminal OS-logon user is used.
    /// First use may take a moment and authorization timeout failures are retryable.
    #[pyo3(signature = (service))]
    fn identity_is_authorized<'py>(
        &self,
        py: Python<'py>,
        service: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, &self.engine, async move {
            let authorized = engine
                .identity_is_authorized(&service)
                .await
                .map_err(blp_async_error_to_pyerr)?;
            Python::attach(|py| Ok(PyBool::new(py, authorized).to_owned().into_any().unbind()))
        })
    }

    /// Resolve exchange metadata using override -> cache -> Bloomberg waterfall.
    fn resolve_exchange<'py>(
        &self,
        py: Python<'py>,
        ticker: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, &self.engine, async move {
            let info = engine.resolve_exchange(&ticker).await;
            Python::attach(|py| exchange_info_to_pydict(py, &info))
        })
    }

    /// Fetch market-level metadata (exchange, timezone, futures cycle info).
    fn fetch_market_info<'py>(
        &self,
        py: Python<'py>,
        ticker: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        shutdown_safe_future(py, &self.engine, async move {
            let info = engine
                .fetch_market_info(&ticker)
                .await
                .map_err(blp_async_error_to_pyerr)?;
            Python::attach(|py| market_info_to_pydict(py, &info))
        })
    }

    /// Resolve market timing (BOD/EOD/FINISHED) for a ticker/date.
    #[pyo3(signature = (ticker, date, timing="EOD", tz=None))]
    fn market_timing<'py>(
        &self,
        py: Python<'py>,
        ticker: String,
        date: String,
        timing: &str,
        tz: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let timing = MarketTiming::parse(timing)
            .ok_or_else(|| PyValueError::new_err("timing must be one of: BOD, EOD, FINISHED"))?;
        let date = NaiveDate::parse_from_str(&date, "%Y-%m-%d")
            .map_err(|_| PyValueError::new_err("date must be YYYY-MM-DD"))?;

        shutdown_safe_future(py, &self.engine, async move {
            let value = engine
                .resolve_market_timing(&ticker, date, timing, tz.as_deref())
                .await
                .map_err(blp_async_error_to_pyerr)?;
            Python::attach(|py| Ok(value.into_pyobject(py)?.into_any().unbind()))
        })
    }

    /// Invalidate exchange cache (one ticker or all entries).
    #[pyo3(signature = (ticker=None))]
    fn invalidate_exchange_cache(&self, ticker: Option<String>) -> PyResult<()> {
        self.engine
            .invalidate_exchange_cache(ticker.as_deref())
            .map_err(PyRuntimeError::new_err)
    }

    /// Persist exchange cache to disk.
    fn save_exchange_cache(&self, py: Python<'_>) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(move || engine.save_exchange_cache())
            .map_err(PyRuntimeError::new_err)
    }

    // =========================================================================
    // Field Type Resolution API
    // =========================================================================

    /// Resolve field types for a list of fields.
    #[pyo3(signature = (fields, overrides=None, default_type="string"))]
    fn resolve_field_types<'py>(
        &self,
        py: Python<'py>,
        fields: Vec<String>,
        overrides: Option<HashMap<String, String>>,
        default_type: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let default = default_type.to_string();

        shutdown_safe_future(py, &self.engine, async move {
            let resolved = engine
                .resolve_field_types(&fields, overrides.as_ref(), &default)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| {
                let dict = PyDict::new(py);
                for (k, v) in resolved {
                    dict.set_item(k, v)?;
                }
                Ok(dict.into_any().unbind())
            })
        })
    }

    /// Get field info from cache.
    fn get_field_info(&self, field: &str) -> Option<HashMap<String, String>> {
        self.engine.get_field_info(field).map(|info| {
            let mut map = HashMap::new();
            map.insert("field_id".to_string(), info.field_id);
            map.insert("arrow_type".to_string(), info.arrow_type);
            map.insert("description".to_string(), info.description);
            map.insert("category".to_string(), info.category);
            map
        })
    }

    /// Clear the field type cache.
    fn clear_field_cache(&self) {
        self.engine.clear_field_cache();
    }

    /// Save the field type cache to disk.
    fn save_field_cache(&self, py: Python<'_>) -> PyResult<()> {
        let engine = self.engine.clone();
        py.detach(move || engine.save_field_cache())
            .map_err(PyRuntimeError::new_err)
    }

    /// Get field cache statistics including the active cache path.
    fn field_cache_stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (entry_count, cache_path) = self.engine.field_cache_stats();
        let dict = PyDict::new(py);
        dict.set_item("entry_count", entry_count)?;
        dict.set_item("cache_path", cache_path.to_string_lossy().into_owned())?;
        Ok(dict.into())
    }

    /// Validate Bloomberg field names.
    ///
    /// Queries Bloomberg's field info service to check if the given fields exist.
    /// Returns a list of invalid field names (fields that Bloomberg doesn't recognize).
    ///
    /// Example:
    ///     invalid = await engine.validate_fields(["PX_LAST", "INVALID_FIELD"])
    ///     # invalid = ["INVALID_FIELD"]
    fn validate_fields<'py>(
        &self,
        py: Python<'py>,
        fields: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        shutdown_safe_future(py, &self.engine, async move {
            let invalid = engine
                .validate_fields(&fields)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| Ok(invalid.into_pyobject(py)?.into_any().unbind()))
        })
    }

    // =========================================================================
    // Schema Cache API
    // =========================================================================

    /// Get service schema (from cache or introspect).
    ///
    /// Returns a dictionary with schema information including operations.
    /// First checks disk cache; if not cached, introspects the service.
    #[pyo3(signature = (service))]
    fn get_schema<'py>(&self, py: Python<'py>, service: String) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        shutdown_safe_future(py, &self.engine, async move {
            let schema = engine
                .get_schema(&service)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            // Convert to JSON string for Python (dereference Arc)
            let json = serde_json::to_string(&*schema)
                .map_err(|e| PyRuntimeError::new_err(format!("serialize schema: {e}")))?;

            Python::attach(|py| Ok(json.into_pyobject(py)?.into_any().unbind()))
        })
    }

    /// Get a specific operation schema.
    ///
    /// Returns operation details including request/response element definitions.
    #[pyo3(signature = (service, operation))]
    fn get_operation<'py>(
        &self,
        py: Python<'py>,
        service: String,
        operation: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        shutdown_safe_future(py, &self.engine, async move {
            let op = engine
                .get_operation(&service, &operation)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            let json = serde_json::to_string(&op)
                .map_err(|e| PyRuntimeError::new_err(format!("serialize operation: {e}")))?;

            Python::attach(|py| Ok(json.into_pyobject(py)?.into_any().unbind()))
        })
    }

    /// List all operations for a service.
    #[pyo3(signature = (service))]
    fn list_operations<'py>(
        &self,
        py: Python<'py>,
        service: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        shutdown_safe_future(py, &self.engine, async move {
            let ops = engine
                .list_operations(&service)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| {
                let list = pyo3::types::PyList::new(py, ops)?;
                Ok(list.into_any().unbind())
            })
        })
    }

    /// Get cached schema without introspection.
    ///
    /// Returns None if the schema is not cached.
    fn get_cached_schema(&self, service: &str) -> Option<String> {
        self.engine
            .get_cached_schema(service)
            .and_then(|s| serde_json::to_string(&*s).ok())
    }

    /// Invalidate a cached schema.
    fn invalidate_schema(&self, service: &str) {
        self.engine.invalidate_schema(service);
    }

    /// Clear all cached schemas.
    fn clear_schema_cache(&self) {
        self.engine.clear_schema_cache();
    }

    /// List all cached service URIs.
    fn list_cached_schemas(&self) -> Vec<String> {
        self.engine.list_cached_schemas()
    }

    // =========================================================================
    // Schema Validation API
    // =========================================================================

    /// Get valid enum values for an element.
    ///
    /// Returns a list of valid enum values, or None if the element is not an enum.
    #[pyo3(signature = (service, operation, element))]
    fn get_enum_values<'py>(
        &self,
        py: Python<'py>,
        service: String,
        operation: String,
        element: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        shutdown_safe_future(py, &self.engine, async move {
            let values = engine
                .get_enum_values(&service, &operation, &element)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| match values {
                Some(v) => {
                    let list = pyo3::types::PyList::new(py, v)?;
                    Ok(list.into_any().unbind())
                }
                None => Ok(py.None()),
            })
        })
    }

    /// List all valid element names for an operation.
    #[pyo3(signature = (service, operation))]
    fn list_valid_elements<'py>(
        &self,
        py: Python<'py>,
        service: String,
        operation: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();

        shutdown_safe_future(py, &self.engine, async move {
            let elements = engine
                .list_valid_elements(&service, &operation)
                .await
                .map_err(blp_async_error_to_pyerr)?;

            Python::attach(|py| match elements {
                Some(v) => {
                    let list = pyo3::types::PyList::new(py, v)?;
                    Ok(list.into_any().unbind())
                }
                None => Ok(py.None()),
            })
        })
    }

    // =========================================================================
    // Subscription API
    // =========================================================================

    /// Subscribe to real-time market data.
    ///
    /// Returns a PySubscription that supports async iteration and dynamic add/remove.
    /// GIL is released during async operations; iteration and add/remove use separate
    /// locks to avoid contention.
    ///
    /// Example:
    /// ```python
    /// sub = await engine.subscribe(['AAPL US Equity'], ['LAST_PRICE', 'BID', 'ASK'])
    /// async for batch in sub:
    ///     print(batch)
    /// await sub.unsubscribe()
    /// ```
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (tickers, fields, flush_threshold=None, overflow_policy=None, stream_capacity=None, all_fields=false))]
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        tickers: Vec<String>,
        fields: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<String>,
        stream_capacity: Option<usize>,
        all_fields: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let tickers_clone = tickers.clone();
        let fields_clone = fields.clone();

        let op = overflow_policy
            .as_deref()
            .map(|s| {
                s.parse::<OverflowPolicy>()
                    .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))
            })
            .transpose()?;

        debug!(
            tickers = ?tickers,
            fields = ?fields,
            "PyEngine: creating subscription"
        );

        shutdown_safe_future(py, &self.engine, async move {
            let stream = engine
                .subscribe_with_options(
                    "//blp/mktdata".to_string(),
                    tickers_clone.clone(),
                    fields_clone.clone(),
                    all_fields,
                    vec![],
                    stream_capacity,
                    flush_threshold,
                    op,
                )
                .await
                .map_err(blp_async_error_to_pyerr)?;

            debug!("PyEngine: subscription created");

            // Destructure the SubscriptionStream to separate rx from the rest
            // This allows iteration (rx) and modification (claim) to use separate locks
            let (rx, tx, claim, status, ft, op_policy, service, options, all_fields) =
                stream.into_parts().map_err(blp_error_to_pyerr)?;

            let (close_signal, _) = watch::channel(false);
            let handle = SubscriptionStreamHandle {
                tx,
                claim: Some(claim),
                fields: fields_clone,
                all_fields,
                service,
                options,
                flush_threshold: ft,
                overflow_policy: op_policy,
                _stream_capacity: stream_capacity,
                status,
            };

            Python::attach(|py| {
                let py_sub = PySubscription {
                    rx: Arc::new(Mutex::new(Some(rx))),
                    stream: Arc::new(Mutex::new(Some(handle))),
                    ops: Arc::new(Mutex::new(())),
                    close_signal,
                    engine_shutdown: engine.shutdown_receiver(),
                };
                Ok(Py::new(py, py_sub)?.into_any())
            })
        })
    }

    /// Subscribe to real-time data with custom service and options.
    ///
    /// This is the generic subscription method for services like //blp/mktvwap.
    ///
    /// Args:
    ///     service: Bloomberg service URI (e.g., "//blp/mktvwap")
    ///     tickers: List of securities to subscribe to
    ///     fields: List of fields to subscribe to
    ///     options: List of subscription options (e.g., ["VWAP_START_TIME=09:30"])
    ///
    /// Example:
    /// ```python
    /// sub = await engine.subscribe_with_options(
    ///     '//blp/mktvwap',
    ///     ['//blp/mktvwap/ticker/IBM US Equity'],
    ///     ['VWAP'],
    ///     ['VWAP_START_TIME=10:00', 'VWAP_END_TIME=16:00']
    /// )
    /// async for batch in sub:
    ///     print(batch)
    /// ```
    #[pyo3(signature = (service, tickers, fields, options=None, flush_threshold=None, overflow_policy=None, stream_capacity=None, all_fields=false))]
    #[allow(clippy::too_many_arguments)]
    fn subscribe_with_options<'py>(
        &self,
        py: Python<'py>,
        service: String,
        tickers: Vec<String>,
        fields: Vec<String>,
        options: Option<Vec<String>>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<String>,
        stream_capacity: Option<usize>,
        all_fields: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let tickers_clone = tickers.clone();
        let fields_clone = fields.clone();
        let options_clone = options.clone().unwrap_or_default();
        let service_clone = service.clone();

        let op = overflow_policy
            .as_deref()
            .map(|s| {
                s.parse::<OverflowPolicy>()
                    .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))
            })
            .transpose()?;

        debug!(
            service = %service,
            tickers = ?tickers,
            fields = ?fields,
            options = ?options,
            "PyEngine: creating subscription with options"
        );

        shutdown_safe_future(py, &self.engine, async move {
            let stream = engine
                .subscribe_with_options(
                    service_clone.clone(),
                    tickers_clone.clone(),
                    fields_clone.clone(),
                    all_fields,
                    options_clone.clone(),
                    stream_capacity,
                    flush_threshold,
                    op,
                )
                .await
                .map_err(blp_async_error_to_pyerr)?;

            debug!("PyEngine: subscription with options created");

            let (rx, tx, claim, status, ft, op_policy, service, options, all_fields) =
                stream.into_parts().map_err(blp_error_to_pyerr)?;

            let (close_signal, _) = watch::channel(false);
            let handle = SubscriptionStreamHandle {
                tx,
                claim: Some(claim),
                fields: fields_clone,
                all_fields,
                service,
                options,
                flush_threshold: ft,
                overflow_policy: op_policy,
                _stream_capacity: stream_capacity,
                status,
            };

            Python::attach(|py| {
                let py_sub = PySubscription {
                    rx: Arc::new(Mutex::new(Some(rx))),
                    stream: Arc::new(Mutex::new(Some(handle))),
                    ops: Arc::new(Mutex::new(())),
                    close_signal,
                    engine_shutdown: engine.shutdown_receiver(),
                };
                Ok(Py::new(py, py_sub)?.into_any())
            })
        })
    }

    // =========================================================================
    // Lifecycle Management
    // =========================================================================

    /// Signal engine shutdown (non-blocking).
    ///
    /// Signals all worker threads to stop. They will terminate when they
    /// finish their current work or see the shutdown signal.
    ///
    /// This is called automatically during Python interpreter shutdown via atexit.
    /// You usually don't need to call this directly.
    fn signal_shutdown(&self) {
        info!("PyEngine: signal_shutdown called");
        self.engine.signal_shutdown();
    }

    fn worker_health(&self) -> PyResult<Vec<(usize, String)>> {
        Ok(self
            .engine
            .request_pool_health()
            .into_iter()
            .map(|(id, h)| (id, h.as_str().to_string()))
            .collect())
    }

    /// Check if the Bloomberg connection is healthy.
    ///
    /// Returns True if at least one worker has a live Bloomberg session.
    fn is_connected(&self) -> bool {
        self.engine
            .request_pool_health()
            .iter()
            .any(|(_, h)| h.as_str() == "healthy")
    }
}

impl PyEngine {
    /// Shared helper: release GIL and start Engine on a blocking thread.
    #[allow(clippy::result_large_err)]
    fn start_engine(py: Python<'_>, config: EngineConfig) -> PyResult<Self> {
        // Release GIL during blocking Engine::start().
        // Engine::start() creates Bloomberg sessions and waits for them to connect,
        // which can take seconds — must not hold GIL during this.
        let engine = py.detach(|| Engine::start(config)).map_err(|e| {
            warn!(error = %e, "PyEngine: connection failed");
            blp_async_error_to_pyerr(e)
        })?;

        info!("PyEngine: connected successfully");

        Ok(Self {
            engine: Arc::new(engine),
        })
    }
}

// =============================================================================
// PySubscription - Async iterator for real-time market data
// =============================================================================

/// Python subscription handle for real-time market data.
///
/// Supports:
/// - Async iteration (`async for batch in sub`)
/// - Dynamic add/remove of tickers
/// - Explicit unsubscribe with optional drain
/// - Context manager (`async with`)
///
/// Data arrives as `Result<RecordBatch, BlpError>`:
/// - `Ok(batch)` — yields an xbbg ArrowRecordBatch
/// - `Err(error)` — raises a Python exception (BlpRequestError, BlpInternalError, etc.)
///
/// Design: Uses separate locks for rx (data receiving) vs stream (metadata snapshots),
/// plus a dedicated operation lock to serialize add/remove/unsubscribe without holding
/// the stream metadata lock across Bloomberg awaits.
#[cfg_attr(feature = "stub-gen", gen_stub_pyclass)]
#[pyclass]
pub struct PySubscription {
    /// Receiver for incoming data - separate lock so iteration doesn't block add/remove
    rx: SharedStreamReceiver,
    /// Stream handle for metadata and modification operations
    stream: Arc<Mutex<Option<SubscriptionStreamHandle>>>,
    /// Serializes add/remove/unsubscribe without holding the stream lock across await.
    ops: Arc<Mutex<()>>,
    /// Signal used to wake pending iteration during unsubscribe/close.
    close_signal: watch::Sender<bool>,
    /// Engine-level shutdown signal — wakes pending iteration when the engine shuts down.
    engine_shutdown: watch::Receiver<bool>,
}

/// Internal handle for subscription metadata and operations (without the receiver)
struct SubscriptionStreamHandle {
    tx: StreamSender,
    claim: Option<xbbg_async::engine::SessionClaim>,
    fields: Vec<String>,
    all_fields: bool,
    service: String,
    options: Vec<String>,
    flush_threshold: Option<usize>,
    overflow_policy: Option<OverflowPolicy>,
    _stream_capacity: Option<usize>,
    status: xbbg_async::engine::SharedSubscriptionStatus,
}

struct PendingAdd {
    command: SubscriptionCommandHandle,
    new_topics: Vec<String>,
    service: String,
    fields: Vec<String>,
    all_fields: bool,
    options: Vec<String>,
    flush_threshold: Option<usize>,
    overflow_policy: Option<OverflowPolicy>,
    tx: StreamSender,
    status: xbbg_async::engine::SharedSubscriptionStatus,
}

struct PendingRemove {
    command: SubscriptionCommandHandle,
    topics: Vec<String>,
    keys: Vec<usize>,
}

impl SubscriptionStreamHandle {
    fn prepare_add(&self, tickers: Vec<String>) -> PyResult<Option<PendingAdd>> {
        let claim = self
            .claim
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("subscription already closed"))?;
        let command = claim.command_handle().map_err(blp_async_error_to_pyerr)?;

        let mut seen_topics = HashSet::new();
        let snapshot = self.status.load();
        let new_topics: Vec<String> = tickers
            .into_iter()
            .filter(|t| !snapshot.topic_to_key().contains_key(t) && seen_topics.insert(t.clone()))
            .collect();

        if new_topics.is_empty() {
            return Ok(None);
        }

        Ok(Some(PendingAdd {
            command,
            new_topics,
            service: self.service.clone(),
            fields: self.fields.clone(),
            all_fields: self.all_fields,
            options: self.options.clone(),
            flush_threshold: self.flush_threshold,
            overflow_policy: self.overflow_policy,
            tx: self.tx.clone(),
            status: self.status.clone(),
        }))
    }

    fn apply_add(
        &mut self,
        topics: &[String],
        new_keys: Vec<usize>,
        new_metrics: Vec<Arc<SubscriptionMetrics>>,
    ) {
        self.status.update(|state| {
            state.add_active(topics, &new_keys, new_metrics.clone());
        });
    }

    fn prepare_remove(&self, tickers: Vec<String>) -> PyResult<Option<PendingRemove>> {
        let claim = self
            .claim
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("subscription already closed"))?;
        let command = claim.command_handle().map_err(blp_async_error_to_pyerr)?;

        let mut seen_keys = HashSet::new();
        let mut topics = Vec::new();
        let mut keys = Vec::new();
        let snapshot = self.status.load();

        for ticker in tickers {
            if let Some(&key) = snapshot.topic_to_key().get(&ticker) {
                if seen_keys.insert(key) {
                    topics.push(ticker);
                    keys.push(key);
                }
            }
        }

        if keys.is_empty() {
            return Ok(None);
        }

        Ok(Some(PendingRemove {
            command,
            topics,
            keys,
        }))
    }

    fn apply_remove(&mut self, topics: &[String]) {
        self.status.update(|state| {
            for topic in topics {
                state.remove_topic(topic);
            }
        });
    }
}

#[derive(Clone, Default)]
struct SubscriptionSnapshot {
    present: bool,
    topics: Vec<String>,
    fields: Vec<String>,
    is_active: bool,
    all_failed: bool,
    messages_received: u64,
    dropped_batches: u64,
    batches_sent: u64,
    slow_consumer: bool,
    data_loss_events: u64,
    last_message_us: u64,
    last_data_loss_us: u64,
    failures: Vec<SubscriptionFailureInfo>,
    topic_states: Vec<TopicStatusInfo>,
    session: SessionStatusInfo,
    services: Vec<ServiceStatusInfo>,
    admin: AdminStatusInfo,
    events: Vec<SubscriptionEventInfo>,
    effective_overflow_policy: String,
}

impl PySubscription {
    fn snapshot_from_stream(
        stream: &Arc<Mutex<Option<SubscriptionStreamHandle>>>,
    ) -> SubscriptionSnapshot {
        let guard = stream.blocking_lock();
        match guard.as_ref() {
            Some(handle) => {
                let snapshot = handle.status.load();
                let (
                    messages_received,
                    dropped_batches,
                    batches_sent,
                    slow_consumer,
                    data_loss_events,
                    last_message_us,
                    last_data_loss_us,
                ) = subscription_metrics_totals(snapshot.fields_metrics());
                let mut topic_states: Vec<TopicStatusInfo> =
                    snapshot.topic_statuses().values().cloned().collect();
                topic_states.sort_by(|left, right| left.topic.cmp(&right.topic));

                let mut services: Vec<ServiceStatusInfo> =
                    snapshot.services().values().cloned().collect();
                services.sort_by(|left, right| left.service.cmp(&right.service));

                SubscriptionSnapshot {
                    present: true,
                    topics: snapshot.topics().to_vec(),
                    fields: handle.fields.clone(),
                    is_active: snapshot.has_active_topics() && handle.claim.is_some(),
                    all_failed: !snapshot.has_active_topics() && !snapshot.failures().is_empty(),
                    messages_received,
                    dropped_batches,
                    batches_sent,
                    slow_consumer,
                    data_loss_events,
                    last_message_us,
                    last_data_loss_us,
                    failures: snapshot.failures().to_vec(),
                    topic_states,
                    session: snapshot.session().clone(),
                    services,
                    admin: snapshot.admin().clone(),
                    events: snapshot.events().iter().cloned().collect(),
                    effective_overflow_policy: match handle
                        .overflow_policy
                        .unwrap_or(OverflowPolicy::DropNewest)
                    {
                        OverflowPolicy::DropNewest => "drop_newest".to_string(),
                        OverflowPolicy::Block => "block".to_string(),
                    },
                }
            }
            None => SubscriptionSnapshot::default(),
        }
    }

    fn snapshot(&self, py: Python<'_>) -> SubscriptionSnapshot {
        let stream = self.stream.clone();
        py.detach(move || Self::snapshot_from_stream(&stream))
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pymethods)]
#[pymethods]
impl PySubscription {
    /// Async iterator protocol.
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Get next batch of data.
    /// Only locks the rx, not the stream - so add/remove can run concurrently.
    ///
    /// Returns an xbbg ArrowRecordBatch on success.
    /// Raises a Python exception (BlpRequestError, BlpInternalError, etc.) on error.
    /// Raises StopAsyncIteration when the subscription is closed.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.rx.clone();
        let close_signal = self.close_signal.clone();
        let mut engine_shutdown_rx = self.engine_shutdown.clone();

        future_into_py(py, async move {
            let mut close_rx = close_signal.subscribe();
            let item = {
                let mut guard = rx.lock().await;
                let rx_ref = guard
                    .as_mut()
                    .ok_or_else(|| PyStopAsyncIteration::new_err("subscription closed"))?;
                tokio::select! {
                    item = rx_ref.recv() => Ok(item),
                    _ = wait_for_subscription_close(&mut close_rx) => Err(()),
                    _ = engine_shutdown_rx.changed() => Err(()),
                }
            };

            match item {
                Ok(Some(Ok(update))) => {
                    try_attach_or_suspend(|py| {
                        subscription_update_to_arrow_record_batch(py, update)
                    })
                    .await
                }
                Ok(Some(Err(blp_err))) => Err(blp_error_to_pyerr(blp_err)),
                Ok(None) => Err(PyStopAsyncIteration::new_err("subscription ended")),
                Err(()) => Err(PyStopAsyncIteration::new_err("subscription closed")),
            }
        })
    }

    /// Get next update as a Python dict without building Arrow.
    fn __anext_tick_dict__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.rx.clone();
        let close_signal = self.close_signal.clone();
        let mut engine_shutdown_rx = self.engine_shutdown.clone();

        future_into_py(py, async move {
            let mut close_rx = close_signal.subscribe();
            let item = {
                let mut guard = rx.lock().await;
                let rx_ref = guard
                    .as_mut()
                    .ok_or_else(|| PyStopAsyncIteration::new_err("subscription closed"))?;
                tokio::select! {
                    item = rx_ref.recv() => Ok(item),
                    _ = wait_for_subscription_close(&mut close_rx) => Err(()),
                    _ = engine_shutdown_rx.changed() => Err(()),
                }
            };

            match item {
                Ok(Some(Ok(update))) => {
                    try_attach_or_suspend(|py| subscription_update_to_pydict(py, update)).await
                }
                Ok(Some(Err(blp_err))) => Err(blp_error_to_pyerr(blp_err)),
                Ok(None) => Err(PyStopAsyncIteration::new_err("subscription ended")),
                Err(()) => Err(PyStopAsyncIteration::new_err("subscription closed")),
            }
        })
    }

    /// Add tickers to the subscription dynamically.
    /// Iteration can continue while Bloomberg work is in flight.
    #[pyo3(signature = (tickers))]
    fn add<'py>(&self, py: Python<'py>, tickers: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        let ops = self.ops.clone();

        debug!(tickers = ?tickers, "PySubscription: adding tickers");

        future_into_py(py, async move {
            let _op_guard = ops.lock().await;

            let pending = {
                let guard = stream.lock().await;
                let handle = guard
                    .as_ref()
                    .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;
                handle.prepare_add(tickers)?
            };

            let Some(pending) = pending else {
                return Ok(());
            };

            // Add new topics using the same stream sender
            let (new_keys, new_metrics) = pending
                .command
                .add_topics(
                    pending.service.clone(),
                    pending.new_topics.clone(),
                    pending.fields.clone(),
                    pending.all_fields,
                    pending.options.clone(),
                    pending.flush_threshold,
                    pending.overflow_policy,
                    pending.tx.clone(),
                    pending.status.clone(),
                )
                .await
                .map_err(blp_async_error_to_pyerr)?;

            let mut guard = stream.lock().await;
            let handle = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;
            handle.apply_add(&pending.new_topics, new_keys, new_metrics);

            Ok(())
        })
    }

    /// Remove tickers from the subscription dynamically.
    /// Iteration can continue while Bloomberg work is in flight.
    #[pyo3(signature = (tickers))]
    fn remove<'py>(&self, py: Python<'py>, tickers: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        let ops = self.ops.clone();

        debug!(tickers = ?tickers, "PySubscription: removing tickers");

        future_into_py(py, async move {
            let _op_guard = ops.lock().await;

            let pending = {
                let guard = stream.lock().await;
                let handle = guard
                    .as_ref()
                    .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;
                handle.prepare_remove(tickers)?
            };

            let Some(pending) = pending else {
                return Ok(());
            };

            pending
                .command
                .unsubscribe(pending.keys.clone())
                .await
                .map_err(blp_async_error_to_pyerr)?;

            let mut guard = stream.lock().await;
            let handle = guard
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("subscription closed"))?;
            handle.apply_remove(&pending.topics);
            Ok(())
        })
    }

    /// Get the currently subscribed tickers.
    #[getter]
    fn tickers(&self, py: Python<'_>) -> Vec<String> {
        self.snapshot(py).topics
    }

    /// Get the subscribed fields.
    #[getter]
    fn fields(&self, py: Python<'_>) -> Vec<String> {
        self.snapshot(py).fields
    }

    /// Check if the subscription is still active.
    #[getter]
    fn is_active(&self, py: Python<'_>) -> bool {
        self.snapshot(py).is_active
    }

    #[getter]
    fn all_failed(&self, py: Python<'_>) -> bool {
        self.snapshot(py).all_failed
    }

    /// Get subscription metrics.
    ///
    /// Returns a dict with keys:
    /// - messages_received: int — total messages received from Bloomberg
    /// - dropped_batches: int — batches dropped due to overflow
    /// - batches_sent: int — batches successfully sent to Python
    /// - slow_consumer: bool — True if DATALOSS was received
    #[getter]
    fn stats(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let snapshot = self.snapshot(py);
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("messages_received", snapshot.messages_received)?;
        dict.set_item("dropped_batches", snapshot.dropped_batches)?;
        dict.set_item("batches_sent", snapshot.batches_sent)?;
        dict.set_item("slow_consumer", snapshot.slow_consumer)?;
        dict.set_item("data_loss_events", snapshot.data_loss_events)?;
        dict.set_item("last_message_us", snapshot.last_message_us)?;
        dict.set_item("last_data_loss_us", snapshot.last_data_loss_us)?;
        dict.set_item(
            "effective_overflow_policy",
            snapshot.effective_overflow_policy,
        )?;
        Ok(dict.into())
    }

    #[getter]
    fn session_status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let snapshot = self.snapshot(py);
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("state", snapshot.session.state.as_str())?;
        dict.set_item("last_change_us", snapshot.session.last_change_us)?;
        dict.set_item("disconnect_count", snapshot.session.disconnect_count)?;
        dict.set_item("reconnect_count", snapshot.session.reconnect_count)?;
        Ok(dict.into())
    }

    #[getter]
    fn admin_status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let snapshot = self.snapshot(py);
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item(
            "slow_consumer_warning_active",
            snapshot.admin.slow_consumer_warning_active,
        )?;
        dict.set_item(
            "slow_consumer_warning_count",
            snapshot.admin.slow_consumer_warning_count,
        )?;
        dict.set_item(
            "slow_consumer_cleared_count",
            snapshot.admin.slow_consumer_cleared_count,
        )?;
        dict.set_item("data_loss_count", snapshot.admin.data_loss_count)?;
        dict.set_item("last_warning_us", snapshot.admin.last_warning_us)?;
        dict.set_item("last_cleared_us", snapshot.admin.last_cleared_us)?;
        dict.set_item("last_data_loss_us", snapshot.admin.last_data_loss_us)?;
        Ok(dict.into())
    }

    #[getter]
    fn service_status(&self, py: Python<'_>) -> Vec<(String, bool, i64)> {
        self.snapshot(py)
            .services
            .into_iter()
            .map(|service| (service.service, service.up, service.last_change_us))
            .collect()
    }

    #[getter]
    fn topic_states(&self, py: Python<'_>) -> Vec<(String, String, i64)> {
        self.snapshot(py)
            .topic_states
            .into_iter()
            .map(|topic| {
                (
                    topic.topic,
                    topic.state.as_str().to_string(),
                    topic.last_change_us,
                )
            })
            .collect()
    }

    #[getter]
    fn events(&self, py: Python<'_>) -> Vec<SubscriptionEventTuple> {
        self.snapshot(py)
            .events
            .into_iter()
            .map(|event| {
                (
                    event.at_us,
                    event.category.as_str().to_string(),
                    event.level.as_str().to_string(),
                    event.message_type,
                    event.topic,
                    event.detail,
                )
            })
            .collect()
    }

    #[getter]
    fn failed_tickers(&self, py: Python<'_>) -> Vec<String> {
        self.snapshot(py)
            .failures
            .into_iter()
            .map(|failure| failure.topic)
            .collect()
    }

    #[getter]
    fn failures(&self, py: Python<'_>) -> Vec<(String, String, String)> {
        self.snapshot(py)
            .failures
            .into_iter()
            .map(|failure| {
                (
                    failure.topic,
                    failure.reason,
                    failure.kind.as_str().to_string(),
                )
            })
            .collect()
    }

    /// Unsubscribe and close the stream.
    ///
    /// If drain=True, returns remaining buffered batches before closing.
    #[pyo3(signature = (drain = false))]
    fn unsubscribe<'py>(&self, py: Python<'py>, drain: bool) -> PyResult<Bound<'py, PyAny>> {
        let stream_arc = self.stream.clone();
        let rx_arc = self.rx.clone();
        let ops = self.ops.clone();
        let close_signal = self.close_signal.clone();

        debug!(drain = drain, "PySubscription: unsubscribing");

        future_into_py(py, async move {
            let _op_guard = ops.lock().await;
            let _ = close_signal.send(true);

            // Take the stream handle first so add/remove operations stop immediately.
            let handle = {
                let mut guard = stream_arc.lock().await;
                guard.take()
            };

            let mut remaining = Vec::new();

            // Drain remaining updates if requested (skip errors)
            if drain {
                let rx = {
                    let mut guard = rx_arc.lock().await;
                    guard.take()
                };
                if let Some(mut rx) = rx {
                    while let Ok(item) = rx.try_recv() {
                        if let Ok(update) = item {
                            remaining.push(update);
                        }
                    }
                }
            }

            // Unsubscribe from Bloomberg. Only clear status after Bloomberg accepts
            // termination so SessionClaim::Drop can safely return the worker to the pool.
            if let Some(mut h) = handle {
                if let Some(claim) = h.claim.take() {
                    let keys = h.status.load().keys().to_vec();
                    if !keys.is_empty() {
                        claim
                            .unsubscribe(keys)
                            .await
                            .map_err(blp_async_error_to_pyerr)?;
                        h.status.update(|state| {
                            state.clear_active();
                        });
                    }
                    // claim drops here; cleared status means a clean worker lease can be reused.
                }
            }

            if !remaining.is_empty() {
                try_attach_or_suspend(|py| {
                    let list = pyo3::types::PyList::empty(py);
                    for update in remaining {
                        let py_batch = subscription_update_to_arrow_record_batch(py, update)?;
                        list.append(py_batch)?;
                    }
                    Ok(list.into_any().unbind())
                })
                .await
            } else {
                try_attach_or_suspend(|py| Ok(py.None())).await
            }
        })
    }

    /// Context manager entry.
    fn __aenter__<'py>(slf: PyRef<'py, Self>) -> PyRef<'py, Self> {
        slf
    }

    /// Context manager exit - unsubscribes automatically.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_val: Option<Bound<'py, PyAny>>,
        _exc_tb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.unsubscribe(py, false)
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let snapshot = self.snapshot(py);
        if snapshot.present {
            format!(
                "Subscription(tickers={:?}, fields={:?}, active={})",
                snapshot.topics, snapshot.fields, snapshot.is_active
            )
        } else {
            "Subscription(closed)".to_string()
        }
    }
}

fn exchange_info_to_pydict(py: Python<'_>, info: &ExchangeInfo) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("ticker", &info.ticker)?;
    dict.set_item("mic", info.mic.clone())?;
    dict.set_item("exch_code", info.exch_code.clone())?;
    dict.set_item("timezone", &info.timezone)?;
    dict.set_item("utc_offset", info.utc_offset)?;
    dict.set_item("source", info.source.as_str())?;
    dict.set_item("day", info.sessions.day.clone())?;
    dict.set_item("allday", info.sessions.allday.clone())?;
    dict.set_item("pre", info.sessions.pre.clone())?;
    dict.set_item("post", info.sessions.post.clone())?;
    dict.set_item("am", info.sessions.am.clone())?;
    dict.set_item("pm", info.sessions.pm.clone())?;
    Ok(dict.into_any().unbind())
}

fn market_info_to_pydict(py: Python<'_>, info: &MarketInfo) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("exch", info.exch.clone())?;
    dict.set_item("tz", info.tz.clone())?;
    dict.set_item("freq", info.freq.clone())?;
    dict.set_item("is_fut", info.is_fut)?;
    Ok(dict.into_any().unbind())
}

fn subscription_update_to_arrow_record_batch(
    py: Python<'_>,
    update: SubscriptionUpdate,
) -> PyResult<Py<PyAny>> {
    let batch = subscription_update_to_record_batch(&update).map_err(blp_error_to_pyerr)?;
    native_arrow::record_batch_to_arrow_record_batch(py, batch)
}

fn date32_to_py(py: Python<'_>, days: i32) -> PyResult<Py<PyAny>> {
    let Some(epoch) = NaiveDate::from_ymd_opt(1970, 1, 1) else {
        return Ok(py.None());
    };
    let Some(date) = epoch.checked_add_signed(chrono::Duration::days(days as i64)) else {
        return Ok(py.None());
    };
    Ok(
        PyDate::new(py, date.year(), date.month() as u8, date.day() as u8)?
            .into_any()
            .unbind(),
    )
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

fn timestamp_micros_to_py(py: Python<'_>, micros: i64) -> PyResult<Py<PyAny>> {
    let Some(dt) = DateTime::from_timestamp_micros(micros) else {
        return Ok(py.None());
    };
    let utc = PyTzInfo::utc(py)?;
    Ok(PyDateTime::new(
        py,
        dt.year(),
        dt.month() as u8,
        dt.day() as u8,
        dt.hour() as u8,
        dt.minute() as u8,
        dt.second() as u8,
        dt.timestamp_subsec_micros(),
        Some(&utc),
    )?
    .into_any()
    .unbind())
}

fn subscription_update_to_pydict(
    py: Python<'_>,
    update: SubscriptionUpdate,
) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item(
        "timestamp",
        timestamp_micros_to_py(py, update.timestamp_us)?,
    )?;
    dict.set_item("topic", update.topic.as_ref())?;
    for field in update.values.iter() {
        let Some(meta) = update.layout.fields.get(field.index as usize) else {
            continue;
        };
        match &field.value {
            UpdateValue::Null => dict.set_item(meta.name.as_ref(), py.None())?,
            UpdateValue::Bool(v) => dict.set_item(meta.name.as_ref(), *v)?,
            UpdateValue::I32(v) => dict.set_item(meta.name.as_ref(), *v)?,
            UpdateValue::I64(v) => dict.set_item(meta.name.as_ref(), *v)?,
            UpdateValue::F64(v) => dict.set_item(meta.name.as_ref(), *v)?,
            UpdateValue::Str(v) => dict.set_item(meta.name.as_ref(), v.as_ref())?,
            UpdateValue::Date32(v) => dict.set_item(meta.name.as_ref(), date32_to_py(py, *v)?)?,
            UpdateValue::Time64Micros(v) => {
                dict.set_item(meta.name.as_ref(), time64_micros_to_py(py, *v)?)?
            }
            UpdateValue::TimestampMicros(v) => {
                dict.set_item(meta.name.as_ref(), timestamp_micros_to_py(py, *v)?)?
            }
        }
    }
    Ok(dict.into_any().unbind())
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn version() -> String {
    xbbg_core::version().to_string()
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn sdk_version() -> (i32, i32, i32, i32) {
    xbbg_core::sdk_version()
}

#[pymodule]
#[pyo3(name = "_core")]
fn _core(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize zero-GIL logging: tracing -> AtomicLevelFilter -> non-blocking stderr
    //
    // Python controls via xbbg.set_log_level("debug").
    // Developers override with RUST_LOG=xbbg_core=trace,xbbg_async=debug.
    xbbg_log::init();

    // Initialize tokio runtime for pyo3-async-runtimes (future_into_py).
    //
    // pyo3-async-runtimes creates its own runtime on first use via get_runtime()
    // if we don't call init_with_runtime(). This is fine — the Engine also has
    // its own runtime for worker threads. The pyo3-async-runtimes runtime only
    // handles the Python↔Rust async bridge (future_into_py scheduling), while
    // the Engine's runtime handles Bloomberg SDK I/O.

    info!("xbbg._core module initialized");

    // Version from git describe (e.g., "v1.0.0" or "v1.0.0-5-g1a2b3c4")
    // Strip the leading 'v' for Python version string
    let git_version = env!("VERGEN_GIT_DESCRIBE");
    let pkg_version = git_version.strip_prefix('v').unwrap_or(git_version);
    m.add("__version__", pkg_version)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(sdk_version, m)?)?;
    native_arrow::register(m)?;
    m.add_class::<EntitlementReport>()?;
    m.add_class::<PyEngineConfig>()?;
    m.add_class::<PySubscription>()?;
    m.add_class::<PyEngine>()?;

    // Register exception classes for use from Python
    m.add("BlpError", _py.get_type::<BlpErrorBase>())?;
    m.add("BlpSessionError", _py.get_type::<BlpSessionError>())?;
    m.add("BlpRequestError", _py.get_type::<BlpRequestError>())?;
    m.add("BlpLimitError", _py.get_type::<BlpLimitError>())?;
    m.add("BlpSecurityError", _py.get_type::<BlpSecurityError>())?;
    m.add("BlpFieldError", _py.get_type::<BlpFieldError>())?;
    m.add("BlpValidationError", _py.get_type::<BlpValidationError>())?;
    m.add("BlpTimeoutError", _py.get_type::<BlpTimeoutError>())?;
    m.add("BlpInternalError", _py.get_type::<BlpInternalError>())?;

    // Logging control (zero-GIL)
    m.add_function(wrap_pyfunction!(set_log_level, m)?)?;
    m.add_function(wrap_pyfunction!(get_log_level, m)?)?;
    m.add_function(wrap_pyfunction!(enable_sdk_logging, m)?)?;

    // Register ext functions (date, pivot, ticker, futures, cdx, currency utilities)
    ext::register_ext_module(m)?;

    // Register markets functions (session derivation, market rules, timezone inference)
    markets::register(m)?;

    // Register recipe functions (12 high-level Bloomberg workflows)
    recipes::register_recipes_module(m)?;

    Ok(())
}

// =============================================================================
// Logging control — Python-facing functions
// =============================================================================

/// Set the Rust log level.
///
/// Accepts: "trace", "debug", "info", "warn", "error".
/// Default is "warn" (quiet for end users).
///
/// This sets an atomic integer — no GIL is held on the logging hot path.
/// For per-crate control, use the RUST_LOG environment variable instead.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn set_log_level(level: &str) -> PyResult<()> {
    let lvl = xbbg_log::parse_level(level).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid log level '{}'. Expected: trace, debug, info, warn, error",
            level
        ))
    })?;
    xbbg_log::set_level(lvl);
    Ok(())
}

/// Get the current Rust log level as a string.
#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn get_log_level() -> &'static str {
    match xbbg_log::current_level() {
        xbbg_log::Level::TRACE => "trace",
        xbbg_log::Level::DEBUG => "debug",
        xbbg_log::Level::INFO => "info",
        xbbg_log::Level::WARN => "warn",
        xbbg_log::Level::ERROR => "error",
    }
}

#[cfg_attr(feature = "stub-gen", gen_stub_pyfunction)]
#[pyfunction]
fn enable_sdk_logging(level: &str) -> PyResult<()> {
    let lvl: xbbg_async::sdk_logging::SdkLogLevel = level
        .parse()
        .map_err(|e: String| pyo3::exceptions::PyValueError::new_err(e))?;
    xbbg_async::sdk_logging::register_sdk_logging(lvl);
    Ok(())
}

#[cfg(feature = "stub-gen")]
define_stub_info_gatherer!(stub_info);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use xbbg_async::engine::ExtractorType;

    fn metrics(
        messages_received: u64,
        dropped_batches: u64,
        batches_sent: u64,
        slow_consumer: bool,
    ) -> Arc<SubscriptionMetrics> {
        Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(messages_received)),
            dropped_batches: Arc::new(AtomicU64::new(dropped_batches)),
            batches_sent: Arc::new(AtomicU64::new(batches_sent)),
            slow_consumer: Arc::new(AtomicBool::new(slow_consumer)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        })
    }

    #[test]
    fn subscription_metrics_totals_only_counts_active_entries() {
        let mut metrics_map = SubscriptionMetricsMap::new();
        metrics_map.insert(10, metrics(5, 1, 4, false));
        metrics_map.insert(11, metrics(7, 2, 6, true));

        metrics_map.remove(&10);

        assert_eq!(
            subscription_metrics_totals(&metrics_map),
            (7, 2, 6, true, 0, 0, 0)
        );
    }

    #[test]
    fn py_engine_config_defaults_include_auth_defaults() {
        let config = PyEngineConfig::new(None).expect("default config");
        assert_eq!(config.auth_method, None);
        assert_eq!(config.num_start_attempts, 3);
        assert!(config.auto_restart_on_disconnection);
        assert!(!config.shard_requests);
        assert_eq!(config.shard_threshold, 20);
        assert_eq!(config.shard_chunk_size, 16);
        assert_eq!(config.shard_max_concurrent, 4);
    }

    #[test]
    fn py_engine_config_maps_sharding_fields() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("shard_requests", true)
                .expect("shard_requests");
            kwargs
                .set_item("shard_threshold", 5)
                .expect("shard_threshold");
            kwargs
                .set_item("shard_chunk_size", 3)
                .expect("shard_chunk_size");
            kwargs
                .set_item("shard_max_concurrent", 2)
                .expect("shard_max_concurrent");

            let config = PyEngineConfig::new(Some(&kwargs)).expect("sharding config");
            let engine_config: EngineConfig = (&config).try_into().expect("engine config");

            assert!(engine_config.shard_requests);
            assert_eq!(engine_config.shard_threshold, 5);
            assert_eq!(engine_config.shard_chunk_size, 3);
            assert_eq!(engine_config.shard_max_concurrent, 2);
        });
    }

    #[test]
    fn py_engine_config_maps_manual_auth_to_engine_config() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("auth_method", "manual")
                .expect("auth_method");
            kwargs.set_item("app_name", "my-app").expect("app_name");
            kwargs.set_item("user_id", "123456").expect("user_id");
            kwargs
                .set_item("ip_address", "10.0.0.1")
                .expect("ip_address");

            let config = PyEngineConfig::new(Some(&kwargs)).expect("manual auth config");
            let engine_config: EngineConfig = (&config).try_into().expect("engine config");

            assert_eq!(
                engine_config.auth,
                Some(AuthConfig::Manual {
                    app_name: "my-app".to_string(),
                    user_id: "123456".to_string(),
                    ip_address: "10.0.0.1".to_string(),
                })
            );
        });
    }

    #[test]
    fn py_engine_config_rejects_missing_auth_fields() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs.set_item("auth_method", "app").expect("auth_method");

            let config = PyEngineConfig::new(Some(&kwargs)).expect("partial auth config");
            let err = match EngineConfig::try_from(&config) {
                Ok(_) => panic!("missing app_name should fail"),
                Err(err) => err,
            };
            assert!(err.to_string().contains("app_name is required"));
        });
    }

    #[test]
    fn py_engine_config_rejects_zero_subscription_stream_capacity() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.subscription_stream_capacity = 0;
        let err = match EngineConfig::try_from(&config) {
            Ok(_) => panic!("zero capacity should fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("subscription_stream_capacity must be greater than zero"));
    }

    #[test]
    fn py_engine_config_rejects_negative_keep_alive_inactivity() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.keep_alive_inactivity_ms = Some(-1);
        let err = match EngineConfig::try_from(&config) {
            Ok(_) => panic!("negative keep-alive should fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("keep_alive_inactivity_ms must be non-negative"));
    }

    #[test]
    fn py_engine_config_rejects_out_of_range_watermarks() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.slow_consumer_hi_water_mark = Some(1.5);
        let err = match EngineConfig::try_from(&config) {
            Ok(_) => panic!("hi watermark above 1.0 should fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("slow_consumer_hi_water_mark must be in 0.0..=1.0"));

        // The hi watermark accepts exactly 1.0; the lo watermark is half-open
        // and rejects it (mirrors napi).
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.slow_consumer_hi_water_mark = Some(1.0);
        config.slow_consumer_lo_water_mark = Some(1.0);
        let err = match EngineConfig::try_from(&config) {
            Ok(_) => panic!("lo watermark of 1.0 should fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("slow_consumer_lo_water_mark must be in 0.0..1.0"));
    }

    #[test]
    fn py_engine_config_rejects_negative_tls_timeouts() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.tls_handshake_timeout_ms = Some(-5);
        let err = match EngineConfig::try_from(&config) {
            Ok(_) => panic!("negative tls timeout should fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains(
            "tls_handshake_timeout_ms must be a non-negative integer number of milliseconds"
        ));
    }

    #[test]
    fn build_auth_config_accepts_directory_alias() {
        let mut config = PyEngineConfig::new(None).expect("default config");
        config.auth_method = Some("directory".to_string());
        config.dir_property = Some("mail".to_string());
        assert_eq!(
            build_auth_config(&config).expect("directory auth"),
            Some(AuthConfig::Directory {
                property_name: "mail".to_string(),
            })
        );
    }

    #[test]
    fn build_auth_config_supports_all_auth_methods() {
        let mut config = PyEngineConfig::new(None).expect("default config");

        config.auth_method = Some("user".to_string());
        assert_eq!(
            build_auth_config(&config).expect("user auth"),
            Some(AuthConfig::User)
        );

        config.auth_method = Some("app".to_string());
        config.app_name = Some("app-name".to_string());
        assert_eq!(
            build_auth_config(&config).expect("app auth"),
            Some(AuthConfig::App {
                app_name: "app-name".to_string(),
            })
        );

        config.auth_method = Some("userapp".to_string());
        assert_eq!(
            build_auth_config(&config).expect("userapp auth"),
            Some(AuthConfig::UserApp {
                app_name: "app-name".to_string(),
            })
        );

        config.auth_method = Some("dir".to_string());
        config.dir_property = Some("mail=jane@example.com".to_string());
        assert_eq!(
            build_auth_config(&config).expect("dir auth"),
            Some(AuthConfig::Directory {
                property_name: "mail=jane@example.com".to_string(),
            })
        );

        config.auth_method = Some("token".to_string());
        config.token = Some("tok-123".to_string());
        assert_eq!(
            build_auth_config(&config).expect("token auth"),
            Some(AuthConfig::Token {
                token: "tok-123".to_string(),
            })
        );
    }
    #[test]
    fn core_module_registration_exposes_public_names() {
        Python::initialize();
        Python::attach(|py| {
            let module = pyo3::types::PyModule::new(py, "_core").expect("module");
            _core(py, &module).expect("register module");

            for name in [
                "__version__",
                "version",
                "sdk_version",
                "PyEngine",
                "PyEngineConfig",
                "PySubscription",
                "ArrowTable",
                "ArrowRecordBatch",
                "ArrowSchema",
                "ArrowField",
                "EntitlementReport",
                "BlpError",
                "BlpSessionError",
                "BlpRequestError",
                "BlpLimitError",
                "BlpSecurityError",
                "BlpFieldError",
                "BlpValidationError",
                "BlpTimeoutError",
                "BlpInternalError",
                "set_log_level",
                "get_log_level",
                "enable_sdk_logging",
            ] {
                assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
            }
        });
    }

    #[test]
    fn dict_to_request_params_preserves_all_accepted_keys() {
        Python::initialize();
        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("service", "//blp/refdata").expect("service");
            dict.set_item("operation", "").expect("operation");
            dict.set_item("request_operation", "ReferenceDataRequest")
                .expect("request_operation");
            dict.set_item("request_id", "req-123").expect("request_id");
            dict.set_item("extractor", "refdata").expect("extractor");
            dict.set_item("securities", vec!["IBM US Equity"])
                .expect("securities");
            dict.set_item("security", "IBM US Equity")
                .expect("security");
            dict.set_item("fields", vec!["PX_LAST"]).expect("fields");
            dict.set_item("overrides", vec![("EQY_FUND_CRNCY", "USD")])
                .expect("overrides");
            dict.set_item("elements", vec![("returnEids", "true")])
                .expect("elements");
            dict.set_item(
                "kwargs",
                HashMap::from([("Period".to_string(), "D".to_string())]),
            )
            .expect("kwargs");
            dict.set_item("start_date", "20240101").expect("start_date");
            dict.set_item("end_date", "20240131").expect("end_date");
            dict.set_item("start_datetime", "2024-01-01T09:30:00")
                .expect("start_datetime");
            dict.set_item("end_datetime", "2024-01-01T10:00:00")
                .expect("end_datetime");
            dict.set_item("request_tz", "NY").expect("request_tz");
            dict.set_item("output_tz", "UTC").expect("output_tz");
            dict.set_item("event_type", "TRADE").expect("event_type");
            dict.set_item("event_types", vec!["TRADE", "BID"])
                .expect("event_types");
            dict.set_item("interval", 5_u32).expect("interval");
            dict.set_item("options", vec![("includeConditionCodes", "true")])
                .expect("options");
            dict.set_item(
                "field_types",
                HashMap::from([("PX_LAST".to_string(), "Float64".to_string())]),
            )
            .expect("field_types");
            dict.set_item("include_security_errors", true)
                .expect("include_security_errors");
            dict.set_item("return_eids", true).expect("return_eids");
            dict.set_item("validate_fields", false)
                .expect("validate_fields");
            dict.set_item("search_spec", "price").expect("search_spec");
            dict.set_item("field_ids", vec!["PX_LAST"])
                .expect("field_ids");
            dict.set_item("format", "long_typed").expect("format");

            let params = dict_to_request_params(&dict).expect("request params");

            assert_eq!(params.service, "//blp/refdata");
            assert_eq!(params.operation, "");
            assert_eq!(
                params.request_operation.as_deref(),
                Some("ReferenceDataRequest")
            );
            assert_eq!(params.request_id.as_deref(), Some("req-123"));
            assert_eq!(params.extractor, ExtractorType::RefData);
            assert!(params.extractor_set);
            assert_eq!(
                params.securities.as_deref(),
                Some(&["IBM US Equity".to_string()][..])
            );
            assert_eq!(params.security.as_deref(), Some("IBM US Equity"));
            assert_eq!(params.fields.as_deref(), Some(&["PX_LAST".to_string()][..]));
            assert_eq!(
                params.overrides.as_deref(),
                Some(&[("EQY_FUND_CRNCY".to_string(), "USD".to_string())][..])
            );
            assert_eq!(
                params.elements.as_deref(),
                Some(&[("returnEids".to_string(), "true".to_string())][..])
            );
            assert_eq!(
                params
                    .kwargs
                    .as_ref()
                    .and_then(|values| values.get("Period")),
                Some(&"D".to_string())
            );
            assert_eq!(params.start_date.as_deref(), Some("20240101"));
            assert_eq!(params.end_date.as_deref(), Some("20240131"));
            assert_eq!(
                params.start_datetime.as_deref(),
                Some("2024-01-01T09:30:00")
            );
            assert_eq!(params.end_datetime.as_deref(), Some("2024-01-01T10:00:00"));
            assert_eq!(params.request_tz.as_deref(), Some("NY"));
            assert_eq!(params.output_tz.as_deref(), Some("UTC"));
            assert_eq!(params.event_type.as_deref(), Some("TRADE"));
            assert_eq!(
                params.event_types.as_deref(),
                Some(&["TRADE".to_string(), "BID".to_string()][..])
            );
            assert_eq!(params.interval, Some(5));
            assert_eq!(
                params.options.as_deref(),
                Some(&[("includeConditionCodes".to_string(), "true".to_string())][..])
            );
            assert_eq!(
                params
                    .field_types
                    .as_ref()
                    .and_then(|values| values.get("PX_LAST")),
                Some(&"Float64".to_string())
            );
            assert!(params.include_security_errors);
            assert!(params.return_eids);
            assert_eq!(params.validate_fields, Some(false));
            assert_eq!(params.search_spec.as_deref(), Some("price"));
            assert_eq!(
                params.field_ids.as_deref(),
                Some(&["PX_LAST".to_string()][..])
            );
            assert_eq!(params.format.as_deref(), Some("long_typed"));
        });
    }
    #[test]
    fn dict_to_request_params_extracts_request_id() {
        Python::initialize();
        Python::attach(|py| {
            let dict = PyDict::new(py);
            dict.set_item("service", "//blp/refdata").expect("service");
            dict.set_item("operation", "ReferenceDataRequest")
                .expect("operation");
            dict.set_item("request_id", "req-123").expect("request_id");

            let params = dict_to_request_params(&dict).expect("request params");
            assert_eq!(params.request_id.as_deref(), Some("req-123"));
        });
    }

    #[test]
    fn timestamp_micros_to_py_returns_utc_aware_datetime() {
        Python::initialize();
        Python::attach(|py| {
            let value = timestamp_micros_to_py(py, 1_704_067_200_123_456).expect("timestamp");
            let dt = value.bind(py);
            let tzinfo = dt.getattr("tzinfo").expect("tzinfo");
            assert!(tzinfo
                .eq(PyTzInfo::utc(py).expect("utc"))
                .expect("tz equality"));
        });
    }
}
