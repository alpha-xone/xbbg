//! Request worker pool with round-robin dispatch.
//!
//! The pool manages a collection of pre-warmed async-session workers and
//! distributes incoming requests across them using round-robin scheduling.
//! Submissions call straight into the chosen worker (async Bloomberg
//! sessions are thread-safe), so there is no command queue; a single
//! timeout-scanner thread enforces `request_timeout_ms` across all workers.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use arrow_array::RecordBatch;
use tokio::sync::{mpsc, oneshot};

use xbbg_core::BlpError;

use super::worker::{AsyncRequestWorker, RequestTicket};
use super::{BlpAsyncError, EngineConfig, PreparedRequest, WorkerHealth};

/// How often the scanner thread sweeps workers for slow/expired requests.
const TIMEOUT_SCAN_INTERVAL: Duration = Duration::from_secs(1);

/// Polling granularity for scanner shutdown responsiveness.
const TIMEOUT_SCAN_TICK: Duration = Duration::from_millis(100);

/// Cancels the in-flight request when the caller's future is dropped before
/// the reply arrives.
struct RequestCancelGuard {
    worker: Arc<AsyncRequestWorker>,
    ticket: RequestTicket,
    armed: bool,
}

impl RequestCancelGuard {
    fn new(worker: Arc<AsyncRequestWorker>, ticket: RequestTicket) -> Self {
        Self {
            worker,
            ticket,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RequestCancelGuard {
    fn drop(&mut self) {
        if self.armed {
            self.worker.cancel_request(self.ticket);
        }
    }
}

/// Background thread enforcing slow-request warnings and hard timeouts.
struct TimeoutScanner {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl TimeoutScanner {
    fn spawn(workers: Vec<Weak<AsyncRequestWorker>>, request_timeout_ms: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("xbbg-request-timeouts".to_string())
            .spawn(move || {
                let hard_timeout =
                    (request_timeout_ms > 0).then(|| Duration::from_millis(request_timeout_ms));
                let ticks_per_scan =
                    (TIMEOUT_SCAN_INTERVAL.as_millis() / TIMEOUT_SCAN_TICK.as_millis()).max(1);
                loop {
                    for _ in 0..ticks_per_scan {
                        if stop_flag.load(Ordering::Acquire) {
                            return;
                        }
                        std::thread::sleep(TIMEOUT_SCAN_TICK);
                    }
                    for worker in &workers {
                        let Some(worker) = worker.upgrade() else {
                            continue;
                        };
                        for ticket in worker.scan_timeouts(hard_timeout) {
                            worker.timeout_request(ticket, request_timeout_ms);
                        }
                    }
                }
            })
            .ok();
        if thread.is_none() {
            xbbg_log::error!("failed to spawn request timeout scanner thread");
        }
        Self { stop, thread }
    }

    fn signal_stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    fn join(&mut self) {
        self.signal_stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for TimeoutScanner {
    fn drop(&mut self) {
        self.signal_stop();
    }
}

/// Pool of request workers with round-robin dispatch.
///
/// The public surface is limited to construction, health/introspection, and
/// shutdown. Request dispatch is intentionally crate-private because correct
/// preparation depends on [`super::Engine`]-owned schema, field-cache, and
/// intraday-timezone state.
pub struct RequestWorkerPool {
    /// Workers; `Arc` so cancel guards and the scanner can address them
    /// after the submitting borrow ends.
    workers: Vec<Arc<AsyncRequestWorker>>,
    /// Round-robin counter.
    next_worker: AtomicUsize,
    /// Configuration.
    config: Arc<EngineConfig>,
    /// Slow-request / hard-timeout enforcement. `None` only in unit tests.
    scanner: Option<TimeoutScanner>,
}

impl RequestWorkerPool {
    /// Create a new pool with the specified number of workers.
    ///
    /// Each worker owns a pre-warmed asynchronous Bloomberg session; creation
    /// blocks until every session has started (parity with the previous
    /// thread-per-worker design, which also blocked on session startup).
    pub fn new(size: usize, config: Arc<EngineConfig>) -> Result<Self, BlpAsyncError> {
        if size == 0 {
            return Err(BlpAsyncError::ConfigError {
                detail: "request_pool_size must be at least 1".to_string(),
            });
        }

        xbbg_log::info!(pool_size = size, "creating request worker pool");

        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            let worker = AsyncRequestWorker::new(id, config.clone()).map_err(|e| {
                BlpAsyncError::BlpError(BlpError::Internal {
                    detail: format!("failed to spawn worker {}: {}", id, e),
                })
            })?;
            workers.push(Arc::new(worker));
        }

        let scanner = TimeoutScanner::spawn(
            workers.iter().map(Arc::downgrade).collect(),
            config.request_timeout_ms,
        );

        xbbg_log::info!(pool_size = size, "request worker pool ready");

        Ok(Self {
            workers,
            next_worker: AtomicUsize::new(0),
            config,
            scanner: Some(scanner),
        })
    }

    fn next_healthy_worker(&self) -> Result<&Arc<AsyncRequestWorker>, BlpAsyncError> {
        let len = self.workers.len();
        let start = self.next_worker.fetch_add(1, Ordering::Relaxed) % len;

        for offset in 0..len {
            let idx = (start + offset) % len;
            let worker = &self.workers[idx];
            if worker.health() != WorkerHealth::Dead {
                return Ok(worker);
            }
        }

        Err(BlpAsyncError::AllWorkersDown { pool_size: len })
    }

    /// A healthy worker for out-of-band session operations (identity /
    /// entitlement queries). Same round-robin selection as request dispatch.
    pub(crate) fn any_healthy_worker(&self) -> Result<Arc<AsyncRequestWorker>, BlpAsyncError> {
        self.next_healthy_worker().map(Arc::clone)
    }

    fn retry_delay(&self, attempt: usize) -> u64 {
        if attempt == 0 {
            return 0;
        }

        let policy = &self.config.retry_policy;
        let exponent = (attempt - 1) as f64;
        let delay = (policy.initial_delay_ms as f64) * policy.backoff_factor.powf(exponent);
        let bounded = if delay.is_finite() {
            delay.min(policy.max_delay_ms as f64)
        } else {
            policy.max_delay_ms as f64
        };

        bounded.max(0.0).round() as u64
    }

    fn is_retryable(&self, error: &BlpError) -> bool {
        match error {
            BlpError::Internal { detail } => {
                let detail = detail.to_ascii_lowercase();
                detail.contains("session")
                    || detail.contains("connection")
                    || detail.contains("transport")
            }
            _ => false,
        }
    }

    /// Dispatch a prepared request to an available worker and wait for the result.
    pub(crate) async fn request(
        &self,
        request: PreparedRequest,
    ) -> Result<RecordBatch, BlpAsyncError> {
        let params = request.params();
        let max_attempts = 1 + self.config.retry_policy.max_retries as usize;
        let mut last_error = None;

        for attempt in 0..max_attempts {
            if attempt > 0 {
                let delay = self.retry_delay(attempt);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                xbbg_log::info!(attempt = attempt, delay_ms = delay, "retrying request");
            }

            let worker = match self.next_healthy_worker() {
                Ok(worker) => Arc::clone(worker),
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };

            let (reply_tx, reply_rx) = oneshot::channel();

            xbbg_log::debug!(
                worker_id = worker.id,
                service = %params.service,
                operation = %params.operation,
                attempt = attempt,
                "dispatching request"
            );

            // Failures inside submit are routed through the reply channel;
            // a ticket means the request is in flight and cancellable.
            let ticket = worker.submit(request.clone(), reply_tx).await;
            let mut cancel_guard =
                ticket.map(|ticket| RequestCancelGuard::new(Arc::clone(&worker), ticket));

            let reply_result = reply_rx.await;
            if let Some(guard) = &mut cancel_guard {
                guard.disarm();
            }

            match reply_result {
                Ok(Ok(batch)) => return Ok(batch),
                Ok(Err(error)) if self.is_retryable(&error) && attempt + 1 < max_attempts => {
                    last_error = Some(BlpAsyncError::BlpError(error));
                    continue;
                }
                Ok(Err(error)) => return Err(BlpAsyncError::BlpError(error)),
                Err(_) if attempt + 1 < max_attempts => {
                    last_error = Some(BlpAsyncError::ChannelClosed);
                    continue;
                }
                Err(_) => return Err(BlpAsyncError::ChannelClosed),
            }
        }

        Err(last_error.unwrap_or(BlpAsyncError::ChannelClosed))
    }

    /// Dispatch a prepared streaming request to an available worker.
    ///
    /// Returns a receiver that will receive batches as they arrive.
    pub(crate) async fn request_stream(
        &self,
        request: PreparedRequest,
    ) -> Result<mpsc::Receiver<Result<RecordBatch, BlpError>>, BlpAsyncError> {
        let params = request.params();
        let (stream_tx, stream_rx) = mpsc::channel(self.config.subscription_stream_capacity);

        let worker = self.next_healthy_worker()?;
        xbbg_log::debug!(
            worker_id = worker.id,
            service = %params.service,
            operation = %params.operation,
            "dispatching stream request"
        );
        // Stream errors (including submit failures) arrive through the
        // stream itself; stream requests are not cancel-guarded (parity with
        // the previous design).
        let _ticket = worker.submit_stream(request, stream_tx).await;

        Ok(stream_rx)
    }

    /// Get the number of workers in the pool.
    pub fn size(&self) -> usize {
        self.workers.len()
    }

    pub fn worker_health(&self) -> Vec<(usize, WorkerHealth)> {
        self.workers
            .iter()
            .map(|worker| (worker.id, worker.health()))
            .collect()
    }

    /// Introspect a service's schema via a worker.
    pub async fn introspect_schema(
        &self,
        service: String,
    ) -> Result<crate::schema::ServiceSchema, BlpAsyncError> {
        let worker = self.next_healthy_worker()?;
        worker
            .introspect_schema(&service)
            .await
            .map_err(BlpAsyncError::BlpError)
    }

    /// Signal shutdown to all workers (non-blocking).
    ///
    /// Sessions begin stopping asynchronously; used by Drop to avoid blocking
    /// during interpreter shutdown.
    pub fn signal_shutdown(&self) {
        xbbg_log::info!(
            pool_size = self.workers.len(),
            "signaling request pool shutdown"
        );
        if let Some(scanner) = &self.scanner {
            scanner.signal_stop();
        }
        for worker in &self.workers {
            worker.signal_shutdown();
        }
    }

    /// Graceful shutdown - waits for all workers' sessions to stop (blocking).
    ///
    /// Use this for clean shutdown when you can afford to wait.
    pub fn shutdown_blocking(&mut self) {
        xbbg_log::info!(
            pool_size = self.workers.len(),
            "shutting down request pool (blocking)"
        );
        if let Some(scanner) = &mut self.scanner {
            scanner.join();
        }
        for worker in &self.workers {
            worker.shutdown_blocking();
        }
    }
}

impl Drop for RequestWorkerPool {
    fn drop(&mut self) {
        // Non-blocking: signal sessions to stop; AsyncSession::drop completes
        // the (already-initiated, hence brief) stop before destroying.
        self.signal_shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::RetryPolicy;

    fn pool_with_retry_policy(retry_policy: RetryPolicy) -> RequestWorkerPool {
        let config = Arc::new(EngineConfig {
            retry_policy,
            ..EngineConfig::default()
        });

        RequestWorkerPool {
            workers: Vec::new(),
            next_worker: AtomicUsize::new(0),
            config,
            scanner: None,
        }
    }

    #[test]
    fn retry_delay_uses_exponential_backoff_and_max_delay() {
        let pool = pool_with_retry_policy(RetryPolicy {
            max_retries: 4,
            initial_delay_ms: 100,
            backoff_factor: 2.5,
            max_delay_ms: 600,
        });

        assert_eq!(pool.retry_delay(0), 0);
        assert_eq!(pool.retry_delay(1), 100);
        assert_eq!(pool.retry_delay(2), 250);
        assert_eq!(pool.retry_delay(3), 600);
        assert_eq!(pool.retry_delay(4), 600);
    }

    #[test]
    fn retry_delay_clamps_non_finite_backoff_to_max_delay() {
        let pool = pool_with_retry_policy(RetryPolicy {
            max_retries: 1,
            initial_delay_ms: u64::MAX,
            backoff_factor: f64::INFINITY,
            max_delay_ms: 750,
        });

        assert_eq!(pool.retry_delay(2), 750);
    }

    #[test]
    fn is_retryable_only_matches_transient_internal_errors() {
        let pool = pool_with_retry_policy(RetryPolicy::default());

        assert!(pool.is_retryable(&BlpError::Internal {
            detail: "session connection dropped".to_string(),
        }));
        assert!(pool.is_retryable(&BlpError::Internal {
            detail: "transport reset".to_string(),
        }));
        assert!(!pool.is_retryable(&BlpError::Internal {
            detail: "bad request shape".to_string(),
        }));
        assert!(!pool.is_retryable(&BlpError::InvalidArgument {
            detail: "invalid field".to_string(),
        }));
        assert!(!pool.is_retryable(&BlpError::Timeout));
    }
}
