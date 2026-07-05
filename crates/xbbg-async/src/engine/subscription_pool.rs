//! Subscription session pool with claim/release semantics.
//!
//! Subscription sessions use Bloomberg SDK asynchronous callback mode: idle
//! workers do not poll, and events are dispatched on the SDK dispatcher thread.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use slab::Slab;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use xbbg_core::{AsyncSession, BlpError, CorrelationId, EventType, SubscriptionList};

/// Max wall time for an async open_service reply before we give up.
const SERVICE_OPEN_TIMEOUT_MS: u64 = 10_000;

use super::dispatch::{DispatchKey, SERVICE_OPEN_CID_TAG};
use super::state::{MessageOutcome, SubscriptionMetrics, SubscriptionState, SubscriptionUpdate};
use super::{
    attach_auth_context, build_session_options, BlpAsyncError, EngineConfig, OverflowPolicy,
    SessionLifecycleState, SharedSubscriptionStatus, SlabKey, SubscriptionEventCategory,
    SubscriptionEventLevel, SubscriptionFailureKind, WorkerHealth, SESSION_STARTUP_TIMEOUT_MS,
};

struct PendingServiceOpen {
    cid: i64,
    waiters: Vec<oneshot::Sender<Result<(), BlpError>>>,
}

#[derive(Default)]
struct StartupLatch {
    resolved: bool,
    result: Option<Result<(), BlpError>>,
}

struct SubscriptionWorkerState {
    id: usize,
    subs: Slab<SubscriptionState>,
    config: Arc<EngineConfig>,
    open_services: HashSet<String>,
    pending_cancel: HashSet<SlabKey>,
    status: Option<SharedSubscriptionStatus>,
    last_streams_warn_us: HashMap<SlabKey, i64>,
    pending_service_opens: HashMap<String, PendingServiceOpen>,
}

struct SubscriptionWorkerShared {
    id: usize,
    state: Mutex<SubscriptionWorkerState>,
    health: Arc<AtomicU8>,
    startup: Mutex<StartupLatch>,
    startup_cv: Condvar,
    next_service_open_id: AtomicI64,
}

impl SubscriptionWorkerShared {
    fn new(id: usize, config: Arc<EngineConfig>, health: Arc<AtomicU8>) -> Self {
        Self {
            id,
            state: Mutex::new(SubscriptionWorkerState::new(id, config)),
            health,
            startup: Mutex::new(StartupLatch::default()),
            startup_cv: Condvar::new(),
            next_service_open_id: AtomicI64::new(0),
        }
    }

    fn resolve_startup(&self, result: Result<(), BlpError>) {
        let mut startup = self.startup.lock();
        if !startup.resolved {
            startup.resolved = true;
            startup.result = Some(result);
            self.startup_cv.notify_all();
        }
    }

    fn wait_startup(&self, timeout: Duration) -> Result<(), BlpError> {
        let deadline = Instant::now() + timeout;
        let mut startup = self.startup.lock();
        while startup.result.is_none() {
            if self.startup_cv.wait_until(&mut startup, deadline).timed_out() {
                return Err(BlpError::Timeout);
            }
        }
        startup.result.take().expect("checked above")
    }

    fn next_service_cid(&self) -> i64 {
        SERVICE_OPEN_CID_TAG | self.next_service_open_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }

    fn dispatch_event(self: &Arc<Self>, ev: xbbg_core::Event) {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut state = self.state.lock();
            state.dispatch_event(ev, self);
        }));
        if result.is_err() {
            self.health.store(WorkerHealth::Degraded as u8, Ordering::Release);
            xbbg_log::error!(worker_id = self.id, "panic in subscription SDK callback; event dropped");
        }
    }

    fn check_streams_deactivated(&self) {
        self.state.lock().check_streams_deactivated();
    }

    fn set_status(&self, status: SharedSubscriptionStatus) {
        self.state.lock().status = Some(status);
    }

    fn service_is_open(&self, service: &str) -> bool {
        self.state.lock().open_services.contains(service)
    }

    fn record_service_ready_if_already_open(&self, service: &str, was_open: bool) {
        self.state.lock().record_service_ready_if_already_open(service, was_open);
    }

    fn record_service_open_error(&self, service: &str, error: &BlpError) {
        self.state.lock().record_service_open_error(service, error);
    }

    fn register_service_waiter(
        &self,
        service: &str,
    ) -> (bool, i64, oneshot::Receiver<Result<(), BlpError>>) {
        let (tx, rx) = oneshot::channel();
        let mut state = self.state.lock();
        if state.open_services.contains(service) {
            let _ = tx.send(Ok(()));
            return (false, 0, rx);
        }
        if let Some(open) = state.pending_service_opens.get_mut(service) {
            open.waiters.push(tx);
            return (false, open.cid, rx);
        }
        let cid = self.next_service_cid();
        state.pending_service_opens.insert(
            service.to_string(),
            PendingServiceOpen {
                cid,
                waiters: vec![tx],
            },
        );
        (true, cid, rx)
    }

    fn remove_pending_service_open(&self, service: &str) {
        self.state.lock().pending_service_opens.remove(service);
    }

    fn register_subscriptions(
        &self,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
    ) -> Result<(SubscriptionList, Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpError> {
        self.state.lock().register_subscriptions(
            topics,
            fields,
            all_fields,
            options,
            flush_threshold,
            overflow_policy,
            stream,
        )
    }

    fn cleanup_failed_subscribe(&self, keys: &[SlabKey]) {
        let mut state = self.state.lock();
        for &key in keys {
            if state.subs.contains(key) {
                state.subs.remove(key);
            }
            state.pending_cancel.remove(&key);
            state.last_streams_warn_us.remove(&key);
        }
    }

    fn build_unsubscribe_list(&self, keys: Vec<SlabKey>) -> (SubscriptionList, usize) {
        self.state.lock().build_unsubscribe_list(keys)
    }

}

impl SubscriptionWorkerState {
    fn new(id: usize, config: Arc<EngineConfig>) -> Self {
        Self {
            id,
            subs: Slab::new(),
            config,
            open_services: HashSet::new(),
            pending_cancel: HashSet::new(),
            status: None,
            last_streams_warn_us: HashMap::new(),
            pending_service_opens: HashMap::new(),
        }
    }

    fn record_failure(
        &mut self,
        key: SlabKey,
        reason: String,
        kind: SubscriptionFailureKind,
    ) -> Option<String> {
        let status = self.status.as_ref()?;
        let topic = status.load().topic_for_key(key).map(str::to_string)?;
        status.update(|next| {
            next.record_failure(key, reason.clone(), kind);
        });
        Some(topic)
    }

    fn clear_active_status(&mut self) {
        if let Some(status) = &self.status {
            status.update(|next| next.clear_active());
        }
    }

    fn record_service_ready_if_already_open(&self, service: &str, was_open: bool) {
        if !was_open {
            return;
        }
        if let Some(status) = &self.status {
            status.update(|next| {
                next.record_service_state(
                    service.to_string(),
                    true,
                    "ServiceReady",
                    Some("service available for subscription".to_string()),
                );
            });
        }
    }

    fn record_service_open_error(&self, service: &str, error: &BlpError) {
        if let Some(status) = &self.status {
            let message_type = match error {
                BlpError::Timeout => "ServiceOpenTimeout",
                _ => "ServiceOpenFailure",
            };
            let detail = Some(error.to_string());
            status.update(|next| {
                let already_recorded = next.events().back().is_some_and(|event| {
                    event.category == SubscriptionEventCategory::Service
                        && event.topic.as_deref() == Some(service)
                        && event.message_type == message_type
                });
                if !already_recorded {
                    next.record_service_state(
                        service.to_string(),
                        false,
                        message_type,
                        detail.clone(),
                    );
                }
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn register_subscriptions(
        &mut self,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
    ) -> Result<(SubscriptionList, Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpError> {
        let mut sub_list = SubscriptionList::new();
        let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
        let options_str = options.join(",");
        let mut keys = Vec::with_capacity(topics.len());
        let mut metrics = Vec::with_capacity(topics.len());
        let ft = flush_threshold.unwrap_or(self.config.subscription_flush_threshold);
        let op = overflow_policy.unwrap_or(self.config.overflow_policy);

        for topic in &topics {
            let state = SubscriptionState::with_policy(
                topic.clone(),
                fields.clone(),
                stream.clone(),
                ft,
                op,
                all_fields,
            );
            let metrics_arc = state.metrics.clone();
            let key = self.subs.insert(state);
            if let Some(state) = self.subs.get_mut(key) {
                state.set_topic_id(key as u32);
            }
            let cid = DispatchKey::from_slab_key(key).to_correlation_id();
            if let Err(e) = sub_list.add(topic, &field_refs, &options_str, &cid) {
                xbbg_log::error!(worker_id = self.id, topic = %topic, error = %e, "failed to add topic");
                self.subs.remove(key);
                continue;
            }
            keys.push(key);
            metrics.push(metrics_arc);
            xbbg_log::debug!(worker_id = self.id, topic = %topic, key = key, "subscription added");
        }

        if keys.is_empty() {
            return Err(BlpError::SubscriptionFailure {
                cid: None,
                label: Some("failed to build any subscription entries".to_string()),
            });
        }
        Ok((sub_list, keys, metrics))
    }

    fn build_unsubscribe_list(&mut self, keys: Vec<SlabKey>) -> (SubscriptionList, usize) {
        let mut unsub_list = SubscriptionList::new();
        let mut unsub_count = 0usize;
        for &key in &keys {
            if self.subs.contains(key) {
                let state = &mut self.subs[key];
                state.mark_closing();
                let cid = DispatchKey::from_slab_key(key).to_correlation_id();
                if let Err(e) = unsub_list.add(&state.topic, &[], "", &cid) {
                    xbbg_log::error!(worker_id = self.id, key = key, error = %e, "failed to build unsub list entry");
                } else {
                    unsub_count += 1;
                }
            }
        }

        for &key in &keys {
            if self.subs.contains(key) {
                self.pending_cancel.insert(key);
                if let Some(status) = &self.status {
                    status.update(|next| {
                        let topic = next.mark_topic_unsubscribing(key);
                        next.record_subscription_event(
                            "SubscriptionPendingCancel",
                            topic,
                            None,
                            SubscriptionEventLevel::Info,
                        );
                    });
                }
                xbbg_log::debug!(worker_id = self.id, key = key, "subscription pending cancel");
            }
        }

        (unsub_list, unsub_count)
    }

    fn dispatch_event(&mut self, ev: xbbg_core::Event, shared: &SubscriptionWorkerShared) {
        let et = ev.event_type();
        for msg in ev.iter() {
            match et {
                EventType::SubscriptionData => self.handle_subscription_data(&msg),
                EventType::SubscriptionStatus => self.handle_subscription_status(&msg),
                EventType::SessionStatus => self.handle_session_status(&msg, shared),
                EventType::ServiceStatus => self.handle_service_status(&msg),
                EventType::Admin => self.handle_admin_event(&msg),
                _ => {}
            }
        }
    }

    fn handle_subscription_data(&mut self, msg: &xbbg_core::Message<'_>) {
        for correlation_id in msg.correlation_ids() {
            let Some(dispatch_key) = DispatchKey::from_correlation_id(&correlation_id) else {
                continue;
            };
            let key = dispatch_key.to_slab_key();
            if self.pending_cancel.contains(&key) {
                continue;
            }
            if let Some(state) = self.subs.get_mut(key) {
                match state.on_message(msg) {
                    MessageOutcome::DataLoss => {
                        let topic = state.topic.to_string();
                        if let Some(status) = &self.status {
                            status.update(|next| {
                                next.record_admin_data_loss(
                                    Some(topic.clone()),
                                    Some("subscription data reported DATALOSS".to_string()),
                                );
                            });
                        }
                    }
                    MessageOutcome::Normal { first_message } => {
                        if first_message {
                            let topic = if let Some(status) = &self.status {
                                let topic = status.load().topic_for_key(key).map(str::to_string);
                                status.update(|next| {
                                    next.mark_topic_streaming(key);
                                    next.record_subscription_event(
                                        "SubscriptionStreaming",
                                        topic.clone(),
                                        None,
                                        SubscriptionEventLevel::Info,
                                    );
                                });
                                topic
                            } else {
                                None
                            };
                            xbbg_log::debug!(
                                worker_id = self.id,
                                key = key,
                                topic = ?topic,
                                "subscription entered streaming state"
                            );
                        }
                    }
                }
            }
        }
    }

    fn handle_subscription_status(&mut self, msg: &xbbg_core::Message<'_>) {
        let msg_type_name = msg.message_type();
        let msg_type = msg_type_name.as_str();
        let n = msg.num_correlation_ids();

        // Extract reason description from the message if available
        let reason = msg
            .elements()
            .get_by_str("reason")
            .and_then(|r| r.get_by_str("description"))
            .and_then(|d| d.get_str(0))
            .map(|s| s.to_string());

        for i in 0..n {
            if let Some(correlation_id) = msg.correlation_id(i) {
                let Some(dispatch_key) = DispatchKey::from_correlation_id(&correlation_id) else {
                    continue;
                };
                let key = dispatch_key.to_slab_key();
                match msg_type {
                    "SubscriptionStarted" => {
                        xbbg_log::debug!(
                            worker_id = self.id,
                            key = key,
                            reason = %reason.as_deref().unwrap_or(""),
                            "subscription started"
                        );
                        if let Some(status) = &self.status {
                            status.update(|next| {
                                                                let topic = next.mark_topic_started(key);
                                // Bloomberg sometimes includes partial-permission details in the
                                // `reason` element of SubscriptionStarted (e.g. "only delayed data
                                // authorized"). Surface it via the status event so callers see it.
                                next.record_subscription_event(
                                    "SubscriptionStarted",
                                    topic,
                                    reason.clone(),
                                    SubscriptionEventLevel::Info,
                                );
                                                            });
                        }
                    }
                    "SubscriptionFailure" => {
                        if self.pending_cancel.remove(&key) {
                            // Bloomberg sends SubscriptionFailure (instead of SubscriptionTerminated)
                            // when a subscription is cancelled before it fully starts. Since this was
                            // explicitly requested via unsubscribe(), silently clean up.
                            if self.subs.contains(key) {
                                let mut state = self.subs.remove(key);
                                state.mark_closing();
                                if let Some(status) = &self.status {
                                    status.update(|next| {
                                                                                let topic = next.mark_topic_unsubscribed(key);
                                        next.record_subscription_event(
                                            "SubscriptionCancelled",
                                            topic,
                                            reason.clone(),
                                            SubscriptionEventLevel::Info,
                                        );
                                                                            });
                                }
                            }
                            xbbg_log::debug!(
                                worker_id = self.id,
                                key = key,
                                "pending cancel confirmed via SubscriptionFailure"
                            );
                        } else {
                            let reason_text = reason
                                .clone()
                                .unwrap_or_else(|| "subscription failed".to_string());
                            if self.subs.contains(key) {
                                let mut state = self.subs.remove(key);
                                state.mark_closing();
                                let topic = self
                                    .record_failure(
                                        key,
                                        reason_text.clone(),
                                        SubscriptionFailureKind::Failure,
                                    )
                                    .unwrap_or_else(|| state.topic.to_string());
                                xbbg_log::warn!(
                                    worker_id = self.id,
                                    key = key,
                                    topic = %topic,
                                    reason = %reason_text,
                                    "subscription failed for topic"
                                );
                                if let Some(status) = &self.status {
                                    status.update(|next| {
                                                                                next.record_subscription_event(
                                            "SubscriptionFailure",
                                            Some(topic.clone()),
                                            Some(reason_text.clone()),
                                            SubscriptionEventLevel::Warning,
                                        );
                                                                            });
                                }
                                if self.subs.is_empty() && self.pending_cancel.is_empty() {
                                    state.fail(BlpError::SubscriptionFailure {
                                        cid: None,
                                        label: Some(format!(
                                            "All subscriptions failed; last failure: {} ({})",
                                            topic, reason_text,
                                        )),
                                    });
                                }
                            }
                        }
                    }
                    "SubscriptionTerminated" => {
                        if self.pending_cancel.remove(&key) {
                            // This termination was explicitly requested via unsubscribe().
                            // Silently clean up the slab entry — don't propagate an error.
                            if self.subs.contains(key) {
                                let mut state = self.subs.remove(key);
                                state.mark_closing();
                                if let Some(status) = &self.status {
                                    status.update(|next| {
                                                                                let topic = next.mark_topic_unsubscribed(key);
                                        next.record_subscription_event(
                                            "SubscriptionTerminated",
                                            topic,
                                            reason.clone(),
                                            SubscriptionEventLevel::Info,
                                        );
                                                                            });
                                }
                            }
                            xbbg_log::debug!(
                                worker_id = self.id,
                                key = key,
                                "pending cancel confirmed by Bloomberg"
                            );
                        } else {
                            let reason_text = reason
                                .clone()
                                .unwrap_or_else(|| "subscription terminated".to_string());
                            if self.subs.contains(key) {
                                let mut state = self.subs.remove(key);
                                state.mark_closing();
                                let topic = self
                                    .record_failure(
                                        key,
                                        reason_text.clone(),
                                        SubscriptionFailureKind::Terminated,
                                    )
                                    .unwrap_or_else(|| state.topic.to_string());
                                xbbg_log::warn!(
                                    worker_id = self.id,
                                    key = key,
                                    topic = %topic,
                                    reason = %reason_text,
                                    "subscription terminated for topic"
                                );
                                if let Some(status) = &self.status {
                                    status.update(|next| {
                                                                                next.record_subscription_event(
                                            "SubscriptionTerminated",
                                            Some(topic.clone()),
                                            Some(reason_text.clone()),
                                            SubscriptionEventLevel::Warning,
                                        );
                                                                            });
                                }
                                if self.subs.is_empty() && self.pending_cancel.is_empty() {
                                    state.fail(BlpError::SubscriptionFailure {
                                        cid: None,
                                        label: Some(format!(
                                            "All subscriptions ended; last termination: {} ({})",
                                            topic, reason_text,
                                        )),
                                    });
                                }
                            }
                        }
                    }
                    "SubscriptionStreamsActivated" => {
                        // Bloomberg fires this on initial subscribe success and
                        // again whenever streams come back after a temporary
                        // disconnection (per BLPAPI ChangeLog v3.11.6). This is
                        // the authoritative "data is flowing" signal.
                        self.last_streams_warn_us.remove(&key);
                        if self.subs.contains(key) {
                            if let Some(status) = &self.status {
                                let snapshot = status.load();
                                let topic = snapshot.topic_for_key(key).map(|t| t.to_string());
                                let prev = topic.as_ref().and_then(|t| {
                                    snapshot
                                        .topic_statuses()
                                        .get(t)
                                        .map(|info| info.streams_active)
                                });
                                drop(snapshot);
                                if let Some(topic) = topic {
                                    status.update(|next| {
                                                                                next.set_topic_streams_active(&topic, true);
                                        // Only emit a status event on a real transition
                                        // (avoids spamming on the initial activation which
                                        // already fires SubscriptionStarted right before).
                                        if prev == Some(false) {
                                            next.record_subscription_event(
                                                "SubscriptionStreamsActivated",
                                                Some(topic.clone()),
                                                reason.clone(),
                                                SubscriptionEventLevel::Info,
                                            );
                                        }
                                                                            });
                                }
                            }
                        }
                        xbbg_log::debug!(
                            worker_id = self.id,
                            key = key,
                            "subscription streams activated"
                        );
                    }
                    "SubscriptionStreamsDeactivated" => {
                        // Streams for this subscription are temporarily unavailable.
                        // The SDK will auto-recover; we just surface the state so
                        // callers polling status can tell "quiet" from "dead".
                        if self.subs.contains(key) {
                            if let Some(status) = &self.status {
                                let snapshot = status.load();
                                let topic = snapshot.topic_for_key(key).map(|t| t.to_string());
                                let prev = topic.as_ref().and_then(|t| {
                                    snapshot
                                        .topic_statuses()
                                        .get(t)
                                        .map(|info| info.streams_active)
                                });
                                drop(snapshot);
                                if let Some(topic) = topic {
                                    status.update(|next| {
                                                                                next.set_topic_streams_active(&topic, false);
                                        if prev != Some(false) {
                                            next.record_subscription_event(
                                                "SubscriptionStreamsDeactivated",
                                                Some(topic.clone()),
                                                reason.clone(),
                                                SubscriptionEventLevel::Warning,
                                            );
                                        }
                                                                            });
                                }
                            }
                        }
                        xbbg_log::warn!(
                            worker_id = self.id,
                            key = key,
                            reason = %reason.as_deref().unwrap_or(""),
                            "subscription streams deactivated"
                        );
                    }
                    _ => {
                        xbbg_log::trace!(
                            worker_id = self.id,
                            key = key,
                            msg_type = msg_type,
                            "subscription status"
                        );
                    }
                }
            }
        }
    }

    fn handle_session_status(&mut self, msg: &xbbg_core::Message<'_>, shared: &SubscriptionWorkerShared) {
        let msg_type_name = msg.message_type();
        let msg_type = msg_type_name.as_str();
        match msg_type {
            "SessionStarted" => {
                shared.health.store(WorkerHealth::Healthy as u8, Ordering::Release);
                shared.resolve_startup(Ok(()));
                xbbg_log::info!(worker_id = self.id, "session started");
                if let Some(status) = &self.status {
                    status.update(|next| {
                                                next.record_session_state(
                            SessionLifecycleState::Up,
                            "SessionStarted",
                            None,
                        );
                                            });
                }
            }
            "SessionConnectionDown" => {
                // Bloomberg SDK contract: SessionConnectionDown is informational.
                // The SDK's auto_restart_on_disconnection machinery handles reconnection
                // and will auto-recover active subscriptions (see BLPAPI ChangeLog v3.11.6).
                // We just record state for diagnostics; do NOT drain subscriptions and
                // do NOT resubscribe on the subsequent Up.
                let reason = extract_reason_description(msg);
                xbbg_log::warn!(
                    worker_id = self.id,
                    active_subs = self.subs.len(),
                    reason = %reason.as_deref().unwrap_or(""),
                    "SessionConnectionDown — informational; SDK will auto-reconnect"
                );
                if let Some(status) = &self.status {
                    let detail = reason.or_else(|| {
                        Some(format!(
                            "worker={} active_subscriptions={}",
                            self.id,
                            self.subs.len(),
                        ))
                    });
                    status.update(|next| {
                                                next.record_session_state(
                            SessionLifecycleState::Down,
                            "SessionConnectionDown",
                            detail.clone(),
                        );
                                            });
                }
            }
            "AuthorizationRevoked" => {
                // Session identity was revoked mid-session (e.g. token expired,
                // entitlement change). Any authorized request/subscribe will now
                // fail. Treat this as terminal for the worker: we have no
                // re-auth flow, so drain subs, mark Dead, and let the pool spawn
                // a fresh worker that re-auths during startup.
                let reason = extract_reason_description(msg);
                xbbg_log::error!(
                    worker_id = self.id,
                    active_subs = self.subs.len(),
                    reason = %reason.as_deref().unwrap_or(""),
                    "AuthorizationRevoked — identity gone; closing subscriptions"
                );
                let keys: Vec<usize> = self.subs.iter().map(|(k, _)| k).collect();
                for key in keys {
                    let mut state = self.subs.remove(key);
                    state.mark_closing();
                    state.fail(BlpError::Internal {
                        detail: format!(
                            "Bloomberg session identity revoked (worker={}){}. \
                             Subscription closed. Please re-authenticate and resubscribe.",
                            self.id,
                            reason
                                .as_deref()
                                .map(|r| format!(": {}", r))
                                .unwrap_or_default(),
                        ),
                    });
                }
                self.clear_active_status();
                shared.health.store(WorkerHealth::Dead as u8, Ordering::Release);
                if let Some(status) = &self.status {
                    let detail = reason.or_else(|| Some(format!("worker={}", self.id)));
                    status.update(|next| {
                                                next.record_session_state(
                            SessionLifecycleState::Terminated,
                            "AuthorizationRevoked",
                            detail.clone(),
                        );
                                            });
                }
            }
            "SessionStartupFailure" => {
                let reason = extract_reason_description(msg);
                shared.health.store(WorkerHealth::Dead as u8, Ordering::Release);
                shared.resolve_startup(Err(session_start_error("subscription session startup failure", reason.clone())));
                xbbg_log::error!(worker_id = self.id, reason = %reason.as_deref().unwrap_or(""), "subscription session startup failed");
            }
            "SessionTerminated" => {
                let reason = extract_reason_description(msg);
                shared.resolve_startup(Err(session_start_error("subscription session terminated during startup", reason.clone())));
                xbbg_log::error!(
                    worker_id = self.id,
                    active_subs = self.subs.len(),
                    reason = %reason.as_deref().unwrap_or(""),
                    "SessionTerminated — SDK gave up reconnecting; closing subscriptions"
                );
                // Session is dead. Send error to all consumers and remove all subs.
                let keys: Vec<usize> = self.subs.iter().map(|(k, _)| k).collect();
                for key in keys {
                    let mut state = self.subs.remove(key);
                    state.mark_closing();
                    state.fail(BlpError::Internal {
                        detail: format!(
                            "Bloomberg session terminated (worker={}){}. \
                             Subscription closed. Please resubscribe.",
                            self.id,
                            reason
                                .as_deref()
                                .map(|r| format!(": {}", r))
                                .unwrap_or_default(),
                        ),
                    });
                }
                self.clear_active_status();
                // Mark the worker Dead so the pool refuses to hand it out to
                // new claims — the session ptr is terminated and can't be restarted.
                shared.health.store(WorkerHealth::Dead as u8, Ordering::Release);
                if let Some(status) = &self.status {
                    let detail = reason.or_else(|| Some(format!("worker={}", self.id)));
                    status.update(|next| {
                                                next.record_session_state(
                            SessionLifecycleState::Terminated,
                            "SessionTerminated",
                            detail.clone(),
                        );
                                            });
                }
            }
            "SessionConnectionUp" => {
                // Informational. The SDK has re-established the TCP connection and
                // will automatically re-activate our subscriptions (per BLPAPI
                // ChangeLog v3.11.6: Subscription Streams{Activated,Deactivated}
                // events track per-subscription availability).
                let reason = extract_reason_description(msg);
                xbbg_log::info!(
                    worker_id = self.id,
                    active_subs = self.subs.len(),
                    reason = %reason.as_deref().unwrap_or(""),
                    "SessionConnectionUp — informational; SDK re-established the connection"
                );
                if let Some(status) = &self.status {
                    let detail = reason.or_else(|| {
                        Some(format!(
                            "worker={} active_subscriptions={}",
                            self.id,
                            self.subs.len(),
                        ))
                    });
                    status.update(|next| {
                                                next.record_session_state(
                            SessionLifecycleState::Up,
                            "SessionConnectionUp",
                            detail.clone(),
                        );
                                            });
                }
            }
            _ => {
                xbbg_log::debug!(worker_id = self.id, msg_type = msg_type, "session status");
            }
        }
    }

    fn handle_service_status(&mut self, msg: &xbbg_core::Message<'_>) {
        let msg_type_name = msg.message_type();
        let msg_type = msg_type_name.as_str();

        if matches!(msg_type, "ServiceOpened" | "ServiceOpenFailure") {
            if let Some(CorrelationId::Int(cid_int)) = msg.correlation_id(0) {
                let service = self
                    .pending_service_opens
                    .iter()
                    .find_map(|(service, open)| (open.cid == cid_int).then(|| service.clone()));
                if let Some(service_name) = service {
                    let open = self.pending_service_opens.remove(&service_name).expect("found pending open");
                    match msg_type {
                        "ServiceOpened" => {
                            self.open_services.insert(service_name.clone());
                            if let Some(status) = &self.status {
                                let service_for_status = service_name.clone();
                                status.update(|next| {
                                    next.record_service_state(
                                        service_for_status,
                                        true,
                                        "ServiceOpened",
                                        Some("service opened on demand".to_string()),
                                    );
                                });
                            }
                            for waiter in open.waiters {
                                let _ = waiter.send(Ok(()));
                            }
                        }
                        "ServiceOpenFailure" => {
                            let reason = extract_reason_description(msg);
                            if let Some(status) = &self.status {
                                let service_for_status = service_name.clone();
                                let reason_for_status = reason.clone();
                                status.update(|next| {
                                    next.record_service_state(
                                        service_for_status,
                                        false,
                                        "ServiceOpenFailure",
                                        reason_for_status,
                                    );
                                });
                            }
                            for waiter in open.waiters {
                                let _ = waiter.send(Err(BlpError::OpenService {
                                    service: service_name.clone(),
                                    source: None,
                                    label: reason.clone(),
                                }));
                            }
                        }
                        _ => {}
                    }
                    return;
                }
            }
        }

        let service = msg
            .elements()
            .get_by_str("serviceName")
            .and_then(|value| value.get_str(0))
            .map(str::to_string);
        if let Some(status) = &self.status {
            match msg_type {
                "ServiceDown" => {
                    let service_name = service.clone().unwrap_or_else(|| "unknown".to_string());
                    let active_subs = self.subs.len();
                    let has_active = !self.subs.is_empty();
                    status.update(|next| {
                        next.record_service_state(service_name.clone(), false, msg_type, None);
                        if has_active {
                            next.record_subscription_event(
                                "ServiceDownAffectsActiveSubscriptions",
                                None,
                                Some(format!(
                                    "service={} active_subscriptions={}",
                                    service_name, active_subs,
                                )),
                                SubscriptionEventLevel::Warning,
                            );
                        }
                    });
                    if has_active {
                        xbbg_log::warn!(
                            worker_id = self.id,
                            service = %service_name,
                            active_subs = active_subs,
                            "ServiceDown — active subscriptions may be silently quieted"
                        );
                    }
                }
                "ServiceUp" | "ServiceOpened" => {
                    let service_name = service.unwrap_or_else(|| "unknown".to_string());
                    status.update(|next| {
                        next.record_service_state(service_name.clone(), true, msg_type, None);
                    });
                }
                _ => {}
            }
        }
        xbbg_log::debug!(worker_id = self.id, msg_type = msg_type, "service status");
    }

    fn handle_admin_event(&mut self, msg: &xbbg_core::Message<'_>) {
        let msg_type_name = msg.message_type();
        let msg_type = msg_type_name.as_str();
        match msg_type {
            "SlowConsumerWarning" => {
                if let Some(status) = &self.status {
                    status.update(|next| {
                                                next.record_admin_warning(msg_type, None);
                                            });
                }
                xbbg_log::warn!(worker_id = self.id, "slow consumer warning");
            }
            "SlowConsumerWarningCleared" => {
                for (_, state) in self.subs.iter_mut() {
                    state.clear_slow_consumer();
                }
                if let Some(status) = &self.status {
                    status.update(|next| {
                                                next.record_admin_warning_cleared(msg_type, None);
                                            });
                }
                xbbg_log::info!(worker_id = self.id, "slow consumer warning cleared");
            }
            "DataLoss" => {
                let timestamp_us = msg.time_received_us();
                let correlation_count = msg.num_correlation_ids();
                if correlation_count == 0 {
                    if let Some(status) = &self.status {
                        status.update(|next| {
                                                        next.record_admin_data_loss(None, None);
                                                    });
                    }
                }
                for index in 0..correlation_count {
                    if let Some(correlation_id) = msg.correlation_id(index) {
                        let Some(dispatch_key) = DispatchKey::from_correlation_id(&correlation_id)
                        else {
                            continue;
                        };
                        let key = dispatch_key.to_slab_key();
                        if let Some(state) = self.subs.get_mut(key) {
                            let topic = state.topic.to_string();
                            state.on_dataloss(timestamp_us);
                            if let Some(status) = &self.status {
                                status.update(|next| {
                                                                        next.record_admin_data_loss(Some(topic.clone()), None);
                                                                    });
                            }
                        }
                    }
                }
                xbbg_log::warn!(worker_id = self.id, "data loss event received");
            }
            _ => {
                if let Some(status) = &self.status {
                    status.update(|next| {
                                                next.push_event(
                            super::SubscriptionEventCategory::Admin,
                            SubscriptionEventLevel::Info,
                            msg_type,
                            None,
                            None,
                        );
                                            });
                }
                xbbg_log::debug!(worker_id = self.id, msg_type = msg_type, "admin event");
            }
        }
    }

    /// Check if any topics are in streams-deactivated state longer than the configured
    /// warn threshold and emit a one-shot Warning event so callers polling status
    /// see "your data is quiet, not broken — SDK is still trying to recover".
    fn check_streams_deactivated(&mut self) {
        let warn_ms = self.config.streams_deactivated_warn_ms;
        if warn_ms == 0 {
            return;
        }
        let Some(status_arc) = self.status.clone() else {
            return;
        };

        let now = super::timestamp_now_us();
        let warn_us = (warn_ms as i64) * 1_000;

        // Collect keys to warn about without holding a snapshot across the warn emission.
        let mut to_warn: Vec<(SlabKey, String, i64)> = Vec::new();
        {
            let snapshot = status_arc.load();
            for (topic, info) in snapshot.topic_statuses().iter() {
                if info.streams_active {
                    continue;
                }
                // Only warn for topics that have actually been deactivated (not pre-streaming).
                if info.streams_changed_us == 0 {
                    continue;
                }
                let elapsed = now - info.streams_changed_us;
                if elapsed < warn_us {
                    continue;
                }
                // Map back to slab key for debouncing.
                if let Some(&key) = snapshot.topic_to_key().get(topic) {
                    let last_warn = self.last_streams_warn_us.get(&key).copied().unwrap_or(0);
                    if now - last_warn >= warn_us {
                        to_warn.push((key, topic.clone(), elapsed));
                    }
                }
            }
        }

        if to_warn.is_empty() {
            return;
        }

        for (key, topic, elapsed_us) in &to_warn {
            self.last_streams_warn_us.insert(*key, now);
            xbbg_log::warn!(
                worker_id = self.id,
                topic = %topic,
                elapsed_ms = elapsed_us / 1_000,
                "subscription streams still deactivated"
            );
        }
        status_arc.update(|next| {
                        for (_, topic, elapsed_us) in &to_warn {
                let detail = format!(
                    "topic has been streams-inactive for {}ms; SDK is still trying to recover",
                    elapsed_us / 1_000
                );
                next.record_subscription_event(
                    "SubscriptionStreamsDeactivatedPersisting",
                    Some(topic.clone()),
                    Some(detail),
                    SubscriptionEventLevel::Warning,
                );
            }
                    });
    }
}



fn session_start_error(context: &str, reason: Option<String>) -> BlpError {
    BlpError::SessionStart {
        source: None,
        label: Some(match reason {
            Some(reason) => format!("{context}: {reason}"),
            None => context.to_string(),
        }),
    }
}

fn extract_reason_description(msg: &xbbg_core::Message<'_>) -> Option<String> {
    let reason = msg.elements().get_by_str("reason")?;
    for key in ["description", "category", "message"] {
        if let Some(s) = reason.get_by_str(key).and_then(|e| e.get_str(0)) {
            return Some(s.to_string());
        }
    }
    None
}

struct SubscriptionWorkerHandleInner {
    id: usize,
    session: Arc<AsyncSession>,
    shared: Arc<SubscriptionWorkerShared>,
    health: Arc<AtomicU8>,
    shutdown: AtomicBool,
    monitor: Mutex<Option<JoinHandle<()>>>,
}

impl SubscriptionWorkerHandleInner {
    fn ensure_monitor_started(self: &Arc<Self>) {
        let mut monitor = self.monitor.lock();
        if monitor.is_some() || self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let shared = Arc::clone(&self.shared);
        *monitor = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                shared.check_streams_deactivated();
            }
        }));
    }

    fn signal_shutdown(&self) {
        if self.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(handle) = self.monitor.lock().take() {
            handle.abort();
        }
        self.session.stop_async();
    }
}

#[derive(Clone)]
pub struct SubscriptionCommandHandle {
    inner: Arc<SubscriptionWorkerHandleInner>,
}

impl SubscriptionCommandHandle {
    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe(
        &self,
        service: String,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
        status: SharedSubscriptionStatus,
    ) -> Result<(Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpAsyncError> {
        self.inner.ensure_monitor_started();
        self.inner.shared.set_status(status);
        let was_open = self.inner.shared.service_is_open(&service);
        if let Err(e) = self.ensure_service(&service).await {
            self.inner.shared.record_service_open_error(&service, &e);
            xbbg_log::error!(worker_id = self.worker_id(), service = %service, error = %e, "failed to open service");
            return Err(BlpAsyncError::BlpError(e));
        }
        self.inner.shared.record_service_ready_if_already_open(&service, was_open);
        let (sub_list, keys, metrics) = self.inner.shared.register_subscriptions(
            topics,
            fields,
            all_fields,
            options,
            flush_threshold,
            overflow_policy,
            stream,
        ).map_err(BlpAsyncError::BlpError)?;
        if let Err(e) = self.inner.session.subscribe(&sub_list, None) {
            self.inner.shared.cleanup_failed_subscribe(&keys);
            xbbg_log::error!(worker_id = self.worker_id(), error = %e, "subscribe failed");
            return Err(BlpAsyncError::BlpError(e));
        }
        Ok((keys, metrics))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_topics(
        &self,
        service: String,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
        status: SharedSubscriptionStatus,
    ) -> Result<(Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpAsyncError> {
        self.subscribe(
            service,
            topics,
            fields,
            all_fields,
            options,
            flush_threshold,
            overflow_policy,
            stream,
            status,
        ).await
    }

    async fn ensure_service(&self, service: &str) -> Result<(), BlpError> {
        if self.inner.shared.service_is_open(service) {
            return Ok(());
        }
        let (should_open, cid_int, rx) = self.inner.shared.register_service_waiter(service);
        if should_open {
            xbbg_log::info!(worker_id = self.worker_id(), service = service, "opening service on demand (async)");
            let cid = CorrelationId::Int(cid_int);
            if let Err(e) = self.inner.session.open_service_async(service, &cid) {
                self.inner.shared.remove_pending_service_open(service);
                return Err(e);
            }
        }
        match tokio::time::timeout(Duration::from_millis(SERVICE_OPEN_TIMEOUT_MS), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(BlpError::Internal {
                detail: format!("pending service open for {service} was cancelled"),
            }),
            Err(_) => {
                self.inner.shared.remove_pending_service_open(service);
                Err(BlpError::Timeout)
            }
        }
    }

    pub async fn unsubscribe(&self, keys: Vec<SlabKey>) -> Result<(), BlpAsyncError> {
        self.unsubscribe_now(keys);
        Ok(())
    }

    fn unsubscribe_now(&self, keys: Vec<SlabKey>) {
        let (unsub_list, unsub_count) = self.inner.shared.build_unsubscribe_list(keys);
        if unsub_count > 0 {
            if let Err(e) = self.inner.session.unsubscribe(&unsub_list) {
                xbbg_log::error!(worker_id = self.worker_id(), error = %e, "session.unsubscribe failed");
            }
        }
    }

    pub fn worker_id(&self) -> usize {
        self.inner.id
    }

}

pub struct SubscriptionWorkerHandle {
    inner: Arc<SubscriptionWorkerHandleInner>,
}

impl SubscriptionWorkerHandle {
    fn spawn(id: usize, config: Arc<EngineConfig>) -> Result<Self, BlpError> {
        let options = build_session_options(&config, true)?;
        let health = Arc::new(AtomicU8::new(WorkerHealth::Healthy as u8));
        let shared = Arc::new(SubscriptionWorkerShared::new(id, Arc::clone(&config), Arc::clone(&health)));
        let handler_shared = Arc::clone(&shared);
        let session = AsyncSession::new(&options, move |event| {
            handler_shared.dispatch_event(event);
        })?;
        session
            .start()
            .map_err(|err| attach_auth_context(err, config.auth.as_ref()))?;
        shared
            .wait_startup(Duration::from_millis(u64::from(SESSION_STARTUP_TIMEOUT_MS)))
            .map_err(|err| attach_auth_context(err, config.auth.as_ref()))?;
        session.open_service(crate::services::Service::MktData.as_str())?;
        shared
            .state
            .lock()
            .open_services
            .insert(crate::services::Service::MktData.to_string());
        xbbg_log::info!(worker_id = id, "subscription worker pre-warmed");
        let session = Arc::new(session);
        Ok(Self {
            inner: Arc::new(SubscriptionWorkerHandleInner {
                id,
                session,
                shared,
                health,
                shutdown: AtomicBool::new(false),
                monitor: Mutex::new(None),
            }),
        })
    }

    pub fn health(&self) -> WorkerHealth {
        match self.inner.health.load(Ordering::Acquire) {
            0 => WorkerHealth::Healthy,
            1 => WorkerHealth::Degraded,
            2 => WorkerHealth::Dead,
            _ => WorkerHealth::Dead,
        }
    }

    fn id(&self) -> usize {
        self.inner.id
    }

    fn command_handle(&self) -> SubscriptionCommandHandle {
        SubscriptionCommandHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    fn signal_shutdown(&self) {
        self.inner.signal_shutdown();
    }

    fn shutdown_blocking(&mut self) {
        self.signal_shutdown();
    }
}

impl Drop for SubscriptionWorkerHandle {
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

pub struct SubscriptionSessionPool {
    available: Mutex<Vec<SubscriptionWorkerHandle>>,
    next_id: AtomicUsize,
    config: Arc<EngineConfig>,
    initial_size: usize,
}

impl SubscriptionSessionPool {
    pub fn new(size: usize, config: Arc<EngineConfig>) -> Result<Self, BlpAsyncError> {
        xbbg_log::info!(pool_size = size, "creating subscription session pool");
        let mut available = Vec::with_capacity(size);
        for id in 0..size {
            let handle = SubscriptionWorkerHandle::spawn(id, config.clone()).map_err(|e| {
                BlpAsyncError::BlpError(BlpError::Internal {
                    detail: format!("failed to spawn subscription worker {}: {}", id, e),
                })
            })?;
            available.push(handle);
        }
        xbbg_log::info!(pool_size = size, "subscription session pool ready");
        Ok(Self {
            available: Mutex::new(available),
            next_id: AtomicUsize::new(size),
            config,
            initial_size: size,
        })
    }

    pub fn claim(self: &Arc<Self>) -> Result<SessionClaim, BlpAsyncError> {
        let handle = {
            let mut available = self.available.lock();
            let mut chosen = None;
            while let Some(candidate) = available.pop() {
                if candidate.health() == WorkerHealth::Dead {
                    xbbg_log::warn!(worker_id = candidate.id(), "discarding dead subscription worker (SessionTerminated)");
                    drop(candidate);
                    continue;
                }
                chosen = Some(candidate);
                break;
            }
            if let Some(handle) = chosen {
                xbbg_log::debug!(worker_id = handle.id(), remaining = available.len(), "claimed session from pool");
                handle
            } else {
                drop(available);
                let new_id = self.next_id.fetch_add(1, Ordering::Relaxed);
                xbbg_log::warn!(worker_id = new_id, initial_size = self.initial_size, "subscription pool exhausted or all dead, creating new session");
                SubscriptionWorkerHandle::spawn(new_id, self.config.clone()).map_err(|e| {
                    BlpAsyncError::BlpError(BlpError::Internal {
                        detail: format!("failed to create dynamic subscription worker: {}", e),
                    })
                })?
            }
        };
        Ok(SessionClaim {
            handle: Some(handle),
            pool: Arc::clone(self),
            cleanup_status: None,
        })
    }

    fn release(&self, handle: SubscriptionWorkerHandle) {
        if handle.health() == WorkerHealth::Dead {
            xbbg_log::warn!(worker_id = handle.id(), "discarding dead subscription worker on release (SessionTerminated)");
            drop(handle);
            return;
        }
        let mut available = self.available.lock();
        xbbg_log::debug!(worker_id = handle.id(), pool_size = available.len() + 1, "session returned to pool");
        available.push(handle);
    }

    pub fn available_count(&self) -> usize {
        self.available.lock().len()
    }

    pub fn signal_shutdown(&self) {
        let available = self.available.lock();
        xbbg_log::info!(count = available.len(), "signaling subscription pool shutdown");
        for handle in available.iter() {
            handle.signal_shutdown();
        }
    }

    pub fn shutdown_blocking(&self) {
        let mut available = self.available.lock();
        xbbg_log::info!(count = available.len(), "shutting down subscription pool (blocking)");
        for handle in available.iter_mut() {
            handle.shutdown_blocking();
        }
        available.clear();
    }
}

impl Drop for SubscriptionSessionPool {
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

pub struct SessionClaim {
    handle: Option<SubscriptionWorkerHandle>,
    pool: Arc<SubscriptionSessionPool>,
    cleanup_status: Option<SharedSubscriptionStatus>,
}

impl SessionClaim {
    pub fn command_handle(&self) -> Result<SubscriptionCommandHandle, BlpAsyncError> {
        self.handle
            .as_ref()
            .map(SubscriptionWorkerHandle::command_handle)
            .ok_or_else(|| BlpAsyncError::ConfigError {
                detail: "session already released".to_string(),
            })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe(
        &self,
        service: String,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
        status: SharedSubscriptionStatus,
    ) -> Result<(Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpAsyncError> {
        self.command_handle()?
            .subscribe(
                service,
                topics,
                fields,
                all_fields,
                options,
                flush_threshold,
                overflow_policy,
                stream,
                status,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_topics(
        &self,
        service: String,
        topics: Vec<String>,
        fields: Vec<String>,
        all_fields: bool,
        options: Vec<String>,
        flush_threshold: Option<usize>,
        overflow_policy: Option<OverflowPolicy>,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
        status: SharedSubscriptionStatus,
    ) -> Result<(Vec<SlabKey>, Vec<Arc<SubscriptionMetrics>>), BlpAsyncError> {
        self.command_handle()?
            .add_topics(
                service,
                topics,
                fields,
                all_fields,
                options,
                flush_threshold,
                overflow_policy,
                stream,
                status,
            )
            .await
    }

    pub async fn unsubscribe(&self, keys: Vec<SlabKey>) -> Result<(), BlpAsyncError> {
        self.command_handle()?.unsubscribe(keys).await
    }

    pub fn set_cleanup_status(&mut self, status: SharedSubscriptionStatus) {
        self.cleanup_status = Some(status);
    }

    pub fn close_without_reuse(mut self, keys: Vec<SlabKey>) {
        if let Some(handle) = self.handle.take() {
            if !keys.is_empty() {
                handle.command_handle().unsubscribe_now(keys);
            }
            handle.signal_shutdown();
            drop(handle);
        }
    }

    pub fn worker_id(&self) -> Option<usize> {
        self.handle.as_ref().map(SubscriptionWorkerHandle::id)
    }
}

impl Drop for SessionClaim {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let active_keys = self
            .cleanup_status
            .as_ref()
            .map(|status| status.load().keys().to_vec())
            .unwrap_or_default();
        if active_keys.is_empty() {
            self.pool.release(handle);
            return;
        }
        handle.command_handle().unsubscribe_now(active_keys);
        if let Some(status) = &self.cleanup_status {
            status.update(|next| next.clear_active());
        }
        handle.signal_shutdown();
        drop(handle);
    }
}
