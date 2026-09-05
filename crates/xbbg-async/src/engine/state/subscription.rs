//! Update-first subscription state for real-time data.
//!
//! Extracts Bloomberg subscription messages into native `SubscriptionUpdate`s
//! without constructing Arrow on the hot path. Arrow conversion is an explicit
//! compatibility adapter in `update_arrow`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use smallvec::SmallVec;
use tokio::sync::{mpsc, oneshot};

use xbbg_core::{BlpError, DataType as BlpDataType, Message, Name};

use super::super::OverflowPolicy;
use super::update::{
    FieldIndex, FieldKind, FieldLayout, FieldMeta, StringValueCache, SubscriptionUpdate, TopicId,
    UpdateField, UpdateValue,
};

pub struct SubscriptionMetrics {
    pub messages_received: Arc<AtomicU64>,
    pub dropped_batches: Arc<AtomicU64>,
    pub batches_sent: Arc<AtomicU64>,
    pub slow_consumer: Arc<AtomicBool>,
    pub data_loss_events: Arc<AtomicU64>,
    pub last_message_us: Arc<AtomicU64>,
    pub last_data_loss_us: Arc<AtomicU64>,
}

const BLOCK_FORWARD_TIMEOUT: Duration = Duration::from_millis(50);

struct ForwardedSubscriptionUpdate {
    stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
    update: SubscriptionUpdate,
    metrics: Arc<SubscriptionMetrics>,
    topic: Arc<str>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "Keep the common update inline: boxing would allocate on every SDK callback; drain barriers are rare"
)]
enum ForwarderCommand {
    Update(ForwardedSubscriptionUpdate),
    Drain(oneshot::Sender<()>),
}

/// Bounded, non-blocking ingress from Bloomberg's callback into an async
/// forwarding task. One forwarder is shared by every topic on a session.
#[derive(Clone)]
pub(crate) struct SubscriptionForwarder {
    tx: mpsc::Sender<ForwarderCommand>,
}

pub(crate) fn subscription_forwarder_channel(
    capacity: usize,
) -> (
    SubscriptionForwarder,
    impl Future<Output = ()> + Send + 'static,
) {
    let (tx, mut rx) = mpsc::channel::<ForwarderCommand>(capacity);
    let future = async move {
        while let Some(command) = rx.recv().await {
            let item = match command {
                ForwarderCommand::Update(item) => item,
                ForwarderCommand::Drain(done) => {
                    let _ = done.send(());
                    continue;
                }
            };
            let ForwardedSubscriptionUpdate {
                stream,
                update,
                metrics,
                topic,
            } = item;
            match tokio::time::timeout(BLOCK_FORWARD_TIMEOUT, stream.send(Ok(update))).await {
                Ok(Ok(())) => {
                    metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(_)) => {}
                Err(_) => {
                    let dropped = metrics.dropped_batches.fetch_add(1, Ordering::Relaxed) + 1;
                    metrics.slow_consumer.store(true, Ordering::Relaxed);
                    if dropped == 1 || dropped.is_multiple_of(1024) {
                        xbbg_log::warn!(
                            topic = %topic,
                            dropped,
                            policy = "Block",
                            "bounded forwarding timed out - dropping update"
                        );
                    }
                }
            }
        }
    };
    (SubscriptionForwarder { tx }, future)
}

impl SubscriptionForwarder {
    fn try_forward(
        &self,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
        update: SubscriptionUpdate,
        metrics: Arc<SubscriptionMetrics>,
        topic: Arc<str>,
    ) -> Result<(), mpsc::error::TrySendError<()>> {
        self.tx
            .try_send(ForwarderCommand::Update(ForwardedSubscriptionUpdate {
                stream,
                update,
                metrics,
                topic,
            }))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => mpsc::error::TrySendError::Full(()),
                mpsc::error::TrySendError::Closed(_) => mpsc::error::TrySendError::Closed(()),
            })
    }

    pub(crate) async fn drain(&self) -> Result<(), ()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(ForwarderCommand::Drain(done_tx))
            .await
            .map_err(|_| ())?;
        done_rx.await.map_err(|_| ())
    }
}

#[derive(Clone, Copy)]
enum AllFieldSlot {
    Captured {
        key: usize,
        idx: FieldIndex,
        datatype: BlpDataType,
    },
    Skipped {
        key: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageOutcome {
    Normal { first_message: bool },
    DataLoss,
}

/// State for a single subscription, owned by PumpA.
pub struct SubscriptionState {
    /// Topic string (e.g., "IBM US Equity")
    pub topic: Arc<str>,
    topic_id: TopicId,
    /// Field names as strings for layout, logs, and compatibility schemas.
    pub field_strings: Vec<Arc<str>>,
    /// Pre-interned field names for requested-field hot-path lookup.
    field_names: Vec<Name>,
    /// Fast dynamic-field lookup keyed by Bloomberg's interned Name pointer.
    field_name_keys: HashMap<usize, FieldIndex>,
    /// Per-field flag for Bloomberg date-or-time fields that cannot be read safely.
    invalid_dateortime_fields: Vec<bool>,
    /// Per-position allFields cache for stable Bloomberg subscription schemas.
    all_field_slots: Vec<Option<AllFieldSlot>>,
    field_kinds: Vec<FieldKind>,
    layout_version: u32,
    layout: Arc<FieldLayout>,
    /// Stream to send native updates (or errors for subscription failures).
    pub stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
    /// Session-scoped off-callback forwarding for `OverflowPolicy::Block`.
    forwarder: Option<SubscriptionForwarder>,
    /// Retained for option/status compatibility. Updates are emitted immediately.
    pub flush_threshold: usize,
    /// Slow consumer flag (DATALOSS received)
    pub slow_consumer: bool,
    /// Overflow policy for slow consumers
    pub overflow_policy: OverflowPolicy,
    /// Dropped update count (keeps historical field name for stats compatibility)
    pub dropped_batches: u64,
    pub metrics: Arc<SubscriptionMetrics>,
    /// Whether at least one data message has been observed.
    has_received_data: bool,
    /// Suppress stream-closed warnings during expected shutdown paths.
    suppress_closed_warning: bool,
    /// Whether to append all top-level scalar fields Bloomberg exposes.
    capture_all_fields: bool,
    /// Optional projected field for Bloomberg mktbar message kind (MarketBarStart/Update/End).
    subscription_data_index: Option<FieldIndex>,
    event_type_index: FieldIndex,
    event_subtype_index: FieldIndex,
    string_value_cache: Vec<StringValueCache>,
    subscription_data_type_cache: [Option<(usize, Arc<str>)>; 4],
}

impl SubscriptionState {
    const EVENT_METADATA_FIELDS: [&'static str; 2] =
        ["MKTDATA_EVENT_TYPE", "MKTDATA_EVENT_SUBTYPE"];
    const SUBSCRIPTION_DATA_FIELD: &'static str = "SUBSCRIPTION_DATA";
    // Bloomberg can publish date-or-time values here with invalid date parts;
    // any typed/string getter makes the SDK emit warnings, so capture nulls.
    const INVALID_DATEORTIME_FIELDS: [&'static str; 1] = ["LAST_UPDATE_ALL_SESSIONS_RT"];

    /// Create a new subscription state with default overflow policy.
    pub fn new(
        topic: String,
        fields: Vec<String>,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
        flush_threshold: usize,
        capture_all_fields: bool,
    ) -> Self {
        Self::with_policy(
            topic,
            fields,
            stream,
            flush_threshold,
            OverflowPolicy::default(),
            capture_all_fields,
        )
    }

    /// Create a new subscription state with specified overflow policy.
    pub fn with_policy(
        topic: String,
        fields: Vec<String>,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
        flush_threshold: usize,
        overflow_policy: OverflowPolicy,
        capture_all_fields: bool,
    ) -> Self {
        Self::with_policy_and_forwarder(
            topic,
            fields,
            stream,
            flush_threshold,
            overflow_policy,
            capture_all_fields,
            None,
        )
    }

    pub(crate) fn with_policy_and_forwarder(
        topic: String,
        fields: Vec<String>,
        stream: mpsc::Sender<Result<SubscriptionUpdate, BlpError>>,
        flush_threshold: usize,
        overflow_policy: OverflowPolicy,
        capture_all_fields: bool,
        forwarder: Option<SubscriptionForwarder>,
    ) -> Self {
        let mut field_strings =
            Vec::with_capacity(fields.len() + Self::EVENT_METADATA_FIELDS.len());
        let mut field_names = Vec::with_capacity(fields.len() + Self::EVENT_METADATA_FIELDS.len());
        let mut field_name_keys =
            HashMap::with_capacity(fields.len() + Self::EVENT_METADATA_FIELDS.len());
        let mut invalid_dateortime_fields =
            Vec::with_capacity(fields.len() + Self::EVENT_METADATA_FIELDS.len());
        let mut field_kinds = Vec::with_capacity(fields.len() + Self::EVENT_METADATA_FIELDS.len());

        for field in fields {
            Self::push_field_if_new(
                &mut field_strings,
                &mut field_names,
                &mut field_name_keys,
                &mut invalid_dateortime_fields,
                &mut field_kinds,
                field.into(),
            );
        }
        let event_type_index = Self::push_field_if_new(
            &mut field_strings,
            &mut field_names,
            &mut field_name_keys,
            &mut invalid_dateortime_fields,
            &mut field_kinds,
            Arc::from("MKTDATA_EVENT_TYPE"),
        );
        let event_subtype_index = Self::push_field_if_new(
            &mut field_strings,
            &mut field_names,
            &mut field_name_keys,
            &mut invalid_dateortime_fields,
            &mut field_kinds,
            Arc::from("MKTDATA_EVENT_SUBTYPE"),
        );
        let subscription_data_index = if topic.starts_with("//blp/mktbar/") {
            Some(Self::push_field_if_new(
                &mut field_strings,
                &mut field_names,
                &mut field_name_keys,
                &mut invalid_dateortime_fields,
                &mut field_kinds,
                Arc::from(Self::SUBSCRIPTION_DATA_FIELD),
            ))
        } else {
            None
        };

        let metrics = Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(0)),
            dropped_batches: Arc::new(AtomicU64::new(0)),
            batches_sent: Arc::new(AtomicU64::new(0)),
            slow_consumer: Arc::new(AtomicBool::new(false)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        });
        let layout = Self::build_layout(1, &field_strings, &field_kinds);
        let string_value_cache = vec![None; field_strings.len()];

        Self {
            topic: Arc::from(topic),
            topic_id: 0,
            field_strings,
            field_names,
            field_name_keys,
            invalid_dateortime_fields,
            all_field_slots: Vec::new(),
            field_kinds,
            layout_version: 1,
            layout,
            stream,
            forwarder,
            flush_threshold,
            slow_consumer: false,
            overflow_policy,
            dropped_batches: 0,
            metrics,
            has_received_data: false,
            suppress_closed_warning: false,
            capture_all_fields,
            subscription_data_index,
            event_type_index,
            event_subtype_index,
            string_value_cache,
            subscription_data_type_cache: std::array::from_fn(|_| None),
        }
    }

    pub fn set_topic_id(&mut self, topic_id: TopicId) {
        self.topic_id = topic_id;
    }

    fn push_field_if_new(
        field_strings: &mut Vec<Arc<str>>,
        field_names: &mut Vec<Name>,
        field_name_keys: &mut HashMap<usize, FieldIndex>,
        invalid_dateortime_fields: &mut Vec<bool>,
        field_kinds: &mut Vec<FieldKind>,
        field: Arc<str>,
    ) -> FieldIndex {
        let name = Name::get_or_intern(&field);
        let key = name.as_ptr() as usize;
        if let Some(&idx) = field_name_keys.get(&key) {
            return idx;
        }
        let idx = field_strings.len() as FieldIndex;
        field_name_keys.insert(key, idx);
        invalid_dateortime_fields.push(Self::is_invalid_dateortime_field(&field));
        field_kinds.push(FieldKind::Unknown);
        field_names.push(name);
        field_strings.push(field);
        idx
    }

    fn build_layout(version: u32, names: &[Arc<str>], kinds: &[FieldKind]) -> Arc<FieldLayout> {
        Arc::new(FieldLayout::new(
            version,
            names
                .iter()
                .zip(kinds.iter())
                .enumerate()
                .map(|(idx, (name, kind))| FieldMeta::new(name.clone(), idx as FieldIndex, *kind))
                .collect(),
        ))
    }

    /// Process a SUBSCRIPTION_DATA message using Element API.
    ///
    /// Timestamps use Bloomberg SDK receive time when available (requires
    /// `setRecordSubscriptionDataReceiveTimes(true)`), falling back to
    /// `SystemTime::now()` if not enabled.
    pub fn on_message(&mut self, msg: &Message) -> MessageOutcome {
        let timestamp = msg.time_received_us().unwrap_or_else(Self::system_time_us);
        let subscription_data = self.subscription_data_arc(msg);
        let elem = msg.elements();
        let values = if self.capture_all_fields {
            self.extract_all_fields(&elem, subscription_data.as_ref())
        } else {
            self.extract_requested_fields(&elem, subscription_data.as_ref())
        };

        if self.is_dataloss_update(&values) {
            self.on_dataloss(msg.time_received_us());
            return MessageOutcome::DataLoss;
        }

        self.metrics
            .messages_received
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .last_message_us
            .store(timestamp as u64, Ordering::Relaxed);

        let first_message = !self.has_received_data;
        self.has_received_data = true;

        let update = SubscriptionUpdate {
            timestamp_us: timestamp,
            topic_id: self.topic_id,
            topic: self.topic.clone(),
            layout: self.layout.clone(),
            values,
        };
        self.send_update(update);

        MessageOutcome::Normal { first_message }
    }

    fn subscription_data_arc(&mut self, msg: &Message) -> Option<Arc<str>> {
        self.subscription_data_index?;
        let key = msg.name_key();
        for (cached_key, cached) in self.subscription_data_type_cache.iter().flatten() {
            if *cached_key == key {
                return Some(Arc::clone(cached));
            }
        }
        let value = Arc::<str>::from(msg.type_str());
        let slot = key % self.subscription_data_type_cache.len();
        self.subscription_data_type_cache[slot] = Some((key, Arc::clone(&value)));
        Some(value)
    }

    fn is_dataloss_update(&self, values: &[UpdateField]) -> bool {
        let mut is_summary = false;
        let mut is_dataloss = false;
        for field in values {
            if field.index == self.event_type_index {
                is_summary =
                    matches!(&field.value, UpdateValue::Str(value) if value.as_ref() == "SUMMARY");
            } else if field.index == self.event_subtype_index {
                is_dataloss =
                    matches!(&field.value, UpdateValue::Str(value) if value.as_ref() == "DATALOSS");
            }
        }
        is_summary && is_dataloss
    }

    fn system_time_us() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }

    fn extract_requested_fields(
        &mut self,
        elem: &xbbg_core::Element<'_>,
        subscription_data: Option<&Arc<str>>,
    ) -> SmallVec<[UpdateField; 8]> {
        let mut values = SmallVec::with_capacity(self.field_names.len());
        for idx in 0..self.field_names.len() {
            let value = if Some(idx as FieldIndex) == self.subscription_data_index {
                subscription_data
                    .cloned()
                    .map(UpdateValue::Str)
                    .unwrap_or(UpdateValue::Null)
            } else if self.invalid_dateortime_fields[idx] {
                UpdateValue::Null
            } else if let Some(field) = elem.get(&self.field_names[idx]) {
                let datatype = field.datatype();
                let value = self.update_value_for_field(idx, &field, datatype);
                if matches!(value, UpdateValue::Null) {
                    self.observe_field_kind(
                        idx as FieldIndex,
                        FieldKind::from_blp_datatype(datatype),
                    );
                }
                value
            } else {
                UpdateValue::Null
            };
            self.observe_kind(idx as FieldIndex, &value);
            values.push(UpdateField {
                index: idx as FieldIndex,
                value,
            });
        }
        values
    }

    fn extract_all_fields(
        &mut self,
        elem: &xbbg_core::Element<'_>,
        subscription_data: Option<&Arc<str>>,
    ) -> SmallVec<[UpdateField; 8]> {
        let mut values = SmallVec::with_capacity(elem.num_children() + 1);
        self.push_subscription_data(&mut values, subscription_data);
        for child_idx in 0..elem.num_children() {
            let Some(child) = elem.get_at(child_idx) else {
                continue;
            };

            let key = child.name_key();
            if let Some(Some(slot)) = self.all_field_slots.get(child_idx).copied() {
                match slot {
                    AllFieldSlot::Captured {
                        key: cached_key,
                        idx,
                        datatype,
                    } if cached_key == key => {
                        let value = self.extract_child_value(idx, &child, datatype);
                        self.observe_kind(idx, &value);
                        values.push(UpdateField { index: idx, value });
                        continue;
                    }
                    AllFieldSlot::Skipped { key: cached_key } if cached_key == key => continue,
                    _ => {}
                }
            }

            let datatype = child.datatype();
            if !Self::should_capture_datatype(datatype) {
                self.cache_all_field_slot(child_idx, AllFieldSlot::Skipped { key });
                continue;
            }

            let idx = self.ensure_field_for_child(&child, key);
            self.cache_all_field_slot(child_idx, AllFieldSlot::Captured { key, idx, datatype });
            let value = self.extract_child_value(idx, &child, datatype);
            self.observe_kind(idx, &value);
            values.push(UpdateField { index: idx, value });
        }
        values
    }

    fn push_subscription_data(
        &mut self,
        values: &mut SmallVec<[UpdateField; 8]>,
        subscription_data: Option<&Arc<str>>,
    ) {
        let Some(idx) = self.subscription_data_index else {
            return;
        };
        let value = subscription_data
            .cloned()
            .map(UpdateValue::Str)
            .unwrap_or(UpdateValue::Null);
        self.observe_kind(idx, &value);
        values.push(UpdateField { index: idx, value });
    }

    fn extract_child_value(
        &mut self,
        idx: FieldIndex,
        child: &xbbg_core::Element<'_>,
        datatype: BlpDataType,
    ) -> UpdateValue {
        if self.invalid_dateortime_fields[idx as usize] {
            UpdateValue::Null
        } else {
            self.update_value_for_field(idx as usize, child, datatype)
        }
    }

    fn update_value_for_field(
        &mut self,
        idx: usize,
        element: &xbbg_core::Element<'_>,
        datatype: BlpDataType,
    ) -> UpdateValue {
        UpdateValue::from_blp_with_str_cache(
            element.get_value_fast_with_datatype(0, datatype),
            self.string_value_cache.get_mut(idx),
        )
    }

    fn observe_kind(&mut self, idx: FieldIndex, value: &UpdateValue) {
        self.observe_field_kind(idx, FieldKind::from_value(value));
    }

    fn observe_field_kind(&mut self, idx: FieldIndex, observed: FieldKind) {
        let idx = idx as usize;
        let merged = self.field_kinds[idx].merge_observed(observed);
        if merged != self.field_kinds[idx] {
            self.field_kinds[idx] = merged;
            self.layout_version = self.layout_version.wrapping_add(1).max(1);
            self.layout =
                Self::build_layout(self.layout_version, &self.field_strings, &self.field_kinds);
        }
    }

    fn should_capture_datatype(datatype: BlpDataType) -> bool {
        !matches!(
            datatype,
            BlpDataType::Sequence
                | BlpDataType::Choice
                | BlpDataType::ByteArray
                | BlpDataType::CorrelationId
        )
    }

    fn cache_all_field_slot(&mut self, child_idx: usize, slot: AllFieldSlot) {
        if child_idx >= self.all_field_slots.len() {
            self.all_field_slots.resize(child_idx + 1, None);
        }
        self.all_field_slots[child_idx] = Some(slot);
    }

    fn ensure_field_for_child(
        &mut self,
        field: &xbbg_core::Element<'_>,
        field_key: usize,
    ) -> FieldIndex {
        if let Some(&idx) = self.field_name_keys.get(&field_key) {
            return idx;
        }

        let field_name = Arc::<str>::from(field.name_str());
        let idx = self.field_strings.len() as FieldIndex;
        let name = Name::get_or_intern(&field_name);
        self.field_strings.push(field_name.clone());
        self.field_names.push(name);
        self.field_name_keys.insert(field_key, idx);
        self.invalid_dateortime_fields
            .push(Self::is_invalid_dateortime_field(&field_name));
        self.field_kinds.push(FieldKind::Unknown);
        self.string_value_cache.push(None);
        self.layout_version = self.layout_version.wrapping_add(1).max(1);
        self.layout =
            Self::build_layout(self.layout_version, &self.field_strings, &self.field_kinds);
        idx
    }

    fn is_invalid_dateortime_field(field_name: &str) -> bool {
        Self::INVALID_DATEORTIME_FIELDS.contains(&field_name)
    }

    /// Handle DATALOSS indicator.
    pub fn on_dataloss(&mut self, timestamp_us: Option<i64>) {
        self.slow_consumer = true;
        self.metrics.slow_consumer.store(true, Ordering::Relaxed);
        self.metrics
            .data_loss_events
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.last_data_loss_us.store(
            timestamp_us.unwrap_or_default().max(0) as u64,
            Ordering::Relaxed,
        );
        xbbg_log::warn!(topic = %self.topic, "DATALOSS detected - slow consumer");
    }

    pub fn clear_slow_consumer(&mut self) {
        self.slow_consumer = false;
        self.metrics.slow_consumer.store(false, Ordering::Relaxed);
    }

    pub fn mark_closing(&mut self) {
        self.suppress_closed_warning = true;
    }

    /// Native updates are emitted immediately. This remains for existing worker
    /// shutdown/drop callsites that previously flushed Arrow builders.
    pub fn flush(&mut self) {}

    /// Send an error to the consumer.
    pub fn fail(&self, error: BlpError) {
        let _ = self.stream.try_send(Err(error));
    }

    fn send_update(&mut self, update: SubscriptionUpdate) {
        match self.overflow_policy {
            OverflowPolicy::Block => {
                let result = match &self.forwarder {
                    Some(forwarder) => forwarder.try_forward(
                        self.stream.clone(),
                        update,
                        Arc::clone(&self.metrics),
                        Arc::clone(&self.topic),
                    ),
                    None => self
                        .stream
                        .try_send(Ok(update))
                        .map_err(|error| match error {
                            mpsc::error::TrySendError::Full(_) => {
                                mpsc::error::TrySendError::Full(())
                            }
                            mpsc::error::TrySendError::Closed(_) => {
                                mpsc::error::TrySendError::Closed(())
                            }
                        }),
                };
                match result {
                    Ok(()) if self.forwarder.is_none() => {
                        self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(())) => {
                        self.dropped_batches += 1;
                        self.metrics.dropped_batches.fetch_add(1, Ordering::Relaxed);
                        self.metrics.slow_consumer.store(true, Ordering::Relaxed);
                        if self.dropped_batches == 1 || self.dropped_batches.is_multiple_of(1024) {
                            xbbg_log::warn!(
                                topic = %self.topic,
                                dropped = self.dropped_batches,
                                policy = "Block",
                                "bounded forwarding queue full - dropping update"
                            );
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(())) => {
                        if !self.suppress_closed_warning {
                            xbbg_log::warn!(topic = %self.topic, "stream closed");
                        }
                    }
                }
            }
            OverflowPolicy::DropNewest => match self.stream.try_send(Ok(update)) {
                Ok(()) => {
                    self.metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.dropped_batches += 1;
                    self.metrics.dropped_batches.fetch_add(1, Ordering::Relaxed);
                    xbbg_log::warn!(
                        topic = %self.topic,
                        dropped = self.dropped_batches,
                        policy = "DropNewest",
                        "stream full - dropping update"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    if !self.suppress_closed_warning {
                        xbbg_log::warn!(topic = %self.topic, "stream closed");
                    }
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_names_stay_aligned_with_requested_fields() {
        let (tx, _rx) = mpsc::channel(1);
        let state = SubscriptionState::new(
            "AAPL US Equity".to_string(),
            vec!["LAST_PRICE".to_string(), "LAST_PRICE".to_string()],
            tx,
            10,
            false,
        );

        assert_eq!(
            state
                .field_strings
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            vec![
                "LAST_PRICE".to_string(),
                "MKTDATA_EVENT_TYPE".to_string(),
                "MKTDATA_EVENT_SUBTYPE".to_string(),
            ]
        );
        assert_eq!(state.field_names.len(), state.field_strings.len());
        assert_eq!(
            state.invalid_dateortime_fields.len(),
            state.field_strings.len()
        );
        for (field, name) in state.field_strings.iter().zip(state.field_names.iter()) {
            assert_eq!(name.as_str(), field.as_ref());
        }
    }

    #[test]
    fn mktbar_topics_include_subscription_data_metadata() {
        let (tx, _rx) = mpsc::channel(1);
        let state = SubscriptionState::new(
            "//blp/mktbar/ticker/EURUSD Curncy".to_string(),
            vec!["LAST_PRICE".to_string()],
            tx,
            10,
            false,
        );

        assert_eq!(
            state
                .field_strings
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            vec![
                "LAST_PRICE".to_string(),
                "MKTDATA_EVENT_TYPE".to_string(),
                "MKTDATA_EVENT_SUBTYPE".to_string(),
                "SUBSCRIPTION_DATA".to_string(),
            ]
        );
        assert_eq!(state.subscription_data_index, Some(3));
    }

    #[test]
    fn invalid_dateortime_guard_is_preserved_in_layout() {
        let (tx, _rx) = mpsc::channel(1);
        let state = SubscriptionState::new(
            "AAPL US Equity".to_string(),
            vec!["LAST_UPDATE_ALL_SESSIONS_RT".to_string()],
            tx,
            1,
            false,
        );

        assert!(state.invalid_dateortime_fields[0]);
    }
    fn test_update(topic_id: TopicId) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us: topic_id as i64,
            topic_id,
            topic: Arc::from("TEST"),
            layout: Arc::new(FieldLayout::new(1, Vec::new())),
            values: SmallVec::new(),
        }
    }

    fn test_metrics() -> Arc<SubscriptionMetrics> {
        Arc::new(SubscriptionMetrics {
            messages_received: Arc::new(AtomicU64::new(0)),
            dropped_batches: Arc::new(AtomicU64::new(0)),
            batches_sent: Arc::new(AtomicU64::new(0)),
            slow_consumer: Arc::new(AtomicBool::new(false)),
            data_loss_events: Arc::new(AtomicU64::new(0)),
            last_message_us: Arc::new(AtomicU64::new(0)),
            last_data_loss_us: Arc::new(AtomicU64::new(0)),
        })
    }

    #[test]
    fn block_policy_never_waits_when_forwarding_queue_is_full() {
        let (consumer_tx, _consumer_rx) = mpsc::channel(1);
        let (forwarder, _task) = subscription_forwarder_channel(1);
        forwarder
            .try_forward(
                consumer_tx.clone(),
                test_update(1),
                test_metrics(),
                Arc::from("TEST"),
            )
            .expect("fill forwarding queue");
        let mut state = SubscriptionState::with_policy_and_forwarder(
            "TEST".to_string(),
            Vec::new(),
            consumer_tx,
            1,
            OverflowPolicy::Block,
            false,
            Some(forwarder),
        );

        let started = std::time::Instant::now();
        state.send_update(test_update(2));

        assert!(started.elapsed() < Duration::from_millis(10));
        assert_eq!(state.metrics.dropped_batches.load(Ordering::Relaxed), 1);
        assert!(state.metrics.slow_consumer.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn block_forwarder_preserves_enqueue_order() {
        let (consumer_tx, mut consumer_rx) = mpsc::channel(4);
        let (forwarder, task) = subscription_forwarder_channel(4);
        let handle = tokio::spawn(task);
        let metrics = test_metrics();
        for topic_id in [10, 11, 12] {
            forwarder
                .try_forward(
                    consumer_tx.clone(),
                    test_update(topic_id),
                    Arc::clone(&metrics),
                    Arc::from("TEST"),
                )
                .expect("enqueue update");
        }
        forwarder.drain().await.unwrap();
        for expected in [10, 11, 12] {
            let update = consumer_rx
                .try_recv()
                .expect("forwarded item")
                .expect("successful update");
            assert_eq!(update.topic_id, expected);
        }
        drop(forwarder);
        handle.await.expect("forwarder task");
        assert_eq!(metrics.batches_sent.load(Ordering::Relaxed), 3);
    }
}
