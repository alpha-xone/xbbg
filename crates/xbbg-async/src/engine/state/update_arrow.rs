use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use xbbg_core::BlpError;

use super::update::{FieldKind, FieldLayout, SubscriptionUpdate, UpdateValue};

pub struct SubscriptionArrowBatcher {
    layout: Option<Arc<FieldLayout>>,
    schema: Option<SchemaRef>,
    builders: Vec<SubscriptionColumnBuilder>,
    scratch: Vec<Option<usize>>,
    rows: usize,
}

impl SubscriptionArrowBatcher {
    pub fn new() -> Self {
        Self {
            layout: None,
            schema: None,
            builders: Vec::new(),
            scratch: Vec::new(),
            rows: 0,
        }
    }

    /// Append one update, flushing pending rows first when the layout changes.
    pub fn append(&mut self, update: &SubscriptionUpdate) -> Option<RecordBatch> {
        if !self.matches_layout(&update.layout) {
            let batch = self.flush();
            self.rebuild_for_layout(update.layout.clone());
            self.append_current_layout(update);
            return batch;
        }

        self.append_current_layout(update);
        None
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Build a batch from pending rows. Schema and builders are retained for reuse.
    pub fn flush(&mut self) -> Option<RecordBatch> {
        if self.rows == 0 {
            return None;
        }

        let schema = self
            .schema
            .as_ref()
            .expect("subscription arrow schema initialized")
            .clone();
        let columns: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(SubscriptionColumnBuilder::finish)
            .collect();
        self.rows = 0;

        Some(
            RecordBatch::try_new(schema, columns)
                .expect("subscription arrow builders must match cached schema"),
        )
    }

    fn matches_layout(&self, layout: &Arc<FieldLayout>) -> bool {
        self.layout.as_ref().is_some_and(|current| {
            Arc::ptr_eq(current, layout) || current.version == layout.version
        })
    }

    fn rebuild_for_layout(&mut self, layout: Arc<FieldLayout>) {
        let mut fields = Vec::with_capacity(layout.fields.len() + 2);
        fields.push(Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ));
        fields.push(Field::new("topic", DataType::Utf8, false));
        fields.extend(layout.fields.iter().map(|meta| {
            Field::new(meta.name.as_ref(), arrow_datatype(meta.kind), true)
        }));

        let mut builders = Vec::with_capacity(fields.len());
        builders.push(SubscriptionColumnBuilder::Timestamp(
            TimestampMicrosecondBuilder::new(),
        ));
        builders.push(SubscriptionColumnBuilder::Topic(StringBuilder::new()));
        builders.extend(
            layout
                .fields
                .iter()
                .map(|meta| SubscriptionColumnBuilder::for_kind(meta.kind)),
        );

        self.layout = Some(layout);
        self.schema = Some(Arc::new(Schema::new(fields)));
        self.builders = builders;
        self.scratch.clear();
    }

    fn append_current_layout(&mut self, update: &SubscriptionUpdate) {
        let layout = self
            .layout
            .as_ref()
            .expect("subscription arrow layout initialized")
            .clone();

        self.scratch.clear();
        self.scratch.resize(layout.fields.len(), None);
        for (position, field) in update.values.iter().enumerate() {
            if let Some(slot) = self.scratch.get_mut(field.index as usize) {
                *slot = Some(position);
            }
        }

        self.builders[0].append_timestamp(update.timestamp_us);
        self.builders[1].append_topic(update.topic.as_ref());

        for (position, meta) in layout.fields.iter().enumerate() {
            let value = self
                .scratch
                .get(meta.index as usize)
                .and_then(|slot| slot.map(|value_position| &update.values[value_position].value));
            self.builders[position + 2].append_update(value);
        }

        self.rows += 1;
    }
}

impl Default for SubscriptionArrowBatcher {
    fn default() -> Self {
        Self::new()
    }
}

pub fn subscription_update_to_record_batch(
    update: &SubscriptionUpdate,
) -> Result<RecordBatch, BlpError> {
    let mut batcher = SubscriptionArrowBatcher::new();
    batcher.append(update);
    Ok(batcher
        .flush()
        .expect("subscription update batcher should flush appended row"))
}

fn arrow_datatype(kind: FieldKind) -> DataType {
    match kind {
        FieldKind::Unknown | FieldKind::Str => DataType::Utf8,
        FieldKind::Bool => DataType::Boolean,
        FieldKind::I32 => DataType::Int32,
        FieldKind::I64 => DataType::Int64,
        FieldKind::F64 => DataType::Float64,
        FieldKind::Date32 => DataType::Date32,
        FieldKind::Time64Micros => DataType::Time64(TimeUnit::Microsecond),
        FieldKind::TimestampMicros => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
    }
}

enum SubscriptionColumnBuilder {
    Timestamp(TimestampMicrosecondBuilder),
    Topic(StringBuilder),
    Bool(BooleanBuilder),
    I32(Int32Builder),
    I64(Int64Builder),
    F64(Float64Builder),
    String(StringBuilder),
    Date32(Date32Builder),
    Time64Micros(Time64MicrosecondBuilder),
    TimestampMicros(TimestampMicrosecondBuilder),
}

impl SubscriptionColumnBuilder {
    fn for_kind(kind: FieldKind) -> Self {
        match kind {
            FieldKind::Unknown | FieldKind::Str => Self::String(StringBuilder::new()),
            FieldKind::Bool => Self::Bool(BooleanBuilder::new()),
            FieldKind::I32 => Self::I32(Int32Builder::new()),
            FieldKind::I64 => Self::I64(Int64Builder::new()),
            FieldKind::F64 => Self::F64(Float64Builder::new()),
            FieldKind::Date32 => Self::Date32(Date32Builder::new()),
            FieldKind::Time64Micros => Self::Time64Micros(Time64MicrosecondBuilder::new()),
            FieldKind::TimestampMicros => Self::TimestampMicros(TimestampMicrosecondBuilder::new()),
        }
    }

    fn append_timestamp(&mut self, value: i64) {
        match self {
            Self::Timestamp(builder) => builder.append_value(value),
            _ => unreachable!("timestamp append used with non-timestamp builder"),
        }
    }

    fn append_topic(&mut self, value: &str) {
        match self {
            Self::Topic(builder) => builder.append_value(value),
            _ => unreachable!("topic append used with non-topic builder"),
        }
    }

    fn append_update(&mut self, value: Option<&UpdateValue>) {
        match self {
            Self::Timestamp(_) | Self::Topic(_) => {
                unreachable!("fixed subscription columns are appended separately")
            }
            Self::Bool(builder) => match value {
                Some(UpdateValue::Bool(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
            Self::I32(builder) => match value {
                Some(UpdateValue::I32(value)) => builder.append_value(*value),
                Some(UpdateValue::I64(value)) => builder.append_value(*value as i32),
                _ => builder.append_null(),
            },
            Self::I64(builder) => match value {
                Some(UpdateValue::I64(value)) => builder.append_value(*value),
                Some(UpdateValue::I32(value)) => builder.append_value(*value as i64),
                _ => builder.append_null(),
            },
            Self::F64(builder) => match value {
                Some(UpdateValue::F64(value)) => builder.append_value(*value),
                Some(UpdateValue::I32(value)) => builder.append_value(*value as f64),
                Some(UpdateValue::I64(value)) => builder.append_value(*value as f64),
                _ => builder.append_null(),
            },
            Self::String(builder) => append_string_value(builder, value),
            Self::Date32(builder) => match value {
                Some(UpdateValue::Date32(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
            Self::Time64Micros(builder) => match value {
                Some(UpdateValue::Time64Micros(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
            Self::TimestampMicros(builder) => match value {
                Some(UpdateValue::TimestampMicros(value)) => builder.append_value(*value),
                _ => builder.append_null(),
            },
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Timestamp(builder) => Arc::new(builder.finish().with_timezone("UTC")),
            Self::Topic(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::I32(builder) => Arc::new(builder.finish()),
            Self::I64(builder) => Arc::new(builder.finish()),
            Self::F64(builder) => Arc::new(builder.finish()),
            Self::String(builder) => Arc::new(builder.finish()),
            Self::Date32(builder) => Arc::new(builder.finish()),
            Self::Time64Micros(builder) => Arc::new(builder.finish()),
            Self::TimestampMicros(builder) => Arc::new(builder.finish().with_timezone("UTC")),
        }
    }
}

fn append_string_value(builder: &mut StringBuilder, value: Option<&UpdateValue>) {
    match value {
        Some(UpdateValue::Bool(value)) => builder.append_value(if *value { "true" } else { "false" }),
        Some(UpdateValue::I32(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::I64(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::F64(value)) if value.is_finite() => {
            let mut buffer = ryu::Buffer::new();
            builder.append_value(buffer.format_finite(*value));
        }
        Some(UpdateValue::F64(value)) => {
            let value = value.to_string();
            builder.append_value(&value);
        }
        Some(UpdateValue::Str(value)) => builder.append_value(value.as_ref()),
        Some(UpdateValue::Date32(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::Time64Micros(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::TimestampMicros(value)) => {
            let mut buffer = itoa::Buffer::new();
            builder.append_value(buffer.format(*value));
        }
        Some(UpdateValue::Null) | None => builder.append_null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::state::update::{FieldMeta, UpdateField};
    use arrow_array::{
        Array, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
    };

    fn layout(version: u32, fields: Vec<FieldMeta>) -> Arc<FieldLayout> {
        Arc::new(FieldLayout::new(version, fields))
    }

    fn update(
        timestamp_us: i64,
        topic: &str,
        layout: Arc<FieldLayout>,
        values: impl IntoIterator<Item = UpdateField>,
    ) -> SubscriptionUpdate {
        SubscriptionUpdate {
            timestamp_us,
            topic_id: 1,
            topic: Arc::from(topic),
            layout,
            values: values.into_iter().collect(),
        }
    }

    fn update_field(index: u16, value: UpdateValue) -> UpdateField {
        UpdateField { index, value }
    }

    #[test]
    fn wrapper_schema_matches_legacy_shape() {
        let layout = layout(
            1,
            vec![
                FieldMeta::new("BID", 0, FieldKind::F64),
                FieldMeta::new("ASK_SIZE", 1, FieldKind::I32),
                FieldMeta::new("OPEN", 2, FieldKind::Date32),
                FieldMeta::new("ACTIVE", 3, FieldKind::Bool),
                FieldMeta::new("LAST_UPDATE", 4, FieldKind::TimestampMicros),
            ],
        );
        let update = update(
            10,
            "IBM US Equity",
            layout,
            [
                update_field(0, UpdateValue::F64(1.25)),
                update_field(1, UpdateValue::I32(100)),
                update_field(2, UpdateValue::Date32(20_000)),
                update_field(3, UpdateValue::Bool(true)),
                update_field(4, UpdateValue::TimestampMicros(11)),
            ],
        );

        let batch = subscription_update_to_record_batch(&update).unwrap();
        let expected = Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("topic", DataType::Utf8, false),
            Field::new("BID", DataType::Float64, true),
            Field::new("ASK_SIZE", DataType::Int32, true),
            Field::new("OPEN", DataType::Date32, true),
            Field::new("ACTIVE", DataType::Boolean, true),
            Field::new(
                "LAST_UPDATE",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);

        assert_eq!(batch.schema().as_ref(), &expected);
    }

    #[test]
    fn multi_append_flush_preserves_rows_and_sparse_nulls() {
        let layout = layout(
            1,
            vec![
                FieldMeta::new("BID", 0, FieldKind::F64),
                FieldMeta::new("ASK", 1, FieldKind::F64),
                FieldMeta::new("STATUS", 2, FieldKind::Str),
            ],
        );
        let mut batcher = SubscriptionArrowBatcher::new();

        assert!(batcher
            .append(&update(
                10,
                "IBM US Equity",
                layout.clone(),
                [
                    update_field(0, UpdateValue::F64(1.25)),
                    update_field(2, UpdateValue::Str(Arc::from("OK"))),
                ],
            ))
            .is_none());
        assert!(batcher
            .append(&update(
                20,
                "MSFT US Equity",
                layout,
                [update_field(1, UpdateValue::F64(2.5))],
            ))
            .is_none());

        let batch = batcher.flush().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(timestamps.value(0), 10);
        assert_eq!(timestamps.value(1), 20);
        let topics = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(topics.value(0), "IBM US Equity");
        assert_eq!(topics.value(1), "MSFT US Equity");
        let bid = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(bid.value(0), 1.25);
        assert!(bid.is_null(1));
        let ask = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(ask.is_null(0));
        assert_eq!(ask.value(1), 2.5);
        let status = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(status.value(0), "OK");
        assert!(status.is_null(1));
    }

    #[test]
    fn layout_change_flushes_old_rows_and_keeps_new_layout_pending() {
        let first_layout = layout(1, vec![FieldMeta::new("BID", 0, FieldKind::F64)]);
        let second_layout = layout(2, vec![FieldMeta::new("ASK_SIZE", 0, FieldKind::I32)]);
        let mut batcher = SubscriptionArrowBatcher::new();

        assert!(batcher
            .append(&update(
                10,
                "IBM US Equity",
                first_layout,
                [update_field(0, UpdateValue::F64(1.25))],
            ))
            .is_none());
        let old_batch = batcher
            .append(&update(
                20,
                "IBM US Equity",
                second_layout,
                [update_field(0, UpdateValue::I32(100))],
            ))
            .unwrap();

        assert_eq!(old_batch.num_rows(), 1);
        assert_eq!(old_batch.schema().field(2).name(), "BID");
        assert_eq!(
            old_batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.25
        );
        assert_eq!(batcher.rows(), 1);

        let new_batch = batcher.flush().unwrap();
        assert_eq!(new_batch.num_rows(), 1);
        assert_eq!(new_batch.schema().field(2).name(), "ASK_SIZE");
        assert_eq!(
            new_batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            100
        );
    }

    #[test]
    fn builders_are_reused_after_flush() {
        let layout = layout(1, vec![FieldMeta::new("BID", 0, FieldKind::F64)]);
        let mut batcher = SubscriptionArrowBatcher::new();

        batcher.append(&update(
            10,
            "IBM US Equity",
            layout.clone(),
            [update_field(0, UpdateValue::F64(1.25))],
        ));
        let first = batcher.flush().unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(
            first
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.25
        );
        assert!(batcher.is_empty());

        batcher.append(&update(
            20,
            "IBM US Equity",
            layout,
            [update_field(0, UpdateValue::F64(2.5))],
        ));
        let second = batcher.flush().unwrap();
        assert_eq!(second.num_rows(), 1);
        assert_eq!(
            second
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.5
        );
    }

    #[test]
    fn arrow_adapter_null_fills_sparse_layout() {
        let layout = layout(
            2,
            vec![
                FieldMeta::new("BID", 0, FieldKind::F64),
                FieldMeta::new("ASK", 1, FieldKind::F64),
            ],
        );
        let update = update(
            10,
            "IBM US Equity",
            layout,
            [update_field(0, UpdateValue::F64(1.25))],
        );

        let batch = subscription_update_to_record_batch(&update).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(2).name(), "BID");
        assert_eq!(batch.schema().field(3).name(), "ASK");
        let ask = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(ask.is_null(0));
    }
}
