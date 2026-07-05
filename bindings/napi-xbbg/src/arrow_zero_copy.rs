use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, NullArray,
    StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::buffer::Buffer as ArrowBuffer;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use napi::bindgen_prelude::{
    Buffer, BufferSlice, Env, JsObjectValue, JsValue, Object, ToNapiValue,
};
use napi::{Error, Result, Status};

use crate::StringPair;

pub struct NativeArrowBatch {
    num_rows: usize,
    columns: Vec<NativeArrowColumn>,
    metadata: Vec<StringPair>,
}

struct NativeArrowColumn {
    batch: Arc<RecordBatch>,
    name: String,
    arrow_type: NativeArrowType,
    nullable: bool,
    length: usize,
    null_count: usize,
    data: Option<ArrowBuffer>,
    offsets: Option<ArrowBuffer>,
    null_bitmap: Option<ArrowBuffer>,
}

#[derive(Clone)]
enum NativeArrowType {
    Bool,
    Binary,
    Date32,
    Date64,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    LargeBinary,
    LargeUtf8,
    Null,
    Time32Millisecond,
    Time32Second,
    Time64Microsecond,
    Time64Nanosecond,
    TimestampMicrosecond { timezone: Option<String> },
    TimestampMillisecond { timezone: Option<String> },
    TimestampNanosecond { timezone: Option<String> },
    TimestampSecond { timezone: Option<String> },
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Utf8,
}

struct ExternalBufferOwner {
    _batch: Arc<RecordBatch>,
    _buffer: ArrowBuffer,
}

fn checked_js_u32_len(label: &str, value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| {
        Error::new(
            Status::InvalidArg,
            format!("{label} {value} exceeds the JavaScript zero-copy Arrow length limit"),
        )
    })
}
impl NativeArrowBatch {
    pub fn from_record_batch(batch: RecordBatch) -> Result<Self> {
        let unsupported = unsupported_columns(&batch);
        if !unsupported.is_empty() {
            return Err(Error::new(
                Status::GenericFailure,
                format!(
                    "zero-copy subscription transfer does not support this Arrow schema. \
                     Unsupported columns: {}. Supported subscription column types are: \
                     bool, binary, date32, date64, float32, float64, int8, int16, int32, int64, \
                     large_binary, large_utf8, null, time32[s], time32[ms], time64[us], time64[ns], \
                     timestamp[s], timestamp[ms], timestamp[us], timestamp[ns], uint8, uint16, uint32, uint64, utf8. \
                     Sliced Arrow arrays are not supported.",
                    unsupported.join("; ")
                ),
            ));
        }

        let schema = batch.schema();
        let metadata = schema
            .metadata()
            .iter()
            .map(|(key, value)| StringPair {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let batch = Arc::new(batch);
        let schema = batch.schema();
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, array)| {
                let field = schema.field(idx);
                NativeArrowColumn::from_array(
                    batch.clone(),
                    field.name().clone(),
                    field.is_nullable(),
                    array.as_ref(),
                )
            })
            .collect();

        Ok(Self {
            num_rows: batch.num_rows(),
            columns,
            metadata,
        })
    }
}

impl NativeArrowColumn {
    fn from_array(
        batch: Arc<RecordBatch>,
        name: String,
        nullable: bool,
        array: &dyn Array,
    ) -> Self {
        let null_bitmap = array.nulls().map(|nulls| nulls.inner().inner().clone());
        let null_count = array.null_count();
        let length = array.len();

        macro_rules! primitive_column {
            ($array_ty:ty, $arrow_type:expr, $expect:literal) => {{
                let values = array
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .expect($expect)
                    .values()
                    .inner()
                    .clone();
                Self {
                    batch,
                    name,
                    arrow_type: $arrow_type,
                    nullable,
                    length,
                    null_count,
                    data: Some(values),
                    offsets: None,
                    null_bitmap,
                }
            }};
        }

        macro_rules! variable_width_column {
            ($array_ty:ty, $arrow_type:expr, $expect:literal) => {{
                let array = array.as_any().downcast_ref::<$array_ty>().expect($expect);
                Self {
                    batch,
                    name,
                    arrow_type: $arrow_type,
                    nullable,
                    length,
                    null_count,
                    data: Some(array.values().clone()),
                    offsets: Some(array.offsets().inner().inner().clone()),
                    null_bitmap,
                }
            }};
        }

        match array.data_type() {
            DataType::Boolean => {
                let values = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("supported boolean array")
                    .values()
                    .inner()
                    .clone();
                Self {
                    batch,
                    name,
                    arrow_type: NativeArrowType::Bool,
                    nullable,
                    length,
                    null_count,
                    data: Some(values),
                    offsets: None,
                    null_bitmap,
                }
            }
            DataType::Binary => variable_width_column!(
                BinaryArray,
                NativeArrowType::Binary,
                "supported binary array"
            ),
            DataType::Date32 => primitive_column!(
                Date32Array,
                NativeArrowType::Date32,
                "supported date32 array"
            ),
            DataType::Date64 => primitive_column!(
                Date64Array,
                NativeArrowType::Date64,
                "supported date64 array"
            ),
            DataType::Float32 => primitive_column!(
                Float32Array,
                NativeArrowType::Float32,
                "supported float32 array"
            ),
            DataType::Float64 => primitive_column!(
                Float64Array,
                NativeArrowType::Float64,
                "supported float64 array"
            ),
            DataType::Int8 => {
                primitive_column!(Int8Array, NativeArrowType::Int8, "supported int8 array")
            }
            DataType::Int16 => {
                primitive_column!(Int16Array, NativeArrowType::Int16, "supported int16 array")
            }
            DataType::Int32 => {
                primitive_column!(Int32Array, NativeArrowType::Int32, "supported int32 array")
            }
            DataType::Int64 => {
                primitive_column!(Int64Array, NativeArrowType::Int64, "supported int64 array")
            }
            DataType::LargeBinary => variable_width_column!(
                LargeBinaryArray,
                NativeArrowType::LargeBinary,
                "supported large_binary array"
            ),
            DataType::LargeUtf8 => variable_width_column!(
                LargeStringArray,
                NativeArrowType::LargeUtf8,
                "supported large_utf8 array"
            ),
            DataType::Null => {
                let _ = array
                    .as_any()
                    .downcast_ref::<NullArray>()
                    .expect("supported null array");
                Self {
                    batch,
                    name,
                    arrow_type: NativeArrowType::Null,
                    nullable,
                    length,
                    null_count,
                    data: None,
                    offsets: None,
                    null_bitmap,
                }
            }
            DataType::Time32(TimeUnit::Second) => primitive_column!(
                Time32SecondArray,
                NativeArrowType::Time32Second,
                "supported time32[s] array"
            ),
            DataType::Time32(TimeUnit::Millisecond) => primitive_column!(
                Time32MillisecondArray,
                NativeArrowType::Time32Millisecond,
                "supported time32[ms] array"
            ),
            DataType::Time64(TimeUnit::Microsecond) => primitive_column!(
                Time64MicrosecondArray,
                NativeArrowType::Time64Microsecond,
                "supported time64[us] array"
            ),
            DataType::Time64(TimeUnit::Nanosecond) => primitive_column!(
                Time64NanosecondArray,
                NativeArrowType::Time64Nanosecond,
                "supported time64[ns] array"
            ),
            DataType::Timestamp(TimeUnit::Second, timezone) => primitive_column!(
                TimestampSecondArray,
                NativeArrowType::TimestampSecond {
                    timezone: timezone.as_ref().map(|tz| tz.to_string()),
                },
                "supported timestamp[s] array"
            ),
            DataType::Timestamp(TimeUnit::Millisecond, timezone) => primitive_column!(
                TimestampMillisecondArray,
                NativeArrowType::TimestampMillisecond {
                    timezone: timezone.as_ref().map(|tz| tz.to_string()),
                },
                "supported timestamp[ms] array"
            ),
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => primitive_column!(
                TimestampMicrosecondArray,
                NativeArrowType::TimestampMicrosecond {
                    timezone: timezone.as_ref().map(|tz| tz.to_string()),
                },
                "supported timestamp[us] array"
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, timezone) => primitive_column!(
                TimestampNanosecondArray,
                NativeArrowType::TimestampNanosecond {
                    timezone: timezone.as_ref().map(|tz| tz.to_string()),
                },
                "supported timestamp[ns] array"
            ),
            DataType::UInt8 => {
                primitive_column!(UInt8Array, NativeArrowType::UInt8, "supported uint8 array")
            }
            DataType::UInt16 => primitive_column!(
                UInt16Array,
                NativeArrowType::UInt16,
                "supported uint16 array"
            ),
            DataType::UInt32 => primitive_column!(
                UInt32Array,
                NativeArrowType::UInt32,
                "supported uint32 array"
            ),
            DataType::UInt64 => primitive_column!(
                UInt64Array,
                NativeArrowType::UInt64,
                "supported uint64 array"
            ),
            DataType::Utf8 => {
                variable_width_column!(StringArray, NativeArrowType::Utf8, "supported utf8 array")
            }
            _ => unreachable!("unsupported array checked before conversion"),
        }
    }
}

impl NativeArrowType {
    fn label(&self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Binary => "binary",
            Self::Date32 => "date32",
            Self::Date64 => "date64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::LargeBinary => "large_binary",
            Self::LargeUtf8 => "large_utf8",
            Self::Null => "null",
            Self::Time32Millisecond => "time32_ms",
            Self::Time32Second => "time32_s",
            Self::Time64Microsecond => "time64_us",
            Self::Time64Nanosecond => "time64_ns",
            Self::TimestampMicrosecond { .. } => "timestamp_us",
            Self::TimestampMillisecond { .. } => "timestamp_ms",
            Self::TimestampNanosecond { .. } => "timestamp_ns",
            Self::TimestampSecond { .. } => "timestamp_s",
            Self::UInt8 => "uint8",
            Self::UInt16 => "uint16",
            Self::UInt32 => "uint32",
            Self::UInt64 => "uint64",
            Self::Utf8 => "utf8",
        }
    }

    fn timezone(&self) -> Option<&str> {
        match self {
            Self::TimestampMicrosecond { timezone }
            | Self::TimestampMillisecond { timezone }
            | Self::TimestampNanosecond { timezone }
            | Self::TimestampSecond { timezone } => timezone.as_deref(),
            _ => None,
        }
    }
}

fn unsupported_columns(batch: &RecordBatch) -> Vec<String> {
    let schema = batch.schema();
    batch
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(idx, array)| {
            unsupported_array_reason(array.as_ref()).map(|reason| {
                let field = schema.field(idx);
                format!("#{idx} '{}' ({reason})", field.name())
            })
        })
        .collect()
}

fn unsupported_array_reason(array: &dyn Array) -> Option<String> {
    if array.offset() != 0 {
        return Some(format!(
            "sliced array offset={} type={:?}",
            array.offset(),
            array.data_type()
        ));
    }

    match array.data_type() {
        DataType::Boolean
        | DataType::Binary
        | DataType::Date32
        | DataType::Date64
        | DataType::Float32
        | DataType::Float64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::LargeBinary
        | DataType::LargeUtf8
        | DataType::Null
        | DataType::Time32(TimeUnit::Second)
        | DataType::Time32(TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond)
        | DataType::Time64(TimeUnit::Nanosecond)
        | DataType::Timestamp(TimeUnit::Second, _)
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Timestamp(TimeUnit::Nanosecond, _)
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8 => None,
        data_type => Some(format!("unsupported type={data_type:?}")),
    }
}

/// Expose an Arrow buffer to JS as a zero-copy V8 `Buffer`.
///
/// The returned `Buffer` aliases the Arrow allocation directly — no bytes are
/// copied. The Arrow buffer is logically **immutable**: JS must treat the
/// resulting `Buffer` as read-only. Mutating it would corrupt the shared Arrow
/// data (and violate Arrow's immutability invariants for any other holder of
/// the same `RecordBatch`).
fn external_buffer(
    env: &Env,
    batch: Arc<RecordBatch>,
    buffer: ArrowBuffer,
) -> Result<Option<Buffer>> {
    let len = buffer.len();
    if len == 0 {
        return Ok(None);
    }

    // `from_external` takes `*mut u8`, so the `*const` from `as_ptr()` must be
    // cast. The pointer is never written through: the buffer is treated as
    // immutable on both the Rust and JS sides (see the doc comment above).
    let data = buffer.as_ptr() as *mut u8;
    let owner = ExternalBufferOwner {
        _batch: batch,
        _buffer: buffer,
    };
    // SAFETY: `data`/`len` describe the live Arrow allocation, and `owner`
    // holds the `Arc<RecordBatch>` plus the backing `Buffer` for the entire V8
    // lifetime of the slice. The finalizer below drops `owner` only after V8 is
    // done with the buffer, so the pointer can never dangle. The data is never
    // mutated through `data`, satisfying Arrow's immutability requirement.
    let slice = unsafe {
        BufferSlice::from_external(env, data, len, owner, |_env, _owner| {
            // Dropping the owner releases the Arrow buffer/RecordBatch once V8 is done.
        })?
    };
    slice.into_buffer(env).map(Some)
}

impl ToNapiValue for NativeArrowBatch {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut obj = Object::new(&env)?;
        let mut metadata = Object::new(&env)?;
        for pair in value.metadata {
            metadata.set_named_property(&pair.key, pair.value)?;
        }
        obj.set_named_property("kind", "zeroCopy")?;
        obj.set_named_property("numRows", checked_js_u32_len("numRows", value.num_rows)?)?;
        obj.set_named_property("columns", value.columns)?;
        obj.set_named_property("metadata", metadata)?;
        Ok(obj.raw())
    }
}

impl ToNapiValue for NativeArrowColumn {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut obj = Object::new(&env)?;
        obj.set_named_property("name", value.name)?;
        obj.set_named_property("type", value.arrow_type.label())?;
        obj.set_named_property("nullable", value.nullable)?;
        obj.set_named_property("length", checked_js_u32_len("length", value.length)?)?;
        obj.set_named_property(
            "nullCount",
            checked_js_u32_len("nullCount", value.null_count)?,
        )?;
        if let Some(timezone) = value.arrow_type.timezone() {
            obj.set_named_property("timezone", timezone)?;
        }
        if let Some(buffer) = value.data {
            if let Some(buffer) = external_buffer(&env, value.batch.clone(), buffer).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to expose Arrow data buffer: {e}"),
                )
            })? {
                obj.set_named_property("data", buffer)?;
            }
        }
        if let Some(buffer) = value.offsets {
            if let Some(buffer) = external_buffer(&env, value.batch.clone(), buffer).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to expose Arrow offsets buffer: {e}"),
                )
            })? {
                obj.set_named_property("offsets", buffer)?;
            }
        }
        if let Some(buffer) = value.null_bitmap {
            if let Some(buffer) = external_buffer(&env, value.batch, buffer).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to expose Arrow null bitmap: {e}"),
                )
            })? {
                obj.set_named_property("nullBitmap", buffer)?;
            }
        }
        Ok(obj.raw())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_js_u32_len_accepts_u32_max() {
        assert_eq!(
            checked_js_u32_len("length", u32::MAX as usize).unwrap(),
            u32::MAX
        );
    }

    #[test]
    fn checked_js_u32_len_rejects_overflow() {
        let err = checked_js_u32_len("length", (u32::MAX as usize) + 1).unwrap_err();
        assert_eq!(err.status, Status::InvalidArg);
        assert!(err
            .reason
            .contains("exceeds the JavaScript zero-copy Arrow length limit"));
    }
}
