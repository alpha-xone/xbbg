//! Shared utility functions used across recipe modules.
use chrono::NaiveDate;

use arrow_array::{
    Array, ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray,
};

use crate::error::{RecipeError, Result};

/// Extract a value from an Arrow array at `idx` as a `String`.
///
/// Supports `StringArray`, `LargeStringArray`, numeric arrays, and `Date32Array`.
/// Returns `None` for null values, out-of-bounds indices, or unsupported array
/// types.
pub fn array_value_as_string(array: &ArrayRef, idx: usize) -> Option<String> {
    if idx >= array.len() || array.is_null(idx) {
        return None;
    }

    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Some(arr.value(idx).to_string());
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(arr.value(idx).to_string());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(arr.value(idx).to_string());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(arr.value(idx).to_string());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(arr.value(idx).to_string());
    }
    if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
        return date32_to_naive(arr.value(idx)).map(|date| date.format("%Y-%m-%d").to_string());
    }

    None
}

/// Convert a `Date32` value (days since Unix epoch) to a `chrono::NaiveDate`.
pub fn date32_to_naive(days_since_epoch: i32) -> Option<chrono::NaiveDate> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    epoch.checked_add_signed(chrono::Duration::days(days_since_epoch as i64))
}

/// Borrow a column from a `RecordBatch` as a `&StringArray`, returning an
/// error if the column is missing or is not `Utf8`.
pub fn as_string_col<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(column)
        .ok_or_else(|| RecipeError::Other(format!("missing '{column}' column")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| RecipeError::Other(format!("'{column}' column must be Utf8")))
}

/// Clean a Bloomberg text value, mapping blank and sentinel values
/// (`nan`, `n/a`, `#n/a`, `null`, case-insensitive) to `None`.
pub(crate) fn clean_bloomberg_text(value: &str) -> Option<String> {
    let text = value.trim();
    if text.is_empty()
        || text.eq_ignore_ascii_case("nan")
        || text.eq_ignore_ascii_case("n/a")
        || text.eq_ignore_ascii_case("#n/a")
        || text.eq_ignore_ascii_case("null")
    {
        None
    } else {
        Some(text.to_string())
    }
}

/// Return a lowercase alphanumeric key for matching Bloomberg sub-field labels.
pub fn canonical_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

/// Find the first column whose canonical label matches one of `candidates`.
pub fn find_column(batch: &RecordBatch, candidates: &[&str]) -> Option<String> {
    let wanted = candidates
        .iter()
        .map(|candidate| canonical_name(candidate))
        .collect::<Vec<_>>();

    batch.schema().fields().iter().find_map(|field| {
        let key = canonical_name(field.name());
        wanted
            .iter()
            .any(|candidate| candidate == &key)
            .then(|| field.name().to_string())
    })
}

/// Convert a `NaiveDate` to Arrow Date32 days since Unix epoch.
pub fn naive_to_date32(date: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid unix epoch");
    (date - epoch).num_days() as i32
}

/// Parse common Bloomberg date string representations.
pub fn parse_any_date(value: &str) -> Option<NaiveDate> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }

    for fmt in ["%Y%m%d", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"] {
        if let Ok(date) = NaiveDate::parse_from_str(value, fmt) {
            return Some(date);
        }
    }

    value
        .get(..10)
        .and_then(|prefix| NaiveDate::parse_from_str(prefix, "%Y-%m-%d").ok())
}

/// Extract a value from an Arrow array as `f64`.
pub fn array_value_as_f64(array: &ArrayRef, idx: usize) -> Option<f64> {
    if idx >= array.len() || array.is_null(idx) {
        return None;
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(arr.value(idx));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(arr.value(idx) as f64);
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(arr.value(idx) as f64);
    }
    array_value_as_string(array, idx).and_then(|value| parse_f64_like(&value))
}

/// Extract a value from an Arrow array as `NaiveDate`.
pub fn array_value_as_date(array: &ArrayRef, idx: usize) -> Option<NaiveDate> {
    if idx >= array.len() || array.is_null(idx) {
        return None;
    }
    if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
        return date32_to_naive(arr.value(idx));
    }
    array_value_as_string(array, idx).and_then(|value| parse_any_date(&value))
}

/// Parse a Bloomberg numeric string, allowing comma thousands separators.
pub fn parse_f64_like(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.as_bytes().contains(&b',') {
        let mut cleaned = String::with_capacity(trimmed.len());
        cleaned.extend(trimmed.chars().filter(|ch| *ch != ','));
        cleaned
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
    } else {
        trimmed
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Date32Array, Float64Array, StringArray};

    use super::*;

    #[test]
    fn array_value_as_string_formats_typed_values() {
        let string_values: ArrayRef = Arc::new(StringArray::from(vec![Some("abc")]));
        let numeric_values: ArrayRef = Arc::new(Float64Array::from(vec![Some(12.5)]));
        let date_values: ArrayRef = Arc::new(Date32Array::from(vec![Some(naive_to_date32(
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        ))]));

        assert_eq!(
            array_value_as_string(&string_values, 0).as_deref(),
            Some("abc")
        );
        assert_eq!(
            array_value_as_string(&numeric_values, 0).as_deref(),
            Some("12.5")
        );
        assert_eq!(
            array_value_as_string(&date_values, 0).as_deref(),
            Some("2024-01-02")
        );
    }

    #[test]
    fn parse_f64_like_parses_plain_and_grouped_numbers() {
        assert_eq!(parse_f64_like("123.45"), Some(123.45));
        assert_eq!(parse_f64_like("1,234.5"), Some(1234.5));
        assert_eq!(parse_f64_like("nan"), None);
    }
}
