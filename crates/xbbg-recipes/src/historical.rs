//! Historical data recipes.
//!
//! Convenience recipes for dividend, earnings, turnover, and ETF holdings data.
//!
//! # Recipes
//!
//! - [`recipe_dividend`]: Fetch dividend history
//! - [`recipe_earning`]: Fetch earnings data with hierarchical percentages
//! - [`recipe_turnover`]: Fetch volume/turnover data
//! - [`recipe_etf_holdings`]: Fetch ETF constituent holdings via BQL

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::error::{RecipeError, Result};
use crate::utils::{
    array_value_as_date, array_value_as_f64, array_value_as_string, as_string_col, canonical_name,
    naive_to_date32,
};
use arrow_array::builder::{Date32Builder, Float64Builder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use chrono::{Duration, NaiveDate};
use xbbg_async::engine::{Engine, ExtractorType, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::transforms::bql::build_etf_holdings_query;
use xbbg_ext::transforms::historical::{apply_column_renames, calculate_level_percentages};
use xbbg_ext::{fmt_date, parse_date};

pub async fn recipe_dividend(
    engine: &Engine,
    tickers: Vec<String>,
    dvd_type: Option<String>,
    _start_date: String,
    _end_date: String,
) -> Result<RecordBatch> {
    let mut overrides = vec![];
    if let Some(dt) = dvd_type {
        overrides.push(("DVD_TYPE".to_string(), dt));
    }
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(tickers),
        fields: Some(vec!["DVD_HIST_ALL".to_string()]),
        overrides: if overrides.is_empty() {
            None
        } else {
            Some(overrides)
        },
        ..Default::default()
    };
    engine.request(params).await.map_err(Into::into)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DividendEvent {
    ticker: String,
    ex_date: NaiveDate,
    declared_date: Option<NaiveDate>,
    record_date: Option<NaiveDate>,
    payable_date: Option<NaiveDate>,
    dividend_type: Option<String>,
    amount: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DividendYieldRow {
    ticker: String,
    date: NaiveDate,
    dividend_amount: Option<f64>,
    trailing_dividend_amount: Option<f64>,
    price: Option<f64>,
    dividend_yield: Option<f64>,
    dividend_type: Option<String>,
    declared_date: Option<NaiveDate>,
    record_date: Option<NaiveDate>,
    payable_date: Option<NaiveDate>,
}

/// Compute trailing realized dividend amount and trailing dividend yield.
pub async fn recipe_dividend_yield(
    engine: &Engine,
    tickers: Vec<String>,
    start_date: String,
    end_date: String,
    dividend_types: Option<Vec<String>>,
    window_days: Option<i32>,
) -> Result<RecordBatch> {
    let start = parse_date(&start_date)?;
    let end = parse_date(&end_date)?;
    if end < start {
        return Err(RecipeError::InvalidArgument(format!(
            "end_date {end_date} is before start_date {start_date}"
        )));
    }
    let window_days = window_days.unwrap_or(365).max(1);
    let event_start = start - Duration::days(window_days as i64);
    let dividend_type_filter = normalize_dividend_type_filter(dividend_types);

    let dividend_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(tickers.clone()),
        fields: Some(vec!["DVD_HIST_ALL".to_string()]),
        overrides: Some(vec![
            ("DVD_Start_Dt".to_string(), fmt_date(event_start, None)),
            ("DVD_End_Dt".to_string(), fmt_date(end, None)),
        ]),
        ..Default::default()
    };
    let dividend_batch = engine.request(dividend_params).await?;
    let events = aggregate_dividend_events(extract_dividend_events(
        &dividend_batch,
        dividend_type_filter.as_ref(),
    )?);

    let price_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(tickers.clone()),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some(fmt_date(start, None)),
        end_date: Some(fmt_date(end, None)),
        ..Default::default()
    };
    let price_batch = engine.request(price_params).await?;
    let prices = extract_price_history(&price_batch)?;
    let rows = build_dividend_yield_rows(&tickers, start, end, window_days, &events, &prices);
    build_dividend_yield_batch(&rows)
}

fn normalize_dividend_type_filter(dividend_types: Option<Vec<String>>) -> Option<HashSet<String>> {
    dividend_types.map(|types| {
        types
            .into_iter()
            .map(|typ| canonical_name(&typ))
            .filter(|typ| !typ.is_empty())
            .collect::<HashSet<_>>()
    })
}

fn dividend_type_allowed(value: Option<&str>, filter: Option<&HashSet<String>>) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if filter.is_empty() {
        return true;
    }
    let Some(value) = value else {
        return false;
    };
    filter.contains(&canonical_name(value))
}

fn find_dividend_column(batch: &RecordBatch, candidates: &[&str]) -> Option<String> {
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

pub(crate) fn extract_dividend_events(
    batch: &RecordBatch,
    dividend_type_filter: Option<&HashSet<String>>,
) -> Result<Vec<DividendEvent>> {
    let ticker_col = batch
        .column_by_name("ticker")
        .ok_or_else(|| RecipeError::Other("missing 'ticker' column".to_string()))?;
    let ex_date_col = find_dividend_column(
        batch,
        &[
            "ex date",
            "ex-date",
            "ex_date",
            "dvd ex dt",
            "dividend ex date",
        ],
    )
    .and_then(|name| batch.column_by_name(&name))
    .ok_or_else(|| {
        RecipeError::Other("DVD_HIST_ALL response missing ex-date column".to_string())
    })?;
    let amount_col = find_dividend_column(
        batch,
        &[
            "amount",
            "dividend amount",
            "dvd amount",
            "cash amount",
            "gross amount",
        ],
    )
    .and_then(|name| batch.column_by_name(&name));
    let type_col = find_dividend_column(
        batch,
        &["dividend type", "dvd type", "type", "distribution type"],
    )
    .and_then(|name| batch.column_by_name(&name));
    let declared_col = find_dividend_column(
        batch,
        &[
            "declared date",
            "declaration date",
            "declared_date",
            "announcement date",
        ],
    )
    .and_then(|name| batch.column_by_name(&name));
    let record_col = find_dividend_column(batch, &["record date", "record_date"])
        .and_then(|name| batch.column_by_name(&name));
    let payable_col = find_dividend_column(
        batch,
        &["payable date", "payment date", "pay date", "payable_date"],
    )
    .and_then(|name| batch.column_by_name(&name));

    let mut events = Vec::new();
    for row in 0..batch.num_rows() {
        let Some(ticker) =
            array_value_as_string(ticker_col, row).map(|value| value.trim().to_string())
        else {
            continue;
        };
        if ticker.is_empty() {
            continue;
        }
        let Some(ex_date) = array_value_as_date(ex_date_col, row) else {
            continue;
        };
        let dividend_type = type_col
            .and_then(|col| array_value_as_string(col, row))
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if !dividend_type_allowed(dividend_type.as_deref(), dividend_type_filter) {
            continue;
        }

        events.push(DividendEvent {
            ticker,
            ex_date,
            declared_date: declared_col.and_then(|col| array_value_as_date(col, row)),
            record_date: record_col.and_then(|col| array_value_as_date(col, row)),
            payable_date: payable_col.and_then(|col| array_value_as_date(col, row)),
            dividend_type,
            amount: amount_col.and_then(|col| array_value_as_f64(col, row)),
        });
    }

    Ok(events)
}

pub(crate) fn aggregate_dividend_events(events: Vec<DividendEvent>) -> Vec<DividendEvent> {
    type EventKey = (
        String,
        NaiveDate,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<NaiveDate>,
        Option<String>,
    );

    let mut grouped: HashMap<EventKey, DividendEvent> = HashMap::new();
    for event in events {
        let key = (
            event.ticker.clone(),
            event.ex_date,
            event.declared_date,
            event.record_date,
            event.payable_date,
            event.dividend_type.clone(),
        );
        grouped
            .entry(key)
            .and_modify(|existing| {
                existing.amount = match (existing.amount, event.amount) {
                    (Some(left), Some(right)) => Some(left + right),
                    (Some(left), None) => Some(left),
                    (None, Some(right)) => Some(right),
                    (None, None) => None,
                };
            })
            .or_insert(event);
    }

    let mut output = grouped.into_values().collect::<Vec<_>>();
    output.sort_by(|left, right| {
        left.ticker
            .cmp(&right.ticker)
            .then(left.ex_date.cmp(&right.ex_date))
            .then(left.declared_date.cmp(&right.declared_date))
            .then(left.record_date.cmp(&right.record_date))
            .then(left.payable_date.cmp(&right.payable_date))
            .then(left.dividend_type.cmp(&right.dividend_type))
    });
    output
}

fn extract_price_history(batch: &RecordBatch) -> Result<HashMap<(String, NaiveDate), f64>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;
    let date_col = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("missing 'date' column".to_string()))?;
    let mut prices = HashMap::new();

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }
        if !field_col.value(row).eq_ignore_ascii_case("PX_LAST") {
            continue;
        }
        let Some(date) = array_value_as_date(date_col, row) else {
            continue;
        };
        let Some(price) = array_value_as_f64(value_col, row) else {
            continue;
        };
        prices.insert((ticker_col.value(row).to_string(), date), price);
    }

    Ok(prices)
}

pub(crate) fn build_dividend_yield_rows(
    tickers: &[String],
    start: NaiveDate,
    end: NaiveDate,
    window_days: i32,
    events: &[DividendEvent],
    prices: &HashMap<(String, NaiveDate), f64>,
) -> Vec<DividendYieldRow> {
    // Sort references rather than cloning events. The ordinal keeps the prior
    // input order for same-day events, which defines the representative event.
    let mut events_by_ticker: HashMap<&str, Vec<(usize, &DividendEvent)>> = HashMap::new();
    for (ordinal, event) in events.iter().enumerate() {
        events_by_ticker
            .entry(event.ticker.as_str())
            .or_default()
            .push((ordinal, event));
    }
    for ticker_events in events_by_ticker.values_mut() {
        ticker_events.sort_by(|(left_ordinal, left), (right_ordinal, right)| {
            left.ex_date
                .cmp(&right.ex_date)
                .then(left_ordinal.cmp(right_ordinal))
        });
    }

    // Pre-group prices once: scanning every (ticker, date) price key per
    // ticker is O(tickers x total entries), and the tuple lookup below would
    // clone the ticker String per row.
    let mut prices_by_ticker: HashMap<&str, HashMap<NaiveDate, f64>> = HashMap::new();
    for ((price_ticker, date), price) in prices {
        prices_by_ticker
            .entry(price_ticker.as_str())
            .or_default()
            .insert(*date, *price);
    }

    let mut rows = Vec::new();
    for ticker in tickers {
        let ticker_events = events_by_ticker
            .get(ticker.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let ticker_prices = prices_by_ticker.get(ticker.as_str());
        let mut dates = BTreeSet::new();
        if let Some(ticker_prices) = ticker_prices {
            for date in ticker_prices.keys() {
                if *date >= start && *date <= end {
                    dates.insert(*date);
                }
            }
        }
        for (_, event) in ticker_events {
            if event.ex_date >= start && event.ex_date <= end {
                dates.insert(event.ex_date);
            }
        }

        let mut window_start = 0;
        let mut window_end = 0;
        let mut window_amount_count = 0usize;
        let mut window_total = 0.0;
        let mut same_day_start = 0;

        for date in dates {
            while window_end < ticker_events.len() && ticker_events[window_end].1.ex_date <= date {
                if let Some(amount) = ticker_events[window_end].1.amount {
                    window_total += amount;
                    window_amount_count += 1;
                }
                window_end += 1;
            }

            let trailing_start = date - Duration::days(window_days as i64);
            let previous_window_start = window_start;
            while window_start < window_end
                && ticker_events[window_start].1.ex_date <= trailing_start
            {
                window_start += 1;
            }

            // Floating-point subtraction cannot undo an earlier rounded add
            // (and cannot recover after NaN/infinity expires). Re-fold only
            // when the left edge moves; dates without expiries stay O(1).
            if window_start != previous_window_start {
                window_amount_count = 0;
                window_total = 0.0;
                for (_, event) in &ticker_events[window_start..window_end] {
                    if let Some(amount) = event.amount {
                        window_total += amount;
                        window_amount_count += 1;
                    }
                }
            }

            while same_day_start < ticker_events.len()
                && ticker_events[same_day_start].1.ex_date < date
            {
                same_day_start += 1;
            }
            let mut same_day_end = same_day_start;
            while same_day_end < ticker_events.len()
                && ticker_events[same_day_end].1.ex_date == date
            {
                same_day_end += 1;
            }
            let same_day_events = &ticker_events[same_day_start..same_day_end];
            let dividend_amount =
                sum_optional(same_day_events.iter().filter_map(|(_, event)| event.amount));
            let trailing_dividend_amount = (window_amount_count != 0).then_some(window_total);
            let price = ticker_prices.and_then(|prices| prices.get(&date)).copied();
            let dividend_yield = match (trailing_dividend_amount, price) {
                (Some(amount), Some(price)) if price != 0.0 => Some(amount / price),
                _ => None,
            };
            let representative = same_day_events.first().map(|(_, event)| *event);

            rows.push(DividendYieldRow {
                ticker: ticker.clone(),
                date,
                dividend_amount,
                trailing_dividend_amount,
                price,
                dividend_yield,
                dividend_type: representative.and_then(|event| event.dividend_type.clone()),
                declared_date: representative.and_then(|event| event.declared_date),
                record_date: representative.and_then(|event| event.record_date),
                payable_date: representative.and_then(|event| event.payable_date),
            });
        }
    }

    rows.sort_by(|left, right| {
        left.ticker
            .cmp(&right.ticker)
            .then(left.date.cmp(&right.date))
    });
    rows
}

fn sum_optional(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut seen = false;
    let mut total = 0.0;
    for value in values {
        seen = true;
        total += value;
    }
    seen.then_some(total)
}

fn append_date_opt(builder: &mut Date32Builder, value: Option<NaiveDate>) {
    match value {
        Some(value) => builder.append_value(naive_to_date32(value)),
        None => builder.append_null(),
    }
}

fn append_f64_opt(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_string_opt(builder: &mut StringBuilder, value: Option<&String>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn build_dividend_yield_batch(rows: &[DividendYieldRow]) -> Result<RecordBatch> {
    let mut ticker = StringBuilder::new();
    let mut date = Date32Builder::new();
    let mut amount = Float64Builder::new();
    let mut trailing = Float64Builder::new();
    let mut price = Float64Builder::new();
    let mut yield_builder = Float64Builder::new();
    let mut dividend_type = StringBuilder::new();
    let mut declared = Date32Builder::new();
    let mut record = Date32Builder::new();
    let mut payable = Date32Builder::new();

    for row in rows {
        ticker.append_value(&row.ticker);
        date.append_value(naive_to_date32(row.date));
        append_f64_opt(&mut amount, row.dividend_amount);
        append_f64_opt(&mut trailing, row.trailing_dividend_amount);
        append_f64_opt(&mut price, row.price);
        append_f64_opt(&mut yield_builder, row.dividend_yield);
        append_string_opt(&mut dividend_type, row.dividend_type.as_ref());
        append_date_opt(&mut declared, row.declared_date);
        append_date_opt(&mut record, row.record_date);
        append_date_opt(&mut payable, row.payable_date);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::Utf8, false),
        Field::new("date", DataType::Date32, false),
        Field::new("dividend_amount", DataType::Float64, true),
        Field::new("trailing_dividend_amount", DataType::Float64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("dividend_yield", DataType::Float64, true),
        Field::new("dividend_type", DataType::Utf8, true),
        Field::new("declared_date", DataType::Date32, true),
        Field::new("record_date", DataType::Date32, true),
        Field::new("payable_date", DataType::Date32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ticker.finish()),
            Arc::new(date.finish()),
            Arc::new(amount.finish()),
            Arc::new(trailing.finish()),
            Arc::new(price.finish()),
            Arc::new(yield_builder.finish()),
            Arc::new(dividend_type.finish()),
            Arc::new(declared.finish()),
            Arc::new(record.finish()),
            Arc::new(payable.finish()),
        ],
    )
    .map_err(Into::into)
}

/// Fetch earnings bulk data and derive hierarchical percentages.
///
/// Workflow:
/// 1. Query `PG_Bulk_Header` using the bulk extractor to discover dynamic period labels.
/// 2. Query `PG_{typ}` for the actual earnings values.
/// 3. Rename period columns using header-derived names (for example, `Period 1 Value` -> `fy2024`).
/// 4. Add `{period}_pct` columns using hierarchy semantics:
///    - level 1 rows: percentage of total level 1 sum
///    - level 2 rows: percentage of parent level 1 group sum
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `tickers` - Securities to query
/// * `by` - Period granularity override (`Q` or `A`)
/// * `typ` - Earnings type (`IS`, `BS`, or `CF`)
/// * `ccy` - Currency override
/// * `level` - Optional hierarchy level filter (`1` or `2`)
pub async fn recipe_earning(
    engine: &Engine,
    tickers: Vec<String>,
    by: Option<String>,
    typ: String,
    ccy: Option<String>,
    level: Option<i32>,
) -> Result<RecordBatch> {
    let typ = normalize_earning_type(&typ)?;
    let data_field = format!("PG_{typ}");

    let (header_overrides, data_overrides) =
        build_earning_overrides(by.as_deref(), ccy.as_deref(), level)?;

    let header_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(tickers.clone()),
        fields: Some(vec!["PG_Bulk_Header".to_string()]),
        overrides: to_option_overrides(header_overrides),
        ..Default::default()
    };

    let data_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(tickers),
        fields: Some(vec![data_field]),
        overrides: to_option_overrides(data_overrides),
        ..Default::default()
    };

    let header_batch = engine.request(header_params).await?;
    let mut data_batch = engine.request(data_params).await?;

    if header_batch.num_rows() > 0 && data_batch.num_rows() > 0 {
        let renames = build_earning_header_rename(&header_batch, &data_batch);
        if !renames.is_empty() {
            data_batch = apply_column_renames(&data_batch, &renames)?;
        }
    }

    add_earning_percentage_columns(data_batch)
}

fn normalize_earning_type(typ: &str) -> Result<String> {
    let normalized = typ.trim().to_ascii_uppercase();
    let normalized = normalized.strip_prefix("PG_").unwrap_or(&normalized);

    match normalized {
        "IS" | "BS" | "CF" => Ok(normalized.to_string()),
        _ => Err(crate::error::RecipeError::InvalidArgument(format!(
            "unsupported earning type '{typ}', expected IS/BS/CF"
        ))),
    }
}

type OverridePairs = Vec<(String, String)>;
type EarningOverrides = (OverridePairs, OverridePairs);

fn build_earning_overrides(
    by: Option<&str>,
    ccy: Option<&str>,
    level: Option<i32>,
) -> Result<EarningOverrides> {
    let mut header_overrides = Vec::new();
    let mut data_overrides = Vec::new();

    if let Some(period) = by {
        let period = period.trim().to_ascii_uppercase();
        if !period.is_empty() {
            if period != "Q" && period != "A" {
                return Err(crate::error::RecipeError::InvalidArgument(format!(
                    "unsupported by='{period}', expected Q or A"
                )));
            }

            header_overrides.push(("PER".to_string(), period.clone()));
            data_overrides.push(("PER".to_string(), period));
        }
    }

    if let Some(currency) = ccy {
        let currency = currency.trim().to_ascii_uppercase();
        if !currency.is_empty() {
            data_overrides.push(("CURRENCY".to_string(), currency));
        }
    }

    if let Some(hierarchy_level) = level {
        if hierarchy_level != 1 && hierarchy_level != 2 {
            return Err(crate::error::RecipeError::InvalidArgument(format!(
                "unsupported level='{hierarchy_level}', expected 1 or 2"
            )));
        }
        data_overrides.push((
            "PG_Hierarchy_Level".to_string(),
            hierarchy_level.to_string(),
        ));
    }

    Ok((header_overrides, data_overrides))
}

fn to_option_overrides(overrides: Vec<(String, String)>) -> Option<Vec<(String, String)>> {
    if overrides.is_empty() {
        None
    } else {
        Some(overrides)
    }
}

fn build_earning_header_rename(
    header_batch: &RecordBatch,
    data_batch: &RecordBatch,
) -> Vec<(String, String)> {
    let mut header_values: HashMap<String, String> = HashMap::new();

    for field in header_batch.schema().fields() {
        let column_name = field.name();
        if column_name == "ticker" || column_name == "field" {
            continue;
        }

        let Some(column) = header_batch.column_by_name(column_name) else {
            continue;
        };

        let first_value = (0..header_batch.num_rows())
            .find_map(|idx| array_value_as_string(column, idx))
            .map(|raw| raw.trim().to_string())
            .filter(|raw| !raw.is_empty());

        if let Some(value) = first_value {
            header_values.insert(column_name.to_string(), value);
        }
    }

    if header_values.is_empty() {
        return Vec::new();
    }

    let mut used_names: HashSet<String> = data_batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_ascii_lowercase())
        .collect();

    let mut renames = Vec::new();
    for field in data_batch.schema().fields() {
        let data_col = field.name();
        if data_col == "ticker" || data_col == "field" {
            continue;
        }

        let header_col = if let Some(period_col) = data_col.strip_suffix(" Value") {
            format!("{period_col} Header")
        } else {
            format!("{data_col} Header")
        };

        let Some(raw_header_value) = header_values.get(&header_col) else {
            continue;
        };

        let normalized_name = normalize_earning_header_value(raw_header_value);
        if normalized_name.is_empty() {
            continue;
        }

        let normalized_key = normalized_name.to_ascii_lowercase();
        if normalized_key == data_col.to_ascii_lowercase() || used_names.contains(&normalized_key) {
            continue;
        }

        renames.push((data_col.to_string(), normalized_name.clone()));
        used_names.insert(normalized_key);
    }

    renames
}

fn normalize_earning_header_value(value: &str) -> String {
    let mut normalized = value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '/', '.'], "_");

    while normalized.contains("__") {
        normalized = normalized.replace("__", "_");
    }

    normalized = normalized.replace("_20", "20");
    normalized.trim_matches('_').to_string()
}

fn add_earning_percentage_columns(batch: RecordBatch) -> Result<RecordBatch> {
    let Some(level_col_name) = find_level_column_name(&batch) else {
        return Ok(batch);
    };

    let Some(level_col) = batch.column_by_name(&level_col_name) else {
        return Ok(batch);
    };
    let levels = extract_level_values(level_col);

    if levels.iter().all(Option::is_none) {
        return Ok(batch);
    }

    let value_cols = earning_value_columns(&batch);
    let mut output = batch;

    for value_col in value_cols {
        let pct_col = format!("{value_col}_pct");
        if output.column_by_name(&pct_col).is_some() {
            continue;
        }

        let Some(values_col) = output.column_by_name(&value_col) else {
            continue;
        };

        let values = extract_numeric_values(values_col);
        if values.iter().all(Option::is_none) {
            continue;
        }

        let percentages = calculate_level_percentages(&values, &levels);
        output = insert_pct_column_after(&output, &value_col, &pct_col, percentages)?;
    }

    Ok(output)
}

fn find_level_column_name(batch: &RecordBatch) -> Option<String> {
    if batch.column_by_name("level").is_some() {
        return Some("level".to_string());
    }

    batch
        .schema()
        .fields()
        .iter()
        .find(|field| field.name().eq_ignore_ascii_case("level"))
        .map(|field| field.name().to_string())
}

fn earning_value_columns(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name())
        .filter(|name| is_earning_value_column(name))
        .cloned()
        .collect()
}

fn is_earning_value_column(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    (lower.starts_with("fy") && !lower.ends_with("_pct")) || lower.ends_with(" value")
}

fn extract_numeric_values(array: &ArrayRef) -> Vec<Option<f64>> {
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx))
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx) as f64)
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx) as f64)
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    parse_f64_like(arr.value(idx))
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    parse_f64_like(arr.value(idx))
                }
            })
            .collect();
    }

    vec![None; array.len()]
}

fn extract_level_values(array: &ArrayRef) -> Vec<Option<i64>> {
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx))
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(arr.value(idx) as i64)
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    let v = arr.value(idx);
                    if v.is_finite() && v.fract() == 0.0 {
                        Some(v as i64)
                    } else {
                        None
                    }
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    parse_i64_like(arr.value(idx))
                }
            })
            .collect();
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return (0..arr.len())
            .map(|idx| {
                if arr.is_null(idx) {
                    None
                } else {
                    parse_i64_like(arr.value(idx))
                }
            })
            .collect();
    }

    vec![None; array.len()]
}

fn parse_f64_like(value: &str) -> Option<f64> {
    let cleaned = value.trim().replace(',', "");
    if cleaned.is_empty() {
        None
    } else {
        cleaned.parse::<f64>().ok()
    }
}

fn parse_i64_like(value: &str) -> Option<i64> {
    let cleaned = value.trim();
    if cleaned.is_empty() {
        return None;
    }

    if let Ok(parsed) = cleaned.parse::<i64>() {
        return Some(parsed);
    }

    let parsed = cleaned.parse::<f64>().ok()?;
    if parsed.is_finite() && parsed.fract() == 0.0 {
        Some(parsed as i64)
    } else {
        None
    }
}

fn insert_pct_column_after(
    batch: &RecordBatch,
    after_col: &str,
    pct_col: &str,
    percentages: Vec<Option<f64>>,
) -> Result<RecordBatch> {
    if percentages.len() != batch.num_rows() {
        return Err(crate::error::RecipeError::Other(format!(
            "percentage length mismatch for '{pct_col}'"
        )));
    }

    let insert_after_idx = batch
        .schema()
        .index_of(after_col)
        .map_err(|_| crate::error::RecipeError::Other(format!("missing '{after_col}' column")))?;

    let pct_array: ArrayRef = Arc::new(Float64Array::from(percentages));

    let mut fields = Vec::with_capacity(batch.num_columns() + 1);
    let mut columns = Vec::with_capacity(batch.num_columns() + 1);

    for idx in 0..batch.num_columns() {
        fields.push(batch.schema().field(idx).as_ref().clone());
        columns.push(batch.column(idx).clone());

        if idx == insert_after_idx {
            fields.push(Field::new(pct_col, DataType::Float64, true));
            columns.push(pct_array.clone());
        }
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

/// Fetch trading volume and turnover for securities.
///
/// Requests the TURNOVER field via HistoricalData. Callers may perform
/// a second request for volume × VWAP if turnover is unavailable for
/// some tickers (fallback logic lives at the Python layer).
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `tickers` - Securities to query
/// * `start_date` - Start date (YYYYMMDD format)
/// * `end_date` - End date (YYYYMMDD format)
/// * `ccy` - Currency for conversion. None for local currency.
/// * `factor` - Division factor (e.g., 1_000_000.0 for millions)
///
/// # Returns
///
/// Arrow RecordBatch with turnover data in historical format
pub async fn recipe_turnover(
    engine: &Engine,
    tickers: Vec<String>,
    start_date: String,
    end_date: String,
    ccy: Option<String>,
    _factor: Option<f64>,
) -> Result<RecordBatch> {
    let mut overrides = vec![];
    if let Some(c) = ccy {
        if c.to_lowercase() != "local" {
            overrides.push(("EQY_FUND_CRNCY".to_string(), c));
        }
    }

    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(tickers),
        fields: Some(vec!["TURNOVER".to_string()]),
        start_date: Some(start_date),
        end_date: Some(end_date),
        overrides: if overrides.is_empty() {
            None
        } else {
            Some(overrides)
        },
        ..Default::default()
    };

    engine.request(params).await.map_err(Into::into)
}

/// Fetch ETF constituent holdings via BQL.
///
/// Uses Bloomberg Query Language to retrieve holdings for an ETF including
/// ISIN, weights, and position IDs.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `etf_ticker` - ETF ticker (e.g., "SPY US Equity")
/// * `fields` - Additional fields to retrieve beyond defaults (id_isin, weights, id().position)
///
/// # Returns
///
/// Arrow RecordBatch with ETF holdings data
pub async fn recipe_etf_holdings(
    engine: &Engine,
    etf_ticker: String,
    fields: Option<Vec<String>>,
) -> Result<RecordBatch> {
    // Query construction lives in xbbg-ext (single source of truth). Note:
    // the builder appends " US Equity" to bare tickers, matching the other
    // BQL recipes (previously this recipe passed bare tickers through).
    let extra = fields.unwrap_or_default();
    let extra_refs: Vec<&str> = extra.iter().map(String::as_str).collect();
    let bql_query = build_etf_holdings_query(&etf_ticker, &extra_refs);

    let params = RequestParams {
        service: Service::BqlSvc.to_string(),
        operation: Operation::BqlSendQuery.to_string(),
        elements: Some(vec![("expression".to_string(), bql_query)]),
        ..Default::default()
    };

    engine.request(params).await.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_build_earning_header_rename() {
        let header_schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("Period 1 Header", DataType::Utf8, true),
            Field::new("Period 2 Header", DataType::Utf8, true),
        ]));

        let header_batch = RecordBatch::try_new(
            header_schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL US Equity"])),
                Arc::new(StringArray::from(vec!["PG_Bulk_Header"])),
                Arc::new(StringArray::from(vec!["FY 2023"])),
                Arc::new(StringArray::from(vec!["FY 2024"])),
            ],
        )
        .unwrap();

        let data_schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("Period 1 Value", DataType::Float64, true),
            Field::new("Period 2 Value", DataType::Float64, true),
        ]));

        let data_batch = RecordBatch::try_new(
            data_schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL US Equity"])),
                Arc::new(StringArray::from(vec!["PG_IS"])),
                Arc::new(Float64Array::from(vec![Some(100.0)])),
                Arc::new(Float64Array::from(vec![Some(120.0)])),
            ],
        )
        .unwrap();

        let rename_map = build_earning_header_rename(&header_batch, &data_batch);

        assert!(rename_map.contains(&("Period 1 Value".to_string(), "fy2023".to_string())));
        assert!(rename_map.contains(&("Period 2 Value".to_string(), "fy2024".to_string())));
    }

    #[test]
    fn test_add_earning_percentage_columns_fy_data() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("level", DataType::Utf8, true),
            Field::new("fy2023", DataType::Float64, true),
            Field::new("fy2024", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "AAPL US Equity",
                    "AAPL US Equity",
                    "AAPL US Equity",
                    "AAPL US Equity",
                ])),
                Arc::new(StringArray::from(vec!["PG_IS", "PG_IS", "PG_IS", "PG_IS"])),
                Arc::new(StringArray::from(vec!["1", "1", "2", "2"])),
                Arc::new(Float64Array::from(vec![
                    Some(100.0),
                    Some(200.0),
                    Some(50.0),
                    Some(50.0),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(300.0),
                    Some(100.0),
                    Some(60.0),
                    Some(40.0),
                ])),
            ],
        )
        .unwrap();

        let output = add_earning_percentage_columns(batch).unwrap();

        let fy23_idx = output.schema().index_of("fy2023").unwrap();
        let fy23_pct_idx = output.schema().index_of("fy2023_pct").unwrap();
        assert_eq!(fy23_pct_idx, fy23_idx + 1);

        let fy23_pct = output
            .column_by_name("fy2023_pct")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((fy23_pct.value(0) - 33.333).abs() < 0.01);
        assert!((fy23_pct.value(1) - 66.667).abs() < 0.01);
        assert!((fy23_pct.value(2) - 50.0).abs() < 0.01);
        assert!((fy23_pct.value(3) - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_add_earning_percentage_columns_case_insensitive_level() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("Level", DataType::Int32, true),
            Field::new("fy2023", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "AAPL US Equity",
                    "AAPL US Equity",
                    "AAPL US Equity",
                ])),
                Arc::new(StringArray::from(vec!["PG_IS", "PG_IS", "PG_IS"])),
                Arc::new(Int32Array::from(vec![Some(1), Some(1), Some(2)])),
                Arc::new(StringArray::from(vec!["100", "200", "50"])),
            ],
        )
        .unwrap();

        let output = add_earning_percentage_columns(batch).unwrap();
        let pct_col = output
            .column_by_name("fy2023_pct")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert!((pct_col.value(0) - 33.333).abs() < 0.01);
        assert!((pct_col.value(1) - 66.667).abs() < 0.01);
        assert!((pct_col.value(2) - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_turnover_ccy_override_local() {
        // "local" currency should produce no overrides
        let ccy = Some("local".to_string());
        let mut overrides = vec![];
        if let Some(c) = &ccy {
            if c.to_lowercase() != "local" {
                overrides.push(("EQY_FUND_CRNCY".to_string(), c.clone()));
            }
        }
        assert!(overrides.is_empty());
    }

    #[test]
    fn test_turnover_ccy_override_usd() {
        let ccy = Some("USD".to_string());
        let mut overrides = vec![];
        if let Some(c) = &ccy {
            if c.to_lowercase() != "local" {
                overrides.push(("EQY_FUND_CRNCY".to_string(), c.clone()));
            }
        }
        assert_eq!(overrides.len(), 1);
        assert_eq!(
            overrides[0],
            ("EQY_FUND_CRNCY".to_string(), "USD".to_string())
        );
    }

    #[test]
    fn test_turnover_ccy_none() {
        let ccy: Option<String> = None;
        let mut overrides = vec![];
        if let Some(c) = &ccy {
            if c.to_lowercase() != "local" {
                overrides.push(("EQY_FUND_CRNCY".to_string(), c.clone()));
            }
        }
        assert!(overrides.is_empty());
    }

    #[test]
    fn test_etf_holdings_default_fields() {
        let fields: Option<Vec<String>> = None;
        let mut all_fields = vec![
            "id_isin".to_string(),
            "weights".to_string(),
            "id().position".to_string(),
        ];
        if let Some(extra) = fields {
            for f in extra {
                if !all_fields.contains(&f) {
                    all_fields.push(f);
                }
            }
        }
        assert_eq!(all_fields, vec!["id_isin", "weights", "id().position"]);
    }

    #[test]
    fn test_etf_holdings_custom_fields() {
        let fields = Some(vec!["name".to_string(), "px_last".to_string()]);
        let mut all_fields = vec![
            "id_isin".to_string(),
            "weights".to_string(),
            "id().position".to_string(),
        ];
        if let Some(extra) = fields {
            for f in extra {
                if !all_fields.contains(&f) {
                    all_fields.push(f);
                }
            }
        }
        assert_eq!(
            all_fields,
            vec!["id_isin", "weights", "id().position", "name", "px_last"]
        );
    }

    #[test]
    fn test_etf_holdings_no_duplicate_fields() {
        let fields = Some(vec!["id_isin".to_string(), "name".to_string()]);
        let mut all_fields = vec![
            "id_isin".to_string(),
            "weights".to_string(),
            "id().position".to_string(),
        ];
        if let Some(extra) = fields {
            for f in extra {
                if !all_fields.contains(&f) {
                    all_fields.push(f);
                }
            }
        }
        // id_isin should not be duplicated
        assert_eq!(
            all_fields,
            vec!["id_isin", "weights", "id().position", "name"]
        );
    }

    #[test]
    fn test_etf_bql_query_format() {
        let etf_ticker = "SPY US Equity";
        let fields_str = "id_isin, weights, id().position";
        let bql_query = format!("get({fields_str}) for(holdings('{etf_ticker}'))");
        assert_eq!(
            bql_query,
            "get(id_isin, weights, id().position) for(holdings('SPY US Equity'))"
        );
    }

    #[test]
    fn test_dividend_yield_aggregates_duplicates_and_rolls_window() {
        let ticker = "AAPL US Equity".to_string();
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 4, 10).unwrap();
        let price_date = NaiveDate::from_ymd_opt(2024, 4, 11).unwrap();
        let events = aggregate_dividend_events(vec![
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: d1,
                declared_date: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(0.24),
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: d1,
                declared_date: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(0.01),
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: d2,
                declared_date: Some(NaiveDate::from_ymd_opt(2024, 4, 1).unwrap()),
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(0.25),
            },
        ]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].amount, Some(0.25));

        let prices = HashMap::from([((ticker.clone(), price_date), 100.0)]);
        let rows = build_dividend_yield_rows(
            &[ticker],
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            price_date,
            365,
            &events,
            &prices,
        );
        let priced = rows.iter().find(|row| row.date == price_date).unwrap();
        assert_eq!(priced.trailing_dividend_amount, Some(0.50));
        assert_eq!(priced.dividend_yield, Some(0.005));
    }

    #[test]
    fn test_dividend_yield_missing_price_keeps_yield_null() {
        let ticker = "AAPL US Equity".to_string();
        let ex_date = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let events = vec![DividendEvent {
            ticker: ticker.clone(),
            ex_date,
            declared_date: None,
            record_date: None,
            payable_date: None,
            dividend_type: Some("Regular Cash".to_string()),
            amount: Some(0.25),
        }];
        let rows = build_dividend_yield_rows(
            &[ticker],
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 31).unwrap(),
            365,
            &events,
            &HashMap::new(),
        );
        assert_eq!(rows[0].price, None);
        assert_eq!(rows[0].dividend_yield, None);
    }

    #[test]
    fn test_dividend_yield_rolling_window_matches_date_predicate_boundaries() {
        let ticker = "AAPL US Equity".to_string();
        let boundary = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let inside = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let evaluation = NaiveDate::from_ymd_opt(2024, 1, 11).unwrap();
        let after = NaiveDate::from_ymd_opt(2024, 1, 12).unwrap();
        let representative_declared = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let events = vec![
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: evaluation,
                declared_date: Some(representative_declared),
                record_date: None,
                payable_date: None,
                dividend_type: Some("Special Cash".to_string()),
                amount: None,
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: boundary,
                declared_date: None,
                record_date: None,
                payable_date: None,
                dividend_type: Some("Old".to_string()),
                amount: Some(10.0),
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: evaluation,
                declared_date: None,
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(2.0),
            },
            DividendEvent {
                ticker: ticker.clone(),
                ex_date: inside,
                declared_date: None,
                record_date: None,
                payable_date: None,
                dividend_type: Some("Regular Cash".to_string()),
                amount: Some(1.0),
            },
        ];
        let prices = HashMap::from([
            ((ticker.clone(), evaluation), 0.0),
            ((ticker.clone(), after), 100.0),
        ]);

        let rows = build_dividend_yield_rows(
            std::slice::from_ref(&ticker),
            boundary,
            after,
            10,
            &events,
            &prices,
        );
        assert_eq!(rows.len(), 4);

        // Differential oracle for the previous definition: every date uses
        // events in original order and the calendar predicate
        // (date - window_days, date].
        for row in &rows {
            let same_day = events
                .iter()
                .filter(|event| event.ex_date == row.date)
                .collect::<Vec<_>>();
            let expected_amount = sum_optional(same_day.iter().filter_map(|event| event.amount));
            let trailing_start = row.date - Duration::days(10);
            let expected_trailing = sum_optional(events.iter().filter_map(|event| {
                (event.ex_date > trailing_start && event.ex_date <= row.date)
                    .then_some(event.amount)
                    .flatten()
            }));
            let expected_price = prices.get(&(ticker.clone(), row.date)).copied();
            let expected_yield = match (expected_trailing, expected_price) {
                (Some(amount), Some(price)) if price != 0.0 => Some(amount / price),
                _ => None,
            };
            let representative = same_day.first().copied();

            assert_eq!(row.dividend_amount, expected_amount);
            assert_eq!(row.trailing_dividend_amount, expected_trailing);
            assert_eq!(row.price, expected_price);
            assert_eq!(row.dividend_yield, expected_yield);
            assert_eq!(
                row.dividend_type,
                representative.and_then(|event| event.dividend_type.clone())
            );
            assert_eq!(
                row.declared_date,
                representative.and_then(|event| event.declared_date)
            );
        }

        let evaluation_row = rows.iter().find(|row| row.date == evaluation).unwrap();
        assert_eq!(evaluation_row.trailing_dividend_amount, Some(3.0));
        assert_eq!(evaluation_row.dividend_amount, Some(2.0));
        assert_eq!(evaluation_row.dividend_yield, None);
        assert_eq!(
            evaluation_row.dividend_type.as_deref(),
            Some("Special Cash")
        );
        assert_eq!(evaluation_row.declared_date, Some(representative_declared));

        let after_row = rows.iter().find(|row| row.date == after).unwrap();
        assert_eq!(after_row.trailing_dividend_amount, Some(2.0));
        assert_eq!(after_row.dividend_yield, Some(0.02));
    }

    fn trailing_after_boundary_expiry(expiring_amount: f64) -> Option<f64> {
        let ticker = "AAPL US Equity".to_string();
        let boundary = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let retained = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let evaluation = NaiveDate::from_ymd_opt(2024, 1, 11).unwrap();
        let event = |ex_date, amount| DividendEvent {
            ticker: ticker.clone(),
            ex_date,
            declared_date: None,
            record_date: None,
            payable_date: None,
            dividend_type: None,
            amount: Some(amount),
        };
        let events = vec![event(boundary, expiring_amount), event(retained, 1.0)];
        let prices = HashMap::from([((ticker.clone(), evaluation), 100.0)]);
        let rows = build_dividend_yield_rows(&[ticker], boundary, evaluation, 10, &events, &prices);
        rows.iter()
            .find(|row| row.date == evaluation)
            .and_then(|row| row.trailing_dividend_amount)
    }

    #[test]
    fn test_dividend_yield_refolds_after_finite_cancellation_expires() {
        assert_eq!(trailing_after_boundary_expiry(1.0e16), Some(1.0));
    }

    #[test]
    fn test_dividend_yield_refolds_after_nonfinite_amount_expires() {
        for expiring_amount in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(trailing_after_boundary_expiry(expiring_amount), Some(1.0));
        }
    }
}
