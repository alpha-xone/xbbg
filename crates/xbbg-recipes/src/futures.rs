//! Futures resolution recipes.
//!
//! These recipes resolve generic futures tickers to specific contract tickers,
//! determine active contracts, and handle CDX series resolution.
//!
//! # Recipes
//!
//! - [`recipe_fut_ticker`]: Resolve generic ticker to specific contract
//! - [`recipe_active_futures`]: Find most active futures contract
//! - [`recipe_cdx_ticker`]: Resolve CDX series
//! - [`recipe_active_cdx`]: Find most active CDX series

use std::sync::Arc;

use arrow_array::builder::{Float64Builder, Int32Builder, StringBuilder};
use arrow_array::{Array, Date32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::{Datelike, Duration, NaiveDate};
use xbbg_async::engine::{Engine, ExtractorType, RequestParams};
use xbbg_async::services::{Operation, Service};
use xbbg_ext::resolvers::cdx::{
    build_resolved_cdx_ticker_with_options, gen_to_specific, parse_cdx_ticker, ResolvedCdxInfo,
};
use xbbg_ext::resolvers::futures::{
    contract_index, generate_futures_candidates, validate_generic_ticker, RollFrequency,
};
use xbbg_ext::{fmt_date, parse_date, parse_ticker_parts};

use crate::error::{RecipeError, Result};
use crate::utils::{
    array_value_as_date, array_value_as_f64, array_value_as_string, as_string_col, canonical_name,
    date32_to_naive, naive_to_date32, parse_any_date,
};

/// Resolve a generic futures ticker to a specific contract ticker for a date.
///
/// Workflow:
/// 1. Parse `dt` and resolve contract index from generic ticker.
/// 2. Generate contract candidates via `xbbg-ext` futures resolver.
/// 3. Query Bloomberg `LAST_TRADEABLE_DT` for all candidates.
/// 4. Keep contracts maturing after `dt`, sort by maturity, and select by index.
/// 5. Return the selected ticker as a single-row `RecordBatch`.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `gen_ticker` - Generic futures ticker (e.g. `ES1 Index`, `CL2 Comdty`)
/// * `dt` - Reference date (`YYYYMMDD`)
/// * `freq` - Optional roll frequency (`M` monthly, `Q`/`QE` quarterly)
pub async fn recipe_fut_ticker(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
    freq: Option<String>,
) -> Result<RecordBatch> {
    let dt_parsed = parse_date(&dt)?;
    let idx = contract_index(&gen_ticker)?;

    use std::str::FromStr;
    let freq = freq
        .as_deref()
        .and_then(|s| RollFrequency::from_str(s).ok())
        .unwrap_or(RollFrequency::Monthly);

    let count = futures_candidate_count(&gen_ticker, idx)?;
    let candidates = generate_futures_candidates(&gen_ticker, dt_parsed, freq, count)?;

    if candidates.is_empty() {
        return Err(RecipeError::Other(format!(
            "no futures candidates generated for '{gen_ticker}'"
        )));
    }

    let candidate_tickers: Vec<String> = candidates.into_iter().map(|c| c.ticker).collect();
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(candidate_tickers),
        fields: Some(vec!["LAST_TRADEABLE_DT".to_string()]),
        ..Default::default()
    };

    let maturity_batch = engine.request(params).await?;
    let mut valid_contracts = extract_refdata_date_values(&maturity_batch, "LAST_TRADEABLE_DT")?
        .into_iter()
        .filter(|(_, maturity)| *maturity > dt_parsed)
        .collect::<Vec<_>>();

    valid_contracts.sort_by_key(|(_, maturity)| *maturity);

    let selected = valid_contracts
        .get(idx)
        .map(|(ticker, _)| ticker.clone())
        .ok_or_else(|| {
            RecipeError::Other(format!(
                "unable to resolve futures contract for '{gen_ticker}' on {dt}: requested index {} but only {} valid maturities",
                idx + 1,
                valid_contracts.len()
            ))
        })?;

    build_single_ticker_batch(selected)
}

/// Resolve the most active futures contract around a reference date.
///
/// Workflow:
/// 1. Validate generic ticker input and parse the reference date.
/// 2. Query historical `FUT_CUR_GEN_TICKER` up to `dt` and return Bloomberg's
///    own generic mapping when available.
/// 3. Otherwise build front/second generic contracts and resolve both via
///    [`recipe_fut_ticker`].
/// 4. Compare front maturity month vs `dt`.
/// 5. If near roll, query 10-day historical `VOLUME` and compare contracts.
/// 6. Return the selected active ticker as a single-row `RecordBatch`.
///
/// # Arguments
///
/// * `engine` - Bloomberg engine reference
/// * `gen_ticker` - Generic futures ticker (e.g. `ES1 Index`)
/// * `dt` - Reference date (`YYYYMMDD`)
/// * `freq` - Optional roll frequency (`M` monthly, `Q`/`QE` quarterly)
pub async fn recipe_active_futures(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
    freq: Option<String>,
) -> Result<RecordBatch> {
    validate_generic_ticker(&gen_ticker)?;
    let dt_parsed = parse_date(&dt)?;

    if let Ok(Some(mapped_ticker)) =
        bloomberg_current_generic_ticker(engine, &gen_ticker, dt_parsed).await
    {
        return build_single_ticker_batch(mapped_ticker);
    }

    let front_gen = with_generic_index(&gen_ticker, 1)?;
    let second_gen = with_generic_index(&gen_ticker, 2)?;

    let front_ticker = {
        let batch = recipe_fut_ticker(engine, front_gen, dt.clone(), freq.clone()).await?;
        extract_single_ticker(&batch)?
    };

    let second_ticker = match recipe_fut_ticker(engine, second_gen, dt.clone(), freq).await {
        Ok(batch) => extract_single_ticker(&batch)?,
        Err(_) => return build_single_ticker_batch(front_ticker),
    };

    let maturity_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(vec![front_ticker.clone(), second_ticker.clone()]),
        fields: Some(vec!["LAST_TRADEABLE_DT".to_string()]),
        ..Default::default()
    };

    let maturity_batch = engine.request(maturity_params).await?;
    if let Some(front_maturity) =
        extract_refdata_date_for_ticker(&maturity_batch, &front_ticker, "LAST_TRADEABLE_DT")?
    {
        let dt_month = (dt_parsed.year(), dt_parsed.month());
        let maturity_month = (front_maturity.year(), front_maturity.month());
        if dt_month < maturity_month {
            return build_single_ticker_batch(front_ticker);
        }
    }

    let start_date = dt_parsed - Duration::days(10);
    let volume_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(vec![front_ticker.clone(), second_ticker.clone()]),
        fields: Some(vec!["VOLUME".to_string()]),
        start_date: Some(fmt_date(start_date, None)),
        end_date: Some(fmt_date(dt_parsed, None)),
        ..Default::default()
    };

    let volume_batch = engine.request(volume_params).await?;
    let front_volume = latest_history_numeric_point(&volume_batch, &front_ticker, "VOLUME")?;
    let second_volume = latest_history_numeric_point(&volume_batch, &second_ticker, "VOLUME")?;

    let selected = match (front_volume, second_volume) {
        (Some((_, f)), Some((_, s))) if s > f => second_ticker,
        (Some(_), Some(_)) => front_ticker,
        (Some(_), None) => front_ticker,
        (None, Some(_)) => second_ticker,
        (None, None) => front_ticker,
    };

    build_single_ticker_batch(selected)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FuturesCurveRow {
    source_ticker: String,
    contract_ticker: String,
    generic_number: i32,
    px_bid: Option<f64>,
    px_ask: Option<f64>,
    last_tradeable_dt: Option<NaiveDate>,
    mid: Option<f64>,
    annualized_carry: Option<f64>,
    extra_values: Vec<Option<String>>,
}

/// Build a futures chain table with contract metadata, mid, and annualized carry.
pub async fn recipe_futures_curve(
    engine: &Engine,
    gen_ticker: String,
    asof: Option<String>,
    chain_field: Option<String>,
    fields: Option<Vec<String>>,
    max_contracts: Option<i32>,
) -> Result<RecordBatch> {
    validate_generic_ticker(&gen_ticker)?;
    let chain_field = chain_field.unwrap_or_else(|| "FUT_CHAIN_LAST_TRADE_DATES".to_string());

    let overrides = match asof {
        Some(date) => Some(vec![(
            "CHAIN_DATE".to_string(),
            fmt_date(parse_date(&date)?, None),
        )]),
        None => None,
    };
    let chain_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        extractor: ExtractorType::BulkData,
        extractor_set: true,
        securities: Some(vec![gen_ticker.clone()]),
        fields: Some(vec![chain_field.clone()]),
        overrides,
        ..Default::default()
    };
    let chain_batch = engine.request(chain_params).await?;
    let mut contracts = extract_chain_contracts(&chain_batch, max_contracts)?;
    if contracts.is_empty() {
        return Err(RecipeError::Other(format!(
            "Bloomberg returned no futures contracts for '{gen_ticker}' using field '{chain_field}'"
        )));
    }

    let mut request_fields = fields.unwrap_or_else(|| {
        vec![
            "PX_BID".to_string(),
            "PX_ASK".to_string(),
            "LAST_TRADEABLE_DT".to_string(),
        ]
    });
    ensure_field(&mut request_fields, "PX_BID");
    ensure_field(&mut request_fields, "PX_ASK");
    ensure_field(&mut request_fields, "LAST_TRADEABLE_DT");

    let metadata_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(
            contracts
                .iter()
                .map(|contract| contract.ticker.clone())
                .collect(),
        ),
        fields: Some(request_fields.clone()),
        ..Default::default()
    };
    let metadata_batch = engine.request(metadata_params).await?;
    let metadata = extract_refdata_values(&metadata_batch)?;

    for contract in &mut contracts {
        if contract.expiry.is_none() {
            contract.expiry = metadata
                .get(&contract.ticker)
                .and_then(|fields| fields.get("LAST_TRADEABLE_DT"))
                .and_then(|value| parse_any_date(value));
        }
    }

    contracts.sort_by(|left, right| {
        left.expiry
            .cmp(&right.expiry)
            .then(left.order.cmp(&right.order))
            .then(left.ticker.cmp(&right.ticker))
    });

    let extra_fields = request_fields
        .iter()
        .filter(|field| {
            !matches!(
                field.to_ascii_uppercase().as_str(),
                "PX_BID" | "PX_ASK" | "LAST_TRADEABLE_DT"
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let rows = build_futures_curve_rows(&gen_ticker, &contracts, &metadata, &extra_fields);
    build_futures_curve_batch(&rows, &extra_fields)
}

/// Resolve a generic CDX ticker (`GEN`) to a canonical specific series.
///
/// The result always includes an explicit `Vn`, including V1.
pub async fn recipe_cdx_ticker(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
) -> Result<RecordBatch> {
    recipe_cdx_ticker_with_options(engine, gen_ticker, dt, false).await
}

/// Resolve a generic CDX ticker with presentation options.
///
/// `versionless` affects only the returned ticker; resolution still requires
/// Bloomberg `VERSION` metadata.
pub async fn recipe_cdx_ticker_with_options(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
    versionless: bool,
) -> Result<RecordBatch> {
    let resolved = resolve_current_cdx(engine, &gen_ticker, parse_date(&dt)?).await?;
    build_single_ticker_batch(build_resolved_cdx_ticker_with_options(
        &resolved,
        versionless,
    ))
}

/// Resolve the most active canonical CDX series around a reference date.
///
/// Current and previous candidates are resolved and queried with their own
/// explicit versions.
pub async fn recipe_active_cdx(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
    lookback_days: Option<i32>,
) -> Result<RecordBatch> {
    recipe_active_cdx_with_options(engine, gen_ticker, dt, lookback_days, false).await
}

/// Resolve the most active CDX series with presentation options.
///
/// `versionless` affects only the selected output ticker.
pub async fn recipe_active_cdx_with_options(
    engine: &Engine,
    gen_ticker: String,
    dt: String,
    lookback_days: Option<i32>,
    versionless: bool,
) -> Result<RecordBatch> {
    let dt_parsed = parse_date(&dt)?;
    let start_date = cdx_lookback_start(dt_parsed, lookback_days)?;
    let current = resolve_current_cdx(engine, &gen_ticker, dt_parsed).await?;

    let Some(previous_alias) = current.previous_alias() else {
        return build_single_ticker_batch(build_resolved_cdx_ticker_with_options(
            &current,
            versionless,
        ));
    };
    let previous = resolve_specific_cdx(engine, &previous_alias).await?;
    let current_ticker = build_resolved_cdx_ticker_with_options(&current, false);
    let previous_ticker = build_resolved_cdx_ticker_with_options(&previous, false);

    let price_params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(vec![current_ticker.clone(), previous_ticker.clone()]),
        fields: Some(vec!["PX_LAST".to_string()]),
        start_date: Some(fmt_date(start_date, None)),
        end_date: Some(fmt_date(dt_parsed, None)),
        ..Default::default()
    };

    let price_batch = engine.request(price_params).await?;
    let current_latest = latest_history_numeric_point(&price_batch, &current_ticker, "PX_LAST")?
        .map(|(date, _)| date);
    let previous_latest = latest_history_numeric_point(&price_batch, &previous_ticker, "PX_LAST")?
        .map(|(date, _)| date);

    let selected = match (current_latest, previous_latest) {
        (Some(cur), Some(prev)) if prev > cur => &previous,
        (Some(_), Some(_)) | (Some(_), None) | (None, None) => &current,
        (None, Some(_)) => &previous,
    };

    build_single_ticker_batch(build_resolved_cdx_ticker_with_options(
        selected,
        versionless,
    ))
}

fn cdx_lookback_start(dt: NaiveDate, lookback_days: Option<i32>) -> Result<NaiveDate> {
    let lookback_days = i64::from(lookback_days.unwrap_or(10).max(1));
    let lookback = Duration::try_days(lookback_days).ok_or_else(|| {
        RecipeError::InvalidArgument(format!("lookback_days={lookback_days} is out of range"))
    })?;
    dt.checked_sub_signed(lookback).ok_or_else(|| {
        RecipeError::InvalidArgument(format!(
            "lookback_days={lookback_days} is out of range for {dt}"
        ))
    })
}

async fn resolve_current_cdx(
    engine: &Engine,
    gen_ticker: &str,
    dt: NaiveDate,
) -> Result<ResolvedCdxInfo> {
    let parsed = parse_cdx_ticker(gen_ticker)?;
    if !parsed.is_generic {
        return Err(RecipeError::InvalidArgument(format!(
            "'{gen_ticker}' must be a generic CDX ticker containing GEN"
        )));
    }

    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(vec![gen_ticker.to_string()]),
        fields: Some(vec![
            "ROLLING_SERIES".to_string(),
            "VERSION".to_string(),
            "CDS_FIRST_ACCRUAL_START_DATE".to_string(),
        ]),
        ..Default::default()
    };

    let batch = engine.request(params).await?;
    let series = required_cdx_number(&batch, gen_ticker, "ROLLING_SERIES")?;
    let accrual_dt =
        extract_refdata_date_for_ticker(&batch, gen_ticker, "CDS_FIRST_ACCRUAL_START_DATE")?;

    if accrual_dt.is_some_and(|start| dt < start) && series > 1 {
        let previous_alias = gen_to_specific(gen_ticker, series - 1)?;
        resolve_specific_cdx(engine, &previous_alias).await
    } else {
        let version = required_cdx_number(&batch, gen_ticker, "VERSION")?;
        ResolvedCdxInfo::resolve(parsed, series, version).map_err(Into::into)
    }
}

async fn resolve_specific_cdx(engine: &Engine, ticker: &str) -> Result<ResolvedCdxInfo> {
    let parsed = parse_cdx_ticker(ticker)?;
    if parsed.is_generic {
        return Err(RecipeError::InvalidArgument(format!(
            "'{ticker}' must be a specific CDX ticker"
        )));
    }
    let series = parsed
        .series_num
        .ok_or_else(|| RecipeError::Other(format!("missing parsed CDX series for '{ticker}'")))?;
    let version = resolve_cdx_version(engine, ticker).await?;
    ResolvedCdxInfo::resolve(parsed, series, version).map_err(Into::into)
}

async fn resolve_cdx_version(engine: &Engine, ticker: &str) -> Result<u32> {
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::ReferenceData.to_string(),
        securities: Some(vec![ticker.to_string()]),
        fields: Some(vec!["VERSION".to_string()]),
        ..Default::default()
    };
    let batch = engine.request(params).await?;
    required_cdx_number(&batch, ticker, "VERSION")
}

fn required_cdx_number(batch: &RecordBatch, ticker: &str, field: &str) -> Result<u32> {
    let raw = extract_refdata_string_for_ticker(batch, ticker, field)?.ok_or_else(|| {
        RecipeError::Other(format!(
            "missing {field} for '{ticker}' in Bloomberg response"
        ))
    })?;
    parse_series_number(&raw).ok_or_else(|| {
        RecipeError::Other(format!("unable to parse {field}='{raw}' for '{ticker}'"))
    })
}

async fn bloomberg_current_generic_ticker(
    engine: &Engine,
    gen_ticker: &str,
    dt: NaiveDate,
) -> Result<Option<String>> {
    let start_date = dt - Duration::days(10);
    let params = RequestParams {
        service: Service::RefData.to_string(),
        operation: Operation::HistoricalData.to_string(),
        securities: Some(vec![gen_ticker.to_string()]),
        fields: Some(vec!["FUT_CUR_GEN_TICKER".to_string()]),
        start_date: Some(fmt_date(start_date, None)),
        end_date: Some(fmt_date(dt, None)),
        ..Default::default()
    };

    let batch = engine.request(params).await?;
    let Some((_, mapped_root)) =
        latest_history_string_point(&batch, gen_ticker, "FUT_CUR_GEN_TICKER")?
    else {
        return Ok(None);
    };

    normalize_mapped_generic_ticker(&mapped_root, gen_ticker)
}

fn normalize_mapped_generic_ticker(raw: &str, gen_ticker: &str) -> Result<Option<String>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    if raw.split_whitespace().count() > 1 {
        return Ok(Some(raw.to_string()));
    }

    let parts = parse_ticker_parts(gen_ticker)?;
    let ticker = match parts.asset.as_str() {
        "Equity" => {
            let exchange = parts.exchange.unwrap_or_else(|| "US".to_string());
            format!("{raw} {exchange} {}", parts.asset)
        }
        _ => format!("{raw} {}", parts.asset),
    };

    Ok(Some(ticker))
}

fn futures_candidate_count(gen_ticker: &str, idx: usize) -> Result<usize> {
    let parts = parse_ticker_parts(gen_ticker)?;
    let month_ext = if parts.asset == "Comdty" { 4 } else { 2 };
    Ok(std::cmp::max(idx + month_ext, 3))
}

fn with_generic_index(gen_ticker: &str, index: u32) -> Result<String> {
    let parts = parse_ticker_parts(gen_ticker)?;

    let ticker = match parts.asset.as_str() {
        "Equity" => {
            let exchange = parts.exchange.unwrap_or_else(|| "US".to_string());
            format!("{}{} {} {}", parts.prefix, index, exchange, parts.asset)
        }
        _ => format!("{}{} {}", parts.prefix, index, parts.asset),
    };

    Ok(ticker)
}

#[derive(Debug, Clone, PartialEq)]
struct ChainContract {
    order: usize,
    ticker: String,
    expiry: Option<NaiveDate>,
}

fn ensure_field(fields: &mut Vec<String>, required: &str) {
    if !fields
        .iter()
        .any(|field| field.eq_ignore_ascii_case(required))
    {
        fields.push(required.to_string());
    }
}

fn extract_chain_contracts(
    batch: &RecordBatch,
    max_contracts: Option<i32>,
) -> Result<Vec<ChainContract>> {
    let contract_col_name = find_contract_column(batch).ok_or_else(|| {
        RecipeError::Other(
            "futures chain response did not include a contract ticker column".to_string(),
        )
    })?;
    let contract_col = batch
        .column_by_name(&contract_col_name)
        .ok_or_else(|| RecipeError::Other(format!("missing '{contract_col_name}' column")))?;
    let expiry_col = find_chain_expiry_column(batch).and_then(|name| batch.column_by_name(&name));
    let limit = max_contracts
        .and_then(|value| (value > 0).then_some(value as usize))
        .unwrap_or(usize::MAX);

    let mut seen = std::collections::HashSet::new();
    let mut contracts = Vec::new();
    for row in 0..batch.num_rows() {
        if contracts.len() >= limit {
            break;
        }
        let Some(raw_ticker) = crate::utils::array_value_as_string(contract_col, row) else {
            continue;
        };
        let ticker = raw_ticker.trim();
        if ticker.is_empty() || !seen.insert(ticker.to_string()) {
            continue;
        }
        let expiry = expiry_col.and_then(|col| crate::utils::array_value_as_date(col, row));
        contracts.push(ChainContract {
            order: contracts.len(),
            ticker: ticker.to_string(),
            expiry,
        });
    }

    Ok(contracts)
}

fn find_contract_column(batch: &RecordBatch) -> Option<String> {
    let candidates = [
        "future's ticker",
        "futures ticker",
        "future ticker",
        "security description",
        "contract ticker",
        "ticker",
    ];
    let wanted = candidates
        .iter()
        .map(|candidate| canonical_name(candidate))
        .collect::<Vec<_>>();
    batch
        .schema()
        .fields()
        .iter()
        .find_map(|field| {
            let name = field.name();
            if name == "ticker" || name == "field" {
                return None;
            }
            let key = canonical_name(name);
            wanted
                .iter()
                .any(|candidate| candidate == &key)
                .then(|| name.to_string())
        })
        .or_else(|| {
            batch.schema().fields().iter().find_map(|field| {
                let name = field.name();
                (name != "ticker" && name != "field").then(|| name.to_string())
            })
        })
}

fn find_chain_expiry_column(batch: &RecordBatch) -> Option<String> {
    let candidates = [
        "last trade date",
        "last tradeable dt",
        "last_tradeable_dt",
        "expiry",
        "expiration date",
    ];
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

fn extract_refdata_values(
    batch: &RecordBatch,
) -> Result<std::collections::HashMap<String, std::collections::HashMap<String, String>>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;
    let mut out = std::collections::HashMap::new();

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }
        let Some(value) = array_value_as_string(value_col, row) else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        out.entry(ticker_col.value(row).to_string())
            .or_insert_with(std::collections::HashMap::new)
            .insert(field_col.value(row).to_ascii_uppercase(), value.to_string());
    }

    Ok(out)
}

fn build_futures_curve_rows(
    source_ticker: &str,
    contracts: &[ChainContract],
    metadata: &std::collections::HashMap<String, std::collections::HashMap<String, String>>,
    extra_fields: &[String],
) -> Vec<FuturesCurveRow> {
    let mut rows = Vec::with_capacity(contracts.len());
    let mut previous_mid: Option<f64> = None;
    let mut previous_expiry: Option<NaiveDate> = None;

    for (idx, contract) in contracts.iter().enumerate() {
        let fields = metadata.get(&contract.ticker);
        let px_bid = fields
            .and_then(|map| map.get("PX_BID"))
            .and_then(|value| crate::utils::parse_f64_like(value));
        let px_ask = fields
            .and_then(|map| map.get("PX_ASK"))
            .and_then(|value| crate::utils::parse_f64_like(value));
        let expiry = contract.expiry.or_else(|| {
            fields
                .and_then(|map| map.get("LAST_TRADEABLE_DT"))
                .and_then(|value| parse_any_date(value))
        });
        let mid = match (px_bid, px_ask) {
            (Some(bid), Some(ask)) if bid.is_finite() && ask.is_finite() => Some((bid + ask) / 2.0),
            _ => None,
        };
        let annualized_carry = match (previous_mid, mid, previous_expiry, expiry) {
            (Some(prev_mid), Some(cur_mid), Some(prev_expiry), Some(cur_expiry))
                if prev_mid != 0.0 && cur_expiry > prev_expiry =>
            {
                let year_delta = (cur_expiry - prev_expiry).num_days() as f64 / 365.25;
                (year_delta > 0.0).then_some((cur_mid / prev_mid - 1.0) / year_delta)
            }
            _ => None,
        };
        let extra_values = extra_fields
            .iter()
            .map(|field| {
                fields
                    .and_then(|map| map.get(&field.to_ascii_uppercase()))
                    .cloned()
            })
            .collect::<Vec<_>>();

        rows.push(FuturesCurveRow {
            source_ticker: source_ticker.to_string(),
            contract_ticker: contract.ticker.clone(),
            generic_number: (idx + 1) as i32,
            px_bid,
            px_ask,
            last_tradeable_dt: expiry,
            mid,
            annualized_carry,
            extra_values,
        });

        if let Some(value) = mid {
            previous_mid = Some(value);
        }
        if let Some(value) = expiry {
            previous_expiry = Some(value);
        }
    }

    rows
}

fn append_f64(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_date(builder: &mut arrow_array::builder::Date32Builder, value: Option<NaiveDate>) {
    match value {
        Some(value) => builder.append_value(naive_to_date32(value)),
        None => builder.append_null(),
    }
}

fn append_string(builder: &mut StringBuilder, value: Option<&String>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn build_futures_curve_batch(
    rows: &[FuturesCurveRow],
    extra_fields: &[String],
) -> Result<RecordBatch> {
    let mut source = StringBuilder::new();
    let mut contract = StringBuilder::new();
    let mut generic = Int32Builder::new();
    let mut bid = Float64Builder::new();
    let mut ask = Float64Builder::new();
    let mut expiry = arrow_array::builder::Date32Builder::new();
    let mut mid = Float64Builder::new();
    let mut carry = Float64Builder::new();
    let mut extra_builders = extra_fields
        .iter()
        .map(|_| StringBuilder::new())
        .collect::<Vec<_>>();

    for row in rows {
        source.append_value(&row.source_ticker);
        contract.append_value(&row.contract_ticker);
        generic.append_value(row.generic_number);
        append_f64(&mut bid, row.px_bid);
        append_f64(&mut ask, row.px_ask);
        append_date(&mut expiry, row.last_tradeable_dt);
        append_f64(&mut mid, row.mid);
        append_f64(&mut carry, row.annualized_carry);
        for (builder, value) in extra_builders.iter_mut().zip(row.extra_values.iter()) {
            append_string(builder, value.as_ref());
        }
    }

    let mut schema_fields = vec![
        Field::new("source_ticker", DataType::Utf8, false),
        Field::new("contract_ticker", DataType::Utf8, false),
        Field::new("generic_number", DataType::Int32, false),
        Field::new("px_bid", DataType::Float64, true),
        Field::new("px_ask", DataType::Float64, true),
        Field::new("last_tradeable_dt", DataType::Date32, true),
        Field::new("mid", DataType::Float64, true),
        Field::new("annualized_carry", DataType::Float64, true),
    ];
    for field in extra_fields {
        schema_fields.push(Field::new(canonical_name(field), DataType::Utf8, true));
    }

    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(source.finish()),
        Arc::new(contract.finish()),
        Arc::new(generic.finish()),
        Arc::new(bid.finish()),
        Arc::new(ask.finish()),
        Arc::new(expiry.finish()),
        Arc::new(mid.finish()),
        Arc::new(carry.finish()),
    ];
    for mut builder in extra_builders {
        columns.push(Arc::new(builder.finish()));
    }

    let schema = Arc::new(Schema::new(schema_fields));
    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

fn build_single_ticker_batch(ticker: String) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ticker",
        DataType::Utf8,
        false,
    )]));
    let ticker_array = StringArray::from(vec![ticker]);
    RecordBatch::try_new(schema, vec![Arc::new(ticker_array)]).map_err(Into::into)
}

fn extract_single_ticker(batch: &RecordBatch) -> Result<String> {
    let ticker_col = batch
        .column_by_name("ticker")
        .ok_or_else(|| RecipeError::Other("missing 'ticker' column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| RecipeError::Other("'ticker' column must be Utf8".to_string()))?;

    if ticker_col.is_empty() || ticker_col.is_null(0) {
        return Err(RecipeError::Other(
            "resolved ticker batch is empty or null".to_string(),
        ));
    }

    Ok(ticker_col.value(0).to_string())
}

fn extract_refdata_string_for_ticker(
    batch: &RecordBatch,
    ticker: &str,
    field: &str,
) -> Result<Option<String>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }

        if ticker_col.value(row) != ticker || !field_col.value(row).eq_ignore_ascii_case(field) {
            continue;
        }

        let Some(raw) = array_value_as_string(value_col, row) else {
            continue;
        };
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }

        return Ok(Some(raw.to_string()));
    }

    Ok(None)
}

fn extract_refdata_date_values(
    batch: &RecordBatch,
    field: &str,
) -> Result<Vec<(String, NaiveDate)>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;

    let mut output = Vec::new();

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }

        if !field_col.value(row).eq_ignore_ascii_case(field) {
            continue;
        }

        let Some(parsed) = array_value_as_date(value_col, row) else {
            continue;
        };

        output.push((ticker_col.value(row).to_string(), parsed));
    }

    Ok(output)
}

fn extract_refdata_date_for_ticker(
    batch: &RecordBatch,
    ticker: &str,
    field: &str,
) -> Result<Option<NaiveDate>> {
    let raw = extract_refdata_string_for_ticker(batch, ticker, field)?;
    Ok(raw.and_then(|v| parse_any_date(&v)))
}

fn latest_history_numeric_point(
    batch: &RecordBatch,
    ticker: &str,
    field: &str,
) -> Result<Option<(NaiveDate, f64)>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;

    let date_col = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("missing 'date' column".to_string()))?;
    let date32_col = date_col.as_any().downcast_ref::<Date32Array>();
    let date_str_col = date_col.as_any().downcast_ref::<StringArray>();

    let mut best: Option<(NaiveDate, f64)> = None;

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }

        if ticker_col.value(row) != ticker || !field_col.value(row).eq_ignore_ascii_case(field) {
            continue;
        }

        let Some(value) = array_value_as_f64(value_col, row) else {
            continue;
        };

        let row_date = if let Some(col) = date32_col {
            if col.is_null(row) {
                None
            } else {
                date32_to_naive(col.value(row))
            }
        } else if let Some(col) = date_str_col {
            if col.is_null(row) {
                None
            } else {
                parse_any_date(col.value(row))
            }
        } else {
            None
        };

        let Some(row_date) = row_date else {
            continue;
        };

        match best {
            Some((best_date, _)) if row_date < best_date => {}
            _ => best = Some((row_date, value)),
        }
    }

    Ok(best)
}

fn latest_history_string_point(
    batch: &RecordBatch,
    ticker: &str,
    field: &str,
) -> Result<Option<(NaiveDate, String)>> {
    let ticker_col = as_string_col(batch, "ticker")?;
    let field_col = as_string_col(batch, "field")?;
    let value_col = batch
        .column_by_name("value")
        .ok_or_else(|| RecipeError::Other("missing 'value' column".to_string()))?;

    let date_col = batch
        .column_by_name("date")
        .ok_or_else(|| RecipeError::Other("missing 'date' column".to_string()))?;
    let date32_col = date_col.as_any().downcast_ref::<Date32Array>();
    let date_str_col = date_col.as_any().downcast_ref::<StringArray>();

    let mut best: Option<(NaiveDate, String)> = None;

    for row in 0..batch.num_rows() {
        if ticker_col.is_null(row) || field_col.is_null(row) || value_col.is_null(row) {
            continue;
        }

        if ticker_col.value(row) != ticker || !field_col.value(row).eq_ignore_ascii_case(field) {
            continue;
        }

        let Some(raw_value) = array_value_as_string(value_col, row) else {
            continue;
        };
        let raw_value = raw_value.trim();
        if raw_value.is_empty() {
            continue;
        }

        let row_date = if let Some(col) = date32_col {
            if col.is_null(row) {
                None
            } else {
                date32_to_naive(col.value(row))
            }
        } else if let Some(col) = date_str_col {
            if col.is_null(row) {
                None
            } else {
                parse_any_date(col.value(row))
            }
        } else {
            None
        };

        let Some(row_date) = row_date else {
            continue;
        };

        match best {
            Some((best_date, _)) if row_date < best_date => {}
            _ => best = Some((row_date, raw_value.to_string())),
        }
    }

    Ok(best)
}

fn parse_series_number(value: &str) -> Option<u32> {
    let value = value.trim();

    if let Ok(v) = value.parse::<u32>() {
        return (v > 0).then_some(v);
    }

    let parsed = value.parse::<f64>().ok()?;
    if !parsed.is_finite() || parsed < 1.0 || parsed > f64::from(u32::MAX) || parsed.fract() != 0.0
    {
        return None;
    }

    Some(parsed as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_futures_candidate_count_rules() {
        let idx0 = futures_candidate_count("ES1 Index", 0).unwrap();
        let idx2 = futures_candidate_count("CL3 Comdty", 2).unwrap();

        assert_eq!(idx0, 3);
        assert_eq!(idx2, 6);
    }

    #[test]
    fn test_with_generic_index_preserves_asset_shape() {
        assert_eq!(with_generic_index("ES1 Index", 2).unwrap(), "ES2 Index");
        assert_eq!(
            with_generic_index("SPY1 US Equity", 2).unwrap(),
            "SPY2 US Equity"
        );
    }

    #[test]
    fn test_normalize_mapped_generic_ticker_adds_asset_suffix() {
        assert_eq!(
            normalize_mapped_generic_ticker("UXK6", "UX1 Index")
                .unwrap()
                .unwrap(),
            "UXK6 Index"
        );
        assert_eq!(
            normalize_mapped_generic_ticker("CLM6", "CL1 Comdty")
                .unwrap()
                .unwrap(),
            "CLM6 Comdty"
        );
        assert_eq!(
            normalize_mapped_generic_ticker("SPYH6", "SPY1 US Equity")
                .unwrap()
                .unwrap(),
            "SPYH6 US Equity"
        );
        assert_eq!(
            normalize_mapped_generic_ticker("UXK6 Index", "UX1 Index")
                .unwrap()
                .unwrap(),
            "UXK6 Index"
        );
    }

    #[test]
    fn test_extract_refdata_date_values_parses_iso_dates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ESH24 Index", "ESM24 Index"])),
                Arc::new(StringArray::from(vec![
                    "LAST_TRADEABLE_DT",
                    "LAST_TRADEABLE_DT",
                ])),
                Arc::new(StringArray::from(vec!["2024-03-15", "2024-06-21"])),
            ],
        )
        .unwrap();

        let parsed = extract_refdata_date_values(&batch, "LAST_TRADEABLE_DT").unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, "ESH24 Index");
        assert_eq!(parsed[0].1, NaiveDate::from_ymd_opt(2024, 3, 15).unwrap());
    }

    #[test]
    fn test_extract_refdata_date_values_accepts_typed_date_value_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Date32, false),
        ]));
        let d1 = (NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d2 = (NaiveDate::from_ymd_opt(2024, 6, 21).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ESH24 Index", "ESM24 Index"])),
                Arc::new(StringArray::from(vec![
                    "LAST_TRADEABLE_DT",
                    "LAST_TRADEABLE_DT",
                ])),
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2)])),
            ],
        )
        .unwrap();

        let parsed = extract_refdata_date_values(&batch, "LAST_TRADEABLE_DT").unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, "ESH24 Index");
        assert_eq!(parsed[0].1, NaiveDate::from_ymd_opt(2024, 3, 15).unwrap());
    }

    #[test]
    fn test_latest_history_numeric_point_uses_latest_non_null_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("date", DataType::Date32, true),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let d1 = (NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d2 = (NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "ESH24 Index",
                    "ESH24 Index",
                    "ESM24 Index",
                ])),
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2), Some(d2)])),
                Arc::new(StringArray::from(vec!["VOLUME", "VOLUME", "VOLUME"])),
                Arc::new(StringArray::from(vec!["100", "250", "175"])),
            ],
        )
        .unwrap();

        let latest = latest_history_numeric_point(&batch, "ESH24 Index", "VOLUME")
            .unwrap()
            .unwrap();

        assert_eq!(latest.0, NaiveDate::from_ymd_opt(2024, 1, 3).unwrap());
        assert_eq!(latest.1, 250.0);
    }

    #[test]
    fn test_latest_history_numeric_point_accepts_typed_value_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("date", DataType::Date32, true),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let d1 = (NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d2 = (NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "UXK24 Index",
                    "UXK24 Index",
                    "UXM24 Index",
                ])),
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2), Some(d2)])),
                Arc::new(StringArray::from(vec!["VOLUME", "VOLUME", "VOLUME"])),
                Arc::new(arrow_array::Float64Array::from(vec![
                    Some(100.0),
                    Some(250.0),
                    Some(300.0),
                ])),
            ],
        )
        .unwrap();

        let latest = latest_history_numeric_point(&batch, "UXK24 Index", "VOLUME")
            .unwrap()
            .unwrap();

        assert_eq!(latest.0, NaiveDate::from_ymd_opt(2024, 1, 3).unwrap());
        assert_eq!(latest.1, 250.0);
    }

    #[test]
    fn test_latest_history_string_point_uses_latest_mapping_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("date", DataType::Date32, true),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let d1 = (NaiveDate::from_ymd_opt(2026, 4, 15).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d2 = (NaiveDate::from_ymd_opt(2026, 4, 16).unwrap()
            - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "UX1 Index",
                    "UX1 Index",
                    "UX2 Index",
                ])),
                Arc::new(Date32Array::from(vec![Some(d1), Some(d2), Some(d2)])),
                Arc::new(StringArray::from(vec![
                    "FUT_CUR_GEN_TICKER",
                    "FUT_CUR_GEN_TICKER",
                    "FUT_CUR_GEN_TICKER",
                ])),
                Arc::new(StringArray::from(vec!["UXJ6", "UXK6", "UXM6"])),
            ],
        )
        .unwrap();

        let latest = latest_history_string_point(&batch, "UX1 Index", "FUT_CUR_GEN_TICKER")
            .unwrap()
            .unwrap();

        assert_eq!(latest.0, NaiveDate::from_ymd_opt(2026, 4, 16).unwrap());
        assert_eq!(latest.1, "UXK6");
    }

    #[test]
    fn test_parse_series_number_accepts_int_and_decimal_text() {
        assert_eq!(parse_series_number("45"), Some(45));
        assert_eq!(parse_series_number("45.0"), Some(45));
        assert_eq!(parse_series_number("45.5"), None);
        assert_eq!(parse_series_number("abc"), None);
        assert_eq!(parse_series_number("4294967296.0"), None);
    }

    #[test]
    fn test_cdx_lookback_start_rejects_out_of_range() {
        let dt = NaiveDate::from_ymd_opt(2026, 7, 17).unwrap();
        assert_eq!(
            cdx_lookback_start(dt, None).unwrap(),
            dt - Duration::days(10)
        );
        assert!(cdx_lookback_start(dt, Some(i32::MAX)).is_err());
    }

    #[test]
    fn test_required_cdx_number_requires_present_positive_metadata() {
        let ticker = "CDX HY CDSI GEN 5Y Corp";
        let schema = Arc::new(Schema::new(vec![
            Field::new("ticker", DataType::Utf8, false),
            Field::new("field", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![ticker])),
                Arc::new(StringArray::from(vec!["VERSION"])),
                Arc::new(StringArray::from(vec!["2"])),
            ],
        )
        .unwrap();

        assert_eq!(required_cdx_number(&batch, ticker, "VERSION").unwrap(), 2);
        assert!(required_cdx_number(&batch, ticker, "ROLLING_SERIES").is_err());
        assert_eq!(parse_series_number("0"), None);
    }

    #[test]
    fn test_build_and_extract_single_ticker_batch() {
        let batch = build_single_ticker_batch("CDX IG CDSI S45 5Y Corp".to_string()).unwrap();
        let ticker = extract_single_ticker(&batch).unwrap();
        assert_eq!(ticker, "CDX IG CDSI S45 5Y Corp");
    }

    #[test]
    fn test_futures_curve_rows_compute_mid_and_carry() {
        let front_expiry = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let second_expiry = NaiveDate::from_ymd_opt(2024, 6, 21).unwrap();
        let contracts = vec![
            ChainContract {
                order: 0,
                ticker: "ESH24 Index".to_string(),
                expiry: Some(front_expiry),
            },
            ChainContract {
                order: 1,
                ticker: "ESM24 Index".to_string(),
                expiry: Some(second_expiry),
            },
        ];
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            "ESH24 Index".to_string(),
            std::collections::HashMap::from([
                ("PX_BID".to_string(), "4999".to_string()),
                ("PX_ASK".to_string(), "5001".to_string()),
            ]),
        );
        metadata.insert(
            "ESM24 Index".to_string(),
            std::collections::HashMap::from([
                ("PX_BID".to_string(), "5049".to_string()),
                ("PX_ASK".to_string(), "5051".to_string()),
            ]),
        );

        let rows = build_futures_curve_rows("ES1 Index", &contracts, &metadata, &[]);

        assert_eq!(rows[0].generic_number, 1);
        assert_eq!(rows[0].mid, Some(5000.0));
        assert_eq!(rows[0].annualized_carry, None);
        assert_eq!(rows[1].mid, Some(5050.0));
        assert!(rows[1].annualized_carry.unwrap() > 0.0);
    }

    #[test]
    fn test_futures_curve_missing_bid_or_ask_keeps_mid_null() {
        let contracts = vec![ChainContract {
            order: 0,
            ticker: "ESH24 Index".to_string(),
            expiry: Some(NaiveDate::from_ymd_opt(2024, 3, 15).unwrap()),
        }];
        let metadata = std::collections::HashMap::from([(
            "ESH24 Index".to_string(),
            std::collections::HashMap::from([("PX_BID".to_string(), "4999".to_string())]),
        )]);

        let rows = build_futures_curve_rows("ES1 Index", &contracts, &metadata, &[]);

        assert_eq!(rows[0].mid, None);
        assert_eq!(rows[0].annualized_carry, None);
    }
}
