use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use rmcp::ErrorData;
use schemars::JsonSchema;
use serde::Deserialize;
use xbbg_async::engine::{RequestParams, RequestParamsInput};
use xbbg_async::services::Operation;

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReferenceFormat {
    Long,
    LongTyped,
    LongMetadata,
}

impl ReferenceFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::LongTyped => "long_typed",
            Self::LongMetadata => "long_metadata",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoricalFormat {
    Long,
    LongTyped,
    LongMetadata,
    /// `wide` is the pre-1.2 spelling; both reach the same engine output.
    #[serde(alias = "wide")]
    SemiLong,
}

impl HistoricalFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::LongTyped => "long_typed",
            Self::LongMetadata => "long_metadata",
            Self::SemiLong => "semi_long",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct BdpArgs {
    tickers: Vec<String>,
    fields: Vec<String>,
    #[serde(default)]
    overrides: Option<BTreeMap<String, String>>,
    #[serde(default)]
    options: Option<BTreeMap<String, String>>,
    #[serde(default)]
    field_types: Option<BTreeMap<String, String>>,
    #[serde(default)]
    format: Option<ReferenceFormat>,
    #[serde(default)]
    include_security_errors: bool,
    #[serde(default)]
    validate_fields: Option<bool>,
    #[serde(default)]
    return_eids: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct BdhArgs {
    tickers: Vec<String>,
    fields: Vec<String>,
    start_date: String,
    end_date: String,
    #[serde(default)]
    overrides: Option<BTreeMap<String, String>>,
    #[serde(default)]
    options: Option<BTreeMap<String, String>>,
    #[serde(default)]
    field_types: Option<BTreeMap<String, String>>,
    #[serde(default)]
    format: Option<HistoricalFormat>,
    #[serde(default)]
    validate_fields: Option<bool>,
    #[serde(default)]
    return_eids: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct BdsArgs {
    tickers: Vec<String>,
    field: String,
    #[serde(default)]
    overrides: Option<BTreeMap<String, String>>,
    #[serde(default)]
    options: Option<BTreeMap<String, String>>,
    #[serde(default)]
    validate_fields: Option<bool>,
    #[serde(default)]
    return_eids: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct BdibArgs {
    ticker: String,
    start_datetime: String,
    end_datetime: String,
    interval: u32,
    #[serde(default)]
    event_type: Option<String>,
    #[serde(default)]
    request_tz: Option<String>,
    #[serde(default)]
    output_tz: Option<String>,
    #[serde(default)]
    options: Option<BTreeMap<String, String>>,
    #[serde(default)]
    return_eids: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct BqlArgs {
    expression: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct BsrchArgs {
    domain: String,
    #[serde(default)]
    parameters: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct BfldsArgs {
    #[serde(default)]
    fields: Option<Vec<String>>,
    #[serde(default)]
    search_spec: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct RequestArgs {
    service: String,
    #[serde(default)]
    operation: Option<String>,
    #[serde(default)]
    request_operation: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    extractor: Option<String>,
    #[serde(default)]
    securities: Option<Vec<String>>,
    #[serde(default)]
    security: Option<String>,
    #[serde(default)]
    fields: Option<Vec<String>>,
    #[serde(default)]
    overrides: Option<BTreeMap<String, String>>,
    #[serde(default)]
    elements: Option<BTreeMap<String, String>>,
    #[serde(default)]
    kwargs: Option<BTreeMap<String, String>>,
    #[serde(default)]
    start_date: Option<String>,
    #[serde(default)]
    end_date: Option<String>,
    #[serde(default)]
    start_datetime: Option<String>,
    #[serde(default)]
    end_datetime: Option<String>,
    #[serde(default)]
    request_tz: Option<String>,
    #[serde(default)]
    output_tz: Option<String>,
    #[serde(default)]
    event_type: Option<String>,
    #[serde(default)]
    event_types: Option<Vec<String>>,
    #[serde(default)]
    interval: Option<u32>,
    #[serde(default)]
    options: Option<BTreeMap<String, String>>,
    #[serde(default)]
    field_types: Option<BTreeMap<String, String>>,
    #[serde(default)]
    include_security_errors: Option<bool>,
    /// Request entitlement IDs (`returnEids`) in response metadata (`xbbg.eid_data`).
    /// Supported by ReferenceDataRequest (including BDS/bulk), HistoricalDataRequest,
    /// IntradayBarRequest, and IntradayTickRequest.
    #[serde(default)]
    return_eids: Option<bool>,
    #[serde(default)]
    validate_fields: Option<bool>,
    #[serde(default)]
    search_spec: Option<String>,
    #[serde(default)]
    field_ids: Option<Vec<String>>,
    #[serde(default)]
    format: Option<HistoricalFormat>,
}

/// Maximum number of EIDs accepted by one entitlement check.
pub(crate) const MAX_ENTITLEMENT_EIDS: usize = 10_000;

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct CheckEntitlementsArgs {
    /// One to 10,000 positive Bloomberg entitlement IDs. Duplicates are removed.
    eids: Vec<i32>,
    /// Bloomberg service used for the check. Defaults to `//blp/refdata`.
    #[serde(default)]
    service: Option<String>,
}

fn build_request_params(input: RequestParamsInput) -> Result<RequestParams, ErrorData> {
    input
        .into_request_params()
        .map_err(|err| ErrorData::invalid_params(err.to_string(), None))
}
pub(crate) fn bdp_request_params(args: BdpArgs) -> Result<RequestParams, ErrorData> {
    build_request_params(RequestParamsInput {
        service: String::new(),
        operation: Some(Operation::ReferenceData.to_string()),
        securities: Some(normalize_nonempty_list("tickers", args.tickers)?),
        fields: Some(normalize_nonempty_list("fields", args.fields)?),
        overrides: map_to_pairs(args.overrides),
        options: map_to_pairs(args.options),
        field_types: map_to_hash_map(args.field_types),
        include_security_errors: Some(args.include_security_errors),
        validate_fields: args.validate_fields,
        return_eids: Some(args.return_eids),
        format: args.format.map(|format| format.as_str().to_string()),
        ..Default::default()
    })
}

pub(crate) fn bdh_request_params(args: BdhArgs) -> Result<RequestParams, ErrorData> {
    build_request_params(RequestParamsInput {
        service: String::new(),
        operation: Some(Operation::HistoricalData.to_string()),
        securities: Some(normalize_nonempty_list("tickers", args.tickers)?),
        fields: Some(normalize_nonempty_list("fields", args.fields)?),
        start_date: Some(normalize_bloomberg_date("start_date", args.start_date)?),
        end_date: Some(normalize_bloomberg_date("end_date", args.end_date)?),
        overrides: map_to_pairs(args.overrides),
        options: map_to_pairs(args.options),
        field_types: map_to_hash_map(args.field_types),
        validate_fields: args.validate_fields,
        return_eids: Some(args.return_eids),
        format: args.format.map(|format| format.as_str().to_string()),
        ..Default::default()
    })
}

pub(crate) fn bds_request_params(args: BdsArgs) -> Result<RequestParams, ErrorData> {
    build_request_params(RequestParamsInput {
        service: String::new(),
        operation: Some(Operation::ReferenceData.to_string()),
        extractor: Some("bulk".to_string()),
        securities: Some(normalize_nonempty_list("tickers", args.tickers)?),
        fields: Some(vec![normalize_required_string("field", args.field)?]),
        overrides: map_to_pairs(args.overrides),
        options: map_to_pairs(args.options),
        validate_fields: args.validate_fields,
        return_eids: Some(args.return_eids),
        ..Default::default()
    })
}

pub(crate) fn bdib_request_params(args: BdibArgs) -> Result<RequestParams, ErrorData> {
    if args.interval == 0 {
        return Err(ErrorData::invalid_params(
            "interval must be greater than zero",
            None,
        ));
    }

    build_request_params(RequestParamsInput {
        service: String::new(),
        operation: Some(Operation::IntradayBar.to_string()),
        security: Some(normalize_required_string("ticker", args.ticker)?),
        event_type: Some(trim_optional(args.event_type).unwrap_or_else(|| "TRADE".to_string())),
        interval: Some(args.interval),
        start_datetime: Some(validate_datetime_string(
            "start_datetime",
            args.start_datetime,
        )?),
        end_datetime: Some(validate_datetime_string("end_datetime", args.end_datetime)?),
        request_tz: trim_optional(args.request_tz),
        output_tz: trim_optional(args.output_tz),
        options: map_to_pairs(args.options),
        return_eids: Some(args.return_eids),
        ..Default::default()
    })
}

pub(crate) fn bql_request_params(args: BqlArgs) -> Result<RequestParams, ErrorData> {
    build_request_params(RequestParamsInput {
        service: String::new(),
        operation: Some(Operation::BqlSendQuery.to_string()),
        elements: Some(vec![(
            "expression".to_string(),
            normalize_required_string("expression", args.expression)?,
        )]),
        ..Default::default()
    })
}

pub(crate) fn bsrch_request_params(args: BsrchArgs) -> Result<RequestParams, ErrorData> {
    let mut elements = vec![(
        "Domain".to_string(),
        normalize_required_string("domain", args.domain)?,
    )];
    if let Some(parameters) = map_to_pairs(args.parameters) {
        elements.extend(parameters);
    }

    build_request_params(RequestParamsInput {
        service: String::new(),
        operation: Some(Operation::ExcelGetGrid.to_string()),
        elements: Some(elements),
        ..Default::default()
    })
}

pub(crate) fn bflds_request_params(args: BfldsArgs) -> Result<RequestParams, ErrorData> {
    let fields = args
        .fields
        .map(|values| normalize_nonempty_list("fields", values))
        .transpose()?;
    let search_spec = trim_optional(args.search_spec);

    match (fields, search_spec) {
        (Some(field_ids), None) => build_request_params(RequestParamsInput {
            service: String::new(),
            operation: Some(Operation::FieldInfo.to_string()),
            field_ids: Some(field_ids),
            ..Default::default()
        }),
        (None, Some(search_spec)) => build_request_params(RequestParamsInput {
            service: String::new(),
            operation: Some(Operation::FieldSearch.to_string()),
            search_spec: Some(search_spec),
            ..Default::default()
        }),
        (Some(_), Some(_)) => Err(ErrorData::invalid_params(
            "bflds accepts either fields or search_spec, not both",
            None,
        )),
        (None, None) => Err(ErrorData::invalid_params(
            "bflds requires either fields or search_spec",
            None,
        )),
    }
}

pub(crate) fn generic_request_params(args: RequestArgs) -> Result<RequestParams, ErrorData> {
    let service = normalize_required_string("service", args.service)?;
    let request_operation = trim_optional(args.request_operation);
    let operation = trim_optional(args.operation);
    let fields = args
        .fields
        .map(|values| normalize_nonempty_list("fields", values))
        .transpose()?;
    let field_ids = args
        .field_ids
        .map(|values| normalize_nonempty_list("field_ids", values))
        .transpose()?;
    let search_spec = trim_optional(args.search_spec);
    let format = args.format.map(|format| format.as_str().to_string());

    build_request_params(RequestParamsInput {
        service,
        operation,
        request_operation,
        request_id: trim_optional(args.request_id),
        extractor: args.extractor.and_then(|value| trim_optional(Some(value))),
        securities: args
            .securities
            .map(|values| normalize_nonempty_list("securities", values))
            .transpose()?,
        security: args
            .security
            .map(|value| normalize_required_string("security", value))
            .transpose()?,
        fields,
        overrides: map_to_pairs(args.overrides),
        security_overrides: None,
        elements: map_to_pairs(args.elements),
        kwargs: map_to_hash_map(args.kwargs),
        // The generic tool intentionally preserves caller-supplied request strings instead of
        // normalizing them to one wrapper opinion; power users may rely on raw/custom semantics.
        start_date: trim_optional(args.start_date),
        end_date: trim_optional(args.end_date),
        start_datetime: trim_optional(args.start_datetime),
        end_datetime: trim_optional(args.end_datetime),
        request_tz: trim_optional(args.request_tz),
        output_tz: trim_optional(args.output_tz),
        event_type: trim_optional(args.event_type),
        event_types: args
            .event_types
            .map(|values| normalize_nonempty_list("event_types", values))
            .transpose()?,
        interval: args.interval,
        options: map_to_pairs(args.options),
        field_types: map_to_hash_map(args.field_types),
        include_security_errors: args.include_security_errors,
        return_eids: args.return_eids,
        validate_fields: args.validate_fields,
        search_spec,
        field_ids,
        format,
    })
}

pub(crate) fn check_entitlements_params(
    args: CheckEntitlementsArgs,
) -> Result<(String, Vec<i32>), ErrorData> {
    if args.eids.is_empty() || args.eids.len() > MAX_ENTITLEMENT_EIDS {
        return Err(ErrorData::invalid_params(
            format!("eids must contain between 1 and {MAX_ENTITLEMENT_EIDS} entitlement IDs"),
            None,
        ));
    }
    if args.eids.iter().any(|eid| *eid <= 0) {
        return Err(ErrorData::invalid_params(
            "eids must contain only positive entitlement IDs",
            None,
        ));
    }
    let mut seen = HashSet::with_capacity(args.eids.len());
    let eids = args
        .eids
        .into_iter()
        .filter(|eid| seen.insert(*eid))
        .collect();
    let service = match args.service {
        Some(service) => normalize_required_string("service", service)?,
        None => "//blp/refdata".to_string(),
    };
    Ok((service, eids))
}

fn normalize_required_string(field: &str, value: String) -> Result<String, ErrorData> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ErrorData::invalid_params(
            format!("{field} must be a non-empty string"),
            None,
        ));
    }
    Ok(trimmed.to_string())
}

fn normalize_nonempty_list(field: &str, values: Vec<String>) -> Result<Vec<String>, ErrorData> {
    let normalized = values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if normalized.is_empty() {
        return Err(ErrorData::invalid_params(
            format!("{field} must contain at least one non-empty value"),
            None,
        ));
    }
    Ok(normalized)
}

fn trim_optional(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_bloomberg_date(field: &str, value: String) -> Result<String, ErrorData> {
    let trimmed = normalize_required_string(field, value)?;
    let parsed = NaiveDate::parse_from_str(&trimmed, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(&trimmed, "%Y-%m-%d"))
        .map_err(|_| {
            ErrorData::invalid_params(format!("{field} must be YYYYMMDD or YYYY-MM-DD"), None)
        })?;
    Ok(parsed.format("%Y%m%d").to_string())
}

fn validate_datetime_string(field: &str, value: String) -> Result<String, ErrorData> {
    let trimmed = normalize_required_string(field, value)?;
    let valid = DateTime::parse_from_rfc3339(&trimmed).is_ok()
        || NaiveDateTime::parse_from_str(&trimmed, "%Y-%m-%dT%H:%M:%S").is_ok()
        || NaiveDateTime::parse_from_str(&trimmed, "%Y-%m-%d %H:%M:%S").is_ok()
        || NaiveDateTime::parse_from_str(&trimmed, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
        || NaiveDateTime::parse_from_str(&trimmed, "%Y-%m-%d %H:%M:%S%.f").is_ok();

    if !valid {
        return Err(ErrorData::invalid_params(
            format!("{field} must be an ISO-8601 datetime string"),
            None,
        ));
    }

    Ok(trimmed)
}

fn map_to_pairs(map: Option<BTreeMap<String, String>>) -> Option<Vec<(String, String)>> {
    match map {
        Some(entries) if !entries.is_empty() => Some(entries.into_iter().collect::<Vec<_>>()),
        _ => None,
    }
}

fn map_to_hash_map(map: Option<BTreeMap<String, String>>) -> Option<HashMap<String, String>> {
    match map {
        Some(entries) if !entries.is_empty() => {
            Some(entries.into_iter().collect::<HashMap<_, _>>())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xbbg_async::engine::ExtractorType;
    use xbbg_async::services::Service;

    fn params(values: &[(&str, &str)]) -> BTreeMap<String, String> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn empty_request_args(service: &str, operation: Option<&str>) -> RequestArgs {
        RequestArgs {
            service: service.to_string(),
            operation: operation.map(str::to_string),
            request_operation: None,
            request_id: None,
            extractor: None,
            securities: None,
            security: None,
            fields: None,
            overrides: None,
            elements: None,
            kwargs: None,
            start_date: None,
            end_date: None,
            start_datetime: None,
            end_datetime: None,
            request_tz: None,
            output_tz: None,
            event_type: None,
            event_types: None,
            interval: None,
            options: None,
            field_types: None,
            include_security_errors: None,
            return_eids: None,
            validate_fields: None,
            search_spec: None,
            field_ids: None,
            format: None,
        }
    }

    #[test]
    fn mcp_tool_adapters_build_expected_request_params() {
        let bdp = bdp_request_params(BdpArgs {
            tickers: vec!["IBM US Equity".to_string()],
            fields: vec!["PX_LAST".to_string()],
            overrides: Some(params(&[("EQY_FUND_CRNCY", "USD")])),
            options: None,
            field_types: Some(params(&[("PX_LAST", "Float64")])),
            format: Some(ReferenceFormat::LongTyped),
            include_security_errors: true,
            validate_fields: Some(false),
            return_eids: true,
        })
        .unwrap();
        assert_eq!(bdp.service, Service::RefData.to_string());
        assert_eq!(bdp.operation, Operation::ReferenceData.to_string());
        assert_eq!(bdp.extractor, ExtractorType::RefData);
        assert!(!bdp.extractor_set);
        assert_eq!(
            bdp.securities.as_deref(),
            Some(&["IBM US Equity".to_string()][..])
        );
        assert_eq!(bdp.fields.as_deref(), Some(&["PX_LAST".to_string()][..]));
        assert_eq!(bdp.format.as_deref(), Some("long_typed"));
        assert!(bdp.include_security_errors);
        assert_eq!(bdp.validate_fields, Some(false));
        assert!(bdp.return_eids);

        let bdh = bdh_request_params(BdhArgs {
            tickers: vec!["IBM US Equity".to_string()],
            fields: vec!["PX_LAST".to_string()],
            start_date: "2024-01-01".to_string(),
            end_date: "20240131".to_string(),
            overrides: None,
            options: None,
            field_types: None,
            format: Some(HistoricalFormat::SemiLong),
            validate_fields: Some(true),
            return_eids: true,
        })
        .unwrap();
        assert_eq!(bdh.operation, Operation::HistoricalData.to_string());
        assert_eq!(bdh.extractor, ExtractorType::HistData);
        assert_eq!(bdh.start_date.as_deref(), Some("20240101"));
        assert_eq!(bdh.end_date.as_deref(), Some("20240131"));
        assert_eq!(bdh.format.as_deref(), Some("semi_long"));
        assert_eq!(bdh.validate_fields, Some(true));
        assert!(bdh.return_eids);

        let bds = bds_request_params(BdsArgs {
            tickers: vec!["INDU Index".to_string()],
            field: "INDX_MEMBERS".to_string(),
            overrides: None,
            options: None,
            validate_fields: None,
            return_eids: true,
        })
        .unwrap();
        assert_eq!(bds.extractor, ExtractorType::BulkData);
        assert!(bds.extractor_set);
        assert_eq!(
            bds.fields.as_deref(),
            Some(&["INDX_MEMBERS".to_string()][..])
        );
        assert!(bds.return_eids);

        let bdib = bdib_request_params(BdibArgs {
            ticker: "IBM US Equity".to_string(),
            start_datetime: "2024-01-01T09:30:00".to_string(),
            end_datetime: "2024-01-01T10:00:00".to_string(),
            interval: 5,
            event_type: None,
            request_tz: Some("NY".to_string()),
            output_tz: Some("UTC".to_string()),
            options: Some(params(&[("gapFillInitialBar", "true")])),
            return_eids: true,
        })
        .unwrap();
        assert_eq!(bdib.extractor, ExtractorType::IntradayBar);
        assert_eq!(bdib.event_type.as_deref(), Some("TRADE"));
        assert_eq!(bdib.interval, Some(5));
        assert_eq!(bdib.request_tz.as_deref(), Some("NY"));
        assert!(bdib.return_eids);

        let bql = bql_request_params(BqlArgs {
            expression: "get(px_last) for(['IBM US Equity'])".to_string(),
        })
        .unwrap();
        assert_eq!(bql.service, Service::BqlSvc.to_string());
        assert_eq!(bql.extractor, ExtractorType::Bql);
        assert_eq!(
            bql.elements.as_deref(),
            Some(
                &[(
                    "expression".to_string(),
                    "get(px_last) for(['IBM US Equity'])".to_string()
                )][..]
            )
        );

        let bsrch = bsrch_request_params(BsrchArgs {
            domain: "FI".to_string(),
            parameters: Some(params(&[("Ticker", "IBM")])),
        })
        .unwrap();
        assert_eq!(bsrch.service, Service::ExrSvc.to_string());
        assert_eq!(bsrch.operation, Operation::ExcelGetGrid.to_string());
        assert_eq!(bsrch.extractor, ExtractorType::Bsrch);
        assert_eq!(
            bsrch.elements.as_deref(),
            Some(
                &[
                    ("Domain".to_string(), "FI".to_string()),
                    ("Ticker".to_string(), "IBM".to_string())
                ][..]
            )
        );

        let bflds_info = bflds_request_params(BfldsArgs {
            fields: Some(vec!["PX_LAST".to_string()]),
            search_spec: None,
        })
        .unwrap();
        assert_eq!(bflds_info.operation, Operation::FieldInfo.to_string());
        assert_eq!(bflds_info.extractor, ExtractorType::FieldInfo);
        assert_eq!(
            bflds_info.field_ids.as_deref(),
            Some(&["PX_LAST".to_string()][..])
        );

        let bflds_search = bflds_request_params(BfldsArgs {
            fields: None,
            search_spec: Some("price".to_string()),
        })
        .unwrap();
        assert_eq!(bflds_search.operation, Operation::FieldSearch.to_string());
        assert_eq!(bflds_search.extractor, ExtractorType::Generic);
        assert_eq!(bflds_search.search_spec.as_deref(), Some("price"));

        let mut raw = empty_request_args("//blp/refdata", None);
        raw.request_operation = Some(Operation::ReferenceData.to_string());
        raw.request_id = Some("req-123".to_string());
        raw.fields = Some(vec!["PX_LAST".to_string()]);
        raw.kwargs = Some(params(&[("returnEids", "true")]));
        let generic = generic_request_params(raw).unwrap();
        assert_eq!(generic.operation, Operation::RawRequest.to_string());
        assert_eq!(
            generic.request_operation.as_deref(),
            Some("ReferenceDataRequest")
        );
        assert_eq!(generic.request_id.as_deref(), Some("req-123"));
        assert_eq!(
            generic
                .kwargs
                .as_ref()
                .and_then(|values| values.get("returnEids")),
            Some(&"true".to_string())
        );

        let mut with_eids = empty_request_args("//blp/refdata", None);
        with_eids.operation = Some(Operation::ReferenceData.to_string());
        with_eids.securities = Some(vec!["IBM US Equity".to_string()]);
        with_eids.fields = Some(vec!["PX_LAST".to_string()]);
        with_eids.return_eids = Some(true);
        let with_eids = generic_request_params(with_eids).unwrap();
        assert!(with_eids.return_eids, "return_eids should map through");

        let mut tick_with_eids = empty_request_args("//blp/refdata", Some("IntradayTickRequest"));
        tick_with_eids.security = Some("IBM US Equity".to_string());
        tick_with_eids.event_types = Some(vec!["TRADE".to_string()]);
        tick_with_eids.return_eids = Some(true);
        let tick_with_eids = generic_request_params(tick_with_eids).unwrap();
        assert_eq!(
            tick_with_eids.operation,
            Operation::IntradayTick.to_string()
        );
        assert!(tick_with_eids.return_eids);
    }

    #[test]
    fn entitlement_arguments_are_normalized_and_bounded() {
        let (service, eids) = check_entitlements_params(CheckEntitlementsArgs {
            eids: vec![101, 202, 101],
            service: None,
        })
        .unwrap();
        assert_eq!(service, "//blp/refdata");
        assert_eq!(eids, [101, 202]);

        let at_limit = (1..=MAX_ENTITLEMENT_EIDS as i32).collect();
        assert!(check_entitlements_params(CheckEntitlementsArgs {
            eids: at_limit,
            service: None,
        })
        .is_ok());
        for rejected in [
            Vec::new(),
            vec![0],
            vec![-1],
            (1..=(MAX_ENTITLEMENT_EIDS as i32 + 1)).collect(),
        ] {
            assert!(check_entitlements_params(CheckEntitlementsArgs {
                eids: rejected,
                service: None,
            })
            .is_err());
        }
    }

    #[test]
    fn mcp_adapters_reject_invalid_shapes() {
        assert!(bflds_request_params(BfldsArgs {
            fields: Some(vec!["PX_LAST".to_string()]),
            search_spec: Some("price".to_string()),
        })
        .is_err());
    }
}
