//! CDX (Credit Default Index) ticker resolution utilities.
use std::num::NonZeroU32;

use crate::error::{ExtError, Result};

/// CDX ticker series information.
///
/// This source-compatible view intentionally omits explicit version state.
/// Use [`parse_cdx_ticker`] when the input's `Vn` token matters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdxInfo {
    /// The index name (e.g., "CDX IG CDSI")
    pub index: String,
    /// Series indicator - either "GEN" or "S{n}" (e.g., "S45")
    pub series: String,
    /// Tenor (e.g., "5Y")
    pub tenor: String,
    /// Asset class (e.g., "Corp")
    pub asset: String,
    /// Whether this is a generic ticker
    pub is_generic: bool,
    /// Series number if specific (None for GEN)
    pub series_num: Option<u32>,
}

/// Syntactically parsed CDX ticker, including an optional explicit version.
///
/// `version` is `None` for a versionless Bloomberg alias. That state is
/// unresolved and must never be interpreted as V1 without Bloomberg metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCdxInfo {
    /// The index name (e.g., "CDX IG CDSI")
    pub index: String,
    /// Series indicator - either "GEN" or "S{n}" (e.g., "S45")
    pub series: String,
    /// Explicit version token, when present in the input ticker
    pub version: Option<CdxVersion>,
    /// Tenor (e.g., "5Y")
    pub tenor: String,
    /// Asset class (e.g., "Corp")
    pub asset: String,
    /// Whether this is a generic ticker
    pub is_generic: bool,
    /// Series number if specific (None for GEN)
    pub series_num: Option<u32>,
}

impl From<ParsedCdxInfo> for CdxInfo {
    fn from(parsed: ParsedCdxInfo) -> Self {
        Self {
            index: parsed.index,
            series: parsed.series,
            tenor: parsed.tenor,
            asset: parsed.asset,
            is_generic: parsed.is_generic,
            series_num: parsed.series_num,
        }
    }
}

/// Positive CDX contract version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CdxVersion(NonZeroU32);

impl CdxVersion {
    /// Construct a positive CDX version.
    pub fn new(value: u32) -> Result<Self> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or_else(|| ExtError::InvalidInput("CDX version must be positive".to_string()))
    }

    /// Return the numeric version.
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// Fully resolved specific CDX contract.
///
/// Unlike [`ParsedCdxInfo`], this type cannot represent a generic or
/// versionless ticker. Construct it only after resolving both series and
/// version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCdxInfo {
    index: String,
    series: NonZeroU32,
    version: CdxVersion,
    tenor: String,
    asset: String,
}

impl ResolvedCdxInfo {
    /// Resolve parsed ticker information with Bloomberg series/version metadata.
    pub fn resolve(parsed: ParsedCdxInfo, series: u32, version: u32) -> Result<Self> {
        let series = NonZeroU32::new(series)
            .ok_or_else(|| ExtError::InvalidInput("CDX series must be positive".to_string()))?;
        let version = CdxVersion::new(version)?;

        if !parsed.is_generic && parsed.series_num != Some(series.get()) {
            return Err(ExtError::InvalidInput(format!(
                "resolved CDX series S{} conflicts with parsed {}",
                series.get(),
                parsed.series
            )));
        }
        if parsed
            .version
            .is_some_and(|parsed_version| parsed_version != version)
        {
            return Err(ExtError::InvalidInput(format!(
                "resolved CDX version V{} conflicts with parsed ticker",
                version.get()
            )));
        }

        Ok(Self {
            index: parsed.index,
            series,
            version,
            tenor: parsed.tenor,
            asset: parsed.asset,
        })
    }

    /// Numeric series.
    pub fn series(&self) -> u32 {
        self.series.get()
    }

    /// Numeric version.
    pub fn version(&self) -> u32 {
        self.version.get()
    }

    /// Versionless alias for the preceding series, whose version is unresolved.
    pub fn previous_alias(&self) -> Option<String> {
        (self.series.get() > 1).then(|| {
            format!(
                "{} S{} {} {}",
                self.index,
                self.series.get() - 1,
                self.tenor,
                self.asset
            )
        })
    }
}

/// Parse a CDX ticker into its source-compatible series view.
///
/// # Examples
///
/// ```
/// use xbbg_ext::resolvers::cdx::cdx_series_from_ticker;
///
/// let info = cdx_series_from_ticker("CDX IG CDSI GEN 5Y Corp").unwrap();
/// assert!(info.is_generic);
/// assert_eq!(info.series, "GEN");
/// assert_eq!(info.tenor, "5Y");
///
/// let info2 = cdx_series_from_ticker("CDX IG CDSI S45 5Y Corp").unwrap();
/// assert!(!info2.is_generic);
/// assert_eq!(info2.series_num, Some(45));
/// ```
pub fn cdx_series_from_ticker(ticker: &str) -> Result<CdxInfo> {
    parse_cdx_ticker(ticker).map(Into::into)
}

/// Parse a CDX ticker, preserving an optional explicit `Vn` token.
pub fn parse_cdx_ticker(ticker: &str) -> Result<ParsedCdxInfo> {
    let parts: Vec<&str> = ticker.split_whitespace().collect();

    if parts.len() < 5 {
        return Err(ExtError::InvalidTicker(ticker.to_string()));
    }

    let mut series_idx = None;
    let mut series = String::new();
    let mut is_generic = false;
    let mut series_num = None;

    for (i, part) in parts.iter().enumerate() {
        if *part == "GEN" {
            series_idx = Some(i);
            series = "GEN".to_string();
            is_generic = true;
            break;
        }
        if let Some(num_part) = part.strip_prefix('S') {
            if let Ok(n) = num_part.parse::<u32>() {
                if n == 0 {
                    return Err(ExtError::InvalidTicker(ticker.to_string()));
                }
                series_idx = Some(i);
                series = part.to_string();
                series_num = Some(n);
                break;
            }
        }
    }

    let series_idx = series_idx.ok_or_else(|| ExtError::InvalidTicker(ticker.to_string()))?;
    let index = parts[..series_idx].join(" ");
    let mut next_idx = series_idx + 1;

    let version = if let Some(token) = parts.get(next_idx).filter(|token| token.starts_with('V')) {
        let raw_version = token
            .strip_prefix('V')
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| ExtError::InvalidTicker(ticker.to_string()))?;
        next_idx += 1;
        Some(
            CdxVersion::new(raw_version)
                .map_err(|_| ExtError::InvalidTicker(ticker.to_string()))?,
        )
    } else {
        None
    };

    if next_idx + 2 != parts.len() {
        return Err(ExtError::InvalidTicker(ticker.to_string()));
    }

    Ok(ParsedCdxInfo {
        index,
        series,
        version,
        tenor: parts[next_idx].to_string(),
        asset: parts[next_idx + 1].to_string(),
        is_generic,
        series_num,
    })
}

/// Build a ticker from the source-compatible CDX series view.
pub fn build_cdx_ticker(info: &CdxInfo) -> String {
    format!(
        "{} {} {} {}",
        info.index, info.series, info.tenor, info.asset
    )
}

/// Build the canonical ticker for a fully resolved CDX contract.
///
/// The canonical form always contains an explicit version, including V1.
pub fn build_resolved_cdx_ticker(info: &ResolvedCdxInfo) -> String {
    build_resolved_cdx_ticker_with_options(info, false)
}

/// Format a resolved CDX contract, optionally omitting its version token.
///
/// `versionless` is presentation-only; the resolved value retains its version.
pub fn build_resolved_cdx_ticker_with_options(info: &ResolvedCdxInfo, versionless: bool) -> String {
    if versionless {
        format!(
            "{} S{} {} {}",
            info.index,
            info.series.get(),
            info.tenor,
            info.asset
        )
    } else {
        format!(
            "{} S{} V{} {} {}",
            info.index,
            info.series.get(),
            info.version.get(),
            info.tenor,
            info.asset
        )
    }
}

fn build_cdx_alias(info: &ParsedCdxInfo) -> String {
    match info.version {
        Some(version) => format!(
            "{} {} V{} {} {}",
            info.index,
            info.series,
            version.get(),
            info.tenor,
            info.asset
        ),
        None => format!(
            "{} {} {} {}",
            info.index, info.series, info.tenor, info.asset
        ),
    }
}

/// Get the previous series ticker for a CDX index.
///
/// # Examples
///
/// ```
/// use xbbg_ext::resolvers::cdx::previous_series_ticker;
///
/// let prev = previous_series_ticker("CDX IG CDSI S45 5Y Corp").unwrap();
/// assert!(prev.is_some());
/// assert!(prev.unwrap().contains("S44"));
///
/// // GEN doesn't have a previous series
/// let prev_gen = previous_series_ticker("CDX IG CDSI GEN 5Y Corp").unwrap();
/// assert!(prev_gen.is_none());
/// ```
pub fn previous_series_ticker(ticker: &str) -> Result<Option<String>> {
    let mut info = parse_cdx_ticker(ticker)?;

    if info.is_generic {
        return Ok(None);
    }

    match info.series_num {
        Some(n) if n > 1 => {
            info.series = format!("S{}", n - 1);
            info.series_num = Some(n - 1);
            info.version = None;
            Ok(Some(build_cdx_alias(&info)))
        }
        _ => Ok(None),
    }
}

/// Convert a generic CDX ticker to a specific series.
///
/// # Examples
///
/// ```
/// use xbbg_ext::resolvers::cdx::gen_to_specific;
///
/// let specific = gen_to_specific("CDX IG CDSI GEN 5Y Corp", 45).unwrap();
/// assert!(specific.contains("S45"));
/// assert!(!specific.contains("GEN"));
/// ```
pub fn gen_to_specific(gen_ticker: &str, series: u32) -> Result<String> {
    let mut info = parse_cdx_ticker(gen_ticker)?;
    if !info.is_generic {
        return Err(ExtError::SpecificTicker(gen_ticker.to_string()));
    }
    if series == 0 {
        return Err(ExtError::InvalidInput(
            "CDX series must be positive".to_string(),
        ));
    }

    info.series = format!("S{series}");
    info.series_num = Some(series);
    info.version = None;
    info.is_generic = false;

    Ok(build_cdx_alias(&info))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_generic_cdx() {
        let info = parse_cdx_ticker("CDX IG CDSI GEN 5Y Corp").unwrap();
        assert_eq!(info.index, "CDX IG CDSI");
        assert_eq!(info.series, "GEN");
        assert_eq!(info.version, None);
        assert_eq!(info.tenor, "5Y");
        assert_eq!(info.asset, "Corp");
        assert!(info.is_generic);
        assert_eq!(info.series_num, None);
    }

    #[test]
    fn test_parse_specific_cdx_alias() {
        let info = parse_cdx_ticker("CDX HY CDSI S46 5Y Corp").unwrap();
        assert_eq!(info.series_num, Some(46));
        assert_eq!(info.version, None);
        assert_eq!(info.tenor, "5Y");
        assert!(!info.is_generic);
    }

    #[test]
    fn test_parse_specific_cdx_with_version() {
        let info = parse_cdx_ticker("CDX HY CDSI S46 V2 5Y Corp").unwrap();
        assert_eq!(info.series_num, Some(46));
        assert_eq!(info.version.map(CdxVersion::get), Some(2));
        assert_eq!(info.tenor, "5Y");
    }

    #[test]
    fn test_parse_five_token_legacy_alias() {
        let info = cdx_series_from_ticker("CDX IG S40 5Y Corp").unwrap();
        assert_eq!(info.index, "CDX IG");
        assert_eq!(info.series_num, Some(40));
        assert_eq!(info.tenor, "5Y");
        assert_eq!(
            previous_series_ticker("CDX IG S40 5Y Corp").unwrap(),
            Some("CDX IG S39 5Y Corp".to_string())
        );
    }

    #[test]
    fn test_legacy_info_shape_and_formatter_remain_supported() {
        let info = CdxInfo {
            index: "CDX IG CDSI".to_string(),
            series: "S45".to_string(),
            tenor: "5Y".to_string(),
            asset: "Corp".to_string(),
            is_generic: false,
            series_num: Some(45),
        };
        assert_eq!(build_cdx_ticker(&info), "CDX IG CDSI S45 5Y Corp");
    }

    #[test]
    fn test_resolved_ticker_is_explicit_by_default() {
        let parsed = parse_cdx_ticker("CDX IG CDSI GEN 5Y Corp").unwrap();
        let resolved = ResolvedCdxInfo::resolve(parsed, 46, 1).unwrap();

        assert_eq!(
            build_resolved_cdx_ticker(&resolved),
            "CDX IG CDSI S46 V1 5Y Corp"
        );
        assert_eq!(
            build_resolved_cdx_ticker_with_options(&resolved, true),
            "CDX IG CDSI S46 5Y Corp"
        );
        assert_eq!(resolved.series(), 46);
        assert_eq!(resolved.version(), 1);
    }

    #[test]
    fn test_previous_series_is_an_unresolved_alias() {
        let prev = previous_series_ticker("CDX HY CDSI S46 V2 5Y Corp")
            .unwrap()
            .unwrap();
        assert_eq!(prev, "CDX HY CDSI S45 5Y Corp");

        assert!(previous_series_ticker("CDX IG CDSI S1 V1 5Y Corp")
            .unwrap()
            .is_none());
        assert!(previous_series_ticker("CDX IG CDSI GEN 5Y Corp")
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_resolution_rejects_conflicting_explicit_version() {
        let parsed = parse_cdx_ticker("CDX HY CDSI S46 V2 5Y Corp").unwrap();
        assert!(ResolvedCdxInfo::resolve(parsed, 46, 1).is_err());
    }

    #[test]
    fn test_gen_to_specific_returns_unresolved_alias() {
        let specific = gen_to_specific("CDX IG CDSI GEN 5Y Corp", 45).unwrap();
        assert_eq!(specific, "CDX IG CDSI S45 5Y Corp");
        assert!(gen_to_specific("CDX IG CDSI S44 5Y Corp", 45).is_err());
        assert!(gen_to_specific("CDX IG CDSI GEN 5Y Corp", 0).is_err());
    }

    #[test]
    fn test_invalid_ticker() {
        assert!(parse_cdx_ticker("INVALID").is_err());
        assert!(parse_cdx_ticker("CDX IG").is_err());
        assert!(parse_cdx_ticker("CDX HY CDSI S0 V1 5Y Corp").is_err());
        assert!(parse_cdx_ticker("CDX HY CDSI S46 V0 5Y Corp").is_err());
        assert!(parse_cdx_ticker("CDX HY CDSI S46 VX 5Y Corp").is_err());
        assert!(parse_cdx_ticker("CDX HY CDSI S46 V2 V3 5Y Corp").is_err());
    }
}
