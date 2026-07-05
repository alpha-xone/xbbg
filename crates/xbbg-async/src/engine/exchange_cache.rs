use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use xbbg_ext::{ExchangeInfo, ExchangeInfoSource};

/// Days an exchange cache entry stays valid. Exchange metadata (timezones,
/// session hours) drifts rarely; a month bounds staleness without forcing
/// per-run Bloomberg lookups.
pub const EXCHANGE_CACHE_TTL_DAYS: i64 = 30;

fn is_fresh(info: &ExchangeInfo) -> bool {
    info.cached_at.is_some_and(|cached_at| {
        Utc::now().signed_duration_since(cached_at)
            <= chrono::Duration::days(EXCHANGE_CACHE_TTL_DAYS)
    })
}

fn eviction_timestamp(info: &ExchangeInfo) -> DateTime<Utc> {
    info.cached_at.unwrap_or(DateTime::<Utc>::MIN_UTC)
}

const DEFAULT_MAX_EXCHANGE_CACHE_ENTRIES: usize = 16_384;

/// In-memory + disk cache for exchange metadata.
///
/// In-memory reads are lock-free (atomic pointer load) via `ArcSwap`; writers
/// mutate a mutex-protected source-of-truth map and publish cloned snapshots.
/// Disk is loaded lazily at most once via `OnceLock`.
///
/// Entries carry a 30-day TTL ([`EXCHANGE_CACHE_TTL_DAYS`]): expired entries
/// (and legacy entries without a `cached_at` stamp) are served as misses and
/// replaced by the caller's next resolution `put`; expired disk entries are
/// skipped at load. Use [`ExchangeCache::invalidate`] for manual eviction.
pub struct ExchangeCache {
    cache: ArcSwap<HashMap<String, ExchangeInfo>>,
    write_cache: Mutex<HashMap<String, ExchangeInfo>>,
    cache_path: PathBuf,
    loaded: OnceLock<()>,
    max_entries: usize,
}

impl ExchangeCache {
    pub fn new() -> Self {
        Self::with_cache_path(Self::default_cache_path())
    }

    pub fn with_cache_path(path: PathBuf) -> Self {
        Self::with_cache_path_and_max_entries(path, DEFAULT_MAX_EXCHANGE_CACHE_ENTRIES)
    }

    pub fn with_cache_path_and_max_entries(path: PathBuf, max_entries: usize) -> Self {
        Self {
            cache: ArcSwap::from_pointee(HashMap::new()),
            write_cache: Mutex::new(HashMap::new()),
            cache_path: path,
            loaded: OnceLock::new(),
            max_entries,
        }
    }

    pub fn get(&self, ticker: &str) -> Option<ExchangeInfo> {
        self.ensure_loaded();
        let key = ticker.trim();
        if key.is_empty() {
            return None;
        }
        self.cache
            .load()
            .get(key)
            .filter(|info| is_fresh(info))
            .cloned()
            .map(ExchangeInfo::as_cache_hit)
    }

    pub fn put(&self, ticker: &str, info: ExchangeInfo) {
        self.put_many(std::iter::once((ticker, info)));
    }

    pub fn put_many<I, S>(&self, entries: I)
    where
        I: IntoIterator<Item = (S, ExchangeInfo)>,
        S: AsRef<str>,
    {
        self.ensure_loaded();

        let mut write_cache = self.write_cache.lock();
        let mut changed = false;
        for (ticker, mut info) in entries {
            let key = ticker.as_ref().trim();
            if key.is_empty() {
                continue;
            }
            info.cached_at = Some(Utc::now());
            if info.source == ExchangeInfoSource::Fallback {
                info.source = ExchangeInfoSource::Bloomberg;
            }
            write_cache.insert(key.to_string(), info);
            changed = true;
        }

        if changed {
            Self::evict_oldest(&mut write_cache, self.max_entries);
            self.publish_snapshot(&write_cache);
        }
    }

    pub fn invalidate(&self, ticker: Option<&str>) {
        self.ensure_loaded();
        let mut write_cache = self.write_cache.lock();
        match ticker {
            Some(t) if !t.trim().is_empty() => {
                if write_cache.remove(t.trim()).is_some() {
                    self.publish_snapshot(&write_cache);
                }
            }
            _ => {
                if !write_cache.is_empty() {
                    write_cache.clear();
                    self.publish_snapshot(&write_cache);
                } else {
                    self.cache.store(Arc::new(HashMap::new()));
                }
            }
        }
    }

    pub fn save_to_disk(&self) -> Result<(), String> {
        self.ensure_loaded();

        if let Some(parent) = self.cache_path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create cache dir failed: {e}"))?;
        }

        let snapshot = self.cache.load();
        let entries: Vec<&ExchangeInfo> = snapshot.values().collect();

        let file = fs::File::create(&self.cache_path)
            .map_err(|e| format!("create exchange cache file failed: {e}"))?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &entries)
            .map_err(|e| format!("write exchange cache JSON failed: {e}"))
    }

    /// Eagerly load the on-disk cache (idempotent).
    pub fn preload(&self) -> Result<(), String> {
        self.ensure_loaded();
        Ok(())
    }

    fn publish_snapshot(&self, write_cache: &HashMap<String, ExchangeInfo>) {
        self.cache.store(Arc::new(write_cache.clone()));
    }

    fn evict_oldest(cache: &mut HashMap<String, ExchangeInfo>, max_entries: usize) {
        while cache.len() > max_entries {
            let Some(key) = cache
                .iter()
                .min_by_key(|(key, info)| (eviction_timestamp(info), key.as_str()))
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            cache.remove(&key);
        }
    }
    fn ensure_loaded(&self) {
        self.loaded.get_or_init(|| {
            let _ = self.load_from_disk();
        });
    }

    fn load_from_disk(&self) -> Result<(), String> {
        if !self.cache_path.exists() {
            return Ok(());
        }
        let file = match fs::File::open(&self.cache_path) {
            Ok(f) => f,
            Err(e) => {
                xbbg_log::warn!(error = %e, path = %self.cache_path.display(), "failed to open exchange cache");
                return Ok(());
            }
        };
        let reader = BufReader::new(file);
        let entries: Vec<ExchangeInfo> = match serde_json::from_reader(reader) {
            Ok(v) => v,
            Err(e) => {
                xbbg_log::warn!(error = %e, path = %self.cache_path.display(), "failed to parse exchange cache");
                return Ok(());
            }
        };

        let pairs: Vec<(String, ExchangeInfo)> = entries
            .into_iter()
            // Drop entries that already exceeded the TTL (or predate the
            // cached_at stamp) so stale disk state never reaches the map.
            .filter(is_fresh)
            .map(|mut entry| {
                entry.source = ExchangeInfoSource::Cache;
                (entry.ticker.clone(), entry)
            })
            .collect();

        if !pairs.is_empty() {
            let mut write_cache = self.write_cache.lock();
            write_cache.extend(pairs);
            Self::evict_oldest(&mut write_cache, self.max_entries);
            self.publish_snapshot(&write_cache);
        }

        Ok(())
    }

    fn default_cache_path() -> PathBuf {
        #[cfg(windows)]
        let home = std::env::var("USERPROFILE").ok().map(PathBuf::from);
        #[cfg(not(windows))]
        let home = std::env::var("HOME").ok().map(PathBuf::from);

        home.unwrap_or_else(|| PathBuf::from("."))
            .join(".xbbg")
            .join("cache")
            .join("exchanges.json")
    }
}

impl Default for ExchangeCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn info(ticker: &str, cached_at: Option<chrono::DateTime<Utc>>) -> ExchangeInfo {
        ExchangeInfo {
            cached_at,
            source: ExchangeInfoSource::Bloomberg,
            ..ExchangeInfo::fallback(ticker)
        }
    }

    fn temp_cache_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "xbbg-exchange-cache-test-{name}-{}.json",
            std::process::id()
        ))
    }

    #[test]
    fn fresh_put_round_trips() {
        let cache = ExchangeCache::with_cache_path(temp_cache_path("fresh"));
        cache.put("AAPL US Equity", info("AAPL US Equity", None));
        let hit = cache.get("AAPL US Equity").expect("fresh entry should hit");
        assert_eq!(hit.source, ExchangeInfoSource::Cache);
        assert!(hit.cached_at.is_some(), "put must stamp cached_at");
    }

    #[test]
    fn put_many_publishes_one_snapshot_and_reads_reflect_writes() {
        let cache = ExchangeCache::with_cache_path(temp_cache_path("batch"));
        let before = cache.cache.load_full();

        cache.put_many([
            ("AAPL US Equity", info("AAPL US Equity", None)),
            ("MSFT US Equity", info("MSFT US Equity", None)),
        ]);

        let after = cache.cache.load_full();
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(after.len(), 2);
        assert!(cache.get("AAPL US Equity").is_some());
        assert!(cache.get("MSFT US Equity").is_some());

        let after_reads = cache.cache.load_full();
        let _ = cache.get("AAPL US Equity");
        let _ = cache.get("MSFT US Equity");
        assert!(Arc::ptr_eq(&after_reads, &cache.cache.load_full()));
    }

    #[test]
    fn evicts_oldest_entry_when_bound_is_exceeded() {
        let cache = ExchangeCache::with_cache_path_and_max_entries(temp_cache_path("evict"), 1);
        cache.put("OLD US Equity", info("OLD US Equity", None));
        std::thread::sleep(std::time::Duration::from_millis(1));
        cache.put("NEW US Equity", info("NEW US Equity", None));

        assert!(cache.get("OLD US Equity").is_none());
        assert!(cache.get("NEW US Equity").is_some());
        assert_eq!(cache.cache.load().len(), 1);
    }

    #[test]
    fn expired_entry_is_a_miss() {
        let cache = ExchangeCache::with_cache_path(temp_cache_path("expired"));
        cache.put("AAPL US Equity", info("AAPL US Equity", None));
        // Overwrite the stamp with one beyond the TTL in the read snapshot.
        let mut next = (*cache.cache.load_full()).clone();
        if let Some(entry) = next.get_mut("AAPL US Equity") {
            entry.cached_at = Some(Utc::now() - Duration::days(EXCHANGE_CACHE_TTL_DAYS + 1));
        }
        cache.cache.store(Arc::new(next));
        assert!(
            cache.get("AAPL US Equity").is_none(),
            "expired entry must miss"
        );
    }

    #[test]
    fn legacy_entry_without_stamp_is_a_miss() {
        let cache = ExchangeCache::with_cache_path(temp_cache_path("legacy"));
        let mut next = HashMap::new();
        next.insert("IBM US Equity".to_string(), info("IBM US Equity", None));
        cache.cache.store(Arc::new(next));
        assert!(cache.get("IBM US Equity").is_none());
    }

    #[test]
    fn load_from_disk_skips_expired_entries() {
        let path = temp_cache_path("disk");
        let fresh = info("FRESH US Equity", Some(Utc::now()));
        let stale = info(
            "STALE US Equity",
            Some(Utc::now() - Duration::days(EXCHANGE_CACHE_TTL_DAYS + 1)),
        );
        let payload = serde_json::to_string(&vec![&fresh, &stale]).unwrap();
        std::fs::write(&path, payload).unwrap();

        let cache = ExchangeCache::with_cache_path(path.clone());
        assert!(cache.get("FRESH US Equity").is_some());
        assert!(cache.get("STALE US Equity").is_none());
        let _ = std::fs::remove_file(path);
    }
}
