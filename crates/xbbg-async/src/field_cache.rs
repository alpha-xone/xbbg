//! Field type cache and resolution.
//!
//! Provides automatic field type resolution using a hierarchy:
//! 1. Manual Override (from Python)
//! 2. Physical Cache (default: `~/.xbbg/field_cache.json`, configurable via `EngineConfig`)
//! 3. API Query (//blp/apiflds service)
//! 4. Defaults (bdp=String, bdh=Float64)
//!
//! In-memory storage uses `ArcSwap` — lock-free reads via atomic pointer load.
//! Writers mutate a mutex-protected source-of-truth map and publish snapshots.

use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use xbbg_log::{debug, info, warn};

/// Bloomberg field type as returned by //blp/apiflds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlpFieldType {
    Boolean,
    Character,
    Date,
    DateOrTime,
    Double,
    Float,
    Int32,
    Int64,
    String,
    Time,
    // Bulk types (arrays)
    BulkFormat,
    // Unknown/other
    Unknown(String),
}

impl BlpFieldType {
    /// Parse from Bloomberg field type string.
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "boolean" | "bool" => BlpFieldType::Boolean,
            "character" | "char" => BlpFieldType::Character,
            "date" => BlpFieldType::Date,
            "dateortime" | "date_or_time" => BlpFieldType::DateOrTime,
            "double" | "real" | "price" => BlpFieldType::Double,
            "float" => BlpFieldType::Float,
            "int32" | "integer" => BlpFieldType::Int32,
            "int64" | "long" => BlpFieldType::Int64,
            "string" | "longcharacter" | "stringorreal" => BlpFieldType::String,
            "time" => BlpFieldType::Time,
            "bulkformat" | "bulk" => BlpFieldType::BulkFormat,
            other => BlpFieldType::Unknown(other.to_string()),
        }
    }

    /// Convert to Arrow DataType.
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            BlpFieldType::Boolean => DataType::Boolean,
            BlpFieldType::Character => DataType::Utf8,
            BlpFieldType::Date => DataType::Date32,
            BlpFieldType::DateOrTime => DataType::Utf8, // Could be either, use string
            BlpFieldType::Double | BlpFieldType::Float => DataType::Float64,
            BlpFieldType::Int32 => DataType::Int32,
            BlpFieldType::Int64 => DataType::Int64,
            BlpFieldType::String => DataType::Utf8,
            BlpFieldType::Time => DataType::Utf8, // Time as string for now
            BlpFieldType::BulkFormat => DataType::Utf8, // Bulk data as JSON string
            BlpFieldType::Unknown(_) => DataType::Utf8,
        }
    }

    /// Convert to Arrow type string (for serialization).
    ///
    /// Matches Python's FTYPE_TO_ARROW mapping exactly.
    pub fn to_arrow_type_str(&self) -> &'static str {
        match self {
            BlpFieldType::Boolean => "bool", // Python uses "bool" not "boolean"
            BlpFieldType::Character => "string",
            BlpFieldType::Date => "date32",
            BlpFieldType::DateOrTime => "string",
            BlpFieldType::Double | BlpFieldType::Float => "float64",
            BlpFieldType::Int32 => "int64", // Python normalizes Int32 → int64
            BlpFieldType::Int64 => "int64",
            BlpFieldType::String => "string",
            BlpFieldType::Time => "timestamp", // Python maps Time → timestamp
            BlpFieldType::BulkFormat => "string",
            BlpFieldType::Unknown(_) => "string",
        }
    }
}

/// Cached field information.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldInfo {
    pub field_id: String,
    pub arrow_type: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub category: String,
}

const DEFAULT_MAX_FIELD_CACHE_ENTRIES: usize = 65_536;

#[derive(Clone, Debug)]
struct FieldCacheEntry {
    info: FieldInfo,
    inserted_at: u64,
}

#[derive(Debug, Default)]
struct FieldCacheState {
    entries: HashMap<String, FieldCacheEntry>,
    next_insert_epoch: u64,
}

/// Field type resolver with caching.
pub struct FieldTypeResolver {
    /// In-memory cache (uppercased field_id -> FieldInfo). Lock-free reads via ArcSwap.
    cache: ArcSwap<HashMap<String, FieldInfo>>,
    /// Write-side source of truth. Writers hold this briefly, then publish one snapshot.
    write_cache: Mutex<FieldCacheState>,
    /// Path to cache file
    cache_path: PathBuf,
    /// Disk load happens at most once (lazy).
    loaded: OnceLock<()>,
    max_entries: usize,
}

impl FieldTypeResolver {
    /// Create a new resolver with default cache path (~/.xbbg/field_cache.json).
    pub fn new() -> Self {
        Self::with_cache_path(Self::default_cache_path())
    }

    /// Create a resolver with a custom cache path.
    pub fn with_cache_path(path: PathBuf) -> Self {
        Self::with_cache_path_and_max_entries(path, DEFAULT_MAX_FIELD_CACHE_ENTRIES)
    }

    /// Create a resolver with a custom cache path and entry bound.
    pub fn with_cache_path_and_max_entries(path: PathBuf, max_entries: usize) -> Self {
        Self {
            cache: ArcSwap::from_pointee(HashMap::new()),
            write_cache: Mutex::new(FieldCacheState::default()),
            cache_path: path,
            loaded: OnceLock::new(),
            max_entries,
        }
    }

    /// Get the default cache path.
    fn default_cache_path() -> PathBuf {
        #[cfg(windows)]
        let home = std::env::var("USERPROFILE").ok().map(PathBuf::from);
        #[cfg(not(windows))]
        let home = std::env::var("HOME").ok().map(PathBuf::from);

        match home {
            Some(h) => h.join(".xbbg").join("field_cache.json"),
            None => {
                warn!(
                    "Home directory not found (USERPROFILE/HOME not set). \
                     Field cache will use current directory. Set field_cache_path in \
                     EngineConfig to specify a persistent location."
                );
                PathBuf::from(".").join(".xbbg").join("field_cache.json")
            }
        }
    }

    /// Ensure cache is loaded from disk (lazy, runs at most once).
    fn ensure_loaded(&self) {
        self.loaded.get_or_init(|| self.load_from_disk());
    }

    fn publish_snapshot(&self, state: &FieldCacheState) {
        let snapshot = state
            .entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.info.clone()))
            .collect();
        self.cache.store(Arc::new(snapshot));
    }

    fn insert_keyed_entries<I>(&self, entries: I)
    where
        I: IntoIterator<Item = (String, FieldInfo)>,
    {
        let mut state = self.write_cache.lock();
        let mut changed = false;

        for (key, info) in entries {
            let key = key.to_uppercase();
            if key.is_empty() {
                continue;
            }

            let inserted_at = match state.entries.get(&key) {
                Some(existing) => existing.inserted_at,
                None => {
                    let epoch = state.next_insert_epoch;
                    state.next_insert_epoch = state.next_insert_epoch.saturating_add(1);
                    epoch
                }
            };

            state
                .entries
                .insert(key, FieldCacheEntry { info, inserted_at });
            changed = true;
        }

        if changed {
            Self::evict_oldest(&mut state, self.max_entries);
            self.publish_snapshot(&state);
        }
    }

    fn evict_oldest(state: &mut FieldCacheState, max_entries: usize) {
        while state.entries.len() > max_entries {
            let Some(key) = state
                .entries
                .iter()
                .min_by_key(|(key, entry)| (entry.inserted_at, key.as_str()))
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            state.entries.remove(&key);
        }
    }

    /// Load cache from JSON file.
    fn load_from_disk(&self) {
        if !self.cache_path.exists() {
            info!(
                path = %self.cache_path.display(),
                "No field cache file found, will build cache from API queries"
            );
            return;
        }

        let file = match fs::File::open(&self.cache_path) {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    error = %e,
                    path = %self.cache_path.display(),
                    "Cannot read field cache file. Field types will be re-queried from \
                     Bloomberg on each session. Check file permissions or set \
                     field_cache_path in EngineConfig."
                );
                return;
            }
        };
        let reader = BufReader::new(file);
        let entries: Vec<FieldInfo> = match serde_json::from_reader(reader) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    error = %e,
                    path = %self.cache_path.display(),
                    "Field cache file is corrupt, ignoring. Will rebuild from API queries."
                );
                return;
            }
        };

        let pairs: Vec<(String, FieldInfo)> = entries
            .into_iter()
            .map(|info| (info.field_id.to_uppercase(), info))
            .collect();

        if !pairs.is_empty() {
            self.insert_keyed_entries(pairs);
        }

        info!(count = self.cache.load().len(), path = %self.cache_path.display(), "Loaded field cache");
    }

    /// Save cache to JSON file.
    pub fn save_to_disk(&self) -> Result<(), String> {
        self.ensure_loaded();

        if let Some(parent) = self.cache_path.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                return Err(format!(
                    "Cannot create field cache directory '{}': {e}. \
                     Field types will not persist between sessions. \
                     Set field_cache_path in EngineConfig to a writable location.",
                    parent.display()
                ));
            }
        }

        // Snapshot via ArcSwap load — no locks held during serialization.
        let snapshot = self.cache.load();
        if snapshot.is_empty() {
            debug!("Cache is empty, nothing to save");
            return Ok(());
        }

        let entries: Vec<FieldInfo> = snapshot.values().cloned().collect();

        let file = fs::File::create(&self.cache_path).map_err(|e| {
            format!(
                "Cannot write field cache to '{}': {e}. \
                 Field types will not persist between sessions.",
                self.cache_path.display()
            )
        })?;
        let writer = BufWriter::new(file);

        serde_json::to_writer_pretty(writer, &entries)
            .map_err(|e| format!("Failed to serialize field cache: {e}"))?;

        info!(count = entries.len(), path = %self.cache_path.display(), "Saved field cache");
        Ok(())
    }

    /// Preload the cache from disk now instead of on first async resolution.
    pub fn preload(&self) {
        self.ensure_loaded();
    }

    /// Get field info from cache.
    pub fn get(&self, field_id: &str) -> Option<FieldInfo> {
        self.ensure_loaded();
        self.cache.load().get(&field_id.to_uppercase()).cloned()
    }

    /// Get Arrow type string for a field.
    pub fn get_arrow_type(&self, field_id: &str) -> Option<String> {
        self.get(field_id).map(|info| info.arrow_type)
    }

    /// Insert field info into cache.
    pub fn insert(&self, info: FieldInfo) {
        self.insert_many(std::iter::once(info));
    }

    /// Insert multiple field infos and publish a single snapshot.
    pub fn insert_many<I>(&self, infos: I)
    where
        I: IntoIterator<Item = FieldInfo>,
    {
        self.ensure_loaded();
        self.insert_keyed_entries(
            infos
                .into_iter()
                .map(|info| (info.field_id.to_uppercase(), info)),
        );
    }

    /// Extend the cache with multiple (uppercase_key, FieldInfo) pairs in one snapshot.
    ///
    /// Exposed only for benchmarks so callers can measure the batched-publish cost directly.
    #[cfg(feature = "bench-internals")]
    pub fn cache_rcu_extend(&self, entries: Vec<(String, FieldInfo)>) {
        self.ensure_loaded();
        self.insert_keyed_entries(entries);
    }

    /// Insert multiple field infos from a FieldInfoRequest response.
    ///
    /// Expects columns from the FieldInfo extractor:
    /// - field: Field mnemonic (e.g., "PX_LAST")
    /// - type: Arrow type string (e.g., "float64")
    /// - description: Field description
    /// - category: Category name
    pub fn insert_from_response(&self, batch: &RecordBatch) {
        self.ensure_loaded();

        let field_col = batch
            .column_by_name("field")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let type_col = batch
            .column_by_name("type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let desc_col = batch
            .column_by_name("description")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let cat_col = batch
            .column_by_name("category")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let (Some(fields), Some(types)) = (field_col, type_col) else {
            warn!("FieldInfo batch missing required columns (field, type)");
            return;
        };

        // Collect all entries first, then publish a single snapshot — avoids O(n²) clone cost.
        let mut entries: Vec<FieldInfo> = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            if fields.is_null(i) || types.is_null(i) {
                continue;
            }
            let field_id = fields.value(i).to_uppercase();
            let arrow_type = types.value(i).to_string();
            let description = desc_col
                .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                .unwrap_or("")
                .to_string();
            let category = cat_col
                .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                .unwrap_or("")
                .to_string();

            debug!(field = %field_id, arrow_type = %arrow_type, "Cached field type");
            entries.push(FieldInfo {
                field_id,
                arrow_type,
                description,
                category,
            });
        }

        if !entries.is_empty() {
            self.insert_many(entries);
        }
    }

    /// Resolve field types for a list of fields.
    ///
    /// Returns a HashMap of field_id -> arrow_type_string.
    /// Uses the hierarchy: manual_overrides -> cache -> default.
    pub fn resolve_types(
        &self,
        fields: &[String],
        manual_overrides: Option<&HashMap<String, String>>,
        default_type: &str,
    ) -> HashMap<String, String> {
        self.ensure_loaded();

        let snapshot = self.cache.load();
        let mut result = HashMap::new();

        for field in fields {
            let field_upper = field.to_uppercase();

            // 1. Check manual overrides
            if let Some(overrides) = manual_overrides {
                if let Some(t) = overrides.get(field).or_else(|| overrides.get(&field_upper)) {
                    result.insert(field.clone(), t.clone());
                    continue;
                }
            }

            // 2. Check cache
            if let Some(info) = snapshot.get(&field_upper) {
                result.insert(field.clone(), info.arrow_type.clone());
                continue;
            }

            // 3. Use default
            result.insert(field.clone(), default_type.to_string());
        }

        result
    }

    /// Resolve only manual overrides and already-cached field types.
    ///
    /// Unlike [`Self::resolve_types`], this does not apply defaults. It lets
    /// request paths opportunistically use metadata that has already been
    /// resolved without changing behavior for unknown fields or issuing extra
    /// Bloomberg metadata requests.
    pub fn resolve_cached_types(
        &self,
        fields: &[String],
        manual_overrides: Option<&HashMap<String, String>>,
    ) -> HashMap<String, String> {
        self.ensure_loaded();
        let mut result = manual_overrides.cloned().unwrap_or_default();

        let snapshot = self.cache.load();
        for field in fields {
            let field_upper = field.to_uppercase();
            if result.contains_key(field) || result.contains_key(&field_upper) {
                continue;
            }

            if let Some(info) = snapshot.get(&field_upper) {
                result.insert(field.clone(), info.arrow_type.clone());
            }
        }

        result
    }

    /// Get list of fields that are not in cache.
    pub fn get_uncached_fields(&self, fields: &[String]) -> Vec<String> {
        self.ensure_loaded();

        let snapshot = self.cache.load();
        fields
            .iter()
            .filter(|f| !snapshot.contains_key(&f.to_uppercase()))
            .cloned()
            .collect()
    }

    /// Clear all cached field info.
    pub fn clear(&self) {
        let mut state = self.write_cache.lock();
        state.entries.clear();
        state.next_insert_epoch = 0;
        self.cache.store(Arc::new(HashMap::new()));
        info!("Cleared field cache");
    }

    /// Get cache statistics.
    pub fn stats(&self) -> (usize, PathBuf) {
        self.ensure_loaded();
        (self.cache.load().len(), self.cache_path.clone())
    }
}

impl Default for FieldTypeResolver {
    fn default() -> Self {
        Self::new()
    }
}

/// Global field type resolver (initialized on first access or via `init_global_resolver`).
static GLOBAL_RESOLVER: std::sync::OnceLock<Arc<FieldTypeResolver>> = std::sync::OnceLock::new();

/// Initialize the global field type resolver with an optional custom cache path.
///
/// If already initialized (e.g., from a previous `Engine::start()` call), this is a no-op
/// and the existing resolver is returned. The cache path cannot be changed after initialization.
pub fn init_global_resolver(cache_path: Option<PathBuf>) -> Arc<FieldTypeResolver> {
    GLOBAL_RESOLVER
        .get_or_init(|| {
            let resolver = match cache_path {
                Some(ref path) => {
                    info!(path = %path.display(), "Using custom field cache path");
                    FieldTypeResolver::with_cache_path(path.clone())
                }
                None => FieldTypeResolver::new(),
            };
            Arc::new(resolver)
        })
        .clone()
}

/// Get the global field type resolver.
///
/// If not yet initialized, creates one with the default cache path.
pub fn global_resolver() -> Arc<FieldTypeResolver> {
    init_global_resolver(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field_info(field_id: &str, arrow_type: &str) -> FieldInfo {
        FieldInfo {
            field_id: field_id.to_string(),
            arrow_type: arrow_type.to_string(),
            description: String::new(),
            category: String::new(),
        }
    }

    #[test]
    fn test_blp_field_type_parsing() {
        assert_eq!(BlpFieldType::parse("Double"), BlpFieldType::Double);
        assert_eq!(BlpFieldType::parse("REAL"), BlpFieldType::Double);
        assert_eq!(BlpFieldType::parse("Price"), BlpFieldType::Double);
        assert_eq!(BlpFieldType::parse("String"), BlpFieldType::String);
        assert_eq!(BlpFieldType::parse("Boolean"), BlpFieldType::Boolean);
        assert_eq!(BlpFieldType::parse("Date"), BlpFieldType::Date);
        assert_eq!(BlpFieldType::parse("Int64"), BlpFieldType::Int64);
    }

    #[test]
    fn test_arrow_type_conversion() {
        assert_eq!(BlpFieldType::Double.to_arrow_type(), DataType::Float64);
        assert_eq!(BlpFieldType::String.to_arrow_type(), DataType::Utf8);
        assert_eq!(BlpFieldType::Boolean.to_arrow_type(), DataType::Boolean);
        assert_eq!(BlpFieldType::Date.to_arrow_type(), DataType::Date32);
        assert_eq!(BlpFieldType::Int64.to_arrow_type(), DataType::Int64);
    }

    #[test]
    fn test_resolve_with_overrides() {
        let resolver = FieldTypeResolver::new();

        let fields = vec!["PX_LAST".to_string(), "VOLUME".to_string()];
        let mut overrides = HashMap::new();
        overrides.insert("VOLUME".to_string(), "int64".to_string());

        let resolved = resolver.resolve_types(&fields, Some(&overrides), "float64");

        assert_eq!(resolved.get("PX_LAST"), Some(&"float64".to_string()));
        assert_eq!(resolved.get("VOLUME"), Some(&"int64".to_string()));
    }

    #[test]
    fn resolve_cached_types_preserves_overrides_and_skips_unknowns() {
        let dir = tempfile::tempdir().unwrap();
        let resolver = FieldTypeResolver::with_cache_path(dir.path().join("field_cache.json"));
        resolver.insert(FieldInfo {
            field_id: "PX_LAST".to_string(),
            arrow_type: "float64".to_string(),
            description: String::new(),
            category: String::new(),
        });

        let fields = vec![
            "PX_LAST".to_string(),
            "VOLUME".to_string(),
            "NAME".to_string(),
        ];
        let mut overrides = HashMap::new();
        overrides.insert("VOLUME".to_string(), "int64".to_string());

        let resolved = resolver.resolve_cached_types(&fields, Some(&overrides));

        assert_eq!(resolved.get("PX_LAST"), Some(&"float64".to_string()));
        assert_eq!(resolved.get("VOLUME"), Some(&"int64".to_string()));
        assert!(!resolved.contains_key("NAME"));
    }

    #[test]
    fn insert_many_publishes_one_snapshot_and_reads_reflect_writes() {
        let dir = tempfile::tempdir().unwrap();
        let resolver = FieldTypeResolver::with_cache_path(dir.path().join("field_cache.json"));
        let before = resolver.cache.load_full();

        resolver.insert_many([
            field_info("PX_LAST", "float64"),
            field_info("VOLUME", "int64"),
        ]);

        let after = resolver.cache.load_full();
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(after.len(), 2);
        assert_eq!(resolver.get_arrow_type("PX_LAST").as_deref(), Some("float64"));
        assert_eq!(resolver.get_arrow_type("VOLUME").as_deref(), Some("int64"));

        let after_reads = resolver.cache.load_full();
        let _ = resolver.get("PX_LAST");
        let _ = resolver.get("VOLUME");
        assert!(Arc::ptr_eq(&after_reads, &resolver.cache.load_full()));
    }

    #[test]
    fn evicts_oldest_inserted_field_when_bound_is_exceeded() {
        let dir = tempfile::tempdir().unwrap();
        let resolver =
            FieldTypeResolver::with_cache_path_and_max_entries(dir.path().join("field_cache.json"), 2);

        resolver.insert_many([
            field_info("FIRST", "float64"),
            field_info("SECOND", "int64"),
            field_info("THIRD", "string"),
        ]);

        assert!(resolver.get("FIRST").is_none());
        assert_eq!(resolver.get_arrow_type("SECOND").as_deref(), Some("int64"));
        assert_eq!(resolver.get_arrow_type("THIRD").as_deref(), Some("string"));
        assert_eq!(resolver.cache.load().len(), 2);
    }
}
