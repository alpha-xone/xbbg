//! Schema cache with in-memory and disk persistence.
//!
//! Caches introspected service schemas to avoid repeated API calls.
//! The cache persists to disk at ~/.xbbg/schema_cache/ for cross-session reuse.
//!
//! In-memory reads are lock-free (atomic pointer load) via `ArcSwap`; writers
//! publish a new snapshot via RCU. This keeps p99.9 reader latency flat even
//! while schemas are being introspected and inserted under burst load.

use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use xbbg_log::{debug, info, warn};

use super::types::ServiceSchema;

type SchemaMap = HashMap<String, Arc<ServiceSchema>>;

/// Schema cache with in-memory and disk persistence.
///
/// Thread-safe cache for service schemas. Schemas are loaded lazily from disk
/// on first access and persisted automatically when updated.
pub struct SchemaCache {
    /// In-memory cache (service_uri -> schema). Lock-free reads.
    cache: ArcSwap<SchemaMap>,
    /// Directory for cached schema files
    cache_dir: PathBuf,
}

impl SchemaCache {
    /// Create a new cache with default directory (~/.xbbg/schema_cache/).
    pub fn new() -> Self {
        Self::with_cache_dir(Self::default_cache_dir())
    }

    /// Create a cache with a custom directory.
    pub fn with_cache_dir(cache_dir: PathBuf) -> Self {
        Self {
            cache: ArcSwap::from_pointee(SchemaMap::new()),
            cache_dir,
        }
    }

    /// Get the default cache directory.
    fn default_cache_dir() -> PathBuf {
        #[cfg(windows)]
        let home = std::env::var("USERPROFILE").ok().map(PathBuf::from);
        #[cfg(not(windows))]
        let home = std::env::var("HOME").ok().map(PathBuf::from);

        home.unwrap_or_else(|| PathBuf::from("."))
            .join(".xbbg")
            .join("schema_cache")
    }

    /// Convert service URI to a safe filename.
    ///
    /// E.g., "//blp/refdata" -> "blp_refdata.json"
    fn service_to_filename(service: &str) -> String {
        let clean = service.trim_start_matches("//").replace(['/', '\\'], "_");
        format!("{}.json", clean)
    }

    /// Get the file path for a service's cached schema.
    fn cache_path(&self, service: &str) -> PathBuf {
        self.cache_dir.join(Self::service_to_filename(service))
    }

    /// Load a schema from disk cache.
    fn load_from_disk(&self, service: &str) -> Option<ServiceSchema> {
        let path = self.cache_path(service);
        if !path.exists() {
            debug!(service, path = %path.display(), "Schema cache file not found");
            return None;
        }

        let file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                warn!(service, error = %e, "Failed to open schema cache file");
                return None;
            }
        };

        let reader = BufReader::new(file);
        match serde_json::from_reader(reader) {
            Ok(schema) => {
                info!(service, path = %path.display(), "Loaded schema from cache");
                Some(schema)
            }
            Err(e) => {
                warn!(service, error = %e, "Failed to parse schema cache file");
                None
            }
        }
    }

    /// Save a schema to disk cache.
    fn save_to_disk(&self, service: &str, schema: &ServiceSchema) -> Result<(), String> {
        // Ensure directory exists
        fs::create_dir_all(&self.cache_dir)
            .map_err(|e| format!("Failed to create cache dir: {e}"))?;

        let path = self.cache_path(service);
        let file =
            fs::File::create(&path).map_err(|e| format!("Failed to create cache file: {e}"))?;

        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, schema)
            .map_err(|e| format!("Failed to write schema JSON: {e}"))?;

        info!(service, path = %path.display(), "Saved schema to cache");
        Ok(())
    }

    /// Publish a new snapshot with `service` mapped to `schema`.
    ///
    /// Uses RCU: clones the current map, inserts, atomically swaps the pointer.
    /// Schema maps stay tiny (normally a handful of services), so this avoids a
    /// write-side mutex while keeping readers lock-free and predictable.
    fn upsert(&self, service: &str, schema: Arc<ServiceSchema>) {
        self.cache.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.insert(service.to_string(), Arc::clone(&schema));
            Arc::new(next)
        });
    }

    /// Publish a new snapshot with `service` removed.
    fn evict(&self, service: &str) {
        self.cache.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.remove(service);
            Arc::new(next)
        });
    }

    /// Get a cached schema from memory only.
    ///
    /// This is safe for request hot paths because it never performs disk I/O.
    pub fn get_memory(&self, service: &str) -> Option<Arc<ServiceSchema>> {
        self.cache.load().get(service).map(Arc::clone)
    }

    /// Get a cached schema.
    ///
    /// First checks in-memory cache (lock-free), then disk cache.
    /// Returns None if not cached anywhere.
    pub fn get(&self, service: &str) -> Option<Arc<ServiceSchema>> {
        if let Some(schema) = self.get_memory(service) {
            return Some(schema);
        }

        // Try loading from disk
        if let Some(schema) = self.load_from_disk(service) {
            return Some(self.insert_memory(service, schema));
        }

        None
    }

    /// Insert a schema into the in-memory cache without disk persistence.
    pub fn insert_memory(&self, service: &str, schema: ServiceSchema) -> Arc<ServiceSchema> {
        let schema = Arc::new(schema);
        self.upsert(service, Arc::clone(&schema));
        schema
    }

    /// Persist a schema to the disk cache without updating memory.
    pub fn persist(&self, service: &str, schema: &ServiceSchema) -> Result<(), String> {
        self.save_to_disk(service, schema)
    }

    /// Return the cache directory used for disk-backed operations.
    pub fn cache_dir(&self) -> PathBuf {
        self.cache_dir.clone()
    }

    /// Insert a schema into the cache.
    ///
    /// Stores in both memory and disk.
    pub fn insert(&self, service: &str, schema: ServiceSchema) -> Arc<ServiceSchema> {
        // Save to disk first (best effort) — outside of any memory synchronization.
        if let Err(e) = self.save_to_disk(service, &schema) {
            warn!(service, error = %e, "Failed to persist schema to disk");
        }

        self.insert_memory(service, schema)
    }

    /// Invalidate a cached schema (removes from memory and disk).
    pub fn invalidate(&self, service: &str) {
        self.evict(service);

        let path = self.cache_path(service);
        if path.exists() {
            if let Err(e) = fs::remove_file(&path) {
                warn!(service, error = %e, "Failed to remove schema cache file");
            } else {
                info!(service, "Invalidated schema cache");
            }
        }
    }

    /// Clear all cached schemas.
    pub fn clear(&self) {
        self.cache.store(Arc::new(SchemaMap::new()));

        if self.cache_dir.exists() {
            if let Ok(entries) = fs::read_dir(&self.cache_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "json") {
                        let _ = fs::remove_file(&path);
                    }
                }
            }
        }

        info!("Cleared schema cache");
    }

    /// List all cached service URIs.
    ///
    /// Returns URIs from both memory and disk.
    pub fn list(&self) -> Vec<String> {
        let snapshot = self.cache.load();
        let mut services: Vec<String> = snapshot.keys().cloned().collect();

        if self.cache_dir.exists() {
            if let Ok(entries) = fs::read_dir(&self.cache_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "json") {
                        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                            let service = format!("//{}", stem.replace('_', "/"));
                            if !services.contains(&service) {
                                services.push(service);
                            }
                        }
                    }
                }
            }
        }

        services.sort();
        services
    }

    /// Check if a schema is cached (memory or disk).
    pub fn contains(&self, service: &str) -> bool {
        if self.cache.load().contains_key(service) {
            return true;
        }
        self.cache_path(service).exists()
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let memory_count = self.cache.load().len();
        let disk_count = if self.cache_dir.exists() {
            fs::read_dir(&self.cache_dir)
                .map(|entries| {
                    entries
                        .flatten()
                        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
                        .count()
                })
                .unwrap_or(0)
        } else {
            0
        };

        CacheStats {
            memory_count,
            disk_count,
            cache_dir: self.cache_dir.clone(),
        }
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of schemas in memory
    pub memory_count: usize,
    /// Number of schemas on disk
    pub disk_count: usize,
    /// Cache directory path
    pub cache_dir: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::OperationSchema;
    use tempfile::TempDir;

    fn create_test_schema(service: &str) -> ServiceSchema {
        ServiceSchema::new(
            service.to_string(),
            "Test Service".to_string(),
            vec![OperationSchema {
                name: "TestRequest".to_string(),
                description: "A test operation".to_string(),
                request: crate::schema::types::ElementInfo::empty(),
                responses: vec![],
            }],
        )
    }

    #[test]
    fn test_service_to_filename() {
        assert_eq!(
            SchemaCache::service_to_filename("//blp/refdata"),
            "blp_refdata.json"
        );
        assert_eq!(
            SchemaCache::service_to_filename("//blp/mktdata"),
            "blp_mktdata.json"
        );
    }

    #[test]
    fn test_memory_cache() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        // Initially empty
        assert!(cache.get("//blp/refdata").is_none());
        assert!(!cache.contains("//blp/refdata"));

        // Insert and retrieve
        let schema = create_test_schema("//blp/refdata");
        cache.insert("//blp/refdata", schema);

        assert!(cache.contains("//blp/refdata"));
        let retrieved = cache.get("//blp/refdata").unwrap();
        assert_eq!(retrieved.service, "//blp/refdata");
    }

    #[test]
    fn test_disk_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Insert with one cache instance
        {
            let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
            cache.insert("//blp/refdata", create_test_schema("//blp/refdata"));
        }

        // Retrieve with a new cache instance (should load from disk)
        {
            let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
            let schema = cache.get("//blp/refdata").unwrap();
            assert_eq!(schema.service, "//blp/refdata");
        }
    }

    #[test]
    fn test_get_memory_does_not_load_disk() {
        let temp_dir = TempDir::new().unwrap();
        {
            let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
            cache.insert("//blp/refdata", create_test_schema("//blp/refdata"));
        }

        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());
        assert!(cache.get_memory("//blp/refdata").is_none());

        let schema = cache.get("//blp/refdata").unwrap();
        assert_eq!(schema.service, "//blp/refdata");
        assert!(cache.get_memory("//blp/refdata").is_some());
    }

    #[test]
    fn test_invalidate() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        cache.insert("//blp/refdata", create_test_schema("//blp/refdata"));
        assert!(cache.contains("//blp/refdata"));

        cache.invalidate("//blp/refdata");
        assert!(!cache.contains("//blp/refdata"));
    }

    #[test]
    fn test_clear() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        cache.insert("//blp/refdata", create_test_schema("//blp/refdata"));
        cache.insert("//blp/mktdata", create_test_schema("//blp/mktdata"));

        let list = cache.list();
        assert_eq!(list.len(), 2);

        cache.clear();
        assert!(cache.list().is_empty());
    }

    #[test]
    fn test_list() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        cache.insert("//blp/refdata", create_test_schema("//blp/refdata"));
        cache.insert("//blp/mktdata", create_test_schema("//blp/mktdata"));

        let list = cache.list();
        assert!(list.contains(&"//blp/refdata".to_string()));
        assert!(list.contains(&"//blp/mktdata".to_string()));
    }

    #[test]
    fn test_stats() {
        let temp_dir = TempDir::new().unwrap();
        let cache = SchemaCache::with_cache_dir(temp_dir.path().to_path_buf());

        let stats = cache.stats();
        assert_eq!(stats.memory_count, 0);
        assert_eq!(stats.disk_count, 0);

        cache.insert("//blp/refdata", create_test_schema("//blp/refdata"));

        let stats = cache.stats();
        assert_eq!(stats.memory_count, 1);
        assert_eq!(stats.disk_count, 1);
    }
}
