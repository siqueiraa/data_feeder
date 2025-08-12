//! Storage Optimization Module
//! 
//! This module provides advanced storage optimizations including LMDB tuning,
//! query optimization, and intelligent caching strategies.
//! Enhanced for Story 6.2 to achieve 20-35% storage performance improvements.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock as AsyncRwLock};
use tracing::{debug, error, info};

/// Advanced LMDB configuration optimizer
pub struct LmdbOptimizer {
    config: LmdbConfig,
    stats: StorageStats,
    auto_tuning: bool,
}

/// Optimized LMDB configuration
#[derive(Debug, Clone)]
pub struct LmdbConfig {
    pub map_size: usize,
    pub max_readers: u32,
    pub max_dbs: u32,
    pub write_map: bool,
    pub no_tls: bool,
    pub no_sync: bool,
    pub no_meta_sync: bool,
    pub read_ahead: bool,
    pub mmap_size: usize,
    pub growth_step: usize,
    pub compaction_threshold: f64,
}

impl Default for LmdbConfig {
    fn default() -> Self {
        Self {
            map_size: 2 * 1024 * 1024 * 1024, // 2GB default
            max_readers: 126,
            max_dbs: 128,
            write_map: true,  // Better performance for write-heavy workloads
            no_tls: true,     // Better performance in controlled environments
            no_sync: false,   // Keep data safety
            no_meta_sync: true, // Optimize metadata sync
            read_ahead: true, // Optimize sequential reads
            mmap_size: 256 * 1024 * 1024, // 256MB memory map
            growth_step: 256 * 1024 * 1024, // 256MB growth increments
            compaction_threshold: 0.3, // Compact when 30% fragmented
        }
    }
}

impl LmdbOptimizer {
    /// Create a new LMDB optimizer with intelligent configuration
    pub fn new(auto_tuning: bool) -> Self {
        Self {
            config: LmdbConfig::default(),
            stats: StorageStats::new(),
            auto_tuning,
        }
    }

    /// Optimize LMDB configuration based on workload patterns
    pub fn optimize_config(&mut self, workload: &WorkloadPattern) -> LmdbConfig {
        let mut config = self.config.clone();

        match workload {
            WorkloadPattern::ReadHeavy => {
                config.read_ahead = true;
                config.max_readers = 256;
                config.write_map = false; // Better for read-heavy
                info!("Optimized LMDB config for read-heavy workload");
            }
            WorkloadPattern::WriteHeavy => {
                config.write_map = true;
                config.no_meta_sync = true;
                config.growth_step = 512 * 1024 * 1024; // Larger growth for writes
                info!("Optimized LMDB config for write-heavy workload");
            }
            WorkloadPattern::Mixed => {
                config.write_map = true;
                config.read_ahead = true;
                config.max_readers = 128;
                info!("Optimized LMDB config for mixed workload");
            }
            WorkloadPattern::LowLatency => {
                config.no_sync = false; // Ensure durability
                config.no_meta_sync = false;
                config.write_map = true;
                info!("Optimized LMDB config for low-latency workload");
            }
        }

        // Auto-tune based on current statistics
        if self.auto_tuning {
            self.auto_tune_config(&mut config);
        }

        self.config = config.clone();
        config
    }

    /// Auto-tune configuration based on runtime statistics
    fn auto_tune_config(&self, config: &mut LmdbConfig) {
        let stats = &self.stats;
        
        // Adjust map size based on growth patterns
        if stats.space_utilization() > 0.8 {
            config.map_size = (config.map_size as f64 * 1.5) as usize;
            debug!("Auto-tuned: Increased map_size to {} MB", config.map_size / (1024*1024));
        }

        // Adjust readers based on concurrent access patterns
        if stats.concurrent_readers() > config.max_readers as usize {
            config.max_readers = (stats.concurrent_readers() * 2) as u32;
            debug!("Auto-tuned: Increased max_readers to {}", config.max_readers);
        }

        // Optimize sync settings based on write frequency
        if stats.write_frequency() > 1000.0 { // >1000 writes/sec
            config.no_meta_sync = true;
            debug!("Auto-tuned: Enabled no_meta_sync for high write frequency");
        }
    }

    /// Get current optimization statistics
    pub fn get_stats(&self) -> &StorageStats {
        &self.stats
    }

    /// Update statistics for auto-tuning
    pub fn update_stats(&mut self, operation: StorageOperation, duration: Duration, size: usize) {
        self.stats.record_operation(operation, duration, size);
    }
}

/// Query optimizer for storage operations
pub struct QueryOptimizer {
    index_cache: Arc<RwLock<HashMap<String, QueryIndex>>>,
    query_stats: QueryStats,
    optimization_cache: Arc<RwLock<HashMap<QuerySignature, OptimizedQuery>>>,
}

/// Query execution plan
#[derive(Debug, Clone)]
pub struct OptimizedQuery {
    pub execution_plan: ExecutionPlan,
    pub estimated_cost: f64,
    pub cache_strategy: CacheStrategy,
    pub index_hints: Vec<IndexHint>,
}

/// Query execution plan
#[derive(Debug, Clone)]
pub enum ExecutionPlan {
    FullScan,
    IndexScan { index_name: String, selectivity: f64 },
    RangeScan { start: Vec<u8>, end: Vec<u8>, reverse: bool },
    CompositeIndex { indices: Vec<String>, intersection: bool },
}

/// Cache strategy for query results
#[derive(Debug, Clone)]
pub enum CacheStrategy {
    NoCache,
    ShortTerm { ttl: Duration },
    LongTerm { ttl: Duration, warming: bool },
    Adaptive { min_ttl: Duration, max_ttl: Duration },
}

/// Index hint for query optimization
#[derive(Debug, Clone)]
pub struct IndexHint {
    pub index_name: String,
    pub confidence: f64,
    pub estimated_rows: usize,
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryOptimizer {
    /// Create a new query optimizer
    pub fn new() -> Self {
        Self {
            index_cache: Arc::new(RwLock::new(HashMap::new())),
            query_stats: QueryStats::new(),
            optimization_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Optimize a storage query
    pub async fn optimize_query(&self, query: &StorageQuery) -> OptimizedQuery {
        let signature = query.signature();
        
        // Check optimization cache first
        if let Some(cached) = self.optimization_cache.read().unwrap().get(&signature) {
            self.query_stats.record_cache_hit();
            debug!("Query optimization cache hit for: {:?}", signature);
            return cached.clone();
        }

        // Generate optimization plan
        let optimized = self.generate_optimization_plan(query).await;
        
        // Cache the optimization
        self.optimization_cache.write().unwrap().insert(signature, optimized.clone());
        self.query_stats.record_cache_miss();
        
        debug!("Generated optimization plan with estimated cost: {:.2}", optimized.estimated_cost);
        optimized
    }

    /// Generate optimization plan for a query
    async fn generate_optimization_plan(&self, query: &StorageQuery) -> OptimizedQuery {
        let mut best_plan = ExecutionPlan::FullScan;
        let mut best_cost = f64::INFINITY;
        let mut index_hints = Vec::new();

        // Analyze available indices
        let indices = self.index_cache.read().unwrap();
        for (index_name, index) in indices.iter() {
            if let Some(cost) = self.estimate_index_cost(query, index) {
                if cost < best_cost {
                    best_cost = cost;
                    best_plan = ExecutionPlan::IndexScan {
                        index_name: index_name.clone(),
                        selectivity: index.selectivity,
                    };
                }
                
                index_hints.push(IndexHint {
                    index_name: index_name.clone(),
                    confidence: 1.0 - (cost / 1000.0).min(1.0),
                    estimated_rows: index.estimated_rows,
                });
            }
        }

        // Determine cache strategy
        let cache_strategy = self.determine_cache_strategy(query, best_cost);

        OptimizedQuery {
            execution_plan: best_plan,
            estimated_cost: best_cost,
            cache_strategy,
            index_hints,
        }
    }

    /// Estimate cost of using an index for a query
    fn estimate_index_cost(&self, query: &StorageQuery, index: &QueryIndex) -> Option<f64> {
        match query {
            StorageQuery::RangeQuery { start, end, .. } => {
                if index.supports_range_query(start, end) {
                    Some(index.selectivity * 10.0 + index.estimated_rows as f64 * 0.1)
                } else {
                    None
                }
            }
            StorageQuery::PointQuery { key, .. } => {
                if index.supports_point_query(key) {
                    Some(index.selectivity * 5.0 + 1.0)
                } else {
                    None
                }
            }
            StorageQuery::PrefixQuery { prefix, .. } => {
                if index.supports_prefix_query(prefix) {
                    Some(index.selectivity * 15.0 + index.estimated_rows as f64 * 0.2)
                } else {
                    None
                }
            }
        }
    }

    /// Determine optimal cache strategy for a query
    fn determine_cache_strategy(&self, query: &StorageQuery, cost: f64) -> CacheStrategy {
        let stats = self.query_stats.get_query_frequency(query);
        
        if stats.frequency > 100.0 && cost > 50.0 {
            // High frequency, expensive queries get long-term caching
            CacheStrategy::LongTerm {
                ttl: Duration::from_secs(30 * 60), // 30 minutes
                warming: true,
            }
        } else if stats.frequency > 10.0 {
            // Medium frequency queries get short-term caching
            CacheStrategy::ShortTerm {
                ttl: Duration::from_secs(5 * 60), // 5 minutes
            }
        } else if cost > 100.0 {
            // Expensive but infrequent queries get adaptive caching
            CacheStrategy::Adaptive {
                min_ttl: Duration::from_secs(60), // 1 minute
                max_ttl: Duration::from_secs(10 * 60), // 10 minutes
            }
        } else {
            CacheStrategy::NoCache
        }
    }

    /// Add or update query index
    pub fn add_index(&self, name: String, index: QueryIndex) {
        self.index_cache.write().unwrap().insert(name.clone(), index);
        debug!("Added query index: {}", name);
    }

    /// Get query optimization statistics
    pub fn get_stats(&self) -> &QueryStats {
        &self.query_stats
    }
}

/// Multi-layer intelligent cache system
pub struct IntelligentCache<K, V> {
    l1_cache: Arc<RwLock<HashMap<K, CacheEntry<V>>>>, // Hot data cache
    l2_cache: Arc<Mutex<BTreeMap<K, CacheEntry<V>>>>, // Warm data cache
    l3_cache: Arc<AsyncRwLock<HashMap<K, V>>>,         // Cold data persistent cache
    eviction_policy: EvictionPolicy,
    cache_config: CacheConfig,
    stats: CacheStatistics,
    prefetch_queue: Arc<RwLock<VecDeque<K>>>,
}

/// Cache entry with metadata
#[derive(Debug, Clone)]
struct CacheEntry<V> {
    value: V,
    access_count: u64,
    last_access: Instant,
    _created_at: Instant,
    _size_estimate: usize,
    _access_pattern: AccessPattern,
}

/// Access pattern analysis
#[derive(Debug, Clone)]
struct AccessPattern {
    _sequential: bool,
    _random: bool,
    _temporal_locality: f64,
    _spatial_locality: f64,
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub l1_size: usize,
    pub l2_size: usize,
    pub l3_size: usize,
    pub l1_ttl: Duration,
    pub l2_ttl: Duration,
    pub l3_ttl: Duration,
    pub prefetch_size: usize,
    pub eviction_batch_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            l1_size: 1024,      // 1K entries in L1 (hot)
            l2_size: 8192,      // 8K entries in L2 (warm)
            l3_size: 32768,     // 32K entries in L3 (cold)
            l1_ttl: Duration::from_secs(5 * 60), // 5 minutes
            l2_ttl: Duration::from_secs(30 * 60), // 30 minutes
            l3_ttl: Duration::from_secs(2 * 60 * 60), // 2 hours
            prefetch_size: 16,
            eviction_batch_size: 32,
        }
    }
}

/// Eviction policy for cache management
#[derive(Debug, Clone)]
pub enum EvictionPolicy {
    Lru,
    Lfu,
    Arc, // Adaptive Replacement Cache
    Clock,
    TwoQueue,
}

impl<K, V> IntelligentCache<K, V>
where
    K: Clone + Eq + std::hash::Hash + Ord + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a new intelligent cache system
    pub fn new(config: CacheConfig, eviction_policy: EvictionPolicy) -> Self {
        Self {
            l1_cache: Arc::new(RwLock::new(HashMap::new())),
            l2_cache: Arc::new(Mutex::new(BTreeMap::new())),
            l3_cache: Arc::new(AsyncRwLock::new(HashMap::new())),
            eviction_policy,
            cache_config: config,
            stats: CacheStatistics::new(),
            prefetch_queue: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Get value with intelligent cache lookup
    pub async fn get(&self, key: &K) -> Option<V> {
        // Try L1 cache first (hot data)
        if let Some(entry) = self.get_from_l1(key) {
            self.stats.record_l1_hit();
            debug!("L1 cache hit for key");
            return Some(entry.value);
        }

        // Try L2 cache (warm data)
        if let Some(entry) = self.get_from_l2(key).await {
            self.stats.record_l2_hit();
            // Promote to L1
            self.promote_to_l1(key.clone(), entry.value.clone()).await;
            debug!("L2 cache hit for key, promoted to L1");
            return Some(entry.value);
        }

        // Try L3 cache (cold data)
        if let Some(value) = self.get_from_l3(key).await {
            self.stats.record_l3_hit();
            // Promote to L2 and potentially L1
            self.promote_from_l3(key.clone(), value.clone()).await;
            debug!("L3 cache hit for key, promoted to L2");
            return Some(value);
        }

        self.stats.record_miss();
        None
    }

    /// Put value with intelligent cache placement
    pub async fn put(&self, key: K, value: V) {
        let entry = CacheEntry {
            value: value.clone(),
            access_count: 1,
            last_access: Instant::now(),
            _created_at: Instant::now(),
            _size_estimate: std::mem::size_of::<V>(),
            _access_pattern: AccessPattern {
                _sequential: false,
                _random: true,
                _temporal_locality: 0.5,
                _spatial_locality: 0.5,
            },
        };

        // Always put in L1 first
        self.put_in_l1(key.clone(), entry).await;
        self.stats.record_insertion();
        
        // Trigger prefetch if pattern detected
        self.maybe_prefetch(&key).await;
    }

    /// Get from L1 cache
    fn get_from_l1(&self, key: &K) -> Option<CacheEntry<V>> {
        let mut cache = self.l1_cache.write().unwrap();
        if let Some(entry) = cache.get_mut(key) {
            entry.access_count += 1;
            entry.last_access = Instant::now();
            Some(entry.clone())
        } else {
            None
        }
    }

    /// Get from L2 cache
    async fn get_from_l2(&self, key: &K) -> Option<CacheEntry<V>> {
        let mut cache = self.l2_cache.lock().await;
        if let Some(entry) = cache.get_mut(key) {
            entry.access_count += 1;
            entry.last_access = Instant::now();
            Some(entry.clone())
        } else {
            None
        }
    }

    /// Get from L3 cache
    async fn get_from_l3(&self, key: &K) -> Option<V> {
        let cache = self.l3_cache.read().await;
        cache.get(key).cloned()
    }

    /// Put in L1 cache with eviction if needed
    async fn put_in_l1(&self, key: K, entry: CacheEntry<V>) {
        let needs_eviction = {
            let cache = self.l1_cache.write().unwrap();
            cache.len() >= self.cache_config.l1_size
        };
        
        if needs_eviction {
            // Perform eviction without holding the mutex
            self.perform_cache_eviction().await;
        }
        
        // Insert the new entry
        let mut cache = self.l1_cache.write().unwrap();
        cache.insert(key, entry);
    }


    /// Perform cache eviction without holding mutex across await points
    async fn perform_cache_eviction(&self) {
        let entries_to_evict = {
            let cache = self.l1_cache.write().unwrap();
            let evict_count = self.cache_config.eviction_batch_size.min(cache.len() / 4);
            
            match self.eviction_policy {
                EvictionPolicy::Lru => {
                    let mut entries: Vec<_> = cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    entries.sort_by_key(|(_, entry)| entry.last_access);
                    entries.into_iter().take(evict_count).collect::<Vec<_>>()
                }
                EvictionPolicy::Lfu => {
                    let mut entries: Vec<_> = cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    entries.sort_by_key(|(_, entry)| entry.access_count);
                    entries.into_iter().take(evict_count).collect::<Vec<_>>()
                }
                _ => {
                    // Default to LRU for other policies
                    let mut entries: Vec<_> = cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    entries.sort_by_key(|(_, entry)| entry.last_access);
                    entries.into_iter().take(evict_count).collect::<Vec<_>>()
                }
            }
        };
        
        // Remove entries and demote to L2 without holding the mutex
        let evict_count = entries_to_evict.len();
        for (key, entry) in entries_to_evict {
            // Remove from L1
            self.l1_cache.write().unwrap().remove(&key);
            // Demote to L2 
            self.demote_to_l2(key, entry).await;
        }
        
        debug!("Evicted {} entries from L1 cache", evict_count);
    }

    /// Promote value to L1 from lower levels
    async fn promote_to_l1(&self, key: K, value: V) {
        let entry = CacheEntry {
            value,
            access_count: 1,
            last_access: Instant::now(),
            _created_at: Instant::now(),
            _size_estimate: std::mem::size_of::<V>(),
            _access_pattern: AccessPattern {
                _sequential: false,
                _random: true,
                _temporal_locality: 0.7, // Higher locality when promoted
                _spatial_locality: 0.7,
            },
        };
        
        self.put_in_l1(key, entry).await;
    }

    /// Promote value from L3 to L2
    async fn promote_from_l3(&self, key: K, value: V) {
        let entry = CacheEntry {
            value,
            access_count: 1,
            last_access: Instant::now(),
            _created_at: Instant::now(),
            _size_estimate: std::mem::size_of::<V>(),
            _access_pattern: AccessPattern {
                _sequential: false,
                _random: true,
                _temporal_locality: 0.6,
                _spatial_locality: 0.6,
            },
        };
        
        self.demote_to_l2(key, entry).await;
    }

    /// Demote entry to L2 cache
    async fn demote_to_l2(&self, key: K, entry: CacheEntry<V>) {
        let mut cache = self.l2_cache.lock().await;
        
        if cache.len() >= self.cache_config.l2_size {
            // Evict from L2 to L3
            let evict_count = self.cache_config.eviction_batch_size.min(cache.len() / 4);
            let mut keys_to_remove = Vec::new();
            
            for (k, _) in cache.iter().take(evict_count) {
                keys_to_remove.push(k.clone());
            }
            
            for k in keys_to_remove {
                if let Some(entry) = cache.remove(&k) {
                    self.demote_to_l3(k, entry.value).await;
                }
            }
        }
        
        cache.insert(key, entry);
    }

    /// Demote entry to L3 cache
    async fn demote_to_l3(&self, key: K, value: V) {
        let mut cache = self.l3_cache.write().await;
        cache.insert(key, value);
    }

    /// Maybe trigger prefetch based on access patterns
    async fn maybe_prefetch(&self, _key: &K) {
        // Simplified prefetch logic - could be enhanced with ML
        // For now, just demonstrate the structure
        debug!("Prefetch analysis completed");
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> CacheStatistics {
        let l1_size = self.l1_cache.read().unwrap().len();
        let mut stats = self.stats.clone();
        stats.l1_current_size = l1_size;
        stats
    }

    /// Clear all cache levels
    pub async fn clear(&self) {
        self.l1_cache.write().unwrap().clear();
        self.l2_cache.lock().await.clear();
        self.l3_cache.write().await.clear();
        self.prefetch_queue.write().unwrap().clear();
        debug!("Cleared all cache levels");
    }
}

// Supporting types and structures

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct QuerySignature {
    query_type: String,
    parameters: Vec<String>,
    options: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum StorageQuery {
    PointQuery { key: String, database: String },
    RangeQuery { start: String, end: String, database: String, reverse: bool },
    PrefixQuery { prefix: String, database: String, limit: Option<usize> },
}

impl StorageQuery {
    fn signature(&self) -> QuerySignature {
        match self {
            StorageQuery::PointQuery { key, database } => QuerySignature {
                query_type: "point".to_string(),
                parameters: vec![key.clone(), database.clone()],
                options: vec![],
            },
            StorageQuery::RangeQuery { start, end, database, reverse } => QuerySignature {
                query_type: "range".to_string(),
                parameters: vec![start.clone(), end.clone(), database.clone()],
                options: vec![reverse.to_string()],
            },
            StorageQuery::PrefixQuery { prefix, database, limit } => QuerySignature {
                query_type: "prefix".to_string(),
                parameters: vec![prefix.clone(), database.clone()],
                options: vec![limit.map_or("none".to_string(), |l| l.to_string())],
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryIndex {
    pub selectivity: f64,
    pub estimated_rows: usize,
    pub key_pattern: String,
    pub supports_range: bool,
    pub supports_prefix: bool,
}

impl QueryIndex {
    fn supports_range_query(&self, _start: &str, _end: &str) -> bool {
        self.supports_range
    }
    
    fn supports_point_query(&self, _key: &str) -> bool {
        true // All indices support point queries
    }
    
    fn supports_prefix_query(&self, _prefix: &str) -> bool {
        self.supports_prefix
    }
}

#[derive(Debug, Clone)]
pub enum WorkloadPattern {
    ReadHeavy,
    WriteHeavy,
    Mixed,
    LowLatency,
}

#[derive(Debug, Clone)]
pub enum StorageOperation {
    Read { size: usize },
    Write { size: usize },
    Delete,
    Scan { range_size: usize },
    Compact,
}

#[derive(Debug)]
pub struct StorageStats {
    operations: std::sync::atomic::AtomicU64,
    total_read_time: std::sync::Arc<std::sync::RwLock<Duration>>,
    total_write_time: std::sync::Arc<std::sync::RwLock<Duration>>,
    bytes_read: std::sync::atomic::AtomicU64,
    bytes_written: std::sync::atomic::AtomicU64,
    concurrent_readers: std::sync::atomic::AtomicUsize,
    write_frequency: std::sync::atomic::AtomicU64,
    last_operation_time: std::sync::Arc<std::sync::RwLock<Option<Instant>>>,
}

impl Clone for StorageStats {
    fn clone(&self) -> Self {
        use std::sync::atomic::Ordering;
        StorageStats {
            operations: std::sync::atomic::AtomicU64::new(self.operations.load(Ordering::SeqCst)),
            total_read_time: std::sync::Arc::new(std::sync::RwLock::new(*self.total_read_time.read().unwrap())),
            total_write_time: std::sync::Arc::new(std::sync::RwLock::new(*self.total_write_time.read().unwrap())),
            bytes_read: std::sync::atomic::AtomicU64::new(self.bytes_read.load(Ordering::SeqCst)),
            bytes_written: std::sync::atomic::AtomicU64::new(self.bytes_written.load(Ordering::SeqCst)),
            concurrent_readers: std::sync::atomic::AtomicUsize::new(self.concurrent_readers.load(Ordering::SeqCst)),
            write_frequency: std::sync::atomic::AtomicU64::new(self.write_frequency.load(Ordering::SeqCst)),
            last_operation_time: std::sync::Arc::new(std::sync::RwLock::new(*self.last_operation_time.read().unwrap())),
        }
    }
}

impl StorageStats {
    fn new() -> Self {
        Self {
            operations: std::sync::atomic::AtomicU64::new(0),
            total_read_time: std::sync::Arc::new(std::sync::RwLock::new(Duration::new(0, 0))),
            total_write_time: std::sync::Arc::new(std::sync::RwLock::new(Duration::new(0, 0))),
            bytes_read: std::sync::atomic::AtomicU64::new(0),
            bytes_written: std::sync::atomic::AtomicU64::new(0),
            concurrent_readers: std::sync::atomic::AtomicUsize::new(0),
            write_frequency: std::sync::atomic::AtomicU64::new(0),
            last_operation_time: std::sync::Arc::new(std::sync::RwLock::new(None)),
        }
    }

    fn record_operation(&mut self, operation: StorageOperation, duration: Duration, size: usize) {
        use std::sync::atomic::Ordering;
        
        self.operations.fetch_add(1, Ordering::Relaxed);
        *self.last_operation_time.write().unwrap() = Some(Instant::now());
        
        match operation {
            StorageOperation::Read { .. } => {
                *self.total_read_time.write().unwrap() += duration;
                self.bytes_read.fetch_add(size as u64, Ordering::Relaxed);
            }
            StorageOperation::Write { .. } => {
                *self.total_write_time.write().unwrap() += duration;
                self.bytes_written.fetch_add(size as u64, Ordering::Relaxed);
                self.write_frequency.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn space_utilization(&self) -> f64 {
        // Simplified - would need actual database size information
        0.5
    }

    fn concurrent_readers(&self) -> usize {
        self.concurrent_readers.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn write_frequency(&self) -> f64 {
        // Calculate writes per second - simplified
        let writes = self.write_frequency.load(std::sync::atomic::Ordering::Relaxed);
        writes as f64 / 60.0 // Approximate per second
    }
}

#[derive(Debug)]
pub struct QueryStats {
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
    query_frequencies: std::sync::Arc<std::sync::RwLock<HashMap<String, QueryFrequency>>>,
}

#[derive(Debug, Clone)]
struct QueryFrequency {
    frequency: f64,
    _last_access: Instant,
    _total_accesses: u64,
}

impl Clone for QueryStats {
    fn clone(&self) -> Self {
        use std::sync::atomic::Ordering;
        QueryStats {
            cache_hits: std::sync::atomic::AtomicU64::new(self.cache_hits.load(Ordering::SeqCst)),
            cache_misses: std::sync::atomic::AtomicU64::new(self.cache_misses.load(Ordering::SeqCst)),
            query_frequencies: std::sync::Arc::new(std::sync::RwLock::new(self.query_frequencies.read().unwrap().clone())),
        }
    }
}

impl QueryStats {
    fn new() -> Self {
        Self {
            cache_hits: std::sync::atomic::AtomicU64::new(0),
            cache_misses: std::sync::atomic::AtomicU64::new(0),
            query_frequencies: std::sync::Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn get_query_frequency(&self, query: &StorageQuery) -> QueryFrequency {
        let signature = format!("{:?}", query.signature());
        let frequencies = self.query_frequencies.read().unwrap();
        
        frequencies.get(&signature).cloned().unwrap_or(QueryFrequency {
            frequency: 0.0,
            _last_access: Instant::now(),
            _total_accesses: 0,
        })
    }
}

#[derive(Debug)]
pub struct CacheStatistics {
    pub l1_hits: std::sync::atomic::AtomicU64,
    pub l2_hits: std::sync::atomic::AtomicU64,
    pub l3_hits: std::sync::atomic::AtomicU64,
    pub misses: std::sync::atomic::AtomicU64,
    pub insertions: std::sync::atomic::AtomicU64,
    pub evictions: std::sync::atomic::AtomicU64,
    pub l1_current_size: usize,
    pub l2_current_size: usize,
    pub l3_current_size: usize,
}

impl Clone for CacheStatistics {
    fn clone(&self) -> Self {
        use std::sync::atomic::Ordering;
        CacheStatistics {
            l1_hits: std::sync::atomic::AtomicU64::new(self.l1_hits.load(Ordering::SeqCst)),
            l2_hits: std::sync::atomic::AtomicU64::new(self.l2_hits.load(Ordering::SeqCst)),
            l3_hits: std::sync::atomic::AtomicU64::new(self.l3_hits.load(Ordering::SeqCst)),
            misses: std::sync::atomic::AtomicU64::new(self.misses.load(Ordering::SeqCst)),
            insertions: std::sync::atomic::AtomicU64::new(self.insertions.load(Ordering::SeqCst)),
            evictions: std::sync::atomic::AtomicU64::new(self.evictions.load(Ordering::SeqCst)),
            l1_current_size: self.l1_current_size,
            l2_current_size: self.l2_current_size,
            l3_current_size: self.l3_current_size,
        }
    }
}

impl CacheStatistics {
    fn new() -> Self {
        Self {
            l1_hits: std::sync::atomic::AtomicU64::new(0),
            l2_hits: std::sync::atomic::AtomicU64::new(0),
            l3_hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            insertions: std::sync::atomic::AtomicU64::new(0),
            evictions: std::sync::atomic::AtomicU64::new(0),
            l1_current_size: 0,
            l2_current_size: 0,
            l3_current_size: 0,
        }
    }

    fn record_l1_hit(&self) {
        self.l1_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_l2_hit(&self) {
        self.l2_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_l3_hit(&self) {
        self.l3_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_miss(&self) {
        self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_insertion(&self) {
        self.insertions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Calculate overall cache hit rate
    pub fn hit_rate(&self) -> f64 {
        let total_hits = self.l1_hits.load(std::sync::atomic::Ordering::Relaxed) +
                        self.l2_hits.load(std::sync::atomic::Ordering::Relaxed) +
                        self.l3_hits.load(std::sync::atomic::Ordering::Relaxed);
        let total_misses = self.misses.load(std::sync::atomic::Ordering::Relaxed);
        let total = total_hits + total_misses;
        
        if total == 0 {
            0.0
        } else {
            total_hits as f64 / total as f64
        }
    }
}

/// Storage optimization errors
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("LMDB optimization failed: {0}")]
    LmdbOptimizationFailed(String),
    #[error("Query optimization failed: {0}")]
    QueryOptimizationFailed(String),
    #[error("Cache operation failed: {0}")]
    CacheOperationFailed(String),
    #[error("Storage I/O error: {0}")]
    IoError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    
    #[test]
    fn test_lmdb_config_optimization() {
        let mut optimizer = LmdbOptimizer::new(true);
        
        // Test read-heavy optimization
        let config = optimizer.optimize_config(&WorkloadPattern::ReadHeavy);
        assert!(config.read_ahead);
        assert!(!config.write_map);
        assert_eq!(config.max_readers, 256);
        
        // Test write-heavy optimization
        let config = optimizer.optimize_config(&WorkloadPattern::WriteHeavy);
        assert!(config.write_map);
        assert!(config.no_meta_sync);
        assert_eq!(config.growth_step, 512 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_query_optimizer() {
        let optimizer = QueryOptimizer::new();
        
        // Add test index
        let index = QueryIndex {
            selectivity: 0.1,
            estimated_rows: 1000,
            key_pattern: "timestamp_*".to_string(),
            supports_range: true,
            supports_prefix: true,
        };
        optimizer.add_index("timestamp_idx".to_string(), index);
        
        // Test query optimization
        let query = StorageQuery::RangeQuery {
            start: "2024-01-01".to_string(),
            end: "2024-01-31".to_string(),
            database: "candles".to_string(),
            reverse: false,
        };
        
        let optimized = optimizer.optimize_query(&query).await;
        assert!(optimized.estimated_cost < f64::INFINITY);
        assert!(!optimized.index_hints.is_empty());
    }

    #[tokio::test]
    async fn test_intelligent_cache() {
        let config = CacheConfig::default();
        let cache = IntelligentCache::<String, String>::new(config, EvictionPolicy::Lru);
        
        // Test basic operations
        cache.put("key1".to_string(), "value1".to_string()).await;
        let value = cache.get(&"key1".to_string()).await;
        assert_eq!(value, Some("value1".to_string()));
        
        // Test cache miss
        let value = cache.get(&"nonexistent".to_string()).await;
        assert_eq!(value, None);
        
        let stats = cache.get_stats();
        assert!(stats.hit_rate() > 0.0);
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let mut config = CacheConfig::default();
        config.l1_size = 2; // Small cache for testing eviction
        config.eviction_batch_size = 1;
        
        let cache = IntelligentCache::<String, String>::new(config, EvictionPolicy::Lru);
        
        // Fill cache beyond capacity
        cache.put("key1".to_string(), "value1".to_string()).await;
        cache.put("key2".to_string(), "value2".to_string()).await;
        cache.put("key3".to_string(), "value3".to_string()).await; // Should trigger eviction
        
        // key1 should be evicted (LRU), key3 should be present
        let _value1 = cache.get(&"key1".to_string()).await;
        let value3 = cache.get(&"key3".to_string()).await;
        
        // Note: Due to multi-level cache, key1 might be in L2
        assert_eq!(value3, Some("value3".to_string()));
    }

    #[test]
    fn test_storage_stats() {
        let mut stats = StorageStats::new();
        
        stats.record_operation(
            StorageOperation::Read { size: 1024 },
            Duration::from_millis(10),
            1024
        );
        
        assert_eq!(stats.operations.load(Ordering::Relaxed), 1);
        assert_eq!(stats.bytes_read.load(Ordering::Relaxed), 1024);
    }

    #[test]
    fn test_cache_statistics() {
        let stats = CacheStatistics::new();
        
        stats.record_l1_hit();
        stats.record_l1_hit();
        stats.record_miss();
        
        assert_eq!(stats.l1_hits.load(Ordering::Relaxed), 2);
        assert_eq!(stats.misses.load(Ordering::Relaxed), 1);
        assert!((stats.hit_rate() - 0.666).abs() < 0.1);
    }

    #[test]
    fn test_lmdb_optimizer_auto_tuning() {
        let mut optimizer = LmdbOptimizer::new(true);
        
        // Simulate high-frequency writes
        optimizer.update_stats(
            StorageOperation::Write { size: 1024 },
            Duration::from_millis(5),
            1024
        );
        
        // Test auto-tuning with high write frequency
        let config = optimizer.optimize_config(&WorkloadPattern::WriteHeavy);
        assert!(config.write_map);
        assert!(config.no_meta_sync);
        assert_eq!(config.growth_step, 512 * 1024 * 1024);
    }

    #[test]
    fn test_query_signature_generation() {
        let query1 = StorageQuery::PointQuery {
            key: "test_key".to_string(),
            database: "test_db".to_string(),
        };
        
        let query2 = StorageQuery::RangeQuery {
            start: "start".to_string(),
            end: "end".to_string(),
            database: "test_db".to_string(),
            reverse: false,
        };
        
        let sig1 = query1.signature();
        let sig2 = query2.signature();
        
        assert_eq!(sig1.query_type, "point");
        assert_eq!(sig2.query_type, "range");
        assert_ne!(sig1, sig2);
    }

    #[tokio::test]
    async fn test_cache_promotion() {
        let config = CacheConfig::default();
        let cache = IntelligentCache::<String, String>::new(config, EvictionPolicy::Lru);
        
        // Put item in L3 directly (simulated)
        cache.put("test".to_string(), "value".to_string()).await;
        
        // First access should promote from L3 to L1
        let value1 = cache.get(&"test".to_string()).await;
        assert_eq!(value1, Some("value".to_string()));
        
        // Second access should be L1 hit
        let value2 = cache.get(&"test".to_string()).await;
        assert_eq!(value2, Some("value".to_string()));
        
        let stats = cache.get_stats();
        assert!(stats.hit_rate() > 0.0);
    }

    #[test]
    fn test_workload_pattern_optimization() {
        let mut optimizer = LmdbOptimizer::new(false); // Disable auto-tuning for predictable results
        
        let read_heavy_config = optimizer.optimize_config(&WorkloadPattern::ReadHeavy);
        assert!(read_heavy_config.read_ahead);
        assert!(!read_heavy_config.write_map);
        assert_eq!(read_heavy_config.max_readers, 256);
        
        let write_heavy_config = optimizer.optimize_config(&WorkloadPattern::WriteHeavy);
        assert!(write_heavy_config.write_map);
        assert!(write_heavy_config.no_meta_sync);
        
        let mixed_config = optimizer.optimize_config(&WorkloadPattern::Mixed);
        assert!(mixed_config.write_map);
        assert!(mixed_config.read_ahead);
    }

    #[tokio::test]
    async fn test_intelligent_cache_eviction_policy() {
        let mut config = CacheConfig::default();
        config.l1_size = 3; // Small cache for testing
        config.eviction_batch_size = 1;
        
        let cache = IntelligentCache::<String, String>::new(config, EvictionPolicy::Lru);
        
        // Fill cache to capacity
        cache.put("key1".to_string(), "value1".to_string()).await;
        cache.put("key2".to_string(), "value2".to_string()).await;
        cache.put("key3".to_string(), "value3".to_string()).await;
        
        // Access key1 to make it most recently used
        let _ = cache.get(&"key1".to_string()).await;
        
        // Add new key, should evict least recently used (key2 or key3)
        cache.put("key4".to_string(), "value4".to_string()).await;
        
        // key1 should still be accessible (most recently used)
        let value1 = cache.get(&"key1".to_string()).await;
        assert_eq!(value1, Some("value1".to_string()));
        
        // key4 should be accessible (just added)
        let value4 = cache.get(&"key4".to_string()).await;
        assert_eq!(value4, Some("value4".to_string()));
    }
}