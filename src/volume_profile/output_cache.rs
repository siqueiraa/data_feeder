//! Pre-computed Output Caching System
//! 
//! This module implements intelligent caching for volume profile calculations
//! to eliminate redundant serialization work and achieve <1ms output generation.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use crate::volume_profile::structs::VolumeProfileData;
use crate::volume_profile::zero_copy_serialization::{VolumeProfileSerializer, ZeroCopySerializationError};

/// Cache key for volume profile results
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub symbol: String,
    pub date: String,
    pub calculation_hash: u64, // Hash of calculation parameters
}

/// Cached output with metadata
#[derive(Debug, Clone)]
pub struct CachedOutput {
    pub serialized_data: String,
    pub created_at: Instant,
    pub access_count: u64,
    pub last_accessed: Instant,
    pub profile_hash: u64, // Hash of the profile data for invalidation
}

/// Cache performance metrics
#[derive(Debug, Clone)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub invalidations: u64,
    pub current_entries: usize,
    pub memory_usage_bytes: usize,
    pub hit_ratio: f64,
}

/// Configuration for the output cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub max_entries: usize,
    pub ttl_seconds: u64,
    pub max_memory_mb: usize,
    pub cleanup_interval_seconds: u64,
    pub warming_enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10000,        // Support 10k cached profiles
            ttl_seconds: 300,          // 5-minute TTL (trading data changes frequently)
            max_memory_mb: 500,        // 500MB memory limit
            cleanup_interval_seconds: 60, // Clean expired entries every minute
            warming_enabled: true,     // Enable predictive cache warming
        }
    }
}

/// Pre-computed output cache for volume profile results
pub struct OutputCache {
    cache: Arc<RwLock<HashMap<CacheKey, CachedOutput>>>,
    serializer: Arc<AsyncMutex<VolumeProfileSerializer>>,
    config: CacheConfig,
    metrics: Arc<RwLock<CacheMetrics>>,
    last_cleanup: Arc<AsyncMutex<Instant>>,
}

impl OutputCache {
    /// Create new output cache with configuration
    pub fn new(config: CacheConfig) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            serializer: Arc::new(AsyncMutex::new(VolumeProfileSerializer::new())),
            config,
            metrics: Arc::new(RwLock::new(CacheMetrics {
                hits: 0,
                misses: 0,
                invalidations: 0,
                current_entries: 0,
                memory_usage_bytes: 0,
                hit_ratio: 0.0,
            })),
            last_cleanup: Arc::new(AsyncMutex::new(Instant::now())),
        }
    }

    /// Get cached output or compute and cache if not present
    pub async fn get_or_compute(
        &self,
        key: CacheKey,
        profile_data: &VolumeProfileData,
    ) -> Result<String, ZeroCopySerializationError> {
        let profile_hash = self.calculate_profile_hash(profile_data);

        // Try to get from cache first
        if let Some(cached) = self.get_cached(&key, profile_hash).await {
            self.update_metrics_hit();
            return Ok(cached.serialized_data);
        }

        // Cache miss - compute and store
        self.update_metrics_miss();
        
        let mut serializer = self.serializer.lock().await;
        let serialized = serializer.serialize_volume_profile(profile_data)?;
        drop(serializer);

        // Store in cache
        self.put_cached(key, serialized.clone(), profile_hash).await;

        // Perform cleanup if needed
        self.maybe_cleanup().await;

        Ok(serialized)
    }

    /// Get cached result with validation
    async fn get_cached(&self, key: &CacheKey, profile_hash: u64) -> Option<CachedOutput> {
        let cached = {
            let cache = self.cache.read().unwrap();
            cache.get(key).cloned()
        }?;

        // Validate cache entry
        if cached.profile_hash != profile_hash {
            // Profile data changed, cache invalid
            self.invalidate_key(key).await;
            return None;
        }

        if cached.created_at.elapsed() > Duration::from_secs(self.config.ttl_seconds) {
            // Expired
            self.invalidate_key(key).await;
            return None;
        }

        // Update access tracking
        let mut updated = cached.clone();
        updated.access_count += 1;
        updated.last_accessed = Instant::now();
        
        // Update cache with new access info
        let mut cache_write = self.cache.write().unwrap();
        cache_write.insert(key.clone(), updated.clone());
        drop(cache_write);

        Some(updated)
    }

    /// Store result in cache
    async fn put_cached(&self, key: CacheKey, data: String, profile_hash: u64) {
        let entry = CachedOutput {
            serialized_data: data,
            created_at: Instant::now(),
            access_count: 1,
            last_accessed: Instant::now(),
            profile_hash,
        };

        let mut cache = self.cache.write().unwrap();
        cache.insert(key, entry);
        
        // Update metrics
        let mut metrics = self.metrics.write().unwrap();
        metrics.current_entries = cache.len();
        metrics.memory_usage_bytes = self.estimate_memory_usage(&cache);
    }

    /// Invalidate specific cache key
    async fn invalidate_key(&self, key: &CacheKey) {
        let mut cache = self.cache.write().unwrap();
        cache.remove(key);
        
        let mut metrics = self.metrics.write().unwrap();
        metrics.invalidations += 1;
        metrics.current_entries = cache.len();
        metrics.memory_usage_bytes = self.estimate_memory_usage(&cache);
    }

    /// Invalidate all cache entries for a symbol (when new calculation cycle starts)
    pub async fn invalidate_symbol(&self, symbol: &str) {
        let mut cache = self.cache.write().unwrap();
        cache.retain(|key, _| key.symbol != symbol);
        
        let mut metrics = self.metrics.write().unwrap();
        metrics.invalidations += 1;
        metrics.current_entries = cache.len();
        metrics.memory_usage_bytes = self.estimate_memory_usage(&cache);
    }

    /// Warm cache with frequently accessed profiles
    pub async fn warm_cache(&self, profiles: Vec<(CacheKey, VolumeProfileData)>) {
        if !self.config.warming_enabled {
            return;
        }

        for (key, profile) in profiles {
            // Only warm if not already cached
            let profile_hash = self.calculate_profile_hash(&profile);
            if self.get_cached(&key, profile_hash).await.is_none() {
                let _ = self.get_or_compute(key, &profile).await;
            }
        }
    }

    /// Clean up expired entries and enforce memory limits
    async fn maybe_cleanup(&self) {
        let mut last_cleanup = self.last_cleanup.lock().await;
        if last_cleanup.elapsed() < Duration::from_secs(self.config.cleanup_interval_seconds) {
            return;
        }
        *last_cleanup = Instant::now();
        drop(last_cleanup);

        let now = Instant::now();
        let ttl_duration = Duration::from_secs(self.config.ttl_seconds);
        
        let mut cache = self.cache.write().unwrap();
        
        // Remove expired entries
        cache.retain(|_, entry| now.duration_since(entry.created_at) < ttl_duration);
        
        // Enforce memory limits by removing least recently used entries
        let memory_limit_bytes = self.config.max_memory_mb * 1024 * 1024;
        let current_memory = self.estimate_memory_usage(&cache);
        
        if current_memory > memory_limit_bytes || cache.len() > self.config.max_entries {
            // Convert to vector, sort by access time, remove oldest
            let mut entries: Vec<_> = cache.iter().collect();
            entries.sort_by_key(|(_, v)| v.last_accessed);
            
            let target_size = (self.config.max_entries * 8) / 10; // Keep 80% of max entries
            let keys_to_remove: Vec<_> = entries.iter()
                .take(cache.len().saturating_sub(target_size))
                .map(|(k, _)| (*k).clone())
                .collect();
                
            for key in keys_to_remove {
                cache.remove(&key);
            }
        }
        
        // Update metrics
        let mut metrics = self.metrics.write().unwrap();
        metrics.current_entries = cache.len();
        metrics.memory_usage_bytes = self.estimate_memory_usage(&cache);
        metrics.hit_ratio = if metrics.hits + metrics.misses > 0 {
            metrics.hits as f64 / (metrics.hits + metrics.misses) as f64
        } else {
            0.0
        };
    }

    /// Get cache performance metrics
    pub fn get_metrics(&self) -> CacheMetrics {
        let metrics = self.metrics.read().unwrap();
        metrics.clone()
    }

    /// Reset cache metrics
    pub fn reset_metrics(&self) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.hits = 0;
        metrics.misses = 0;
        metrics.invalidations = 0;
    }

    /// Clear all cache entries
    pub async fn clear(&self) {
        let mut cache = self.cache.write().unwrap();
        cache.clear();
        
        let mut metrics = self.metrics.write().unwrap();
        metrics.current_entries = 0;
        metrics.memory_usage_bytes = 0;
        metrics.invalidations += 1;
    }

    /// Calculate hash of profile data for change detection
    fn calculate_profile_hash(&self, profile: &VolumeProfileData) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        profile.date.hash(&mut hasher);
        profile.total_volume.hash(&mut hasher);
        profile.vwap.hash(&mut hasher);
        profile.poc.hash(&mut hasher);
        profile.candle_count.hash(&mut hasher);
        profile.last_updated.hash(&mut hasher);
        profile.price_levels.len().hash(&mut hasher);
        
        // Hash first and last few price levels to detect significant changes
        if let Some(first) = profile.price_levels.first() {
            first.price.hash(&mut hasher);
            first.volume.hash(&mut hasher);
        }
        if let Some(last) = profile.price_levels.last() {
            last.price.hash(&mut hasher);
            last.volume.hash(&mut hasher);
        }

        hasher.finish()
    }

    /// Estimate memory usage of cache
    fn estimate_memory_usage(&self, cache: &HashMap<CacheKey, CachedOutput>) -> usize {
        cache.iter().map(|(key, value)| {
            // Rough estimate: key size + value size + overhead
            std::mem::size_of::<CacheKey>() +
            key.symbol.len() +
            key.date.len() +
            std::mem::size_of::<CachedOutput>() +
            value.serialized_data.len()
        }).sum()
    }

    /// Update hit metrics
    fn update_metrics_hit(&self) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.hits += 1;
    }

    /// Update miss metrics
    fn update_metrics_miss(&self) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.misses += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_profile::structs::{PriceLevelData, ValueArea};
    use rust_decimal_macros::dec;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_test_profile() -> VolumeProfileData {
        VolumeProfileData {
            date: "2023-08-10".to_string(),
            total_volume: dec!(1000000),
            vwap: dec!(50000.0),
            poc: dec!(50050.0),
            price_increment: dec!(0.01),
            min_price: dec!(49900.0),
            max_price: dec!(50100.0),
            candle_count: 100,
            last_updated: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            price_levels: vec![
                PriceLevelData {
                    price: dec!(50000.0),
                    volume: dec!(100000),
                    percentage: dec!(10.0),
                    candle_count: 50,
                },
            ],
            value_area: ValueArea {
                high: dec!(50075.0),
                low: dec!(50025.0),
                volume_percentage: dec!(70.0),
                volume: dec!(700000),
            },
        }
    }

    #[tokio::test]
    async fn test_cache_hit_miss() {
        let cache = OutputCache::new(CacheConfig::default());
        let key = CacheKey {
            symbol: "BTCUSDT".to_string(),
            date: "2023-08-10".to_string(),
            calculation_hash: 12345,
        };
        let profile = create_test_profile();

        // First call should be a miss
        let result1 = cache.get_or_compute(key.clone(), &profile).await.unwrap();
        let metrics1 = cache.get_metrics();
        assert_eq!(metrics1.misses, 1);
        assert_eq!(metrics1.hits, 0);

        // Second call should be a hit
        let result2 = cache.get_or_compute(key.clone(), &profile).await.unwrap();
        let metrics2 = cache.get_metrics();
        assert_eq!(metrics2.misses, 1);
        assert_eq!(metrics2.hits, 1);

        // Results should be identical
        assert_eq!(result1, result2);
    }

    #[tokio::test]
    async fn test_cache_invalidation_on_data_change() {
        let cache = OutputCache::new(CacheConfig::default());
        let key = CacheKey {
            symbol: "BTCUSDT".to_string(),
            date: "2023-08-10".to_string(),
            calculation_hash: 12345,
        };
        let mut profile = create_test_profile();

        // Cache initial profile
        let _result1 = cache.get_or_compute(key.clone(), &profile).await.unwrap();

        // Modify profile data
        profile.total_volume = dec!(2000000);

        // Should be a miss due to data change
        let _result2 = cache.get_or_compute(key.clone(), &profile).await.unwrap();
        let metrics = cache.get_metrics();
        assert_eq!(metrics.misses, 2); // Both calls should be misses
    }

    #[tokio::test]
    async fn test_symbol_invalidation() {
        let cache = OutputCache::new(CacheConfig::default());
        let key1 = CacheKey {
            symbol: "BTCUSDT".to_string(),
            date: "2023-08-10".to_string(),
            calculation_hash: 12345,
        };
        let key2 = CacheKey {
            symbol: "ETHUSDT".to_string(),
            date: "2023-08-10".to_string(),
            calculation_hash: 12345,
        };
        let profile = create_test_profile();

        // Cache profiles for both symbols
        let _result1 = cache.get_or_compute(key1.clone(), &profile).await.unwrap();
        let _result2 = cache.get_or_compute(key2.clone(), &profile).await.unwrap();

        // Invalidate BTCUSDT
        cache.invalidate_symbol("BTCUSDT").await;

        // BTCUSDT should be a miss, ETHUSDT should be a hit
        let _result3 = cache.get_or_compute(key1.clone(), &profile).await.unwrap();
        let _result4 = cache.get_or_compute(key2.clone(), &profile).await.unwrap();

        let metrics = cache.get_metrics();
        assert_eq!(metrics.hits, 1); // Only ETHUSDT hit
        assert_eq!(metrics.misses, 3); // Initial 2 + BTCUSDT after invalidation
    }

    #[tokio::test]
    async fn test_cache_warming() {
        let cache = OutputCache::new(CacheConfig::default());
        let profiles = vec![
            (CacheKey {
                symbol: "BTCUSDT".to_string(),
                date: "2023-08-10".to_string(),
                calculation_hash: 12345,
            }, create_test_profile()),
            (CacheKey {
                symbol: "ETHUSDT".to_string(),
                date: "2023-08-10".to_string(),
                calculation_hash: 12346,
            }, create_test_profile()),
        ];

        // Warm cache
        cache.warm_cache(profiles.clone()).await;

        // Subsequent calls should be hits
        for (key, profile) in profiles {
            let _result = cache.get_or_compute(key, &profile).await.unwrap();
        }

        let metrics = cache.get_metrics();
        assert_eq!(metrics.hits, 2);
        assert_eq!(metrics.misses, 2); // From warming
    }
}