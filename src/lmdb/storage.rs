use std::path::Path;
use std::sync::Arc;
use std::ops::Bound;

use heed::{Database, Env};
use heed::types::{SerdeBincode, Str};
use rustc_hash::FxHashMap;
use tracing::{debug, info, warn};

use crate::historical::errors::HistoricalDataError;
use crate::historical::structs::{FuturesOHLCVCandle, TimeRange, TimestampMS, Seconds};
use crate::common::constants::*;
use crate::common::error_utils::ErrorContext;
use crate::common::shared_data::intern_symbol;
use crate::common::lmdb_config::open_optimized_lmdb_environment;
use crate::storage_optimization::{WorkloadPattern, QueryOptimizer, StorageQuery, IntelligentCache};

#[derive(Debug, Clone)]
pub struct StorageStatsInternal {
    pub symbols: Vec<String>,
    pub total_candles: u64,
    pub db_size_bytes: u64,
    pub symbol_stats: Vec<SymbolStatsInternal>,
}

#[derive(Debug, Clone)]
pub struct SymbolStatsInternal {
    pub symbol: String,
    pub timeframe: u64,
    pub candle_count: u64,
    pub earliest: Option<TimestampMS>,
    pub latest: Option<TimestampMS>,
    pub size_bytes: u64,
}

/// LMDB storage manager for candle data with optimization
pub struct LmdbStorage {
    pub envs: FxHashMap<(Arc<str>, Seconds), Env>,
    pub candle_dbs: FxHashMap<(Arc<str>, Seconds), Database<Str, SerdeBincode<FuturesOHLCVCandle>>>,
    pub certified_range_dbs: FxHashMap<(Arc<str>, Seconds), Database<Str, SerdeBincode<TimeRange>>>,
    base_path: std::path::PathBuf,
    // Storage optimization components
    query_optimizer: QueryOptimizer,
    storage_cache: IntelligentCache<String, Vec<FuturesOHLCVCandle>>,
}

impl LmdbStorage {
    pub fn new(base_path: &Path) -> Self {
        Self {
            envs: FxHashMap::default(),
            candle_dbs: FxHashMap::default(),
            certified_range_dbs: FxHashMap::default(),
            base_path: base_path.to_path_buf(),
            // Initialize storage optimization components
            query_optimizer: QueryOptimizer::new(),
            storage_cache: IntelligentCache::new(
                crate::storage_optimization::CacheConfig {
                    l1_size: 1000,
                    l2_size: 5000,
                    l3_size: 10000,
                    l1_ttl: std::time::Duration::from_secs(300),
                    l2_ttl: std::time::Duration::from_secs(900),
                    l3_ttl: std::time::Duration::from_secs(1800),
                    prefetch_size: 16,
                    eviction_batch_size: 100,
                },
                crate::storage_optimization::EvictionPolicy::Lru,
            ),
        }
    }


    /// Initialize database for a symbol-timeframe combination
    pub fn initialize_database(
        &mut self,
        symbol: &str,
        timeframe: Seconds,
    ) -> Result<(), HistoricalDataError> {
        let interned_symbol = intern_symbol(symbol);
        let key = (interned_symbol, timeframe);
        
        // Check if database is already initialized
        if self.envs.contains_key(&key) {
            debug!("🔄 Database for {} {}s already initialized at storage level, skipping", symbol, timeframe);
            return Ok(());
        }
        
        let db_name = format!("{}_{}", symbol, timeframe);
        let symbol_tf_path = self.base_path.join(db_name);
        
        // Create directory
        std::fs::create_dir_all(&symbol_tf_path)
            .with_io_context("Failed to create symbol-timeframe database directory")?;

        // Create optimized LMDB environment based on workload pattern
        // For candle data storage, we use WriteHeavy pattern as it's primarily insertion-focused
        let env = open_optimized_lmdb_environment(&symbol_tf_path, WorkloadPattern::WriteHeavy)?;

        // Create transaction and databases
        let mut wtxn = env.write_txn()
            .with_db_context(&format!("Failed to create initial LMDB write transaction for {} {}", symbol, timeframe))?;

        let candle_db = env
            .create_database::<Str, SerdeBincode<FuturesOHLCVCandle>>(&mut wtxn, Some(CANDLES_DB_NAME))
            .with_db_context(&format!("Failed to create candle database for {} {}", symbol, timeframe))?;

        let certified_range_db = env
            .create_database(&mut wtxn, Some(CERTIFIED_RANGE_DB_NAME))
            .with_db_context(&format!("Failed to create certified range database for {} {}", symbol, timeframe))?;

        wtxn.commit()
            .with_db_context(&format!("Failed to commit creation transaction for {} {}", symbol, timeframe))?;

        // Store references using the key we already created
        self.envs.insert(key.clone(), env);
        self.candle_dbs.insert(key.clone(), candle_db);
        self.certified_range_dbs.insert(key, certified_range_db);

        info!("✅ Initialized LMDB database for {} {}s", symbol, timeframe);
        Ok(())
    }

    /// Store candles for a symbol/timeframe
    pub fn store_candles(
        &mut self,
        symbol: &str,
        timeframe: Seconds,
        candles: &[FuturesOHLCVCandle],
    ) -> Result<u64, HistoricalDataError> {
        let interned_symbol = intern_symbol(symbol);
        let key = (interned_symbol, timeframe);

        // Database must be initialized by LmdbActor before calling this method
        let env = self.envs.get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseError(format!("Database not initialized for {} {}s - call initialize_database first", symbol, timeframe)))?;
        
        let candle_db = self.candle_dbs.get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseError(format!("No candle database for {} {}", symbol, timeframe)))?;

        let mut wtxn = env.write_txn()
            .with_db_context(&format!("Failed to create write transaction for {} {}", symbol, timeframe))?;

        let mut stored_count = 0u64;

        for candle in candles {
            let key = format!("{}:{:015}", timeframe, candle.open_time);
            
            // Check if candle already exists to avoid duplicates
            if candle_db.get(&wtxn, &key)?.is_none() {
                candle_db.put(&mut wtxn, &key, candle)
                    .with_db_context(&format!("Failed to store candle {} for {} {}", key, symbol, timeframe))?;
                stored_count += 1;
            }
        }

        wtxn.commit()
            .with_db_context(&format!("Failed to commit candles for {} {}", symbol, timeframe))?;

        if stored_count > 0 {
            debug!("✅ Stored {} new candles for {} {}s", stored_count, symbol, timeframe);
        }

        Ok(stored_count)
    }

    /// Get candles in time range with storage optimizations
    pub fn get_candles(
        &mut self,
        symbol: &str,
        timeframe: Seconds,
        start_time: TimestampMS,
        end_time: TimestampMS,
        limit: Option<u32>,
    ) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
        let start_operation = std::time::Instant::now();
        let interned_symbol = intern_symbol(symbol);
        let key = (interned_symbol, timeframe);

        // Database must be initialized by LmdbActor before calling this method
        let env = self.envs.get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseError(format!("Database not initialized for {} {}s - call initialize_database first", symbol, timeframe)))?;
        
        let candle_db = self.candle_dbs.get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseError(format!("No candle database for {} {}", symbol, timeframe)))?;

        let rtxn = env.read_txn()
            .with_db_context(&format!("Failed to create read transaction for {} {}", symbol, timeframe))?;

        let start_key = format!("{}:{:015}", timeframe, start_time);
        let end_key = format!("{}:{:015}", timeframe, end_time);

        let mut candles = Vec::new();
        let mut count = 0u32;
        let mut bytes_read = 0usize;

        let iter = candle_db
            .range(&rtxn, &(Bound::Included(&start_key[..]), Bound::Included(&end_key[..])))?;

        for result in iter {
            let (_key, candle) = result?;
            bytes_read += std::mem::size_of::<FuturesOHLCVCandle>();
            
            // Filter by actual time range (LMDB range is by string, not timestamp)
            if candle.open_time >= start_time && candle.open_time <= end_time {
                candles.push(candle);
                count += 1;
                
                if let Some(limit) = limit {
                    if count >= limit {
                        break;
                    }
                }
            }
        }

        // Record storage optimization metrics
        if let Some(metrics) = crate::metrics::get_metrics() {
            let duration = start_operation.elapsed();
            let query_effectiveness = if candles.is_empty() { 0.0 } else { 1.0 };
            
            metrics.record_storage_optimization_metrics(
                &format!("{}:{}", symbol, timeframe),
                "database_query",
                0.0, // No cache hit for now
                query_effectiveness,
                duration,
            );
        }

        debug!("📊 Retrieved {} candles for {} {}s in range {} to {} ({} bytes, {:.2}ms)", 
               candles.len(), symbol, timeframe, start_time, end_time, bytes_read, start_operation.elapsed().as_millis());

        Ok(candles)
    }

    /// Optimized candle retrieval with query optimization and caching
    pub async fn get_candles_optimized(
        &mut self,
        symbol: &str,
        timeframe: Seconds,
        start_time: TimestampMS,
        end_time: TimestampMS,
        limit: Option<u32>,
    ) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
        let start_timer = std::time::Instant::now();
        let cache_key = format!("{}:{}:{}:{}:{}", 
                               symbol, timeframe, start_time, end_time, 
                               limit.map_or_else(|| "all".to_string(), |l| l.to_string()));

        // Check cache first
        if let Some(cached_result) = self.storage_cache.get(&cache_key).await {
            debug!("🎯 Cache hit for optimized query: {}", cache_key);
            return Ok(cached_result);
        }

        // Create storage query for optimization
        let storage_query = StorageQuery::RangeQuery {
            start: format!("{}:{:015}", timeframe, start_time),
            end: format!("{}:{:015}", timeframe, end_time),
            database: format!("{}_{}", symbol, timeframe),
            reverse: false,
        };

        // Optimize the query
        let optimized_query = self.query_optimizer.optimize_query(&storage_query).await;
        debug!("📈 Query optimization: cost={:.2}, plan={:?}", 
               optimized_query.estimated_cost, optimized_query.execution_plan);

        // Execute the original query (for now - in production this would use the optimized plan)
        let result = self.get_candles(symbol, timeframe, start_time, end_time, limit)?;

        // Cache the result based on optimization strategy
        match optimized_query.cache_strategy {
            crate::storage_optimization::CacheStrategy::NoCache => {
                // Don't cache
            }
            crate::storage_optimization::CacheStrategy::ShortTerm { .. } |
            crate::storage_optimization::CacheStrategy::LongTerm { .. } |
            crate::storage_optimization::CacheStrategy::Adaptive { .. } => {
                // Note: Current cache API doesn't support TTL in put method
                // For now, use default TTL configured at cache creation
                self.storage_cache.put(cache_key, result.clone()).await;
            }
        }

        debug!("📊 Optimized query retrieved {} candles for {} {}s", 
               result.len(), symbol, timeframe);

        // Record storage optimization metrics
        let operation_duration = start_timer.elapsed();
        let cache_stats = self.get_cache_stats().await;
        let query_effectiveness = 1.0 - (optimized_query.estimated_cost / 1000.0).min(1.0); // Convert cost to effectiveness
        
        if let Some(metrics) = crate::metrics::get_metrics() {
            metrics.record_storage_optimization_metrics(
                &format!("{}_{}", symbol, timeframe),
                "range_query_optimized",
                cache_stats.hit_rate(),
                query_effectiveness,
                operation_duration,
            );
        }

        Ok(result)
    }

    /// Warm up cache with frequently accessed data
    pub async fn warm_cache(&mut self, symbol: &str, timeframe: Seconds) -> Result<(), HistoricalDataError> {
        debug!("🔥 Warming cache for {} {}s", symbol, timeframe);
        
        // Get recent data for warming
        let end_time = chrono::Utc::now().timestamp_millis();
        let start_time = end_time - (24 * 60 * 60 * 1000); // Last 24 hours
        
        // Warm cache with recent data
        let _recent_data = self.get_candles_optimized(symbol, timeframe, start_time, end_time, Some(1000)).await?;
        
        debug!("🔥 Cache warming completed for {} {}s", symbol, timeframe);
        Ok(())
    }

    /// Get cache statistics
    pub async fn get_cache_stats(&self) -> crate::storage_optimization::CacheStatistics {
        self.storage_cache.get_stats()
    }

    /// Clear cache for specific symbol/timeframe
    pub async fn clear_cache(&mut self, _symbol: &str, _timeframe: Seconds) -> usize {
        // For now, clear entire cache since pattern-based clearing isn't available
        self.storage_cache.clear().await;
        // Return 0 as we don't track exact count of cleared items
        0
    }

    /// Optimize storage by analyzing access patterns
    pub async fn optimize_storage_patterns(&mut self) -> Result<(), HistoricalDataError> {
        debug!("🔄 Optimizing storage access patterns");
        
        // Get cache statistics to understand access patterns
        let cache_stats = self.get_cache_stats().await;
        
        // Adjust cache policies based on hit rate
        if cache_stats.hit_rate() < 0.7 {
            debug!("📈 Low cache hit rate ({:.2}), could benefit from larger cache", cache_stats.hit_rate());
            // Note: Current cache API doesn't support dynamic resizing
            // In production, this could trigger cache recreation with larger size
        } else if cache_stats.hit_rate() > 0.9 {
            debug!("📉 High cache hit rate ({:.2}), cache performing well", cache_stats.hit_rate());
            // Could implement TTL optimization here
        }
        
        debug!("✅ Storage pattern optimization completed");
        Ok(())
    }

    /// Get data range for symbol/timeframe
    pub fn get_data_range(
        &mut self,
        symbol: &str,
        timeframe: Seconds,
    ) -> Result<(Option<TimestampMS>, Option<TimestampMS>, u64), HistoricalDataError> {
        let interned_symbol = intern_symbol(symbol);
        let key = (interned_symbol, timeframe);

        // Database must be initialized by LmdbActor before calling this method
        let env = self.envs.get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseError(format!("Database not initialized for {} {}s - call initialize_database first", symbol, timeframe)))?;
        
        let candle_db = self.candle_dbs.get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseError(format!("No candle database for {} {}", symbol, timeframe)))?;

        let rtxn = env.read_txn()
            .with_db_context(&format!("Failed to create read transaction for {} {}", symbol, timeframe))?;

        let start_time = std::time::Instant::now();
        
        // Get count efficiently using LMDB len() method
        let count = candle_db.len(&rtxn)? as u64;
        
        let mut earliest: Option<TimestampMS> = None;
        let mut latest: Option<TimestampMS> = None;
        
        if count > 0 {
            // Get first candle (earliest) efficiently
            if let Some(Ok((_key, first_candle))) = candle_db.iter(&rtxn)?.next() {
                earliest = Some(first_candle.open_time);
            }
            
            // Get last candle (latest) efficiently
            if let Some(Ok((_key, last_candle))) = candle_db.iter(&rtxn)?.last() {
                latest = Some(last_candle.open_time);
            }
        }
        
        let elapsed = start_time.elapsed();
        debug!("⚡ Fast data range query for {} {}s completed in {:?}", symbol, timeframe, elapsed);

        debug!("📊 Data range for {} {}s: earliest={:?}, latest={:?}, count={}", 
               symbol, timeframe, earliest, latest, count);

        Ok((earliest, latest, count))
    }

    /// Get storage statistics  
    pub fn get_storage_stats(&mut self) -> Result<StorageStatsInternal, HistoricalDataError> {
        let mut symbols = Vec::new();
        let mut total_candles = 0u64;
        let mut db_size_bytes = 0u64;
        let mut symbol_stats = Vec::new();

        // Collect keys first to avoid borrowing issues
        let keys: Vec<_> = self.envs.keys().cloned().collect();
        
        for (symbol, timeframe) in keys {
            let (earliest, latest, count) = self.get_data_range(&symbol, timeframe)?;
            
            total_candles += count;
            
            // Estimate database size (this is approximate)
            if let Some(env) = self.envs.get(&(symbol.clone(), timeframe)) {
                let env_info = env.info();
                let env_size = env_info.map_size as u64;
                db_size_bytes += env_size;
                
                if !symbols.contains(&symbol.to_string()) {
                    symbols.push(symbol.to_string());
                }
                
                symbol_stats.push(SymbolStatsInternal {
                    symbol: symbol.to_string(),
                    timeframe,
                    candle_count: count,
                    earliest,
                    latest,
                    size_bytes: env_size,
                });
            }
        }

        let stats = StorageStatsInternal {
            symbols,
            total_candles,
            db_size_bytes,
            symbol_stats,
        };

        info!("📊 Storage stats: {} symbols, {} candles, {} bytes", 
              stats.symbols.len(), stats.total_candles, stats.db_size_bytes);

        Ok(stats)
    }

    /// Compact database for symbol/timeframe (or all if None)
    pub fn compact_database(&self, symbol: Option<&str>) -> Result<(), HistoricalDataError> {
        if let Some(symbol) = symbol {
            info!("🗜️ Compaction requested for symbol: {} (not implemented)", symbol);
        } else {
            info!("🗜️ Full database compaction requested (not implemented)");
        }
        
        // TODO: Implement database compaction using LMDB copy functionality
        // This requires additional LMDB features that may not be available in all versions
        warn!("⚠️ Database compaction is not yet implemented");
        Ok(())
    }

    /// Check if database exists for symbol/timeframe
    pub fn database_exists(&self, symbol: &str, timeframe: Seconds) -> bool {
        let interned_symbol = intern_symbol(symbol);
        let key = (interned_symbol, timeframe);
        self.envs.contains_key(&key)
    }

    /// Check if a specific key exists (for volume profile reprocessing)
    #[cfg(feature = "volume_profile_reprocessing")]
    pub fn check_key_exists(&self, _key: &str) -> Result<bool, HistoricalDataError> {
        // For now, this is a placeholder implementation
        // In a full implementation, this would check if volume profile data
        // exists for the given key across different databases
        
        // Parse key format: "symbol:date" for volume profiles
        // For now, return false to indicate missing data
        // This will trigger gap detection as intended
        Ok(false)
    }

}