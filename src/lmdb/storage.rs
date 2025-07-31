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
use crate::common::lmdb_config::open_lmdb_environment;

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

/// LMDB storage manager for candle data
pub struct LmdbStorage {
    pub envs: FxHashMap<(Arc<str>, Seconds), Env>,
    pub candle_dbs: FxHashMap<(Arc<str>, Seconds), Database<Str, SerdeBincode<FuturesOHLCVCandle>>>,
    pub certified_range_dbs: FxHashMap<(Arc<str>, Seconds), Database<Str, SerdeBincode<TimeRange>>>,
    base_path: std::path::PathBuf,
}

impl LmdbStorage {
    pub fn new(base_path: &Path) -> Self {
        Self {
            envs: FxHashMap::default(),
            candle_dbs: FxHashMap::default(),
            certified_range_dbs: FxHashMap::default(),
            base_path: base_path.to_path_buf(),
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

        // Create LMDB environment using shared configuration
        let env = open_lmdb_environment(&symbol_tf_path)?;

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

    /// Get candles in time range
    pub fn get_candles(
        &mut self,
        symbol: &str,
        timeframe: Seconds,
        start_time: TimestampMS,
        end_time: TimestampMS,
        limit: Option<u32>,
    ) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
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

        let iter = candle_db
            .range(&rtxn, &(Bound::Included(&start_key[..]), Bound::Included(&end_key[..])))?;

        for result in iter {
            let (_key, candle) = result?;
            
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

        debug!("📊 Retrieved {} candles for {} {}s in range {} to {}", 
               candles.len(), symbol, timeframe, start_time, end_time);

        Ok(candles)
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
}