use std::path::Path;
use std::sync::Arc;

use kameo::actor::ActorRef;
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use rustc_hash::FxHashSet;
use tracing::{error, info, warn, debug};

use crate::historical::errors::HistoricalDataError;
use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS, Seconds};
use super::messages::{LmdbActorMessage, LmdbActorResponse, LmdbActorTell};
use super::gap_detector::GapDetector;
use super::storage::LmdbStorage;

pub struct LmdbActor {
    storage: LmdbStorage,
    gap_detector: GapDetector,
    initialized_dbs: FxHashSet<(Arc<str>, Seconds)>,
}

impl LmdbActor {
    /// Create new LmdbActor
    pub fn new(base_path: &Path, min_gap_threshold_seconds: u64) -> Result<Self, HistoricalDataError> {
        // Ensure base path exists
        if !base_path.exists() {
            info!("📁 Creating LMDB base directory at: {}", base_path.display());
            std::fs::create_dir_all(base_path)
                .map_err(HistoricalDataError::Io)?;
        }

        let storage = LmdbStorage::new(base_path);
        let gap_detector = GapDetector::new(min_gap_threshold_seconds);

        info!("🎭 LmdbActor initialized with base path: {}", base_path.display());
        
        Ok(Self {
            storage,
            gap_detector,
            initialized_dbs: FxHashSet::default(),
        })
    }

    /// Initialize databases for multiple symbols and timeframes
    pub fn initialize_databases(
        &mut self,
        symbols: &[String],
        timeframes: &[Seconds],
    ) -> Result<(), HistoricalDataError> {
        for symbol in symbols {
            for &timeframe in timeframes {
                self.storage.initialize_database(symbol, timeframe)?;
                let interned_symbol = crate::common::shared_data::intern_symbol(symbol);
                self.initialized_dbs.insert((interned_symbol, timeframe));
            }
        }
        
        info!("✅ Initialized LMDB databases for {} symbols and {} timeframes", 
              symbols.len(), timeframes.len());
        Ok(())
    }

    /// Ensure database is initialized for the given symbol and timeframe
    /// This method is thread-safe because it runs within the actor's single-threaded context
    fn ensure_database_initialized(
        &mut self,
        symbol: &str,
        timeframe: Seconds,
    ) -> Result<(), HistoricalDataError> {
        let interned_symbol = crate::common::shared_data::intern_symbol(symbol);
        let key = (interned_symbol, timeframe);
        
        // Check if database is already initialized
        if self.initialized_dbs.contains(&key) {
            return Ok(());
        }
        
        // Initialize the database
        debug!("🔧 Initializing database for {} {}s (on-demand)", symbol, timeframe);
        self.storage.initialize_database(symbol, timeframe)?;
        
        // Track that this database is now initialized
        self.initialized_dbs.insert(key);
        
        info!("✅ Database initialized for {} {}s", symbol, timeframe);
        Ok(())
    }

    async fn handle_validate_data_range(
        &mut self,
        symbol: String,
        timeframe: u64,
        start: TimestampMS,
        end: TimestampMS,
    ) -> LmdbActorResponse {
        info!("🔍 Validating data range for {} {}s from {} to {}", symbol, timeframe, start, end);

        // Ensure database is initialized before proceeding
        if let Err(e) = self.ensure_database_initialized(&symbol, timeframe) {
            error!("❌ Failed to initialize database for {} {}s: {}", symbol, timeframe, e);
            return LmdbActorResponse::ErrorResponse(format!("Database initialization error: {}", e));
        }

        // Get all candles in the requested range
        match self.storage.get_candles(&symbol, timeframe, start, end, None) {
            Ok(candles) => {
                info!("📊 Retrieved {} candles for gap analysis", candles.len());
                
                // Detect gaps using the gap detector
                let gaps = self.gap_detector.detect_gaps(&candles, timeframe, start, end);
                
                // Provide detailed analysis
                let analysis = self.gap_detector.analyze_gaps(&gaps, &symbol, timeframe);
                let total_requested_time = end - start;
                let missing_percentage = analysis.missing_percentage(total_requested_time);
                
                if !gaps.is_empty() {
                    warn!("⚠️ Found {} gaps for {} {}s ({:.1}% missing)", 
                          gaps.len(), symbol, timeframe, missing_percentage);
                } else {
                    info!("✅ No gaps found for {} {}s - data is complete", symbol, timeframe);
                }

                LmdbActorResponse::ValidationResult { gaps }
            }
            Err(e) => {
                error!("❌ Failed to retrieve candles for gap analysis: {}", e);
                LmdbActorResponse::ErrorResponse(format!("Failed to retrieve candles: {}", e))
            }
        }
    }

    async fn handle_store_candles(
        &mut self,
        symbol: String,
        timeframe: u64,
        candles: Vec<FuturesOHLCVCandle>,
    ) -> LmdbActorResponse {
        debug!("💾 Storing {} candles for {} {}s", candles.len(), symbol, timeframe);

        // Ensure database is initialized before proceeding
        if let Err(e) = self.ensure_database_initialized(&symbol, timeframe) {
            error!("❌ Failed to initialize database for {} {}s: {}", symbol, timeframe, e);
            return LmdbActorResponse::ErrorResponse(format!("Database initialization error: {}", e));
        }

        match self.storage.store_candles(&symbol, timeframe, &candles) {
            Ok(stored_count) => {
                if stored_count > 0 {
                    info!("✅ Stored {} new candles for {} {}s", stored_count, symbol, timeframe);
                }
                LmdbActorResponse::Success
            }
            Err(e) => {
                error!("❌ Failed to store candles: {}", e);
                LmdbActorResponse::ErrorResponse(format!("Failed to store candles: {}", e))
            }
        }
    }

    async fn handle_get_data_range(
        &mut self,
        symbol: String,
        timeframe: u64,
    ) -> LmdbActorResponse {
        debug!("📊 Getting data range for {} {}s", symbol, timeframe);

        // Ensure database is initialized before proceeding
        if let Err(e) = self.ensure_database_initialized(&symbol, timeframe) {
            error!("❌ Failed to initialize database for {} {}s: {}", symbol, timeframe, e);
            return LmdbActorResponse::ErrorResponse(format!("Database initialization error: {}", e));
        }

        match self.storage.get_data_range(&symbol, timeframe) {
            Ok((earliest, latest, count)) => {
                LmdbActorResponse::DataRange { earliest, latest, count }
            }
            Err(e) => {
                error!("❌ Failed to get data range: {}", e);
                LmdbActorResponse::ErrorResponse(format!("Failed to get data range: {}", e))
            }
        }
    }

    async fn handle_get_candles(
        &mut self,
        symbol: String,
        timeframe: u64,
        start: TimestampMS,
        end: TimestampMS,
        limit: Option<u32>,
    ) -> LmdbActorResponse {
        debug!("📈 Getting candles for {} {}s from {} to {} (limit: {:?})", 
               symbol, timeframe, start, end, limit);

        // Ensure database is initialized before proceeding
        if let Err(e) = self.ensure_database_initialized(&symbol, timeframe) {
            error!("❌ Failed to initialize database for {} {}s: {}", symbol, timeframe, e);
            return LmdbActorResponse::ErrorResponse(format!("Database initialization error: {}", e));
        }

        match self.storage.get_candles(&symbol, timeframe, start, end, limit) {
            Ok(candles) => {
                debug!("✅ Retrieved {} candles", candles.len());
                LmdbActorResponse::Candles(candles)
            }
            Err(e) => {
                error!("❌ Failed to get candles: {}", e);
                LmdbActorResponse::ErrorResponse(format!("Failed to get candles: {}", e))
            }
        }
    }

    async fn handle_compact_database(&mut self, symbol: Option<String>) -> LmdbActorResponse {
        match symbol {
            Some(symbol) => {
                info!("🗜️ Compacting database for symbol: {}", symbol);
                match self.storage.compact_database(Some(&symbol)) {
                    Ok(()) => {
                        info!("✅ Database compaction completed for {}", symbol);
                        LmdbActorResponse::Success
                    }
                    Err(e) => {
                        error!("❌ Database compaction failed for {}: {}", symbol, e);
                        LmdbActorResponse::ErrorResponse(format!("Compaction failed: {}", e))
                    }
                }
            }
            None => {
                info!("🗜️ Compacting all databases");
                match self.storage.compact_database(None) {
                    Ok(()) => {
                        info!("✅ All database compaction completed");
                        LmdbActorResponse::Success
                    }
                    Err(e) => {
                        error!("❌ Database compaction failed: {}", e);
                        LmdbActorResponse::ErrorResponse(format!("Compaction failed: {}", e))
                    }
                }
            }
        }
    }

    async fn handle_get_storage_stats(&mut self) -> LmdbActorResponse {
        info!("📊 Getting storage statistics");

        match self.storage.get_storage_stats() {
            Ok(stats) => {
                info!("✅ Retrieved storage stats: {} symbols, {} total candles", 
                      stats.symbols.len(), stats.total_candles);
                
                // Convert internal stats to message stats
                let symbol_stats = stats.symbol_stats.into_iter().map(|s| {
                    crate::lmdb::messages::SymbolStats {
                        symbol: s.symbol,
                        timeframe: s.timeframe,
                        candle_count: s.candle_count,
                        earliest: s.earliest,
                        latest: s.latest,
                        size_bytes: s.size_bytes,
                    }
                }).collect();
                
                LmdbActorResponse::StorageStats {
                    symbols: stats.symbols,
                    total_candles: stats.total_candles,
                    db_size_bytes: stats.db_size_bytes,
                    symbol_stats,
                }
            }
            Err(e) => {
                error!("❌ Failed to get storage stats: {}", e);
                LmdbActorResponse::ErrorResponse(format!("Failed to get storage stats: {}", e))
            }
        }
    }

    async fn handle_initialize_database(
        &mut self,
        symbol: String,
        timeframe: u64,
    ) -> LmdbActorResponse {
        let interned_symbol = crate::common::shared_data::intern_symbol(&symbol);
        let key = (interned_symbol, timeframe);
        
        // Check if database is already initialized at actor level
        if self.initialized_dbs.contains(&key) {
            debug!("🔄 Database for {} {}s already initialized at actor level, skipping", symbol, timeframe);
            return LmdbActorResponse::Success;
        }

        info!("🔧 Initializing database for {} {}s", symbol, timeframe);

        match self.storage.initialize_database(&symbol, timeframe) {
            Ok(()) => {
                // Track that this database is now initialized
                self.initialized_dbs.insert(key);
                
                info!("✅ Database initialized for {} {}s", symbol, timeframe);
                LmdbActorResponse::Success
            }
            Err(e) => {
                error!("❌ Failed to initialize database for {} {}s: {}", symbol, timeframe, e);
                LmdbActorResponse::ErrorResponse(format!("Failed to initialize database: {}", e))
            }
        }
    }

    /// Handle checking if volume profile data exists
    #[cfg(feature = "volume_profile_reprocessing")]
    async fn handle_check_volume_profile_exists(
        &mut self,
        key: String,
    ) -> LmdbActorResponse {
        debug!("🔍 Checking volume profile existence for key: {}", key);
        
        // Use LMDB storage to check if the key exists
        // For now, we'll check in the standard candle database as volume profiles
        // are derived from candle data. In a full implementation, there would be
        // a separate volume profile database.
        
        // For this implementation, assume volume profile exists if we have candle data
        // for the corresponding symbol and date
        match self.storage.check_key_exists(&key) {
            Ok(exists) => {
                debug!("✅ Volume profile key {} exists: {}", key, exists);
                LmdbActorResponse::VolumeProfileExists { exists }
            }
            Err(e) => {
                error!("❌ Failed to check volume profile existence for {}: {}", key, e);
                LmdbActorResponse::ErrorResponse(format!("Failed to check existence: {}", e))
            }
        }
    }



}

impl Actor for LmdbActor {
    type Mailbox = UnboundedMailbox<Self>;

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        info!("🎭 LmdbActor started");
        Ok(())
    }

    async fn on_stop(&mut self, _actor_ref: kameo::actor::WeakActorRef<Self>, _reason: ActorStopReason) -> Result<(), BoxError> {
        info!("🎭 LmdbActor stopped");
        Ok(())
    }
}

impl Message<LmdbActorMessage> for LmdbActor {
    type Reply = LmdbActorResponse;

    async fn handle(
        &mut self,
        message: LmdbActorMessage,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        match message {
            LmdbActorMessage::ValidateDataRange { symbol, timeframe, start, end } => {
                self.handle_validate_data_range(symbol, timeframe, start, end).await
            }
            LmdbActorMessage::StoreCandles { symbol, timeframe, candles } => {
                self.handle_store_candles(symbol, timeframe, candles).await
            }
            LmdbActorMessage::GetDataRange { symbol, timeframe } => {
                self.handle_get_data_range(symbol, timeframe).await
            }
            LmdbActorMessage::GetCandles { symbol, timeframe, start, end, limit } => {
                self.handle_get_candles(symbol, timeframe, start, end, limit).await
            }
            LmdbActorMessage::CompactDatabase { symbol } => {
                self.handle_compact_database(symbol).await
            }
            LmdbActorMessage::GetStorageStats => {
                self.handle_get_storage_stats().await
            }
            LmdbActorMessage::InitializeDatabase { symbol, timeframe } => {
                self.handle_initialize_database(symbol, timeframe).await
            }
            #[cfg(feature = "volume_profile_reprocessing")]
            LmdbActorMessage::CheckVolumeProfileExists { key } => {
                self.handle_check_volume_profile_exists(key).await
            }
        }
    }
}

impl Message<LmdbActorTell> for LmdbActor {
    type Reply = ();

    async fn handle(
        &mut self,
        message: LmdbActorTell,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        match message {
            LmdbActorTell::StoreCandlesAsync { symbol, timeframe, candles } => {
                // Ensure database is initialized before proceeding
                if let Err(e) = self.ensure_database_initialized(&symbol, timeframe) {
                    error!("❌ Failed to initialize database for {} {}s: {}", symbol, timeframe, e);
                    return;
                }
                
                // Handle storage asynchronously without returning a response
                match self.storage.store_candles(&symbol, timeframe, &candles) {
                    Ok(stored_count) => {
                        if stored_count > 0 {
                            debug!("💾 Async stored {} candles for {} {}s", stored_count, symbol, timeframe);
                        }
                    }
                    Err(e) => {
                        error!("❌ Async storage failed for {} {}s: {}", symbol, timeframe, e);
                    }
                }
            }
        }
    }
}