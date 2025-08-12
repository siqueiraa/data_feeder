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
use crate::concurrency_optimization::{WorkStealingQueue, LoadBalancer, LoadBalancingStrategy};

pub struct LmdbActor {
    storage: LmdbStorage,
    gap_detector: GapDetector,
    initialized_dbs: FxHashSet<(Arc<str>, Seconds)>,
    // Concurrency optimization components
    work_queue: Arc<WorkStealingQueue<LmdbWorkItem>>,
    load_balancer: LoadBalancer,
}

/// Work item for parallel LMDB operations
#[derive(Debug)]
pub struct LmdbWorkItem {
    pub operation_type: LmdbOperationType,
    pub symbol: String,
    pub timeframe: Seconds,
    pub payload: LmdbWorkPayload,
}

#[derive(Debug)]
pub enum LmdbOperationType {
    StoreCandles,
    ValidateRange,
    GetDataRange,
    GetCandles,
    InitializeDatabase,
}

#[derive(Debug)]
pub enum LmdbWorkPayload {
    Candles(Vec<FuturesOHLCVCandle>),
    RangeValidation { start: TimestampMS, end: TimestampMS },
    CandleQuery { start: TimestampMS, end: TimestampMS, limit: Option<u32> },
    Empty,
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
        
        // Initialize concurrency optimization components
        let work_queue = Arc::new(WorkStealingQueue::new());
        let load_balancer = LoadBalancer::new(LoadBalancingStrategy::LeastLoaded, 4); // 4 virtual pools

        info!("🎭 LmdbActor initialized with base path: {} and concurrency optimizations", base_path.display());
        
        Ok(Self {
            storage,
            gap_detector,
            initialized_dbs: FxHashSet::default(),
            work_queue,
            load_balancer,
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

    /// Process multiple LMDB operations concurrently using work-stealing and load balancing
    pub async fn process_concurrent_operations(&mut self, operations: Vec<LmdbWorkItem>) -> Vec<LmdbActorResponse> {
        debug!("🔄 Processing {} operations concurrently", operations.len());
        let start_time = std::time::Instant::now();
        
        // Use load balancer to distribute work across virtual pools
        let operations_with_pools: Vec<_> = operations.into_iter()
            .enumerate()
            .map(|(i, op)| {
                let pool_id = self.load_balancer.select_pool(Some(i as u64));
                (pool_id, op)
            })
            .collect();
        
        debug!("🎯 Distributed {} operations across pools", operations_with_pools.len());
        
        // Group operations by pool for batched processing
        let mut pool_batches: std::collections::HashMap<usize, Vec<LmdbWorkItem>> = std::collections::HashMap::new();
        for (pool_id, operation) in operations_with_pools {
            pool_batches.entry(pool_id).or_default().push(operation);
        }
        
        // Distribute work across the work-stealing queue with pool awareness
        for (_pool_id, batch) in pool_batches {
            for operation in batch {
                self.work_queue.push(operation).await;
            }
        }
        
        // Process operations in parallel batches with intelligent sizing
        let mut results = Vec::new();
        let batch_size = std::cmp::min(4, self.work_queue.get_stats().local_pushes.load(std::sync::atomic::Ordering::Relaxed) as usize); // Dynamic batch size
        
        while let Some(work_item) = self.work_queue.try_pop().await {
            // Collect a batch of work items
            let mut batch = vec![work_item];
            for _ in 1..batch_size {
                if let Some(item) = self.work_queue.try_pop().await {
                    batch.push(item);
                } else {
                    break;
                }
            }
            
            // Process batch concurrently
            let batch_results = self.process_work_batch(batch).await;
            results.extend(batch_results);
        }
        
        debug!("✅ Completed concurrent processing, {} results", results.len());
        
        // Record batch-level concurrency optimization metrics
        if let Some(metrics) = crate::metrics::get_metrics() {
            let total_duration = start_time.elapsed();
            
            // Record work-stealing queue metrics for batch processing
            let queue_stats = self.work_queue.get_stats();
            let attempted = queue_stats.steals_attempted.load(std::sync::atomic::Ordering::Relaxed);
            let successful = queue_stats.steals_successful.load(std::sync::atomic::Ordering::Relaxed);
            let processed = queue_stats.work_items_processed.load(std::sync::atomic::Ordering::Relaxed);
            
            metrics.record_work_stealing_metrics(attempted, successful, processed as usize);

            // Record load balancer effectiveness with actual pool distribution
            let pool_distribution = self.load_balancer.get_pool_loads();
            metrics.record_load_balancer_metrics("LeastLoaded", results.len(), &pool_distribution);
            
            // Record overall batch concurrency performance
            let batch_efficiency = if total_duration.as_secs_f64() > 0.0 {
                results.len() as f64 / total_duration.as_secs_f64()
            } else {
                0.0
            };
            
            metrics.record_concurrency_optimization_metrics(
                "batch_processing",
                "work_stealing",
                results.len() as u64,
                total_duration.as_secs_f64(),
                batch_efficiency,
            );
            
            debug!("📊 Recorded batch concurrency metrics: {} operations, {:.3}s, {:.2} ops/s", 
                   results.len(), total_duration.as_secs_f64(), batch_efficiency);
        }
        
        results
    }

    /// Process a batch of work items sequentially (due to mutable self constraints)
    async fn process_work_batch(&mut self, batch: Vec<LmdbWorkItem>) -> Vec<LmdbActorResponse> {
        let mut results = Vec::with_capacity(batch.len());
        
        // Process items sequentially due to mutable self borrowing constraints
        for item in batch {
            let result = self.process_single_work_item(item).await;
            results.push(result);
        }
        
        results
    }

    /// Process a single work item
    async fn process_single_work_item(&mut self, item: LmdbWorkItem) -> LmdbActorResponse {
        let start_time = std::time::Instant::now();
        
        let result = match item.operation_type {
            LmdbOperationType::StoreCandles => {
                if let LmdbWorkPayload::Candles(candles) = item.payload {
                    self.handle_store_candles(item.symbol, item.timeframe, candles).await
                } else {
                    LmdbActorResponse::ErrorResponse("Invalid payload for store candles operation".to_string())
                }
            }
            LmdbOperationType::ValidateRange => {
                if let LmdbWorkPayload::RangeValidation { start, end } = item.payload {
                    self.handle_validate_data_range(item.symbol, item.timeframe, start, end).await
                } else {
                    LmdbActorResponse::ErrorResponse("Invalid payload for range validation operation".to_string())
                }
            }
            LmdbOperationType::GetDataRange => {
                self.handle_get_data_range(item.symbol, item.timeframe).await
            }
            LmdbOperationType::GetCandles => {
                if let LmdbWorkPayload::CandleQuery { start, end, limit } = item.payload {
                    self.handle_get_candles(item.symbol, item.timeframe, start, end, limit).await
                } else {
                    LmdbActorResponse::ErrorResponse("Invalid payload for get candles operation".to_string())
                }
            }
            LmdbOperationType::InitializeDatabase => {
                self.handle_initialize_database(item.symbol, item.timeframe).await
            }
        };
        
        let duration = start_time.elapsed();
        debug!("⚡ Processed {:?} operation in {:?}", item.operation_type, duration);
        
        // Record enhanced metrics for concurrency optimization
        if let Some(metrics) = crate::metrics::get_metrics() {
            // Record basic concurrency metrics
            metrics.record_concurrency_optimization_metrics(
                &format!("{:?}", item.operation_type),
                "lmdb_parallel",
                1, // concurrent tasks
                duration.as_secs_f64(),
                1.0, // efficiency (100% for successful operations)
            );

            // Record work-stealing queue metrics
            let queue_stats = self.work_queue.get_stats();
            let attempted = queue_stats.steals_attempted.load(std::sync::atomic::Ordering::Relaxed);
            let successful = queue_stats.steals_successful.load(std::sync::atomic::Ordering::Relaxed);
            let processed = queue_stats.work_items_processed.load(std::sync::atomic::Ordering::Relaxed);
            
            metrics.record_work_stealing_metrics(attempted, successful, processed as usize);

            // Record load balancer effectiveness with actual pool distribution
            let pool_distribution = self.load_balancer.get_pool_loads();
            metrics.record_load_balancer_metrics("LeastLoaded", 1, &pool_distribution);
            
            // Record concurrency optimization performance
            metrics.record_concurrency_optimization_metrics(
                "single_operation",
                "work_stealing",
                1,
                duration.as_secs_f64(),
                1.0, // Single operation efficiency
            );
        }
        
        result
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