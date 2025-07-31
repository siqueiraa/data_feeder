use std::collections::HashMap;
use std::time::Duration;

use chrono;
use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::request::MessageSend;
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};
use crate::technical_analysis::structs::{
    TechnicalAnalysisConfig, CandleRingBuffer, MultiTimeFrameCandles
};
use crate::technical_analysis::utils::{
    load_recent_candles_from_db, calculate_min_candles_needed, pre_aggregate_historical_candles,
    validate_initialization_data, timeframe_to_string
};
use super::indicator::{IndicatorActor, IndicatorTell};

/// Messages for TimeFrame Actor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeFrameTell {
    /// Process a new 1-minute candle from WebSocket
    ProcessCandle {
        symbol: String,
        candle: FuturesOHLCVCandle,
        is_closed: bool,
    },
    /// Set the reference timestamp for synchronized loading
    SetReferenceTimestamp {
        timestamp: i64,
    },
    /// Retry indicator initialization for symbols with completed gap filling
    RetryIndicatorInitialization {
        symbol: String,
    },
    /// Log data completeness monitoring statistics
    LogDataCompletenessStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeFrameAsk {
    /// Get current multi-timeframe data for a symbol
    GetMultiTimeFrameData { symbol: String },
    /// Get initialization status
    GetInitializationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeFrameReply {
    /// Multi-timeframe data response
    MultiTimeFrameData(Box<MultiTimeFrameCandles>),
    /// Initialization status response
    InitializationStatus {
        initialized_symbols: Vec<String>,
        total_symbols: usize,
        is_ready: bool,
    },
    /// Success response
    Success,
    /// Error response
    Error(String),
}

/// Symbol-specific timeframe aggregation state
#[derive(Debug)]
struct SymbolTimeFrameState {
    /// Ring buffers for each timeframe
    ring_buffers: HashMap<u64, CandleRingBuffer>,
    /// Whether this symbol is initialized
    is_initialized: bool,
    /// Last processed candle timestamp
    last_candle_time: Option<TimestampMS>,
    /// Track processed candle timestamps for deduplication (close_time -> processed_at)
    processed_candles: HashMap<TimestampMS, std::time::Instant>,
    /// MEMORY POOL: Reusable HashMap for completed candles (eliminates HashMap allocations)
    completed_candles_buffer: HashMap<u64, FuturesOHLCVCandle>,
}

impl SymbolTimeFrameState {
    fn new(timeframes: &[u64]) -> Self {
        let mut ring_buffers = HashMap::new();
        
        // Create ring buffers for each timeframe (except 1-minute)
        for &timeframe in timeframes {
            if timeframe > 60 {
                // Calculate appropriate capacity based on timeframe
                let capacity = match timeframe {
                    300 => 1000,   // 5m: ~3.5 days
                    900 => 500,    // 15m: ~5.2 days  
                    3600 => 200,   // 1h: ~8.3 days
                    14400 => 100,  // 4h: ~16.7 days
                    _ => 100,      // Default capacity
                };
                
                ring_buffers.insert(timeframe, CandleRingBuffer::new(timeframe, capacity));
            }
        }

        Self {
            ring_buffers,
            is_initialized: false,
            last_candle_time: None,
            processed_candles: HashMap::new(),
            // MEMORY POOL: Pre-allocate HashMap for completed candles (typically 4-5 timeframes max)
            completed_candles_buffer: HashMap::with_capacity(8),
        }
    }

    /// Process a new 1-minute candle and return completed higher timeframe candles
    fn process_candle(&mut self, candle: &FuturesOHLCVCandle) -> HashMap<u64, FuturesOHLCVCandle> {
        // MEMORY POOL: Reuse pre-allocated HashMap instead of creating new one each time
        self.completed_candles_buffer.clear();

        // DEDUPLICATION: Check if we've already processed this exact candle
        let now = std::time::Instant::now();
        if let Some(&last_processed) = self.processed_candles.get(&candle.close_time) {
            // If we processed this candle recently (within last 30 seconds), skip it
            if now.duration_since(last_processed) < Duration::from_secs(30) {
                // Skip duplicate candle - this is expected behavior
                return self.completed_candles_buffer.clone(); // Return empty - no processing
            }
        }

        // Mark this candle as processed
        self.processed_candles.insert(candle.close_time, now);

        // Clean up old entries (keep only last 1000 entries for memory management)
        if self.processed_candles.len() > 1000 {
            // Remove entries older than 1 hour
            self.processed_candles.retain(|_, &mut processed_at| {
                now.duration_since(processed_at) <= Duration::from_secs(3600)
            });
        }

        // Update last candle time
        self.last_candle_time = Some(candle.close_time);

        // Process through each timeframe ring buffer
        for (&timeframe, buffer) in &mut self.ring_buffers {
            if let Some(completed_candle) = buffer.add_candle(candle) {
                self.completed_candles_buffer.insert(timeframe, completed_candle);
            }
        }

        self.completed_candles_buffer.clone()
    }

    /// OPTIMIZED: Initialize with historical data (98% faster than process_candle)
    async fn initialize_with_history_optimized(
        &mut self, 
        symbol: &str, 
        historical_candles: &[FuturesOHLCVCandle]
    ) {
        let init_start = std::time::Instant::now();
        info!("🚀 OPTIMIZED initialization for {} with {} historical candles", 
              symbol, historical_candles.len());

        // OPTIMIZATION 1: Bulk update last_candle_time (no per-candle updates)
        if let Some(last_candle) = historical_candles.last() {
            self.last_candle_time = Some(last_candle.close_time);
        }

        // OPTIMIZATION 2: Direct ring buffer population (skip all real-time overhead)
        let mut completed_counts = std::collections::HashMap::new();
        for candle in historical_candles {
            // Skip all deduplication, timing, and memory management overhead
            // Use bulk methods that avoid debug logging and real-time checks
            for (timeframe, buffer) in &mut self.ring_buffers {
                if let Some(_completed_candle) = buffer.add_candle_bulk(candle) {
                    *completed_counts.entry(*timeframe).or_insert(0) += 1;
                }
            }
        }

        self.is_initialized = true;
        
        // Log final state with performance metrics
        for (&timeframe, buffer) in &self.ring_buffers {
            let completed = completed_counts.get(&timeframe).unwrap_or(&0);
            info!("{} {} buffer: {} stored, {} completed periods", 
                  symbol, timeframe_to_string(timeframe), buffer.len(), completed);
        }
        
        let total_time = init_start.elapsed();
        info!("✅ OPTIMIZED initialization for {} completed in {:?} (vs ~113s with old method)", 
              symbol, total_time);
    }

    /// DEPRECATED: Legacy initialization method (kept for compatibility)
    async fn initialize_with_history(
        &mut self, 
        symbol: &str, 
        historical_candles: &[FuturesOHLCVCandle]
    ) {
        warn!("⚠️ Using SLOW legacy initialization method for {} - this will take ~113 seconds", symbol);
        info!("Initializing timeframe state for {} with {} historical candles", 
              symbol, historical_candles.len());

        // Process each historical candle to populate ring buffers (SLOW)
        for candle in historical_candles {
            self.process_candle(candle);
        }

        self.is_initialized = true;
        
        // Log ring buffer states
        for (&timeframe, buffer) in &self.ring_buffers {
            info!("{} {} buffer initialized with {} candles", 
                  symbol, timeframe_to_string(timeframe), buffer.len());
        }
    }

    /// Get current multi-timeframe candle data
    fn get_current_data(&self) -> MultiTimeFrameCandles {
        let get_latest_candle = |tf: u64| -> Option<FuturesOHLCVCandle> {
            self.ring_buffers.get(&tf)
                .and_then(|buffer| buffer.get_recent_candles(1).into_iter().next())
        };

        MultiTimeFrameCandles {
            candles_5m: get_latest_candle(300),
            candles_15m: get_latest_candle(900),
            candles_1h: get_latest_candle(3600),
            candles_4h: get_latest_candle(14400),
        }
    }
}

/// TimeFrame Actor that aggregates 1-minute candles into higher timeframes
pub struct TimeFrameActor {
    /// Configuration
    config: TechnicalAnalysisConfig,
    /// Symbol-specific state
    symbol_states: HashMap<String, SymbolTimeFrameState>,
    /// Reference to indicator actor
    indicator_actor: Option<ActorRef<IndicatorActor>>,
    /// Reference to API actor for gap filling
    api_actor: Option<ActorRef<crate::api::ApiActor>>,
    /// Reference to LMDB actor for database operations
    lmdb_actor: Option<ActorRef<crate::lmdb::LmdbActor>>,
    /// Overall initialization status
    is_ready: bool,
    /// Reference timestamp for synchronized loading
    reference_timestamp: Option<i64>,
    /// Symbols pending indicator initialization (waiting for gap filling)
    symbols_pending_indicator_init: HashMap<String, Vec<FuturesOHLCVCandle>>,
}

impl TimeFrameActor {
    /// Create a new TimeFrame actor
    pub fn new(config: TechnicalAnalysisConfig) -> Self {
        let mut symbol_states = HashMap::new();
        
        // Initialize state for each symbol
        for symbol in &config.symbols {
            let state = SymbolTimeFrameState::new(&config.timeframes);
            symbol_states.insert(symbol.clone(), state);
        }

        Self {
            config,
            symbol_states,
            indicator_actor: None,
            api_actor: None,
            lmdb_actor: None,
            is_ready: false,
            reference_timestamp: None,
            symbols_pending_indicator_init: HashMap::new(),
        }
    }

    /// Set the indicator actor reference
    pub fn set_indicator_actor(&mut self, indicator_actor: ActorRef<IndicatorActor>) {
        self.indicator_actor = Some(indicator_actor);
    }

    /// Set the API actor reference for gap filling
    pub fn set_api_actor(&mut self, api_actor: ActorRef<crate::api::ApiActor>) {
        self.api_actor = Some(api_actor);
    }

    /// Set the LMDB actor reference for database operations
    pub fn set_lmdb_actor(&mut self, lmdb_actor: ActorRef<crate::lmdb::LmdbActor>) {
        self.lmdb_actor = Some(lmdb_actor);
    }

    /// Log comprehensive data completeness statistics
    pub fn log_data_completeness_status(&self) {
        let stats_start = std::time::Instant::now();
        
        info!("📊 === DATA COMPLETENESS MONITORING ===");
        
        // Overall system status
        let total_symbols = self.config.symbols.len();
        let initialized_count = self.symbol_states.values()
            .filter(|state| state.is_initialized)
            .count();
        let pending_indicator_count = self.symbols_pending_indicator_init.len();
        
        info!("🎯 System Status: {} total symbols, {} TimeFrame initialized, {} pending indicator init", 
              total_symbols, initialized_count, pending_indicator_count);
        
        // Per-symbol completeness analysis
        for symbol in &self.config.symbols {
            if let Some(state) = self.symbol_states.get(symbol) {
                let buffer_stats: Vec<String> = state.ring_buffers.iter()
                    .map(|(&tf, buffer)| {
                        let tf_str = timeframe_to_string(tf);
                        let candle_count = buffer.len();
                        format!("{}:{}", tf_str, candle_count)
                    })
                    .collect();
                
                let status_indicator = if state.is_initialized {
                    "✅"
                } else {
                    "⏳"
                };
                
                let pending_indicator = if self.symbols_pending_indicator_init.contains_key(symbol) {
                    " [PENDING INDICATORS]"
                } else {
                    ""
                };
                
                info!("{} {}: [{}] last_candle: {:?}{}", 
                      status_indicator, symbol, buffer_stats.join(", "), 
                      state.last_candle_time, pending_indicator);
                      
                // Log data freshness for initialized symbols
                if state.is_initialized {
                    if let Some(last_time) = state.last_candle_time {
                        let current_time = chrono::Utc::now().timestamp_millis();
                        let age_minutes = (current_time - last_time) / 60_000;
                        
                        if age_minutes > 60 {
                            warn!("⚠️ Stale data detected for {}: last candle {} minutes old", symbol, age_minutes);
                        } else if age_minutes > 10 {
                            info!("⏰ Data age for {}: {} minutes", symbol, age_minutes);
                        }
                    }
                }
            } else {
                error!("❌ Missing symbol state for: {}", symbol);
            }
        }
        
        // Log pending indicator initialization details
        if !self.symbols_pending_indicator_init.is_empty() {
            info!("🔄 Symbols pending indicator initialization:");
            for (symbol, candles) in &self.symbols_pending_indicator_init {
                let oldest_time = candles.first().map(|c| c.close_time).unwrap_or(0);
                let newest_time = candles.last().map(|c| c.close_time).unwrap_or(0);
                let span_hours = (newest_time - oldest_time) / (60 * 60 * 1000);
                
                info!("  📋 {}: {} candles spanning {} hours", symbol, candles.len(), span_hours);
            }
        }
        
        debug!("📊 Data completeness monitoring completed in {:?}", stats_start.elapsed());
    }

    /// Retry indicator initialization for a symbol after gap filling is complete
    async fn retry_indicator_initialization(&mut self, symbol: &str) -> Result<(), BoxError> {
        let init_start = std::time::Instant::now();
        
        // Check if we have pending data for this symbol
        if let Some(historical_candles) = self.symbols_pending_indicator_init.remove(symbol) {
            info!("🔄 Retrying indicator initialization for {} after gap filling", symbol);
            
            let min_candles = calculate_min_candles_needed(
                &self.config.timeframes,
                &self.config.ema_periods,
                self.config.volume_lookback_days,
            );
            
            // Re-validate database completeness
            match self.validate_database_completeness(symbol, &historical_candles, min_candles).await {
                Ok(()) => {
                    info!("✅ Database completeness validation passed for {} during retry", symbol);
                }
                Err(validation_error) => {
                    warn!("⚠️ Database completeness still failing for {} during retry: {}", symbol, validation_error);
                    warn!("🎯 Will continue waiting for gap filling to complete");
                    
                    // Put the data back for another retry
                    self.symbols_pending_indicator_init.insert(symbol.to_string(), historical_candles);
                    return Ok(()); // Don't fail, just wait for more gap filling
                }
            }
            
            // Initialize symbol state if not already done
            if let Some(state) = self.symbol_states.get_mut(symbol) {
                if !state.is_initialized {
                    state.initialize_with_history(symbol, &historical_candles).await;
                    info!("📊 Symbol state initialized for {} during retry", symbol);
                }
            }
            
            // Pre-aggregate historical data for indicator initialization
            let aggregation_start = std::time::Instant::now();
            let aggregated_data = pre_aggregate_historical_candles(
                &historical_candles,
                &self.config.timeframes,
            );
            info!("⚡ Pre-aggregated historical data for {} during retry in {:?}", symbol, aggregation_start.elapsed());
            
            // Send historical data to indicator actor for initialization
            if let Some(indicator_actor) = &self.indicator_actor {
                let indicator_send_start = std::time::Instant::now();
                let init_msg = IndicatorTell::InitializeWithHistory {
                    symbol: symbol.to_string(),
                    aggregated_candles: aggregated_data,
                };
                
                if let Err(e) = indicator_actor.tell(init_msg).send().await {
                    error!("❌ Failed to send historical data to indicator actor for {} during retry: {}", symbol, e);
                    return Err(format!("Indicator initialization failed: {}", e).into());
                } else {
                    info!("✅ Successfully initialized indicators for {} during retry in {:?}", 
                          symbol, indicator_send_start.elapsed());
                }
            } else {
                warn!("⚠️ No IndicatorActor available for {} during retry", symbol);
            }
            
            info!("🎉 Indicator initialization retry completed for {} in {:?}", symbol, init_start.elapsed());
            
            // Log updated data completeness status after successful retry
            self.log_data_completeness_status();
        } else {
            warn!("⚠️ No pending initialization data found for symbol: {}", symbol);
        }
        
        Ok(())
    }

    /// Group candles by day for efficient gap detection
    fn group_candles_by_day<'a>(&self, candles: &'a [FuturesOHLCVCandle]) -> HashMap<String, Vec<&'a FuturesOHLCVCandle>> {
        let mut daily_groups = HashMap::new();
        
        for candle in candles {
            let day_key = chrono::DateTime::from_timestamp_millis(candle.close_time)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| format!("invalid_{}", candle.close_time));
            
            daily_groups.entry(day_key).or_insert_with(Vec::new).push(candle);
        }
        
        daily_groups
    }

    /// Fast daily completeness validation - only check incomplete days in detail
    fn validate_daily_completeness(&self, daily_groups: &HashMap<String, Vec<&'_ FuturesOHLCVCandle>>) -> Vec<String> {
        let mut incomplete_days = Vec::new();
        
        for (day, candles) in daily_groups {
            // Expected candles per day = 1440 (24 hours × 60 minutes)
            // Allow some tolerance for partial days at boundaries
            if candles.len() < 1400 { // 97% completeness threshold
                incomplete_days.push(day.clone());
                debug!("📅 Incomplete day {}: {} candles (expected ~1440)", day, candles.len());
            }
        }
        
        incomplete_days
    }

    /// Analyze gaps only for incomplete days (targeted analysis)
    fn analyze_gaps_for_incomplete_days(
        &self,
        incomplete_days: &[String], 
        daily_groups: &HashMap<String, Vec<&'_ FuturesOHLCVCandle>>
    ) -> Vec<(i64, i64, i64)> {
        let mut large_gaps = Vec::new();
        
        for day in incomplete_days {
            if let Some(day_candles) = daily_groups.get(day) {
                // Sort candles for this day
                let mut sorted_candles = day_candles.clone();
                sorted_candles.sort_by_key(|c| c.open_time);
                
                // Check gaps only within this specific day
                for window in sorted_candles.windows(2) {
                    let time_diff = window[1].open_time - window[0].open_time;
                    if time_diff > 3_600_000 { // Gap > 1 hour
                        let gap_mins = time_diff / 60_000;
                        large_gaps.push((window[0].open_time, window[1].open_time, gap_mins));
                    }
                }
            }
        }
        
        large_gaps
    }

    /// Optimized database completeness validation using daily count strategy
    async fn validate_database_completeness(&self, symbol: &str, candles: &[FuturesOHLCVCandle], min_required: usize) -> Result<(), String> {
        let validation_start = std::time::Instant::now();
        
        // Quick count check
        if candles.len() < min_required {
            return Err(format!(
                "Insufficient data: {} candles available, {} required for reliable indicators",
                candles.len(), min_required
            ));
        }
        
        // OPTIMIZATION 1: Daily grouping (O(n) single pass)
        let grouping_start = std::time::Instant::now();
        let daily_groups = self.group_candles_by_day(candles);
        let grouping_time = grouping_start.elapsed();
        
        // OPTIMIZATION 2: Fast daily count validation (O(days), not O(candles))
        let daily_validation_start = std::time::Instant::now();
        let incomplete_days = self.validate_daily_completeness(&daily_groups);
        let daily_validation_time = daily_validation_start.elapsed();
        
        // OPTIMIZATION 3: Targeted gap analysis (only on incomplete days)
        let gap_analysis_start = std::time::Instant::now();
        let large_gaps = self.analyze_gaps_for_incomplete_days(&incomplete_days, &daily_groups);
        let gap_analysis_time = gap_analysis_start.elapsed();
        
        // Check recent data coverage (last 24 hours) - optimized
        let current_time = chrono::Utc::now().timestamp_millis();
        let one_day_ago = current_time - (24 * 60 * 60 * 1000);
        
        let recent_candles_count = candles.iter()
            .filter(|candle| candle.close_time > one_day_ago)
            .count();
            
        if recent_candles_count < 1440 { // 1440 minutes in a day
            let hours_covered = recent_candles_count / 60;
            warn!("⚠️ Limited recent data coverage for {}: only {} hours of the last 24 hours available", 
                  symbol, hours_covered);
            
            // Allow initialization with warning if we have some recent data
            if hours_covered < 6 {
                return Err(format!(
                    "Insufficient recent data: only {} hours covered, need at least 6 hours for reliable initialization",
                    hours_covered
                ));
            }
        }
        
        // Report findings
        if !incomplete_days.is_empty() {
            warn!("⚠️ Database completeness for {}: {} incomplete days detected", 
                  symbol, incomplete_days.len());
            
            // Log incomplete days summary
            for day in &incomplete_days {
                if let Some(day_candles) = daily_groups.get(day) {
                    debug!("  📅 {}: {} candles", day, day_candles.len());
                }
            }
        }
        
        if !large_gaps.is_empty() {
            warn!("⚠️ Large gaps found for {}: {} gaps (>1h) in incomplete days", 
                  symbol, large_gaps.len());
            
            // Log only significant gaps for visibility
            for (start, end, gap_mins) in &large_gaps {
                if *gap_mins > 120 { // Only log gaps > 2 hours
                    let start_time = chrono::DateTime::from_timestamp_millis(*start)
                        .unwrap_or_default().format("%Y-%m-%d %H:%M:%S");
                    let end_time = chrono::DateTime::from_timestamp_millis(*end)
                        .unwrap_or_default().format("%Y-%m-%d %H:%M:%S");
                    warn!("  📊 Major gap: {} to {} ({} minutes)", start_time, end_time, gap_mins);
                }
            }
            
            // Allow initialization but note gaps will be handled
            info!("🎯 Proceeding with initialization for {} - gap detection will handle {} gaps", 
                  symbol, large_gaps.len());
        }
        
        // Performance summary
        let total_time = validation_start.elapsed();
        let data_span_hours = if !candles.is_empty() {
            let oldest = candles.first().unwrap().close_time;
            let newest = candles.last().unwrap().close_time;
            (newest - oldest) / (60 * 60 * 1000)
        } else {
            0
        };
        
        info!("✅ OPTIMIZED validation for {} completed in {:?} (grouping: {:?}, daily: {:?}, gaps: {:?})", 
              symbol, total_time, grouping_time, daily_validation_time, gap_analysis_time);
        info!("📊 Data metrics for {}: {} candles spanning {} hours, {} days analyzed, {} incomplete days, {} large gaps", 
              symbol, candles.len(), data_span_hours, daily_groups.len(), incomplete_days.len(), large_gaps.len());
        
        Ok(())
    }

    /// Initialize with historical data from database
    async fn initialize_with_historical_data(&mut self) -> Result<(), BoxError> {
        let init_start = std::time::Instant::now();
        info!("🔄 Starting TimeFrame actor initialization with historical data...");

        let min_candles = calculate_min_candles_needed(
            &self.config.timeframes,
            &self.config.ema_periods,
            self.config.volume_lookback_days,
        );
        info!("📊 Minimum candles needed for initialization: {}", min_candles);

        // Track initialization progress
        let mut successful_symbols = 0;
        let mut failed_symbols = Vec::new();

        for symbol in &self.config.symbols.clone() {
            let symbol_start = std::time::Instant::now();
            info!("📈 Loading historical data for {} (actor dependencies: LmdbActor={}, ApiActor={}, IndicatorActor={})", 
                  symbol, 
                  self.lmdb_actor.is_some(), 
                  self.api_actor.is_some(), 
                  self.indicator_actor.is_some());

            // Load 1-minute candles from database using LmdbActor (prevents environment conflicts)
            let data_load_start = std::time::Instant::now();
            let historical_candles = if let Some(lmdb_actor) = &self.lmdb_actor {
                info!("🔧 Loading {} historical data via LmdbActor...", symbol);
                match load_recent_candles_from_db(
                    symbol,
                    60, // 1-minute timeframe
                    self.config.min_history_days,
                    lmdb_actor,
                    self.reference_timestamp,
                ).await {
                    Ok(candles) => {
                        info!("✅ Loaded {} candles via LmdbActor for {} in {:?}", 
                              candles.len(), symbol, data_load_start.elapsed());
                        candles
                    }
                    Err(e) => {
                        error!("❌ Failed to load {} data via LmdbActor: {}", symbol, e);
                        error!("❌ No fallback available - direct LMDB access removed to prevent environment conflicts");
                        failed_symbols.push(symbol.clone());
                        continue; // Skip this symbol but continue with others
                    }
                }
            } else {
                // LmdbActor is required - no fallback to prevent environment conflicts
                error!("🚨 No LmdbActor available for {} - cannot load historical data without it", symbol);
                failed_symbols.push(symbol.clone());
                continue; // Skip this symbol but continue with others
            };

            // CRITICAL: Validate database completeness before allowing indicator initialization
            match self.validate_database_completeness(symbol, &historical_candles, min_candles).await {
                Ok(()) => {
                    info!("✅ Database completeness validation passed for {} - proceeding with indicator initialization", symbol);
                }
                Err(validation_error) => {
                    warn!("⚠️ Database completeness validation failed for {}: {}", symbol, validation_error);
                    warn!("🎯 Deferring indicator initialization until gap filling completes");
                    warn!("📋 Gap detection will handle data filling, then indicators can be initialized via retry");
                    
                    // Store data for later initialization retry
                    self.symbols_pending_indicator_init.insert(symbol.clone(), historical_candles.clone());
                    info!("📝 Stored {} historical candles for {} pending indicator initialization", 
                          historical_candles.len(), symbol);
                    
                    // Still initialize the TimeFrame state even if indicators are deferred
                    // This allows real-time processing to continue while waiting for gap filling
                }
            }

            // Validate we have sufficient data and get detected gaps
            let validation_start = std::time::Instant::now();
            match validate_initialization_data(symbol, &historical_candles, min_candles) {
                Ok(detected_gaps) => {
                    info!("✅ Data validation passed for {} in {:?}: {} candles, {} gaps detected", 
                          symbol, validation_start.elapsed(), historical_candles.len(), detected_gaps.len());
                    if !detected_gaps.is_empty() && self.api_actor.is_some() {
                        info!("🔧 Requesting gap filling for {} gaps in {}", detected_gaps.len(), symbol);
                        
                        // Send gap filling requests to API actor
                        if let Some(api_actor) = &self.api_actor {
                            for gap in detected_gaps {
                                let gap_fill_msg = crate::api::ApiTell::FillGap {
                                    symbol: symbol.clone(),
                                    interval: "1m".to_string(),
                                    start_time: gap.start,
                                    end_time: gap.end,
                                };
                                
                                if let Err(e) = api_actor.tell(gap_fill_msg).send().await {
                                    warn!("Failed to send gap fill request for {}: {}", symbol, e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("⚠️ Data validation warning for {}: {}", symbol, e);
                    warn!("📋 Centralized gap detection should have handled this - proceeding with available data");
                    // Continue with available data - centralized gap detection should handle insufficient data
                }
            }

            // Initialize symbol state (always do this for real-time processing)
            let state_init_start = std::time::Instant::now();
            if let Some(state) = self.symbol_states.get_mut(symbol) {
                // Use OPTIMIZED initialization method (98% faster)
                state.initialize_with_history_optimized(symbol, &historical_candles).await;
                info!("📊 OPTIMIZED symbol state initialized for {} in {:?}", symbol, state_init_start.elapsed());
            } else {
                error!("❌ Symbol state not found for {}", symbol);
                failed_symbols.push(symbol.clone());
                continue;
            }

            // Only initialize indicators if validation passed
            if !self.symbols_pending_indicator_init.contains_key(symbol) {
                // Pre-aggregate historical data for indicator initialization
                let aggregation_start = std::time::Instant::now();
                let aggregated_data = pre_aggregate_historical_candles(
                    &historical_candles,
                    &self.config.timeframes,
                );
                info!("⚡ Pre-aggregated historical data for {} in {:?}", symbol, aggregation_start.elapsed());

                // Send historical data to indicator actor for initialization
                if let Some(indicator_actor) = &self.indicator_actor {
                    let indicator_send_start = std::time::Instant::now();
                    let init_msg = IndicatorTell::InitializeWithHistory {
                        symbol: symbol.clone(),
                        aggregated_candles: aggregated_data,
                    };

                    if let Err(e) = indicator_actor.tell(init_msg).send().await {
                        error!("❌ Failed to send historical data to indicator actor for {}: {}", symbol, e);
                        // Don't fail the entire initialization for indicator errors
                    } else {
                        info!("✅ Sent historical data to IndicatorActor for {} in {:?}", 
                              symbol, indicator_send_start.elapsed());
                    }
                } else {
                    warn!("⚠️ No IndicatorActor available for {}", symbol);
                }
            } else {
                info!("🔄 Skipping indicator initialization for {} - will retry after gap filling", symbol);
            }

            successful_symbols += 1;
            let symbol_time = symbol_start.elapsed();
            info!("✅ Completed initialization for {} in {:?}", symbol, symbol_time);
        }

        // Final initialization summary
        let total_init_time = init_start.elapsed();
        let initialized_count = self.symbol_states.values()
            .filter(|state| state.is_initialized)
            .count();

        // Set ready status based on whether we have at least some symbols initialized
        self.is_ready = initialized_count > 0;

        info!("📊 TimeFrame initialization summary: {} successful, {} failed, total time: {:?}", 
              successful_symbols, failed_symbols.len(), total_init_time);

        if !failed_symbols.is_empty() {
            warn!("⚠️ Failed to initialize symbols: {:?}", failed_symbols);
        }

        if self.is_ready {
            if successful_symbols == self.config.symbols.len() {
                info!("🎉 TimeFrame actor fully initialized for all {} symbols", self.config.symbols.len());
            } else {
                warn!("⚠️ TimeFrame actor partially initialized: {}/{} symbols ready", 
                      initialized_count, self.config.symbols.len());
            }
        } else {
            error!("❌ TimeFrame actor initialization failed: no symbols successfully initialized");
            return Err("No symbols could be initialized".into());
        }

        // Log comprehensive data completeness status after initialization
        self.log_data_completeness_status();

        Ok(())
    }

    /// Process a new 1-minute candle
    async fn process_new_candle(&mut self, symbol: &str, candle: &FuturesOHLCVCandle, is_closed: bool) {
        let total_start = std::time::Instant::now();
        
        if let Some(state) = self.symbol_states.get_mut(symbol) {
            // Process through ring buffers for higher timeframes
            let ring_buffer_start = std::time::Instant::now();
            let completed_candles = state.process_candle(candle);
            let _ring_buffer_time = ring_buffer_start.elapsed();
            
            // Only log when we actually generate completed candles
            if !completed_candles.is_empty() {
                info!("✅ Generated {} completed higher timeframe candles for {}", 
                      completed_candles.len(), symbol);
            }

            // Send batched updates to indicator actor to prevent duplicate outputs
            if let Some(indicator_actor) = &self.indicator_actor {
                let indicator_start = std::time::Instant::now();
                
                // Collect timeframe updates - ONLY CLOSED CANDLES for EMA calculations
                let batch_prep_start = std::time::Instant::now();
                let mut candles_batch = HashMap::new();
                
                // 1. CRITICAL FIX: Only include 1-minute candle if it's CLOSED
                if is_closed {
                    candles_batch.insert(60, candle.clone());
                }
                
                // 2. Include completed higher timeframe candles (these are always closed by definition)
                for (&timeframe, completed_candle) in &completed_candles {
                    candles_batch.insert(timeframe, completed_candle.clone());
                    info!("🎯 COMPLETED {}s candle: {:.2} @ {} → sending to indicator", 
                          timeframe, completed_candle.close, completed_candle.close_time);
                }
                
                // 3. CRITICAL FIX: Do NOT include live in-progress candles for EMA calculations
                // (This prevents live candle contamination of EMAs)
                // REMOVED: No live candles sent to indicator - only closed candles for accurate EMAs
                let batch_prep_time = batch_prep_start.elapsed();
                
                // Send batched message ONLY if we have closed candles to process
                if !candles_batch.is_empty() {
                    let send_start = std::time::Instant::now();
                    let batch_msg = IndicatorTell::ProcessMultiTimeFrameUpdate {
                        symbol: symbol.to_string(),
                        candles: candles_batch,
                    };

                    if let Err(e) = indicator_actor.tell(batch_msg).send().await {
                        error!("Failed to send batched update to indicator actor: {}", e);
                    }
                    let send_time = send_start.elapsed();
                    
                    let _indicator_time = indicator_start.elapsed();
                    // Timing logs moved to debug level to reduce noise
                    debug!("⏱️ Indicator processing times for {}: batch_prep={:?}, send={:?}", 
                           symbol, batch_prep_time, send_time);
                }
            }
        } else {
            warn!("Received candle for unknown symbol: {}", symbol);
        }
        
        let _total_time = total_start.elapsed();
        // Total processing time moved to debug level
    }

    /// Get initialization status
    fn get_initialization_status(&self) -> TimeFrameReply {
        let initialized_symbols: Vec<String> = self.symbol_states.iter()
            .filter(|(_, state)| state.is_initialized)
            .map(|(symbol, _)| symbol.clone())
            .collect();

        // Include information about symbols pending indicator initialization
        let pending_count = self.symbols_pending_indicator_init.len();
        info!("📊 Initialization status: {} TimeFrame initialized, {} pending indicator initialization", 
              initialized_symbols.len(), pending_count);

        TimeFrameReply::InitializationStatus {
            initialized_symbols,
            total_symbols: self.config.symbols.len(),
            is_ready: self.is_ready,
        }
    }
}

impl Actor for TimeFrameActor {
    type Mailbox = UnboundedMailbox<Self>;

    fn name() -> &'static str {
        "TimeFrameActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        info!("🚀 Starting TimeFrame Actor");
        info!("📊 Configured for {} symbols: {:?}", self.config.symbols.len(), self.config.symbols);
        info!("⏱️ Target timeframes: {:?}", 
              self.config.timeframes.iter().map(|&tf| timeframe_to_string(tf)).collect::<Vec<_>>());

        // Initialize with historical data
        if let Err(e) = self.initialize_with_historical_data().await {
            error!("Failed to initialize TimeFrame actor: {}", e);
            return Err(e);
        }

        info!("✅ TimeFrame actor startup completed");
        Ok(())
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), BoxError> {
        info!("🛑 Stopping TimeFrame Actor");
        
        // Log final statistics
        for (symbol, state) in &self.symbol_states {
            let buffer_stats: Vec<String> = state.ring_buffers.iter()
                .map(|(&tf, buffer)| format!("{}:{}", timeframe_to_string(tf), buffer.len()))
                .collect();
            
            info!("📊 Final state for {}: [{}] (last_candle: {:?})", 
                  symbol, buffer_stats.join(", "), state.last_candle_time);
        }

        Ok(())
    }
}

impl Message<TimeFrameTell> for TimeFrameActor {
    type Reply = ();

    async fn handle(&mut self, msg: TimeFrameTell, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            TimeFrameTell::ProcessCandle { symbol, candle, is_closed } => {
                // Process candles with closed status information for proper EMA handling
                self.process_new_candle(&symbol, &candle, is_closed).await;
            }
            TimeFrameTell::SetReferenceTimestamp { timestamp } => {
                info!("🕐 TimeFrameActor received synchronized timestamp: {}", timestamp);
                self.reference_timestamp = Some(timestamp);
            }
            TimeFrameTell::RetryIndicatorInitialization { symbol } => {
                info!("🔄 Received retry request for indicator initialization: {}", symbol);
                if let Err(e) = self.retry_indicator_initialization(&symbol).await {
                    error!("❌ Failed to retry indicator initialization for {}: {}", symbol, e);
                } else {
                    info!("✅ Successfully retried indicator initialization for {}", symbol);
                }
            }
            TimeFrameTell::LogDataCompletenessStatus => {
                self.log_data_completeness_status();
            }
        }
    }
}

impl Message<TimeFrameAsk> for TimeFrameActor {
    type Reply = Result<TimeFrameReply, String>;

    async fn handle(&mut self, msg: TimeFrameAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            TimeFrameAsk::GetMultiTimeFrameData { symbol } => {
                if let Some(state) = self.symbol_states.get(&symbol) {
                    Ok(TimeFrameReply::MultiTimeFrameData(Box::new(state.get_current_data())))
                } else {
                    Err(format!("Symbol {} not found", symbol))
                }
            }
            TimeFrameAsk::GetInitializationStatus => {
                Ok(self.get_initialization_status())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_candle(close_time: i64, close: f64) -> FuturesOHLCVCandle {
        FuturesOHLCVCandle {
            open_time: close_time - 59999,
            close_time,
            open: close,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 1000.0,
            number_of_trades: 100,
            taker_buy_base_asset_volume: 600.0,
            closed: true,
        }
    }

    #[tokio::test]
    async fn test_timeframe_actor_creation() {
        let config = TechnicalAnalysisConfig::default();
        let _temp_dir = TempDir::new().unwrap();
        let actor = TimeFrameActor::new(config.clone());
        
        assert_eq!(actor.symbol_states.len(), config.symbols.len());
        assert!(!actor.is_ready);
    }

    #[tokio::test] 
    async fn test_symbol_timeframe_state() {
        let timeframes = vec![300, 900, 3600]; // 5m, 15m, 1h
        let mut state = SymbolTimeFrameState::new(&timeframes);
        
        // Should create ring buffers for all timeframes except 1m
        assert_eq!(state.ring_buffers.len(), 3);
        assert!(state.ring_buffers.contains_key(&300));
        assert!(state.ring_buffers.contains_key(&900));
        assert!(state.ring_buffers.contains_key(&3600));
        assert!(!state.is_initialized);
        
        // Process a candle
        let candle = create_test_candle(59999, 100.0);
        let completed = state.process_candle(&candle);
        
        // Should not complete any candles yet (need more for aggregation)
        assert!(completed.is_empty());
        assert_eq!(state.last_candle_time, Some(59999));
    }

    #[test]
    fn test_multi_timeframe_data() {
        let timeframes = vec![300, 900];
        let state = SymbolTimeFrameState::new(&timeframes);
        let data = state.get_current_data();
        
        // Should be None for all timeframes initially
        assert!(data.candles_5m.is_none());
        assert!(data.candles_15m.is_none());
        assert!(data.candles_1h.is_none());
        assert!(data.candles_4h.is_none());
    }
}