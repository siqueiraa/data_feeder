use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

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
    MultiTimeFrameData(MultiTimeFrameCandles),
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
        }
    }

    /// Process a new 1-minute candle and return completed higher timeframe candles
    fn process_candle(&mut self, candle: &FuturesOHLCVCandle) -> HashMap<u64, FuturesOHLCVCandle> {
        let mut completed_candles = HashMap::new();

        // DEDUPLICATION: Check if we've already processed this exact candle
        let now = std::time::Instant::now();
        if let Some(&last_processed) = self.processed_candles.get(&candle.close_time) {
            // If we processed this candle recently (within last 30 seconds), skip it
            if now.duration_since(last_processed) < Duration::from_secs(30) {
                // Skip duplicate candle - this is expected behavior
                return completed_candles; // Return empty - no processing
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
                completed_candles.insert(timeframe, completed_candle);
            }
        }

        completed_candles
    }

    /// Initialize with historical data
    async fn initialize_with_history(
        &mut self, 
        symbol: &str, 
        historical_candles: &[FuturesOHLCVCandle]
    ) {
        info!("Initializing timeframe state for {} with {} historical candles", 
              symbol, historical_candles.len());

        // Process each historical candle to populate ring buffers
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
    /// Base path for LMDB storage
    base_path: PathBuf,
    /// Symbol-specific state
    symbol_states: HashMap<String, SymbolTimeFrameState>,
    /// Reference to indicator actor
    indicator_actor: Option<ActorRef<IndicatorActor>>,
    /// Reference to API actor for gap filling
    api_actor: Option<ActorRef<crate::api::ApiActor>>,
    /// Overall initialization status
    is_ready: bool,
}

impl TimeFrameActor {
    /// Create a new TimeFrame actor
    pub fn new(config: TechnicalAnalysisConfig, base_path: PathBuf) -> Self {
        let mut symbol_states = HashMap::new();
        
        // Initialize state for each symbol
        for symbol in &config.symbols {
            let state = SymbolTimeFrameState::new(&config.timeframes);
            symbol_states.insert(symbol.clone(), state);
        }

        Self {
            config,
            base_path,
            symbol_states,
            indicator_actor: None,
            api_actor: None,
            is_ready: false,
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

    /// Initialize with historical data from database
    async fn initialize_with_historical_data(&mut self) -> Result<(), BoxError> {
        info!("🔄 Starting TimeFrame actor initialization with historical data...");

        let min_candles = calculate_min_candles_needed(
            &self.config.timeframes,
            &self.config.ema_periods,
            self.config.volume_lookback_days,
        );

        // Removed unused initialization_tasks vector

        for symbol in &self.config.symbols.clone() {
            info!("Loading historical data for {}", symbol);

            // Load 1-minute candles from database
            let historical_candles = load_recent_candles_from_db(
                symbol,
                60, // 1-minute timeframe
                self.config.min_history_days,
                &self.base_path,
            ).await?;

            // Validate we have sufficient data and get detected gaps
            match validate_initialization_data(symbol, &historical_candles, min_candles) {
                Ok(detected_gaps) => {
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
                    warn!("⚠️ {}", e);
                    // Continue with available data but mark as not fully initialized
                }
            }

            // Initialize symbol state
            if let Some(state) = self.symbol_states.get_mut(symbol) {
                state.initialize_with_history(symbol, &historical_candles).await;
            }

            // Pre-aggregate historical data for indicator initialization
            let aggregated_data = pre_aggregate_historical_candles(
                &historical_candles,
                &self.config.timeframes,
            );

            // Send historical data to indicator actor for initialization
            if let Some(indicator_actor) = &self.indicator_actor {
                let init_msg = IndicatorTell::InitializeWithHistory {
                    symbol: symbol.clone(),
                    aggregated_candles: aggregated_data,
                };

                if let Err(e) = indicator_actor.tell(init_msg).send().await {
                    error!("Failed to send historical data to indicator actor: {}", e);
                }
            }

            info!("✅ Completed initialization for {}", symbol);
        }

        // Check if all symbols are initialized
        let initialized_count = self.symbol_states.values()
            .filter(|state| state.is_initialized)
            .count();

        self.is_ready = initialized_count == self.config.symbols.len();

        if self.is_ready {
            info!("🎉 TimeFrame actor fully initialized for all {} symbols", self.config.symbols.len());
        } else {
            warn!("⚠️ TimeFrame actor partially initialized: {}/{} symbols ready", 
                  initialized_count, self.config.symbols.len());
        }

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
        }
    }
}

impl Message<TimeFrameAsk> for TimeFrameActor {
    type Reply = Result<TimeFrameReply, String>;

    async fn handle(&mut self, msg: TimeFrameAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            TimeFrameAsk::GetMultiTimeFrameData { symbol } => {
                if let Some(state) = self.symbol_states.get(&symbol) {
                    Ok(TimeFrameReply::MultiTimeFrameData(state.get_current_data()))
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
        let temp_dir = TempDir::new().unwrap();
        let actor = TimeFrameActor::new(config.clone(), temp_dir.path().to_path_buf());
        
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