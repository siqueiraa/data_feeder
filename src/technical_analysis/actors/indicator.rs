use std::collections::{HashMap, VecDeque};

use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};
use crate::technical_analysis::structs::{
    TechnicalAnalysisConfig, IncrementalEMA, TrendAnalyzer, TrendDirection, 
    MaxVolumeTracker, IndicatorOutput
};
use crate::technical_analysis::utils::{
    extract_close_prices, create_volume_records_from_candles, 
    format_timestamp_iso
};

/// Messages for Indicator Actor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndicatorTell {
    /// Initialize with historical aggregated candles
    InitializeWithHistory {
        symbol: String,
        aggregated_candles: HashMap<u64, Vec<FuturesOHLCVCandle>>,
    },
    /// Process a new candle for a specific timeframe
    ProcessCandle {
        symbol: String,
        timeframe_seconds: u64,
        candle: FuturesOHLCVCandle,
    },
    /// Process multiple timeframe updates in a single batch (prevents duplicate outputs)
    ProcessMultiTimeFrameUpdate {
        symbol: String,
        candles: HashMap<u64, FuturesOHLCVCandle>, // timeframe_seconds -> candle
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndicatorAsk {
    /// Get current indicators for a symbol
    GetIndicators { symbol: String },
    /// Get initialization status
    GetInitializationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndicatorReply {
    /// Indicator data response
    Indicators(IndicatorOutput),
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

/// Symbol-specific indicator state
#[derive(Debug)]
struct SymbolIndicatorState {
    /// EMA calculators for each timeframe and period
    /// Key: (timeframe_seconds, ema_period)
    emas: HashMap<(u64, u32), IncrementalEMA>,
    
    /// Trend analyzers for each timeframe
    trend_analyzers: HashMap<u64, TrendAnalyzer>,
    
    /// Volume tracker for maximum volume analysis
    volume_tracker: MaxVolumeTracker,
    
    /// Current multi-timeframe close prices (including live candles)
    current_closes: HashMap<u64, f64>,
    
    /// Last completed candle close prices for each timeframe (for API output)
    completed_closes: HashMap<u64, f64>,
    
    /// Recent candle close prices for trend analysis (last 5 closes per timeframe)
    recent_candle_closes: HashMap<u64, VecDeque<f64>>,
    
    /// Whether this symbol is initialized
    is_initialized: bool,
    
    /// Last processed candle timestamp
    last_update_time: Option<TimestampMS>,

}

impl SymbolIndicatorState {
    fn new(config: &TechnicalAnalysisConfig) -> Self {
        let mut emas = HashMap::new();
        let mut trend_analyzers = HashMap::new();
        
        // Create EMA calculators for each timeframe and period combination
        for &timeframe in &config.timeframes {
            // Add 1-minute timeframe if not already included
            let timeframes = if timeframe != 60 {
                let mut tf = vec![60]; // Always include 1-minute
                tf.push(timeframe);
                tf
            } else {
                vec![timeframe]
            };
            
            for tf in timeframes {
                for &period in &config.ema_periods {
                    emas.insert((tf, period), IncrementalEMA::new(period));
                }
                
                // Create trend analyzer for this timeframe
                trend_analyzers.insert(tf, TrendAnalyzer::new(2)); // 2 candles for confirmation
            }
        }
        
        // Always ensure we have 1-minute EMA and trend analyzers
        for &period in &config.ema_periods {
            emas.insert((60, period), IncrementalEMA::new(period));
        }
        trend_analyzers.insert(60, TrendAnalyzer::new(2));

        Self {
            emas,
            trend_analyzers,
            volume_tracker: MaxVolumeTracker::new(config.volume_lookback_days),
            current_closes: HashMap::new(),
            completed_closes: HashMap::new(),
            recent_candle_closes: HashMap::new(),
            is_initialized: false,
            last_update_time: None
        }
    }

    /// Initialize with historical aggregated candles
    async fn initialize_with_history(
        &mut self,
        symbol: &str,
        aggregated_candles: HashMap<u64, Vec<FuturesOHLCVCandle>>,
    ) {
        info!("Initializing indicator state for {} with {} timeframes", 
              symbol, aggregated_candles.len());

        // Initialize EMA calculators with historical data
        for (&timeframe, candles) in &aggregated_candles {
            let close_prices = extract_close_prices(candles);
            
            // Initialize EMAs for this timeframe
            for &period in [21, 89].iter() { // Common EMA periods
                if let Some(ema) = self.emas.get_mut(&(timeframe, period)) {
                    let initial_value = ema.initialize_with_history(&close_prices);
                    if ema.is_ready() {
                        info!("📊 Initialized EMA{} for {} {}s: {:.8} (from {} candles)", 
                               period, symbol, timeframe, initial_value.unwrap_or(0.0), close_prices.len());
                    }
                }
            }

            // Initialize recent candle closes for trend analysis (last 5 candles)
            let recent_closes_deque: VecDeque<f64> = close_prices.iter()
                .rev()
                .take(5)
                .rev()
                .cloned()
                .collect();
            self.recent_candle_closes.insert(timeframe, recent_closes_deque);
            info!("📊 Initialized recent closes for {} {}s: {} candles", 
                  symbol, timeframe, close_prices.len().min(5));

            // Initialize trend analyzers with actual historical EMA progression
            if let Some(trend_analyzer) = self.trend_analyzers.get_mut(&timeframe) {
                if let Some(ema89) = self.emas.get(&(timeframe, 89)) {
                    if ema89.is_ready() {
                        // Calculate EMA89 progression using the last 10 historical candles
                        let num_trend_points = close_prices.len().min(10).max(3);
                        let trend_closes: Vec<f64> = close_prices.iter().rev().take(num_trend_points).rev().cloned().collect();
                        
                        // Use the same EMA algorithm as the main EMA to get consistent values
                        let mut trend_ema = IncrementalEMA::new(89);
                        let mut ema_progression = Vec::new();
                        
                        // Initialize the trend EMA with all historical data first (to match main EMA)
                        for &price in &close_prices[..close_prices.len().saturating_sub(num_trend_points)] {
                            trend_ema.update(price);
                        }
                        
                        // Now calculate the progression for the last few periods
                        for &price in &trend_closes {
                            if let Some(ema_val) = trend_ema.update(price) {
                                ema_progression.push(ema_val);
                            }
                        }
                        
                        // Use the last 2-3 EMA values for trend detection
                        if ema_progression.len() >= 2 {
                            let final_trend_data: Vec<f64> = ema_progression.iter().rev().take(2).rev().cloned().collect();
                            trend_analyzer.initialize_with_history(&final_trend_data);
                            info!("📈 Initialized trend analyzer for {} {}s with EMA progression: {:?}", 
                                  symbol, timeframe, final_trend_data);
                        }
                    }
                }
            }



            if let Some(last_candle) = candles.last() {
                self.current_closes.insert(timeframe, last_candle.close);
                if last_candle.closed {
                    // Historical candles are always completed
                    self.completed_closes.insert(timeframe, last_candle.close);
                    info!("📊 Historical initialization for {}s: last_close={:.2} @ {}",
              timeframe, last_candle.close, last_candle.close_time);
                } else {
                    if let Some(second_last_candle) = candles.get(candles.len().saturating_sub(2)) {
                        if second_last_candle.closed {
                            self.completed_closes.insert(timeframe, second_last_candle.close);
                            info!("📊 Historical initialization for {}s: last_close={:.2} @ {}",
                      timeframe, last_candle.close, last_candle.close_time);
                        }
                    }
                }
            } else {
                warn!("⚠️ No candles available for timeframe {}s during initialization", timeframe);
            }

        }

        // Initialize volume tracker with 5-minute data
        if let Some(candles_5m) = aggregated_candles.get(&300) {
            let volume_records = create_volume_records_from_candles(
                candles_5m, 
                TrendDirection::Neutral // Will be updated with real trends later
            );
            self.volume_tracker.initialize_with_history(&volume_records);
            
            info!("Initialized volume tracker for {} with {} 5m candles", 
                  symbol, candles_5m.len());
        }

        self.is_initialized = true;
        
        // Log initialization status
        let ready_emas = self.emas.values().filter(|ema| ema.is_ready()).count();
        info!("✅ Initialized indicators for {}: {}/{} EMAs ready", 
              symbol, ready_emas, self.emas.len());
    }

    /// Process a new candle for a specific timeframe
    fn process_candle(&mut self, timeframe_seconds: u64, candle: &FuturesOHLCVCandle) {
        let close_price = candle.close;
        self.last_update_time = Some(candle.close_time);
        
        // Update current close price for this timeframe (includes live candles)
        self.current_closes.insert(timeframe_seconds, close_price);
        
        // Store recent candle closes for trend analysis (keep last 5 closes) and completed closes
        if candle.closed {
            // Update completed close price (only for closed candles)
            self.completed_closes.insert(timeframe_seconds, close_price);
            info!("✅ COMPLETED candle for {}s: {:.2} @ {} (stored as completed close)", 
                  timeframe_seconds, close_price, candle.close_time);
            
            let recent_closes = self.recent_candle_closes.entry(timeframe_seconds).or_insert_with(VecDeque::new);
            // Keep only last 5 closes
            while recent_closes.len() > 5 {
                recent_closes.pop_front();
            }
            debug!("📊 Stored recent close for {}s: {:.2} (history: {} candles)", 
                   timeframe_seconds, close_price, recent_closes.len());
        } else {
            info!("📈 LIVE candle for {}s: {:.2} @ {} (not stored as completed)", 
                  timeframe_seconds, close_price, candle.close_time);
        }
        
        // Update EMAs for this timeframe 
        // Since TimeFrame actor now only sends closed candles, we can process all received candles
        // But keep the closed check as a safety net for historical compatibility
        if candle.closed {
            for &period in [21, 89].iter() {
                if let Some(ema) = self.emas.get_mut(&(timeframe_seconds, period)) {
                    let new_ema = ema.update(close_price);
                    if let Some(ema_val) = new_ema {
                        debug!("📊 Updated EMA{} for {}s: {:.8} (from close: {:.2})", 
                               period, timeframe_seconds, ema_val, close_price);
                    }
                }
            }
        } else {
            warn!("⚠️ Received non-closed candle in indicator - should not happen after TimeFrame fix!");
        }

        // Update trend analysis using candle closes vs EMA89 - ONLY for closed candles
        if candle.closed {
            if let Some(trend_analyzer) = self.trend_analyzers.get(&timeframe_seconds) {
                if let Some(ema89) = self.emas.get(&(timeframe_seconds, 89)) {
                    if let Some(ema89_value) = ema89.value() {
                        if let Some(recent_closes) = self.recent_candle_closes.get(&timeframe_seconds) {
                            let trend = trend_analyzer.analyze_candles_vs_ema(recent_closes, ema89_value);
                            // Only log trend changes, not every analysis
                            debug!("📈 TREND ANALYSIS for {}s: {} | EMA89: {:.2}", 
                                   timeframe_seconds, trend, ema89_value);
                        }
                    }
                }
            }
        }

        // Update volume tracker for 5-minute candles - ONLY for closed candles
        if timeframe_seconds == 300 && candle.closed {
            // Get current trend for this timeframe
            let current_trend = self.trend_analyzers.get(&300)
                .map(|ta| ta.analyze_trend())
                .unwrap_or(TrendDirection::Neutral);
            
            self.volume_tracker.update(
                candle.volume,
                close_price,
                candle.close_time,
                current_trend,
            );
        }
    }

    /// Get trend for a specific timeframe using candles vs EMA89 comparison
    fn get_trend_for_timeframe(&self, timeframe_seconds: u64) -> TrendDirection {
        if let Some(trend_analyzer) = self.trend_analyzers.get(&timeframe_seconds) {
            if let Some(ema89) = self.emas.get(&(timeframe_seconds, 89)) {
                if let Some(ema89_value) = ema89.value() {
                    if let Some(recent_closes) = self.recent_candle_closes.get(&timeframe_seconds) {
                        return trend_analyzer.analyze_candles_vs_ema(recent_closes, ema89_value);
                    }
                }
            }
        }
        TrendDirection::Neutral
    }

    /// Calculate max_volume_trend using 3 4h candles vs max_volume_price (same rule as other trends)
    fn calculate_max_volume_trend(&self, max_volume_price: f64) -> Option<TrendDirection> {
        // Get last 3 completed 4h candle closes
        if let Some(recent_4h_closes) = self.recent_candle_closes.get(&14400) {
            if recent_4h_closes.len() >= 3 {
                let last_3_closes: Vec<f64> = recent_4h_closes.iter()
                    .rev()
                    .take(3)
                    .cloned()
                    .collect();
                
                // Check if ALL 3 candles are consistently above or below max_volume_price
                let all_above_max_price = last_3_closes.iter().all(|&close| close > max_volume_price);
                let all_below_max_price = last_3_closes.iter().all(|&close| close < max_volume_price);
                
                if all_above_max_price {
                    Some(TrendDirection::Buy)
                } else if all_below_max_price {
                    Some(TrendDirection::Sell)
                } else {
                    Some(TrendDirection::Neutral)
                }
            } else {
                Some(TrendDirection::Neutral) // Not enough data
            }
        } else {
            Some(TrendDirection::Neutral) // No 4h data
        }
    }

    /// Generate current indicator output
    fn generate_output(&self, symbol: &str) -> IndicatorOutput {
        let mut output = IndicatorOutput {
            symbol: symbol.to_string(),
            timestamp: self.last_update_time.unwrap_or(0),
            ..Default::default()
        };

        // Multi-timeframe close prices (from last COMPLETED candles)
        output.close_5m = self.completed_closes.get(&300).cloned();
        output.close_15m = self.completed_closes.get(&900).cloned();
        output.close_60m = self.completed_closes.get(&3600).cloned();
        output.close_4h = self.completed_closes.get(&14400).cloned();
        
        // Debug completed closes vs current closes
        info!("🔍 COMPLETED closes: 5m={:?}, 15m={:?}, 1h={:?}, 4h={:?}", 
               self.completed_closes.get(&300), self.completed_closes.get(&900),
               self.completed_closes.get(&3600), self.completed_closes.get(&14400));
        info!("🔍 CURRENT closes: 5m={:?}, 15m={:?}, 1h={:?}, 4h={:?}",
               self.current_closes.get(&300), self.current_closes.get(&900),
               self.current_closes.get(&3600), self.current_closes.get(&14400));
               
        // Check if all completed closes are identical and explain why
        let completed_values: Vec<f64> = [&300, &900, &3600, &14400].iter()
            .filter_map(|&tf| self.completed_closes.get(tf))
            .cloned()
            .collect();
        let unique_completed: std::collections::HashSet<_> = completed_values.iter()
            .map(|&f| (f * 100.0) as i64).collect();
            
        if unique_completed.len() == 1 && !completed_values.is_empty() {
            info!("ℹ️ All timeframes show same close ({:.2}) - this is CORRECT during initialization", completed_values[0]);
            info!("ℹ️ Different values will appear as new real-time candles complete at different times");
        }

        // EMA values
        output.ema21_1min = self.emas.get(&(60, 21)).and_then(|ema| ema.value());
        output.ema89_1min = self.emas.get(&(60, 89)).and_then(|ema| ema.value());
        output.ema89_5min = self.emas.get(&(300, 89)).and_then(|ema| ema.value());
        output.ema89_15min = self.emas.get(&(900, 89)).and_then(|ema| ema.value());
        output.ema89_1h = self.emas.get(&(3600, 89)).and_then(|ema| ema.value());
        output.ema89_4h = self.emas.get(&(14400, 89)).and_then(|ema| ema.value());

        // Trend analysis using candles vs EMA89 comparison
        output.trend_1min = self.get_trend_for_timeframe(60);
        output.trend_5min = self.get_trend_for_timeframe(300);
        output.trend_15min = self.get_trend_for_timeframe(900);
        output.trend_1h = self.get_trend_for_timeframe(3600);
        output.trend_4h = self.get_trend_for_timeframe(14400);

        // Volume analysis
        if let Some(max_vol) = self.volume_tracker.get_max() {
            output.max_volume = Some(max_vol.volume);
            output.max_volume_price = Some(max_vol.price);
            output.max_volume_time = Some(format_timestamp_iso(max_vol.timestamp));
            
            // Calculate max_volume_trend using 3 4h candles vs max_volume_price
            output.max_volume_trend = self.calculate_max_volume_trend(max_vol.price);
        }

        output
    }
}

/// Indicator Actor that calculates technical indicators from multi-timeframe candles
pub struct IndicatorActor {
    /// Configuration
    config: TechnicalAnalysisConfig,
    
    /// Symbol-specific indicator state
    symbol_states: HashMap<String, SymbolIndicatorState>,
    
    /// Overall initialization status
    is_ready: bool,
}

impl IndicatorActor {
    /// Create a new Indicator actor
    pub fn new(config: TechnicalAnalysisConfig) -> Self {
        let mut symbol_states = HashMap::new();
        
        // Initialize state for each symbol
        for symbol in &config.symbols {
            let state = SymbolIndicatorState::new(&config);
            symbol_states.insert(symbol.clone(), state);
        }

        Self {
            config,
            symbol_states,
            is_ready: false,
        }
    }

    /// Get initialization status
    fn get_initialization_status(&self) -> IndicatorReply {
        let initialized_symbols: Vec<String> = self.symbol_states.iter()
            .filter(|(_, state)| state.is_initialized)
            .map(|(symbol, _)| symbol.clone())
            .collect();

        IndicatorReply::InitializationStatus {
            initialized_symbols,
            total_symbols: self.config.symbols.len(),
            is_ready: self.is_ready,
        }
    }

    /// Output current indicators for debugging/monitoring
    async fn output_current_indicators(&self) {
        for (symbol, state) in &self.symbol_states {
            if state.is_initialized {
                let output = state.generate_output(symbol);
                
                // Log key indicators
                info!("📈 {} Indicators: 1m_trend={}, 5m_close={:.2}, ema89_1h={:.2}, max_vol={:.0}",
                      symbol,
                      output.trend_1min,
                      output.close_5m.unwrap_or(0.0),
                      output.ema89_1h.unwrap_or(0.0),
                      output.max_volume.unwrap_or(0.0));
                
                // In production, you might want to:
                // - Send to external systems
                // - Store in database
                // - Publish via WebSocket/HTTP
            }
        }
    }
}

impl Actor for IndicatorActor {
    type Mailbox = UnboundedMailbox<Self>;

    fn name() -> &'static str {
        "IndicatorActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        info!("🚀 Starting Indicator Actor");
        info!("📊 Configured for {} symbols with {} EMA periods", 
              self.config.symbols.len(), self.config.ema_periods.len());

        info!("✅ Indicator actor startup completed");
        Ok(())
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), BoxError> {
        info!("🛑 Stopping Indicator Actor");
        
        // Output final indicators
        self.output_current_indicators().await;
        
        // Log final statistics
        for (symbol, state) in &self.symbol_states {
            let ready_emas = state.emas.values().filter(|ema| ema.is_ready()).count();
            info!("📊 Final state for {}: {}/{} EMAs ready, last_update: {:?}", 
                  symbol, ready_emas, state.emas.len(), state.last_update_time);
        }

        Ok(())
    }
}

impl Message<IndicatorTell> for IndicatorActor {
    type Reply = ();

    async fn handle(&mut self, msg: IndicatorTell, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            IndicatorTell::InitializeWithHistory { symbol, aggregated_candles } => {
                info!("🔄 Initializing indicators for {} with historical data", symbol);
                
                if let Some(state) = self.symbol_states.get_mut(&symbol) {
                    state.initialize_with_history(&symbol, aggregated_candles).await;
                    
                    // Check if all symbols are now initialized
                    let initialized_count = self.symbol_states.values()
                        .filter(|s| s.is_initialized)
                        .count();
                    
                    self.is_ready = initialized_count == self.config.symbols.len();
                    
                    if self.is_ready {
                        info!("🎉 All indicator symbols initialized and ready!");
                    }
                } else {
                    error!("Received initialization for unknown symbol: {}", symbol);
                }
            }
            
            IndicatorTell::ProcessCandle { symbol, timeframe_seconds, candle } => {
                if let Some(state) = self.symbol_states.get_mut(&symbol) {
                    state.process_candle(timeframe_seconds, &candle);
                    
                    // NOTE: No real-time output here - this is for individual candle processing
                    // Real-time output is now handled by ProcessMultiTimeFrameUpdate
                } else {
                    warn!("Received candle for unknown symbol: {}", symbol);
                }
            }
            
            IndicatorTell::ProcessMultiTimeFrameUpdate { symbol, candles } => {
                let handler_start = std::time::Instant::now();
                let mut batch_processing_time = std::time::Duration::new(0, 0);
                let mut output_time = std::time::Duration::new(0, 0);
                
                if let Some(state) = self.symbol_states.get_mut(&symbol) {
                    // Process all timeframe candles in the batch
                    let batch_start = std::time::Instant::now();
                    for (&timeframe_seconds, candle) in &candles {
                        state.process_candle(timeframe_seconds, candle);
                    }
                    batch_processing_time = batch_start.elapsed();
                    
                    // Output consolidated real-time indicators ONCE after processing all timeframes
                    let output_start = std::time::Instant::now();
                    let output = state.generate_output(&symbol);
                    output_time = output_start.elapsed();
                    
                    info!("📈 REAL-TIME {} | TS:{} | 5m:{:.2} 15m:{:.2} 1h:{:.2} 4h:{:.2} | EMA89[1m:{:.8} 5m:{:.8} 1h:{:.8} 4h:{:.8}] | TRENDS[1m:{} 5m:{} 15m:{} 1h:{} 4h:{}]",
                          symbol,
                          output.timestamp,
                          output.close_5m.unwrap_or(0.0),
                          output.close_15m.unwrap_or(0.0),
                          output.close_60m.unwrap_or(0.0),
                          output.close_4h.unwrap_or(0.0),
                          output.ema89_1min.unwrap_or(0.0),
                          output.ema89_5min.unwrap_or(0.0),
                          output.ema89_1h.unwrap_or(0.0),
                          output.ema89_4h.unwrap_or(0.0),
                          output.trend_1min,
                          output.trend_5min,
                          output.trend_15min,
                          output.trend_1h,
                          output.trend_4h
                    );
                } else {
                    warn!("Received batched update for unknown symbol: {}", symbol);
                }
                
                let handler_time = handler_start.elapsed();
                info!("⏱️ IndicatorTell::ProcessMultiTimeFrameUpdate ELAPSED TIMES: total_handler={:?}, batch_processing={:?}, output_generation={:?}", 
                      handler_time, batch_processing_time, output_time);
            }
        }
    }
}

impl Message<IndicatorAsk> for IndicatorActor {
    type Reply = Result<IndicatorReply, String>;

    async fn handle(&mut self, msg: IndicatorAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            IndicatorAsk::GetIndicators { symbol } => {
                if let Some(state) = self.symbol_states.get(&symbol) {
                    let indicators = state.generate_output(&symbol);
                    Ok(IndicatorReply::Indicators(indicators))
                } else {
                    Err(format!("Symbol {} not found", symbol))
                }
            }
            
            IndicatorAsk::GetInitializationStatus => {
                Ok(self.get_initialization_status())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indicator_actor_creation() {
        let config = TechnicalAnalysisConfig::default();
        let actor = IndicatorActor::new(config.clone());
        
        assert_eq!(actor.symbol_states.len(), config.symbols.len());
        assert!(!actor.is_ready);
    }

    #[test]
    fn test_symbol_indicator_state() {
        let config = TechnicalAnalysisConfig::default();
        let state = SymbolIndicatorState::new(&config);
        
        // Should create EMAs for all timeframe/period combinations
        let expected_ema_count = config.timeframes.len() * config.ema_periods.len() + config.ema_periods.len(); // +1min
        assert!(state.emas.len() >= expected_ema_count);
        
        // Should create trend analyzers for all timeframes + 1min
        assert!(state.trend_analyzers.len() >= config.timeframes.len());
        assert!(!state.is_initialized);
    }

    #[test]
    fn test_generate_output() {
        let config = TechnicalAnalysisConfig::default();
        let state = SymbolIndicatorState::new(&config);
        let output = state.generate_output("BTCUSDT");
        
        assert_eq!(output.symbol, "BTCUSDT");
        assert!(matches!(output.trend_1min, TrendDirection::Neutral));
        assert_eq!(output.timestamp, 0); // No updates yet
    }
}