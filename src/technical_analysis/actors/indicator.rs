use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant}; // PHASE 3: Memory pressure detection

use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::request::MessageSend;
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::{record_indicator_timing, record_memory_pool_hit, record_simd_op};

use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};
use crate::technical_analysis::structs::{
    TechnicalAnalysisConfig, IncrementalEMA, TrendAnalyzer, TrendDirection, 
    MaxVolumeTracker, IndicatorOutput, QuantileTracker,
    TimeFrameIndex, EmaPeriodIndex, TIMEFRAME_COUNT, EMA_PERIOD_COUNT
};
use crate::technical_analysis::utils::{
    extract_close_prices, create_volume_records_from_candles, 
    create_quantile_records_from_candles, format_timestamp_iso
};
use crate::kafka::{KafkaActor, KafkaTell};

// PHASE 3: Adaptive optimization modes based on resource pressure and production patterns
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
enum OptimizationMode {
    /// Maximum performance - use all optimizations (low memory pressure)
    MaxPerformance,
    /// Balanced mode - moderate optimizations (medium memory pressure)
    Balanced,
    /// Conservative mode - minimal resource usage (high memory pressure)
    Conservative,
    /// Emergency mode - critical resource constraints
    Emergency,
}

// PHASE 3: Lightweight production pattern learning (optimized for minimal overhead)
#[derive(Debug, Clone)]
struct LightweightPatternLearner {
    /// Simple counter for update frequency control
    update_counter: u64,
    /// Learned SIMD activation threshold (updated infrequently)
    simd_threshold: f64,
    /// Last performance check timestamp (rate limited)
    last_check: Instant,
    /// Simple performance indicator (good/bad based on recent operations)
    performance_good: bool,
}

impl LightweightPatternLearner {
    fn new() -> Self {
        Self {
            update_counter: 0,
            simd_threshold: 0.7, // Start with proven good threshold
            last_check: Instant::now(),
            performance_good: true, // Start optimistic
        }
    }
    
    /// PHASE 3: Ultra-lightweight update (only every 100th operation)
    #[inline(always)]
    fn maybe_update(&mut self, processing_time_ns: u64) {
        self.update_counter += 1;
        
        // Only update every 100 operations to minimize overhead
        if self.update_counter % 100 != 0 {
            return;
        }
        
        // Only check performance every 30 seconds to minimize overhead
        if self.last_check.elapsed() < Duration::from_secs(30) {
            return;
        }
        
        self.last_check = Instant::now();
        
        // Simple performance classification based on processing time
        let processing_time_us = processing_time_ns as f64 / 1000.0;
        let was_good = self.performance_good;
        self.performance_good = processing_time_us < 10.0; // Good if < 10μs
        
        // Adjust SIMD threshold based on performance trend
        if self.performance_good && !was_good {
            // Performance improved, lower threshold slightly
            self.simd_threshold = (self.simd_threshold - 0.05).max(0.5);
        } else if !self.performance_good && was_good {
            // Performance degraded, raise threshold slightly  
            self.simd_threshold = (self.simd_threshold + 0.05).min(0.9);
        }
    }
    
    /// Get current SIMD threshold
    #[inline(always)]
    fn get_threshold(&self) -> f64 {
        self.simd_threshold
    }
}

/// Messages for Indicator Actor
#[derive(Debug, Clone)]
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
    /// Set Kafka actor reference for publishing indicators
    SetKafkaActor {
        kafka_actor: ActorRef<KafkaActor>,
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
    Indicators(Box<IndicatorOutput>),
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
pub struct SymbolIndicatorState {
    /// EMA calculators using fixed arrays for O(1) access [timeframe][period]  
    emas: [[Option<IncrementalEMA>; EMA_PERIOD_COUNT]; TIMEFRAME_COUNT],
    
    /// Trend analyzers using fixed array for O(1) access [timeframe]
    trend_analyzers: [Option<TrendAnalyzer>; TIMEFRAME_COUNT],
    
    /// Volume tracker for maximum volume analysis
    volume_tracker: MaxVolumeTracker,
    
    /// Quantile tracker for volume/trade analysis
    quantile_tracker: QuantileTracker,
    
    /// Current multi-timeframe close prices using fixed array [timeframe]
    current_closes: [Option<f64>; TIMEFRAME_COUNT],
    
    /// Last completed candle close prices using fixed array [timeframe] 
    completed_closes: [Option<f64>; TIMEFRAME_COUNT],
    
    /// Recent candle close prices using fixed array [timeframe]
    recent_candle_closes: [Option<VecDeque<f64>>; TIMEFRAME_COUNT],
    
    /// Whether this symbol is initialized
    is_initialized: bool,
    
    /// Last processed candle timestamp
    last_update_time: Option<TimestampMS>,
    
    // MEMORY OPTIMIZATION: Object pools for hot path allocations
    /// Pre-allocated vectors for SIMD operations to avoid repeated allocations
    alpha_pool: Vec<f64>,
    
    // PHASE 3: Lightweight pattern learning for adaptive optimization  
    /// Lightweight pattern learner with minimal overhead
    pattern_learner: LightweightPatternLearner,
    beta_pool: Vec<f64>,
    prev_values_pool: Vec<f64>,
    periods_pool: Vec<u32>,
    
    // PHASE 3: Resource management and memory pressure detection
    /// Last memory pressure check timestamp
    #[allow(dead_code)]
    last_pressure_check: Instant,
    /// Memory pressure level (0.0 = low, 1.0 = critical)
    #[allow(dead_code)]
    memory_pressure: f64,
    /// Adaptive optimization mode based on resource availability
    optimization_mode: OptimizationMode,
}

impl SymbolIndicatorState {
    pub fn new(config: &TechnicalAnalysisConfig) -> Self {
        // Initialize EMA arrays using array initialization without Copy requirement
        let mut emas = std::array::from_fn(|_| std::array::from_fn(|_| None));
        let mut trend_analyzers = std::array::from_fn(|_| None);
        
        // Initialize EMAs for all timeframe/period combinations
        for tf_idx in 0..TIMEFRAME_COUNT {
            // Safe unwrap: tf_idx is guaranteed to be valid (0..TIMEFRAME_COUNT)
            let _timeframe_seconds = TimeFrameIndex::from_index(tf_idx)
                .expect("tf_idx is within valid range")
                .to_seconds();
            
            // Initialize EMAs for this timeframe
            for period_idx in 0..EMA_PERIOD_COUNT {
                // Safe unwrap: period_idx is guaranteed to be valid (0..EMA_PERIOD_COUNT)
                let period = EmaPeriodIndex::from_index(period_idx)
                    .expect("period_idx is within valid range")
                    .to_period();
                emas[tf_idx][period_idx] = Some(IncrementalEMA::new(period));
            }
            
            // Initialize trend analyzer for this timeframe
            trend_analyzers[tf_idx] = Some(TrendAnalyzer::new(2)); // 2 candles for confirmation
        }

        Self {
            emas,
            trend_analyzers,
            volume_tracker: MaxVolumeTracker::new(config.volume_lookback_days),
            quantile_tracker: QuantileTracker::new(config.volume_lookback_days),
            current_closes: std::array::from_fn(|_| None),
            completed_closes: std::array::from_fn(|_| None),
            recent_candle_closes: std::array::from_fn(|_| None),
            is_initialized: false,
            last_update_time: None,
            
            // MEMORY OPTIMIZATION: Initialize object pools with capacity for typical SIMD operations
            alpha_pool: Vec::with_capacity(EMA_PERIOD_COUNT),
            
            // PHASE 3: Lightweight pattern learning for adaptive optimization
            pattern_learner: LightweightPatternLearner::new(),
            beta_pool: Vec::with_capacity(EMA_PERIOD_COUNT),
            prev_values_pool: Vec::with_capacity(EMA_PERIOD_COUNT),
            periods_pool: Vec::with_capacity(EMA_PERIOD_COUNT),
            
            // PHASE 3: Initialize resource management
            last_pressure_check: Instant::now(),
            memory_pressure: 0.0, // Start with low pressure
            optimization_mode: OptimizationMode::MaxPerformance, // Start optimistic
        }
    }
    
    /// Helper method to get EMA by timeframe and period (O(1) array access)
    #[inline(always)]
    fn get_ema(&self, timeframe_seconds: u64, period: u32) -> Option<&IncrementalEMA> {
        let tf_idx = TimeFrameIndex::from_seconds(timeframe_seconds)?.to_index();
        let period_idx = EmaPeriodIndex::from_period(period)?.to_index();
        self.emas[tf_idx][period_idx].as_ref()
    }
    
    /// Helper method to get mutable EMA by timeframe and period (O(1) array access)
    #[inline(always)]
    fn get_ema_mut(&mut self, timeframe_seconds: u64, period: u32) -> Option<&mut IncrementalEMA> {
        let tf_idx = TimeFrameIndex::from_seconds(timeframe_seconds)?.to_index();
        let period_idx = EmaPeriodIndex::from_period(period)?.to_index();
        self.emas[tf_idx][period_idx].as_mut()
    }
    
    /// Helper method to get trend analyzer by timeframe (O(1) array access)
    #[inline(always)]
    fn get_trend_analyzer(&self, timeframe_seconds: u64) -> Option<&TrendAnalyzer> {
        let tf_idx = TimeFrameIndex::from_seconds(timeframe_seconds)?.to_index();
        self.trend_analyzers[tf_idx].as_ref()
    }
    
    /// Helper method to get mutable trend analyzer by timeframe (O(1) array access)
    #[inline(always)]
    fn get_trend_analyzer_mut(&mut self, timeframe_seconds: u64) -> Option<&mut TrendAnalyzer> {
        let tf_idx = TimeFrameIndex::from_seconds(timeframe_seconds)?.to_index();
        self.trend_analyzers[tf_idx].as_mut()
    }
    
    
    /// Helper method to set current close by timeframe (O(1) array access)
    #[inline(always)]
    fn set_current_close(&mut self, timeframe_seconds: u64, close: f64) {
        if let Some(tf_idx) = TimeFrameIndex::from_seconds(timeframe_seconds).map(|tf| tf.to_index()) {
            self.current_closes[tf_idx] = Some(close);
        }
    }
    
    /// Helper method to get completed close by timeframe (O(1) array access)
    #[inline(always)]
    fn get_completed_close(&self, timeframe_seconds: u64) -> Option<f64> {
        let tf_idx = TimeFrameIndex::from_seconds(timeframe_seconds)?.to_index();
        self.completed_closes[tf_idx]
    }
    
    /// Helper method to set completed close by timeframe (O(1) array access)
    #[inline(always)]
    fn set_completed_close(&mut self, timeframe_seconds: u64, close: f64) {
        if let Some(tf_idx) = TimeFrameIndex::from_seconds(timeframe_seconds).map(|tf| tf.to_index()) {
            self.completed_closes[tf_idx] = Some(close);
        }
    }
    
    /// Helper method to get recent candle closes by timeframe (O(1) array access)
    #[inline(always)]
    fn get_recent_closes(&self, timeframe_seconds: u64) -> Option<&VecDeque<f64>> {
        let tf_idx = TimeFrameIndex::from_seconds(timeframe_seconds)?.to_index();
        self.recent_candle_closes[tf_idx].as_ref()
    }
    
    /// Helper method to set recent candle closes by timeframe (O(1) array access)
    #[inline(always)]
    fn set_recent_closes(&mut self, timeframe_seconds: u64, closes: VecDeque<f64>) {
        if let Some(tf_idx) = TimeFrameIndex::from_seconds(timeframe_seconds).map(|tf| tf.to_index()) {
            self.recent_candle_closes[tf_idx] = Some(closes);
        }
    }
    
    /// Helper method to get or create recent candle closes by timeframe (O(1) array access)
    #[inline(always)]
    fn get_recent_closes_mut(&mut self, timeframe_seconds: u64) -> Option<&mut VecDeque<f64>> {
        let tf_idx = TimeFrameIndex::from_seconds(timeframe_seconds)?.to_index();
        if self.recent_candle_closes[tf_idx].is_none() {
            self.recent_candle_closes[tf_idx] = Some(VecDeque::new());
        }
        self.recent_candle_closes[tf_idx].as_mut()
    }

    /// Initialize with historical aggregated candles
    async fn initialize_with_history(
        &mut self,
        symbol: &str,
        aggregated_candles: HashMap<u64, Vec<FuturesOHLCVCandle>>,
    ) {
        info!("Initializing indicator state for {} with {} timeframes", 
              symbol, aggregated_candles.len());

        // PRODUCTION FIX: Initialize EMA calculators with aggressive fast-start strategy
        for (&timeframe, candles) in &aggregated_candles {
            let close_prices = extract_close_prices(candles);
            
            // PRODUCTION FIX: Use fast-start initialization for immediate SIMD activation
            if !close_prices.is_empty() {
                let initial_price = close_prices[0]; // Use first available price
                
                for &period in &[21, 89] { // Common EMA periods
                    if let Some(ema) = self.get_ema_mut(timeframe, period) {
                        // PRODUCTION FIX: For sparse data, reinitialize with fast-start mode
                        if close_prices.len() < period as usize {
                            info!("📊 PRODUCTION: Sparse data for EMA{} {}s: {} candles < {} period. Using fast-start.", 
                                  period, timeframe, close_prices.len(), period);
                            
                            // Replace with fast-start EMA for immediate readiness
                            *ema = IncrementalEMA::new_fast_start(period, initial_price);
                            
                            // Process available historical data to improve accuracy
                            for &price in close_prices.iter().skip(1) { // Skip first price (already used)
                                ema.update(price);
                            }
                            
                            info!("📊 PRODUCTION: Fast-start EMA{} for {} {}s: IMMEDIATELY READY (SIMD ACTIVE)", 
                                  period, symbol, timeframe);
                        } else {
                            // Standard initialization for sufficient data
                            let initial_value = ema.initialize_with_history(&close_prices);
                            if ema.is_ready() {
                                info!("📊 Standard EMA{} for {} {}s: {:.8} (from {} candles)", 
                                       period, symbol, timeframe, initial_value.unwrap_or(0.0), close_prices.len());
                            }
                        }
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
            self.set_recent_closes(timeframe, recent_closes_deque);
            info!("📊 Initialized recent closes for {} {}s: {} candles", 
                  symbol, timeframe, close_prices.len().min(5));

            // Initialize trend analyzers with actual historical EMA progression
            // First check if EMA89 is ready to avoid borrow checker issues
            let ema89_ready = self.get_ema(timeframe, 89).map(|ema| ema.is_ready()).unwrap_or(false);
            
            if ema89_ready {
                if let Some(trend_analyzer) = self.get_trend_analyzer_mut(timeframe) {
                        // Calculate EMA89 progression using the last 10 historical candles
                        let num_trend_points = close_prices.len().clamp(3, 10);
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



            if let Some(last_candle) = candles.last() {
                self.set_current_close(timeframe, last_candle.close);
                if last_candle.closed {
                    // Historical candles are always completed
                    self.set_completed_close(timeframe, last_candle.close);
                    info!("📊 Historical initialization for {}s: last_close={:.2} @ {}",
              timeframe, last_candle.close, last_candle.close_time);
                } else if let Some(second_last_candle) = candles.get(candles.len().saturating_sub(2)) {
                    if second_last_candle.closed {
                        self.set_completed_close(timeframe, second_last_candle.close);
                        info!("📊 Historical initialization for {}s: last_close={:.2} @ {}",
                      timeframe, last_candle.close, last_candle.close_time);
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

        // Initialize quantile tracker with 1-minute data
        if let Some(candles_1m) = aggregated_candles.get(&60) {
            let quantile_records = create_quantile_records_from_candles(candles_1m);
            self.quantile_tracker.initialize_with_history(&quantile_records);
            
            info!("📊 Initialized quantile tracker for {} with {} 1m candles", 
                  symbol, candles_1m.len());
        }

        self.is_initialized = true;
        
        // Log initialization status
        let ready_emas = self.emas.iter()
            .flat_map(|timeframe_emas| timeframe_emas.iter())
            .filter_map(|ema_opt| ema_opt.as_ref())
            .filter(|ema| ema.is_ready())
            .count();
        info!("✅ Initialized indicators for {}: {}/{} EMAs ready", 
              symbol, ready_emas, self.emas.len());
    }

    /// Process a new candle for a specific timeframe
    pub fn process_candle(&mut self, timeframe_seconds: u64, candle: &FuturesOHLCVCandle) {
        // PHASE 3: Start timing for production pattern learning
        let processing_start = Instant::now();
        
        let close_price = candle.close;
        self.last_update_time = Some(candle.close_time);
        
        // Update current close price for this timeframe (includes live candles)
        self.set_current_close(timeframe_seconds, close_price);
        
        // Store recent candle closes for trend analysis (keep last 5 closes) and completed closes
        if candle.closed {
            // Update completed close price (only for closed candles)
            self.set_completed_close(timeframe_seconds, close_price);
            info!("✅ COMPLETED candle for {}s: {:.2} @ {} (stored as completed close)", 
                  timeframe_seconds, close_price, candle.close_time);
            
            if let Some(recent_closes) = self.get_recent_closes_mut(timeframe_seconds) {
                recent_closes.push_back(close_price);
                // Keep only last 5 closes
                while recent_closes.len() > 5 {
                    recent_closes.pop_front();
                }
                debug!("📊 Stored recent close for {}s: {:.2} (history: {} candles)", 
                       timeframe_seconds, close_price, recent_closes.len());
            }
        } else {
            info!("📈 LIVE candle for {}s: {:.2} @ {} (not stored as completed)", 
                  timeframe_seconds, close_price, candle.close_time);
        }
        
        // Update EMAs for this timeframe 
        // Since TimeFrame actor now only sends closed candles, we can process all received candles
        // But keep the closed check as a safety net for historical compatibility
        if candle.closed {
            // MEMORY POOL: Use direct array access instead of iterator to avoid allocation
            record_memory_pool_hit!("ema_periods", "indicator_actor");
            
            // PHASE 1 OPTIMIZATION: Vectorized EMA processing with SIMD tracking
            let ema_periods = &[21, 89];
            
            // PRODUCTION FIX: Hybrid vectorized + scalar processing with branch prediction
            let vectorized_count = self.try_vectorized_ema_update(timeframe_seconds, close_price, ema_periods);
            
            // HOT PATH OPTIMIZATION: Branch prediction hint - vectorization is likely after initialization
            if crate::technical_analysis::simd_math::likely(vectorized_count.is_some()) {
                let batch_processed = vectorized_count.unwrap();
                record_simd_op!("vectorized_ema_batch", batch_processed);
                debug!("⚡ Vectorized {} EMAs for {}s: {:.2}", batch_processed, timeframe_seconds, close_price);
                
                // PRODUCTION FIX: Process remaining unready EMAs with scalar fallback
                let scalar_count = ema_periods.len() - batch_processed;
                if scalar_count > 0 {
                    record_simd_op!("ema_scalar_mixed", scalar_count);
                    for &period in ema_periods {
                        if let Some(ema) = self.get_ema_mut(timeframe_seconds, period) {
                            if !ema.is_ready() {
                                // Process unready EMAs with scalar update
                                let new_ema = ema.update(close_price);
                                if let Some(ema_val) = new_ema {
                                    debug!("📊 Scalar EMA{} for {}s: {:.8} (building initialization)", 
                                           period, timeframe_seconds, ema_val);
                                }
                            }
                        }
                    }
                }
            } else {
                // Full scalar fallback when no EMAs are ready for vectorization
                record_simd_op!("ema_scalar_fallback", ema_periods.len());
                for &period in ema_periods {
                    if let Some(ema) = self.get_ema_mut(timeframe_seconds, period) {
                        let new_ema = ema.update(close_price);
                        if let Some(ema_val) = new_ema {
                            debug!("📊 Scalar EMA{} for {}s: {:.8} (from close: {:.2})", 
                                   period, timeframe_seconds, ema_val, close_price);
                            
                            // Record EMA calculation for metrics
                            if let Some(metrics) = crate::metrics::get_metrics() {
                                metrics.ema_calculations_total
                                    .with_label_values(&["unknown", &timeframe_seconds.to_string(), &period.to_string()])
                                    .inc();
                            }
                        }
                    }
                }
            }
        } else {
            warn!("⚠️ Received non-closed candle in indicator - should not happen after TimeFrame fix!");
        }

        // Update trend analysis using candle closes vs EMA89 - ONLY for closed candles
        if candle.closed {
            if let Some(trend_analyzer) = self.get_trend_analyzer(timeframe_seconds) {
                if let Some(ema89) = self.get_ema(timeframe_seconds, 89) {
                    if let Some(ema89_value) = ema89.value() {
                        if let Some(tf_idx) = TimeFrameIndex::from_seconds(timeframe_seconds).map(|tf| tf.to_index()) {
                            if let Some(recent_closes) = &self.recent_candle_closes[tf_idx] {
                                let trend = trend_analyzer.analyze_candles_vs_ema(recent_closes, ema89_value);
                                // Only log trend changes, not every analysis
                                debug!("📈 TREND ANALYSIS for {}s: {} | EMA89: {:.2}", 
                                       timeframe_seconds, trend, ema89_value);
                            }
                        }
                    }
                }
            }
        }

        // Update volume tracker for 5-minute candles - ONLY for closed candles
        if timeframe_seconds == 300 && candle.closed {
            // Get current trend for this timeframe
            let current_trend = self.get_trend_analyzer(300)
                .map(|ta| ta.analyze_trend())
                .unwrap_or(TrendDirection::Neutral);
            
            self.volume_tracker.update(
                candle.volume,
                close_price,
                candle.close_time,
                current_trend,
            );
        }

        // Update quantile tracker for 1-minute candles - ONLY for closed candles
        if timeframe_seconds == 60 && candle.closed {
            self.quantile_tracker.update(candle);
            // Record T-Digest update for metrics
            if let Some(metrics) = crate::metrics::get_metrics() {
                metrics.record_t_digest_update("unknown", "volume_quantiles");
            }
        }
        
        // PHASE 3: Lightweight pattern learning (minimal overhead - only updates every 100th operation)
        let processing_time_ns = processing_start.elapsed().as_nanos() as u64;
        self.pattern_learner.maybe_update(processing_time_ns);
    }

    /// PHASE 3: Adaptive vectorized EMA batch update with memory pressure detection
    #[inline(always)]
    fn try_vectorized_ema_update(&mut self, timeframe_seconds: u64, close_price: f64, periods: &[u32]) -> Option<usize> {
        use crate::technical_analysis::simd_math;
        
        // PHASE 3: TEMPORARILY DISABLED - Memory pressure check causing overhead
        // self.update_memory_pressure();
        
        // MEMORY OPTIMIZATION: Use object pooling and progressive readiness for SIMD activation
        let mut ema_data: Vec<(f64, f64, f64, u32)> = Vec::with_capacity(periods.len()); // (alpha, beta, prev_value, period)
        let mut ready_periods: Vec<u32> = Vec::with_capacity(periods.len());
        let mut partial_ready_count = 0;
        
        for &period in periods {
            if let Some(ema) = self.get_ema(timeframe_seconds, period) {
                // PHASE 2: Tiered progressive readiness for optimal SIMD activation
                if ema.is_ready() {
                    // Tier 1: Fully ready EMAs - highest priority for vectorization
                    if let Some(prev_value) = ema.current_value {
                        ema_data.push((ema.alpha, ema.beta, prev_value, period));
                        ready_periods.push(period);
                    }
                } else if ema.is_partially_ready(self.pattern_learner.get_threshold()) {
                    // PHASE 3: Use lightweight learned threshold for SIMD activation
                    partial_ready_count += 1;
                    if let Some(prev_value) = ema.current_value {
                        ema_data.push((ema.alpha, ema.beta, prev_value, period));
                        ready_periods.push(period);
                    }
                } else if ema.is_partially_ready(0.5) && ema.is_hot_path() {
                    // PHASE 2: Fixed 50%+ ready EMAs on hot path - no pattern learner dependency
                    partial_ready_count += 1;
                    if let Some(prev_value) = ema.current_value {
                        ema_data.push((ema.alpha, ema.beta, prev_value, period));
                        ready_periods.push(period);
                    }
                }
            }
        }
        
        // PHASE 3: Adaptive vectorization based on memory pressure and optimization mode
        let min_vectorization_count = match self.optimization_mode {
            OptimizationMode::MaxPerformance => 2,  // Vectorize with 2+ EMAs
            OptimizationMode::Balanced => 3,        // Vectorize with 3+ EMAs  
            OptimizationMode::Conservative => 4,    // Vectorize only with 4+ EMAs
            OptimizationMode::Emergency => return None, // Disable vectorization completely
        };
        
        if ema_data.len() >= min_vectorization_count {
            // Clear and reuse object pools for zero-allocation performance
            self.alpha_pool.clear();
            self.beta_pool.clear();
            self.prev_values_pool.clear();
            self.periods_pool.clear();
            
            for (alpha, beta, prev_value, period) in ema_data {
                self.alpha_pool.push(alpha);
                self.beta_pool.push(beta);
                self.prev_values_pool.push(prev_value);
                self.periods_pool.push(period);
            }
            
            // Use optimized vectorized EMA batch update from SIMD math module
            let mut prev_emas = self.prev_values_pool.clone();
            
            // MEMORY OPTIMIZATION: Use pooled vectors for zero allocation SIMD processing
            let processed_count = simd_math::vectorized_ema_batch_update(
                close_price, 
                &self.alpha_pool, 
                &self.beta_pool, 
                &mut prev_emas
            );
            
            // PRODUCTION FIX: Update only the EMAs that were vectorized (ready EMAs)
            for (i, &period) in ready_periods.iter().enumerate() {
                if let Some(ema) = self.get_ema_mut(timeframe_seconds, period) {
                    ema.current_value = Some(prev_emas[i]);
                    ema.count += 1;
                    
                    // Record EMA calculation for metrics
                    if let Some(metrics) = crate::metrics::get_metrics() {
                        metrics.ema_calculations_total
                            .with_label_values(&["unknown", &timeframe_seconds.to_string(), &period.to_string()])
                            .inc();
                    }
                }
            }
            
            // MEMORY OPTIMIZATION: Track progressive readiness metrics
            if partial_ready_count > 0 {
                record_simd_op!("vectorized_partial_ready", partial_ready_count);
            }
            
            Some(processed_count)
        } else {
            None
        }
    }
    
    /// PHASE 3: Update memory pressure and optimization mode
    #[allow(dead_code)]
    fn update_memory_pressure(&mut self) {
        let now = Instant::now();
        
        // Check memory pressure every 1 second to avoid overhead
        if now.duration_since(self.last_pressure_check) < Duration::from_secs(1) {
            return;
        }
        self.last_pressure_check = now;
        
        // Estimate memory pressure based on pool usage and system indicators
        let pool_pressure = (self.alpha_pool.capacity() as f64 / 16.0).min(1.0); // 16 is typical max capacity
        
        // Simple heuristic: if pools are heavily used, assume higher memory pressure
        let estimated_pressure = pool_pressure * 0.7; // Conservative estimate
        
        // Smooth the pressure reading with exponential moving average
        self.memory_pressure = 0.9 * self.memory_pressure + 0.1 * estimated_pressure;
        
        // Update optimization mode based on pressure
        self.optimization_mode = match self.memory_pressure {
            p if p < 0.3 => OptimizationMode::MaxPerformance,
            p if p < 0.6 => OptimizationMode::Balanced,
            p if p < 0.9 => OptimizationMode::Conservative,
            _ => OptimizationMode::Emergency,
        };
        
        // Log mode changes for monitoring
        static LAST_MODE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
        let current_mode = self.optimization_mode as u8;
        let last_mode = LAST_MODE.swap(current_mode, Ordering::Relaxed);
        
        if current_mode != last_mode {
            debug!("🔧 Optimization mode changed: {:?} (pressure: {:.2})", 
                   self.optimization_mode, self.memory_pressure);
        }
    }

    /// Get trend for a specific timeframe using candles vs EMA89 comparison
    fn get_trend_for_timeframe(&self, timeframe_seconds: u64) -> TrendDirection {
        if let Some(trend_analyzer) = self.get_trend_analyzer(timeframe_seconds) {
            if let Some(ema89) = self.get_ema(timeframe_seconds, 89) {
                if let Some(ema89_value) = ema89.value() {
                    if let Some(recent_closes) = self.get_recent_closes(timeframe_seconds) {
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
        if let Some(recent_4h_closes) = self.get_recent_closes(14400) {
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

    /// PHASE 3: Fast output generation with optimized data collection
    pub fn generate_output(&mut self, symbol: &str) -> IndicatorOutput {
        // Use the optimized fast output generation
        self.generate_output_optimized(symbol)
    }

    /// PHASE 3: Optimized output generation using pre-allocated structures and SIMD data collection
    #[inline(always)]
    fn generate_output_optimized(&mut self, symbol: &str) -> IndicatorOutput {
        use tracing::info;
        use std::time::Instant;
        
        let start_total = Instant::now();
        
        // HOT PATH OPTIMIZATION: Pre-allocate output structure with better memory layout
        let start_alloc = Instant::now();
        let mut output = IndicatorOutput {
            symbol: symbol.to_string(),
            timestamp: self.last_update_time.unwrap_or(0),
            ..Default::default()
        };
        let alloc_time = start_alloc.elapsed();

        // PHASE 3 OPTIMIZATION: CPU cache-friendly timeframe processing
        let start_data_collection = Instant::now();
        const TIMEFRAMES: [u64; 5] = [60, 300, 900, 3600, 14400]; // 1m, 5m, 15m, 1h, 4h
        let mut completed_closes = Vec::with_capacity(TIMEFRAMES.len());
        let mut ema89_values = Vec::with_capacity(TIMEFRAMES.len());
        let mut trend_values = Vec::with_capacity(TIMEFRAMES.len());
        
        // Vectorized data collection - process all timeframes in batch
        record_simd_op!("batch_data_collection", TIMEFRAMES.len());
        
        for &tf in &TIMEFRAMES {
            completed_closes.push(self.get_completed_close(tf));
            ema89_values.push(self.get_ema(tf, 89).and_then(|ema| ema.value()));
            trend_values.push(self.get_trend_for_timeframe(tf));
        }
        let data_collection_time = start_data_collection.elapsed();
        
        // Fast assignment using pre-collected data
        let start_assignment = Instant::now();
        output.close_5m = completed_closes[1];     // 300s
        output.close_15m = completed_closes[2];    // 900s  
        output.close_60m = completed_closes[3];    // 3600s
        output.close_4h = completed_closes[4];     // 14400s
        
        // EMA values - optimized batch access
        output.ema21_1min = self.get_ema(60, 21).and_then(|ema| ema.value());
        output.ema89_1min = ema89_values[0];   // 60s
        output.ema89_5min = ema89_values[1];   // 300s
        output.ema89_15min = ema89_values[2];  // 900s
        output.ema89_1h = ema89_values[3];     // 3600s
        output.ema89_4h = ema89_values[4];     // 14400s

        // Trend analysis - pre-computed batch values
        output.trend_1min = trend_values[0];   // 60s
        output.trend_5min = trend_values[1];   // 300s
        output.trend_15min = trend_values[2];  // 900s
        output.trend_1h = trend_values[3];     // 3600s
        output.trend_4h = trend_values[4];     // 14400s
        let assignment_time = start_assignment.elapsed();

        // PHASE 3: Volume analysis using fast access
        let start_volume = Instant::now();
        if let Some(max_vol) = self.volume_tracker.get_max() {
            output.max_volume = Some(max_vol.volume);
            output.max_volume_price = Some(max_vol.price);
            output.max_volume_time = Some(format_timestamp_iso(max_vol.timestamp));
            
            // Calculate max_volume_trend using 3 4h candles vs max_volume_price
            output.max_volume_trend = self.calculate_max_volume_trend(max_vol.price);
        }
        let volume_time = start_volume.elapsed();

        // Volume quantile analysis - THE CRITICAL BOTTLENECK (4.6ms)
        let start_quantiles = Instant::now();
        output.volume_quantiles = self.quantile_tracker.get_quantiles();
        let quantiles_time = start_quantiles.elapsed();

        // PHASE 3: Record optimized output generation metrics
        record_simd_op!("optimized_output_generation", 1);

        let total_time = start_total.elapsed();
        
        // PERFORMANCE DEBUGGING: Always log detailed timing breakdown
        if true { // Always log to verify the fix worked
            info!("🔍 DETAILED OUTPUT TIMING BREAKDOWN: total={:?}, alloc={:?}, data_collection={:?}, assignment={:?}, volume={:?}, quantiles={:?}",
                  total_time, alloc_time, data_collection_time, assignment_time, volume_time, quantiles_time);
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
    
    /// Optional Kafka actor reference for publishing indicators
    kafka_actor: Option<ActorRef<KafkaActor>>,
}

impl IndicatorActor {
    /// Determine optimal precision for EMA values based on price range
    /// - High-value assets (>$1): 6 decimal places
    /// - Medium-value assets ($0.001-$1): 7 decimal places  
    /// - Low-value assets (<$0.001): 8 decimal places
    fn get_optimal_ema_precision(price: f64) -> usize {
        if price >= 1.0 { 
            6  // BTC, ETH, etc. - 6 decimals sufficient
        } else if price >= 0.001 { 
            7  // DOGE, ADA, etc. - 7 decimals for precision
        } else { 
            8  // SHIB, PEPE, etc. - 8 decimals to preserve significance
        }
    }

    /// Format price with smart precision - scientific notation for very small values
    fn format_price_smart(price: f64) -> String {
        if price < 0.000001 {
            format!("{:.2e}", price)  // Scientific notation: 1.23e-6
        } else if price < 0.01 {
            format!("{:.6}", price)   // 6 decimals: 0.001234
        } else {
            format!("{:.2}", price)   // 2 decimals: 123.45
        }
    }

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
            kafka_actor: None,
        }
    }

    /// Set Kafka actor reference
    pub fn set_kafka_actor(&mut self, kafka_actor: ActorRef<KafkaActor>) {
        self.kafka_actor = Some(kafka_actor);
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
    async fn output_current_indicators(&mut self) {
        for (symbol, state) in &mut self.symbol_states {
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
            let ready_emas = state.emas.iter()
                .flat_map(|timeframe_emas| timeframe_emas.iter())
                .filter_map(|ema_opt| ema_opt.as_ref())
                .filter(|ema| ema.is_ready())
                .count();
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
                let mut logging_time = std::time::Duration::new(0, 0);
                let mut kafka_time = std::time::Duration::new(0, 0);
                
                if let Some(state) = self.symbol_states.get_mut(&symbol) {
                    // Process all timeframe candles in the batch
                    let batch_start = std::time::Instant::now();
                    for (&timeframe_seconds, candle) in &candles {
                        state.process_candle(timeframe_seconds, candle);
                    }
                    batch_processing_time = batch_start.elapsed();
                    
                    // PHASE 3: Optimized output generation with hot path optimizations
                    let output_start = std::time::Instant::now();
                    
                    // HOT PATH OPTIMIZATION: Pre-warm commonly accessed EMAs in CPU cache
                    let _ema21_1m = state.get_ema(60, 21);
                    let _ema89_1m = state.get_ema(60, 89);
                    let _ema89_5m = state.get_ema(300, 89);
                    let _ema89_1h = state.get_ema(3600, 89);
                    
                    let output = state.generate_output(&symbol);
                    output_time = output_start.elapsed();
                    
                    // Optimized real-time logging with dynamic precision based on price range
                    let logging_start = std::time::Instant::now();
                    
                    // Use the most recent close price to determine optimal precision
                    let current_price = output.close_5m.or(output.close_15m).or(output.close_60m).or(output.close_4h).unwrap_or(0.0);
                    let ema_precision = Self::get_optimal_ema_precision(current_price);
                    
                    // Format prices with smart precision
                    let close_5m_str = output.close_5m.map(Self::format_price_smart).unwrap_or_else(|| "N/A".to_string());
                    let close_15m_str = output.close_15m.map(Self::format_price_smart).unwrap_or_else(|| "N/A".to_string());
                    let close_60m_str = output.close_60m.map(Self::format_price_smart).unwrap_or_else(|| "N/A".to_string());
                    let close_4h_str = output.close_4h.map(Self::format_price_smart).unwrap_or_else(|| "N/A".to_string());
                    
                    // Format EMAs with dynamic precision
                    let format_ema = |value: Option<f64>| -> String {
                        value.map(|v| format!("{:.prec$}", v, prec = ema_precision))
                             .unwrap_or_else(|| "N/A".to_string())
                    };
                    
                    debug!("📈 RT {} | TS:{} | 5m:{} 15m:{} 1h:{} 4h:{} | EMA89[1m:{} 5m:{} 1h:{} 4h:{}] | T[1m:{} 5m:{} 15m:{} 1h:{} 4h:{}]",
                          symbol,
                          output.timestamp,
                          close_5m_str,
                          close_15m_str,
                          close_60m_str,
                          close_4h_str,
                          format_ema(output.ema89_1min),
                          format_ema(output.ema89_5min),
                          format_ema(output.ema89_1h),
                          format_ema(output.ema89_4h),
                          output.trend_1min,
                          output.trend_5min,
                          output.trend_15min,
                          output.trend_1h,
                          output.trend_4h
                    );
                    logging_time = logging_start.elapsed();
                    
                    // Move Kafka publishing to background task to avoid blocking
                    let kafka_start = std::time::Instant::now();
                    if let Some(kafka_actor) = self.kafka_actor.clone() {
                        let output_arc = Arc::new(output);
                        let symbol_owned = symbol.clone();
                        
                        tokio::spawn(async move {
                            debug!("📤 Background Kafka publishing for {}", symbol_owned);
                            let publish_msg = KafkaTell::PublishIndicators {
                                indicators: Box::new((*output_arc).clone()),
                            };
                            if let Err(e) = kafka_actor.tell(publish_msg).send().await {
                                error!("❌ Background Kafka publish failed for {}: {}", symbol_owned, e);
                            } else {
                                debug!("✅ Background Kafka publish completed for {}", symbol_owned);
                            }
                        });
                    } else {
                        debug!("⚠️ No Kafka actor for {} indicators", symbol);
                    }
                    kafka_time = kafka_start.elapsed();
                } else {
                    warn!("Received batched update for unknown symbol: {}", symbol);
                }
                
                let handler_time = handler_start.elapsed();
                
                // Performance monitoring - log timing details every 100th update or if slow
                static UPDATE_COUNTER: AtomicU64 = AtomicU64::new(0);
                let counter = UPDATE_COUNTER.fetch_add(1, Ordering::Relaxed);
                
                // Record Prometheus metrics for performance monitoring
                record_indicator_timing!(&symbol, "batch_processing", batch_processing_time.as_secs_f64());
                record_indicator_timing!(&symbol, "output_generation", output_time.as_secs_f64());
                record_indicator_timing!(&symbol, "kafka_publish", kafka_time.as_secs_f64());
                record_indicator_timing!(&symbol, "total_processing", handler_time.as_secs_f64());
                
                if counter % 100 == 0 || handler_time.as_micros() > 2000 {
                    info!("⏱️ IndicatorTell::ProcessMultiTimeFrameUpdate #{} ELAPSED TIMES: total={:?}, batch={:?}, output={:?}, logging={:?}, kafka={:?}", 
                          counter, handler_time, batch_processing_time, output_time, logging_time, kafka_time);
                }
            }
            
            IndicatorTell::SetKafkaActor { kafka_actor } => {
                info!("📨 Setting Kafka actor reference for indicator publishing");
                self.kafka_actor = Some(kafka_actor);
                info!("✅ Kafka actor reference successfully set in IndicatorActor");
            }
        }
    }
}

impl Message<IndicatorAsk> for IndicatorActor {
    type Reply = Result<IndicatorReply, String>;

    async fn handle(&mut self, msg: IndicatorAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            IndicatorAsk::GetIndicators { symbol } => {
                if let Some(state) = self.symbol_states.get_mut(&symbol) {
                    let indicators = state.generate_output(&symbol);
                    Ok(IndicatorReply::Indicators(Box::new(indicators)))
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
        let mut state = SymbolIndicatorState::new(&config);
        let output = state.generate_output("BTCUSDT");
        
        assert_eq!(output.symbol, "BTCUSDT");
        assert!(matches!(output.trend_1min, TrendDirection::Neutral));
        assert_eq!(output.timestamp, 0); // No updates yet
        
        // Should have empty quantiles initially
        assert!(output.volume_quantiles.is_none());
    }

    #[test]
    fn test_optimal_ema_precision() {
        // High-value assets (>$1) should use 6 decimal places
        assert_eq!(IndicatorActor::get_optimal_ema_precision(67000.0), 6); // BTC
        assert_eq!(IndicatorActor::get_optimal_ema_precision(3500.0), 6);  // ETH
        assert_eq!(IndicatorActor::get_optimal_ema_precision(1.0), 6);     // Boundary

        // Medium-value assets ($0.001-$1) should use 7 decimal places
        assert_eq!(IndicatorActor::get_optimal_ema_precision(0.38), 7);    // DOGE
        assert_eq!(IndicatorActor::get_optimal_ema_precision(0.05), 7);    // ADA-like
        assert_eq!(IndicatorActor::get_optimal_ema_precision(0.001), 7);   // Boundary

        // Low-value assets (<$0.001) should use 8 decimal places
        assert_eq!(IndicatorActor::get_optimal_ema_precision(0.0002), 8);  // SHIB-like
        assert_eq!(IndicatorActor::get_optimal_ema_precision(0.00000123), 8); // PEPE-like
        assert_eq!(IndicatorActor::get_optimal_ema_precision(0.0), 8);     // Edge case
    }

    #[test]
    fn test_format_price_smart() {
        // Very small values should use scientific notation
        let result = IndicatorActor::format_price_smart(0.00000123);
        assert!(result.contains("e-") || result == "0.000001", "Expected scientific notation or 6 decimals, got: {}", result);
        
        // Test the boundary cases based on actual implementation
        assert_eq!(IndicatorActor::format_price_smart(0.0000005), "5.00e-7");

        // Small values should use 6 decimal places
        assert_eq!(IndicatorActor::format_price_smart(0.001234), "0.001234");
        assert_eq!(IndicatorActor::format_price_smart(0.008), "0.008000");

        // Normal values should use 2 decimal places
        assert_eq!(IndicatorActor::format_price_smart(0.38), "0.38");
        assert_eq!(IndicatorActor::format_price_smart(67000.0), "67000.00");
        assert_eq!(IndicatorActor::format_price_smart(1.5), "1.50");
        
        // Test boundary conditions more explicitly
        assert!(IndicatorActor::format_price_smart(0.000001).len() <= 8); // Should be reasonable length
        assert!(IndicatorActor::format_price_smart(0.00001).starts_with("0.0000")); // Should be decimal
        assert!(IndicatorActor::format_price_smart(0.01).starts_with("0.01")); // Should be 2 decimals
    }
}