use serde::{Deserialize, Serialize};
use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};
use std::collections::VecDeque;
use tracing::debug;


/// Configuration for technical analysis system
#[derive(Debug, Clone)]
pub struct TechnicalAnalysisConfig {
    /// Symbols to analyze
    pub symbols: Vec<String>,
    /// Minimum days of historical data needed for initialization
    pub min_history_days: u32,
    /// EMA periods to calculate
    pub ema_periods: Vec<u32>,
    /// Timeframes to generate (in seconds)
    pub timeframes: Vec<u64>,
    /// Volume analysis lookback period in days
    pub volume_lookback_days: u32,
}

impl Default for TechnicalAnalysisConfig {
    fn default() -> Self {
        Self {
            symbols: vec!["BTCUSDT".to_string()],
            min_history_days: 30,
            ema_periods: vec![21, 89],
            timeframes: vec![300, 900, 3600, 14400], // 5m, 15m, 1h, 4h
            volume_lookback_days: 30,
        }
    }
}

/// Multi-timeframe candle data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiTimeFrameCandles {
    /// 5-minute candles
    pub candles_5m: Option<FuturesOHLCVCandle>,
    /// 15-minute candles
    pub candles_15m: Option<FuturesOHLCVCandle>,
    /// 1-hour candles
    pub candles_1h: Option<FuturesOHLCVCandle>,
    /// 4-hour candles
    pub candles_4h: Option<FuturesOHLCVCandle>,
}

/// Ring buffer for efficient timeframe aggregation
#[derive(Debug, Clone)]
pub struct CandleRingBuffer {
    /// Internal buffer for candles
    buffer: VecDeque<FuturesOHLCVCandle>,
    /// Maximum capacity
    capacity: usize,
    /// Timeframe duration in milliseconds
    timeframe_ms: i64,
    /// Current aggregated candle being built
    current_candle: Option<FuturesOHLCVCandle>,
    /// Start time of current aggregation period
    current_period_start: Option<TimestampMS>,
}

impl CandleRingBuffer {
    /// Create a new ring buffer for a specific timeframe
    pub fn new(timeframe_seconds: u64, capacity: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(capacity),
            capacity,
            timeframe_ms: (timeframe_seconds * 1000) as i64,
            current_candle: None,
            current_period_start: None,
        }
    }

    /// Add a 1-minute candle and return completed higher timeframe candle if ready
    pub fn add_candle(&mut self, candle: &FuturesOHLCVCandle) -> Option<FuturesOHLCVCandle> {
        // Calculate which period this candle belongs to using open_time for proper alignment
        let period_start = self.calculate_period_start(candle.open_time);
        
        if self.current_period_start != Some(period_start) {
            // Starting a new period
            let completed_candle = self.finalize_current_candle();
            self.start_new_period(period_start, candle);
            completed_candle
        } else {
            // Update current candle with this 1m candle
            self.update_current_candle(candle);
            None
        }
    }

    /// Bulk add candle for historical initialization (no real-time overhead)
    pub fn add_candle_bulk(&mut self, candle: &FuturesOHLCVCandle) -> Option<FuturesOHLCVCandle> {
        // Calculate which period this candle belongs to using open_time for proper alignment
        let period_start = self.calculate_period_start(candle.open_time);
        
        if self.current_period_start != Some(period_start) {
            // Starting a new period - finalize previous and start new
            let completed_candle = self.finalize_current_candle_bulk();
            self.start_new_period_bulk(period_start, candle);
            completed_candle
        } else {
            // Update current candle with this 1m candle (no debugging overhead)
            self.update_current_candle_bulk(candle);
            None
        }
    }

    /// Calculate the start time of the aggregation period for a given timestamp
    fn calculate_period_start(&self, timestamp: TimestampMS) -> TimestampMS {
        // Align timestamp to timeframe boundaries
        // E.g., for 5m (300000ms): timestamps get rounded down to nearest 5-minute boundary
        (timestamp / self.timeframe_ms) * self.timeframe_ms
    }

    /// Start a new aggregation period
    fn start_new_period(&mut self, period_start: TimestampMS, first_candle: &FuturesOHLCVCandle) {
        self.current_period_start = Some(period_start);
        self.current_candle = Some(FuturesOHLCVCandle {
            open_time: period_start,
            close_time: period_start + self.timeframe_ms - 1,
            open: first_candle.open,
            high: first_candle.high,
            low: first_candle.low,
            close: first_candle.close,
            volume: first_candle.volume,
            number_of_trades: first_candle.number_of_trades,
            taker_buy_base_asset_volume: first_candle.taker_buy_base_asset_volume,
            closed: false, // Will be set to true when period completes
        });
        
        // Debug new 4h period starts
        if self.timeframe_ms == 14400000 { // 4h
            let human_start = chrono::DateTime::from_timestamp_millis(period_start)
                .unwrap_or_default()
                .format("%Y-%m-%d %H:%M:%S UTC");
            let human_end = chrono::DateTime::from_timestamp_millis(period_start + self.timeframe_ms - 1)
                .unwrap_or_default()
                .format("%Y-%m-%d %H:%M:%S UTC");
                
            debug!("🆕 NEW 4H PERIOD STARTED: {} → {}", human_start, human_end);
            debug!("   Initial candle: O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.0}",
                  first_candle.open, first_candle.high, first_candle.low, first_candle.close, first_candle.volume);
        }
    }

    /// Start a new aggregation period (bulk version - no debug overhead)
    fn start_new_period_bulk(&mut self, period_start: TimestampMS, first_candle: &FuturesOHLCVCandle) {
        self.current_period_start = Some(period_start);
        self.current_candle = Some(FuturesOHLCVCandle {
            open_time: period_start,
            close_time: period_start + self.timeframe_ms - 1,
            open: first_candle.open,
            high: first_candle.high,
            low: first_candle.low,
            close: first_candle.close,
            volume: first_candle.volume,
            number_of_trades: first_candle.number_of_trades,
            taker_buy_base_asset_volume: first_candle.taker_buy_base_asset_volume,
            closed: false, // Will be set to true when period completes
        });
        // No debug logging for bulk operations
    }

    /// Update the current aggregated candle with a new 1m candle
    fn update_current_candle(&mut self, candle: &FuturesOHLCVCandle) {
        if let Some(current) = &mut self.current_candle {
            // CRITICAL FIX: Proper OHLCV incremental aggregation
            // - Open: NEVER changes (first candle's open)
            // - High: Maximum of all candles in period
            // - Low: Minimum of all candles in period  
            // - Close: Latest candle's close (most recent)
            // - Volume: Sum of all volumes
            // - Trades: Sum of all trades
            // - Taker Volume: Sum of all taker volumes

            current.high = current.high.max(candle.high);
            current.low = current.low.min(candle.low);
            current.close = candle.close; // Latest close
            
            // Accumulate volume data
            current.volume += candle.volume;
            current.number_of_trades += candle.number_of_trades;
            current.taker_buy_base_asset_volume += candle.taker_buy_base_asset_volume;
            
            // Mark as closed if this is the last candle of the period
            if candle.close_time >= current.close_time {
                current.closed = true;
                debug!("🔚 Ring buffer ({}s) period completed: C:{:.2} at {}",
                       self.timeframe_ms / 1000, current.close, current.close_time);
            }
        }
    }

    /// Update the current aggregated candle (bulk version - no debug overhead)
    fn update_current_candle_bulk(&mut self, candle: &FuturesOHLCVCandle) {
        if let Some(current) = &mut self.current_candle {
            // Proper OHLCV incremental aggregation (same logic, no debug)
            current.high = current.high.max(candle.high);
            current.low = current.low.min(candle.low);
            current.close = candle.close; // Latest close
            
            // Accumulate volume data
            current.volume += candle.volume;
            current.number_of_trades += candle.number_of_trades;
            current.taker_buy_base_asset_volume += candle.taker_buy_base_asset_volume;
            
            // Mark as closed if this is the last candle of the period
            if candle.close_time >= current.close_time {
                current.closed = true;
                // No debug logging for bulk operations
            }
        }
    }

    /// Finalize the current candle and add it to the buffer
    fn finalize_current_candle(&mut self) -> Option<FuturesOHLCVCandle> {
        if let Some(mut candle) = self.current_candle.take() {
            let now = chrono::Utc::now().timestamp_millis();

            candle.closed = candle.close_time < now;
            
            // Add to ring buffer
            if self.buffer.len() >= self.capacity {
                self.buffer.pop_front();
            }
            self.buffer.push_back(candle.clone());
            
            Some(candle)
        } else {
            None
        }
    }

    /// Finalize the current candle (bulk version - no timestamp checks)
    fn finalize_current_candle_bulk(&mut self) -> Option<FuturesOHLCVCandle> {
        if let Some(mut candle) = self.current_candle.take() {
            // For historical data, assume all candles are closed
            candle.closed = true;
            
            // Add to ring buffer
            if self.buffer.len() >= self.capacity {
                self.buffer.pop_front();
            }
            self.buffer.push_back(candle.clone());
            
            Some(candle)
        } else {
            None
        }
    }

    /// Get the most recent candles
    pub fn get_recent_candles(&self, count: usize) -> Vec<FuturesOHLCVCandle> {
        self.buffer.iter().rev().take(count).rev().cloned().collect()
    }

    /// Get the current in-progress candle for real-time updates
    pub fn get_current_candle(&self) -> Option<&FuturesOHLCVCandle> {
        self.current_candle.as_ref()
    }

    /// Get the current length of the buffer
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

/// Incremental EMA calculator that maintains state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalEMA {
    /// EMA period
    pub period: u32,
    /// Smoothing factor (2 / (period + 1))
    pub alpha: f64,
    /// Current EMA value
    pub current_value: Option<f64>,
    /// Count of values processed (for initial SMA calculation)
    pub count: u32,
    /// Sum for initial SMA calculation
    pub sum: f64,
    /// Whether EMA is fully initialized
    pub is_initialized: bool,
}

impl IncrementalEMA {
    /// Create a new incremental EMA calculator
    pub fn new(period: u32) -> Self {
        let alpha = 2.0 / (period as f64 + 1.0);
        
        Self {
            period,
            alpha,
            current_value: None,
            count: 0,
            sum: 0.0,
            is_initialized: false,
        }
    }

    /// Add a new price value and return the updated EMA
    pub fn update(&mut self, price: f64) -> Option<f64> {
        self.count += 1;

        if self.count <= self.period {
            // Building initial SMA
            self.sum += price;
            
            if self.count == self.period {
                // Switch to EMA with SMA as first value
                let sma = self.sum / self.period as f64;
                self.current_value = Some(sma);
                self.is_initialized = true;
                Some(sma)
            } else {
                None // Not enough data yet
            }
        } else {
            // Update EMA incrementally
            if let Some(prev_ema) = self.current_value {
                let new_ema = self.alpha * price + (1.0 - self.alpha) * prev_ema;
                let _prev_ema_val = prev_ema;
                self.current_value = Some(new_ema);
                
                
                Some(new_ema)
            } else {
                None
            }
        }
    }

    /// Get current EMA value if available
    pub fn value(&self) -> Option<f64> {
        self.current_value
    }

    /// Check if EMA is fully initialized
    pub fn is_ready(&self) -> bool {
        self.is_initialized
    }

    /// Initialize EMA with historical data
    pub fn initialize_with_history(&mut self, historical_prices: &[f64]) -> Option<f64> {
        use tracing::warn;
        
        // Validate we have enough data
        if historical_prices.len() < self.period as usize {
            warn!("⚠️ Insufficient data for EMA{}: got {}, need {}", 
                  self.period, historical_prices.len(), self.period);
        }
        
        for &price in historical_prices.iter() {
            self.update(price);
            
        }
        
        
        self.current_value
    }
}

/// Trend detection based on EMA and candle patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrendDirection {
    Buy,
    Sell,
    Neutral,
}

impl std::fmt::Display for TrendDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrendDirection::Buy => write!(f, "Buy"),
            TrendDirection::Sell => write!(f, "Sell"),
            TrendDirection::Neutral => write!(f, "No Trend"),
        }
    }
}

/// Trend analyzer for a specific timeframe
#[derive(Debug, Clone)]
pub struct TrendAnalyzer {
    /// Required consecutive candles for trend confirmation
    confirmation_candles: usize,
}

impl TrendAnalyzer {
    /// Create a new trend analyzer
    pub fn new(confirmation_candles: usize) -> Self {
        Self {
            confirmation_candles,
        }
    }

    /// Analyze trend by comparing recent candle closes against EMA89 value
    /// Returns Buy if last N candles are ALL above EMA, Sell if ALL below, otherwise Neutral
    pub fn analyze_candles_vs_ema(&self, recent_candle_closes: &VecDeque<f64>, current_ema89: f64) -> TrendDirection {
        // Need at least confirmation_candles to analyze trend
        if recent_candle_closes.len() < self.confirmation_candles {
            return TrendDirection::Neutral;
        }
        
        // Get the last N candles (most recent)
        let last_candles: Vec<f64> = recent_candle_closes.iter()
            .rev()
            .take(self.confirmation_candles)
            .cloned()
            .collect();
        
        // Check if ALL candles are consistently above or below EMA89
        let all_above_ema = last_candles.iter().all(|&close| close > current_ema89);
        let all_below_ema = last_candles.iter().all(|&close| close < current_ema89);
        
        if all_above_ema {
            TrendDirection::Buy
        } else if all_below_ema {
            TrendDirection::Sell
        } else {
            TrendDirection::Neutral
        }
    }

    /// DEPRECATED: Old method for backward compatibility - not used anymore
    pub fn update(&mut self, _ema_value: f64) -> TrendDirection {
        // This method is deprecated and should not be used
        // Use analyze_candles_vs_ema instead
        TrendDirection::Neutral
    }

    /// DEPRECATED: Old method for backward compatibility
    pub fn analyze_trend(&self) -> TrendDirection {
        TrendDirection::Neutral
    }

    /// DEPRECATED: Old initialization method for backward compatibility
    pub fn initialize_with_history(&mut self, _historical_ema: &[f64]) {
        // No longer needed - trend analysis now uses direct candle-vs-EMA comparison
    }
}

/// Volume tracking for maximum volume analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeRecord {
    pub volume: f64,
    pub price: f64,
    pub timestamp: TimestampMS,
    pub trend: TrendDirection,
}

/// Maximum volume tracker over a rolling window
#[derive(Debug, Clone)]
pub struct MaxVolumeTracker {
    /// Rolling window of volume records
    records: VecDeque<VolumeRecord>,
    /// Window duration in milliseconds
    window_duration_ms: i64,
    /// Current maximum volume record
    current_max: Option<VolumeRecord>,
}

impl MaxVolumeTracker {
    /// Create a new maximum volume tracker
    pub fn new(window_days: u32) -> Self {
        let window_duration_ms = window_days as i64 * 24 * 3600 * 1000;
        Self {
            records: VecDeque::new(),
            window_duration_ms,
            current_max: None,
        }
    }

    /// Update with new volume data
    pub fn update(&mut self, volume: f64, price: f64, timestamp: TimestampMS, trend: TrendDirection) {
        let record = VolumeRecord {
            volume,
            price,
            timestamp,
            trend,
        };

        // Remove expired records
        let cutoff_time = timestamp - self.window_duration_ms;
        while let Some(front) = self.records.front() {
            if front.timestamp < cutoff_time {
                self.records.pop_front();
            } else {
                break;
            }
        }

        // Add new record
        self.records.push_back(record.clone());

        // Update maximum if this is a new max
        if let Some(current_max) = &self.current_max {
            if record.volume > current_max.volume {
                self.current_max = Some(record);
            }
        } else {
            self.current_max = Some(record);
        }

        // Recalculate max if current max was expired
        if let Some(current_max) = &self.current_max {
            if current_max.timestamp < cutoff_time {
                self.recalculate_max();
            }
        }
    }

    /// Recalculate maximum volume from current records
    fn recalculate_max(&mut self) {
        self.current_max = self.records.iter()
            .max_by(|a, b| a.volume.partial_cmp(&b.volume).unwrap_or(std::cmp::Ordering::Equal))
            .cloned();
    }

    /// Get current maximum volume record
    pub fn get_max(&self) -> Option<&VolumeRecord> {
        self.current_max.as_ref()
    }

    /// Initialize with historical data
    pub fn initialize_with_history(&mut self, historical_records: &[VolumeRecord]) {
        for record in historical_records {
            self.update(record.volume, record.price, record.timestamp, record.trend.clone());
        }
    }
}

/// Quantile data record for volume/trade analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantileRecord {
    pub taker_buy_volume: f64,
    pub sell_buy_volume: f64,    // volume - taker_buy_volume
    pub trade_count: u64,
    pub timestamp: TimestampMS,
}

/// Quantile values for a specific metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantileValues {
    pub q25: f64,    // 25th percentile
    pub q50: f64,    // 50th percentile (median)
    pub q75: f64,    // 75th percentile  
    pub q90: f64,    // 90th percentile
}

impl Default for QuantileValues {
    fn default() -> Self {
        Self {
            q25: 0.0,
            q50: 0.0,
            q75: 0.0,
            q90: 0.0,
        }
    }
}

/// Quantile results for all metrics
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QuantileResults {
    pub taker_buy_volume: QuantileValues,
    pub sell_buy_volume: QuantileValues,
    pub trade_count: QuantileValues,
}

/// Efficient quantile tracker using sorted vectors with binary search
#[derive(Debug, Clone)]
pub struct QuantileTracker {
    /// Rolling window of records (unsorted for O(1) insert/remove)
    records: VecDeque<QuantileRecord>,
    /// Sorted vectors for efficient quantile calculation
    sorted_taker_buy: Vec<f64>,
    sorted_sell_buy: Vec<f64>, 
    sorted_trade_count: Vec<u64>,
    /// Window duration in milliseconds  
    window_duration_ms: i64,
}

impl QuantileTracker {
    /// Create a new quantile tracker with 30-day lookback window
    pub fn new(window_days: u32) -> Self {
        let window_duration_ms = window_days as i64 * 24 * 3600 * 1000;
        // Pre-allocate for ~30 days of 1-minute candles (30 * 24 * 60 = 43200)
        let expected_capacity = (window_days as usize * 24 * 60).max(1000);
        
        Self {
            records: VecDeque::with_capacity(expected_capacity),
            sorted_taker_buy: Vec::with_capacity(expected_capacity),
            sorted_sell_buy: Vec::with_capacity(expected_capacity),
            sorted_trade_count: Vec::with_capacity(expected_capacity),
            window_duration_ms,
        }
    }

    /// Add new record with incremental O(log n) insertion
    pub fn update(&mut self, candle: &FuturesOHLCVCandle) {
        // Calculate sell buy volume
        let sell_buy_volume = candle.volume - candle.taker_buy_base_asset_volume;
        
        let record = QuantileRecord {
            taker_buy_volume: candle.taker_buy_base_asset_volume,
            sell_buy_volume,
            trade_count: candle.number_of_trades,
            timestamp: candle.close_time,
        };
        
        // Remove expired records first (may remove multiple items)
        self.remove_expired_records(record.timestamp);
        
        // Insert into sorted vectors using binary search (O(log n))
        self.insert_into_sorted_vectors(&record);
        
        // Add new record to deque (O(1))
        self.records.push_back(record);
    }

    /// Remove expired records from the front of the deque with incremental sorted vector updates
    fn remove_expired_records(&mut self, current_timestamp: TimestampMS) {
        let cutoff_time = current_timestamp - self.window_duration_ms;
        while let Some(front) = self.records.front() {
            if front.timestamp < cutoff_time {
                let expired_record = self.records.pop_front().unwrap();
                // Remove from sorted vectors using binary search (O(log n))
                self.remove_from_sorted_vectors(&expired_record);
            } else {
                break;
            }
        }
    }
    
    /// Insert record into sorted vectors using binary search O(log n)
    fn insert_into_sorted_vectors(&mut self, record: &QuantileRecord) {
        // Insert taker buy volume
        let taker_pos = self.sorted_taker_buy.binary_search_by(|probe| {
            probe.partial_cmp(&record.taker_buy_volume).unwrap_or(std::cmp::Ordering::Equal)
        }).unwrap_or_else(|pos| pos);
        self.sorted_taker_buy.insert(taker_pos, record.taker_buy_volume);

        // Insert sell buy volume
        let sell_pos = self.sorted_sell_buy.binary_search_by(|probe| {
            probe.partial_cmp(&record.sell_buy_volume).unwrap_or(std::cmp::Ordering::Equal)
        }).unwrap_or_else(|pos| pos);
        self.sorted_sell_buy.insert(sell_pos, record.sell_buy_volume);

        // Insert trade count
        let trade_pos = self.sorted_trade_count.binary_search(&record.trade_count)
            .unwrap_or_else(|pos| pos);
        self.sorted_trade_count.insert(trade_pos, record.trade_count);
    }

    /// Remove record from sorted vectors using binary search O(log n)
    fn remove_from_sorted_vectors(&mut self, record: &QuantileRecord) {
        // Remove taker buy volume
        if let Ok(pos) = self.sorted_taker_buy.binary_search_by(|probe| {
            probe.partial_cmp(&record.taker_buy_volume).unwrap_or(std::cmp::Ordering::Equal)
        }) {
            self.sorted_taker_buy.remove(pos);
        }

        // Remove sell buy volume
        if let Ok(pos) = self.sorted_sell_buy.binary_search_by(|probe| {
            probe.partial_cmp(&record.sell_buy_volume).unwrap_or(std::cmp::Ordering::Equal)
        }) {
            self.sorted_sell_buy.remove(pos);
        }

        // Remove trade count
        if let Ok(pos) = self.sorted_trade_count.binary_search(&record.trade_count) {
            self.sorted_trade_count.remove(pos);
        }
    }

    /// Calculate quantiles with O(1) access to already-sorted vectors
    pub fn get_quantiles(&self) -> Option<QuantileResults> {
        if self.records.is_empty() { 
            return None; 
        }
        
        // Vectors are always sorted due to incremental updates - no sorting needed!
        Some(QuantileResults {
            taker_buy_volume: self.calculate_quantiles_for_f64_vec(&self.sorted_taker_buy),
            sell_buy_volume: self.calculate_quantiles_for_f64_vec(&self.sorted_sell_buy),
            trade_count: self.calculate_quantiles_for_u64_vec(&self.sorted_trade_count),
        })
    }


    /// Optimized quantile calculation for f64 values using interpolation
    fn calculate_quantiles_for_f64_vec(&self, sorted_data: &[f64]) -> QuantileValues {
        let len = sorted_data.len();
        if len == 0 { 
            return QuantileValues::default(); 
        }
        
        QuantileValues {
            q25: self.interpolate_f64_quantile(sorted_data, 0.25),
            q50: self.interpolate_f64_quantile(sorted_data, 0.50),
            q75: self.interpolate_f64_quantile(sorted_data, 0.75),
            q90: self.interpolate_f64_quantile(sorted_data, 0.90),
        }
    }

    /// Optimized quantile calculation for u64 values
    fn calculate_quantiles_for_u64_vec(&self, sorted_data: &[u64]) -> QuantileValues {
        let len = sorted_data.len();
        if len == 0 { 
            return QuantileValues::default(); 
        }
        
        QuantileValues {
            q25: self.interpolate_u64_quantile(sorted_data, 0.25),
            q50: self.interpolate_u64_quantile(sorted_data, 0.50),
            q75: self.interpolate_u64_quantile(sorted_data, 0.75),
            q90: self.interpolate_u64_quantile(sorted_data, 0.90),
        }
    }

    /// Linear interpolation for accurate f64 quantile calculation
    fn interpolate_f64_quantile(&self, sorted_data: &[f64], percentile: f64) -> f64 {
        let len = sorted_data.len();
        let index = percentile * (len - 1) as f64;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;
        
        if lower == upper {
            sorted_data[lower]
        } else {
            let weight = index - lower as f64;
            sorted_data[lower] * (1.0 - weight) + sorted_data[upper] * weight
        }
    }

    /// Linear interpolation for accurate u64 quantile calculation (returns f64)
    fn interpolate_u64_quantile(&self, sorted_data: &[u64], percentile: f64) -> f64 {
        let len = sorted_data.len();
        let index = percentile * (len - 1) as f64;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;
        
        if lower == upper {
            sorted_data[lower] as f64
        } else {
            let weight = index - lower as f64;
            sorted_data[lower] as f64 * (1.0 - weight) + sorted_data[upper] as f64 * weight
        }
    }

    /// Initialize with historical volume records
    pub fn initialize_with_history(&mut self, historical_records: &[QuantileRecord]) {
        for record in historical_records {
            self.records.push_back(record.clone());
            // Insert into sorted vectors incrementally
            self.insert_into_sorted_vectors(record);
        }
    }

    /// Get current number of records in the tracker
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if tracker is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Final output structure for technical indicators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorOutput {
    pub symbol: String,
    pub timestamp: TimestampMS,
    
    // Multi-timeframe close prices
    pub close_5m: Option<f64>,
    pub close_15m: Option<f64>,
    pub close_60m: Option<f64>,
    pub close_4h: Option<f64>,
    
    // EMA indicators for different timeframes
    pub ema21_1min: Option<f64>,
    pub ema89_1min: Option<f64>,
    pub ema89_5min: Option<f64>,
    pub ema89_15min: Option<f64>,
    pub ema89_1h: Option<f64>,
    pub ema89_4h: Option<f64>,
    
    // Trend analysis for each timeframe
    pub trend_1min: TrendDirection,
    pub trend_5min: TrendDirection,
    pub trend_15min: TrendDirection,
    pub trend_1h: TrendDirection,
    pub trend_4h: TrendDirection,
    
    // Volume analysis
    pub max_volume: Option<f64>,
    pub max_volume_price: Option<f64>,
    pub max_volume_time: Option<String>, // ISO format
    pub max_volume_trend: Option<TrendDirection>,

    // Volume quantile analysis (30-day lookback)
    pub volume_quantiles: Option<QuantileResults>,
}

impl Default for IndicatorOutput {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            timestamp: 0,
            close_5m: None,
            close_15m: None,
            close_60m: None,
            close_4h: None,
            ema21_1min: None,
            ema89_1min: None,
            ema89_5min: None,
            ema89_15min: None,
            ema89_1h: None,
            ema89_4h: None,
            trend_1min: TrendDirection::Neutral,
            trend_5min: TrendDirection::Neutral,
            trend_15min: TrendDirection::Neutral,
            trend_1h: TrendDirection::Neutral,
            trend_4h: TrendDirection::Neutral,
            max_volume: None,
            max_volume_price: None,
            max_volume_time: None,
            max_volume_trend: None,
            volume_quantiles: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_incremental_ema_calculation() {
        let mut ema = IncrementalEMA::new(3);
        
        // Not enough data initially
        assert_eq!(ema.update(10.0), None);
        assert_eq!(ema.update(12.0), None);
        
        // SMA calculation at period completion
        let first_ema = ema.update(14.0).unwrap();
        assert!((first_ema - 12.0).abs() < 0.01); // (10+12+14)/3 = 12
        
        // Incremental EMA updates
        let second_ema = ema.update(16.0).unwrap();
        assert!(second_ema > first_ema); // Should be higher due to increasing prices
    }

    #[test]
    fn test_trend_analyzer() {
        let analyzer = TrendAnalyzer::new(3);
        
        // Test rising trend - all candles above EMA
        let mut rising_candles = VecDeque::new();
        rising_candles.push_back(101.0);
        rising_candles.push_back(102.0);
        rising_candles.push_back(103.0);
        let trend = analyzer.analyze_candles_vs_ema(&rising_candles, 100.0); // EMA below all candles
        assert!(matches!(trend, TrendDirection::Buy));
        
        // Test falling trend - all candles below EMA
        let mut falling_candles = VecDeque::new();
        falling_candles.push_back(99.0);
        falling_candles.push_back(98.0);
        falling_candles.push_back(97.0);
        let trend = analyzer.analyze_candles_vs_ema(&falling_candles, 100.0); // EMA above all candles
        assert!(matches!(trend, TrendDirection::Sell));
        
        // Test neutral trend - mixed candles
        let mut mixed_candles = VecDeque::new();
        mixed_candles.push_back(99.0);  // Below EMA
        mixed_candles.push_back(101.0); // Above EMA
        mixed_candles.push_back(102.0); // Above EMA
        let trend = analyzer.analyze_candles_vs_ema(&mixed_candles, 100.0);
        assert!(matches!(trend, TrendDirection::Neutral));
    }

    #[test]
    fn test_candle_ring_buffer() {
        let mut buffer = CandleRingBuffer::new(300, 100); // 5-minute timeframe
        
        let candle1 = FuturesOHLCVCandle {
            open_time: 0,
            close_time: 59999, // First minute
            open: 100.0,
            high: 105.0,
            low: 95.0,
            close: 103.0,
            volume: 1000.0,
            number_of_trades: 50,
            taker_buy_base_asset_volume: 600.0,
            closed: true,
        };
        
        // Should not complete 5m candle yet
        assert!(buffer.add_candle(&candle1).is_none());
        assert_eq!(buffer.len(), 0);
        
        // Should have current candle
        assert!(buffer.get_current_candle().is_some());
        let current = buffer.get_current_candle().unwrap();
        assert_eq!(current.close, 103.0);
    }

    #[test]
    fn test_ring_buffer_current_candle() {
        let mut buffer = CandleRingBuffer::new(300, 100); // 5-minute buffer
        
        // Initially no current candle
        assert!(buffer.get_current_candle().is_none());
        
        // Add first candle - should be current candle
        let candle1 = FuturesOHLCVCandle {
            open_time: 0,
            close_time: 59999,
            open: 100.0,
            high: 105.0,
            low: 95.0,
            close: 103.0,
            volume: 1000.0,
            number_of_trades: 50,
            taker_buy_base_asset_volume: 600.0,
            closed: true,
        };
        let completed = buffer.add_candle(&candle1);
        assert!(completed.is_none()); // No completed candle yet
        assert!(buffer.get_current_candle().is_some()); // Should have current candle
        
        // Current candle should match what we added
        let current = buffer.get_current_candle().unwrap();
        assert_eq!(current.close, 103.0);
    }

    #[test]
    fn test_quantile_tracker() {
        let mut tracker = QuantileTracker::new(30); // 30-day window
        
        // Test with empty tracker
        assert!(tracker.get_quantiles().is_none());
        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);
        
        // Add test candles
        let test_candles = vec![
            FuturesOHLCVCandle {
                open_time: 0,
                close_time: 1000,
                open: 100.0,
                high: 105.0,
                low: 95.0,
                close: 103.0,
                volume: 1000.0,
                number_of_trades: 50,
                taker_buy_base_asset_volume: 600.0,
                closed: true,
            },
            FuturesOHLCVCandle {
                open_time: 1000,
                close_time: 2000,
                open: 103.0,
                high: 108.0,
                low: 102.0,
                close: 106.0,
                volume: 1500.0,
                number_of_trades: 75,
                taker_buy_base_asset_volume: 900.0,
                closed: true,
            },
            FuturesOHLCVCandle {
                open_time: 2000,
                close_time: 3000,
                open: 106.0,
                high: 110.0,
                low: 104.0,
                close: 108.0,
                volume: 2000.0,
                number_of_trades: 100,
                taker_buy_base_asset_volume: 1200.0,
                closed: true,
            },
        ];
        
        // Add all candles
        for candle in &test_candles {
            tracker.update(candle);
        }
        
        // Should have 3 records
        assert_eq!(tracker.len(), 3);
        assert!(!tracker.is_empty());
        
        // Get quantiles
        let quantiles = tracker.get_quantiles().unwrap();
        
        // Verify taker buy volume quantiles (600.0, 900.0, 1200.0)
        assert_eq!(quantiles.taker_buy_volume.q50, 900.0); // Median
        assert_eq!(quantiles.taker_buy_volume.q25, 750.0); // Between 600 and 900
        assert_eq!(quantiles.taker_buy_volume.q75, 1050.0); // Between 900 and 1200
        
        // Verify sell buy volume quantiles (400.0, 600.0, 800.0)
        assert_eq!(quantiles.sell_buy_volume.q50, 600.0); // Median
        
        // Verify trade count quantiles (50, 75, 100)
        assert_eq!(quantiles.trade_count.q50, 75.0); // Median
    }

    #[test]
    fn test_quantile_interpolation() {
        let tracker = QuantileTracker::new(30);
        
        // Test f64 interpolation
        let data_f64 = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(tracker.interpolate_f64_quantile(&data_f64, 0.0), 1.0);
        assert_eq!(tracker.interpolate_f64_quantile(&data_f64, 1.0), 5.0);
        assert_eq!(tracker.interpolate_f64_quantile(&data_f64, 0.5), 3.0);
        assert_eq!(tracker.interpolate_f64_quantile(&data_f64, 0.25), 2.0);
        
        // Test u64 interpolation
        let data_u64 = vec![10u64, 20u64, 30u64, 40u64, 50u64];
        assert_eq!(tracker.interpolate_u64_quantile(&data_u64, 0.0), 10.0);
        assert_eq!(tracker.interpolate_u64_quantile(&data_u64, 1.0), 50.0);
        assert_eq!(tracker.interpolate_u64_quantile(&data_u64, 0.5), 30.0);
        assert_eq!(tracker.interpolate_u64_quantile(&data_u64, 0.25), 20.0);
    }

    #[test]
    fn test_quantile_with_realistic_trading_data() {
        let mut tracker = QuantileTracker::new(30);
        let current_time = chrono::Utc::now().timestamp_millis();
        
        // Create realistic trading candles with varying volumes and trade counts
        let realistic_candles = vec![
            FuturesOHLCVCandle {
                open_time: current_time,
                close_time: current_time + 299999,
                open: 50000.0,
                high: 50100.0,
                low: 49950.0,
                close: 50050.0,
                volume: 1200.0,
                number_of_trades: 150,
                taker_buy_base_asset_volume: 720.0, // 60% buying pressure
                closed: true,
            },
            FuturesOHLCVCandle {
                open_time: current_time + 300000,
                close_time: current_time + 599999,
                open: 50050.0,
                high: 50200.0,
                low: 50000.0,
                close: 50150.0,
                volume: 800.0,
                number_of_trades: 95,
                taker_buy_base_asset_volume: 320.0, // 40% buying pressure
                closed: true,
            },
            FuturesOHLCVCandle {
                open_time: current_time + 600000,
                close_time: current_time + 899999,
                open: 50150.0,
                high: 50300.0,
                low: 50100.0,
                close: 50250.0,
                volume: 1500.0,
                number_of_trades: 200,
                taker_buy_base_asset_volume: 1125.0, // 75% buying pressure
                closed: true,
            },
        ];
        
        // Update tracker with realistic data
        for candle in &realistic_candles {
            tracker.update(candle);
        }
        
        let quantiles = tracker.get_quantiles().expect("Should have quantile results with data");
        
        // Verify taker buy volume quantiles are non-zero and realistic
        assert!(quantiles.taker_buy_volume.q25 > 0.0, "Q25 taker buy volume should be positive: {}", quantiles.taker_buy_volume.q25);
        assert!(quantiles.taker_buy_volume.q50 > 0.0, "Q50 taker buy volume should be positive: {}", quantiles.taker_buy_volume.q50);
        assert!(quantiles.taker_buy_volume.q75 > 0.0, "Q75 taker buy volume should be positive: {}", quantiles.taker_buy_volume.q75);
        assert!(quantiles.taker_buy_volume.q90 > 0.0, "Q90 taker buy volume should be positive: {}", quantiles.taker_buy_volume.q90);
        
        // Verify sell buy volume quantiles are non-zero and realistic
        assert!(quantiles.sell_buy_volume.q25 > 0.0, "Q25 sell buy volume should be positive: {}", quantiles.sell_buy_volume.q25);
        assert!(quantiles.sell_buy_volume.q50 > 0.0, "Q50 sell buy volume should be positive: {}", quantiles.sell_buy_volume.q50);
        assert!(quantiles.sell_buy_volume.q75 > 0.0, "Q75 sell buy volume should be positive: {}", quantiles.sell_buy_volume.q75);
        assert!(quantiles.sell_buy_volume.q90 > 0.0, "Q90 sell buy volume should be positive: {}", quantiles.sell_buy_volume.q90);
        
        // Verify trade count quantiles are non-zero and realistic
        assert!(quantiles.trade_count.q25 > 0.0, "Q25 trade count should be positive: {}", quantiles.trade_count.q25);
        assert!(quantiles.trade_count.q50 > 0.0, "Q50 trade count should be positive: {}", quantiles.trade_count.q50);
        assert!(quantiles.trade_count.q75 > 0.0, "Q75 trade count should be positive: {}", quantiles.trade_count.q75);
        assert!(quantiles.trade_count.q90 > 0.0, "Q90 trade count should be positive: {}", quantiles.trade_count.q90);
        
        // Verify ordering within each metric (Q25 < Q50 < Q75 < Q90)
        assert!(quantiles.taker_buy_volume.q25 <= quantiles.taker_buy_volume.q50);
        assert!(quantiles.taker_buy_volume.q50 <= quantiles.taker_buy_volume.q75);
        assert!(quantiles.taker_buy_volume.q75 <= quantiles.taker_buy_volume.q90);
        
        assert!(quantiles.sell_buy_volume.q25 <= quantiles.sell_buy_volume.q50);
        assert!(quantiles.sell_buy_volume.q50 <= quantiles.sell_buy_volume.q75);
        assert!(quantiles.sell_buy_volume.q75 <= quantiles.sell_buy_volume.q90);
        
        assert!(quantiles.trade_count.q25 <= quantiles.trade_count.q50);
        assert!(quantiles.trade_count.q50 <= quantiles.trade_count.q75);
        assert!(quantiles.trade_count.q75 <= quantiles.trade_count.q90);
        
        // Verify that taker buy + sell buy = total volume
        let expected_total_volume_q50 = quantiles.taker_buy_volume.q50 + quantiles.sell_buy_volume.q50;
        assert!(expected_total_volume_q50 > 0.0, "Combined volume should be positive");
        
        println!("✅ Realistic quantile test passed!");
        println!("   Taker Buy Volume - Q25: {:.2}, Q50: {:.2}, Q75: {:.2}, Q90: {:.2}", 
                 quantiles.taker_buy_volume.q25, quantiles.taker_buy_volume.q50, 
                 quantiles.taker_buy_volume.q75, quantiles.taker_buy_volume.q90);
        println!("   Sell Buy Volume - Q25: {:.2}, Q50: {:.2}, Q75: {:.2}, Q90: {:.2}", 
                 quantiles.sell_buy_volume.q25, quantiles.sell_buy_volume.q50, 
                 quantiles.sell_buy_volume.q75, quantiles.sell_buy_volume.q90);
        println!("   Trade Count - Q25: {:.2}, Q50: {:.2}, Q75: {:.2}, Q90: {:.2}", 
                 quantiles.trade_count.q25, quantiles.trade_count.q50, 
                 quantiles.trade_count.q75, quantiles.trade_count.q90);
    }

    #[test]
    fn test_quantile_record_creation() {
        let candle = FuturesOHLCVCandle {
            open_time: 0,
            close_time: 1000,
            open: 100.0,
            high: 105.0,
            low: 95.0,
            close: 103.0,
            volume: 1000.0,
            number_of_trades: 50,
            taker_buy_base_asset_volume: 600.0,
            closed: true,
        };
        
        let mut tracker = QuantileTracker::new(30);
        tracker.update(&candle);
        
        // Should have one record
        assert_eq!(tracker.len(), 1);
        
        let quantiles = tracker.get_quantiles().unwrap();
        
        // With single record, all quantiles should be the same
        assert_eq!(quantiles.taker_buy_volume.q25, 600.0);
        assert_eq!(quantiles.taker_buy_volume.q50, 600.0);
        assert_eq!(quantiles.taker_buy_volume.q75, 600.0);
        assert_eq!(quantiles.taker_buy_volume.q90, 600.0);
        
        // Sell buy volume = 1000.0 - 600.0 = 400.0
        assert_eq!(quantiles.sell_buy_volume.q50, 400.0);
        
        // Trade count
        assert_eq!(quantiles.trade_count.q50, 50.0);
    }
}