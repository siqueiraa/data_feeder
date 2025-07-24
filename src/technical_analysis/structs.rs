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

    /// Finalize the current candle and add it to the buffer
    fn finalize_current_candle(&mut self) -> Option<FuturesOHLCVCandle> {
        if let Some(mut candle) = self.current_candle.take() {
            let now = chrono::Utc::now().timestamp_millis();

            if candle.close_time < now {
                candle.closed = true;
            } else {
                candle.closed = false;
            }
            
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
        
        for (_i, &price) in historical_prices.iter().enumerate() {
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
}