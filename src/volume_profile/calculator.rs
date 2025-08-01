use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};
use super::structs::{
    VolumeProfileConfig, VolumeProfileData, ValueArea, 
    PriceLevelMap, PriceIncrementMode, PriceKey
};

/// Daily volume profile calculator with real-time incremental updates
#[derive(Debug, Clone)]
pub struct DailyVolumeProfile {
    /// Trading symbol
    pub symbol: String,
    /// Trading date
    pub date: NaiveDate,
    /// Configuration for this profile
    config: VolumeProfileConfig,
    /// Price level map for efficient volume tracking
    price_levels: PriceLevelMap,
    /// Total volume for the day
    pub total_volume: f64,
    /// Minimum price seen
    min_price: f64,
    /// Maximum price seen
    max_price: f64,
    /// Number of 1-minute candles processed
    pub candle_count: u32,
    /// Last update timestamp
    last_updated: TimestampMS,
    /// Cached VWAP (recalculated when needed)
    cached_vwap: Option<f64>,
    /// Cached POC (recalculated when needed)
    cached_poc: Option<f64>,
    /// Cached value area (recalculated when needed)
    cached_value_area: Option<ValueArea>,
    /// Flag indicating if caches need recalculation
    cache_dirty: bool,
}

impl DailyVolumeProfile {
    /// Create new daily volume profile
    pub fn new(symbol: String, date: NaiveDate, config: VolumeProfileConfig) -> Self {
        let price_increment = Self::calculate_price_increment(&config, None);
        
        Self {
            symbol,
            date,
            config,
            price_levels: PriceLevelMap::new(price_increment),
            total_volume: 0.0,
            min_price: f64::MAX,
            max_price: f64::MIN,
            candle_count: 0,
            last_updated: 0,
            cached_vwap: None,
            cached_poc: None,
            cached_value_area: None,
            cache_dirty: false,
        }
    }

    /// Add new 1-minute candle (incremental update for real-time processing)
    pub fn add_candle(&mut self, candle: &FuturesOHLCVCandle) {
        // Validate candle belongs to this trading day
        if !self.is_candle_for_date(candle) {
            warn!("Candle timestamp {} does not belong to date {}", 
                  candle.open_time, self.date);
            return;
        }

        debug!("Adding candle to volume profile: {} at {} (volume: {:.2})", 
               self.symbol, candle.open_time, candle.volume);

        // Update price range
        self.update_price_range(candle);
        
        // Distribute volume across OHLC price range
        self.distribute_volume_across_range(candle);
        
        // Update statistics
        self.total_volume += candle.volume;
        self.candle_count += 1;
        self.last_updated = candle.close_time;
        
        // Mark caches as dirty
        self.cache_dirty = true;

        debug!("Volume profile updated: {} candles, total volume: {:.2}", 
               self.candle_count, self.total_volume);
    }

    /// Rebuild from complete set of candles (used on startup)
    pub fn rebuild_from_candles(&mut self, candles: &[FuturesOHLCVCandle]) {
        info!("Rebuilding volume profile for {} {} from {} candles", 
              self.symbol, self.date, candles.len());

        // Reset internal state
        self.reset();

        // Process all candles
        for candle in candles {
            self.add_candle(candle);
        }

        info!("Volume profile rebuilt: {} candles processed, total volume: {:.2}", 
              self.candle_count, self.total_volume);
    }

    /// Reset profile to initial state
    pub fn reset(&mut self) {
        let price_increment = Self::calculate_price_increment(&self.config, None);
        self.price_levels = PriceLevelMap::new(price_increment);
        self.total_volume = 0.0;
        self.min_price = f64::MAX;
        self.max_price = f64::MIN;
        self.candle_count = 0;
        self.last_updated = 0;
        self.invalidate_caches();
    }

    /// Get current volume profile data
    pub fn get_profile_data(&mut self) -> VolumeProfileData {
        // Recalculate cached values if needed
        if self.cache_dirty {
            self.recalculate_caches();
        }

        VolumeProfileData {
            date: self.date.format("%Y-%m-%d").to_string(),
            price_levels: self.price_levels.to_price_levels(),
            total_volume: self.total_volume,
            vwap: self.cached_vwap.unwrap_or(0.0),
            poc: self.cached_poc.unwrap_or(0.0),
            value_area: self.cached_value_area.clone().unwrap_or_default(),
            price_increment: self.price_levels.price_increment,
            min_price: if self.min_price == f64::MAX { 0.0 } else { self.min_price },
            max_price: if self.max_price == f64::MIN { 0.0 } else { self.max_price },
            candle_count: self.candle_count,
            last_updated: self.last_updated,
        }
    }

    /// Check if candle belongs to this trading date
    fn is_candle_for_date(&self, candle: &FuturesOHLCVCandle) -> bool {
        // Convert timestamp to UTC date
        if let Some(datetime) = chrono::DateTime::from_timestamp_millis(candle.open_time) {
            let candle_date = datetime.date_naive();
            candle_date == self.date
        } else {
            false
        }
    }

    /// Update min/max price range
    fn update_price_range(&mut self, candle: &FuturesOHLCVCandle) {
        self.min_price = self.min_price.min(candle.low);
        self.max_price = self.max_price.max(candle.high);
    }

    /// Distribute candle volume using standard market profile methodology
    /// All volume is attributed to the closing price (time-based distribution)
    fn distribute_volume_across_range(&mut self, candle: &FuturesOHLCVCandle) {
        let volume = candle.volume;
        if volume <= 0.0 {
            return;
        }

        // Standard market profile: attribute all volume to the closing price
        // This provides cleaner, more realistic volume profiles
        self.price_levels.add_volume(candle.close, volume);
        
        debug!("Assigned volume {:.2} to closing price {:.2}", volume, candle.close);
    }

    /// Calculate appropriate price increment based on configuration and market data
    fn calculate_price_increment(config: &VolumeProfileConfig, price_range: Option<f64>) -> f64 {
        match config.price_increment_mode {
            PriceIncrementMode::Fixed => config.fixed_price_increment,
            PriceIncrementMode::Adaptive => {
                if let Some(range) = price_range {
                    // Adaptive increment based on price range
                    // Target ~100-200 price levels for optimal granularity
                    let target_levels = 150.0;
                    let calculated_increment = range / target_levels;
                    
                    // Clamp to configured min/max
                    calculated_increment
                        .max(config.min_price_increment)
                        .min(config.max_price_increment)
                } else {
                    // Default to middle of range if no price data available
                    (config.min_price_increment + config.max_price_increment) / 2.0
                }
            }
        }
    }

    /// Recalculate cached values (VWAP, POC, Value Area)
    fn recalculate_caches(&mut self) {
        if self.total_volume <= 0.0 {
            self.cached_vwap = Some(0.0);
            self.cached_poc = Some(0.0);
            self.cached_value_area = Some(ValueArea::default());
        } else {
            // Calculate VWAP
            self.cached_vwap = Some(self.price_levels.calculate_vwap());
            
            // Calculate POC (Point of Control)
            self.cached_poc = self.price_levels.get_poc();
            
            // Calculate Value Area
            self.cached_value_area = Some(
                self.price_levels.calculate_value_area(self.config.value_area_percentage)
            );
        }
        
        self.cache_dirty = false;
        
        debug!("Recalculated volume profile caches: VWAP={:.2}, POC={:.2}", 
               self.cached_vwap.unwrap_or(0.0), self.cached_poc.unwrap_or(0.0));
    }

    /// Invalidate all cached calculations
    fn invalidate_caches(&mut self) {
        self.cached_vwap = None;
        self.cached_poc = None;
        self.cached_value_area = None;
        self.cache_dirty = true;
    }

    /// Get current statistics for monitoring
    pub fn get_statistics(&self) -> VolumeProfileStatistics {
        VolumeProfileStatistics {
            symbol: self.symbol.clone(),
            date: self.date,
            candle_count: self.candle_count,
            total_volume: self.total_volume,
            price_levels_count: self.price_levels.levels.len(),
            min_price: if self.min_price == f64::MAX { 0.0 } else { self.min_price },
            max_price: if self.max_price == f64::MIN { 0.0 } else { self.max_price },
            price_increment: self.price_levels.price_increment,
            last_updated: self.last_updated,
        }
    }

    /// Validate data completeness for a trading day
    pub fn validate_data_completeness(&self) -> DataCompleteness {
        const EXPECTED_MINUTE_CANDLES: u32 = 1440; // 24 hours * 60 minutes
        const MINIMUM_ACCEPTABLE: u32 = 1380; // 95% completion rate

        let completeness = if self.candle_count >= EXPECTED_MINUTE_CANDLES {
            100.0
        } else {
            (self.candle_count as f64 / EXPECTED_MINUTE_CANDLES as f64) * 100.0
        };

        let is_complete = self.candle_count >= MINIMUM_ACCEPTABLE;

        DataCompleteness {
            expected_candles: EXPECTED_MINUTE_CANDLES,
            actual_candles: self.candle_count,
            completeness_percentage: completeness,
            is_complete,
            has_data: !self.is_empty(),
        }
    }

    /// Check if profile has any data
    pub fn is_empty(&self) -> bool {
        self.candle_count == 0 || self.total_volume <= 0.0
    }

    /// Get memory usage estimation
    pub fn estimate_memory_usage(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let price_levels_size = self.price_levels.levels.len() * (std::mem::size_of::<PriceKey>() + std::mem::size_of::<f64>());
        let string_size = self.symbol.len();
        
        base_size + price_levels_size + string_size
    }
}

impl Default for ValueArea {
    fn default() -> Self {
        Self {
            high: 0.0,
            low: 0.0,
            volume_percentage: 0.0,
            volume: 0.0,
        }
    }
}

/// Statistics for monitoring volume profile performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfileStatistics {
    pub symbol: String,
    pub date: NaiveDate,
    pub candle_count: u32,
    pub total_volume: f64,
    pub price_levels_count: usize,
    pub min_price: f64,
    pub max_price: f64,
    pub price_increment: f64,
    pub last_updated: TimestampMS,
}

/// Data completeness validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCompleteness {
    pub expected_candles: u32,
    pub actual_candles: u32,
    pub completeness_percentage: f64,
    pub is_complete: bool,
    pub has_data: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn create_test_config() -> VolumeProfileConfig {
        VolumeProfileConfig {
            enabled: true,
            price_increment_mode: PriceIncrementMode::Fixed,
            fixed_price_increment: 0.01,
            min_price_increment: 0.001,
            max_price_increment: 1.0,
            update_frequency: super::super::structs::UpdateFrequency::EveryCandle,
            batch_size: 1,
            value_area_percentage: 70.0,
        }
    }

    fn create_test_candle(open_time: i64, close_time: i64, open: f64, high: f64, low: f64, close: f64, volume: f64) -> FuturesOHLCVCandle {
        FuturesOHLCVCandle {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            number_of_trades: 100,
            taker_buy_base_asset_volume: volume * 0.6,
            closed: true,
        }
    }

    #[test]
    fn test_new_daily_volume_profile() {
        let config = create_test_config();
        let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, config);

        assert_eq!(profile.symbol, "BTCUSDT");
        assert_eq!(profile.date, date);
        assert_eq!(profile.candle_count, 0);
        assert_eq!(profile.total_volume, 0.0);
        assert!(profile.is_empty());
    }

    #[test]
    fn test_add_single_candle() {
        let config = create_test_config();
        let timestamp = 1736985600000; // This actually converts to 2025-01-16 00:00:00 UTC
        // Use the correct date that matches the timestamp
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, config);

        let candle = create_test_candle(timestamp, timestamp + 59999, 50000.0, 50100.0, 49950.0, 50050.0, 1000.0);

        profile.add_candle(&candle);

        assert_eq!(profile.candle_count, 1);
        assert_eq!(profile.total_volume, 1000.0);
        assert!(!profile.is_empty());
        
        let stats = profile.get_statistics();
        assert_eq!(stats.candle_count, 1);
        assert_eq!(stats.total_volume, 1000.0);
        assert!(stats.price_levels_count > 0);
    }

    #[test]
    fn test_volume_distribution() {
        let config = create_test_config();
        let timestamp = 1736985600000; // This actually converts to 2025-01-16 00:00:00 UTC
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, config);
        let candle = create_test_candle(timestamp, timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);

        profile.add_candle(&candle);

        let profile_data = profile.get_profile_data();
        
        // With time-based distribution (close price only), should have 1 price level
        assert_eq!(profile_data.price_levels.len(), 1);
        assert_eq!(profile_data.price_levels[0].price, 101.0);
        assert_eq!(profile_data.price_levels[0].volume, 1000.0);
        
        // Total volume should equal candle volume
        let total_distributed_volume: f64 = profile_data.price_levels.iter().map(|p| p.volume).sum();
        assert!((total_distributed_volume - 1000.0).abs() < 0.01);
        
        // VWAP should equal close price for single candle
        assert!((profile_data.vwap - 101.0).abs() < 0.01);
        
        // POC should equal close price
        assert!((profile_data.poc - 101.0).abs() < 0.01);
    }

    #[test]
    fn test_multiple_candles() {
        let config = create_test_config();
        let base_timestamp = 1736985600000; // This actually converts to 2025-01-16 00:00:00 UTC
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, config);
        
        // Add multiple candles with different volumes and prices
        let candles = vec![
            create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 102.0, 98.0, 101.00, 1000.0),
            create_test_candle(base_timestamp + 60000, base_timestamp + 119999, 101.0, 103.0, 100.0, 101.01, 1500.0),
            create_test_candle(base_timestamp + 120000, base_timestamp + 179999, 102.0, 104.0, 101.0, 101.02, 800.0),
        ];

        for candle in &candles {
            profile.add_candle(candle);
        }

        assert_eq!(profile.candle_count, 3);
        assert_eq!(profile.total_volume, 3300.0);

        let profile_data = profile.get_profile_data();
        
        // With time-based distribution, should have 3 price levels (close prices)
        assert_eq!(profile_data.price_levels.len(), 3);
        
        // With 3 discrete price levels, value area should include highest volume levels
        // to achieve as close to 70% as possible with available levels
        assert!(profile_data.value_area.volume_percentage >= 60.0, 
                "Value area should capture majority of volume, got {:.2}%", 
                profile_data.value_area.volume_percentage);
        
        // Should include the POC (highest volume level)
        let poc_level = profile_data.price_levels.iter().max_by(|a, b| 
            a.volume.partial_cmp(&b.volume).unwrap_or(std::cmp::Ordering::Equal)
        ).unwrap();
        assert!(profile_data.value_area.low <= poc_level.price && 
                profile_data.value_area.high >= poc_level.price);
    }

    #[test]
    fn test_rebuild_from_candles() {
        let config = create_test_config();
        let base_timestamp = 1736985600000; // This actually converts to 2025-01-16 00:00:00 UTC
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, config);
        let candles = vec![
            create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0),
            create_test_candle(base_timestamp + 60000, base_timestamp + 119999, 101.0, 103.0, 100.0, 102.0, 1500.0),
        ];

        profile.rebuild_from_candles(&candles);

        assert_eq!(profile.candle_count, 2);
        assert_eq!(profile.total_volume, 2500.0);
        
        let profile_data = profile.get_profile_data();
        assert!(profile_data.price_levels.len() > 0);
        assert!(profile_data.vwap > 0.0);
    }

    #[test]
    fn test_wrong_date_candle() {
        let config = create_test_config();
        let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, config);

        // Create candle for different date (Jan 16, 2025)
        let wrong_timestamp = 1737072000000; // 2025-01-16 12:00:00 UTC
        let candle = create_test_candle(wrong_timestamp, wrong_timestamp + 59999, 50000.0, 50100.0, 49950.0, 50050.0, 1000.0);

        profile.add_candle(&candle);

        // Should not add candle from wrong date
        assert_eq!(profile.candle_count, 0);
        assert_eq!(profile.total_volume, 0.0);
        assert!(profile.is_empty());
    }

    #[test]
    fn test_adaptive_price_increment() {
        let mut config = create_test_config();
        config.price_increment_mode = PriceIncrementMode::Adaptive;
        config.min_price_increment = 0.001;
        config.max_price_increment = 1.0;

        // Test with small price range
        let small_range_increment = DailyVolumeProfile::calculate_price_increment(&config, Some(1.0));
        assert!(small_range_increment >= config.min_price_increment);
        assert!(small_range_increment <= config.max_price_increment);

        // Test with large price range
        let large_range_increment = DailyVolumeProfile::calculate_price_increment(&config, Some(1000.0));
        assert!(large_range_increment >= config.min_price_increment);
        assert!(large_range_increment <= config.max_price_increment);
        assert!(large_range_increment > small_range_increment);
    }

    #[test]
    fn test_memory_usage_estimation() {
        let config = create_test_config();
        let base_timestamp = 1736985600000; // This actually converts to 2025-01-16 00:00:00 UTC
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, config);

        let initial_memory = profile.estimate_memory_usage();
        assert!(initial_memory > 0);

        // Add some candles
        for i in 0..10 {
            let candle = create_test_candle(
                base_timestamp + i * 60000, 
                base_timestamp + i * 60000 + 59999, 
                100.0 + i as f64, 
                102.0 + i as f64, 
                98.0 + i as f64, 
                101.0 + i as f64, 
                1000.0
            );
            profile.add_candle(&candle);
        }

        let final_memory = profile.estimate_memory_usage();
        assert!(final_memory > initial_memory);
    }
}