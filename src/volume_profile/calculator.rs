use chrono::{NaiveDate, Datelike, Timelike};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};
use super::structs::{
    VolumeProfileConfig, VolumeProfileData, ValueArea, 
    PriceLevelMap, PriceIncrementMode, PriceKey, ResolvedAssetConfig,
    VolumeProfileDebugMetadata, PrecisionMetrics, CalculationPerformance, ValidationFlags
};

#[cfg(test)]
use super::structs::{VolumeDistributionMode, ValueAreaCalculationMode, VolumeProfileCalculationMode};

/// Daily volume profile calculator with real-time incremental updates
#[derive(Debug, Clone)]
pub struct DailyVolumeProfile {
    /// Trading symbol
    pub symbol: String,
    /// Trading date
    pub date: NaiveDate,
    /// Resolved configuration for this specific asset
    config: ResolvedAssetConfig,
    /// Price level map for efficient volume tracking
    price_levels: PriceLevelMap,
    /// Total volume for the day
    pub total_volume: Decimal,
    /// Minimum price seen
    min_price: Decimal,
    /// Maximum price seen
    max_price: Decimal,
    /// Number of 1-minute candles processed
    pub candle_count: u32,
    /// Last update timestamp
    last_updated: TimestampMS,
    /// Cached VWAP (recalculated when needed)
    cached_vwap: Option<Decimal>,
    /// Cached POC (recalculated when needed)
    cached_poc: Option<Decimal>,
    /// Cached value area (recalculated when needed)
    cached_value_area: Option<ValueArea>,
    /// Flag indicating if caches need recalculation
    cache_dirty: bool,
    /// Cache coordinator for enhanced cache management
    cache_coordinator: CacheCoordinator,
    /// Debug metadata for calculation validation (optional)
    debug_metadata: Option<VolumeProfileDebugMetadata>,
}

impl DailyVolumeProfile {
    /// Create new daily volume profile
    pub fn new(symbol: String, date: NaiveDate, global_config: &VolumeProfileConfig) -> Self {
        // Resolve asset-specific configuration
        let config = global_config.resolve_for_asset(&symbol);
        let price_increment = Self::calculate_price_increment(&config, None);
        
        Self {
            symbol,
            date,
            config,
            price_levels: PriceLevelMap::new(price_increment),
            total_volume: Decimal::ZERO,
            min_price: Decimal::MAX,
            max_price: Decimal::MIN,
            candle_count: 0,
            last_updated: 0,
            cached_vwap: None,
            cached_poc: None,
            cached_value_area: None,
            cache_dirty: false,
            cache_coordinator: CacheCoordinator::new(),
            debug_metadata: None,
        }
    }

    /// Add new 1-minute candle (incremental update for real-time processing)
    pub fn add_candle(&mut self, candle: &FuturesOHLCVCandle) {
        // Validate candle belongs to this trading day
        if !self.is_candle_for_date(candle) {
            warn!("Candle timestamp {} does not belong to date {}", 
                  candle.open_time, self.date);
            
            // Track rejected candle in debug metadata
            if let Some(ref mut debug) = self.debug_metadata {
                debug.validation_flags.rejected_candles_count += 1;
                debug.validation_flags.rejection_reasons.push(
                    format!("Date mismatch: candle {} not for date {}", candle.open_time, self.date)
                );
            }
            return;
        }

        debug!("Adding candle to volume profile: {} at {} (volume: {:.2})", 
               self.symbol, candle.open_time, candle.volume);

        // Update price range
        self.update_price_range(candle);
        
        // Distribute volume across OHLC price range
        self.distribute_volume_across_range(candle);
        
        // Update statistics
        self.total_volume += Decimal::try_from(candle.volume).unwrap_or(Decimal::ZERO);
        self.candle_count += 1;
        self.last_updated = candle.close_time;
        
        // Mark caches as dirty with coordinator tracking
        self.cache_dirty = true;
        self.cache_coordinator.mark_dirty();

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
        self.total_volume = Decimal::ZERO;
        self.min_price = Decimal::MAX;
        self.max_price = Decimal::MIN;
        self.candle_count = 0;
        self.last_updated = 0;
        self.invalidate_caches();
    }

    /// Get current volume profile data
    pub fn get_profile_data(&mut self) -> VolumeProfileData {
        // Recalculate cached values if needed
        if self.cache_dirty {
            self.cache_coordinator.record_cache_miss();
            self.recalculate_caches();
        } else {
            self.cache_coordinator.record_cache_hit();
        }

        VolumeProfileData {
            date: self.date.format("%Y-%m-%d").to_string(),
            price_levels: self.price_levels.to_price_levels(),
            total_volume: self.total_volume,
            vwap: self.cached_vwap.unwrap_or(Decimal::ZERO),
            poc: self.cached_poc.unwrap_or(Decimal::ZERO),
            value_area: self.cached_value_area.clone().unwrap_or_default(),
            price_increment: self.price_levels.price_increment,
            min_price: if self.min_price == Decimal::MAX { Decimal::ZERO } else { self.min_price },
            max_price: if self.max_price == Decimal::MIN { Decimal::ZERO } else { self.max_price },
            candle_count: self.candle_count,
            last_updated: self.last_updated,
        }
    }

    /// Check if candle belongs to this trading date with enhanced validation
    fn is_candle_for_date(&self, candle: &FuturesOHLCVCandle) -> bool {
        // Convert timestamp to UTC date with explicit validation
        if let Some(datetime) = chrono::DateTime::from_timestamp_millis(candle.open_time) {
            let candle_date = datetime.date_naive();
            
            // Direct date match (most common case)
            if candle_date == self.date {
                return true;
            }
            
            // Handle timezone boundary conditions (within 1 hour of day boundary)
            let day_difference = (candle_date - self.date).num_days().abs();
            if day_difference == 1 {
                let hour = datetime.time().hour();
                // Accept candles within 1 hour of day boundary to handle timezone shifts
                if hour >= 23 || hour == 0 {
                    debug!("Accepting candle at timezone boundary: candle_date={}, target_date={}, hour={}", 
                           candle_date, self.date, hour);
                    return true;
                }
            }
            
            // Handle potential DST transition cases
            if day_difference == 1 {
                let month = self.date.month();
                if month == 3 || month == 11 { // DST transition months
                    let day = self.date.day();
                    if (8..=15).contains(&day) { // Typical DST transition period
                        let hour = datetime.time().hour();
                        if hour >= 22 || hour <= 2 { // Extended boundary for DST
                            debug!("Accepting candle during potential DST transition: candle_date={}, target_date={}, hour={}", 
                                   candle_date, self.date, hour);
                            return true;
                        }
                    }
                }
            }
            
            // Log rejected candles for debugging
            debug!("Rejecting candle for date mismatch: candle_timestamp={}, candle_date={}, target_date={}", 
                   candle.open_time, candle_date, self.date);
            false
        } else {
            warn!("Invalid candle timestamp: {}", candle.open_time);
            false
        }
    }

    /// Update min/max price range
    fn update_price_range(&mut self, candle: &FuturesOHLCVCandle) {
        self.min_price = self.min_price.min(Decimal::try_from(candle.low).unwrap_or(Decimal::ZERO));
        self.max_price = self.max_price.max(Decimal::try_from(candle.high).unwrap_or(Decimal::ZERO));
    }

    /// Distribute candle volume using the configured distribution method
    fn distribute_volume_across_range(&mut self, candle: &FuturesOHLCVCandle) {
        let volume = candle.volume;
        if volume <= 0.0 {
            return;
        }

        // Store original total for validation
        let original_total = self.price_levels.total_volume();

        // Use the configured volume distribution method
        self.price_levels.distribute_candle_volume(
            Decimal::try_from(candle.open).unwrap_or(Decimal::ZERO),
            Decimal::try_from(candle.high).unwrap_or(Decimal::ZERO),
            Decimal::try_from(candle.low).unwrap_or(Decimal::ZERO),
            Decimal::try_from(candle.close).unwrap_or(Decimal::ZERO),
            Decimal::try_from(volume).unwrap_or(Decimal::ZERO),
            &self.config.volume_distribution_mode
        );
        
        // Validate volume conservation
        let new_total = self.price_levels.total_volume();
        let expected_total = original_total + Decimal::try_from(volume).unwrap_or(Decimal::ZERO);
        
        // Allow reasonable tolerance for financial data
        // Use relative tolerance based on the magnitude of values being compared
        let epsilon_factor = dec!(0.000000000000222); // Approximate f64::EPSILON * 1000 as Decimal
        let relative_tolerance = expected_total.abs() * epsilon_factor;
        let absolute_tolerance = dec!(0.0000000001); // Minimum absolute tolerance for very small differences
        let tolerance = relative_tolerance.max(absolute_tolerance);
        let difference = (new_total - expected_total).abs();
        
        if difference > tolerance {
            warn!("Volume conservation issue: expected={}, actual={}, difference={} (tolerance={})", 
                  expected_total, new_total, difference, tolerance);
        } else {
            debug!("Volume conservation validated: candle_volume={}, total_volume={}, difference={}", 
                   volume, new_total, difference);
        }
        
        debug!("Distributed volume {:.2} using {:?} method across OHLC range [{:.2}, {:.2}, {:.2}, {:.2}]", 
               volume, self.config.volume_distribution_mode, candle.open, candle.high, candle.low, candle.close);
    }

    /// Calculate appropriate price increment based on configuration and market data
    pub fn calculate_price_increment(config: &ResolvedAssetConfig, price_range: Option<Decimal>) -> Decimal {
        match config.price_increment_mode {
            PriceIncrementMode::Fixed => config.fixed_price_increment,
            PriceIncrementMode::Adaptive => {
                if let Some(range) = price_range {
                    // Adaptive increment based on price range and target levels
                    let target_levels = Decimal::from(config.target_price_levels);
                    let calculated_increment = range / target_levels;
                    
                    // Clamp to configured min/max bounds
                    calculated_increment
                        .max(config.min_price_increment)
                        .min(config.max_price_increment)
                } else {
                    // Default to middle of range if no price data available
                    use rust_decimal_macros::dec;
                    (config.min_price_increment + config.max_price_increment) / dec!(2.0)
                }
            }
        }
    }

    /// Recalculate cached values (VWAP, POC, Value Area) using dual-method algorithm
    fn recalculate_caches(&mut self) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let start_time = SystemTime::now();
        let calculation_timestamp = start_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        
        if self.total_volume <= Decimal::ZERO {
            self.cached_vwap = Some(Decimal::ZERO);
            self.cached_poc = Some(Decimal::ZERO);
            self.cached_value_area = Some(ValueArea::default());
            
            // Create debug metadata for empty profile
            self.debug_metadata = Some(VolumeProfileDebugMetadata {
                calculation_timestamp,
                algorithm_version: "1.4.0".to_string(),
                precision_metrics: PrecisionMetrics {
                    price_range_span: dec!(0),
                    price_increment_used: self.price_levels.price_increment,
                    total_price_keys: 0,
                    precision_errors_detected: 0,
                    volume_conservation_check: dec!(1),
                },
                performance_metrics: CalculationPerformance {
                    value_area_calculation_time_ms: dec!(0),
                    price_distribution_time_ms: dec!(0),
                    cache_operations_count: 1,
                    memory_usage_bytes: self.estimate_memory_usage(),
                    candles_processed_count: self.candle_count,
                },
                validation_flags: ValidationFlags {
                    degenerate_value_area_detected: true, // Empty profile is degenerate
                    unusual_volume_concentration: false,
                    rejected_candles_count: 0,
                    rejection_reasons: vec!["No volume data available".to_string()],
                    precision_errors_excessive: false,
                },
            });
        } else {
            debug!("Recalculating volume profile caches using {:?} method", self.config.calculation_mode);
            
            // Force cache invalidation to ensure fresh calculations with new algorithm
            self.invalidate_caches();
            
            // Track value area calculation timing
            let va_start = SystemTime::now();
            
            // Calculate VWAP
            self.cached_vwap = Some(self.price_levels.calculate_vwap());
            
            // Calculate POC (Point of Control) using configured method
            self.cached_poc = self.price_levels.identify_poc(&self.config.calculation_mode);
            
            // Calculate Value Area using configured method
            self.cached_value_area = Some(
                self.price_levels.calculate_value_area(
                    self.config.value_area_percentage,
                    &self.config.value_area_calculation_mode,
                    &self.config.calculation_mode
                )
            );
            
            let va_duration = va_start.elapsed().unwrap().as_secs_f64() * 1000.0;
            
            // Collect precision metrics
            let price_range = if self.max_price > self.min_price { self.max_price - self.min_price } else { dec!(0) };
            let total_price_levels = self.price_levels.levels.len();
            
            // Detect edge cases
            let value_area = self.cached_value_area.as_ref().unwrap();
            let degenerate_va = (value_area.high - value_area.low).abs() < dec!(0.000000000000222);
            
            // Check for unusual volume concentration (>80% in single price level)
            let max_volume_at_level = self.price_levels.levels.values().fold(dec!(0), |max, &vol| max.max(vol));
            let unusual_concentration = max_volume_at_level / self.total_volume > dec!(0.8);
            
            debug!("Value area calculation: range=[{:.6}, {:.6}], volume={:.2} ({:.1}%), POC={:.6}", 
                   value_area.low, value_area.high, value_area.volume, value_area.volume_percentage,
                   self.cached_poc.unwrap_or(dec!(0.0)));
            
            if degenerate_va {
                warn!("Degenerate value area detected: VAH={:.6}, VAL={:.6}", value_area.high, value_area.low);
            }
            
            if unusual_concentration {
                debug!("Unusual volume concentration detected: {:.1}% at single price level", 
                       (max_volume_at_level / self.total_volume) * dec!(100.0));
            }
            
            // Create comprehensive debug metadata
            self.debug_metadata = Some(VolumeProfileDebugMetadata {
                calculation_timestamp,
                algorithm_version: "1.4.0".to_string(),
                precision_metrics: PrecisionMetrics {
                    price_range_span: price_range,
                    price_increment_used: self.price_levels.price_increment,
                    total_price_keys: total_price_levels,
                    precision_errors_detected: 0, // Would be populated by price key conversion validation
                    volume_conservation_check: self.total_volume / self.price_levels.total_volume(), // Should be close to 1.0
                },
                performance_metrics: CalculationPerformance {
                    value_area_calculation_time_ms: Decimal::from_f64(va_duration).unwrap_or(dec!(0.0)),
                    price_distribution_time_ms: dec!(0.0), // Would be tracked during candle processing
                    cache_operations_count: 3, // VWAP, POC, Value Area calculations
                    memory_usage_bytes: self.estimate_memory_usage(),
                    candles_processed_count: self.candle_count,
                },
                validation_flags: ValidationFlags {
                    degenerate_value_area_detected: degenerate_va,
                    unusual_volume_concentration: unusual_concentration,
                    rejected_candles_count: 0, // Would be tracked during candle validation
                    rejection_reasons: Vec::new(),
                    precision_errors_excessive: false,
                },
            });
        }
        
        self.cache_dirty = false;
        self.cache_coordinator.reset_after_recalculation();
        
        debug!("Recalculated volume profile caches: VWAP={:.6}, POC={:.6}, VA=[{:.6}, {:.6}]", 
               self.cached_vwap.unwrap_or(dec!(0.0)), 
               self.cached_poc.unwrap_or(dec!(0.0)),
               self.cached_value_area.as_ref().map(|va| va.low).unwrap_or(dec!(0.0)),
               self.cached_value_area.as_ref().map(|va| va.high).unwrap_or(dec!(0.0)));
    }

    /// Invalidate all cached calculations
    fn invalidate_caches(&mut self) {
        self.cache_coordinator.invalidate_before_update();
        self.cached_vwap = None;
        self.cached_poc = None;
        self.cached_value_area = None;
        self.cache_dirty = true;
    }
}

/// Cache state tracking for coordinated invalidation
#[derive(Debug, Clone, PartialEq)]
pub enum CacheState {
    Valid,
    Dirty,
    InvalidatedBeforeUpdate,
}

/// Cache coordinator for enhanced cache management
#[derive(Debug, Clone)]
pub struct CacheCoordinator {
    /// Track if cache was explicitly invalidated before update
    invalidated_before_update: bool,
    /// Track cache coherency state
    state: CacheState,
    /// Cache hit/miss tracking for performance monitoring
    cache_hits: u64,
    /// Cache miss count
    cache_misses: u64,
    /// Timing metrics for cache operations
    last_invalidation_time_ms: Decimal,
    last_recalculation_time_ms: Decimal,
}

impl CacheCoordinator {
    /// Create new cache coordinator
    pub fn new() -> Self {
        Self {
            invalidated_before_update: false,
            state: CacheState::Valid,
            cache_hits: 0,
            cache_misses: 0,
            last_invalidation_time_ms: dec!(0),
            last_recalculation_time_ms: dec!(0),
        }
    }
    
    /// Invalidate cache before update operation
    pub fn invalidate_before_update(&mut self) -> CacheState {
        use std::time::{SystemTime, UNIX_EPOCH};
        let start_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        self.last_invalidation_time_ms = Decimal::from_f64_retain(start_time.as_secs_f64() * 1000.0).unwrap_or(dec!(0));
        
        self.invalidated_before_update = true;
        self.state = CacheState::InvalidatedBeforeUpdate;
        self.state.clone()
    }
    
    /// Validate cache coherency
    pub fn validate_cache_coherency(&self) -> bool {
        // Cache is coherent if it was properly invalidated before updates
        match self.state {
            CacheState::Valid => true,
            CacheState::InvalidatedBeforeUpdate => true,
            CacheState::Dirty => self.invalidated_before_update, // Only coherent if explicitly invalidated first
        }
    }
    
    /// Mark cache as dirty
    pub fn mark_dirty(&mut self) {
        if !self.invalidated_before_update {
            self.state = CacheState::Dirty;
        }
    }
    
    /// Reset coordinator after cache recalculation
    pub fn reset_after_recalculation(&mut self) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let end_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        self.last_recalculation_time_ms = Decimal::from_f64_retain(end_time.as_secs_f64() * 1000.0).unwrap_or(dec!(0));
        
        self.invalidated_before_update = false;
        self.state = CacheState::Valid;
    }
    
    /// Record cache hit
    pub fn record_cache_hit(&mut self) {
        self.cache_hits += 1;
    }
    
    /// Record cache miss
    pub fn record_cache_miss(&mut self) {
        self.cache_misses += 1;
    }
    
    /// Get cache hit rate
    pub fn cache_hit_rate(&self) -> Decimal {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            dec!(0)
        } else {
            Decimal::from(self.cache_hits) / Decimal::from(total)
        }
    }
    
    /// Get cache performance metrics
    pub fn get_cache_metrics(&self) -> CacheMetrics {
        CacheMetrics {
            hits: self.cache_hits,
            misses: self.cache_misses,
            hit_rate: self.cache_hit_rate(),
            last_invalidation_time_ms: self.last_invalidation_time_ms,
            last_recalculation_time_ms: self.last_recalculation_time_ms,
        }
    }
}

impl Default for CacheCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache performance metrics structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetrics {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Cache hit rate as percentage
    pub hit_rate: Decimal,
    /// Last invalidation timestamp in milliseconds
    pub last_invalidation_time_ms: Decimal,
    /// Last recalculation timestamp in milliseconds
    pub last_recalculation_time_ms: Decimal,
}

impl DailyVolumeProfile {
    /// Get current statistics for monitoring
    pub fn get_statistics(&self) -> VolumeProfileStatistics {
        VolumeProfileStatistics {
            symbol: self.symbol.clone(),
            date: self.date,
            candle_count: self.candle_count,
            total_volume: self.total_volume,
            price_levels_count: self.price_levels.levels.len(),
            min_price: if self.min_price == Decimal::MAX { dec!(0.0) } else { self.min_price },
            max_price: if self.max_price == Decimal::MIN { dec!(0.0) } else { self.max_price },
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
        self.candle_count == 0 || self.total_volume <= dec!(0.0)
    }

    /// Get memory usage estimation
    pub fn estimate_memory_usage(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let price_levels_size = self.price_levels.levels.len() * (std::mem::size_of::<PriceKey>() + std::mem::size_of::<f64>());
        let string_size = self.symbol.len();
        
        base_size + price_levels_size + string_size
    }
    
    /// Get cache performance metrics
    pub fn get_cache_metrics(&self) -> CacheMetrics {
        self.cache_coordinator.get_cache_metrics()
    }
    
    /// Get debug metadata for validation and analysis
    pub fn get_debug_metadata(&self) -> Option<&VolumeProfileDebugMetadata> {
        self.debug_metadata.as_ref()
    }
    
    /// Enable debug metadata collection
    pub fn enable_debug_collection(&mut self) {
        if self.debug_metadata.is_none() {
            self.debug_metadata = Some(VolumeProfileDebugMetadata::default());
        }
    }
}

impl Default for ValueArea {
    fn default() -> Self {
        Self {
            high: dec!(0.0),
            low: dec!(0.0),
            volume_percentage: dec!(0.0),
            volume: dec!(0.0),
        }
    }
}

/// Statistics for monitoring volume profile performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfileStatistics {
    pub symbol: String,
    pub date: NaiveDate,
    pub candle_count: u32,
    pub total_volume: Decimal,
    pub price_levels_count: usize,
    pub min_price: Decimal,
    pub max_price: Decimal,
    pub price_increment: Decimal,
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
            target_price_levels: 200,
            fixed_price_increment: 0.01,
            min_price_increment: 0.001,
            max_price_increment: 1.0,
            update_frequency: super::super::structs::UpdateFrequency::EveryCandle,
            batch_size: 1,
            value_area_percentage: 70.0,
            volume_distribution_mode: VolumeDistributionMode::ClosingPrice,
            value_area_calculation_mode: ValueAreaCalculationMode::Traditional,
            calculation_mode: VolumeProfileCalculationMode::Volume,
            asset_overrides: std::collections::HashMap::new(),
            historical_days: 60,
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
        let profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);

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
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);

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
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);
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
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);
        
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
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);
        let candles = vec![
            create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0),
            create_test_candle(base_timestamp + 60000, base_timestamp + 119999, 101.0, 103.0, 100.0, 102.0, 1500.0),
        ];

        profile.rebuild_from_candles(&candles);

        assert_eq!(profile.candle_count, 2);
        assert_eq!(profile.total_volume, 2500.0);
        
        let profile_data = profile.get_profile_data();
        assert!(!profile_data.price_levels.is_empty());
        assert!(profile_data.vwap > 0.0);
    }

    #[test]
    fn test_wrong_date_candle() {
        let config = create_test_config();
        let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);

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

        // Resolve config for testing
        let resolved_config = config.resolve_for_asset("TESTUSDT");
        
        // Test with small price range
        let small_range_increment = DailyVolumeProfile::calculate_price_increment(&resolved_config, Some(1.0));
        assert!(small_range_increment >= resolved_config.min_price_increment);
        assert!(small_range_increment <= resolved_config.max_price_increment);

        // Test with large price range
        let large_range_increment = DailyVolumeProfile::calculate_price_increment(&resolved_config, Some(1000.0));
        assert!(large_range_increment >= resolved_config.min_price_increment);
        assert!(large_range_increment <= resolved_config.max_price_increment);
        assert!(large_range_increment > small_range_increment);
    }

    #[test]
    fn test_memory_usage_estimation() {
        let config = create_test_config();
        let base_timestamp = 1736985600000; // This actually converts to 2025-01-16 00:00:00 UTC
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);

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

    #[test]
    fn test_different_volume_distribution_modes() {
        let base_timestamp = 1736985600000;
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        
        // Test ClosingPrice distribution
        let mut closing_config = create_test_config();
        closing_config.volume_distribution_mode = VolumeDistributionMode::ClosingPrice;
        let mut closing_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &closing_config);
        
        // Test UniformOHLC distribution  
        let mut uniform_config = create_test_config();
        uniform_config.volume_distribution_mode = VolumeDistributionMode::UniformOHLC;
        let mut uniform_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &uniform_config);
        
        let candle = create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 104.0, 96.0, 102.0, 1000.0);
        
        closing_profile.add_candle(&candle);
        uniform_profile.add_candle(&candle);
        
        let closing_data = closing_profile.get_profile_data();
        let uniform_data = uniform_profile.get_profile_data();
        
        // Both should have same total volume
        assert_eq!(closing_data.total_volume, 1000.0);
        assert_eq!(uniform_data.total_volume, 1000.0);
        
        // Closing price should have 1 price level (all at close)
        assert_eq!(closing_data.price_levels.len(), 1);
        assert_eq!(closing_data.price_levels[0].price, 102.0);
        assert_eq!(closing_data.price_levels[0].volume, 1000.0);
        
        // Uniform should have multiple price levels across the range
        assert!(uniform_data.price_levels.len() > 1);
        
        // Check that uniform distribution spreads across expected range
        let min_price = uniform_data.price_levels.iter().map(|p| p.price).fold(f64::INFINITY, f64::min);
        let max_price = uniform_data.price_levels.iter().map(|p| p.price).fold(f64::NEG_INFINITY, f64::max);
        
        assert!(min_price >= 96.0);
        assert!(max_price <= 104.0);
    }

    #[test]
    fn test_traditional_vs_greedy_value_area_in_calculator() {
        let base_timestamp = 1736985600000;
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        
        // Create profiles with different value area calculation modes
        let mut traditional_config = create_test_config();
        traditional_config.value_area_calculation_mode = ValueAreaCalculationMode::Traditional;
        traditional_config.volume_distribution_mode = VolumeDistributionMode::WeightedOHLC;
        
        let mut greedy_config = create_test_config();
        greedy_config.value_area_calculation_mode = ValueAreaCalculationMode::Greedy;
        greedy_config.volume_distribution_mode = VolumeDistributionMode::WeightedOHLC;
        
        let mut traditional_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &traditional_config);
        let mut greedy_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &greedy_config);
        
        // Add candles with varying volumes to create interesting distribution
        let candles = vec![
            create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 5000.0), // High volume
            create_test_candle(base_timestamp + 60000, base_timestamp + 119999, 101.0, 103.0, 99.0, 102.0, 3000.0),
            create_test_candle(base_timestamp + 120000, base_timestamp + 179999, 102.0, 104.0, 100.0, 103.0, 2000.0),
            create_test_candle(base_timestamp + 180000, base_timestamp + 239999, 103.0, 105.0, 101.0, 104.0, 1000.0),
        ];
        
        for candle in &candles {
            traditional_profile.add_candle(candle);
            greedy_profile.add_candle(candle);
        }
        
        let traditional_data = traditional_profile.get_profile_data();
        let greedy_data = greedy_profile.get_profile_data();
        
        // Both should have same total volume and basic stats
        assert_eq!(traditional_data.total_volume, greedy_data.total_volume);
        assert_eq!(traditional_data.candle_count, greedy_data.candle_count);
        
        // Value areas should both target 70% but may differ in range
        // Traditional method expands contiguously from POC, may include less volume
        // Greedy method selects highest volume levels regardless of contiguity
        assert!(traditional_data.value_area.volume_percentage >= 0.0);
        assert!(traditional_data.value_area.volume_percentage <= 100.0);
        assert!(greedy_data.value_area.volume_percentage >= 0.0);
        assert!(greedy_data.value_area.volume_percentage <= 100.0);
        
        // Both should include high-volume areas but may select differently
        assert!(traditional_data.value_area.volume > 0.0);
        assert!(greedy_data.value_area.volume > 0.0);
    }

    #[test]
    fn test_cache_coordinator_basic_operations() {
        let mut coordinator = CacheCoordinator::new();
        
        // Initial state should be valid
        assert!(coordinator.validate_cache_coherency());
        assert_eq!(coordinator.state, CacheState::Valid);
        
        // Invalidate before update
        let state = coordinator.invalidate_before_update();
        assert_eq!(state, CacheState::InvalidatedBeforeUpdate);
        assert!(coordinator.validate_cache_coherency());
        
        // Reset after recalculation
        coordinator.reset_after_recalculation();
        assert_eq!(coordinator.state, CacheState::Valid);
        assert!(coordinator.validate_cache_coherency());
    }

    #[test]
    fn test_cache_coordinator_dirty_tracking() {
        let mut coordinator = CacheCoordinator::new();
        
        // Mark as dirty without prior invalidation - this should be incoherent
        coordinator.mark_dirty();
        assert_eq!(coordinator.state, CacheState::Dirty);
        assert!(!coordinator.validate_cache_coherency()); // Should be incoherent
        
        // Reset and invalidate before marking dirty - this should be coherent
        coordinator = CacheCoordinator::new();
        coordinator.invalidate_before_update();
        coordinator.mark_dirty();
        assert!(coordinator.validate_cache_coherency()); // Should be coherent
    }

    #[test]
    fn test_dual_method_integration_with_daily_volume_profile() {
        // Test Volume method
        let mut volume_config = create_test_config();
        volume_config.calculation_mode = VolumeProfileCalculationMode::Volume;
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut volume_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &volume_config);
        
        // Test TPO method
        let mut tpo_config = create_test_config();
        tpo_config.calculation_mode = VolumeProfileCalculationMode::TPO;
        let mut tpo_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &tpo_config);
        
        let base_timestamp = 1736985600000; // 2025-01-16 00:00:00 UTC
        
        // Create test data where TPO and Volume methods will produce different results
        let candles = vec![
            // Price 100: 1 big volume candle 
            create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 100.0, 100.0, 100.0, 1000.0),
            
            // Price 101: 5 small volume candles (high TPO, low volume per candle)
            create_test_candle(base_timestamp + 60000, base_timestamp + 119999, 101.0, 101.0, 101.0, 101.0, 50.0),
            create_test_candle(base_timestamp + 120000, base_timestamp + 179999, 101.0, 101.0, 101.0, 101.0, 50.0),
            create_test_candle(base_timestamp + 180000, base_timestamp + 239999, 101.0, 101.0, 101.0, 101.0, 50.0),
            create_test_candle(base_timestamp + 240000, base_timestamp + 299999, 101.0, 101.0, 101.0, 101.0, 50.0),
            create_test_candle(base_timestamp + 300000, base_timestamp + 359999, 101.0, 101.0, 101.0, 101.0, 50.0),
            
            // Price 102: 2 medium volume candles
            create_test_candle(base_timestamp + 360000, base_timestamp + 419999, 102.0, 102.0, 102.0, 102.0, 300.0),
            create_test_candle(base_timestamp + 420000, base_timestamp + 479999, 102.0, 102.0, 102.0, 102.0, 300.0),
        ];
        
        // Process candles in both profiles
        for candle in &candles {
            volume_profile.add_candle(candle);
            tpo_profile.add_candle(candle);
        }
        
        let volume_data = volume_profile.get_profile_data();
        let tpo_data = tpo_profile.get_profile_data();
        
        // Both should have same total volume and candle count
        assert_eq!(volume_data.total_volume, 1850.0, "Total volume should be 1850");
        assert_eq!(tpo_data.total_volume, 1850.0, "Total volume should be same for both methods");
        assert_eq!(volume_data.candle_count, 8, "Should have 8 candles");
        assert_eq!(tpo_data.candle_count, 8, "Should have same candle count");
        
        // Volume method POC should be at price 100 (1000 volume)
        assert_eq!(volume_data.poc, 100.0, "Volume POC should be at price 100");
        
        // TPO method POC should be at price 101 (5 candles)
        assert_eq!(tpo_data.poc, 101.0, "TPO POC should be at price 101");
        
        // Both should have valid value areas
        assert!(volume_data.value_area.high >= volume_data.value_area.low, "Volume VA should have valid range");
        assert!(tpo_data.value_area.high >= tpo_data.value_area.low, "TPO VA should have valid range");
        
        // Volume method should include price 100 in value area
        assert!(volume_data.value_area.low <= 100.0 && volume_data.value_area.high >= 100.0, 
                "Volume method should include price 100 in value area");
        
        // TPO method should include price 101 in value area
        assert!(tpo_data.value_area.low <= 101.0 && tpo_data.value_area.high >= 101.0, 
                "TPO method should include price 101 in value area");
    }

    #[test]
    fn test_edge_case_equal_volumes_equal_candles() {
        let config = create_test_config();
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut profile = DailyVolumeProfile::new("TESTUSDT".to_string(), date, &config);
        
        let base_timestamp = 1736985600000;
        
        // Create edge case: all price levels have equal volumes and candle counts
        let candles = vec![
            create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 100.0, 100.0, 100.0, 500.0),
            create_test_candle(base_timestamp + 60000, base_timestamp + 119999, 101.0, 101.0, 101.0, 101.0, 500.0),
            create_test_candle(base_timestamp + 120000, base_timestamp + 179999, 102.0, 102.0, 102.0, 102.0, 500.0),
        ];
        
        for candle in &candles {
            profile.add_candle(candle);
        }
        
        let profile_data = profile.get_profile_data();
        
        // Should handle equal volumes gracefully
        assert!(profile_data.poc >= 100.0 && profile_data.poc <= 102.0, 
                "POC should be within price range for equal volumes");
        assert!(profile_data.value_area.volume_percentage > 0.0, 
                "Should calculate reasonable value area percentage");
        assert!(profile_data.value_area.high >= profile_data.value_area.low, 
                "Should maintain valid value area range");
    }

    #[test]
    fn test_configuration_switching_runtime() {
        let base_timestamp = 1736985600000;
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        
        // Start with Volume method
        let mut config = create_test_config();
        config.calculation_mode = VolumeProfileCalculationMode::Volume;
        let mut profile = DailyVolumeProfile::new("TESTUSDT".to_string(), date, &config);
        
        let candles = vec![
            create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 100.0, 100.0, 100.0, 1000.0),
            create_test_candle(base_timestamp + 60000, base_timestamp + 119999, 101.0, 101.0, 101.0, 101.0, 100.0),
            create_test_candle(base_timestamp + 120000, base_timestamp + 179999, 101.0, 101.0, 101.0, 101.0, 100.0),
            create_test_candle(base_timestamp + 180000, base_timestamp + 239999, 101.0, 101.0, 101.0, 101.0, 100.0),
        ];
        
        for candle in &candles {
            profile.add_candle(candle);
        }
        
        let volume_data = profile.get_profile_data();
        assert_eq!(volume_data.poc, 100.0, "Volume method should identify POC at 100.0");
        
        // Switch to TPO method (simulating runtime configuration change)
        config.calculation_mode = VolumeProfileCalculationMode::TPO;
        let mut tpo_profile = DailyVolumeProfile::new("TESTUSDT".to_string(), date, &config);
        
        for candle in &candles {
            tpo_profile.add_candle(candle);
        }
        
        let tpo_data = tpo_profile.get_profile_data();
        assert_eq!(tpo_data.poc, 101.0, "TPO method should identify POC at 101.0");
        
        // Both should maintain data integrity
        assert_eq!(volume_data.total_volume, tpo_data.total_volume, "Total volume should be consistent");
        assert_eq!(volume_data.candle_count, tpo_data.candle_count, "Candle count should be consistent");
    }

    #[test]
    fn test_business_rules_compliance_integration() {
        let config = create_test_config();
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut profile = DailyVolumeProfile::new("TESTUSDT".to_string(), date, &config);
        
        let base_timestamp = 1736985600000;
        
        // Create realistic trading data
        let candles = vec![
            create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 100.5, 99.5, 100.2, 500.0),
            create_test_candle(base_timestamp + 60000, base_timestamp + 119999, 100.2, 100.8, 100.0, 100.6, 800.0),
            create_test_candle(base_timestamp + 120000, base_timestamp + 179999, 100.6, 101.2, 100.3, 101.0, 1200.0), // POC area
            create_test_candle(base_timestamp + 180000, base_timestamp + 239999, 101.0, 101.5, 100.7, 101.3, 900.0),
            create_test_candle(base_timestamp + 240000, base_timestamp + 299999, 101.3, 101.8, 101.0, 101.5, 600.0),
        ];
        
        for candle in &candles {
            profile.add_candle(candle);
        }
        
        let profile_data = profile.get_profile_data();
        
        // Validate business rules compliance
        assert!(profile_data.value_area.high > profile_data.value_area.low, 
                "VAH should be greater than VAL");
        assert!(profile_data.poc >= profile_data.value_area.low && profile_data.poc <= profile_data.value_area.high, 
                "POC should be within value area: POC={:.2}, VAL={:.2}, VAH={:.2}", 
                profile_data.poc, profile_data.value_area.low, profile_data.value_area.high);
        assert!(profile_data.value_area.volume_percentage >= 50.0 && profile_data.value_area.volume_percentage <= 100.0, 
                "Value area percentage should be reasonable: {:.2}%", profile_data.value_area.volume_percentage);
    }

    #[test] 
    fn test_performance_comparison_tpo_vs_volume() {
        use std::time::Instant;
        
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let base_timestamp = 1736985600000;
        
        // Create large dataset for performance testing
        let mut candles = Vec::new();
        for i in 0..1000 {
            let price = 100.0 + (i % 50) as f64 * 0.01; // Spread across 50 price levels
            let volume = 100.0 + (i % 20) as f64 * 10.0; // Varying volumes
            candles.push(create_test_candle(
                base_timestamp + i * 60000,
                base_timestamp + i * 60000 + 59999,
                price, price + 0.005, price - 0.005, price + 0.002,
                volume
            ));
        }
        
        // Test Volume method performance
        let mut volume_config = create_test_config();
        volume_config.calculation_mode = VolumeProfileCalculationMode::Volume;
        let mut volume_profile = DailyVolumeProfile::new("PERFTEST".to_string(), date, &volume_config);
        
        let volume_start = Instant::now();
        for candle in &candles {
            volume_profile.add_candle(candle);
        }
        let _volume_data = volume_profile.get_profile_data();
        let volume_duration = volume_start.elapsed();
        
        // Test TPO method performance
        let mut tpo_config = create_test_config();
        tpo_config.calculation_mode = VolumeProfileCalculationMode::TPO;
        let mut tpo_profile = DailyVolumeProfile::new("PERFTEST".to_string(), date, &tpo_config);
        
        let tpo_start = Instant::now();
        for candle in &candles {
            tpo_profile.add_candle(candle);
        }
        let _tpo_data = tpo_profile.get_profile_data();
        let tpo_duration = tpo_start.elapsed();
        
        // Both methods should complete in reasonable time (< 100ms for 1000 candles)
        assert!(volume_duration.as_millis() < 100, 
                "Volume method should complete in <100ms, took {}ms", volume_duration.as_millis());
        assert!(tpo_duration.as_millis() < 100, 
                "TPO method should complete in <100ms, took {}ms", tpo_duration.as_millis());
        
        println!("Performance comparison: Volume={}ms, TPO={}ms", 
                 volume_duration.as_millis(), tpo_duration.as_millis());
    }

    #[test]
    fn test_cache_coordinator_integration() {
        let config = VolumeProfileConfig::default();
        let mut profile = DailyVolumeProfile::new(
            "TESTUSDT".to_string(),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            &config
        );

        // Initial state should have coherent cache
        assert!(profile.cache_coordinator.validate_cache_coherency());
        
        // Add candle - this should mark cache as dirty
        let candle = create_test_candle(1640995200000, 1640995260000, 50000.0, 50001.0, 49999.0, 50000.5, 1000.0);
        profile.add_candle(&candle);
        
        // Cache should still be coherent (coordinator tracks the dirty marking)
        assert!(profile.cache_coordinator.validate_cache_coherency());
        
        // Get profile data - this triggers cache recalculation (miss)
        let _data = profile.get_profile_data();
        
        // After recalculation, cache should be valid and coherent
        assert_eq!(profile.cache_coordinator.state, CacheState::Valid);
        assert!(profile.cache_coordinator.validate_cache_coherency());
        
        // Get profile data again - this should be a cache hit
        let _data2 = profile.get_profile_data();
        
        // Verify cache metrics
        let metrics = profile.get_cache_metrics();
        assert_eq!(metrics.hits, 1);
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.hit_rate, 0.5);
    }
    
    #[test]
    fn test_cache_performance_tracking() {
        let mut coordinator = CacheCoordinator::new();
        
        // Test initial metrics
        let initial_metrics = coordinator.get_cache_metrics();
        assert_eq!(initial_metrics.hits, 0);
        assert_eq!(initial_metrics.misses, 0);
        assert_eq!(initial_metrics.hit_rate, 0.0);
        
        // Test cache operations with timing
        std::thread::sleep(std::time::Duration::from_millis(1));
        coordinator.invalidate_before_update();
        assert!(coordinator.last_invalidation_time_ms > 0.0);
        
        std::thread::sleep(std::time::Duration::from_millis(1));
        coordinator.reset_after_recalculation();
        assert!(coordinator.last_recalculation_time_ms > coordinator.last_invalidation_time_ms);
        
        // Test hit/miss tracking
        coordinator.record_cache_hit();
        coordinator.record_cache_hit();
        coordinator.record_cache_miss();
        
        let final_metrics = coordinator.get_cache_metrics();
        assert_eq!(final_metrics.hits, 2);
        assert_eq!(final_metrics.misses, 1);
        assert!((final_metrics.hit_rate - (2.0/3.0)).abs() < 0.01);
    }
    
    #[test]
    fn test_cache_timing_integration() {
        let config = create_test_config();
        let mut profile = DailyVolumeProfile::new("CACHE_TEST".to_string(), 
                                                  NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(), 
                                                  &config);
        
        let base_timestamp = 1736985600000;
        let candle = create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        
        // Add candle to make cache dirty
        profile.add_candle(&candle);
        
        // Get profile data to trigger cache recalculation with timing
        let _data = profile.get_profile_data();
        
        let metrics = profile.get_cache_metrics();
        assert!(metrics.last_invalidation_time_ms > 0.0, "Should have invalidation timing");
        assert!(metrics.last_recalculation_time_ms > 0.0, "Should have recalculation timing");
        assert_eq!(metrics.misses, 1, "Should record cache miss");
        
        // Second access should be a hit
        let _data2 = profile.get_profile_data();
        let metrics2 = profile.get_cache_metrics();
        assert_eq!(metrics2.hits, 1, "Should record cache hit");
        assert_eq!(metrics2.hit_rate, 0.5, "Hit rate should be 50%");
    }
    
    #[test]
    fn test_debug_metadata_collection() {
        let config = create_test_config();
        let mut profile = DailyVolumeProfile::new("DEBUG_TEST".to_string(), 
                                                  NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(), 
                                                  &config);
        
        // Initially no debug metadata
        assert!(profile.get_debug_metadata().is_none());
        
        let base_timestamp = 1736985600000;
        let candle = create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        
        // Add candle and trigger calculation
        profile.add_candle(&candle);
        let _data = profile.get_profile_data();
        
        // Should now have debug metadata
        let debug = profile.get_debug_metadata().expect("Should have debug metadata after calculation");
        
        // Verify metadata structure
        assert!(debug.calculation_timestamp > 0, "Should have calculation timestamp");
        assert_eq!(debug.algorithm_version, "1.4.0", "Should have correct algorithm version");
        
        // Verify precision metrics
        assert!(debug.precision_metrics.price_range_span > 0.0, "Should have price range span");
        assert_eq!(debug.precision_metrics.price_increment_used, 0.01, "Should track price increment");
        assert!(debug.precision_metrics.total_price_keys > 0, "Should count price keys");
        assert!((debug.precision_metrics.volume_conservation_check - 1.0).abs() < 0.01, 
                "Volume conservation should be close to 1.0");
        
        // Verify performance metrics
        assert!(debug.performance_metrics.value_area_calculation_time_ms >= 0.0, "Should have timing data");
        assert_eq!(debug.performance_metrics.cache_operations_count, 3, "Should count cache operations");
        assert_eq!(debug.performance_metrics.candles_processed_count, 1, "Should count processed candles");
        assert!(debug.performance_metrics.memory_usage_bytes > 0, "Should estimate memory usage");
        
        // Verify validation flags
        assert!(!debug.validation_flags.degenerate_value_area_detected, "Single candle should not be degenerate");
        assert_eq!(debug.validation_flags.rejected_candles_count, 0, "Should have no rejected candles");
        assert!(debug.validation_flags.rejection_reasons.is_empty(), "Should have no rejection reasons");
    }
    
    #[test]
    fn test_debug_metadata_edge_case_detection() {
        let config = create_test_config();
        let mut profile = DailyVolumeProfile::new("EDGE_TEST".to_string(), 
                                                  NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(), 
                                                  &config);
        
        let base_timestamp = 1736985600000;
        
        // Create scenario with high volume concentration at single price
        let high_volume_candle = create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 100.0, 100.0, 100.0, 9000.0);
        let low_volume_candle = create_test_candle(base_timestamp + 60000, base_timestamp + 119999, 101.0, 101.0, 101.0, 101.0, 1000.0);
        
        profile.add_candle(&high_volume_candle);
        profile.add_candle(&low_volume_candle);
        
        let _data = profile.get_profile_data();
        let debug = profile.get_debug_metadata().expect("Should have debug metadata");
        
        // Should detect unusual volume concentration (90% at price 100.0)
        assert!(debug.validation_flags.unusual_volume_concentration, 
                "Should detect unusual volume concentration");
    }
    
    #[test]
    fn test_debug_metadata_rejected_candles() {
        let config = create_test_config();
        let mut profile = DailyVolumeProfile::new("REJECT_TEST".to_string(), 
                                                  NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(), 
                                                  &config);
        
        // Enable debug collection before adding candles
        profile.enable_debug_collection();
        
        let base_timestamp = 1736985600000;
        let valid_candle = create_test_candle(base_timestamp, base_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        let invalid_candle = create_test_candle(base_timestamp + 25 * 60 * 60 * 1000, base_timestamp + 25 * 60 * 60 * 1000 + 59999, 100.0, 102.0, 98.0, 101.0, 500.0); // Next day + 1 hour
        
        // Add valid candle
        profile.add_candle(&valid_candle);
        assert_eq!(profile.candle_count, 1);
        
        // Add invalid candle (should be rejected)
        profile.add_candle(&invalid_candle);
        assert_eq!(profile.candle_count, 1); // Should not increase
        
        let debug = profile.get_debug_metadata().expect("Should have debug metadata");
        assert_eq!(debug.validation_flags.rejected_candles_count, 1, "Should track rejected candle");
        assert!(!debug.validation_flags.rejection_reasons.is_empty(), "Should have rejection reason");
        assert!(debug.validation_flags.rejection_reasons[0].contains("Date mismatch"), 
                "Should specify date mismatch reason");
    }
    
    #[test]
    fn test_debug_metadata_degenerate_value_area() {
        let config = create_test_config();
        let mut profile = DailyVolumeProfile::new("DEGENERATE_TEST".to_string(), 
                                                  NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(), 
                                                  &config);
        
        // Create empty profile scenario
        let _data = profile.get_profile_data();
        let debug = profile.get_debug_metadata().expect("Should have debug metadata for empty profile");
        
        // Empty profile should be flagged as degenerate
        assert!(debug.validation_flags.degenerate_value_area_detected, 
                "Empty profile should be detected as degenerate");
        assert!(!debug.validation_flags.rejection_reasons.is_empty(), 
                "Should have reason for degenerate state");
        assert!(debug.validation_flags.rejection_reasons[0].contains("No volume data"), 
                "Should specify no volume data reason");
    }

    #[test]
    fn test_enhanced_date_validation_basic() {
        let config = create_test_config();
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);

        // Test exact date match
        let exact_timestamp = 1736985600000; // 2025-01-16 00:00:00 UTC
        let exact_candle = create_test_candle(exact_timestamp, exact_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        assert!(profile.is_candle_for_date(&exact_candle));

        // Test clearly wrong date (outside timezone boundary window)
        let wrong_timestamp = 1737072000000 + 5 * 60 * 60 * 1000; // 2025-01-17 05:00:00 UTC (next day, well outside boundary)
        let wrong_candle = create_test_candle(wrong_timestamp, wrong_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        assert!(!profile.is_candle_for_date(&wrong_candle));
    }

    #[test]
    fn test_enhanced_date_validation_timezone_boundary() {
        let config = create_test_config();
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);

        // Test candle at 23:30 of previous day (should be accepted due to timezone boundary)
        let boundary_timestamp = 1736985600000 - 30 * 60 * 1000; // 2025-01-15 23:30:00 UTC
        let boundary_candle = create_test_candle(boundary_timestamp, boundary_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        assert!(profile.is_candle_for_date(&boundary_candle));

        // Test candle at 00:30 of next day (should be accepted due to timezone boundary)
        let next_boundary_timestamp = 1736985600000 + 24 * 60 * 60 * 1000 + 30 * 60 * 1000; // 2025-01-17 00:30:00 UTC
        let next_boundary_candle = create_test_candle(next_boundary_timestamp, next_boundary_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        assert!(profile.is_candle_for_date(&next_boundary_candle));

        // Test candle at 02:00 of next day (should be rejected - too far from boundary)
        let far_timestamp = 1736985600000 + 24 * 60 * 60 * 1000 + 2 * 60 * 60 * 1000; // 2025-01-17 02:00:00 UTC
        let far_candle = create_test_candle(far_timestamp, far_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        assert!(!profile.is_candle_for_date(&far_candle));
    }

    #[test]
    fn test_enhanced_date_validation_dst_transition() {
        let config = create_test_config();
        
        // Test March DST transition (second Sunday in March 2025 is March 9th)
        let march_date = NaiveDate::from_ymd_opt(2025, 3, 9).unwrap();
        let march_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), march_date, &config);

        // March 9, 2025 00:00:00 UTC
        let march_base_timestamp = march_date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
        
        // Test candle from previous day at 22:30 (should be accepted during DST period)
        let dst_boundary_timestamp = march_base_timestamp - (1.5 * 60.0 * 60.0 * 1000.0) as i64; // 22:30 previous day
        let dst_candle = create_test_candle(dst_boundary_timestamp, dst_boundary_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        assert!(march_profile.is_candle_for_date(&dst_candle));

        // Test November DST transition (using a date within DST transition window - Nov 10, 2025)
        let november_date = NaiveDate::from_ymd_opt(2025, 11, 10).unwrap(); // Within typical DST window
        let november_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), november_date, &config);

        let november_base_timestamp = november_date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
        
        // Test candle from next day at 01:30 (should be accepted during DST period)
        let dst_next_timestamp = november_base_timestamp + 24 * 60 * 60 * 1000 + (1.5 * 60.0 * 60.0 * 1000.0) as i64; // 01:30 next day
        let dst_next_candle = create_test_candle(dst_next_timestamp, dst_next_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        assert!(november_profile.is_candle_for_date(&dst_next_candle));
    }

    #[test]
    fn test_enhanced_date_validation_leap_year() {
        let config = create_test_config();
        
        // Test leap year date (2024 is a leap year)
        let leap_date = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let leap_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), leap_date, &config);

        // Test candle on leap day
        let leap_timestamp = leap_date.and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_millis();
        let leap_candle = create_test_candle(leap_timestamp, leap_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        assert!(leap_profile.is_candle_for_date(&leap_candle));

        // Test non-leap year (2025)
        let non_leap_date = NaiveDate::from_ymd_opt(2025, 2, 28).unwrap();
        let non_leap_profile = DailyVolumeProfile::new("BTCUSDT".to_string(), non_leap_date, &config);

        let non_leap_timestamp = non_leap_date.and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_millis();
        let non_leap_candle = create_test_candle(non_leap_timestamp, non_leap_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        assert!(non_leap_profile.is_candle_for_date(&non_leap_candle));
    }

    #[test]
    fn test_enhanced_date_validation_edge_cases() {
        let config = create_test_config();
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);

        // Test invalid timestamp
        let invalid_candle = FuturesOHLCVCandle {
            open_time: -1, // Invalid timestamp
            close_time: 0,
            open: 100.0,
            high: 102.0,
            low: 98.0,
            close: 101.0,
            volume: 1000.0,
            number_of_trades: 100,
            taker_buy_base_asset_volume: 600.0,
            closed: true,
        };
        assert!(!profile.is_candle_for_date(&invalid_candle));

        // Test timestamp at max i64 (should be invalid)
        let max_timestamp_candle = FuturesOHLCVCandle {
            open_time: i64::MAX,
            close_time: i64::MAX,
            open: 100.0,
            high: 102.0,
            low: 98.0,
            close: 101.0,
            volume: 1000.0,
            number_of_trades: 100,
            taker_buy_base_asset_volume: 600.0,
            closed: true,
        };
        assert!(!profile.is_candle_for_date(&max_timestamp_candle));
    }

    #[test]
    fn test_comprehensive_date_validation_logging() {
        let config = create_test_config();
        let date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);

        // Test that accepted candles are logged and processed correctly
        let valid_timestamp = 1736985600000; // 2025-01-16 00:00:00 UTC
        let valid_candle = create_test_candle(valid_timestamp, valid_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 1000.0);
        
        profile.add_candle(&valid_candle);
        assert_eq!(profile.candle_count, 1);
        assert_eq!(profile.total_volume, 1000.0);

        // Test that rejected candles are logged but not processed (use clearly invalid timestamp)
        let wrong_timestamp = 1737072000000 + 10 * 60 * 60 * 1000; // 2025-01-17 10:00:00 UTC (next day, well outside boundary)
        let wrong_candle = create_test_candle(wrong_timestamp, wrong_timestamp + 59999, 100.0, 102.0, 98.0, 101.0, 500.0);
        
        profile.add_candle(&wrong_candle);
        assert_eq!(profile.candle_count, 1); // Should not increase
        assert_eq!(profile.total_volume, 1000.0); // Should not change
    }
}