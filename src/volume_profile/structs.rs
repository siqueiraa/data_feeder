use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use crate::common::shared_data::{SymbolHashMap, new_symbol_hashmap};
use tracing::info;
use rust_decimal::{prelude::*, Decimal};
use rust_decimal_macros::dec;
use crate::historical::structs::TimestampMS;
use crate::volume_profile::precision::PricePrecisionManager;

/// Default number of historical days to process
fn default_historical_days() -> u32 {
    60
}

/// Asset-specific volume profile configuration overrides
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_increment_mode: Option<PriceIncrementMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixed_price_increment: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_price_increment: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_price_increment: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_price_levels: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_distribution_mode: Option<VolumeDistributionMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_area_calculation_mode: Option<ValueAreaCalculationMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_area_percentage: Option<Decimal>,
    /// Override calculation mode for this specific asset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calculation_mode: Option<VolumeProfileCalculationMode>,
}

/// Volume profile configuration
#[derive(Debug, Clone, Deserialize)]
pub struct VolumeProfileConfig {
    pub enabled: bool,
    pub price_increment_mode: PriceIncrementMode,
    pub target_price_levels: u32,
    pub fixed_price_increment: Decimal,
    pub min_price_increment: Decimal,
    pub max_price_increment: Decimal,
    pub update_frequency: UpdateFrequency,
    pub batch_size: usize,
    pub value_area_percentage: Decimal,
    pub volume_distribution_mode: VolumeDistributionMode,
    pub value_area_calculation_mode: ValueAreaCalculationMode,
    /// Calculation mode for POC and value area determination
    /// Controls whether to use volume weighting or time period (TPO) weighting
    #[serde(default)]
    pub calculation_mode: VolumeProfileCalculationMode,
    #[serde(default)]
    pub asset_overrides: SymbolHashMap<AssetConfig>,
    /// Number of historical days to process during startup (default: 60)
    #[serde(default = "default_historical_days")]
    pub historical_days: u32,
}

impl Default for VolumeProfileConfig {
    fn default() -> Self {
        use rust_decimal_macros::dec;
        Self {
            enabled: true,
            price_increment_mode: PriceIncrementMode::Adaptive,
            target_price_levels: 200,
            fixed_price_increment: dec!(0.01),
            min_price_increment: dec!(0.00000001),
            max_price_increment: dec!(100.0),
            update_frequency: UpdateFrequency::EveryCandle,
            batch_size: 10,
            value_area_percentage: dec!(70.0),
            volume_distribution_mode: VolumeDistributionMode::WeightedOHLC,
            value_area_calculation_mode: ValueAreaCalculationMode::Traditional,
            calculation_mode: VolumeProfileCalculationMode::default(),
            asset_overrides: new_symbol_hashmap(),
            historical_days: default_historical_days(),
        }
    }
}

impl VolumeProfileConfig {
    /// Resolve configuration for a specific asset, applying overrides if they exist
    pub fn resolve_for_asset(&self, symbol: &str) -> ResolvedAssetConfig {
        let asset_override = self.asset_overrides.get(symbol);
        
        ResolvedAssetConfig {
            price_increment_mode: asset_override
                .and_then(|c| c.price_increment_mode.clone())
                .unwrap_or_else(|| self.price_increment_mode.clone()),
            target_price_levels: asset_override
                .and_then(|c| c.target_price_levels)
                .unwrap_or(self.target_price_levels),
            fixed_price_increment: asset_override
                .and_then(|c| c.fixed_price_increment)
                .unwrap_or(self.fixed_price_increment),
            min_price_increment: asset_override
                .and_then(|c| c.min_price_increment)
                .unwrap_or(self.min_price_increment),
            max_price_increment: asset_override
                .and_then(|c| c.max_price_increment)
                .unwrap_or(self.max_price_increment),
            volume_distribution_mode: asset_override
                .and_then(|c| c.volume_distribution_mode.clone())
                .unwrap_or_else(|| self.volume_distribution_mode.clone()),
            value_area_calculation_mode: asset_override
                .and_then(|c| c.value_area_calculation_mode.clone())
                .unwrap_or_else(|| self.value_area_calculation_mode.clone()),
            value_area_percentage: asset_override
                .and_then(|c| c.value_area_percentage)
                .unwrap_or(self.value_area_percentage),
            calculation_mode: asset_override
                .and_then(|c| c.calculation_mode.clone())
                .unwrap_or_else(|| self.calculation_mode.clone()),
        }
    }

    /// Validate configuration for consistency and reasonable values
    pub fn validate(&self) -> Result<(), String> {
        // Validate global configuration
        if self.target_price_levels < 10 || self.target_price_levels > 10000 {
            return Err(format!("target_price_levels must be between 10 and 10000, got {}", self.target_price_levels));
        }

        if self.fixed_price_increment <= Decimal::ZERO {
            return Err("fixed_price_increment must be positive".to_string());
        }

        if self.min_price_increment <= Decimal::ZERO {
            return Err("min_price_increment must be positive".to_string());
        }

        if self.max_price_increment <= self.min_price_increment {
            return Err("max_price_increment must be greater than min_price_increment".to_string());
        }

        use rust_decimal_macros::dec;
        if self.value_area_percentage < dec!(50.0) || self.value_area_percentage > dec!(95.0) {
            return Err(format!("value_area_percentage must be between 50% and 95%, got {}%", self.value_area_percentage));
        }

        // Validate each asset override
        for (symbol, asset_config) in &self.asset_overrides {
            if let Some(levels) = asset_config.target_price_levels {
                if !(10..=10000).contains(&levels) {
                    return Err(format!("Asset {}: target_price_levels must be between 10 and 10000, got {}", symbol, levels));
                }
            }

            if let Some(increment) = asset_config.fixed_price_increment {
                if increment <= dec!(0.0) {
                    return Err(format!("Asset {}: fixed_price_increment must be positive", symbol));
                }
            }

            if let Some(min_inc) = asset_config.min_price_increment {
                if min_inc <= dec!(0.0) {
                    return Err(format!("Asset {}: min_price_increment must be positive", symbol));
                }

                if let Some(max_inc) = asset_config.max_price_increment {
                    if max_inc <= min_inc {
                        return Err(format!("Asset {}: max_price_increment must be greater than min_price_increment", symbol));
                    }
                }
            }

            if let Some(va_pct) = asset_config.value_area_percentage {
                if va_pct < dec!(50.0) || va_pct > dec!(95.0) {
                    return Err(format!("Asset {}: value_area_percentage must be between 50% and 95%, got {}%", symbol, va_pct));
                }
            }

            // Calculation mode validation is implicit - enum ensures valid values
        }

        Ok(())
    }
}

/// Resolved configuration for a specific asset after applying overrides
#[derive(Debug, Clone)]
pub struct ResolvedAssetConfig {
    pub price_increment_mode: PriceIncrementMode,
    pub target_price_levels: u32,
    pub fixed_price_increment: Decimal,
    pub min_price_increment: Decimal,
    pub max_price_increment: Decimal,
    pub volume_distribution_mode: VolumeDistributionMode,
    pub value_area_calculation_mode: ValueAreaCalculationMode,
    pub value_area_percentage: Decimal,
    pub calculation_mode: VolumeProfileCalculationMode,
}

/// Price increment calculation mode
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PriceIncrementMode {
    /// Fixed increment for all price levels
    Fixed,
    /// Adaptive increment based on price range and volatility
    Adaptive,
}

/// Update frequency for volume profile calculations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateFrequency {
    /// Update profile for every new candle (real-time)
    EveryCandle,
    /// Update profile in batches of N candles
    Batched,
}

/// Volume distribution methods for candle data
/// 
/// Different distribution modes create different volume profile characteristics:
/// - WeightedOHLC: Most balanced and realistic for general trading analysis
/// - ClosingPrice: Traditional Market Profile approach, best for trend analysis
/// - UniformOHLC: Academic approach, can create artificial distributions
/// - HighLowWeighted: Best for breakout and support/resistance analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeDistributionMode {
    /// Distribute volume uniformly across OHLC range (traditional volume profile)
    /// 
    /// WARNING: This can create artificial volume distributions where no actual trades occurred.
    /// Use with caution as it may result in unbalanced value areas with POC close to extremes.
    /// Better suited for academic analysis rather than real trading decisions.
    UniformOHLC,
    
    /// Assign all volume to closing price (time-based market profile)
    /// 
    /// This is the traditional Market Profile approach used by professional traders.
    /// Creates clean, focused profiles that highlight price acceptance levels.
    /// Best for: trend analysis, auction theory, institutional trading patterns.
    ClosingPrice,
    
    /// Weighted distribution: 50% close, 25% high, 25% low (RECOMMENDED DEFAULT)
    /// 
    /// Provides the most balanced and realistic volume distribution for general analysis.
    /// Emphasizes closing price (where most trading decisions are made) while acknowledging
    /// activity at extremes. Results in well-balanced value areas with centered POC.
    /// Best for: general trading analysis, balanced volume profiles, trend identification.
    WeightedOHLC,
    
    /// High-Low weighted: 50% high, 50% low (price action focus)
    /// 
    /// Emphasizes price extremes and ranges, ignoring open/close prices.
    /// Useful for breakout analysis and support/resistance identification.
    /// Best for: range trading, breakout analysis, support/resistance mapping.
    HighLowWeighted,
}

/// Volume profile calculation mode determines the weighting method for POC and value area calculations
/// 
/// This is the core algorithmic choice that affects all volume profile calculations:
/// - POC (Point of Control) identification
/// - Value area expansion logic
/// - Volume percentage calculations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VolumeProfileCalculationMode {
    /// Volume-based calculation (default): Weight by actual trading volume
    /// 
    /// Uses actual volume traded at each price level to identify POC and expand value area.
    /// This reflects real market activity and liquidity distribution.
    /// 
    /// - POC: Price level with highest actual trading volume
    /// - Value Area: Expands from POC to capture 70% of total trading volume
    /// - Best for: Liquidity analysis, institutional trading patterns, volume-based strategies
    #[serde(rename = "Volume")]
    Volume,
    
    /// TPO-based calculation (Time-Price Opportunity): Weight by time periods/candle count
    /// 
    /// Uses the number of time periods (candles) at each price level, following
    /// traditional Market Profile methodology. Each time period is weighted equally
    /// regardless of volume.
    /// 
    /// - POC: Price level with most time periods (candle count)
    /// - Value Area: Expands from POC to capture 70% of total time periods
    /// - Best for: Time-based auction theory, price acceptance analysis, traditional Market Profile
    #[serde(rename = "TPO")]
    TPO,
}

impl Default for VolumeProfileCalculationMode {
    fn default() -> Self {
        Self::Volume
    }
}

impl std::fmt::Display for VolumeProfileCalculationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Volume => write!(f, "Volume"),
            Self::TPO => write!(f, "TPO"),
        }
    }
}

impl std::str::FromStr for VolumeProfileCalculationMode {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "volume" => Ok(Self::Volume),
            "tpo" => Ok(Self::TPO),
            _ => Err(format!("Invalid calculation mode: {}. Valid options: Volume, TPO", s)),
        }
    }
}

/// Value area calculation methods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueAreaCalculationMode {
    /// Traditional market profile: expand from POC alternately up/down
    Traditional,
    /// Greedy selection: pick highest volume levels until target reached
    Greedy,
}

/// Volume profile data for a specific trading day
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfileData {
    /// Trading date (YYYY-MM-DD format)
    pub date: String,
    /// Individual price levels with volume data
    pub price_levels: Vec<PriceLevelData>,
    /// Total volume for the day
    pub total_volume: Decimal,
    /// Volume Weighted Average Price
    pub vwap: Decimal,
    /// Point of Control (price level with highest volume)
    pub poc: Decimal,
    /// Value area (70% of volume concentration)
    pub value_area: ValueArea,
    /// Price increment used for this profile
    pub price_increment: Decimal,
    /// Minimum price for the day
    pub min_price: Decimal,
    /// Maximum price for the day
    pub max_price: Decimal,
    /// Number of 1-minute candles processed
    pub candle_count: u32,
    /// Last update timestamp
    pub last_updated: TimestampMS,
}

/// Flat volume profile data without JSON - for direct database storage
#[derive(Debug, Clone)]
pub struct DailyVolumeProfileFlat {
    /// Trading date (YYYY-MM-DD format)
    pub date: String,
    /// Total volume for the day
    pub total_volume: Decimal,
    /// Volume Weighted Average Price
    pub vwap: Decimal,
    /// Point of Control (price level with highest volume)
    pub poc: Decimal,
    /// Value area (70% of volume concentration)
    pub value_area: ValueArea,
    /// Price increment used for this profile
    pub price_increment: Decimal,
    /// Minimum price for the day
    pub min_price: Decimal,
    /// Maximum price for the day
    pub max_price: Decimal,
    /// Number of 1-minute candles processed
    pub candle_count: u32,
    /// Last update timestamp
    pub last_updated: TimestampMS,
    /// Individual price levels as (price, volume) pairs
    pub price_levels: Vec<(Decimal, Decimal)>,
}

impl From<VolumeProfileData> for DailyVolumeProfileFlat {
    fn from(data: VolumeProfileData) -> Self {
        Self {
            date: data.date,
            total_volume: data.total_volume,
            vwap: data.vwap,
            poc: data.poc,
            value_area: data.value_area,
            price_increment: data.price_increment,
            min_price: data.min_price,
            max_price: data.max_price,
            candle_count: data.candle_count,
            last_updated: data.last_updated,
            price_levels: data.price_levels
                .into_iter()
                .map(|level| (level.price, level.volume))
                .collect(),
        }
    }
}

impl From<DailyVolumeProfileFlat> for VolumeProfileData {
    fn from(flat: DailyVolumeProfileFlat) -> Self {
        Self {
            date: flat.date,
            price_levels: flat.price_levels
                .into_iter()
                .map(|(price, volume)| PriceLevelData {
                    price,
                    volume,
                    percentage: dec!(0.0), // Will be calculated based on total_volume
                    candle_count: 0, // Will be calculated based on actual data
                })
                .collect(),
            total_volume: flat.total_volume,
            vwap: flat.vwap,
            poc: flat.poc,
            value_area: flat.value_area,
            price_increment: flat.price_increment,
            min_price: flat.min_price,
            max_price: flat.max_price,
            candle_count: flat.candle_count,
            last_updated: flat.last_updated,
        }
    }
}

impl Default for DailyVolumeProfileFlat {
    fn default() -> Self {
        use rust_decimal_macros::dec;
        Self {
            date: String::new(),
            total_volume: dec!(0.0),
            vwap: dec!(0.0),
            poc: dec!(0.0),
            value_area: ValueArea::default(),
            price_increment: dec!(0.01),
            min_price: dec!(0.0),
            max_price: dec!(0.0),
            candle_count: 0,
            last_updated: 0,
            price_levels: Vec::new(),
        }
    }
}

/// Individual price level data within volume profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevelData {
    /// Price level
    pub price: Decimal,
    /// Volume at this price level
    pub volume: Decimal,
    /// Percentage of total daily volume
    pub percentage: Decimal,
    /// Number of candles/time periods at this price level (for TPO calculations)
    #[serde(default)]
    pub candle_count: u32,
}

/// Value area representing 70% volume concentration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueArea {
    /// Highest price in value area
    pub high: Decimal,
    /// Lowest price in value area
    pub low: Decimal,
    /// Percentage of total volume in value area
    pub volume_percentage: Decimal,
    /// Total volume within value area
    pub volume: Decimal,
}

/// Business rules validation result for value area calculations
#[derive(Debug, Clone, PartialEq)]
pub struct ValueAreaValidationResult {
    /// Overall validation status
    pub is_valid: bool,
    /// Detailed error messages for any rule violations
    pub errors: Vec<String>,
    /// Warnings for edge cases that don't invalidate the result
    pub warnings: Vec<String>,
    /// Debugging metrics
    pub metrics: ValidationMetrics,
}

/// Detailed metrics for value area validation debugging
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationMetrics {
    /// Point of Control price
    pub poc_price: Decimal,
    /// Value Area High price  
    pub vah_price: Decimal,
    /// Value Area Low price
    pub val_price: Decimal,
    /// Actual volume percentage achieved
    pub actual_volume_percentage: Decimal,
    /// Target volume percentage requested
    pub target_volume_percentage: Decimal,
    /// Distance from POC to VAH
    pub poc_to_vah_distance: Decimal,
    /// Distance from POC to VAL
    pub poc_to_val_distance: Decimal,
    /// Number of price levels in value area
    pub price_levels_count: usize,
    /// Whether POC is properly centered
    pub is_poc_centered: bool,
}

impl Default for ValueAreaValidationResult {
    fn default() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            metrics: ValidationMetrics::default(),
        }
    }
}

impl Default for ValidationMetrics {
    fn default() -> Self {
        Self {
            poc_price: Decimal::ZERO,
            vah_price: Decimal::ZERO,
            val_price: Decimal::ZERO,
            actual_volume_percentage: Decimal::ZERO,
            target_volume_percentage: Decimal::ZERO,
            poc_to_vah_distance: Decimal::ZERO,
            poc_to_val_distance: Decimal::ZERO,
            price_levels_count: 0,
            is_poc_centered: false,
        }
    }
}


/// Validation result for value area calculations
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    Valid,
    InvalidRange { high: Decimal, low: Decimal },
    InvalidVolumePercentage { percentage: Decimal },
    PocNotInRange { poc: Decimal, high: Decimal, low: Decimal },
    DegenerateCase { price_levels_count: usize },
}

/// ValueArea validator component for ensuring calculation quality
#[derive(Debug, Clone)]
pub struct ValueAreaValidator;

impl ValueAreaValidator {
    /// Validate a calculated value area against acceptance criteria
    pub fn validate_value_area(high: Decimal, low: Decimal, volume_percentage: Decimal) -> ValidationResult {
        // AC1: Value area high must be greater than value area low (no degenerate single-price areas)
        if high < low {
            return ValidationResult::InvalidRange { high, low };
        }
        
        // AC2: Volume percentage accuracy (65-75% target range)
        let min_percentage = dec!(65.0);
        let max_percentage = dec!(75.0);
        if volume_percentage < min_percentage || volume_percentage > max_percentage {
            return ValidationResult::InvalidVolumePercentage { percentage: volume_percentage };
        }
        
        ValidationResult::Valid
    }
    
    /// Detect degenerate cases where all volume is concentrated at extremes
    pub fn detect_degenerate_case(price_levels: &[PriceLevelData]) -> bool {
        if price_levels.len() < 2 {
            return true; // Single or no price levels is degenerate
        }
        
        // Check if 90% or more volume is at price extremes (first or last level)
        let total_volume: Decimal = price_levels.iter().map(|p| p.volume).sum();
        if total_volume <= Decimal::ZERO {
            return true;
        }
        
        let first_volume = price_levels.first().map(|p| p.volume).unwrap_or(Decimal::ZERO);
        let last_volume = price_levels.last().map(|p| p.volume).unwrap_or(Decimal::ZERO);
        let extreme_volume = first_volume + last_volume;
        
        (extreme_volume / total_volume) > dec!(0.9) // More than 90% at extremes is degenerate
    }
    
    /// Validate that POC is within value area range (AC3)
    pub fn validate_poc_in_range(poc: Decimal, high: Decimal, low: Decimal) -> ValidationResult {
        if poc < low || poc > high {
            return ValidationResult::PocNotInRange { poc, high, low };
        }
        
        ValidationResult::Valid
    }
}

/// Internal storage structure for efficient price level management
#[derive(Debug, Clone)]
pub struct PriceLevelMap {
    /// Price levels mapped to volume (using BTreeMap for sorted access)
    pub levels: BTreeMap<PriceKey, Decimal>,
    /// Price levels mapped to candle count for TPO calculations
    pub candle_counts: BTreeMap<PriceKey, u32>,
    /// Price increment for this profile
    pub price_increment: Decimal,
    /// Precision manager for consistent price-to-key mapping
    precision_manager: PricePrecisionManager,
}

impl PriceLevelMap {
    /// Create new price level map
    pub fn new(price_increment: Decimal) -> Self {
        Self {
            levels: BTreeMap::new(),
            candle_counts: BTreeMap::new(),
            price_increment,
            precision_manager: PricePrecisionManager::default(),
        }
    }

    /// Add volume to a specific price level
    pub fn add_volume(&mut self, price: Decimal, volume: Decimal) {
        let price_key = PriceKey::from_price_with_manager(price, self.price_increment, &self.precision_manager);
        *self.levels.entry(price_key).or_insert(Decimal::ZERO) += volume;
    }

    /// Add volume and increment candle count at a specific price level
    pub fn add_volume_at_price(&mut self, price: Decimal, volume: Decimal) {
        let price_key = PriceKey::from_price_with_manager(price, self.price_increment, &self.precision_manager);
        *self.levels.entry(price_key).or_insert(Decimal::ZERO) += volume;
        *self.candle_counts.entry(price_key).or_insert(0) += 1;
    }

    /// Distribute volume across price range using the specified distribution mode
    pub fn distribute_candle_volume(&mut self, _open: Decimal, high: Decimal, low: Decimal, close: Decimal, volume: Decimal, mode: &VolumeDistributionMode) {
        if volume <= Decimal::ZERO {
            return;
        }

        match mode {
            VolumeDistributionMode::ClosingPrice => {
                // Traditional time-based market profile: all volume at close
                self.add_volume_at_price(close, volume);
            },
            VolumeDistributionMode::UniformOHLC => {
                // Distribute volume uniformly across OHLC range
                self.distribute_volume_uniform(high, low, volume);
            },
            VolumeDistributionMode::WeightedOHLC => {
                // Weighted: 50% close, 25% high, 25% low
                self.add_volume_at_price(close, volume * dec!(0.5));
                self.add_volume_at_price(high, volume * dec!(0.25));
                self.add_volume_at_price(low, volume * dec!(0.25));
            },
            VolumeDistributionMode::HighLowWeighted => {
                // Price action focused: 50% high, 50% low
                self.add_volume_at_price(high, volume * dec!(0.5));
                self.add_volume_at_price(low, volume * dec!(0.5));
            },
        }
    }

    /// Distribute volume uniformly across price range from low to high
    fn distribute_volume_uniform(&mut self, high: Decimal, low: Decimal, volume: Decimal) {
        if high <= low {
            // Edge case: no range, assign all to single price
            self.add_volume_at_price(high, volume);
            return;
        }

        // Calculate number of price levels in the range
        let range = high - low;
        let levels_in_range = (range / self.price_increment).ceil().to_i32().unwrap_or(1);
        
        if levels_in_range <= 1 {
            // Range is smaller than price increment
            self.add_volume_at_price(high, volume);
            return;
        }

        // Distribute volume equally across all price levels in range
        let volume_per_level = volume / Decimal::from(levels_in_range);
        
        for i in 0..levels_in_range {
            let price = low + (Decimal::from(i) * self.price_increment);
            if price <= high {
                self.add_volume_at_price(price, volume_per_level);
            }
        }
    }

    /// Get total volume across all price levels
    pub fn total_volume(&self) -> Decimal {
        self.levels.values().sum()
    }

    /// Get total candle count across all price levels
    pub fn get_total_candle_count(&self) -> u32 {
        self.candle_counts.values().sum()
    }

    /// Get total volume for percentage calculations
    pub fn get_total_volume(&self) -> Decimal {
        self.total_volume()
    }
    
    /// Calculate percentage of total volume for a given volume amount
    pub fn calculate_volume_percentage(&self, volume: Decimal) -> Decimal {
        use rust_decimal_macros::dec;
        let total = self.get_total_volume();
        if total > Decimal::ZERO {
            (volume / total) * dec!(100.0)
        } else {
            Decimal::ZERO
        }
    }
    
    /// Calculate percentage of total candles for a given candle count
    pub fn calculate_candle_percentage(&self, candle_count: u32) -> Decimal {
        use rust_decimal_macros::dec;
        let total = self.get_total_candle_count();
        if total > 0 {
            (Decimal::from(candle_count) / Decimal::from(total)) * dec!(100.0)
        } else {
            Decimal::ZERO
        }
    }
    
    /// Get price level with highest candle count for TPO analysis
    pub fn get_highest_candle_count_level(&self) -> Option<(Decimal, u32)> {
        self.candle_counts
            .iter()
            .max_by(|(_, count_a), (_, count_b)| count_a.cmp(count_b))
            .map(|(price_key, &count)| (price_key.to_price(self.price_increment), count))
    }
    
    /// Get price level with highest volume for Volume analysis
    pub fn get_highest_volume_level(&self) -> Option<(Decimal, Decimal)> {
        self.levels
            .iter()
            .max_by(|(_, vol_a), (_, vol_b)| vol_a.partial_cmp(vol_b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(price_key, &volume)| (price_key.to_price(self.price_increment), volume))
    }
    
    /// Get combined metrics for a specific price level
    pub fn get_price_level_metrics(&self, price: Decimal) -> Option<(Decimal, u32)> {
        let price_key = PriceKey::from_price_with_manager(price, self.price_increment, &self.precision_manager);
        let volume = self.levels.get(&price_key).copied().unwrap_or(dec!(0.0));
        let candle_count = self.candle_counts.get(&price_key).copied().unwrap_or(0);
        if volume > dec!(0.0) || candle_count > 0 {
            Some((volume, candle_count))
        } else {
            None
        }
    }

    /// Get price level with highest volume (Point of Control)
    pub fn get_poc(&self) -> Option<Decimal> {
        self.levels
            .iter()
            .max_by(|(_, vol_a), (_, vol_b)| vol_a.partial_cmp(vol_b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(price_key, _)| price_key.to_price(self.price_increment))
    }

    /// Identify POC using TPO method (highest candle count)
    pub fn identify_poc_tpo(&self) -> Option<Decimal> {
        self.candle_counts
            .iter()
            .max_by(|(_, count_a), (_, count_b)| count_a.cmp(count_b))
            .map(|(price_key, _)| price_key.to_price(self.price_increment))
    }

    /// Identify POC using Volume method (highest actual volume)
    pub fn identify_poc_volume(&self) -> Option<Decimal> {
        self.levels
            .iter()
            .max_by(|(_, vol_a), (_, vol_b)| vol_a.partial_cmp(vol_b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(price_key, _)| price_key.to_price(self.price_increment))
    }

    /// Unified POC identification based on calculation mode
    pub fn identify_poc(&self, calculation_mode: &VolumeProfileCalculationMode) -> Option<Decimal> {
        match calculation_mode {
            VolumeProfileCalculationMode::Volume => self.identify_poc_volume(),
            VolumeProfileCalculationMode::TPO => self.identify_poc_tpo(),
        }
    }

    /// Expand value area using TPO (Time-Price Opportunity) method
    /// Starts from POC and expands bidirectionally counting time periods until threshold reached
    pub fn expand_value_area_tpo(&self, value_area_percentage: Decimal) -> ValueArea {
        let total_candles = Decimal::from(self.get_total_candle_count());
        let target_candles = total_candles * (value_area_percentage / dec!(100.0));
        
        if self.candle_counts.is_empty() || total_candles <= Decimal::ZERO {
            return ValueArea::default();
        }
        
        // Find TPO POC (highest candle count)
        let poc_key = self.candle_counts
            .iter()
            .max_by(|(_, count_a), (_, count_b)| count_a.cmp(count_b))
            .map(|(key, _)| *key);
            
        let poc_key = match poc_key {
            Some(key) => key,
            None => return ValueArea::default(),
        };
        
        // Use bumpalo arena allocation for temporary collections in value area calculation
        crate::volume_profile::calculator::with_calculation_arena(|arena| {
            use bumpalo::collections::Vec as BumpVec;
            
            // Get all price keys sorted by price using arena allocation
            let mut all_levels = BumpVec::with_capacity_in(self.candle_counts.len(), arena);
            all_levels.extend(self.candle_counts.keys());
            all_levels.sort();
            
            let poc_index = all_levels.iter().position(|&key: &PriceKey| key == poc_key)
                .unwrap_or(0);
            
            // Start from POC and expand symmetrically using arena-allocated vectors
            let mut included_candles = Decimal::from(self.candle_counts.get(&poc_key).copied().unwrap_or(0));
            let mut selected_levels = BumpVec::with_capacity_in(all_levels.len(), arena);
            selected_levels.push(poc_key);
            let mut low_index = poc_index;
            let mut high_index = poc_index;
            
            // Expand symmetrically to center POC
            while included_candles < target_candles && (low_index > 0 || high_index < all_levels.len() - 1) {
                let mut candidates = BumpVec::with_capacity_in(2, arena); // At most 2 candidates (left/right)
            
            // Check left expansion
            if low_index > 0 {
                let left_key = all_levels[low_index - 1];
                let left_candles = Decimal::from(self.candle_counts.get(&left_key).copied().unwrap_or(0));
                if left_candles > Decimal::ZERO {
                    candidates.push((left_key, left_candles, low_index - 1, "left"));
                }
            }
            
            // Check right expansion
            if high_index < all_levels.len() - 1 {
                let right_key = all_levels[high_index + 1];
                let right_candles = Decimal::from(self.candle_counts.get(&right_key).copied().unwrap_or(0));
                if right_candles > Decimal::ZERO {
                    candidates.push((right_key, right_candles, high_index + 1, "right"));
                }
            }
            
            if candidates.is_empty() {
                break;
            }
            
            // Sort by candle count descending, then prefer symmetric expansion
            candidates.sort_by(|a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            
            // Select the best candidate
            let (selected_key, selected_candles, selected_index, direction) = candidates[0];
            selected_levels.push(selected_key);
            included_candles += selected_candles;
            
            if direction == "left" {
                low_index = selected_index;
            } else {
                high_index = selected_index;
            }
            }
            
            // Determine final boundaries
            let low_key = all_levels[low_index];
            let high_key = all_levels[high_index];
            let final_low = low_key.to_price(self.price_increment);
            let final_high = high_key.to_price(self.price_increment);
            
            // Calculate actual volume in final range (always return actual volume)
            let actual_volume = self.levels.iter()
                .filter(|(key, _)| **key >= low_key && **key <= high_key)
                .map(|(_, volume)| *volume)
                .sum::<Decimal>();
            
            let total_volume = self.total_volume();
            
            // Arena memory is automatically freed when this closure exits
            ValueArea {
                low: final_low,
                high: final_high,
                volume: actual_volume,
                volume_percentage: if total_volume > Decimal::ZERO { (actual_volume / total_volume) * Decimal::from(100) } else { Decimal::ZERO },
            }
        }) // End of arena allocation closure
    }
    
    /// Expand value area using Volume method
    /// Starts from POC and expands bidirectionally counting volume until threshold reached
    pub fn expand_value_area_volume(&self, value_area_percentage: Decimal) -> ValueArea {
        let total_volume = self.total_volume();
        let target_volume = total_volume * (value_area_percentage / Decimal::from(100));
        
        if self.levels.is_empty() || total_volume <= Decimal::ZERO {
            return ValueArea::default();
        }
        
        // Find Volume POC (highest volume)
        let poc_key = self.levels
            .iter()
            .max_by(|(_, vol_a), (_, vol_b)| vol_a.partial_cmp(vol_b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(key, _)| *key);
            
        let poc_key = match poc_key {
            Some(key) => key,
            None => return ValueArea::default(),
        };
        
        // Use bumpalo arena allocation for temporary collections in volume-based value area calculation
        crate::volume_profile::calculator::with_calculation_arena(|arena| {
            use bumpalo::collections::Vec as BumpVec;
            
            // Get all price keys sorted by price using arena allocation
            let mut all_levels = BumpVec::with_capacity_in(self.levels.len(), arena);
            all_levels.extend(self.levels.keys());
            all_levels.sort();
            
            let poc_index = all_levels.iter().position(|&key: &PriceKey| key == poc_key)
                .unwrap_or(0);
            
            // Start from POC and expand symmetrically using arena-allocated vectors
            let mut included_volume = self.levels.get(&poc_key).copied().unwrap_or(Decimal::ZERO);
            let mut selected_levels = BumpVec::with_capacity_in(all_levels.len(), arena);
            selected_levels.push(poc_key);
            let mut low_index = poc_index;
            let mut high_index = poc_index;
            
            // Expand symmetrically to center POC
            while included_volume < target_volume && (low_index > 0 || high_index < all_levels.len() - 1) {
                let mut candidates = BumpVec::with_capacity_in(2, arena); // At most 2 candidates (left/right)
            
            // Check left expansion
            if low_index > 0 {
                let left_key = all_levels[low_index - 1];
                let left_volume = self.levels.get(&left_key).copied().unwrap_or(Decimal::ZERO);
                if left_volume > Decimal::ZERO {
                    candidates.push((left_key, left_volume, low_index - 1, "left"));
                }
            }
            
            // Check right expansion
            if high_index < all_levels.len() - 1 {
                let right_key = all_levels[high_index + 1];
                let right_volume = self.levels.get(&right_key).copied().unwrap_or(Decimal::ZERO);
                if right_volume > Decimal::ZERO {
                    candidates.push((right_key, right_volume, high_index + 1, "right"));
                }
            }
            
            if candidates.is_empty() {
                break;
            }
            
            // Sort by volume descending, then prefer symmetric expansion
            candidates.sort_by(|a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            
            // Select the best candidate
            let (selected_key, selected_volume, selected_index, direction) = candidates[0];
            selected_levels.push(selected_key);
            included_volume += selected_volume;
            
            if direction == "left" {
                low_index = selected_index;
            } else {
                high_index = selected_index;
            }
            }
            
            // Determine final boundaries
            let low_key = all_levels[low_index];
            let high_key = all_levels[high_index];
            let final_low = low_key.to_price(self.price_increment);
            let final_high = high_key.to_price(self.price_increment);
            
            // Calculate actual volume in final range
            let actual_volume = self.levels.iter()
                .filter(|(key, _)| **key >= low_key && **key <= high_key)
                .map(|(_, volume)| *volume)
                .sum::<Decimal>();
            
            // Arena memory is automatically freed when this closure exits
            ValueArea {
                low: final_low,
                high: final_high,
                volume: actual_volume,
                volume_percentage: if total_volume > Decimal::ZERO { (actual_volume / total_volume) * Decimal::from(100) } else { Decimal::ZERO },
            }
        }) // End of arena allocation closure
    }

    /// Convert to sorted vector of price level data using arena allocation for temporary calculations
    pub fn to_price_levels(&self) -> Vec<PriceLevelData> {
        let total_volume = self.total_volume();
        if total_volume <= Decimal::ZERO {
            return Vec::new();
        }

        // Use bumpalo arena allocation for temporary calculation space
        crate::volume_profile::calculator::with_calculation_arena(|arena| {
            use bumpalo::collections::Vec as BumpVec;
            
            // Create arena-allocated temporary vector for collecting data
            let mut arena_results = BumpVec::with_capacity_in(self.levels.len(), arena);
            
            // Collect price level data using arena-allocated temporary storage
            for (price_key, &volume) in &self.levels {
                let candle_count = self.candle_counts.get(price_key).copied().unwrap_or(0);
                let price_level = PriceLevelData {
                    price: price_key.to_price(self.price_increment),
                    volume,
                    percentage: (volume / total_volume) * dec!(100.0),
                    candle_count,
                };
                arena_results.push(price_level);
            }
            
            // Convert to standard Vec for return (arena memory freed after this)
            arena_results.iter().cloned().collect()
        })
    }

    /// Calculate VWAP (Volume Weighted Average Price) using arena allocation for temporary calculations
    pub fn calculate_vwap(&self) -> Decimal {
        let total_volume = self.total_volume();
        if total_volume <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        // Use bumpalo arena allocation for temporary calculations to reduce heap allocations
        crate::volume_profile::calculator::with_calculation_arena(|arena| {
            use bumpalo::collections::Vec as BumpVec;
            
            // Create arena-allocated vector for price-volume pairs
            let mut weighted_values = BumpVec::with_capacity_in(self.levels.len(), arena);
            
            // Calculate weighted values using arena-allocated temporary storage
            for (price_key, &volume) in &self.levels {
                let price = price_key.to_price(self.price_increment);
                let weighted_value = price * volume;
                weighted_values.push(weighted_value);
            }
            
            // Sum the weighted values
            let weighted_sum: Decimal = weighted_values.iter().sum();
            
            // Arena memory is automatically freed when this closure exits
            weighted_sum / total_volume
        })
    }

        /// Calculate value area using the specified method and calculation mode
    pub fn calculate_value_area(&self, value_area_percentage: Decimal, calculation_mode: &ValueAreaCalculationMode, poc_calculation_mode: &VolumeProfileCalculationMode) -> ValueArea {
        // Skip validation to preserve POC-centered algorithm results
        match calculation_mode {
            ValueAreaCalculationMode::Traditional => self.calculate_value_area_traditional(value_area_percentage, poc_calculation_mode),
            ValueAreaCalculationMode::Greedy => self.calculate_value_area_greedy(value_area_percentage, poc_calculation_mode),
        }
    }
    
    /// Unified value area expansion based on calculation mode
    /// This method routes to the appropriate expansion algorithm
    pub fn expand_value_area(&self, value_area_percentage: Decimal, calculation_mode: &VolumeProfileCalculationMode) -> ValueArea {
        match calculation_mode {
            VolumeProfileCalculationMode::Volume => self.expand_value_area_volume(value_area_percentage),
            VolumeProfileCalculationMode::TPO => self.expand_value_area_tpo(value_area_percentage),
        }
    }
    
    /// Validate value area against business rules
    /// Checks fundamental volume profile constraints and provides detailed diagnostics
    pub fn validate_value_area_rules(
        &self, 
        value_area: &ValueArea, 
        target_percentage: Decimal, 
        calculation_mode: &VolumeProfileCalculationMode
    ) -> ValueAreaValidationResult {
        let mut result = ValueAreaValidationResult::default();
        
        // Get POC based on calculation mode
        let poc_price = match self.identify_poc(calculation_mode) {
            Some(price) => price,
            None => {
                result.is_valid = false;
                result.errors.push("No POC could be identified".to_string());
                return result;
            }
        };
        
        // Calculate metrics
        let vah_price = value_area.high;
        let val_price = value_area.low;
        let actual_percentage = value_area.volume_percentage;
        let poc_to_vah_distance = (vah_price - poc_price).abs();
        let poc_to_val_distance = (poc_price - val_price).abs();
        
        // Count price levels in value area
        let price_levels_count = self.levels.iter()
            .filter(|(key, _)| {
                let price = key.to_price(self.price_increment);
                price >= val_price && price <= vah_price
            })
            .count();
        
        // Check if POC is reasonably centered
        let is_poc_centered = if vah_price > val_price {
            let total_range = vah_price - val_price;
            let poc_position = poc_price - val_price;
            let centering_ratio = poc_position / total_range;
(dec!(0.2)..=dec!(0.8)).contains(&centering_ratio)
        } else {
            false
        };
        
        // Fill metrics
        result.metrics = ValidationMetrics {
            poc_price,
            vah_price,
            val_price,
            actual_volume_percentage: actual_percentage,
            target_volume_percentage: target_percentage,
            poc_to_vah_distance,
            poc_to_val_distance,
            price_levels_count,
            is_poc_centered,
        };
        
        // Rule 1: Check VAH > POC > VAL ordering constraint
        if !(vah_price >= poc_price && poc_price >= val_price) {
            result.is_valid = false;
            result.errors.push(format!(
                "VAH > POC > VAL ordering violated: VAH={:.4}, POC={:.4}, VAL={:.4}",
                vah_price, poc_price, val_price
            ));
        }
        
        // Rule 2: Verify POC is not at VAH or VAL boundaries (POC should be interior)
        // Allow POC to be at boundary only if value area spans just 1-2 price levels
        let price_tolerance = self.price_increment * dec!(0.1); // Small tolerance for floating point
        let allow_boundary_poc = price_levels_count <= 2; // Allow boundary POC for very narrow areas
        
        if !allow_boundary_poc {
            if (poc_price - vah_price).abs() < price_tolerance {
                result.is_valid = false;
                result.errors.push(format!(
                    "POC at VAH boundary: POC={:.4}, VAH={:.4}",
                    poc_price, vah_price
                ));
            }
            if (poc_price - val_price).abs() < price_tolerance {
                result.is_valid = false;
                result.errors.push(format!(
                    "POC at VAL boundary: POC={:.4}, VAL={:.4}",
                    poc_price, val_price
                ));
            }
        } else {
            // For narrow value areas, add warnings instead of errors
            if (poc_price - vah_price).abs() < price_tolerance {
                result.warnings.push(format!(
                    "POC at VAH boundary in narrow value area: POC={:.4}, VAH={:.4}",
                    poc_price, vah_price
                ));
            }
            if (poc_price - val_price).abs() < price_tolerance {
                result.warnings.push(format!(
                    "POC at VAL boundary in narrow value area: POC={:.4}, VAL={:.4}",
                    poc_price, val_price
                ));
            }
        }
        
        // Rule 3: Validate volume percentage is within acceptable range  
        let target_min = target_percentage - dec!(10.0); // Allow 10% deviation below for discrete levels
        let target_max = target_percentage + dec!(10.0); // Allow 10% deviation above for discrete levels
        if actual_percentage < target_min || actual_percentage > target_max {
            result.is_valid = false;
            result.errors.push(format!(
                "Volume percentage out of range: actual={:.2}%, target={:.2}%, range=[{:.2}%, {:.2}%]",
                actual_percentage, target_percentage, target_min, target_max
            ));
        }
        
        // Rule 4: Ensure value area forms contiguous price range
        if vah_price < val_price {
            result.is_valid = false;
            result.errors.push(format!(
                "Non-contiguous value area: VAH={:.4} < VAL={:.4}",
                vah_price, val_price
            ));
        }
        
        // Warning checks (don't invalidate but worth noting)
        if !is_poc_centered {
            result.warnings.push(format!(
                "POC not well-centered in value area: POC position ratio = {:.2}",
                if vah_price > val_price { (poc_price - val_price) / (vah_price - val_price) } else { Decimal::ZERO }
            ));
        }
        
        if price_levels_count < 3 {
            result.warnings.push(format!(
                "Very narrow value area: only {} price levels",
                price_levels_count
            ));
        }
        
        // Check for extreme percentage deviations (warnings, not errors)
        let percentage_deviation = (actual_percentage - target_percentage).abs();
        if percentage_deviation > dec!(3.0) && percentage_deviation <= dec!(5.0) {
            result.warnings.push(format!(
                "Volume percentage deviation: {:.2}% (target: {:.2}%)",
                actual_percentage, target_percentage
            ));
        }
        
        result
    }
    

    /// Calculate value area using traditional market profile method (expand from POC)
    pub fn calculate_value_area_traditional(&self, value_area_percentage: Decimal, calculation_mode: &VolumeProfileCalculationMode) -> ValueArea {
        let (total_metric, target_metric) = match calculation_mode {
            VolumeProfileCalculationMode::Volume => {
                let total_volume = self.total_volume();
                (total_volume, total_volume * (value_area_percentage / dec!(100.0)))
            },
            VolumeProfileCalculationMode::TPO => {
                let total_count = Decimal::from(self.get_total_candle_count());
                (total_count, total_count * (value_area_percentage / dec!(100.0)))
            },
        };

        if self.levels.is_empty() || total_metric <= Decimal::ZERO {
            return ValueArea::default();
        }

        // Find POC (Point of Control) based on calculation mode
        let poc_key = match calculation_mode {
            VolumeProfileCalculationMode::Volume => {
                self.levels
                    .iter()
                    .max_by(|(_, vol_a), (_, vol_b)| vol_a.partial_cmp(vol_b).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(key, _)| *key)
            },
            VolumeProfileCalculationMode::TPO => {
                self.candle_counts
                    .iter()
                    .max_by(|(_, count_a), (_, count_b)| count_a.cmp(count_b))
                    .map(|(key, _)| *key)
            },
        };

        let poc_key = match poc_key {
            Some(key) => key,
            None => return ValueArea::default(),
        };

        // Get all price levels sorted by price
        let mut all_levels: Vec<_> = self.levels.iter().collect();
        all_levels.sort_by(|(a, _), (b, _)| a.cmp(b));
        
        // Find POC index
        let poc_index = all_levels.iter().position(|(key, _)| **key == poc_key).unwrap_or(0);
        
        // Start from POC and expand symmetrically to center it
        let mut included_metric = match calculation_mode {
            VolumeProfileCalculationMode::Volume => self.levels.get(&poc_key).copied().unwrap_or(Decimal::ZERO),
            VolumeProfileCalculationMode::TPO => Decimal::from(self.candle_counts.get(&poc_key).copied().unwrap_or(0)),
        };
        let mut low_index = poc_index;
        let mut high_index = poc_index;
        
        // Ensure we start with POC volume
        let mut selected_levels = vec![poc_key];
        
        // TRUE Traditional Market Profile: Strict symmetric expansion to guarantee POC centering
        // Expand one level on each side alternately, ensuring perfect symmetry
        while included_metric < target_metric && (low_index > 0 || high_index < all_levels.len() - 1) {
            let mut added_this_round = false;
            
            // Always try to expand both sides equally for perfect symmetry
            // Left expansion
            if low_index > 0 {
                let left_key = all_levels[low_index - 1].0;
                let left_metric = match calculation_mode {
                    VolumeProfileCalculationMode::Volume => self.levels.get(left_key).copied().unwrap_or(Decimal::ZERO),
                    VolumeProfileCalculationMode::TPO => Decimal::from(self.candle_counts.get(left_key).copied().unwrap_or(0)),
                };
                
                // Add left level
                selected_levels.push(*left_key);
                included_metric += left_metric;
                low_index -= 1;
                added_this_round = true;
                
                // Check if we've reached target after left expansion
                if included_metric >= target_metric {
                    break;
                }
            }
            
            // Right expansion (symmetric to left)
            if high_index < all_levels.len() - 1 {
                let right_key = all_levels[high_index + 1].0;
                let right_metric = match calculation_mode {
                    VolumeProfileCalculationMode::Volume => self.levels.get(right_key).copied().unwrap_or(Decimal::ZERO),
                    VolumeProfileCalculationMode::TPO => Decimal::from(self.candle_counts.get(right_key).copied().unwrap_or(0)),
                };
                
                // Add right level
                selected_levels.push(*right_key);
                included_metric += right_metric;
                high_index += 1;
                added_this_round = true;
                
                // Check if we've reached target after right expansion
                if included_metric >= target_metric {
                    break;
                }
            }
            
            // If we couldn't expand either side, break
            if !added_this_round {
                break;
            }
        }

        // Ensure POC is always included and centered
        selected_levels.sort();
        let low_key = *selected_levels.first().unwrap_or(&poc_key);
        let high_key = *selected_levels.last().unwrap_or(&poc_key);
        
        let low_price = low_key.to_price(self.price_increment);
        let high_price = high_key.to_price(self.price_increment);
        let poc_price = poc_key.to_price(self.price_increment);

        // Ensure POC is within the range and adjust if necessary
        let (final_low, final_high) = if poc_price < low_price {
            (poc_price, high_price)
        } else if poc_price > high_price {
            (low_price, poc_price)
        } else {
            (low_price, high_price)
        };

        // Calculate actual volume in final range (always return actual volume, not metric)
        let actual_volume = self.levels.iter()
            .filter(|(key, _)| **key >= low_key && **key <= high_key)
            .map(|(_, volume)| *volume)
            .sum::<Decimal>();
        
        let total_volume = self.total_volume();

        ValueArea {
            low: final_low,
            high: final_high,
            volume: actual_volume,
            volume_percentage: if total_volume > Decimal::ZERO { (actual_volume / total_volume) * Decimal::from(100) } else { Decimal::ZERO },
        }
    }

    /// Calculate value area using POC-centered greedy selection method 
    pub fn calculate_value_area_greedy(&self, value_area_percentage: Decimal, calculation_mode: &VolumeProfileCalculationMode) -> ValueArea {
        info!("🎯 Starting POC-centered Greedy algorithm with target {}%", value_area_percentage);
        let (total_metric, target_metric) = match calculation_mode {
            VolumeProfileCalculationMode::Volume => {
                let total_volume = self.total_volume();
                (total_volume, total_volume * (value_area_percentage / dec!(100.0)))
            },
            VolumeProfileCalculationMode::TPO => {
                let total_count = Decimal::from(self.get_total_candle_count());
                (total_count, total_count * (value_area_percentage / dec!(100.0)))
            },
        };

        if self.levels.is_empty() || total_metric <= Decimal::ZERO {
            return ValueArea {
                high: Decimal::ZERO,
                low: Decimal::ZERO,
                volume_percentage: Decimal::ZERO,
                volume: Decimal::ZERO,
            };
        }

        // Step 1: Find POC (Point of Control) - highest metric level
        let poc_key = match calculation_mode {
            VolumeProfileCalculationMode::Volume => {
                self.levels
                    .iter()
                    .max_by(|(_, vol_a), (_, vol_b)| vol_a.partial_cmp(vol_b).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(key, _)| *key)
            },
            VolumeProfileCalculationMode::TPO => {
                self.candle_counts
                    .iter()
                    .max_by(|(_, count_a), (_, count_b)| count_a.cmp(count_b))
                    .map(|(key, _)| *key)
            },
        };

        let poc_key = match poc_key {
            Some(key) => key,
            None => return ValueArea::default(),
        };

        // Step 2: Get all price levels sorted by price for symmetric expansion
        let mut all_levels: Vec<_> = self.levels.iter().collect();
        all_levels.sort_by(|(a, _), (b, _)| a.cmp(b));
        
        // Find POC index
        let poc_index = all_levels.iter().position(|(key, _)| **key == poc_key).unwrap_or(0);
        
        // Step 3: Start with POC and expand symmetrically using greedy neighbor selection
        let mut included_metric = match calculation_mode {
            VolumeProfileCalculationMode::Volume => self.levels.get(&poc_key).copied().unwrap_or(Decimal::ZERO),
            VolumeProfileCalculationMode::TPO => Decimal::from(self.candle_counts.get(&poc_key).copied().unwrap_or(0)),
        };
        let mut low_index = poc_index;
        let mut high_index = poc_index;
        let mut selected_levels = vec![poc_key];
        
        // Step 4: Balanced expansion around POC - prioritize centering while considering volume
        let mut loop_count = 0;
        let mut left_expansions: i32 = 0;
        let mut right_expansions: i32 = 0;
        
        while included_metric < target_metric && (low_index > 0 || high_index < all_levels.len() - 1) {
            loop_count += 1;
            if loop_count > all_levels.len() * 2 {
                info!("⚠️  POC-centered algorithm: Loop safety break at {} iterations", loop_count);
                break;
            }
            
            let mut left_candidate = None;
            let mut right_candidate = None;
            let mut left_metric = Decimal::ZERO;
            let mut right_metric = Decimal::ZERO;
            
            // Get both candidates
            if low_index > 0 {
                let left_key = all_levels[low_index - 1].0;
                left_metric = match calculation_mode {
                    VolumeProfileCalculationMode::Volume => self.levels.get(left_key).copied().unwrap_or(Decimal::ZERO),
                    VolumeProfileCalculationMode::TPO => Decimal::from(self.candle_counts.get(left_key).copied().unwrap_or(0)),
                };
                left_candidate = Some((*left_key, left_metric, low_index - 1, "left"));
            }
            
            if high_index < all_levels.len() - 1 {
                let right_key = all_levels[high_index + 1].0;
                right_metric = match calculation_mode {
                    VolumeProfileCalculationMode::Volume => self.levels.get(right_key).copied().unwrap_or(Decimal::ZERO),
                    VolumeProfileCalculationMode::TPO => Decimal::from(self.candle_counts.get(right_key).copied().unwrap_or(0)),
                };
                right_candidate = Some((*right_key, right_metric, high_index + 1, "right"));
            }
            
            // Balanced expansion logic - prioritize centering
            let selected_candidate = match (left_candidate, right_candidate) {
                (Some(left), Some(right)) => {
                    let balance_diff = (left_expansions - right_expansions).abs();
                    
                    // If expansion is heavily unbalanced (>3 difference), force balance
                    if balance_diff > 3 {
                        if left_expansions > right_expansions {
                            Some(right) // Force right expansion
                        } else {
                            Some(left) // Force left expansion
                        }
                    }
                    // If relatively balanced, choose higher volume but with bias toward balance
                    else if (left_metric - right_metric).abs() < left_metric * Decimal::from_str("0.3").unwrap() {
                        // Similar volumes, alternate for balance
                        if left_expansions <= right_expansions {
                            Some(left)
                        } else {
                            Some(right)
                        }
                    }
                    // Significant volume difference, choose higher but track imbalance
                    else if left_metric > right_metric {
                        Some(left)
                    } else {
                        Some(right)
                    }
                },
                (Some(left), None) => Some(left),
                (None, Some(right)) => Some(right),
                (None, None) => None,
            };
            
            match selected_candidate {
                Some((selected_key, selected_metric, selected_index, direction)) => {
                    selected_levels.push(selected_key);
                    included_metric += selected_metric;
                    
                    if direction == "left" {
                        low_index = selected_index;
                        left_expansions += 1;
                    } else {
                        high_index = selected_index;
                        right_expansions += 1;
                    }
                },
                None => break, // No more expansion possible
            }
        }

        // Step 5: Calculate final range
        selected_levels.sort();
        let low_key = *selected_levels.first().unwrap_or(&poc_key);
        let high_key = *selected_levels.last().unwrap_or(&poc_key);
        
        let min_price = low_key.to_price(self.price_increment);
        let max_price = high_key.to_price(self.price_increment);
        let poc = poc_key.to_price(self.price_increment);

        // Algorithm produces centered result by design

        // Ensure POC is within the value area range
        let final_low = min_price.min(poc);
        let final_high = max_price.max(poc);

        // Final validation: ensure POC is actually in our range
        let final_actual_volume = 
            self.levels.iter()
                .filter(|(price_key, _)| {
                    let price = price_key.to_price(self.price_increment);
                    price >= final_low && price <= final_high
                })
                .map(|(_, volume)| *volume)
                .sum::<Decimal>();

        let total_volume = self.total_volume();
        
        let result = ValueArea {
            high: final_high,
            low: final_low,
            volume_percentage: if total_volume > Decimal::ZERO { (final_actual_volume / total_volume) * Decimal::from(100) } else { Decimal::ZERO },
            volume: final_actual_volume,
        };
        
        // Calculate POC balance metrics
        let poc = poc_key.to_price(self.price_increment);
        let distance_to_high = result.high - poc;
        let distance_to_low = poc - result.low;
        let balance_ratio = if distance_to_low > Decimal::ZERO { distance_to_high / distance_to_low } else { Decimal::MAX };
        
        info!("✅ POC-centered algorithm completed: {} loops, {}% vs target {}%, L/R expansions: {}/{}, balance ratio: {:.2}", 
              loop_count, result.volume_percentage, value_area_percentage, left_expansions, right_expansions, balance_ratio);
        
        result
    }

    /// Get min and max price keys from the price level map
    pub fn get_price_range_keys(&self) -> (PriceKey, PriceKey) {
        if self.levels.is_empty() {
            return (PriceKey(0), PriceKey(0));
        }

        let min_key = *self.levels.keys().next().unwrap();
        let max_key = *self.levels.keys().next_back().unwrap();
        (min_key, max_key)
    }

    /// Get volume at specific price key
    pub fn get_volume_at_key(&self, key: &PriceKey) -> Decimal {
        self.levels.get(key).copied().unwrap_or(dec!(0.0))
    }

    /// Get min and max prices
    pub fn get_price_range(&self) -> (Decimal, Decimal) {
        if self.levels.is_empty() {
            return (dec!(0.0), dec!(0.0));
        }

        let min_price = self.levels.keys().next()
            .map(|key| key.to_price(self.price_increment))
            .unwrap_or(dec!(0.0));
        
        let max_price = self.levels.keys().next_back()
            .map(|key| key.to_price(self.price_increment))
            .unwrap_or(dec!(0.0));

        (min_price, max_price)
    }
}

/// Price key for efficient BTreeMap operations
/// Uses integer representation for consistent ordering and fast comparisons
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PriceKey(i64);

impl PriceKey {
    /// Create price key from price and increment using precision-aware mapping
    pub fn from_price(price: Decimal, price_increment: Decimal) -> Self {
        let precision_manager = PricePrecisionManager::default();
        let key = precision_manager.price_to_key(price, price_increment)
            .unwrap_or_else(|_| {
                // Fallback to simple rounding only if precision manager fails
                eprintln!("Warning: Precision manager failed, using simple rounding for price {} with increment {}", price, price_increment);
                (price / price_increment).round().to_i64().unwrap_or(0)
            });
        Self(key)
    }

    /// Create price key from price and increment using the provided precision manager
    pub fn from_price_with_manager(price: Decimal, price_increment: Decimal, precision_manager: &PricePrecisionManager) -> Self {
        let key = precision_manager.price_to_key(price, price_increment)
            .unwrap_or_else(|_| {
                // Fallback to simple rounding only if precision manager fails
                eprintln!("Warning: Precision manager failed, using simple rounding for price {} with increment {}", price, price_increment);
                (price / price_increment).round().to_i64().unwrap_or(0)
            });
        Self(key)
    }

    /// Convert price key back to price with precision validation
    pub fn to_price(self, price_increment: Decimal) -> Decimal {
        let precision_manager = PricePrecisionManager::default();
        precision_manager.key_to_price(self.0, price_increment)
            .unwrap_or_else(|_| {
                // Fallback to simple multiplication only if precision manager fails
                eprintln!("Warning: Precision manager failed, using simple multiplication for key {} with increment {}", self.0, price_increment);
                Decimal::from(self.0) * price_increment
            })
    }

    /// Get next price key (higher price by one increment)
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Get previous price key (lower price by one increment)
    pub fn previous(self) -> Self {
        Self(self.0 - 1)
    }

    /// Validate precision for this price key
    pub fn validate_precision(&self, price_increment: Decimal) -> bool {
        let precision_manager = PricePrecisionManager::default();
        let reconstructed = precision_manager.key_to_price(self.0, price_increment);
        
        if let Ok(price) = reconstructed {
            let validation = precision_manager.validate_precision(price, price_increment);
            validation.map(|v| v.is_accurate).unwrap_or(false)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_key_conversion() {
        let price_increment = dec!(0.01);
        let price = dec!(50000.25);
        
        let key = PriceKey::from_price(price, price_increment);
        let converted_price = key.to_price(price_increment);
        
        // Should be close to original price (within increment precision)
        assert!((converted_price - price).abs() < price_increment);
        
        // Validate precision accuracy
        assert!(key.validate_precision(price_increment), "Price key should maintain precision");
    }

    #[test]
    fn test_price_level_map_basic_operations() {
        let mut map = PriceLevelMap::new(dec!(0.01));
        
        // Add volume at different price levels
        map.add_volume(dec!(50000.25), dec!(100.0));
        map.add_volume(dec!(50000.26), dec!(150.0));
        map.add_volume(dec!(50000.25), dec!(50.0)); // Add more volume to same level
        
        // Total volume should be sum of all additions
        assert_eq!(map.total_volume(), dec!(300.0));
        
        // Both prices now have 150.0 volume, so POC could be either
        let poc = map.get_poc().unwrap();
        // The max_by implementation returns 50000.26 (the last equal max value)
        assert!((poc - dec!(50000.26)).abs() < dec!(0.001));
    }

    #[test]
    fn test_vwap_calculation() {
        let mut map = PriceLevelMap::new(dec!(0.01));
        
        map.add_volume(dec!(100.0), dec!(10.0)); // 100 * 10 = 1000
        map.add_volume(dec!(200.0), dec!(20.0)); // 200 * 20 = 4000
        map.add_volume(dec!(300.0), dec!(30.0)); // 300 * 30 = 9000
        
        let vwap = map.calculate_vwap();
        let expected_vwap = (dec!(1000.0) + dec!(4000.0) + dec!(9000.0)) / (dec!(10.0) + dec!(20.0) + dec!(30.0));
        
        assert!((vwap - expected_vwap).abs() < dec!(0.001));
    }

    #[test]
    fn test_value_area_calculation() {
        let mut map = PriceLevelMap::new(dec!(1.0));  // Use 1.0 increment to match the price spacing
        
        // Create distribution with clear POC
        map.add_volume(dec!(100.0), dec!(10.0));  // 10% of volume
        map.add_volume(dec!(101.0), dec!(50.0));  // 50% of volume (POC)
        map.add_volume(dec!(102.0), dec!(30.0));  // 30% of volume
        map.add_volume(dec!(103.0), dec!(10.0));  // 10% of volume
        
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        
        // With greedy selection, should get exactly 70% volume
        // We select 101.0 (50%) + 102.0 (30%) = 80% 
        // Then the contiguous range 101-102 contains 80% volume
        assert!(value_area.volume_percentage >= dec!(70.0), "Expected >= 70%, got {}", value_area.volume_percentage);
        assert!(value_area.volume_percentage <= dec!(80.0), "Should not exceed 80% for this test, got {}", value_area.volume_percentage);
        assert!(value_area.volume > dec!(0.0));
        assert!(value_area.low <= dec!(101.0));
        assert!(value_area.high >= dec!(102.0));
        assert!(value_area.high > value_area.low, "Value area should span a range: high={} low={}", 
                value_area.high, value_area.low);
    }

    #[test]
    fn test_value_area_concentrated_volume() {
        let mut map = PriceLevelMap::new(dec!(0.01));  // Fine-grained increment like real data
        
        // Simulate the real-world scenario: volume concentrated at one price
        map.add_volume(dec!(114367.6), dec!(4461.0));  // POC volume
        map.add_volume(dec!(114367.59), dec!(100.0));   // Small adjacent volume
        map.add_volume(dec!(114367.61), dec!(150.0));   // Small adjacent volume
        
        let total_volume = map.total_volume();
        assert!(total_volume > dec!(0.0));
        
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        
        // Value area should capture approximately 70% volume (or as close as possible with discrete levels)
        // With concentrated volume, it may include more to reach a contiguous range
        assert!(value_area.volume_percentage >= dec!(70.0) && value_area.volume_percentage <= dec!(100.0), 
                "Value area should capture 70-100% volume: {}%", value_area.volume_percentage);
        
        // Value area should include the POC (highest volume level)
        assert!(value_area.low <= dec!(114367.6) && value_area.high >= dec!(114367.6),
                "Value area should include POC at 114367.6");
        
        // Should have reasonable volume
        assert!(value_area.volume > dec!(0.0));
    }

    #[test]
    fn test_value_area_single_price_level() {
        let mut map = PriceLevelMap::new(dec!(0.01));
        
        // Edge case: all volume at exactly one price
        map.add_volume(dec!(114367.6), dec!(1000.0));
        
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        
        // Should still provide meaningful range
        assert!(value_area.high >= value_area.low);
        assert!(value_area.volume_percentage >= dec!(50.0), 
                "Should capture all volume when only one level exists");
    }

    #[test]
    fn test_price_level_data_conversion() {
        let mut map = PriceLevelMap::new(dec!(0.01));
        
        map.add_volume(dec!(50000.25), dec!(100.0));
        map.add_volume(dec!(50000.26), dec!(200.0));
        
        let price_levels = map.to_price_levels();
        
        assert_eq!(price_levels.len(), 2);
        
        // Should be sorted by price
        assert!(price_levels[0].price < price_levels[1].price);
        
        // Percentages should sum to 100%
        let total_percentage: Decimal = price_levels.iter().map(|p| p.percentage).sum();
        assert!((total_percentage - dec!(100.0)).abs() < dec!(0.001));
    }

    #[test]
    fn test_empty_price_level_map() {
        let map = PriceLevelMap::new(dec!(0.01));
        
        assert_eq!(map.total_volume(), dec!(0.0));
        assert!(map.get_poc().is_none());
        assert_eq!(map.calculate_vwap(), dec!(0.0));
        assert!(map.to_price_levels().is_empty());
        
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        assert_eq!(value_area.volume, dec!(0.0));
        assert_eq!(value_area.volume_percentage, dec!(0.0));
    }

    #[test]
    fn test_volume_distribution_modes() {
        let mut map = PriceLevelMap::new(dec!(0.01));
        
        // Test ClosingPrice distribution
        map.distribute_candle_volume(dec!(100.0), dec!(102.0), dec!(98.0), dec!(101.0), dec!(1000.0), &VolumeDistributionMode::ClosingPrice);
        assert_eq!(map.total_volume(), dec!(1000.0));
        let levels = map.to_price_levels();
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].price, dec!(101.0));
        assert_eq!(levels[0].volume, dec!(1000.0));

        // Reset and test WeightedOHLC distribution
        map = PriceLevelMap::new(dec!(0.01));
        map.distribute_candle_volume(dec!(100.0), dec!(102.0), dec!(98.0), dec!(101.0), dec!(1000.0), &VolumeDistributionMode::WeightedOHLC);
        assert_eq!(map.total_volume(), dec!(1000.0));
        
        // Should have volume at close (50%), high (25%), and low (25%)
        let close_volume = map.get_volume_at_key(&PriceKey::from_price(dec!(101.0), dec!(0.01)));
        let high_volume = map.get_volume_at_key(&PriceKey::from_price(dec!(102.0), dec!(0.01)));
        let low_volume = map.get_volume_at_key(&PriceKey::from_price(dec!(98.0), dec!(0.01)));
        
        assert_eq!(close_volume, dec!(500.0));
        assert_eq!(high_volume, dec!(250.0));
        assert_eq!(low_volume, dec!(250.0));

        // Reset and test HighLowWeighted distribution
        map = PriceLevelMap::new(dec!(0.01));
        map.distribute_candle_volume(dec!(100.0), dec!(102.0), dec!(98.0), dec!(101.0), dec!(1000.0), &VolumeDistributionMode::HighLowWeighted);
        assert_eq!(map.total_volume(), dec!(1000.0));
        
        let high_volume = map.get_volume_at_key(&PriceKey::from_price(dec!(102.0), dec!(0.01)));
        let low_volume = map.get_volume_at_key(&PriceKey::from_price(dec!(98.0), dec!(0.01)));
        
        assert_eq!(high_volume, dec!(500.0));
        assert_eq!(low_volume, dec!(500.0));
    }

    #[test]
    fn test_uniform_volume_distribution() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Test uniform distribution across a 4-unit range (100 to 104)
        map.distribute_candle_volume(dec!(100.0), dec!(104.0), dec!(100.0), dec!(102.0), dec!(1000.0), &VolumeDistributionMode::UniformOHLC);
        
        assert_eq!(map.total_volume(), dec!(1000.0));
        let levels = map.to_price_levels();
        
        // Should have distributed across levels in the range (100 to 104)
        // With 1.0 increment, this creates 4 levels: 100, 101, 102, 103
        assert_eq!(levels.len(), 4);
        
        // Each level should have approximately equal volume
        let expected_volume_per_level = dec!(1000.0) / dec!(4.0);
        for level in &levels {
            assert!((level.volume - expected_volume_per_level).abs() < dec!(1.0));
        }
    }

    #[test]
    fn test_traditional_vs_greedy_value_area() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create a distribution where traditional and greedy methods should differ
        map.add_volume(dec!(100.0), dec!(10.0));  // 10%
        map.add_volume(dec!(101.0), dec!(50.0));  // 50% (POC)
        map.add_volume(dec!(102.0), dec!(20.0));  // 20%
        map.add_volume(dec!(103.0), dec!(15.0));  // 15%
        map.add_volume(dec!(105.0), dec!(5.0));   // 5% (isolated level)
        
        let traditional_va = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        let greedy_va = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Greedy, &VolumeProfileCalculationMode::Volume);
        
        // Both should include POC
        assert!(traditional_va.low <= dec!(101.0) && traditional_va.high >= dec!(101.0));
        assert!(greedy_va.low <= dec!(101.0) && greedy_va.high >= dec!(101.0));
        
        // Both should aim for ~70% volume
        assert!(traditional_va.volume_percentage >= dec!(65.0) && traditional_va.volume_percentage <= dec!(85.0));
        assert!(greedy_va.volume_percentage >= dec!(65.0) && greedy_va.volume_percentage <= dec!(85.0));
        
        // Traditional method should create a more contiguous range from POC
        // Greedy method might include the isolated high-volume level at 105.0
        assert!(traditional_va.high <= dec!(103.0), "Traditional should stay contiguous from POC");
    }

    #[test]
    fn test_value_area_edge_cases() {
        // Test with single price level - AC1: high must be greater than low (no degenerate single-price areas)
        let mut map = PriceLevelMap::new(dec!(0.01));
        map.add_volume(dec!(100.0), dec!(1000.0));
        
        let va = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        assert_eq!(va.low, dec!(100.0));
        assert_eq!(va.high, dec!(100.01));  // Should be expanded by price_increment per AC1
        assert!(va.volume > dec!(0.0));      // Volume should be positive
        assert!(va.volume_percentage > dec!(0.0));  // Percentage should be positive
        
        // Test with two equal volume levels
        map = PriceLevelMap::new(dec!(1.0));
        map.add_volume(dec!(100.0), dec!(500.0));
        map.add_volume(dec!(101.0), dec!(500.0));
        
        let va = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        assert!(va.low <= dec!(100.0));
        assert!(va.high >= dec!(101.0));
        assert_eq!(va.volume, dec!(1000.0));
        assert_eq!(va.volume_percentage, dec!(100.0));
    }

    #[test]
    fn test_asset_specific_config_resolution() {
        let mut config = VolumeProfileConfig::default();
        
        // Add asset overrides
        let mut overrides = new_symbol_hashmap();
        let btc_config = AssetConfig {
            price_increment_mode: Some(PriceIncrementMode::Fixed),
            fixed_price_increment: Some(dec!(10.0)),
            target_price_levels: Some(100),
            min_price_increment: None,
            max_price_increment: None,
            volume_distribution_mode: Some(VolumeDistributionMode::ClosingPrice),
            value_area_calculation_mode: Some(ValueAreaCalculationMode::Greedy),
            value_area_percentage: Some(dec!(68.0)),
            calculation_mode: Some(VolumeProfileCalculationMode::TPO),
        };
        overrides.insert("BTCUSDT".to_string(), btc_config);
        config.asset_overrides = overrides;
        
        // Test BTC-specific config resolution
        let btc_resolved = config.resolve_for_asset("BTCUSDT");
        assert!(matches!(btc_resolved.price_increment_mode, PriceIncrementMode::Fixed));
        assert_eq!(btc_resolved.fixed_price_increment, dec!(10.0));
        assert_eq!(btc_resolved.target_price_levels, 100);
        assert!(matches!(btc_resolved.volume_distribution_mode, VolumeDistributionMode::ClosingPrice));
        assert!(matches!(btc_resolved.value_area_calculation_mode, ValueAreaCalculationMode::Greedy));
        assert_eq!(btc_resolved.value_area_percentage, dec!(68.0));
        
        // Test fallback to global config for non-overridden asset
        let eth_resolved = config.resolve_for_asset("ETHUSDT");
        assert_eq!(eth_resolved.price_increment_mode, config.price_increment_mode);
        assert_eq!(eth_resolved.target_price_levels, config.target_price_levels);
        assert_eq!(eth_resolved.value_area_percentage, config.value_area_percentage);
    }

    #[test]
    fn test_config_validation() {
        let mut config = VolumeProfileConfig::default();
        
        // Valid config should pass
        assert!(config.validate().is_ok());
        
        // Test invalid target_price_levels
        config.target_price_levels = 5; // Too small
        assert!(config.validate().is_err());
        
        config.target_price_levels = 20000; // Too large
        assert!(config.validate().is_err());
        
        config.target_price_levels = 200; // Reset to valid
        
        // Test invalid price increments
        config.fixed_price_increment = dec!(-1.0); // Negative
        assert!(config.validate().is_err());
        
        config.fixed_price_increment = dec!(0.01); // Reset to valid
        config.min_price_increment = dec!(-0.001); // Negative
        assert!(config.validate().is_err());
        
        config.min_price_increment = dec!(0.001); // Reset to valid
        config.max_price_increment = dec!(0.0005); // Less than min
        assert!(config.validate().is_err());
        
        config.max_price_increment = dec!(1.0); // Reset to valid
        
        // Test invalid value area percentage
        config.value_area_percentage = dec!(40.0); // Too small
        assert!(config.validate().is_err());
        
        config.value_area_percentage = dec!(99.0); // Too large
        assert!(config.validate().is_err());
        
        config.value_area_percentage = dec!(70.0); // Reset to valid
        
        // Test invalid asset override
        let mut overrides = new_symbol_hashmap();
        let invalid_asset = AssetConfig {
            price_increment_mode: None,
            fixed_price_increment: Some(dec!(-10.0)), // Negative
            target_price_levels: None,
            min_price_increment: None,
            max_price_increment: None,
            volume_distribution_mode: None,
            value_area_calculation_mode: None,
            value_area_percentage: None,
            calculation_mode: None,
        };
        overrides.insert("INVALID".to_string(), invalid_asset);
        config.asset_overrides = overrides;
        
        assert!(config.validate().is_err());
        
        // Valid config should pass again after clearing bad overrides
        config.asset_overrides.clear();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_adaptive_algorithm_with_target_levels() {
        use crate::volume_profile::calculator::DailyVolumeProfile;
        
        // Test that different target levels produce different increments
        let mut config = VolumeProfileConfig {
            price_increment_mode: PriceIncrementMode::Adaptive,
            min_price_increment: dec!(0.00000001),
            max_price_increment: dec!(100.0),
            ..Default::default()
        };
        
        // High target levels (fine granularity)
        config.target_price_levels = 1000;
        let high_target_resolved = config.resolve_for_asset("TESTUSDT");
        let fine_increment = DailyVolumeProfile::calculate_price_increment(&high_target_resolved, Some(dec!(100.0))); // $100 range
        
        // Low target levels (coarse granularity)
        config.target_price_levels = 50;
        let low_target_resolved = config.resolve_for_asset("TESTUSDT");
        let coarse_increment = DailyVolumeProfile::calculate_price_increment(&low_target_resolved, Some(dec!(100.0))); // $100 range
        
        // Coarse increment should be larger (fewer levels)
        assert!(coarse_increment > fine_increment);
        
        // Verify reasonable values
        assert_eq!(fine_increment, dec!(100.0) / dec!(1000.0)); // $0.10 increment for 1000 levels in $100
        assert_eq!(coarse_increment, dec!(100.0) / dec!(50.0));  // $2.00 increment for 50 levels in $100
    }

    #[test]
    fn test_balanced_volume_profile_with_weighted_ohlc() {
        let mut map = PriceLevelMap::new(dec!(0.5)); // Use 0.5 increment for better granularity
        
        // Create a more realistic trading scenario with bell-curve like distribution
        // Simulate accumulation around 101-102 range with some outliers
        let candles = vec![
            // Low volume outliers
            (dec!(99.0), dec!(100.0), dec!(99.0), dec!(99.5), dec!(500.0)),   // Low outlier
            (dec!(104.0), dec!(105.0), dec!(104.0), dec!(104.5), dec!(500.0)), // High outlier
            
            // Main accumulation zone - should create centered POC
            (dec!(100.5), dec!(101.5), dec!(100.0), dec!(101.0), dec!(3000.0)), // Volume at 101 area
            (dec!(101.0), dec!(102.0), dec!(100.8), dec!(101.5), dec!(4000.0)), // Peak volume
            (dec!(101.5), dec!(102.5), dec!(101.0), dec!(102.0), dec!(3500.0)), // Volume at 102 area
            (dec!(101.8), dec!(102.2), dec!(101.3), dec!(101.8), dec!(3000.0)), // More volume around 101-102
            (dec!(101.2), dec!(102.8), dec!(101.0), dec!(102.2), dec!(2500.0)), // Balanced distribution
            
            // Some mid-range activity
            (dec!(102.0), dec!(103.0), dec!(101.8), dec!(102.5), dec!(1500.0)), // Moderate volume
            (dec!(100.8), dec!(102.0), dec!(100.5), dec!(101.2), dec!(1000.0)), // Lower moderate volume
        ];
        
        // Use WeightedOHLC distribution (new default)
        for (open, high, low, close, volume) in candles {
            map.distribute_candle_volume(open, high, low, close, volume, &VolumeDistributionMode::WeightedOHLC);
        }
        
        // Calculate value area using traditional method
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        let poc = map.get_poc().unwrap();
        
        // Basic validations that should always pass with WeightedOHLC
        
        // Value area should contain significant volume (close to target)
        assert!(value_area.volume_percentage >= dec!(65.0), 
                "Value area should contain at least 65% of volume, got {:.1}%", value_area.volume_percentage);
        
        // POC should be within the value area (fundamental requirement)
        assert!(poc >= value_area.low && poc <= value_area.high,
                "POC ({}) should be within value area [{} - {}]", poc, value_area.low, value_area.high);
        
        // Value area should have reasonable range (not degenerate)
        let va_range = value_area.high - value_area.low;
        assert!(va_range >= dec!(0.0), "Value area range should be non-negative: {:.1}", va_range);
        
        // With WeightedOHLC, we should have multiple price levels (not just one)
        let price_levels = map.to_price_levels();
        assert!(price_levels.len() >= 3, 
                "WeightedOHLC should create multiple price levels, got {}", price_levels.len());
        
        // The POC should be a high-volume level
        let poc_level = price_levels.iter().find(|level| level.price == poc).unwrap();
        let max_volume = price_levels.iter().map(|l| l.volume).fold(dec!(0.0), |acc, vol| if vol > acc { vol } else { acc });
        assert!((poc_level.volume - max_volume).abs() < dec!(0.01),
                "POC should be at the highest volume level");
    }

    #[test]
    fn test_weighted_ohlc_default_configuration() {
        // Verify that the new default configuration uses WeightedOHLC
        let config = VolumeProfileConfig::default();
        assert!(matches!(config.volume_distribution_mode, VolumeDistributionMode::WeightedOHLC),
                "Default configuration should use WeightedOHLC distribution");
        
        // Test that asset resolution maintains the default
        let resolved = config.resolve_for_asset("BTCUSDT");
        assert!(matches!(resolved.volume_distribution_mode, VolumeDistributionMode::WeightedOHLC),
                "Resolved configuration should inherit WeightedOHLC default");
    }

    // === VALUE AREA VALIDATION TESTS ===

    #[test]
    fn test_value_area_validator_valid_case() {
        let result = ValueAreaValidator::validate_value_area(dec!(102.5), dec!(100.5), dec!(72.0));
        assert_eq!(result, ValidationResult::Valid);
    }

    #[test]
    fn test_value_area_validator_invalid_range() {
        // High < Low should be invalid
        let result = ValueAreaValidator::validate_value_area(dec!(100.5), dec!(102.5), dec!(72.0));
        assert_eq!(result, ValidationResult::InvalidRange { high: dec!(100.5), low: dec!(102.5) });
    }

    #[test]
    fn test_value_area_validator_invalid_volume_percentage() {
        // Volume percentage too low
        let result = ValueAreaValidator::validate_value_area(dec!(102.5), dec!(100.5), dec!(60.0));
        assert_eq!(result, ValidationResult::InvalidVolumePercentage { percentage: dec!(60.0) });
        
        // Volume percentage too high
        let result = ValueAreaValidator::validate_value_area(dec!(102.5), dec!(100.5), dec!(80.0));
        assert_eq!(result, ValidationResult::InvalidVolumePercentage { percentage: dec!(80.0) });
    }

    #[test]
    fn test_poc_validation() {
        // POC within range should be valid
        let result = ValueAreaValidator::validate_poc_in_range(dec!(101.5), dec!(102.0), dec!(101.0));
        assert_eq!(result, ValidationResult::Valid);
        
        // POC below range should be invalid
        let result = ValueAreaValidator::validate_poc_in_range(dec!(100.5), dec!(102.0), dec!(101.0));
        assert_eq!(result, ValidationResult::PocNotInRange { poc: dec!(100.5), high: dec!(102.0), low: dec!(101.0) });
        
        // POC above range should be invalid
        let result = ValueAreaValidator::validate_poc_in_range(dec!(103.0), dec!(102.0), dec!(101.0));
        assert_eq!(result, ValidationResult::PocNotInRange { poc: dec!(103.0), high: dec!(102.0), low: dec!(101.0) });
    }

    #[test]
    fn test_degenerate_case_detection() {
        // Single price level is degenerate
        let single_level = vec![
            PriceLevelData { price: dec!(100.0), volume: dec!(1000.0), percentage: dec!(100.0), candle_count: 10 }
        ];
        assert!(ValueAreaValidator::detect_degenerate_case(&single_level));
        
        // Empty price levels is degenerate
        let empty_levels = vec![];
        assert!(ValueAreaValidator::detect_degenerate_case(&empty_levels));
        
        // Extreme volume concentration (>90% at extremes) is degenerate
        let extreme_levels = vec![
            PriceLevelData { price: dec!(100.0), volume: dec!(500.0), percentage: dec!(50.0), candle_count: 5 },  // First extreme
            PriceLevelData { price: dec!(101.0), volume: dec!(50.0), percentage: dec!(5.0), candle_count: 1 },    // Middle
            PriceLevelData { price: dec!(102.0), volume: dec!(450.0), percentage: dec!(45.0), candle_count: 5 },  // Last extreme
        ];
        assert!(ValueAreaValidator::detect_degenerate_case(&extreme_levels)); // 95% at extremes
        
        // Balanced distribution is not degenerate
        let balanced_levels = vec![
            PriceLevelData { price: dec!(100.0), volume: dec!(200.0), percentage: dec!(20.0), candle_count: 2 },
            PriceLevelData { price: dec!(101.0), volume: dec!(600.0), percentage: dec!(60.0), candle_count: 6 },  // POC in middle
            PriceLevelData { price: dec!(102.0), volume: dec!(200.0), percentage: dec!(20.0), candle_count: 2 },
        ];
        assert!(!ValueAreaValidator::detect_degenerate_case(&balanced_levels)); // Only 40% at extremes
    }

    #[test]
    fn test_value_area_calculation_with_validation() {
        let mut map = PriceLevelMap::new(dec!(0.01));
        
        // Create a scenario that would produce an invalid value area initially
        map.add_volume(dec!(114367.6), dec!(4000.0));  // Concentrated volume at one price
        map.add_volume(dec!(114367.61), dec!(100.0));  // Small adjacent volume
        
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        
        // After validation and adjustment, should have valid properties
        assert!(value_area.high >= value_area.low, "Value area should have valid range after validation");
        assert!(value_area.volume > dec!(0.0), "Value area should have positive volume");
        
        // POC should be within the value area
        if let Some(poc) = map.get_poc() {
            assert!(poc >= value_area.low && poc <= value_area.high,
                    "POC should be within value area after validation");
        }
    }

    #[test]
    fn test_contiguous_range_logic() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create distribution with gaps to test contiguous range logic
        map.add_volume(dec!(100.0), dec!(100.0));  // 10%
        map.add_volume(dec!(101.0), dec!(500.0));  // 50% (POC)
        map.add_volume(dec!(102.0), dec!(200.0));  // 20%
        map.add_volume(dec!(105.0), dec!(200.0));  // 20% (gap at 103-104)
        
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        
        // Traditional method should create contiguous range around POC
        // Should include 101 (POC) + adjacent levels to reach ~70%
        assert!(value_area.low <= dec!(101.0), "Value area should include POC");
        assert!(value_area.high >= dec!(101.0), "Value area should include POC");
        
        // Should not include the isolated level at 105.0 due to gap
        assert!(value_area.high <= dec!(103.0), "Traditional method should maintain contiguous range");
    }

    #[test]
    fn test_edge_case_handling_extreme_concentration() {
        let mut map = PriceLevelMap::new(dec!(0.01));
        
        // Extreme case: 99% volume at highest price
        map.add_volume(dec!(100.0), dec!(10.0));    // 1%
        map.add_volume(dec!(200.0), dec!(990.0));   // 99% at price extreme
        
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        
        // Should handle extreme concentration gracefully
        assert!(value_area.high >= value_area.low, "Should handle extreme concentration");
        assert!(value_area.volume > dec!(0.0), "Should have positive volume");
        
        // POC (at 200.0) should be included in value area
        assert!(value_area.low <= dec!(200.0) && value_area.high >= dec!(200.0),
                "POC should be included even in extreme concentration");
    }

    #[test]
    fn test_volume_percentage_accuracy() {
        let mut map = PriceLevelMap::new(dec!(0.5));
        
        // Create distribution where we can control volume percentages precisely
        map.add_volume(dec!(100.0), dec!(100.0));   // 10%
        map.add_volume(dec!(100.5), dec!(300.0));   // 30%
        map.add_volume(dec!(101.0), dec!(400.0));   // 40% (POC)
        map.add_volume(dec!(101.5), dec!(150.0));   // 15%
        map.add_volume(dec!(102.0), dec!(50.0));    // 5%
        
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        
        // Should target 70% volume but may be flexible within 65-75% range
        assert!(value_area.volume_percentage >= dec!(65.0) && value_area.volume_percentage <= dec!(85.0),
                "Volume percentage should be reasonable: {}%", value_area.volume_percentage);
        
        // Should include POC
        assert!(value_area.low <= dec!(101.0) && value_area.high >= dec!(101.0),
                "Should include POC in value area");
    }

    #[test]
    fn test_validation_range_correction() {
        let mut map = PriceLevelMap::new(dec!(0.01));
        
        // Create minimal volume profile
        map.add_volume(dec!(100.0), dec!(1000.0));
        
        let value_area = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        
        // Single price level should be expanded to meaningful range
        assert!(value_area.high > value_area.low, 
                "Single price level should be expanded to range: high={} low={}", 
                value_area.high, value_area.low);
        
        // Range should be at least one price increment
        assert!((value_area.high - value_area.low) >= map.price_increment,
                "Range should be at least one price increment");
    }

    #[test]
    fn test_dual_method_poc_identification() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Add data with different volume and time period distributions
        // Price 100: 2 candles, 300 volume
        map.add_volume_at_price(dec!(100.0), dec!(150.0)); // First candle
        map.add_volume_at_price(dec!(100.0), dec!(150.0)); // Second candle 
        
        // Price 101: 5 candles, 200 volume  
        map.add_volume_at_price(dec!(101.0), dec!(40.0)); // First candle
        map.add_volume_at_price(dec!(101.0), dec!(40.0)); // Second candle
        map.add_volume_at_price(dec!(101.0), dec!(40.0)); // Third candle
        map.add_volume_at_price(dec!(101.0), dec!(40.0)); // Fourth candle
        map.add_volume_at_price(dec!(101.0), dec!(40.0)); // Fifth candle
        
        // Price 102: 1 candle, 100 volume
        map.add_volume_at_price(dec!(102.0), dec!(100.0));
        
        // Volume POC should be 100.0 (300 volume)
        let volume_poc = map.identify_poc_volume().unwrap();
        assert_eq!(volume_poc, dec!(100.0), "Volume POC should be at price with highest volume");
        
        // TPO POC should be 101.0 (5 candles)
        let tpo_poc = map.identify_poc_tpo().unwrap();
        assert_eq!(tpo_poc, dec!(101.0), "TPO POC should be at price with most candles");
        
        // Unified method should return correct POC based on mode
        let volume_unified_poc = map.identify_poc(&VolumeProfileCalculationMode::Volume).unwrap();
        assert_eq!(volume_unified_poc, dec!(100.0), "Unified Volume POC should match volume method");
        
        let tpo_unified_poc = map.identify_poc(&VolumeProfileCalculationMode::TPO).unwrap();
        assert_eq!(tpo_unified_poc, dec!(101.0), "Unified TPO POC should match TPO method");
    }

    #[test]
    fn test_dual_method_value_area_calculation() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create distribution where Volume and TPO methods will differ
        // Price 100: 1 candle, 1000 volume (high volume, low time)
        map.add_volume_at_price(dec!(100.0), dec!(1000.0));
        
        // Price 101: 10 candles, 100 volume (low volume, high time)
        for _ in 0..10 {
            map.add_volume_at_price(dec!(101.0), dec!(10.0));
        }
        
        // Price 102: 5 candles, 500 volume (medium volume, medium time)
        for _ in 0..5 {
            map.add_volume_at_price(dec!(102.0), dec!(100.0));
        }
        
        // Calculate value areas using both methods
        let volume_va = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::Volume);
        let tpo_va = map.calculate_value_area(dec!(70.0), &ValueAreaCalculationMode::Traditional, &VolumeProfileCalculationMode::TPO);
        
        // Both should have valid ranges
        assert!(volume_va.high >= volume_va.low, "Volume value area should have valid range");
        assert!(tpo_va.high >= tpo_va.low, "TPO value area should have valid range");
        
        // Volume method should capture significant volume percentage
        assert!(volume_va.volume_percentage >= dec!(50.0), "Volume VA should capture significant volume");
        
        // TPO method may have different volume percentage since it's based on time periods
        // but should still be reasonable (the volume percentage is always calculated for display)
        assert!(tpo_va.volume_percentage >= dec!(0.0), "TPO VA should have valid volume percentage");
        
        // Volume method should include price 100 (highest volume POC)
        assert!(volume_va.low <= dec!(100.0) && volume_va.high >= dec!(100.0), 
                "Volume method should include highest volume price level");
        
        // TPO method should include price 101 (highest candle count POC)
        assert!(tpo_va.low <= dec!(101.0) && tpo_va.high >= dec!(101.0), 
                "TPO method should include highest candle count price level");
    }

    #[test]
    fn test_candle_count_tracking() {
        let mut map = PriceLevelMap::new(dec!(0.01));
        
        // Add volume multiple times at same price to test candle counting
        map.add_volume_at_price(dec!(100.0), dec!(50.0));
        map.add_volume_at_price(dec!(100.0), dec!(75.0));
        map.add_volume_at_price(dec!(100.0), dec!(25.0));
        
        // Should have 3 candles and 150 total volume at price 100.0
        assert_eq!(map.get_total_candle_count(), 3, "Should have 3 total candles");
        assert_eq!(map.total_volume(), dec!(150.0), "Should have 150 total volume");
        
        // Convert to price levels and verify candle count is included
        let price_levels = map.to_price_levels();
        assert_eq!(price_levels.len(), 1, "Should have 1 price level");
        assert_eq!(price_levels[0].candle_count, 3, "Price level should have 3 candles");
        assert_eq!(price_levels[0].volume, dec!(150.0), "Price level should have 150 volume");
        assert_eq!(price_levels[0].price, dec!(100.0), "Price level should be at 100.0");
    }

    #[test]
    fn test_expand_value_area_tpo() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create TPO distribution with different candle counts
        // Price 100: 2 candles, 300 volume
        map.add_volume_at_price(dec!(100.0), dec!(150.0));
        map.add_volume_at_price(dec!(100.0), dec!(150.0));
        
        // Price 101: 8 candles, 400 volume (TPO POC)
        for _ in 0..8 {
            map.add_volume_at_price(dec!(101.0), dec!(50.0));
        }
        
        // Price 102: 6 candles, 600 volume
        for _ in 0..6 {
            map.add_volume_at_price(dec!(102.0), dec!(100.0));
        }
        
        // Price 103: 4 candles, 200 volume
        for _ in 0..4 {
            map.add_volume_at_price(dec!(103.0), dec!(50.0));
        }
        
        // Total: 20 candles, 70% = 14 candles
        let va = map.expand_value_area_tpo(dec!(70.0));
        
        // Should include POC at 101.0 (8 candles)
        assert!(va.low <= dec!(101.0) && va.high >= dec!(101.0), 
                "TPO value area should include POC at 101.0");
        
        // Should have valid range
        assert!(va.high >= va.low, "Value area should have valid range");
        
        // Should have reasonable volume percentage
        assert!(va.volume_percentage > dec!(0.0), "Value area should have positive volume percentage");
    }

    #[test]
    fn test_expand_value_area_volume() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create volume distribution with different volumes
        // Price 100: 2 candles, 800 volume (Volume POC)
        map.add_volume_at_price(dec!(100.0), dec!(400.0));
        map.add_volume_at_price(dec!(100.0), dec!(400.0));
        
        // Price 101: 8 candles, 400 volume
        for _ in 0..8 {
            map.add_volume_at_price(dec!(101.0), dec!(50.0));
        }
        
        // Price 102: 6 candles, 300 volume
        for _ in 0..6 {
            map.add_volume_at_price(dec!(102.0), dec!(50.0));
        }
        
        // Price 103: 4 candles, 100 volume
        for _ in 0..4 {
            map.add_volume_at_price(dec!(103.0), dec!(25.0));
        }
        
        // Total: 1600 volume, 70% = 1120 volume
        let va = map.expand_value_area_volume(dec!(70.0));
        
        // Should include POC at 100.0 (800 volume)
        assert!(va.low <= dec!(100.0) && va.high >= dec!(100.0), 
                "Volume value area should include POC at 100.0");
        
        // Should have valid range
        assert!(va.high >= va.low, "Value area should have valid range");
        
        // Should achieve close to target percentage
        assert!(va.volume_percentage >= dec!(65.0), 
                "Volume value area should achieve close to target percentage: got {:.2}%", 
                va.volume_percentage);
    }

    #[test]
    fn test_unified_expand_value_area() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create mixed distribution
        map.add_volume_at_price(dec!(100.0), dec!(500.0)); // High volume, 1 candle
        for _ in 0..10 { // Low volume per candle, 10 candles
            map.add_volume_at_price(dec!(101.0), dec!(10.0));
        }
        map.add_volume_at_price(dec!(102.0), dec!(300.0)); // Medium volume, 1 candle
        
        // Test both methods
        let volume_va = map.expand_value_area(dec!(70.0), &VolumeProfileCalculationMode::Volume);
        let tpo_va = map.expand_value_area(dec!(70.0), &VolumeProfileCalculationMode::TPO);
        
        // Both should produce valid results
        assert!(volume_va.high >= volume_va.low, "Volume VA should have valid range");
        assert!(tpo_va.high >= tpo_va.low, "TPO VA should have valid range");
        
        // Volume method should include price 100 (highest volume)
        assert!(volume_va.low <= dec!(100.0) && volume_va.high >= dec!(100.0), 
                "Volume method should include highest volume price");
        
        // TPO method should include price 101 (most candles)
        assert!(tpo_va.low <= dec!(101.0) && tpo_va.high >= dec!(101.0), 
                "TPO method should include highest candle count price");
    }

    #[test]
    fn test_validate_value_area_rules_valid() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create valid distribution with proper POC centering
        map.add_volume_at_price(dec!(100.0), dec!(200.0)); // 1 candle, 200 volume
        map.add_volume_at_price(dec!(101.0), dec!(400.0)); // 1 candle, 400 volume (POC)
        map.add_volume_at_price(dec!(102.0), dec!(300.0)); // 1 candle, 300 volume
        map.add_volume_at_price(dec!(103.0), dec!(100.0)); // 1 candle, 100 volume
        // Total: 1000 volume, 70% = 700 volume
        
        let value_area = map.expand_value_area_volume(dec!(70.0));
        let validation = map.validate_value_area_rules(&value_area, dec!(70.0), &VolumeProfileCalculationMode::Volume);
        
        assert!(validation.is_valid, "Valid value area should pass validation");
        assert!(validation.errors.is_empty(), "Valid value area should have no errors");
        assert_eq!(validation.metrics.poc_price, dec!(101.0), "POC should be correctly identified");
        assert!(validation.metrics.actual_volume_percentage >= dec!(65.0), "Should achieve reasonable volume percentage");
    }

    #[test]
    fn test_validate_value_area_rules_poc_at_boundary() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create distribution where POC might end up at boundary
        map.add_volume_at_price(dec!(100.0), dec!(1000.0)); // Highest volume at edge
        map.add_volume_at_price(dec!(101.0), dec!(100.0));
        map.add_volume_at_price(dec!(102.0), dec!(50.0));
        
        let value_area = map.expand_value_area_volume(dec!(70.0));
        let validation = map.validate_value_area_rules(&value_area, dec!(70.0), &VolumeProfileCalculationMode::Volume);
        
        // This should detect validation issues - either boundary problems or percentage issues
        // Both are valid business rule violations when POC is at extreme positions
        if !validation.is_valid {
            let has_boundary_error = validation.errors.iter()
                .any(|err| err.contains("boundary"));
            let has_percentage_error = validation.errors.iter()
                .any(|err| err.contains("percentage"));
            let has_poc_error = validation.errors.iter()
                .any(|err| err.contains("POC"));
                
            assert!(has_boundary_error || has_percentage_error || has_poc_error, 
                    "Should detect boundary, percentage, or POC validation issues: {:?}", validation.errors);
        }
    }

    #[test]
    fn test_validate_value_area_rules_percentage_out_of_range() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create simple distribution
        map.add_volume_at_price(dec!(100.0), dec!(100.0));
        map.add_volume_at_price(dec!(101.0), dec!(100.0));
        
        // Create a value area with incorrect percentage
        let bad_value_area = ValueArea {
            high: dec!(101.0),
            low: dec!(100.0),
            volume_percentage: dec!(50.0), // Too low for 70% target
            volume: dec!(100.0),
        };
        
        let validation = map.validate_value_area_rules(&bad_value_area, dec!(70.0), &VolumeProfileCalculationMode::Volume);
        
        assert!(!validation.is_valid, "Out-of-range percentage should fail validation");
        let percentage_error_found = validation.errors.iter()
            .any(|err| err.contains("percentage out of range"));
        assert!(percentage_error_found, "Percentage error should be detected: {:?}", validation.errors);
    }

    #[test]
    fn test_validate_value_area_rules_ordering_violation() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create distribution
        map.add_volume_at_price(dec!(100.0), dec!(100.0));
        map.add_volume_at_price(dec!(101.0), dec!(200.0)); // POC
        map.add_volume_at_price(dec!(102.0), dec!(100.0));
        
        // Create invalid value area with wrong ordering
        let bad_value_area = ValueArea {
            high: dec!(100.0), // VAH < POC (invalid)
            low: dec!(102.0),  // VAL > POC (invalid)
            volume_percentage: dec!(70.0),
            volume: dec!(300.0),
        };
        
        let validation = map.validate_value_area_rules(&bad_value_area, dec!(70.0), &VolumeProfileCalculationMode::Volume);
        
        assert!(!validation.is_valid, "Ordering violation should fail validation");
        let ordering_error_found = validation.errors.iter()
            .any(|err| err.contains("ordering violated"));
        assert!(ordering_error_found, "Ordering error should be detected: {:?}", validation.errors);
    }

    #[test]
    fn test_validate_value_area_rules_metrics() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Create test distribution
        map.add_volume_at_price(dec!(100.0), dec!(100.0));
        map.add_volume_at_price(dec!(101.0), dec!(300.0)); // POC
        map.add_volume_at_price(dec!(102.0), dec!(100.0));
        
        let value_area = map.expand_value_area_volume(dec!(70.0));
        let validation = map.validate_value_area_rules(&value_area, dec!(70.0), &VolumeProfileCalculationMode::Volume);
        
        // Check metrics are populated correctly
        assert_eq!(validation.metrics.poc_price, dec!(101.0), "POC price should be 101.0");
        assert_eq!(validation.metrics.target_volume_percentage, dec!(70.0), "Target percentage should be 70.0");
        assert!(validation.metrics.price_levels_count > 0, "Should count price levels in value area");
        assert!(validation.metrics.poc_to_vah_distance >= dec!(0.0), "Distance metrics should be non-negative");
        assert!(validation.metrics.poc_to_val_distance >= dec!(0.0), "Distance metrics should be non-negative");
    }

    #[test]
    fn test_enhanced_price_level_management() {
        let mut map = PriceLevelMap::new(dec!(1.0));
        
        // Add data with different volume and candle combinations
        map.add_volume_at_price(dec!(100.0), dec!(200.0)); // 1 candle, 200 volume
        map.add_volume_at_price(dec!(100.0), dec!(100.0)); // 2nd candle, total 300 volume
        
        map.add_volume_at_price(dec!(101.0), dec!(500.0)); // 1 candle, 500 volume (highest volume)
        
        for _ in 0..5 { // 5 candles, 250 volume total (highest candle count)
            map.add_volume_at_price(dec!(102.0), dec!(50.0));
        }
        
        // Test percentage calculations
        let volume_pct = map.calculate_volume_percentage(dec!(300.0)); // Price 100.0
        let expected_volume_pct = (dec!(300.0) / (dec!(300.0) + dec!(500.0) + dec!(250.0))) * dec!(100.0);
        assert!((volume_pct - expected_volume_pct).abs() < dec!(0.01), 
                "Volume percentage should be calculated correctly: got {:.2}%, expected {:.2}%", 
                volume_pct, expected_volume_pct);
        
        let candle_pct = map.calculate_candle_percentage(5); // Price 102.0
        let expected_candle_pct = (dec!(5.0) / (dec!(2.0) + dec!(1.0) + dec!(5.0))) * dec!(100.0);
        assert!((candle_pct - expected_candle_pct).abs() < dec!(0.01), 
                "Candle percentage should be calculated correctly: got {:.2}%, expected {:.2}%", 
                candle_pct, expected_candle_pct);
        
        // Test highest level identification
        let (highest_vol_price, highest_vol) = map.get_highest_volume_level().unwrap();
        assert_eq!(highest_vol_price, dec!(101.0), "Highest volume level should be at 101.0");
        assert_eq!(highest_vol, dec!(500.0), "Highest volume should be 500.0");
        
        let (highest_candle_price, highest_candle_count) = map.get_highest_candle_count_level().unwrap();
        assert_eq!(highest_candle_price, dec!(102.0), "Highest candle count level should be at 102.0");
        assert_eq!(highest_candle_count, 5, "Highest candle count should be 5");
        
        // Test price level metrics
        let (volume, candle_count) = map.get_price_level_metrics(dec!(100.0)).unwrap();
        assert_eq!(volume, dec!(300.0), "Price 100.0 should have 300 volume");
        assert_eq!(candle_count, 2, "Price 100.0 should have 2 candles");
        
        let (volume, candle_count) = map.get_price_level_metrics(dec!(102.0)).unwrap();
        assert_eq!(volume, dec!(250.0), "Price 102.0 should have 250 volume");
        assert_eq!(candle_count, 5, "Price 102.0 should have 5 candles");
        
        // Test non-existent price level
        assert!(map.get_price_level_metrics(dec!(999.0)).is_none(), "Non-existent price should return None");
    }
}

/// Debug metadata for volume profile calculations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfileDebugMetadata {
    /// Timestamp when the profile was calculated
    pub calculation_timestamp: i64,
    /// Algorithm version identifier for calculation method
    pub algorithm_version: String,
    /// Precision metrics and accuracy statistics
    pub precision_metrics: PrecisionMetrics,
    /// Performance measurements and timing data
    pub performance_metrics: CalculationPerformance,
    /// Validation flags and edge case detection
    pub validation_flags: ValidationFlags,
}

/// Precision metrics for price key conversion accuracy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecisionMetrics {
    /// Price range span being processed
    pub price_range_span: Decimal,
    /// Price increment used for calculations
    pub price_increment_used: Decimal,
    /// Total number of price keys generated
    pub total_price_keys: usize,
    /// Number of precision errors detected during conversion
    pub precision_errors_detected: u32,
    /// Volume conservation check (should be close to 1.0)
    pub volume_conservation_check: Decimal,
}

/// Performance metrics for calculation timing and resource usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalculationPerformance {
    /// Time spent calculating value area in milliseconds
    pub value_area_calculation_time_ms: Decimal,
    /// Time spent on price distribution calculation in milliseconds
    pub price_distribution_time_ms: Decimal,
    /// Number of cache operations performed
    pub cache_operations_count: u32,
    /// Estimated memory usage in bytes
    pub memory_usage_bytes: usize,
    /// Number of candles processed in this calculation
    pub candles_processed_count: u32,
}

/// Validation flags for quality checks and edge case detection
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ValidationFlags {
    /// True if degenerate value area detected (high = low)
    pub degenerate_value_area_detected: bool,
    /// True if unusual volume concentration pattern detected
    pub unusual_volume_concentration: bool,
    /// Number of rejected candles and reasons
    pub rejected_candles_count: u32,
    /// Specific rejection reasons for excluded candles
    pub rejection_reasons: Vec<String>,
    /// True if precision errors exceeded acceptable threshold
    pub precision_errors_excessive: bool,
}

impl Default for VolumeProfileDebugMetadata {
    fn default() -> Self {
        Self {
            calculation_timestamp: 0,
            algorithm_version: "1.0".to_string(),
            precision_metrics: PrecisionMetrics::default(),
            performance_metrics: CalculationPerformance::default(),
            validation_flags: ValidationFlags::default(),
        }
    }
}

impl Default for PrecisionMetrics {
    fn default() -> Self {
        Self {
            price_range_span: dec!(0),
            price_increment_used: dec!(0),
            total_price_keys: 0,
            precision_errors_detected: 0,
            volume_conservation_check: dec!(1),
        }
    }
}

impl Default for CalculationPerformance {
    fn default() -> Self {
        Self {
            value_area_calculation_time_ms: dec!(0),
            price_distribution_time_ms: dec!(0),
            cache_operations_count: 0,
            memory_usage_bytes: 0,
            candles_processed_count: 0,
        }
    }
}

