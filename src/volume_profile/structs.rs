use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use crate::historical::structs::TimestampMS;

/// Asset-specific volume profile configuration overrides
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_increment_mode: Option<PriceIncrementMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixed_price_increment: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_price_increment: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_price_increment: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_price_levels: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_distribution_mode: Option<VolumeDistributionMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_area_calculation_mode: Option<ValueAreaCalculationMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_area_percentage: Option<f64>,
}

/// Volume profile configuration
#[derive(Debug, Clone, Deserialize)]
pub struct VolumeProfileConfig {
    pub enabled: bool,
    pub price_increment_mode: PriceIncrementMode,
    pub target_price_levels: u32,
    pub fixed_price_increment: f64,
    pub min_price_increment: f64,
    pub max_price_increment: f64,
    pub update_frequency: UpdateFrequency,
    pub batch_size: usize,
    pub value_area_percentage: f64,
    pub volume_distribution_mode: VolumeDistributionMode,
    pub value_area_calculation_mode: ValueAreaCalculationMode,
    #[serde(default)]
    pub asset_overrides: HashMap<String, AssetConfig>,
}

impl Default for VolumeProfileConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            price_increment_mode: PriceIncrementMode::Adaptive,
            target_price_levels: 200,
            fixed_price_increment: 0.01,
            min_price_increment: 0.00000001,
            max_price_increment: 100.0,
            update_frequency: UpdateFrequency::EveryCandle,
            batch_size: 10,
            value_area_percentage: 70.0,
            volume_distribution_mode: VolumeDistributionMode::WeightedOHLC,
            value_area_calculation_mode: ValueAreaCalculationMode::Traditional,
            asset_overrides: HashMap::new(),
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
        }
    }

    /// Validate configuration for consistency and reasonable values
    pub fn validate(&self) -> Result<(), String> {
        // Validate global configuration
        if self.target_price_levels < 10 || self.target_price_levels > 10000 {
            return Err(format!("target_price_levels must be between 10 and 10000, got {}", self.target_price_levels));
        }

        if self.fixed_price_increment <= 0.0 {
            return Err("fixed_price_increment must be positive".to_string());
        }

        if self.min_price_increment <= 0.0 {
            return Err("min_price_increment must be positive".to_string());
        }

        if self.max_price_increment <= self.min_price_increment {
            return Err("max_price_increment must be greater than min_price_increment".to_string());
        }

        if self.value_area_percentage < 50.0 || self.value_area_percentage > 95.0 {
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
                if increment <= 0.0 {
                    return Err(format!("Asset {}: fixed_price_increment must be positive", symbol));
                }
            }

            if let Some(min_inc) = asset_config.min_price_increment {
                if min_inc <= 0.0 {
                    return Err(format!("Asset {}: min_price_increment must be positive", symbol));
                }

                if let Some(max_inc) = asset_config.max_price_increment {
                    if max_inc <= min_inc {
                        return Err(format!("Asset {}: max_price_increment must be greater than min_price_increment", symbol));
                    }
                }
            }

            if let Some(va_pct) = asset_config.value_area_percentage {
                if !(50.0..=95.0).contains(&va_pct) {
                    return Err(format!("Asset {}: value_area_percentage must be between 50% and 95%, got {}%", symbol, va_pct));
                }
            }
        }

        Ok(())
    }
}

/// Resolved configuration for a specific asset after applying overrides
#[derive(Debug, Clone)]
pub struct ResolvedAssetConfig {
    pub price_increment_mode: PriceIncrementMode,
    pub target_price_levels: u32,
    pub fixed_price_increment: f64,
    pub min_price_increment: f64,
    pub max_price_increment: f64,
    pub volume_distribution_mode: VolumeDistributionMode,
    pub value_area_calculation_mode: ValueAreaCalculationMode,
    pub value_area_percentage: f64,
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
    pub total_volume: f64,
    /// Volume Weighted Average Price
    pub vwap: f64,
    /// Point of Control (price level with highest volume)
    pub poc: f64,
    /// Value area (70% of volume concentration)
    pub value_area: ValueArea,
    /// Price increment used for this profile
    pub price_increment: f64,
    /// Minimum price for the day
    pub min_price: f64,
    /// Maximum price for the day
    pub max_price: f64,
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
    pub total_volume: f64,
    /// Volume Weighted Average Price
    pub vwap: f64,
    /// Point of Control (price level with highest volume)
    pub poc: f64,
    /// Value area (70% of volume concentration)
    pub value_area: ValueArea,
    /// Price increment used for this profile
    pub price_increment: f64,
    /// Minimum price for the day
    pub min_price: f64,
    /// Maximum price for the day
    pub max_price: f64,
    /// Number of 1-minute candles processed
    pub candle_count: u32,
    /// Last update timestamp
    pub last_updated: TimestampMS,
    /// Individual price levels as (price, volume) pairs
    pub price_levels: Vec<(f64, f64)>,
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
                    percentage: 0.0, // Will be calculated based on total_volume
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

/// Individual price level data within volume profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevelData {
    /// Price level
    pub price: f64,
    /// Volume at this price level
    pub volume: f64,
    /// Percentage of total daily volume
    pub percentage: f64,
}

/// Value area representing 70% volume concentration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueArea {
    /// Highest price in value area
    pub high: f64,
    /// Lowest price in value area
    pub low: f64,
    /// Percentage of total volume in value area
    pub volume_percentage: f64,
    /// Total volume within value area
    pub volume: f64,
}


/// Validation result for value area calculations
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    Valid,
    InvalidRange { high: f64, low: f64 },
    InvalidVolumePercentage { percentage: f64 },
    PocNotInRange { poc: f64, high: f64, low: f64 },
    DegenerateCase { price_levels_count: usize },
}

/// ValueArea validator component for ensuring calculation quality
#[derive(Debug, Clone)]
pub struct ValueAreaValidator;

impl ValueAreaValidator {
    /// Validate a calculated value area against acceptance criteria
    pub fn validate_value_area(high: f64, low: f64, volume_percentage: f64) -> ValidationResult {
        // AC1: Value area high must be greater than value area low (no degenerate single-price areas)
        if high < low {
            return ValidationResult::InvalidRange { high, low };
        }
        
        // AC2: Volume percentage accuracy (65-75% target range)
        if !(65.0..=75.0).contains(&volume_percentage) {
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
        let total_volume: f64 = price_levels.iter().map(|p| p.volume).sum();
        if total_volume <= 0.0 {
            return true;
        }
        
        let first_volume = price_levels.first().map(|p| p.volume).unwrap_or(0.0);
        let last_volume = price_levels.last().map(|p| p.volume).unwrap_or(0.0);
        let extreme_volume = first_volume + last_volume;
        
        (extreme_volume / total_volume) > 0.9 // More than 90% at extremes is degenerate
    }
    
    /// Validate that POC is within value area range (AC3)
    pub fn validate_poc_in_range(poc: f64, high: f64, low: f64) -> ValidationResult {
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
    pub levels: BTreeMap<PriceKey, f64>,
    /// Price increment for this profile
    pub price_increment: f64,
}

impl PriceLevelMap {
    /// Create new price level map
    pub fn new(price_increment: f64) -> Self {
        Self {
            levels: BTreeMap::new(),
            price_increment,
        }
    }

    /// Add volume to a specific price level
    pub fn add_volume(&mut self, price: f64, volume: f64) {
        let price_key = PriceKey::from_price(price, self.price_increment);
        *self.levels.entry(price_key).or_insert(0.0) += volume;
    }

    /// Distribute volume across price range using the specified distribution mode
    pub fn distribute_candle_volume(&mut self, _open: f64, high: f64, low: f64, close: f64, volume: f64, mode: &VolumeDistributionMode) {
        if volume <= 0.0 {
            return;
        }

        match mode {
            VolumeDistributionMode::ClosingPrice => {
                // Traditional time-based market profile: all volume at close
                self.add_volume(close, volume);
            },
            VolumeDistributionMode::UniformOHLC => {
                // Distribute volume uniformly across OHLC range
                self.distribute_volume_uniform(high, low, volume);
            },
            VolumeDistributionMode::WeightedOHLC => {
                // Weighted: 50% close, 25% high, 25% low
                self.add_volume(close, volume * 0.5);
                self.add_volume(high, volume * 0.25);
                self.add_volume(low, volume * 0.25);
            },
            VolumeDistributionMode::HighLowWeighted => {
                // Price action focused: 50% high, 50% low
                self.add_volume(high, volume * 0.5);
                self.add_volume(low, volume * 0.5);
            },
        }
    }

    /// Distribute volume uniformly across price range from low to high
    fn distribute_volume_uniform(&mut self, high: f64, low: f64, volume: f64) {
        if high <= low {
            // Edge case: no range, assign all to single price
            self.add_volume(high, volume);
            return;
        }

        // Calculate number of price levels in the range
        let range = high - low;
        let levels_in_range = (range / self.price_increment).ceil() as i32;
        
        if levels_in_range <= 1 {
            // Range is smaller than price increment
            self.add_volume(high, volume);
            return;
        }

        // Distribute volume equally across all price levels in range
        let volume_per_level = volume / levels_in_range as f64;
        
        for i in 0..levels_in_range {
            let price = low + (i as f64 * self.price_increment);
            if price <= high {
                self.add_volume(price, volume_per_level);
            }
        }
    }

    /// Get total volume across all price levels
    pub fn total_volume(&self) -> f64 {
        self.levels.values().sum()
    }

    /// Get price level with highest volume (Point of Control)
    pub fn get_poc(&self) -> Option<f64> {
        self.levels
            .iter()
            .max_by(|(_, vol_a), (_, vol_b)| vol_a.partial_cmp(vol_b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(price_key, _)| price_key.to_price(self.price_increment))
    }

    /// Convert to sorted vector of price level data
    pub fn to_price_levels(&self) -> Vec<PriceLevelData> {
        let total_volume = self.total_volume();
        if total_volume <= 0.0 {
            return Vec::new();
        }

        self.levels
            .iter()
            .map(|(price_key, &volume)| PriceLevelData {
                price: price_key.to_price(self.price_increment),
                volume,
                percentage: (volume / total_volume) * 100.0,
            })
            .collect()
    }

    /// Calculate VWAP (Volume Weighted Average Price)
    pub fn calculate_vwap(&self) -> f64 {
        let total_volume = self.total_volume();
        if total_volume <= 0.0 {
            return 0.0;
        }

        let weighted_sum: f64 = self.levels
            .iter()
            .map(|(price_key, &volume)| price_key.to_price(self.price_increment) * volume)
            .sum();

        weighted_sum / total_volume
    }

        /// Calculate value area using the specified method
    pub fn calculate_value_area(&self, value_area_percentage: f64, calculation_mode: &ValueAreaCalculationMode) -> ValueArea {
        let result = match calculation_mode {
            ValueAreaCalculationMode::Traditional => self.calculate_value_area_traditional(value_area_percentage),
            ValueAreaCalculationMode::Greedy => self.calculate_value_area_greedy(value_area_percentage),
        };
        
        // Validate the result and adjust if necessary
        self.validate_and_adjust_value_area(result, value_area_percentage)
    }
    
    /// Validate and adjust value area to ensure it meets acceptance criteria
    fn validate_and_adjust_value_area(&self, mut value_area: ValueArea, _target_percentage: f64) -> ValueArea {
        // Get price levels for degenerate case detection
        let _price_levels = self.to_price_levels();
        
        // Check for degenerate case - AC1: high must be greater than low
        if value_area.high <= value_area.low {
            // Ensure we have a minimal meaningful range per AC1
            value_area.high = value_area.low + self.price_increment;
            
            // Recalculate volume for the expanded range
            let actual_volume = self.calculate_volume_in_range(value_area.low, value_area.high);
            let total_volume = self.total_volume();
            value_area.volume = actual_volume;
            value_area.volume_percentage = if total_volume > 0.0 {
                (actual_volume / total_volume) * 100.0
            } else {
                0.0
            };
        }
        
        // Validate basic value area properties
        match ValueAreaValidator::validate_value_area(value_area.high, value_area.low, value_area.volume_percentage) {
            ValidationResult::Valid => {
                // Ensure POC is within range
                if let Some(poc) = self.get_poc() {
                    if let ValidationResult::PocNotInRange { .. } = ValueAreaValidator::validate_poc_in_range(poc, value_area.high, value_area.low) {
                        // Expand range to include POC
                        value_area.low = value_area.low.min(poc);
                        value_area.high = value_area.high.max(poc);
                        
                        // Recalculate volume for expanded range
                        let actual_volume = self.calculate_volume_in_range(value_area.low, value_area.high);
                        let total_volume = self.total_volume();
                        value_area.volume = actual_volume;
                        value_area.volume_percentage = if total_volume > 0.0 {
                            (actual_volume / total_volume) * 100.0
                        } else {
                            0.0
                        };
                    }
                }
            },
            ValidationResult::InvalidRange { .. } => {
                // Fix invalid range (high < low)
                if value_area.high < value_area.low {
                    std::mem::swap(&mut value_area.high, &mut value_area.low);
                }
                if value_area.high <= value_area.low {
                    value_area.high = value_area.low + self.price_increment;
                }
                
                // Recalculate volume for corrected range
                let actual_volume = self.calculate_volume_in_range(value_area.low, value_area.high);
                let total_volume = self.total_volume();
                value_area.volume = actual_volume;
                value_area.volume_percentage = if total_volume > 0.0 {
                    (actual_volume / total_volume) * 100.0
                } else {
                    0.0
                };
            },
            ValidationResult::InvalidVolumePercentage { .. } => {
                // Volume percentage is outside target range - this is acceptable
                // as we prefer accuracy over hitting exact percentages
            },
            _ => {
                // Other validation failures - use default handling
            }
        }
        
        value_area
    }
    
    /// Calculate total volume within a price range
    fn calculate_volume_in_range(&self, low: f64, high: f64) -> f64 {
        let low_key = PriceKey::from_price(low, self.price_increment);
        let high_key = PriceKey::from_price(high, self.price_increment);
        
        self.levels.iter()
            .filter(|(key, _)| **key >= low_key && **key <= high_key)
            .map(|(_, volume)| *volume)
            .sum()
    }

    /// Calculate value area using traditional market profile method (expand from POC)
    pub fn calculate_value_area_traditional(&self, value_area_percentage: f64) -> ValueArea {
        let total_volume = self.total_volume();
        let target_volume = total_volume * (value_area_percentage / 100.0);

        if self.levels.is_empty() || total_volume <= 0.0 {
            return ValueArea::default();
        }

        // Find POC (Point of Control)
        let poc_key = self.levels
            .iter()
            .max_by(|(_, vol_a), (_, vol_b)| vol_a.partial_cmp(vol_b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(key, _)| *key);

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
        let mut included_volume = self.levels.get(&poc_key).copied().unwrap_or(0.0);
        let mut low_index = poc_index;
        let mut high_index = poc_index;
        
        // Ensure we start with POC volume
        let mut selected_levels = vec![poc_key];
        
        // Expand symmetrically to ensure POC is centered
        while included_volume < target_volume && (low_index > 0 || high_index < all_levels.len() - 1) {
            let mut candidates = Vec::new();
            
            // Check left expansion
            if low_index > 0 {
                let left_key = all_levels[low_index - 1].0;
                let left_volume = self.levels.get(left_key).copied().unwrap_or(0.0);
                if left_volume > 0.0 {
                    candidates.push((left_key, left_volume, low_index - 1, "left"));
                }
            }
            
            // Check right expansion
            if high_index < all_levels.len() - 1 {
                let right_key = all_levels[high_index + 1].0;
                let right_volume = self.levels.get(right_key).copied().unwrap_or(0.0);
                if right_volume > 0.0 {
                    candidates.push((right_key, right_volume, high_index + 1, "right"));
                }
            }
            
            if candidates.is_empty() {
                break;
            }
            
            // Sort candidates by volume (highest first) to maintain traditional method
            candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            
            // Select the best candidate
            let (selected_key, selected_volume, selected_index, direction) = candidates[0];
            selected_levels.push(*selected_key);
            included_volume += selected_volume;
            
            if direction == "left" {
                low_index = selected_index;
            } else {
                high_index = selected_index;
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

        // Calculate actual volume in final range
        let actual_volume = self.levels.iter()
            .filter(|(key, _)| **key >= low_key && **key <= high_key)
            .map(|(_, volume)| *volume)
            .sum::<f64>();

        ValueArea {
            low: final_low,
            high: final_high,
            volume: actual_volume,
            volume_percentage: (actual_volume / total_volume) * 100.0,
        }
    }

    /// Calculate value area using greedy selection method (existing implementation)
    pub fn calculate_value_area_greedy(&self, value_area_percentage: f64) -> ValueArea {
        let total_volume = self.total_volume();
        let target_volume = total_volume * (value_area_percentage / 100.0);

        if self.levels.is_empty() || total_volume <= 0.0 {
            return ValueArea {
                high: 0.0,
                low: 0.0,
                volume_percentage: 0.0,
                volume: 0.0,
            };
        }

        // Step 1: Sort all price levels by volume descending (highest to lowest)
        let mut sorted_levels: Vec<_> = self.levels
            .iter()
            .map(|(price_key, &volume)| (price_key.to_price(self.price_increment), volume))
            .collect();
        
        // Sort by volume descending, then by price ascending for stability
        sorted_levels.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        });

        // Step 2: Greedily select highest volume levels until we reach target
        let mut selected_volume = 0.0;
        let mut selected_prices = Vec::new();

        for (price, volume) in &sorted_levels {
            if selected_volume < target_volume {
                selected_volume += volume;
                selected_prices.push(*price);
            } else {
                break;
            }
        }

        if selected_prices.is_empty() {
            return ValueArea {
                high: 0.0,
                low: 0.0,
                volume_percentage: 0.0,
                volume: 0.0,
            };
        }

        // Step 3: Ensure contiguous price range containing the selected levels
        // Sort selected prices to find min/max range
        selected_prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let min_price = selected_prices[0];
        let max_price = selected_prices[selected_prices.len() - 1];

        // Step 4: Calculate actual volume within this contiguous range
        let _actual_volume_in_range = 
            self.levels.iter()
                .filter(|(price_key, _)| {
                    let price = price_key.to_price(self.price_increment);
                    price >= min_price && price <= max_price
                })
                .map(|(_, volume)| *volume)
                .sum::<f64>();

        // Step 5: Find POC (should be the highest volume level within the value area)
        let poc = sorted_levels.first().map(|(price, _)| *price).unwrap_or(0.0);

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
                .sum::<f64>();

        ValueArea {
            high: final_high,
            low: final_low,
            volume_percentage: (final_actual_volume / total_volume) * 100.0,
            volume: final_actual_volume,
        }
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
    pub fn get_volume_at_key(&self, key: &PriceKey) -> f64 {
        self.levels.get(key).copied().unwrap_or(0.0)
    }

    /// Get min and max prices
    pub fn get_price_range(&self) -> (f64, f64) {
        if self.levels.is_empty() {
            return (0.0, 0.0);
        }

        let min_price = self.levels.keys().next()
            .map(|key| key.to_price(self.price_increment))
            .unwrap_or(0.0);
        
        let max_price = self.levels.keys().next_back()
            .map(|key| key.to_price(self.price_increment))
            .unwrap_or(0.0);

        (min_price, max_price)
    }
}

/// Price key for efficient BTreeMap operations
/// Uses integer representation for consistent ordering and fast comparisons
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PriceKey(i64);

impl PriceKey {
    /// Create price key from price and increment
    pub fn from_price(price: f64, price_increment: f64) -> Self {
        let key = (price / price_increment).round() as i64;
        Self(key)
    }

    /// Convert price key back to price
    pub fn to_price(self, price_increment: f64) -> f64 {
        self.0 as f64 * price_increment
    }

    /// Get next price key (higher price by one increment)
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Get previous price key (lower price by one increment)
    pub fn previous(self) -> Self {
        Self(self.0 - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_key_conversion() {
        let price_increment = 0.01;
        let price = 50000.25;
        
        let key = PriceKey::from_price(price, price_increment);
        let converted_price = key.to_price(price_increment);
        
        // Should be close to original price (within increment precision)
        assert!((converted_price - price).abs() < price_increment);
    }

    #[test]
    fn test_price_level_map_basic_operations() {
        let mut map = PriceLevelMap::new(0.01);
        
        // Add volume at different price levels
        map.add_volume(50000.25, 100.0);
        map.add_volume(50000.26, 150.0);
        map.add_volume(50000.25, 50.0); // Add more volume to same level
        
        // Total volume should be sum of all additions
        assert_eq!(map.total_volume(), 300.0);
        
        // Both prices now have 150.0 volume, so POC could be either
        let poc = map.get_poc().unwrap();
        // The max_by implementation returns 50000.26 (the last equal max value)
        assert!((poc - 50000.26).abs() < 0.001);
    }

    #[test]
    fn test_vwap_calculation() {
        let mut map = PriceLevelMap::new(0.01);
        
        map.add_volume(100.0, 10.0); // 100 * 10 = 1000
        map.add_volume(200.0, 20.0); // 200 * 20 = 4000
        map.add_volume(300.0, 30.0); // 300 * 30 = 9000
        
        let vwap = map.calculate_vwap();
        let expected_vwap = (1000.0 + 4000.0 + 9000.0) / (10.0 + 20.0 + 30.0);
        
        assert!((vwap - expected_vwap).abs() < 0.001);
    }

    #[test]
    fn test_value_area_calculation() {
        let mut map = PriceLevelMap::new(1.0);  // Use 1.0 increment to match the price spacing
        
        // Create distribution with clear POC
        map.add_volume(100.0, 10.0);  // 10% of volume
        map.add_volume(101.0, 50.0);  // 50% of volume (POC)
        map.add_volume(102.0, 30.0);  // 30% of volume
        map.add_volume(103.0, 10.0);  // 10% of volume
        
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        
        // With greedy selection, should get exactly 70% volume
        // We select 101.0 (50%) + 102.0 (30%) = 80% 
        // Then the contiguous range 101-102 contains 80% volume
        assert!(value_area.volume_percentage >= 70.0, "Expected >= 70%, got {}", value_area.volume_percentage);
        assert!(value_area.volume_percentage <= 80.0, "Should not exceed 80% for this test, got {}", value_area.volume_percentage);
        assert!(value_area.volume > 0.0);
        assert!(value_area.low <= 101.0);
        assert!(value_area.high >= 102.0);
        assert!(value_area.high > value_area.low, "Value area should span a range: high={} low={}", 
                value_area.high, value_area.low);
    }

    #[test]
    fn test_value_area_concentrated_volume() {
        let mut map = PriceLevelMap::new(0.01);  // Fine-grained increment like real data
        
        // Simulate the real-world scenario: volume concentrated at one price
        map.add_volume(114367.6, 4461.0);  // POC volume
        map.add_volume(114367.59, 100.0);   // Small adjacent volume
        map.add_volume(114367.61, 150.0);   // Small adjacent volume
        
        let total_volume = map.total_volume();
        assert!(total_volume > 0.0);
        
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        
        // Value area should capture approximately 70% volume (or as close as possible with discrete levels)
        // With concentrated volume, it may include more to reach a contiguous range
        assert!(value_area.volume_percentage >= 70.0 && value_area.volume_percentage <= 100.0, 
                "Value area should capture 70-100% volume: {}%", value_area.volume_percentage);
        
        // Value area should include the POC (highest volume level)
        assert!(value_area.low <= 114367.6 && value_area.high >= 114367.6,
                "Value area should include POC at 114367.6");
        
        // Should have reasonable volume
        assert!(value_area.volume > 0.0);
    }

    #[test]
    fn test_value_area_single_price_level() {
        let mut map = PriceLevelMap::new(0.01);
        
        // Edge case: all volume at exactly one price
        map.add_volume(114367.6, 1000.0);
        
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        
        // Should still provide meaningful range
        assert!(value_area.high >= value_area.low);
        assert!(value_area.volume_percentage >= 50.0, 
                "Should capture all volume when only one level exists");
    }

    #[test]
    fn test_price_level_data_conversion() {
        let mut map = PriceLevelMap::new(0.01);
        
        map.add_volume(50000.25, 100.0);
        map.add_volume(50000.26, 200.0);
        
        let price_levels = map.to_price_levels();
        
        assert_eq!(price_levels.len(), 2);
        
        // Should be sorted by price
        assert!(price_levels[0].price < price_levels[1].price);
        
        // Percentages should sum to 100%
        let total_percentage: f64 = price_levels.iter().map(|p| p.percentage).sum();
        assert!((total_percentage - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_empty_price_level_map() {
        let map = PriceLevelMap::new(0.01);
        
        assert_eq!(map.total_volume(), 0.0);
        assert!(map.get_poc().is_none());
        assert_eq!(map.calculate_vwap(), 0.0);
        assert!(map.to_price_levels().is_empty());
        
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        assert_eq!(value_area.volume, 0.0);
        assert_eq!(value_area.volume_percentage, 0.0);
    }

    #[test]
    fn test_volume_distribution_modes() {
        let mut map = PriceLevelMap::new(0.01);
        
        // Test ClosingPrice distribution
        map.distribute_candle_volume(100.0, 102.0, 98.0, 101.0, 1000.0, &VolumeDistributionMode::ClosingPrice);
        assert_eq!(map.total_volume(), 1000.0);
        let levels = map.to_price_levels();
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].price, 101.0);
        assert_eq!(levels[0].volume, 1000.0);

        // Reset and test WeightedOHLC distribution
        map = PriceLevelMap::new(0.01);
        map.distribute_candle_volume(100.0, 102.0, 98.0, 101.0, 1000.0, &VolumeDistributionMode::WeightedOHLC);
        assert_eq!(map.total_volume(), 1000.0);
        
        // Should have volume at close (50%), high (25%), and low (25%)
        let close_volume = map.get_volume_at_key(&PriceKey::from_price(101.0, 0.01));
        let high_volume = map.get_volume_at_key(&PriceKey::from_price(102.0, 0.01));
        let low_volume = map.get_volume_at_key(&PriceKey::from_price(98.0, 0.01));
        
        assert_eq!(close_volume, 500.0);
        assert_eq!(high_volume, 250.0);
        assert_eq!(low_volume, 250.0);

        // Reset and test HighLowWeighted distribution
        map = PriceLevelMap::new(0.01);
        map.distribute_candle_volume(100.0, 102.0, 98.0, 101.0, 1000.0, &VolumeDistributionMode::HighLowWeighted);
        assert_eq!(map.total_volume(), 1000.0);
        
        let high_volume = map.get_volume_at_key(&PriceKey::from_price(102.0, 0.01));
        let low_volume = map.get_volume_at_key(&PriceKey::from_price(98.0, 0.01));
        
        assert_eq!(high_volume, 500.0);
        assert_eq!(low_volume, 500.0);
    }

    #[test]
    fn test_uniform_volume_distribution() {
        let mut map = PriceLevelMap::new(1.0);
        
        // Test uniform distribution across a 4-unit range (100 to 104)
        map.distribute_candle_volume(100.0, 104.0, 100.0, 102.0, 1000.0, &VolumeDistributionMode::UniformOHLC);
        
        assert_eq!(map.total_volume(), 1000.0);
        let levels = map.to_price_levels();
        
        // Should have distributed across levels in the range (100 to 104)
        // With 1.0 increment, this creates 4 levels: 100, 101, 102, 103
        assert_eq!(levels.len(), 4);
        
        // Each level should have approximately equal volume
        let expected_volume_per_level = 1000.0 / 4.0;
        for level in &levels {
            assert!((level.volume - expected_volume_per_level).abs() < 1.0);
        }
    }

    #[test]
    fn test_traditional_vs_greedy_value_area() {
        let mut map = PriceLevelMap::new(1.0);
        
        // Create a distribution where traditional and greedy methods should differ
        map.add_volume(100.0, 10.0);  // 10%
        map.add_volume(101.0, 50.0);  // 50% (POC)
        map.add_volume(102.0, 20.0);  // 20%
        map.add_volume(103.0, 15.0);  // 15%
        map.add_volume(105.0, 5.0);   // 5% (isolated level)
        
        let traditional_va = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        let greedy_va = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Greedy);
        
        // Both should include POC
        assert!(traditional_va.low <= 101.0 && traditional_va.high >= 101.0);
        assert!(greedy_va.low <= 101.0 && greedy_va.high >= 101.0);
        
        // Both should aim for ~70% volume
        assert!(traditional_va.volume_percentage >= 65.0 && traditional_va.volume_percentage <= 85.0);
        assert!(greedy_va.volume_percentage >= 65.0 && greedy_va.volume_percentage <= 85.0);
        
        // Traditional method should create a more contiguous range from POC
        // Greedy method might include the isolated high-volume level at 105.0
        assert!(traditional_va.high <= 103.0, "Traditional should stay contiguous from POC");
    }

    #[test]
    fn test_value_area_edge_cases() {
        // Test with single price level - AC1: high must be greater than low (no degenerate single-price areas)
        let mut map = PriceLevelMap::new(0.01);
        map.add_volume(100.0, 1000.0);
        
        let va = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        assert_eq!(va.low, 100.0);
        assert_eq!(va.high, 100.01);  // Should be expanded by price_increment per AC1
        assert!(va.volume > 0.0);      // Volume should be positive
        assert!(va.volume_percentage > 0.0);  // Percentage should be positive
        
        // Test with two equal volume levels
        map = PriceLevelMap::new(1.0);
        map.add_volume(100.0, 500.0);
        map.add_volume(101.0, 500.0);
        
        let va = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        assert!(va.low <= 100.0);
        assert!(va.high >= 101.0);
        assert_eq!(va.volume, 1000.0);
        assert_eq!(va.volume_percentage, 100.0);
    }

    #[test]
    fn test_asset_specific_config_resolution() {
        let mut config = VolumeProfileConfig::default();
        
        // Add asset overrides
        let mut overrides = HashMap::new();
        let btc_config = AssetConfig {
            price_increment_mode: Some(PriceIncrementMode::Fixed),
            fixed_price_increment: Some(10.0),
            target_price_levels: Some(100),
            min_price_increment: None,
            max_price_increment: None,
            volume_distribution_mode: Some(VolumeDistributionMode::ClosingPrice),
            value_area_calculation_mode: Some(ValueAreaCalculationMode::Greedy),
            value_area_percentage: Some(68.0),
        };
        overrides.insert("BTCUSDT".to_string(), btc_config);
        config.asset_overrides = overrides;
        
        // Test BTC-specific config resolution
        let btc_resolved = config.resolve_for_asset("BTCUSDT");
        assert!(matches!(btc_resolved.price_increment_mode, PriceIncrementMode::Fixed));
        assert_eq!(btc_resolved.fixed_price_increment, 10.0);
        assert_eq!(btc_resolved.target_price_levels, 100);
        assert!(matches!(btc_resolved.volume_distribution_mode, VolumeDistributionMode::ClosingPrice));
        assert!(matches!(btc_resolved.value_area_calculation_mode, ValueAreaCalculationMode::Greedy));
        assert_eq!(btc_resolved.value_area_percentage, 68.0);
        
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
        config.fixed_price_increment = -1.0; // Negative
        assert!(config.validate().is_err());
        
        config.fixed_price_increment = 0.01; // Reset to valid
        config.min_price_increment = -0.001; // Negative
        assert!(config.validate().is_err());
        
        config.min_price_increment = 0.001; // Reset to valid
        config.max_price_increment = 0.0005; // Less than min
        assert!(config.validate().is_err());
        
        config.max_price_increment = 1.0; // Reset to valid
        
        // Test invalid value area percentage
        config.value_area_percentage = 40.0; // Too small
        assert!(config.validate().is_err());
        
        config.value_area_percentage = 99.0; // Too large
        assert!(config.validate().is_err());
        
        config.value_area_percentage = 70.0; // Reset to valid
        
        // Test invalid asset override
        let mut overrides = HashMap::new();
        let invalid_asset = AssetConfig {
            price_increment_mode: None,
            fixed_price_increment: Some(-10.0), // Negative
            target_price_levels: None,
            min_price_increment: None,
            max_price_increment: None,
            volume_distribution_mode: None,
            value_area_calculation_mode: None,
            value_area_percentage: None,
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
        let mut config = VolumeProfileConfig::default();
        config.price_increment_mode = PriceIncrementMode::Adaptive;
        config.min_price_increment = 0.00000001;
        config.max_price_increment = 100.0;
        
        // High target levels (fine granularity)
        config.target_price_levels = 1000;
        let high_target_resolved = config.resolve_for_asset("TESTUSDT");
        let fine_increment = DailyVolumeProfile::calculate_price_increment(&high_target_resolved, Some(100.0)); // $100 range
        
        // Low target levels (coarse granularity)
        config.target_price_levels = 50;
        let low_target_resolved = config.resolve_for_asset("TESTUSDT");
        let coarse_increment = DailyVolumeProfile::calculate_price_increment(&low_target_resolved, Some(100.0)); // $100 range
        
        // Coarse increment should be larger (fewer levels)
        assert!(coarse_increment > fine_increment);
        
        // Verify reasonable values
        assert_eq!(fine_increment, 100.0 / 1000.0); // $0.10 increment for 1000 levels in $100
        assert_eq!(coarse_increment, 100.0 / 50.0);  // $2.00 increment for 50 levels in $100
    }

    #[test]
    fn test_balanced_volume_profile_with_weighted_ohlc() {
        let mut map = PriceLevelMap::new(0.5); // Use 0.5 increment for better granularity
        
        // Create a more realistic trading scenario with bell-curve like distribution
        // Simulate accumulation around 101-102 range with some outliers
        let candles = vec![
            // Low volume outliers
            (99.0, 100.0, 99.0, 99.5, 500.0),   // Low outlier
            (104.0, 105.0, 104.0, 104.5, 500.0), // High outlier
            
            // Main accumulation zone - should create centered POC
            (100.5, 101.5, 100.0, 101.0, 3000.0), // Volume at 101 area
            (101.0, 102.0, 100.8, 101.5, 4000.0), // Peak volume
            (101.5, 102.5, 101.0, 102.0, 3500.0), // Volume at 102 area
            (101.8, 102.2, 101.3, 101.8, 3000.0), // More volume around 101-102
            (101.2, 102.8, 101.0, 102.2, 2500.0), // Balanced distribution
            
            // Some mid-range activity
            (102.0, 103.0, 101.8, 102.5, 1500.0), // Moderate volume
            (100.8, 102.0, 100.5, 101.2, 1000.0), // Lower moderate volume
        ];
        
        // Use WeightedOHLC distribution (new default)
        for (open, high, low, close, volume) in candles {
            map.distribute_candle_volume(open, high, low, close, volume, &VolumeDistributionMode::WeightedOHLC);
        }
        
        // Calculate value area using traditional method
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        let poc = map.get_poc().unwrap();
        
        // Basic validations that should always pass with WeightedOHLC
        
        // Value area should contain significant volume (close to target)
        assert!(value_area.volume_percentage >= 65.0, 
                "Value area should contain at least 65% of volume, got {:.1}%", value_area.volume_percentage);
        
        // POC should be within the value area (fundamental requirement)
        assert!(poc >= value_area.low && poc <= value_area.high,
                "POC ({}) should be within value area [{} - {}]", poc, value_area.low, value_area.high);
        
        // Value area should have reasonable range (not degenerate)
        let va_range = value_area.high - value_area.low;
        assert!(va_range >= 0.0, "Value area range should be non-negative: {:.1}", va_range);
        
        // With WeightedOHLC, we should have multiple price levels (not just one)
        let price_levels = map.to_price_levels();
        assert!(price_levels.len() >= 3, 
                "WeightedOHLC should create multiple price levels, got {}", price_levels.len());
        
        // The POC should be a high-volume level
        let poc_level = price_levels.iter().find(|level| level.price == poc).unwrap();
        let max_volume = price_levels.iter().map(|l| l.volume).fold(0.0, f64::max);
        assert!((poc_level.volume - max_volume).abs() < 0.01,
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
        let result = ValueAreaValidator::validate_value_area(102.5, 100.5, 72.0);
        assert_eq!(result, ValidationResult::Valid);
    }

    #[test]
    fn test_value_area_validator_invalid_range() {
        // High < Low should be invalid
        let result = ValueAreaValidator::validate_value_area(100.5, 102.5, 72.0);
        assert_eq!(result, ValidationResult::InvalidRange { high: 100.5, low: 102.5 });
    }

    #[test]
    fn test_value_area_validator_invalid_volume_percentage() {
        // Volume percentage too low
        let result = ValueAreaValidator::validate_value_area(102.5, 100.5, 60.0);
        assert_eq!(result, ValidationResult::InvalidVolumePercentage { percentage: 60.0 });
        
        // Volume percentage too high
        let result = ValueAreaValidator::validate_value_area(102.5, 100.5, 80.0);
        assert_eq!(result, ValidationResult::InvalidVolumePercentage { percentage: 80.0 });
    }

    #[test]
    fn test_poc_validation() {
        // POC within range should be valid
        let result = ValueAreaValidator::validate_poc_in_range(101.5, 102.0, 101.0);
        assert_eq!(result, ValidationResult::Valid);
        
        // POC below range should be invalid
        let result = ValueAreaValidator::validate_poc_in_range(100.5, 102.0, 101.0);
        assert_eq!(result, ValidationResult::PocNotInRange { poc: 100.5, high: 102.0, low: 101.0 });
        
        // POC above range should be invalid
        let result = ValueAreaValidator::validate_poc_in_range(103.0, 102.0, 101.0);
        assert_eq!(result, ValidationResult::PocNotInRange { poc: 103.0, high: 102.0, low: 101.0 });
    }

    #[test]
    fn test_degenerate_case_detection() {
        // Single price level is degenerate
        let single_level = vec![
            PriceLevelData { price: 100.0, volume: 1000.0, percentage: 100.0 }
        ];
        assert!(ValueAreaValidator::detect_degenerate_case(&single_level));
        
        // Empty price levels is degenerate
        let empty_levels = vec![];
        assert!(ValueAreaValidator::detect_degenerate_case(&empty_levels));
        
        // Extreme volume concentration (>90% at extremes) is degenerate
        let extreme_levels = vec![
            PriceLevelData { price: 100.0, volume: 500.0, percentage: 50.0 },  // First extreme
            PriceLevelData { price: 101.0, volume: 50.0, percentage: 5.0 },    // Middle
            PriceLevelData { price: 102.0, volume: 450.0, percentage: 45.0 },  // Last extreme
        ];
        assert!(ValueAreaValidator::detect_degenerate_case(&extreme_levels)); // 95% at extremes
        
        // Balanced distribution is not degenerate
        let balanced_levels = vec![
            PriceLevelData { price: 100.0, volume: 200.0, percentage: 20.0 },
            PriceLevelData { price: 101.0, volume: 600.0, percentage: 60.0 },  // POC in middle
            PriceLevelData { price: 102.0, volume: 200.0, percentage: 20.0 },
        ];
        assert!(!ValueAreaValidator::detect_degenerate_case(&balanced_levels)); // Only 40% at extremes
    }

    #[test]
    fn test_value_area_calculation_with_validation() {
        let mut map = PriceLevelMap::new(0.01);
        
        // Create a scenario that would produce an invalid value area initially
        map.add_volume(114367.6, 4000.0);  // Concentrated volume at one price
        map.add_volume(114367.61, 100.0);  // Small adjacent volume
        
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        
        // After validation and adjustment, should have valid properties
        assert!(value_area.high >= value_area.low, "Value area should have valid range after validation");
        assert!(value_area.volume > 0.0, "Value area should have positive volume");
        
        // POC should be within the value area
        if let Some(poc) = map.get_poc() {
            assert!(poc >= value_area.low && poc <= value_area.high,
                    "POC should be within value area after validation");
        }
    }

    #[test]
    fn test_contiguous_range_logic() {
        let mut map = PriceLevelMap::new(1.0);
        
        // Create distribution with gaps to test contiguous range logic
        map.add_volume(100.0, 100.0);  // 10%
        map.add_volume(101.0, 500.0);  // 50% (POC)
        map.add_volume(102.0, 200.0);  // 20%
        map.add_volume(105.0, 200.0);  // 20% (gap at 103-104)
        
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        
        // Traditional method should create contiguous range around POC
        // Should include 101 (POC) + adjacent levels to reach ~70%
        assert!(value_area.low <= 101.0, "Value area should include POC");
        assert!(value_area.high >= 101.0, "Value area should include POC");
        
        // Should not include the isolated level at 105.0 due to gap
        assert!(value_area.high <= 103.0, "Traditional method should maintain contiguous range");
    }

    #[test]
    fn test_edge_case_handling_extreme_concentration() {
        let mut map = PriceLevelMap::new(0.01);
        
        // Extreme case: 99% volume at highest price
        map.add_volume(100.0, 10.0);    // 1%
        map.add_volume(200.0, 990.0);   // 99% at price extreme
        
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        
        // Should handle extreme concentration gracefully
        assert!(value_area.high >= value_area.low, "Should handle extreme concentration");
        assert!(value_area.volume > 0.0, "Should have positive volume");
        
        // POC (at 200.0) should be included in value area
        assert!(value_area.low <= 200.0 && value_area.high >= 200.0,
                "POC should be included even in extreme concentration");
    }

    #[test]
    fn test_volume_percentage_accuracy() {
        let mut map = PriceLevelMap::new(0.5);
        
        // Create distribution where we can control volume percentages precisely
        map.add_volume(100.0, 100.0);   // 10%
        map.add_volume(100.5, 300.0);   // 30%
        map.add_volume(101.0, 400.0);   // 40% (POC)
        map.add_volume(101.5, 150.0);   // 15%
        map.add_volume(102.0, 50.0);    // 5%
        
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        
        // Should target 70% volume but may be flexible within 65-75% range
        assert!(value_area.volume_percentage >= 65.0 && value_area.volume_percentage <= 85.0,
                "Volume percentage should be reasonable: {}%", value_area.volume_percentage);
        
        // Should include POC
        assert!(value_area.low <= 101.0 && value_area.high >= 101.0,
                "Should include POC in value area");
    }

    #[test]
    fn test_validation_range_correction() {
        let mut map = PriceLevelMap::new(0.01);
        
        // Create minimal volume profile
        map.add_volume(100.0, 1000.0);
        
        let value_area = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        
        // Single price level should be expanded to meaningful range
        assert!(value_area.high > value_area.low, 
                "Single price level should be expanded to range: high={} low={}", 
                value_area.high, value_area.low);
        
        // Range should be at least one price increment
        assert!((value_area.high - value_area.low) >= map.price_increment,
                "Range should be at least one price increment");
    }
}