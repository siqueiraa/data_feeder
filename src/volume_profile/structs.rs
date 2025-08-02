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
            volume_distribution_mode: VolumeDistributionMode::UniformOHLC,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeDistributionMode {
    /// Distribute volume uniformly across OHLC range (traditional volume profile)
    UniformOHLC,
    /// Assign all volume to closing price (time-based market profile)
    ClosingPrice,
    /// Weighted distribution: 50% close, 25% high, 25% low
    WeightedOHLC,
    /// High-Low weighted: 50% high, 50% low (price action focus)
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
        match calculation_mode {
            ValueAreaCalculationMode::Traditional => self.calculate_value_area_traditional(value_area_percentage),
            ValueAreaCalculationMode::Greedy => self.calculate_value_area_greedy(value_area_percentage),
        }
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

        // Start from POC and expand alternately up and down
        let mut included_volume = self.levels.get(&poc_key).copied().unwrap_or(0.0);
        let mut low_key = poc_key;
        let mut high_key = poc_key;
        
        while included_volume < target_volume {
            let up_key = high_key.next();
            let down_key = low_key.previous();
            
            let up_volume = self.levels.get(&up_key).copied().unwrap_or(0.0);
            let down_volume = self.levels.get(&down_key).copied().unwrap_or(0.0);
            
            // Expand in direction with higher volume (traditional market profile method)
            if up_volume >= down_volume && up_volume > 0.0 {
                high_key = up_key;
                included_volume += up_volume;
            } else if down_volume > 0.0 {
                low_key = down_key;
                included_volume += down_volume;
            } else {
                // No more levels to expand to
                break;
            }
        }

        let low_price = low_key.to_price(self.price_increment);
        let high_price = high_key.to_price(self.price_increment);

        // Calculate actual volume in final range
        let actual_volume = self.levels.iter()
            .filter(|(key, _)| **key >= low_key && **key <= high_key)
            .map(|(_, volume)| *volume)
            .sum::<f64>();

        ValueArea {
            low: low_price,
            high: high_price,
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
        // Test with single price level
        let mut map = PriceLevelMap::new(0.01);
        map.add_volume(100.0, 1000.0);
        
        let va = map.calculate_value_area(70.0, &ValueAreaCalculationMode::Traditional);
        assert_eq!(va.low, 100.0);
        assert_eq!(va.high, 100.0);
        assert_eq!(va.volume, 1000.0);
        assert_eq!(va.volume_percentage, 100.0);
        
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
}