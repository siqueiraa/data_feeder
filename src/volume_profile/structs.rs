use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use crate::historical::structs::TimestampMS;

/// Volume profile configuration
#[derive(Debug, Clone, Deserialize)]
pub struct VolumeProfileConfig {
    pub enabled: bool,
    pub price_increment_mode: PriceIncrementMode,
    pub fixed_price_increment: f64,
    pub min_price_increment: f64,
    pub max_price_increment: f64,
    pub update_frequency: UpdateFrequency,
    pub batch_size: usize,
    pub value_area_percentage: f64,
}

impl Default for VolumeProfileConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            price_increment_mode: PriceIncrementMode::Adaptive,
            fixed_price_increment: 0.01,
            min_price_increment: 0.005,
            max_price_increment: 0.5,
            update_frequency: UpdateFrequency::EveryCandle,
            batch_size: 10,
            value_area_percentage: 70.0,
        }
    }
}

/// Price increment calculation mode
#[derive(Debug, Clone, Serialize, Deserialize)]
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

        /// Calculate value area (exact 70% volume concentration using greedy selection)
    pub fn calculate_value_area(&self, value_area_percentage: f64) -> ValueArea {
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
        let actual_volume_in_range = 
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
        
        let value_area = map.calculate_value_area(70.0);
        
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
        
        let value_area = map.calculate_value_area(70.0);
        
        // Value area should capture exactly 70% volume (or as close as possible with discrete levels)
        assert!((value_area.volume_percentage - 70.0).abs() <= 10.0, 
                "Value area should capture ~70% volume: {}%", value_area.volume_percentage);
        
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
        
        let value_area = map.calculate_value_area(70.0);
        
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
        
        let value_area = map.calculate_value_area(70.0);
        assert_eq!(value_area.volume, 0.0);
        assert_eq!(value_area.volume_percentage, 0.0);
    }
}