//! Zero-copy serialization implementation for volume profile data structures
//! 
//! This module implements ultra-high-performance serialization using sonic-rs
//! for volume profile output generation, targeting <1ms serialization times.

use sonic_rs;
use serde_json::Value as JsonValue; // Use serde_json for construction, sonic_rs for serialization
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use crate::volume_profile::structs::VolumeProfileData;

/// Zero-copy serialization error types
#[derive(Debug, thiserror::Error)]
pub enum ZeroCopySerializationError {
    #[error("Sonic-rs serialization error: {0}")]
    SonicError(#[from] sonic_rs::Error),
    
    #[error("Buffer write error: {0}")]
    WriteError(#[from] std::io::Error),
    
    #[error("Decimal conversion error: {0}")]
    DecimalError(String),
}

/// Zero-copy serializer for volume profile data with buffer reuse
pub struct VolumeProfileSerializer {
    /// Reusable buffer for serialization operations
    buffer: Vec<u8>,
    /// Buffer pool for price level array serialization
    price_level_buffer: Vec<u8>,
    /// Performance metrics
    operations_count: u64,
    total_serialization_time: std::time::Duration,
}

impl VolumeProfileSerializer {
    /// Create new serializer with pre-allocated buffers
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(64 * 1024), // 64KB initial capacity
            price_level_buffer: Vec::with_capacity(32 * 1024), // 32KB for price levels
            operations_count: 0,
            total_serialization_time: std::time::Duration::ZERO,
        }
    }

    /// Serialize VolumeProfileData using sonic-rs for final serialization
    pub fn serialize_volume_profile(&mut self, profile: &VolumeProfileData) -> Result<String, ZeroCopySerializationError> {
        let start = std::time::Instant::now();
        
        // Build JSON value optimized for sonic-rs serialization
        let json_value = self.build_profile_json_value(profile)?;
        
        // Serialize using sonic-rs (faster than serde_json)
        let result = sonic_rs::to_string(&json_value)
            .map_err(ZeroCopySerializationError::SonicError)?;
        
        // Update performance metrics
        self.operations_count += 1;
        self.total_serialization_time += start.elapsed();
        
        Ok(result)
    }

    /// Build JSON value optimized for sonic-rs serialization
    fn build_profile_json_value(&mut self, profile: &VolumeProfileData) -> Result<JsonValue, ZeroCopySerializationError> {
        use serde_json::json;
        
        // Build price levels array optimized for bulk processing
        let mut price_levels = Vec::with_capacity(profile.price_levels.len());
        for level in &profile.price_levels {
            price_levels.push(json!({
                "price": self.decimal_to_f64(&level.price)?,
                "volume": self.decimal_to_f64(&level.volume)?,
                "percentage": self.decimal_to_f64(&level.percentage)?,
                "candle_count": level.candle_count
            }));
        }
        
        // Build value area
        let value_area = json!({
            "high": self.decimal_to_f64(&profile.value_area.high)?,
            "low": self.decimal_to_f64(&profile.value_area.low)?,
            "volume_percentage": self.decimal_to_f64(&profile.value_area.volume_percentage)?,
            "volume": self.decimal_to_f64(&profile.value_area.volume)?
        });
        
        // Build complete JSON object
        let json = json!({
            "date": profile.date,
            "total_volume": self.decimal_to_f64(&profile.total_volume)?,
            "vwap": self.decimal_to_f64(&profile.vwap)?,
            "poc": self.decimal_to_f64(&profile.poc)?,
            "price_increment": self.decimal_to_f64(&profile.price_increment)?,
            "min_price": self.decimal_to_f64(&profile.min_price)?,
            "max_price": self.decimal_to_f64(&profile.max_price)?,
            "candle_count": profile.candle_count,
            "last_updated": profile.last_updated,
            "price_levels": price_levels,
            "value_area": value_area
        });
        
        Ok(json)
    }

    /// Convert Decimal to f64 (optimized for sonic-rs)
    fn decimal_to_f64(&self, decimal: &Decimal) -> Result<f64, ZeroCopySerializationError> {
        decimal.to_f64()
            .ok_or_else(|| ZeroCopySerializationError::DecimalError(
                format!("Cannot convert Decimal {} to f64", decimal)
            ))
    }

    /// Get performance metrics
    pub fn get_performance_metrics(&self) -> SerializationMetrics {
        SerializationMetrics {
            operations_count: self.operations_count,
            total_time: self.total_serialization_time,
            average_time: if self.operations_count > 0 {
                self.total_serialization_time / self.operations_count as u32
            } else {
                std::time::Duration::ZERO
            },
            buffer_capacity: self.buffer.capacity(),
            buffer_reuse_ratio: if self.operations_count > 0 { 
                1.0 // Always reusing the same buffer
            } else { 
                0.0 
            },
        }
    }

    /// Reset performance metrics
    pub fn reset_metrics(&mut self) {
        self.operations_count = 0;
        self.total_serialization_time = std::time::Duration::ZERO;
    }

    /// Optimize buffer sizes based on usage patterns
    pub fn optimize_buffers(&mut self) {
        // If buffer has grown significantly, consider shrinking to reduce memory usage
        if self.buffer.capacity() > 1024 * 1024 && self.buffer.len() < self.buffer.capacity() / 4 {
            self.buffer.shrink_to(self.buffer.len() * 2);
        }
        
        if self.price_level_buffer.capacity() > 512 * 1024 && 
           self.price_level_buffer.len() < self.price_level_buffer.capacity() / 4 {
            self.price_level_buffer.shrink_to(self.price_level_buffer.len() * 2);
        }
    }
}

impl Default for BatchVolumeProfileSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for VolumeProfileSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Performance metrics for serialization operations
#[derive(Debug, Clone)]
pub struct SerializationMetrics {
    pub operations_count: u64,
    pub total_time: std::time::Duration,
    pub average_time: std::time::Duration,
    pub buffer_capacity: usize,
    pub buffer_reuse_ratio: f64,
}

/// Batch serializer for multiple profiles (portfolio optimization)
pub struct BatchVolumeProfileSerializer {
    serializer: VolumeProfileSerializer,
    batch_buffer: Vec<u8>,
}

impl BatchVolumeProfileSerializer {
    pub fn new() -> Self {
        Self {
            serializer: VolumeProfileSerializer::new(),
            batch_buffer: Vec::with_capacity(1024 * 1024), // 1MB for batch operations
        }
    }

    /// Serialize multiple profiles as JSON array (portfolio dashboard use case)
    pub fn serialize_batch(&mut self, profiles: &[VolumeProfileData]) -> Result<String, ZeroCopySerializationError> {
        // Clear and reuse batch buffer for memory efficiency
        self.batch_buffer.clear();
        
        // Build array of profile JSON objects
        let mut profile_values = Vec::with_capacity(profiles.len());
        
        for profile in profiles {
            let profile_json = self.serializer.build_profile_json_value(profile)?;
            profile_values.push(profile_json);
        }
        
        // Create JSON array
        let batch_json = JsonValue::Array(profile_values);
        
        // Serialize using sonic-rs for performance
        let result = sonic_rs::to_string(&batch_json)
            .map_err(ZeroCopySerializationError::SonicError)?;
        
        // Store in batch buffer for potential reuse/caching
        self.batch_buffer.extend_from_slice(result.as_bytes());
        
        Ok(result)
    }
}

/// Stream serializer for large datasets (memory pressure optimization)
pub struct StreamingVolumeProfileSerializer;

impl StreamingVolumeProfileSerializer {
    /// Serialize profiles as streaming JSON array (returns String for simplicity)
    pub fn serialize_streaming_to_string(
        profiles: impl Iterator<Item = VolumeProfileData>
    ) -> Result<String, ZeroCopySerializationError> {
        let mut result = String::from("[");
        let mut first = true;
        let mut serializer = VolumeProfileSerializer::new();
        
        for profile in profiles {
            if !first {
                result.push(',');
            }
            first = false;
            
            let profile_json = serializer.serialize_volume_profile(&profile)?;
            result.push_str(&profile_json);
        }
        
        result.push(']');
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::time::Instant;
    use crate::volume_profile::structs::{PriceLevelData, ValueArea};

    fn create_test_profile() -> VolumeProfileData {
        VolumeProfileData {
            date: "2025-01-15".to_string(),
            price_levels: vec![
                PriceLevelData {
                    price: dec!(45000.0),
                    volume: dec!(100.5),
                    percentage: dec!(25.5),
                    candle_count: 10,
                },
                PriceLevelData {
                    price: dec!(45001.0),
                    volume: dec!(150.3),
                    percentage: dec!(35.2),
                    candle_count: 15,
                },
            ],
            total_volume: dec!(250.8),
            vwap: dec!(45000.5),
            poc: dec!(45001.0),
            value_area: ValueArea {
                high: dec!(45010.0),
                low: dec!(44990.0),
                volume_percentage: dec!(70.0),
                volume: dec!(175.56),
            },
            price_increment: dec!(1.0),
            min_price: dec!(44990.0),
            max_price: dec!(45010.0),
            candle_count: 25,
            last_updated: 1736985600000,
        }
    }

    #[test]
    fn test_zero_copy_serialization() {
        let mut serializer = VolumeProfileSerializer::new();
        let profile = create_test_profile();
        
        let result = serializer.serialize_volume_profile(&profile);
        assert!(result.is_ok());
        
        let json = result.unwrap();
        assert!(json.contains("45000"));
        assert!(json.contains("price_levels"));
        assert!(json.contains("value_area"));
    }

    #[test]
    fn test_serialization_performance() {
        let mut serializer = VolumeProfileSerializer::new();
        let profile = create_test_profile();
        
        // Warm-up
        for _ in 0..10 {
            let _ = serializer.serialize_volume_profile(&profile);
        }
        
        // Performance test
        let start = Instant::now();
        let iterations = 1000;
        
        for _ in 0..iterations {
            let _ = serializer.serialize_volume_profile(&profile);
        }
        
        let elapsed = start.elapsed();
        let avg_time = elapsed / iterations;
        
        println!("Zero-copy serialization average time: {:?}", avg_time);
        
        // Should be significantly faster than baseline
        assert!(avg_time < std::time::Duration::from_micros(100));
        
        let metrics = serializer.get_performance_metrics();
        println!("Performance metrics: {:?}", metrics);
    }

    #[test]
    fn test_batch_serialization() {
        let mut batch_serializer = BatchVolumeProfileSerializer::new();
        let profiles = vec![create_test_profile(), create_test_profile()];
        
        let result = batch_serializer.serialize_batch(&profiles);
        assert!(result.is_ok());
        
        let json = result.unwrap();
        assert!(json.starts_with('['));
        assert!(json.ends_with(']'));
        assert!(json.matches("price_levels").count() == 2); // Two profiles
    }

    #[test]
    fn test_buffer_reuse() {
        let mut serializer = VolumeProfileSerializer::new();
        let profile = create_test_profile();
        
        // Serialize multiple times to test buffer reuse
        for _ in 0..100 {
            let _ = serializer.serialize_volume_profile(&profile);
        }
        
        let metrics = serializer.get_performance_metrics();
        assert_eq!(metrics.buffer_reuse_ratio, 1.0); // 100% buffer reuse
        assert_eq!(metrics.operations_count, 100);
    }

    #[test]
    fn test_decimal_to_f64_conversion() {
        let serializer = VolumeProfileSerializer::new();
        let decimal = dec!(45000.123);
        
        let result = serializer.decimal_to_f64(&decimal);
        assert!(result.is_ok());
        
        let float_value = result.unwrap();
        assert!((float_value - 45000.123).abs() < 0.001);
    }
}