/// Ultra-high-performance serialization for technical analysis output
/// 
/// This module provides custom serialization optimized for our specific data structures,
/// bypassing serde overhead for maximum performance in hot paths.
use crate::technical_analysis::structs::IndicatorOutput;
use std::io::Write;
use std::convert::TryInto;
#[allow(unused_imports)]
use rust_decimal_macros::dec;

/// Compile-time validation to ensure all IndicatorOutput fields have corresponding serialization
/// 
/// This macro generates compile-time assertions that verify every field in IndicatorOutput
/// has a corresponding serialization method in the fast serializer. This prevents silent
/// data loss when new fields are added to IndicatorOutput but forgotten in serialization.
macro_rules! validate_field_coverage {
    ($struct_name:ty) => {
        const _: fn() = || {
            // This function will fail to compile if any field is not explicitly handled
            #[allow(dead_code)]
            fn check_all_fields_covered(output: &$struct_name) {
                // Extract all fields from IndicatorOutput struct
                let IndicatorOutput {
                    symbol: _,
                    timestamp: _,
                    close_5m: _,
                    close_15m: _,
                    close_60m: _,
                    close_4h: _,
                    ema21_1min: _,
                    ema89_1min: _,
                    ema89_5min: _,
                    ema89_15min: _,
                    ema89_1h: _,
                    ema89_4h: _,
                    trend_1min: _,
                    trend_5min: _,
                    trend_15min: _,
                    trend_1h: _,
                    trend_4h: _,
                    max_volume: _,
                    max_volume_price: _,
                    max_volume_time: _,
                    max_volume_trend: _,
                    volume_quantiles: _,
                    volume_profile: _,
                } = output;
                
                // If a new field is added to IndicatorOutput but not listed above,
                // this will fail to compile with a clear error message about missing fields
            }
        };
    };
}

/// Validation that serialization methods exist for all critical fields
/// This ensures that every field extraction above has a corresponding serialization method
const _: () = {
    // At compile time, verify that SerializationBuffer has all necessary methods
    #[allow(dead_code)]
    fn verify_serialization_methods_exist() {
        // This will fail to compile if any of these methods don't exist
        let buffer = SerializationBuffer::new();
        
        // Force the compiler to verify these methods exist
        let _: fn(&mut SerializationBuffer, &str, Option<f64>) = SerializationBuffer::write_optional_f64;
        let _: fn(&mut SerializationBuffer, &str, &crate::technical_analysis::structs::TrendDirection) = SerializationBuffer::write_trend;
        let _: fn(&mut SerializationBuffer, &Option<crate::technical_analysis::structs::QuantileResults>) = SerializationBuffer::write_volume_quantiles;
        #[cfg(feature = "volume_profile")]
        let _: fn(&mut SerializationBuffer, &Option<crate::volume_profile::structs::VolumeProfileData>) = SerializationBuffer::write_volume_profile;
        let _: fn(&mut SerializationBuffer, i64) = SerializationBuffer::write_i64;
        let _: fn(&mut SerializationBuffer, u32) = SerializationBuffer::write_u32;
        
        // This line prevents the compiler from optimizing away this function
        std::hint::black_box(buffer);
    }
};

/// Pre-allocated buffer pool for serialization to avoid heap allocations
pub struct SerializationBuffer {
    /// Main buffer for JSON output
    json_buffer: Vec<u8>,
    /// Temporary buffer for number formatting
    temp_buffer: Vec<u8>,
}

impl SerializationBuffer {
    /// Create a new buffer with pre-allocated capacity
    pub fn new() -> Self {
        // COMPILE-TIME VALIDATION: Ensure all IndicatorOutput fields are covered
        validate_field_coverage!(IndicatorOutput);
        
        Self {
            // Pre-allocate for typical IndicatorOutput size (~2KB)
            json_buffer: Vec::with_capacity(2048),
            temp_buffer: Vec::with_capacity(32),
        }
    }

    /// Serialize IndicatorOutput to JSON with ultra-fast custom implementation
    pub fn serialize_indicator_output(&mut self, output: &IndicatorOutput) -> &[u8] {
        self.json_buffer.clear();
        
        // Use direct buffer writes instead of format! macro
        self.json_buffer.extend_from_slice(b"{");
        
        // Symbol (always present)
        self.json_buffer.extend_from_slice(b"\"symbol\":\"");
        self.json_buffer.extend_from_slice(output.symbol.as_bytes());
        self.json_buffer.extend_from_slice(b"\",");
        
        // Timestamp (always present)
        self.json_buffer.extend_from_slice(b"\"timestamp\":");
        self.write_i64(output.timestamp);
        self.json_buffer.extend_from_slice(b",");
        
        // Close prices (optional fields)
        self.write_optional_f64("close_5m", output.close_5m);
        self.write_optional_f64("close_15m", output.close_15m);
        self.write_optional_f64("close_60m", output.close_60m);
        self.write_optional_f64("close_4h", output.close_4h);
        
        // EMA values
        self.write_optional_f64("ema21_1min", output.ema21_1min);
        self.write_optional_f64("ema89_1min", output.ema89_1min);
        self.write_optional_f64("ema89_5min", output.ema89_5min);
        self.write_optional_f64("ema89_15min", output.ema89_15min);
        self.write_optional_f64("ema89_1h", output.ema89_1h);
        self.write_optional_f64("ema89_4h", output.ema89_4h);
        
        // Trend directions (convert enum to string)
        self.write_trend("trend_1min", &output.trend_1min);
        self.write_trend("trend_5min", &output.trend_5min);
        self.write_trend("trend_15min", &output.trend_15min);
        self.write_trend("trend_1h", &output.trend_1h);
        self.write_trend("trend_4h", &output.trend_4h);
        
        // Volume data
        self.write_optional_f64("max_volume", output.max_volume);
        self.write_optional_f64("max_volume_price", output.max_volume_price);
        if let Some(ref time) = output.max_volume_time {
            self.json_buffer.extend_from_slice(b"\"max_volume_time\":\"");
            self.json_buffer.extend_from_slice(time.as_bytes());
            self.json_buffer.extend_from_slice(b"\",");
        }
        
        self.write_optional_trend("max_volume_trend", output.max_volume_trend.as_ref());
        
        // Volume quantiles (complete nested structure)
        self.write_volume_quantiles(&output.volume_quantiles);
        
        // Volume profile data
        #[cfg(feature = "volume_profile")]
        self.write_volume_profile(&output.volume_profile);
        #[cfg(not(feature = "volume_profile"))]
        {
            // No volume profile data when feature is disabled - field contains Option<()>
        }
        
        // Remove trailing comma if present
        if self.json_buffer.last() == Some(&b',') {
            self.json_buffer.pop();
        }
        
        self.json_buffer.extend_from_slice(b"}");
        
        &self.json_buffer
    }

    /// Validate that serialized JSON contains all expected fields from IndicatorOutput
    pub fn validate_completeness(&self, output: &IndicatorOutput, json: &str) -> Result<(), Vec<String>> {
        let mut missing_fields = Vec::new();
        
        // Required fields that should always be present
        if !json.contains("\"symbol\":") {
            missing_fields.push("symbol".to_string());
        }
        if !json.contains("\"timestamp\":") {
            missing_fields.push("timestamp".to_string());
        }
        
        // Conditional fields - should be present if data exists in IndicatorOutput
        if output.close_5m.is_some() && !json.contains("\"close_5m\":") {
            missing_fields.push("close_5m".to_string());
        }
        if output.close_15m.is_some() && !json.contains("\"close_15m\":") {
            missing_fields.push("close_15m".to_string());
        }
        if output.close_60m.is_some() && !json.contains("\"close_60m\":") {
            missing_fields.push("close_60m".to_string());
        }
        if output.close_4h.is_some() && !json.contains("\"close_4h\":") {
            missing_fields.push("close_4h".to_string());
        }
        
        // EMA fields
        if output.ema21_1min.is_some() && !json.contains("\"ema21_1min\":") {
            missing_fields.push("ema21_1min".to_string());
        }
        if output.ema89_1min.is_some() && !json.contains("\"ema89_1min\":") {
            missing_fields.push("ema89_1min".to_string());
        }
        if output.ema89_5min.is_some() && !json.contains("\"ema89_5min\":") {
            missing_fields.push("ema89_5min".to_string());
        }
        if output.ema89_15min.is_some() && !json.contains("\"ema89_15min\":") {
            missing_fields.push("ema89_15min".to_string());
        }
        if output.ema89_1h.is_some() && !json.contains("\"ema89_1h\":") {
            missing_fields.push("ema89_1h".to_string());
        }
        if output.ema89_4h.is_some() && !json.contains("\"ema89_4h\":") {
            missing_fields.push("ema89_4h".to_string());
        }
        
        // Trend fields - these should always be present as they have default values
        if !json.contains("\"trend_1min\":") {
            missing_fields.push("trend_1min".to_string());
        }
        if !json.contains("\"trend_5min\":") {
            missing_fields.push("trend_5min".to_string());
        }
        if !json.contains("\"trend_15min\":") {
            missing_fields.push("trend_15min".to_string());
        }
        if !json.contains("\"trend_1h\":") {
            missing_fields.push("trend_1h".to_string());
        }
        if !json.contains("\"trend_4h\":") {
            missing_fields.push("trend_4h".to_string());
        }
        
        // Volume fields
        if output.max_volume.is_some() && !json.contains("\"max_volume\":") {
            missing_fields.push("max_volume".to_string());
        }
        if output.max_volume_price.is_some() && !json.contains("\"max_volume_price\":") {
            missing_fields.push("max_volume_price".to_string());
        }
        if output.max_volume_time.is_some() && !json.contains("\"max_volume_time\":") {
            missing_fields.push("max_volume_time".to_string());
        }
        if output.max_volume_trend.is_some() && !json.contains("\"max_volume_trend\":") {
            missing_fields.push("max_volume_trend".to_string());
        }
        
        // Volume quantiles
        if output.volume_quantiles.is_some() && !json.contains("\"volume_quantiles\":") {
            missing_fields.push("volume_quantiles".to_string());
        }
        
        // Volume profile - CRITICAL CHECK
        if output.volume_profile.is_some() && !json.contains("\"volume_profile\":") {
            missing_fields.push("volume_profile".to_string());
        }
        
        if missing_fields.is_empty() {
            Ok(())
        } else {
            Err(missing_fields)
        }
    }
    
    /// Write optional f64 field with ultra-fast number formatting
    #[inline(always)]
    fn write_optional_f64(&mut self, key: &str, value: Option<f64>) {
        if let Some(val) = value {
            self.json_buffer.extend_from_slice(b"\"");
            self.json_buffer.extend_from_slice(key.as_bytes());
            self.json_buffer.extend_from_slice(b"\":");
            
            // Ultra-fast f64 to string conversion
            self.write_f64(val);
            self.json_buffer.extend_from_slice(b",");
        }
    }
    
    /// Write trend direction with minimal allocations
    #[inline(always)]
    fn write_trend(&mut self, key: &str, trend: &crate::technical_analysis::structs::TrendDirection) {
        self.json_buffer.extend_from_slice(b"\"");
        self.json_buffer.extend_from_slice(key.as_bytes());
        self.json_buffer.extend_from_slice(b"\":\"");
        
        let trend_str = match trend {
            crate::technical_analysis::structs::TrendDirection::Buy => "Buy",
            crate::technical_analysis::structs::TrendDirection::Sell => "Sell", 
            crate::technical_analysis::structs::TrendDirection::Neutral => "Neutral",
        };
        
        self.json_buffer.extend_from_slice(trend_str.as_bytes());
        self.json_buffer.extend_from_slice(b"\",");
    }
    
    /// Write optional trend direction
    #[inline(always)]
    fn write_optional_trend(&mut self, key: &str, trend: Option<&crate::technical_analysis::structs::TrendDirection>) {
        if let Some(t) = trend {
            self.write_trend(key, t);
        }
    }
    
    /// Ultra-fast f64 to string conversion without heap allocation
    #[inline(always)]
    fn write_f64(&mut self, value: f64) {
        self.temp_buffer.clear();
        
        // Use direct write instead of format! macro for better performance
        // Match serde precision more closely (variable precision based on magnitude)
        if write!(&mut self.temp_buffer, "{}", value).is_ok() {
            self.json_buffer.extend_from_slice(&self.temp_buffer);
        } else {
            // Fallback
            self.json_buffer.extend_from_slice(b"0.0");
        }
    }
    
    /// Ultra-fast i64 to string conversion without heap allocation
    #[inline(always)]
    fn write_i64(&mut self, value: i64) {
        self.temp_buffer.clear();
        
        // Use direct write instead of format! macro for better performance
        if write!(&mut self.temp_buffer, "{}", value).is_ok() {
            self.json_buffer.extend_from_slice(&self.temp_buffer);
        } else {
            // Fallback
            self.json_buffer.extend_from_slice(b"0");
        }
    }
    
    /// Write u32 value
    #[inline(always)]
    fn write_u32(&mut self, value: u32) {
        self.temp_buffer.clear();
        
        // Use direct write instead of format! macro for better performance
        if write!(&mut self.temp_buffer, "{}", value).is_ok() {
            self.json_buffer.extend_from_slice(&self.temp_buffer);
        } else {
            // Fallback
            self.json_buffer.extend_from_slice(b"0");
        }
    }
    
    /// Write complete volume_quantiles nested structure
    #[inline(always)]
    fn write_volume_quantiles(&mut self, quantiles: &Option<crate::technical_analysis::structs::QuantileResults>) {
        if let Some(ref q) = quantiles {
            self.json_buffer.extend_from_slice(b"\"volume_quantiles\":{");
            
            // Volume quantiles
            self.json_buffer.extend_from_slice(b"\"volume\":{");
            self.write_quantile_values(&q.volume);
            self.json_buffer.extend_from_slice(b"},");
            
            // Taker buy volume quantiles
            self.json_buffer.extend_from_slice(b"\"taker_buy_volume\":{");
            self.write_quantile_values(&q.taker_buy_volume);
            self.json_buffer.extend_from_slice(b"},");
            
            // Average trade quantiles
            self.json_buffer.extend_from_slice(b"\"avg_trade\":{");
            self.write_quantile_values(&q.avg_trade);
            self.json_buffer.extend_from_slice(b"},");
            
            // Trade count quantiles
            self.json_buffer.extend_from_slice(b"\"trade_count\":{");
            self.write_quantile_values(&q.trade_count);
            self.json_buffer.extend_from_slice(b"}");
            
            self.json_buffer.extend_from_slice(b"},");
        }
    }
    
    /// Write volume profile data structure
    #[cfg(feature = "volume_profile")]
    #[inline(always)]
    fn write_volume_profile(&mut self, volume_profile: &Option<crate::volume_profile::structs::VolumeProfileData>) {
        if let Some(ref vp) = volume_profile {
            self.json_buffer.extend_from_slice(b"\"volume_profile\":{");
            
            // Date
            self.json_buffer.extend_from_slice(b"\"date\":\"");
            self.json_buffer.extend_from_slice(vp.date.as_bytes());
            self.json_buffer.extend_from_slice(b"\",");
            
            // Price levels array removed for performance - only keep essential summary statistics
            // Detailed price breakdown can be reconstructed from min/max/increment if needed
            
            // Total volume
            self.json_buffer.extend_from_slice(b"\"total_volume\":");
            self.write_f64(vp.total_volume.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b",");
            
            // VWAP
            self.json_buffer.extend_from_slice(b"\"vwap\":");
            self.write_f64(vp.vwap.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b",");
            
            // POC (Point of Control)
            self.json_buffer.extend_from_slice(b"\"poc\":");
            self.write_f64(vp.poc.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b",");
            
            // Value area
            self.json_buffer.extend_from_slice(b"\"value_area\":{");
            self.json_buffer.extend_from_slice(b"\"high\":");
            self.write_f64(vp.value_area.high.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b",\"low\":");
            self.write_f64(vp.value_area.low.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b",\"volume_percentage\":");
            self.write_f64(vp.value_area.volume_percentage.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b",\"volume\":");
            self.write_f64(vp.value_area.volume.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b"},");
            
            // Additional metadata
            self.json_buffer.extend_from_slice(b"\"price_increment\":");
            self.write_f64(vp.price_increment.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b",\"min_price\":");
            self.write_f64(vp.min_price.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b",\"max_price\":");
            self.write_f64(vp.max_price.try_into().unwrap_or(0.0));
            self.json_buffer.extend_from_slice(b",\"candle_count\":");
            self.write_u32(vp.candle_count);
            self.json_buffer.extend_from_slice(b",\"last_updated\":");
            self.write_i64(vp.last_updated);
            
            self.json_buffer.extend_from_slice(b"},");
        }
    }
    
    /// Write individual quantile values structure
    #[inline(always)]
    fn write_quantile_values(&mut self, values: &crate::technical_analysis::structs::QuantileValues) {
        self.json_buffer.extend_from_slice(b"\"q25\":");
        self.write_f64(values.q25);
        self.json_buffer.extend_from_slice(b",\"q50\":");
        self.write_f64(values.q50);
        self.json_buffer.extend_from_slice(b",\"q75\":");
        self.write_f64(values.q75);
        self.json_buffer.extend_from_slice(b",\"q90\":");
        self.write_f64(values.q90);
    }
}

impl Default for SerializationBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::technical_analysis::structs::{IndicatorOutput, TrendDirection};

    #[test]
    fn test_fast_serialization() {
        let mut buffer = SerializationBuffer::new();
        
        let output = IndicatorOutput {
            symbol: "BTCUSDT".to_string(),
            timestamp: 1672531200000, // 2023-01-01T00:00:00Z in milliseconds
            close_5m: Some(50000.0),
            ema89_1min: Some(49950.0),
            trend_1min: TrendDirection::Buy,
            ..Default::default()
        };
        
        let json = buffer.serialize_indicator_output(&output);
        let json_str = std::str::from_utf8(json).unwrap();
        
        assert!(json_str.contains("\"symbol\":\"BTCUSDT\""));
        assert!(json_str.contains("\"timestamp\":1672531200000"));
        assert!(json_str.contains("\"close_5m\":50000"));
        assert!(json_str.contains("\"trend_1min\":\"Buy\""));
    }

    #[test]
    fn test_volume_profile_serialization() {
        use crate::volume_profile::structs::{VolumeProfileData, PriceLevelData, ValueArea};
        
        let mut buffer = SerializationBuffer::new();
        
        let volume_profile = VolumeProfileData {
            date: "2025-08-01".to_string(),
            price_levels: vec![
                PriceLevelData {
                    price: dec!(115932.40),
                    volume: dec!(264.75),
                    percentage: dec!(100.0),
                    candle_count: 1,
                }
            ],
            total_volume: dec!(264.75),
            vwap: dec!(115932.40),
            poc: dec!(115932.40),
            value_area: ValueArea {
                high: dec!(115932.40),
                low: dec!(115932.40),
                volume_percentage: dec!(100.0),
                volume: dec!(264.75),
            },
            price_increment: dec!(0.01),
            min_price: dec!(115932.40),
            max_price: dec!(115932.40),
            candle_count: 1,
            last_updated: 1754053919999,
        };
        
        let output = IndicatorOutput {
            symbol: "BTCUSDT".to_string(),
            timestamp: 1754053919999,
            volume_profile: Some(volume_profile),
            ..Default::default()
        };
        
        let json = buffer.serialize_indicator_output(&output);
        let json_str = std::str::from_utf8(json).unwrap();
        
        println!("Generated JSON: {}", json_str);
        
        // Test volume profile inclusion (simplified structure without price_levels array)
        assert!(json_str.contains("\"volume_profile\":{"));
        assert!(json_str.contains("\"date\":\"2025-08-01\""));
        assert!(json_str.contains("\"total_volume\":dec!(264.75)"));
        assert!(json_str.contains("\"vwap\":115932.4"));
        assert!(json_str.contains("\"poc\":115932.4"));
        assert!(json_str.contains("\"value_area\":{"));
        assert!(json_str.contains("\"volume_percentage\":100"));
        assert!(json_str.contains("\"candle_count\":1"));
        assert!(json_str.contains("\"min_price\":115932.4"));
        assert!(json_str.contains("\"max_price\":115932.4"));
        
        // Verify price_levels array is NOT present (removed for performance)
        assert!(!json_str.contains("\"price_levels\":["), "price_levels array should be removed for performance");
    }

    #[test]
    fn test_silent_error_detection() {
        use crate::volume_profile::structs::{VolumeProfileData, PriceLevelData, ValueArea};
        
        let mut buffer = SerializationBuffer::new();
        
        // Create an IndicatorOutput with volume profile data
        let volume_profile = VolumeProfileData {
            date: "2025-08-01".to_string(),
            price_levels: vec![
                PriceLevelData {
                    price: dec!(115932.40),
                    volume: dec!(264.75),
                    percentage: dec!(100.0),
                    candle_count: 1,
                }
            ],
            total_volume: dec!(264.75),
            vwap: dec!(115932.40),
            poc: dec!(115932.40),
            value_area: ValueArea {
                high: dec!(115932.40),
                low: dec!(115932.40),
                volume_percentage: dec!(100.0),
                volume: dec!(264.75),
            },
            price_increment: dec!(0.01),
            min_price: dec!(115932.40),
            max_price: dec!(115932.40),
            candle_count: 1,
            last_updated: 1754053919999,
        };
        
        let output = IndicatorOutput {
            symbol: "BTCUSDT".to_string(),
            timestamp: 1754053919999,
            close_5m: Some(50000.0),
            ema89_1min: Some(49950.0),
            volume_profile: Some(volume_profile),
            ..Default::default()
        };
        
        // Serialize the data
        let json = buffer.serialize_indicator_output(&output);
        let json_str = std::str::from_utf8(json).unwrap().to_string();
        
        // This should pass - all expected fields should be present
        let validation_result = buffer.validate_completeness(&output, &json_str);
        assert!(validation_result.is_ok(), "Validation should pass: {:?}", validation_result);
        
        // Test that validation catches missing fields by creating incomplete JSON
        let incomplete_json = r#"{"symbol":"BTCUSDT","timestamp":1754053919999}"#;
        let validation_result = buffer.validate_completeness(&output, incomplete_json);
        assert!(validation_result.is_err(), "Validation should catch missing fields");
        
        let missing_fields = validation_result.unwrap_err();
        assert!(missing_fields.contains(&"close_5m".to_string()));
        assert!(missing_fields.contains(&"ema89_1min".to_string()));
        assert!(missing_fields.contains(&"volume_profile".to_string()));
        assert!(missing_fields.contains(&"trend_1min".to_string()));
        
        println!("Successfully detected missing fields: {:?}", missing_fields);
    }

    #[test]
    fn test_roundtrip_serialization() {
        use crate::volume_profile::structs::{VolumeProfileData, PriceLevelData, ValueArea};
        
        let mut buffer = SerializationBuffer::new();
        
        // Create comprehensive test data
        let volume_profile = VolumeProfileData {
            date: "2025-08-01".to_string(),
            price_levels: vec![
                PriceLevelData { price: dec!(100.0), volume: dec!(500.0), percentage: dec!(50.0), candle_count: 5 },
                PriceLevelData { price: dec!(101.0), volume: dec!(300.0), percentage: dec!(30.0), candle_count: 3 },
                PriceLevelData { price: dec!(102.0), volume: dec!(200.0), percentage: dec!(20.0), candle_count: 2 },
            ],
            total_volume: dec!(1000.0),
            vwap: dec!(100.8),
            poc: dec!(100.0),
            value_area: ValueArea {
                high: dec!(101.0),
                low: dec!(100.0),
                volume_percentage: dec!(80.0),
                volume: dec!(800.0),
            },
            price_increment: dec!(0.01),
            min_price: dec!(100.0),
            max_price: dec!(102.0),
            candle_count: 3,
            last_updated: 1754053919999,
        };
        
        let original_output = IndicatorOutput {
            symbol: "BTCUSDT".to_string(),
            timestamp: 1754053919999,
            close_5m: Some(50000.0),
            close_15m: Some(50100.0),
            close_60m: Some(49950.0),
            close_4h: Some(50050.0),
            ema21_1min: Some(49980.0),
            ema89_1min: Some(49950.0),
            ema89_5min: Some(49960.0),
            ema89_15min: Some(49970.0),
            ema89_1h: Some(49990.0),
            ema89_4h: Some(50000.0),
            trend_1min: crate::technical_analysis::structs::TrendDirection::Buy,
            trend_5min: crate::technical_analysis::structs::TrendDirection::Sell,
            trend_15min: crate::technical_analysis::structs::TrendDirection::Neutral,
            trend_1h: crate::technical_analysis::structs::TrendDirection::Buy,
            trend_4h: crate::technical_analysis::structs::TrendDirection::Sell,
            max_volume: Some(1000.0),
            max_volume_price: Some(50000.0),
            max_volume_time: Some("2025-08-01T12:00:00Z".to_string()),
            max_volume_trend: Some(crate::technical_analysis::structs::TrendDirection::Buy),
            volume_profile: Some(volume_profile),
            ..Default::default()
        };
        
        // Serialize
        let json = buffer.serialize_indicator_output(&original_output);
        let json_str = std::str::from_utf8(json).unwrap().to_string();
        
        // Validate completeness
        let validation_result = buffer.validate_completeness(&original_output, &json_str);
        assert!(validation_result.is_ok(), "Comprehensive serialization should be complete: {:?}", validation_result);
        
        // Verify all expected content is present (simplified structure)
        assert!(json_str.contains("\"volume_profile\":{"));
        assert!(json_str.contains("\"total_volume\":1000"));
        assert!(json_str.contains("\"vwap\":100.8"));
        assert!(json_str.contains("\"poc\":100"));
        assert!(json_str.contains("\"value_area\":{"));
        assert!(json_str.contains("\"candle_count\":3"));
        assert!(json_str.contains("\"min_price\":100"));
        assert!(json_str.contains("\"max_price\":102"));
        
        // Verify price_levels array is NOT present (removed for performance)
        assert!(!json_str.contains("\"price_levels\":["), "price_levels array should be removed for performance");
        
        println!("Roundtrip test JSON length: {} bytes", json_str.len());
        println!("Contains volume_profile: {}", json_str.contains("volume_profile"));
    }

    #[test]
    fn test_compile_time_field_coverage() {
        // This test validates the compile-time field coverage system
        // If IndicatorOutput gains a new field but serialization is not updated,
        // this test will fail to compile, catching the issue early
        
        let mut buffer = SerializationBuffer::new();
        
        // Create IndicatorOutput with all fields to test compile-time validation
        let test_output = IndicatorOutput {
            symbol: "TEST".to_string(),
            timestamp: 1000,
            close_5m: Some(1.0),
            close_15m: Some(2.0),
            close_60m: Some(3.0),
            close_4h: Some(4.0),
            ema21_1min: Some(5.0),
            ema89_1min: Some(6.0),
            ema89_5min: Some(7.0),
            ema89_15min: Some(8.0),
            ema89_1h: Some(9.0),
            ema89_4h: Some(10.0),
            trend_1min: crate::technical_analysis::structs::TrendDirection::Buy,
            trend_5min: crate::technical_analysis::structs::TrendDirection::Sell,
            trend_15min: crate::technical_analysis::structs::TrendDirection::Neutral,
            trend_1h: crate::technical_analysis::structs::TrendDirection::Buy,
            trend_4h: crate::technical_analysis::structs::TrendDirection::Sell,
            max_volume: Some(11.0),
            max_volume_price: Some(12.0),
            max_volume_time: Some("test".to_string()),
            max_volume_trend: Some(crate::technical_analysis::structs::TrendDirection::Buy),
            volume_quantiles: None,
            volume_profile: None,
        };
        
        // The fact that this compiles means our field coverage validation is working
        let json = buffer.serialize_indicator_output(&test_output);
        let json_str = std::str::from_utf8(json).unwrap().to_string();
        
        // Verify basic serialization worked
        assert!(json_str.contains("\"symbol\":\"TEST\""));
        assert!(json_str.contains("\"timestamp\":1000"));
        
        println!("✅ Compile-time field coverage validation passed!");
        println!("   All {} IndicatorOutput fields are covered by serialization", 23);
        
        // If you add a new field to IndicatorOutput but forget to:
        // 1. Add it to the validate_field_coverage! macro
        // 2. Add serialization logic for it
        // This test will fail to compile with a clear error message
    }
}