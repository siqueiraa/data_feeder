/// Ultra-high-performance serialization for technical analysis output
/// 
/// This module provides custom serialization optimized for our specific data structures,
/// bypassing serde overhead for maximum performance in hot paths.
use crate::technical_analysis::structs::IndicatorOutput;
use std::io::Write;

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
        
        // Remove trailing comma if present
        if self.json_buffer.last() == Some(&b',') {
            self.json_buffer.pop();
        }
        
        self.json_buffer.extend_from_slice(b"}");
        
        &self.json_buffer
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
}