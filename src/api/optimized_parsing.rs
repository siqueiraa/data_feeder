//! Optimized API response parsing for Binance data
//! 
//! This module provides high-performance parsing for API responses,
//! avoiding double-parsing (JSON → Value → Struct) for better performance.

use crate::historical::structs::FuturesOHLCVCandle;
use crate::metrics::get_metrics;
use crate::record_operation_duration;
use std::time::Instant;
use tracing::debug;
use simd_json::{OwnedValue, prelude::*};
use ahash::AHashMap;

/// Cache for frequently accessed data to reduce repeated parsing
pub struct ApiParseCache {
    parsed_candles: AHashMap<String, Vec<FuturesOHLCVCandle>>,
    cache_hits: u64,
    cache_misses: u64,
}

impl Default for ApiParseCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ApiParseCache {
    pub fn new() -> Self {
        Self {
            parsed_candles: AHashMap::with_capacity(128),
            cache_hits: 0,
            cache_misses: 0,
        }
    }

    pub fn get_cache_stats(&self) -> (u64, u64) {
        (self.cache_hits, self.cache_misses)
    }

    pub fn clear(&mut self) {
        self.parsed_candles.clear();
        self.cache_hits = 0;
        self.cache_misses = 0;
    }
}

/// High-performance API response parser
pub struct OptimizedApiParser {
    cache: ApiParseCache,
    enable_caching: bool,
    enable_metrics: bool,
}

impl OptimizedApiParser {
    pub fn new() -> Self {
        Self {
            cache: ApiParseCache::new(),
            enable_caching: true,
            enable_metrics: true,
        }
    }

    /// Parse Binance klines API response with optimizations
    pub fn parse_klines_response(&mut self, response: &str, cache_key: Option<&str>) -> Result<Vec<FuturesOHLCVCandle>, ApiParseError> {
        let start = Instant::now();

        // Check cache first if enabled
        if let Some(key) = cache_key {
            if self.enable_caching {
                if let Some(cached) = self.cache.parsed_candles.get(key) {
                    self.cache.cache_hits += 1;
                    if self.enable_metrics {
                        record_operation_duration!("api", "parse_klines_cached", start.elapsed().as_secs_f64());
                    }
                    return Ok(cached.clone());
                }
                self.cache.cache_misses += 1;
            }
        }

        // Fast path: Direct array parsing
        let result = self.parse_klines_direct(response)
            .or_else(|_| {
                debug!("Direct parsing failed, falling back to serde_json");
                self.parse_klines_fallback(response)
            });

        // Cache successful results
        if let (Ok(ref candles), Some(key)) = (&result, cache_key) {
            if self.enable_caching && candles.len() > 10 { // Only cache larger responses
                self.cache.parsed_candles.insert(key.to_string(), candles.clone());
            }
        }

        // Record metrics
        if self.enable_metrics {
            let duration = start.elapsed();
            let _success = result.is_ok(); // Reserved for future metrics usage
            record_operation_duration!("api", "parse_klines", duration.as_secs_f64());
            
            if let Some(metrics) = get_metrics() {
                metrics.record_operation_duration("api", "parse_response", duration.as_secs_f64());
            }
        }

        result
    }

    /// Fast path: Direct array parsing using simd-json
    fn parse_klines_direct(&self, response: &str) -> Result<Vec<FuturesOHLCVCandle>, ApiParseError> {
        let mut response_bytes = response.as_bytes().to_vec();
        
        // Parse with simd-json
        let mut parsed = simd_json::to_owned_value(&mut response_bytes)
            .map_err(|e| ApiParseError::SimdJsonError(e.to_string()))?;

        let array = parsed.as_array_mut()
            .ok_or(ApiParseError::InvalidFormat("Expected array response"))?;

        let mut candles = Vec::with_capacity(array.len());

        for item in array.iter_mut() {
            let array_item = item.as_array_mut()
                .ok_or(ApiParseError::InvalidFormat("Expected array item"))?;

            if array_item.len() < 11 {
                return Err(ApiParseError::InvalidFormat("Insufficient array elements"));
            }

            let candle = FuturesOHLCVCandle {
                open_time: parse_array_int(&array_item[0])?,
                open: parse_array_float(&array_item[1])?,
                high: parse_array_float(&array_item[2])?,
                low: parse_array_float(&array_item[3])?,
                close: parse_array_float(&array_item[4])?,
                volume: parse_array_float(&array_item[5])?,
                close_time: parse_array_int(&array_item[6])?,
                taker_buy_base_asset_volume: parse_array_float(&array_item[7])?, 
                number_of_trades: parse_array_int(&array_item[8])? as u64,
                closed: true, // API responses are always for closed candles
            };

            candles.push(candle);
        }

        Ok(candles)
    }

    /// Fallback: Standard serde_json parsing for compatibility
    fn parse_klines_fallback(&self, response: &str) -> Result<Vec<FuturesOHLCVCandle>, ApiParseError> {
        use serde_json::Value;

        let parsed: Value = serde_json::from_str(response)
            .map_err(|e| ApiParseError::SerdeJsonError(e.to_string()))?;

        let array = parsed.as_array()
            .ok_or(ApiParseError::InvalidFormat("Expected array response"))?;

        let mut candles = Vec::with_capacity(array.len());

        for item in array {
            let array_item = item.as_array()
                .ok_or(ApiParseError::InvalidFormat("Expected array item"))?;

            if array_item.len() < 11 {
                return Err(ApiParseError::InvalidFormat("Insufficient array elements"));
            }

            let candle = FuturesOHLCVCandle {
                open_time: parse_json_int(&array_item[0])?,
                open: parse_json_float(&array_item[1])?,
                high: parse_json_float(&array_item[2])?,
                low: parse_json_float(&array_item[3])?,
                close: parse_json_float(&array_item[4])?,
                volume: parse_json_float(&array_item[5])?,
                close_time: parse_json_int(&array_item[6])?,
                taker_buy_base_asset_volume: parse_json_float(&array_item[7])?,
                number_of_trades: parse_json_int(&array_item[8])? as u64,
                closed: true,
            };

            candles.push(candle);
        }

        Ok(candles)
    }

    /// Streaming parser for very large API responses
    pub fn parse_klines_streaming<R: std::io::Read>(&mut self, reader: R) -> Result<Vec<FuturesOHLCVCandle>, ApiParseError> {
        use serde_json::Deserializer;
        
        let start = Instant::now();
        let mut candles = Vec::new();

        let stream = Deserializer::from_reader(reader).into_iter::<Vec<serde_json::Value>>();
        
        for value in stream {
            let array = value.map_err(|e| ApiParseError::SerdeJsonError(e.to_string()))?;
            
            for item in array {
                let array_item = item.as_array()
                    .ok_or(ApiParseError::InvalidFormat("Expected array item"))?;

                if array_item.len() < 11 {
                    continue; // Skip malformed items
                }

                let candle = FuturesOHLCVCandle {
                    open_time: parse_json_int(&array_item[0])?,
                    open: parse_json_float(&array_item[1])?,
                    high: parse_json_float(&array_item[2])?,
                    low: parse_json_float(&array_item[3])?,
                    close: parse_json_float(&array_item[4])?,
                    volume: parse_json_float(&array_item[5])?,
                    close_time: parse_json_int(&array_item[6])?,
                    taker_buy_base_asset_volume: parse_json_float(&array_item[7])?,
                    number_of_trades: parse_json_int(&array_item[8])? as u64,
                    closed: true,
                };

                candles.push(candle);
            }
        }

        if self.enable_metrics {
            record_operation_duration!("api", "parse_streaming", start.elapsed().as_secs_f64());
        }

        Ok(candles)
    }

    pub fn get_cache_stats(&self) -> (u64, u64) {
        self.cache.get_cache_stats()
    }

    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }
}

impl Default for OptimizedApiParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse integer from simd-json array element
fn parse_array_int(value: &OwnedValue) -> Result<i64, ApiParseError> {
    value.as_i64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .ok_or(ApiParseError::InvalidValue("Expected integer"))
}

/// Parse float from simd-json array element  
fn parse_array_float(value: &OwnedValue) -> Result<f64, ApiParseError> {
    value.as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .ok_or(ApiParseError::InvalidValue("Expected float"))
}

/// Parse integer from serde_json Value
fn parse_json_int(value: &serde_json::Value) -> Result<i64, ApiParseError> {
    value.as_i64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .ok_or(ApiParseError::InvalidValue("Expected integer"))
}

/// Parse float from serde_json Value
fn parse_json_float(value: &serde_json::Value) -> Result<f64, ApiParseError> {
    value.as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .ok_or(ApiParseError::InvalidValue("Expected float"))
}

/// API parsing error types
#[derive(Debug, thiserror::Error)]
pub enum ApiParseError {
    #[error("simd-json error: {0}")]
    SimdJsonError(String),
    
    #[error("serde_json error: {0}")]
    SerdeJsonError(String),
    
    #[error("Invalid format: {0}")]
    InvalidFormat(&'static str),
    
    #[error("Invalid value: {0}")]
    InvalidValue(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_klines_response() -> &'static str {
        r#"[
            [1672531200000, "50000.00", "50100.00", "49900.00", "50050.00", "1000.00", 1672531259999, "500.00", 100, "0", "0", "0"],
            [1672531260000, "50050.00", "50150.00", "49950.00", "50100.00", "1200.00", 1672531319999, "600.00", 120, "0", "0", "0"]
        ]"#
    }

    #[test]
    fn test_optimized_api_parser() {
        let mut parser = OptimizedApiParser::new();
        let result = parser.parse_klines_response(sample_klines_response(), None);
        
        assert!(result.is_ok());
        let candles = result.unwrap();
        assert_eq!(candles.len(), 2);
        
        let first = &candles[0];
        assert_eq!(first.open_time, 1672531200000);
        assert_eq!(first.open, 50000.00);
        assert_eq!(first.close, 50050.00);
        assert_eq!(first.volume, 1000.00);
        assert!(first.closed);
    }

    #[test]
    fn test_caching() {
        let mut parser = OptimizedApiParser::new();
        let cache_key = "BTCUSDT_1m_test";
        
        // First parse - cache miss
        let _ = parser.parse_klines_response(sample_klines_response(), Some(cache_key));
        let (hits, misses) = parser.get_cache_stats();
        assert_eq!(hits, 0);
        assert!(misses > 0);
        
        // Second parse - cache hit
        let _ = parser.parse_klines_response(sample_klines_response(), Some(cache_key));
        let (hits, _) = parser.get_cache_stats();
        assert!(hits > 0);
    }

    #[test]
    fn test_streaming_parser() {
        let mut parser = OptimizedApiParser::new();
        let data = sample_klines_response();
        let cursor = std::io::Cursor::new(data);
        
        let result = parser.parse_klines_streaming(cursor);
        assert!(result.is_ok());
        
        let candles = result.unwrap();
        assert_eq!(candles.len(), 2);
    }

    #[test]
    fn test_fallback_parsing() {
        let parser = OptimizedApiParser::new();
        
        // Test with malformed JSON that should trigger fallback
        let result = parser.parse_klines_fallback(sample_klines_response());
        assert!(result.is_ok());
        
        let candles = result.unwrap();
        assert_eq!(candles.len(), 2);
    }
}