//! High-performance serialization utilities with JSON compatibility
//! 
//! This module provides a unified interface for ultra-high-performance serialization
//! using sonic-rs and rkyv while maintaining backward compatibility with existing
//! JSON API consumers.

use serde::{Serialize, Deserialize};
use serde::de::Error as _;  // For serde_json::Error::custom
use sonic_rs;
use crate::volume_profile::zero_copy_serialization::{
    VolumeProfileSerializer, ZeroCopySerializationError, SerializationMetrics
};
use crate::technical_analysis::rkyv_serialization::{
    TechnicalAnalysisSerializer, RkyvSerializationError, RkyvSerializationMetrics
};
use crate::volume_profile::structs::VolumeProfileData;
use crate::technical_analysis::structs::IndicatorOutput;

/// Unified serialization error type
#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
    #[error("JSON serialization error: {0}")]
    JsonError(#[from] sonic_rs::Error),
    
    #[error("Zero-copy serialization error: {0}")]
    ZeroCopyError(#[from] ZeroCopySerializationError),
    
    #[error("Binary serialization error: {0}")]
    BinaryError(#[from] RkyvSerializationError),
    
    #[error("Serde JSON fallback error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
}

/// Serialization format selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// High-performance JSON using sonic-rs
    Json,
    /// Ultra-fast binary using rkyv
    Binary,
    /// Automatic selection based on data size and type
    Auto,
}

/// Combined performance metrics for all serialization types
#[derive(Debug, Clone)]
pub struct UnifiedSerializationMetrics {
    pub json_metrics: SerializationMetrics,
    pub binary_metrics: RkyvSerializationMetrics,
    pub total_operations: u64,
    pub json_usage_percent: f64,
    pub binary_usage_percent: f64,
}

/// High-performance serialization utility with intelligent format selection
pub struct HighPerformanceSerializer {
    /// Sonic-rs based volume profile serializer
    volume_serializer: VolumeProfileSerializer,
    /// rkyv based technical analysis serializer
    technical_serializer: TechnicalAnalysisSerializer,
    /// Performance tracking
    total_operations: u64,
    json_operations: u64,
    binary_operations: u64,
    /// Automatic format selection thresholds
    binary_threshold_bytes: usize,
    complexity_threshold: usize,
}

impl HighPerformanceSerializer {
    /// Create new high-performance serializer with intelligent defaults
    pub fn new() -> Self {
        Self {
            volume_serializer: VolumeProfileSerializer::new(),
            technical_serializer: TechnicalAnalysisSerializer::new(),
            total_operations: 0,
            json_operations: 0,
            binary_operations: 0,
            binary_threshold_bytes: 10 * 1024, // Use binary for >10KB data
            complexity_threshold: 100, // Use binary for >100 price levels
        }
    }
    
    /// Serialize VolumeProfileData with format selection
    pub fn serialize_volume_profile(
        &mut self, 
        profile: &VolumeProfileData, 
        format: SerializationFormat
    ) -> Result<SerializationOutput, SerializationError> {
        self.total_operations += 1;
        
        let selected_format = match format {
            SerializationFormat::Auto => {
                self.select_optimal_format_for_volume_profile(profile)
            },
            other => other,
        };
        
        match selected_format {
            SerializationFormat::Json => {
                self.json_operations += 1;
                let json_string = self.volume_serializer.serialize_volume_profile(profile)?;
                Ok(SerializationOutput::Json(json_string))
            },
            SerializationFormat::Binary => {
                self.binary_operations += 1;
                // For volume profiles, we use JSON format with sonic-rs optimization
                // rkyv is primarily for technical analysis due to its complexity
                let json_string = self.volume_serializer.serialize_volume_profile(profile)?;
                Ok(SerializationOutput::Json(json_string))
            },
            SerializationFormat::Auto => unreachable!("Auto should be resolved above"),
        }
    }
    
    /// Serialize IndicatorOutput with format selection
    pub fn serialize_indicator_output(
        &mut self,
        output: &IndicatorOutput,
        format: SerializationFormat
    ) -> Result<SerializationOutput, SerializationError> {
        self.total_operations += 1;
        
        let selected_format = match format {
            SerializationFormat::Auto => {
                self.select_optimal_format_for_indicator_output(output)
            },
            other => other,
        };
        
        match selected_format {
            SerializationFormat::Json => {
                self.json_operations += 1;
                let json_string = sonic_rs::to_string(output)?;
                Ok(SerializationOutput::Json(json_string))
            },
            SerializationFormat::Binary => {
                self.binary_operations += 1;
                let binary_data = self.technical_serializer.serialize_indicator_output(output)?;
                Ok(SerializationOutput::Binary(binary_data))
            },
            SerializationFormat::Auto => unreachable!("Auto should be resolved above"),
        }
    }
    
    /// Deserialize IndicatorOutput from binary format
    pub fn deserialize_indicator_output(
        &mut self,
        data: &[u8]
    ) -> Result<IndicatorOutput, SerializationError> {
        Ok(self.technical_serializer.deserialize_indicator_output(data)?)
    }
    
    /// Serialize any JSON-compatible type using sonic-rs for maximum performance
    pub fn serialize_json<T>(&mut self, value: &T) -> Result<String, SerializationError>
    where
        T: Serialize,
    {
        self.total_operations += 1;
        self.json_operations += 1;
        
        // Try sonic-rs first for maximum performance
        match sonic_rs::to_string(value) {
            Ok(json) => Ok(json),
            Err(_) => {
                // Fallback to serde_json for complex types
                Ok(serde_json::to_string(value)?)
            }
        }
    }
    
    /// Deserialize any JSON-compatible type using sonic-rs for maximum performance
    pub fn deserialize_json<T>(&mut self, json: &str) -> Result<T, SerializationError>
    where
        T: for<'de> Deserialize<'de>,
    {
        // Try sonic-rs first for maximum performance
        match sonic_rs::from_str(json) {
            Ok(value) => Ok(value),
            Err(_) => {
                // Fallback to serde_json for complex types
                Ok(serde_json::from_str(json)?)
            }
        }
    }
    
    /// Get comprehensive performance metrics
    pub fn get_performance_metrics(&self) -> UnifiedSerializationMetrics {
        let json_metrics = self.volume_serializer.get_performance_metrics();
        let binary_metrics = self.technical_serializer.get_performance_metrics();
        
        UnifiedSerializationMetrics {
            json_metrics,
            binary_metrics,
            total_operations: self.total_operations,
            json_usage_percent: if self.total_operations > 0 {
                (self.json_operations as f64 / self.total_operations as f64) * 100.0
            } else {
                0.0
            },
            binary_usage_percent: if self.total_operations > 0 {
                (self.binary_operations as f64 / self.total_operations as f64) * 100.0
            } else {
                0.0
            },
        }
    }
    
    /// Configure automatic format selection thresholds
    pub fn configure_auto_selection(&mut self, binary_threshold_bytes: usize, complexity_threshold: usize) {
        self.binary_threshold_bytes = binary_threshold_bytes;
        self.complexity_threshold = complexity_threshold;
    }
    
    /// Select optimal format for volume profile data
    fn select_optimal_format_for_volume_profile(&self, profile: &VolumeProfileData) -> SerializationFormat {
        // For volume profiles, we prefer JSON with sonic-rs optimization
        // since the API consumers expect JSON format
        
        if profile.price_levels.len() > self.complexity_threshold {
            // High complexity - use optimized JSON
            SerializationFormat::Json
        } else {
            // Normal complexity - use standard JSON
            SerializationFormat::Json
        }
    }
    
    /// Select optimal format for indicator output
    fn select_optimal_format_for_indicator_output(&self, output: &IndicatorOutput) -> SerializationFormat {
        // For technical analysis, binary can provide significant advantages
        // while JSON maintains compatibility
        
        let has_volume_profile = match output.volume_profile {
            #[cfg(feature = "volume_profile")]
            Some(_) => true,
            _ => false,
        };
        
        if has_volume_profile || output.volume_quantiles.is_some() {
            // Complex data - binary serialization provides better performance
            SerializationFormat::Binary
        } else {
            // Simple data - JSON for compatibility
            SerializationFormat::Json
        }
    }
}

impl Default for HighPerformanceSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Serialization output with format indication
#[derive(Debug, Clone)]
pub enum SerializationOutput {
    /// JSON string output (compatible with existing APIs)
    Json(String),
    /// Binary data output (ultra-high performance)
    Binary(Vec<u8>),
}

impl SerializationOutput {
    /// Get the serialized data as bytes
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            SerializationOutput::Json(json) => json.as_bytes(),
            SerializationOutput::Binary(data) => data.as_slice(),
        }
    }
    
    /// Get the data size in bytes
    pub fn size_bytes(&self) -> usize {
        match self {
            SerializationOutput::Json(json) => json.len(),
            SerializationOutput::Binary(data) => data.len(),
        }
    }
    
    /// Check if output is JSON format (API compatible)
    pub fn is_json(&self) -> bool {
        matches!(self, SerializationOutput::Json(_))
    }
    
    /// Check if output is binary format (ultra-high performance)
    pub fn is_binary(&self) -> bool {
        matches!(self, SerializationOutput::Binary(_))
    }
    
    /// Convert to JSON string if possible, otherwise error
    pub fn into_json(self) -> Result<String, SerializationError> {
        match self {
            SerializationOutput::Json(json) => Ok(json),
            SerializationOutput::Binary(_) => Err(SerializationError::SerdeJsonError(
                serde_json::Error::custom("Cannot convert binary data to JSON string")
            )),
        }
    }
    
    /// Convert to binary data if possible, otherwise error
    pub fn into_binary(self) -> Result<Vec<u8>, SerializationError> {
        match self {
            SerializationOutput::Json(json) => Ok(json.into_bytes()),
            SerializationOutput::Binary(data) => Ok(data),
        }
    }
}

/// Convenience functions for backward compatibility
pub mod compatibility {
    use super::*;
    use std::sync::Mutex;
    use std::sync::LazyLock;
    
    /// Global serializer instance for backward compatibility
    static GLOBAL_SERIALIZER: LazyLock<Mutex<HighPerformanceSerializer>> = 
        LazyLock::new(|| Mutex::new(HighPerformanceSerializer::new()));
    
    /// Serialize VolumeProfileData to JSON (backward compatible)
    pub fn serialize_volume_profile_json(profile: &VolumeProfileData) -> Result<String, SerializationError> {
        let mut serializer = GLOBAL_SERIALIZER.lock().unwrap();
        let output = serializer.serialize_volume_profile(profile, SerializationFormat::Json)?;
        output.into_json()
    }
    
    /// Serialize IndicatorOutput to JSON (backward compatible)  
    pub fn serialize_indicator_output_json(output: &IndicatorOutput) -> Result<String, SerializationError> {
        let mut serializer = GLOBAL_SERIALIZER.lock().unwrap();
        let result = serializer.serialize_indicator_output(output, SerializationFormat::Json)?;
        result.into_json()
    }
    
    /// Serialize any type to JSON using high-performance sonic-rs
    pub fn serialize_json<T>(value: &T) -> Result<String, SerializationError>
    where
        T: Serialize,
    {
        let mut serializer = GLOBAL_SERIALIZER.lock().unwrap();
        serializer.serialize_json(value)
    }
    
    /// Get current serialization performance metrics
    pub fn get_global_metrics() -> UnifiedSerializationMetrics {
        let serializer = GLOBAL_SERIALIZER.lock().unwrap();
        serializer.get_performance_metrics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::technical_analysis::structs::{IndicatorOutput, TrendDirection};
    use rust_decimal_macros::dec;
    use std::time::Instant;
    
    fn create_test_volume_profile() -> VolumeProfileData {
        use crate::volume_profile::structs::{PriceLevelData, ValueArea};
        
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
    
    fn create_test_indicator_output() -> IndicatorOutput {
        IndicatorOutput {
            symbol: "BTCUSDT".to_string(),
            timestamp: 1736985600000,
            close_5m: Some(45000.0),
            close_15m: Some(44980.0),
            close_60m: Some(44950.0),
            close_4h: Some(44900.0),
            ema21_1min: Some(44995.0),
            ema89_1min: Some(44985.0),
            ema89_5min: Some(44975.0),
            ema89_15min: Some(44965.0),
            ema89_1h: Some(44955.0),
            ema89_4h: Some(44945.0),
            trend_1min: TrendDirection::Buy,
            trend_5min: TrendDirection::Buy,
            trend_15min: TrendDirection::Neutral,
            trend_1h: TrendDirection::Neutral,
            trend_4h: TrendDirection::Sell,
            max_volume: Some(1500.0),
            max_volume_price: Some(45020.0),
            max_volume_time: Some("2025-01-15T12:30:00Z".to_string()),
            max_volume_trend: Some(TrendDirection::Buy),
            volume_quantiles: None,
            #[cfg(feature = "volume_profile")]
            volume_profile: None,
            #[cfg(not(feature = "volume_profile"))]
            volume_profile: (),
        }
    }
    
    #[test]
    fn test_unified_serialization() {
        let mut serializer = HighPerformanceSerializer::new();
        
        // Test volume profile serialization
        let profile = create_test_volume_profile();
        let result = serializer.serialize_volume_profile(&profile, SerializationFormat::Json);
        assert!(result.is_ok());
        assert!(result.unwrap().is_json());
        
        // Test indicator output serialization
        let output = create_test_indicator_output();
        let result = serializer.serialize_indicator_output(&output, SerializationFormat::Json);
        assert!(result.is_ok());
        assert!(result.unwrap().is_json());
    }
    
    #[test]
    fn test_binary_serialization_roundtrip() {
        let mut serializer = HighPerformanceSerializer::new();
        let original = create_test_indicator_output();
        
        // Serialize to binary
        let serialized = serializer.serialize_indicator_output(&original, SerializationFormat::Binary).unwrap();
        let binary_data = serialized.into_binary().unwrap();
        
        // Deserialize back
        let deserialized = serializer.deserialize_indicator_output(&binary_data).unwrap();
        
        // Verify roundtrip accuracy
        assert_eq!(original.symbol, deserialized.symbol);
        assert_eq!(original.timestamp, deserialized.timestamp);
        assert_eq!(original.close_5m, deserialized.close_5m);
        assert_eq!(original.trend_1min, deserialized.trend_1min);
    }
    
    #[test]
    fn test_automatic_format_selection() {
        let mut serializer = HighPerformanceSerializer::new();
        
        // Test simple indicator output (should prefer JSON)
        let simple_output = create_test_indicator_output();
        let _result = serializer.serialize_indicator_output(&simple_output, SerializationFormat::Auto).unwrap();
        // Auto selection logic may choose either format based on complexity
        
        // Test volume profile (should prefer JSON for API compatibility)
        let profile = create_test_volume_profile();
        let result = serializer.serialize_volume_profile(&profile, SerializationFormat::Auto).unwrap();
        assert!(result.is_json()); // Volume profiles should always be JSON for API compatibility
    }
    
    #[test]
    fn test_performance_metrics() {
        let mut serializer = HighPerformanceSerializer::new();
        
        // Perform some operations
        let profile = create_test_volume_profile();
        let _ = serializer.serialize_volume_profile(&profile, SerializationFormat::Json);
        
        let output = create_test_indicator_output();
        let _ = serializer.serialize_indicator_output(&output, SerializationFormat::Json);
        
        let metrics = serializer.get_performance_metrics();
        assert!(metrics.total_operations >= 2);
        assert!(metrics.json_usage_percent > 0.0);
    }
    
    #[test]
    fn test_compatibility_functions() {
        let profile = create_test_volume_profile();
        let result = compatibility::serialize_volume_profile_json(&profile);
        assert!(result.is_ok());
        
        let output = create_test_indicator_output();
        let result = compatibility::serialize_indicator_output_json(&output);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_serialization_performance_benchmark() {
        let mut serializer = HighPerformanceSerializer::new();
        let output = create_test_indicator_output();
        
        // Warm-up
        for _ in 0..10 {
            let _ = serializer.serialize_indicator_output(&output, SerializationFormat::Json);
        }
        
        // Performance test
        let start = Instant::now();
        let iterations = 1000;
        
        for _ in 0..iterations {
            let _ = serializer.serialize_indicator_output(&output, SerializationFormat::Json);
        }
        
        let elapsed = start.elapsed();
        let avg_time = elapsed / iterations;
        
        println!("Unified serialization average time: {:?}", avg_time);
        
        // Should be faster than baseline performance
        assert!(avg_time < std::time::Duration::from_micros(50));
        
        let metrics = serializer.get_performance_metrics();
        println!("Unified metrics: {:?}", metrics);
    }
}