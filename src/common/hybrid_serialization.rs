//! Hybrid Serialization Architecture
//! 
//! This module implements an intelligent hybrid serialization system that selects
//! optimal serialization formats based on data characteristics and consumer requirements.
//! Combines sonic-rs (JSON), rkyv (binary), and streaming approaches for maximum performance.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use crate::volume_profile::structs::VolumeProfileData;
use crate::volume_profile::zero_copy_serialization::{VolumeProfileSerializer, ZeroCopySerializationError};
use crate::technical_analysis::rkyv_serialization::{TechnicalAnalysisSerializer, TechnicalAnalysisError};
use crate::technical_analysis::structs::IndicatorOutput;

/// Serialization format selection strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// Fast JSON with sonic-rs (best for web APIs, small-medium data)
    JsonSonic,
    /// Binary rkyv format (best for high-frequency, large datasets)
    BinaryRkyv,
    /// Streaming JSON (best for very large datasets with memory constraints)
    StreamingJson,
    /// Adaptive selection based on data characteristics
    Adaptive,
}

/// Consumer type information for format selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumerType {
    /// Web API clients expecting JSON
    WebApi,
    /// High-frequency trading systems needing maximum speed
    HighFrequencyTrading,
    /// Analytics systems processing large batches
    Analytics,
    /// Real-time dashboards needing fast updates
    RealTimeDashboard,
    /// Data export/archival systems
    DataExport,
}

/// Serialization strategy configuration
#[derive(Debug, Clone)]
pub struct SerializationStrategy {
    pub format: SerializationFormat,
    pub consumer_type: ConsumerType,
    pub size_threshold_bytes: usize,
    pub performance_target_ms: f64,
    pub memory_limit_mb: usize,
    pub backward_compatibility: bool,
}

impl Default for SerializationStrategy {
    fn default() -> Self {
        Self {
            format: SerializationFormat::Adaptive,
            consumer_type: ConsumerType::WebApi,
            size_threshold_bytes: 64 * 1024,  // 64KB threshold
            performance_target_ms: 1.0,       // <1ms target
            memory_limit_mb: 100,             // 100MB memory limit
            backward_compatibility: true,
        }
    }
}

/// Performance metrics for serialization operations
#[derive(Debug, Clone)]
pub struct HybridSerializationMetrics {
    pub operations_count: u64,
    pub json_operations: u64,
    pub binary_operations: u64,
    pub streaming_operations: u64,
    pub total_time: std::time::Duration,
    pub average_time: std::time::Duration,
    pub format_selection_time: std::time::Duration,
    pub memory_usage_bytes: usize,
}

/// Serialization errors for hybrid system
#[derive(Debug, thiserror::Error)]
pub enum HybridSerializationError {
    #[error("JSON serialization error: {0}")]
    JsonError(#[from] sonic_rs::Error),
    
    #[error("Binary serialization error: {0}")]
    BinaryError(#[from] TechnicalAnalysisError),
    
    #[error("Volume profile serialization error: {0}")]
    VolumeProfileError(#[from] ZeroCopySerializationError),
    
    #[error("Format selection error: {0}")]
    FormatSelectionError(String),
    
    #[error("Memory limit exceeded: {0} bytes")]
    MemoryLimitExceeded(usize),
    
    #[error("Streaming error: {0}")]
    StreamingError(String),
}

/// Result type for hybrid serialization
pub type HybridSerializationResult = Result<SerializedData, HybridSerializationError>;

/// Serialized data with format information
#[derive(Debug, Clone)]
pub struct SerializedData {
    pub data: Vec<u8>,
    pub format: SerializationFormat,
    pub content_type: String,
    pub size_bytes: usize,
    pub serialization_time: std::time::Duration,
}

impl SerializedData {
    /// Convert to string if format supports it
    pub fn as_string(&self) -> Result<String, HybridSerializationError> {
        match self.format {
            SerializationFormat::JsonSonic | SerializationFormat::StreamingJson => {
                String::from_utf8(self.data.clone())
                    .map_err(|e| HybridSerializationError::FormatSelectionError(
                        format!("Invalid UTF-8: {}", e)
                    ))
            }
            SerializationFormat::BinaryRkyv => {
                Err(HybridSerializationError::FormatSelectionError(
                    "Binary format cannot be converted to string".to_string()
                ))
            }
            SerializationFormat::Adaptive => {
                // Try as string first, fallback to error
                String::from_utf8(self.data.clone())
                    .map_err(|e| HybridSerializationError::FormatSelectionError(
                        format!("Adaptive format not UTF-8 compatible: {}", e)
                    ))
            }
        }
    }
}

/// Hybrid serialization system combining multiple formats
pub struct HybridSerializer {
    volume_profile_serializer: Arc<std::sync::Mutex<VolumeProfileSerializer>>,
    technical_analysis_serializer: Arc<std::sync::Mutex<TechnicalAnalysisSerializer>>,
    metrics: Arc<RwLock<HybridSerializationMetrics>>,
    format_performance_cache: Arc<RwLock<HashMap<(ConsumerType, usize), SerializationFormat>>>,
}

impl HybridSerializer {
    /// Create new hybrid serializer
    pub fn new() -> Self {
        Self {
            volume_profile_serializer: Arc::new(std::sync::Mutex::new(VolumeProfileSerializer::new())),
            technical_analysis_serializer: Arc::new(std::sync::Mutex::new(TechnicalAnalysisSerializer::new())),
            metrics: Arc::new(RwLock::new(HybridSerializationMetrics {
                operations_count: 0,
                json_operations: 0,
                binary_operations: 0,
                streaming_operations: 0,
                total_time: std::time::Duration::ZERO,
                average_time: std::time::Duration::ZERO,
                format_selection_time: std::time::Duration::ZERO,
                memory_usage_bytes: 0,
            })),
            format_performance_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Serialize volume profile with intelligent format selection
    pub fn serialize_volume_profile(
        &mut self,
        profile: &VolumeProfileData,
        strategy: &SerializationStrategy,
    ) -> HybridSerializationResult {
        let start_time = Instant::now();
        
        // Select optimal format
        let format = self.select_format_for_volume_profile(profile, strategy)?;
        
        let format_selection_time = start_time.elapsed();
        let serialize_start = Instant::now();
        
        // Perform serialization based on selected format
        let result = match format {
            SerializationFormat::JsonSonic => {
                self.serialize_volume_profile_json(profile)
            }
            SerializationFormat::BinaryRkyv => {
                Err(HybridSerializationError::FormatSelectionError(
                    "rkyv not implemented for VolumeProfile yet".to_string()
                ))
            }
            SerializationFormat::StreamingJson => {
                self.serialize_volume_profile_streaming(profile)
            }
            SerializationFormat::Adaptive => {
                // Should not reach here as adaptive is resolved in select_format
                self.serialize_volume_profile_json(profile)
            }
        }?;

        let serialization_time = serialize_start.elapsed();
        
        // Update metrics
        self.update_metrics(format, start_time.elapsed(), format_selection_time);
        
        // Cache format decision for similar data sizes
        self.cache_format_decision(strategy.consumer_type, result.size_bytes, format);

        Ok(SerializedData {
            data: result.data,
            format,
            content_type: match format {
                SerializationFormat::JsonSonic | SerializationFormat::StreamingJson => 
                    "application/json".to_string(),
                SerializationFormat::BinaryRkyv => 
                    "application/x-rkyv".to_string(),
                SerializationFormat::Adaptive => 
                    "application/json".to_string(),
            },
            size_bytes: result.size_bytes,
            serialization_time,
        })
    }

    /// Serialize technical analysis data with intelligent format selection
    pub fn serialize_technical_analysis(
        &mut self,
        output: &IndicatorOutput,
        strategy: &SerializationStrategy,
    ) -> HybridSerializationResult {
        let start_time = Instant::now();
        
        // Select optimal format
        let format = self.select_format_for_technical_analysis(output, strategy)?;
        
        let format_selection_time = start_time.elapsed();
        let serialize_start = Instant::now();
        
        // Perform serialization based on selected format
        let result = match format {
            SerializationFormat::JsonSonic => {
                self.serialize_technical_analysis_json(output)
            }
            SerializationFormat::BinaryRkyv => {
                self.serialize_technical_analysis_binary(output)
            }
            SerializationFormat::StreamingJson => {
                // For single indicator, streaming is same as regular JSON
                self.serialize_technical_analysis_json(output)
            }
            SerializationFormat::Adaptive => {
                // Should not reach here as adaptive is resolved in select_format
                self.serialize_technical_analysis_json(output)
            }
        }?;

        let serialization_time = serialize_start.elapsed();
        
        // Update metrics
        self.update_metrics(format, start_time.elapsed(), format_selection_time);
        
        // Cache format decision
        self.cache_format_decision(strategy.consumer_type, result.size_bytes, format);

        Ok(SerializedData {
            data: result.data,
            format,
            content_type: match format {
                SerializationFormat::JsonSonic | SerializationFormat::StreamingJson => 
                    "application/json".to_string(),
                SerializationFormat::BinaryRkyv => 
                    "application/x-rkyv".to_string(),
                SerializationFormat::Adaptive => 
                    "application/json".to_string(),
            },
            size_bytes: result.size_bytes,
            serialization_time,
        })
    }

    /// Select optimal format for volume profile data
    fn select_format_for_volume_profile(
        &self,
        profile: &VolumeProfileData,
        strategy: &SerializationStrategy,
    ) -> Result<SerializationFormat, HybridSerializationError> {
        match strategy.format {
            SerializationFormat::Adaptive => {
                // Check cache first
                let estimated_size = self.estimate_volume_profile_size(profile);
                if let Some(cached_format) = self.get_cached_format_decision(strategy.consumer_type, estimated_size) {
                    return Ok(cached_format);
                }

                // Intelligent selection based on data characteristics
                let format = match strategy.consumer_type {
                    ConsumerType::WebApi | ConsumerType::RealTimeDashboard => {
                        if estimated_size > strategy.size_threshold_bytes {
                            SerializationFormat::StreamingJson
                        } else {
                            SerializationFormat::JsonSonic
                        }
                    }
                    ConsumerType::HighFrequencyTrading => {
                        // Always prefer binary for HFT
                        SerializationFormat::BinaryRkyv
                    }
                    ConsumerType::Analytics | ConsumerType::DataExport => {
                        if estimated_size > strategy.size_threshold_bytes * 10 {
                            SerializationFormat::StreamingJson
                        } else {
                            SerializationFormat::JsonSonic
                        }
                    }
                };

                Ok(format)
            }
            other => Ok(other),
        }
    }

    /// Select optimal format for technical analysis data
    fn select_format_for_technical_analysis(
        &self,
        output: &IndicatorOutput,
        strategy: &SerializationStrategy,
    ) -> Result<SerializationFormat, HybridSerializationError> {
        match strategy.format {
            SerializationFormat::Adaptive => {
                // Check cache first
                let estimated_size = self.estimate_technical_analysis_size(output);
                if let Some(cached_format) = self.get_cached_format_decision(strategy.consumer_type, estimated_size) {
                    return Ok(cached_format);
                }

                // Intelligent selection
                let format = match strategy.consumer_type {
                    ConsumerType::WebApi | ConsumerType::RealTimeDashboard => {
                        SerializationFormat::JsonSonic
                    }
                    ConsumerType::HighFrequencyTrading => {
                        SerializationFormat::BinaryRkyv
                    }
                    ConsumerType::Analytics | ConsumerType::DataExport => {
                        if estimated_size > strategy.size_threshold_bytes {
                            SerializationFormat::BinaryRkyv
                        } else {
                            SerializationFormat::JsonSonic
                        }
                    }
                };

                Ok(format)
            }
            other => Ok(other),
        }
    }

    /// Serialize volume profile as JSON using sonic-rs
    fn serialize_volume_profile_json(&mut self, profile: &VolumeProfileData) -> Result<IntermediateResult, HybridSerializationError> {
        let mut serializer = self.volume_profile_serializer.lock().unwrap();
        let json_string = serializer.serialize_volume_profile(profile)?;
        let size = json_string.len();
        Ok(IntermediateResult {
            data: json_string.into_bytes(),
            size_bytes: size,
        })
    }

    /// Serialize volume profile as streaming JSON
    fn serialize_volume_profile_streaming(&mut self, profile: &VolumeProfileData) -> Result<IntermediateResult, HybridSerializationError> {
        // For now, streaming is the same as regular JSON for single profiles
        // In a full implementation, this would use a streaming JSON writer
        self.serialize_volume_profile_json(profile)
    }

    /// Serialize technical analysis as JSON using sonic-rs
    fn serialize_technical_analysis_json(&mut self, output: &IndicatorOutput) -> Result<IntermediateResult, HybridSerializationError> {
        let json_string = sonic_rs::to_string(output)?;
        let size = json_string.len();
        Ok(IntermediateResult {
            data: json_string.into_bytes(),
            size_bytes: size,
        })
    }

    /// Serialize technical analysis as binary using rkyv
    fn serialize_technical_analysis_binary(&mut self, output: &IndicatorOutput) -> Result<IntermediateResult, HybridSerializationError> {
        let mut serializer = self.technical_analysis_serializer.lock().unwrap();
        let binary_data = serializer.serialize_indicator_output(output)?;
        let size = binary_data.len();
        Ok(IntermediateResult {
            data: binary_data,
            size_bytes: size,
        })
    }

    /// Estimate serialized size of volume profile data
    fn estimate_volume_profile_size(&self, profile: &VolumeProfileData) -> usize {
        // Rough estimation based on data structure
        let base_size = 500; // Base metadata
        let price_levels_size = profile.price_levels.len() * 150; // ~150 bytes per level
        base_size + price_levels_size
    }

    /// Estimate serialized size of technical analysis data
    fn estimate_technical_analysis_size(&self, _output: &IndicatorOutput) -> usize {
        // Most technical analysis outputs are relatively small
        2048 // 2KB estimate
    }

    /// Get cached format decision
    fn get_cached_format_decision(&self, consumer_type: ConsumerType, size: usize) -> Option<SerializationFormat> {
        let cache = self.format_performance_cache.read().unwrap();
        // Use size buckets for better cache hit rate
        let size_bucket = (size / 1024) * 1024; // Round to nearest KB
        cache.get(&(consumer_type, size_bucket)).copied()
    }

    /// Cache format decision for future use
    fn cache_format_decision(&self, consumer_type: ConsumerType, size: usize, format: SerializationFormat) {
        let mut cache = self.format_performance_cache.write().unwrap();
        let size_bucket = (size / 1024) * 1024; // Round to nearest KB
        cache.insert((consumer_type, size_bucket), format);
    }

    /// Update performance metrics
    fn update_metrics(&self, format: SerializationFormat, total_time: std::time::Duration, format_selection_time: std::time::Duration) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.operations_count += 1;
        metrics.total_time += total_time;
        metrics.format_selection_time += format_selection_time;

        match format {
            SerializationFormat::JsonSonic | SerializationFormat::StreamingJson => {
                metrics.json_operations += 1;
            }
            SerializationFormat::BinaryRkyv => {
                metrics.binary_operations += 1;
            }
            _ => {}
        }

        metrics.average_time = metrics.total_time / metrics.operations_count as u32;
    }

    /// Get performance metrics
    pub fn get_metrics(&self) -> HybridSerializationMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Reset performance metrics
    pub fn reset_metrics(&self) {
        let mut metrics = self.metrics.write().unwrap();
        *metrics = HybridSerializationMetrics {
            operations_count: 0,
            json_operations: 0,
            binary_operations: 0,
            streaming_operations: 0,
            total_time: std::time::Duration::ZERO,
            average_time: std::time::Duration::ZERO,
            format_selection_time: std::time::Duration::ZERO,
            memory_usage_bytes: 0,
        };
    }
}

impl Default for HybridSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Intermediate result for internal serialization operations
struct IntermediateResult {
    data: Vec<u8>,
    size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_profile::structs::{PriceLevelData, ValueArea};
    use crate::technical_analysis::structs::TrendDirection;
    use rust_decimal_macros::dec;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_test_volume_profile() -> VolumeProfileData {
        VolumeProfileData {
            date: "2023-08-10".to_string(),
            total_volume: dec!(1000000),
            vwap: dec!(50000.0),
            poc: dec!(50050.0),
            price_increment: dec!(0.01),
            min_price: dec!(49900.0),
            max_price: dec!(50100.0),
            candle_count: 100,
            last_updated: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            price_levels: vec![
                PriceLevelData {
                    price: dec!(50000.0),
                    volume: dec!(100000),
                    percentage: dec!(10.0),
                    candle_count: 50,
                },
            ],
            value_area: ValueArea {
                high: dec!(50075.0),
                low: dec!(50025.0),
                volume_percentage: dec!(70.0),
                volume: dec!(700000),
            },
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
    fn test_adaptive_format_selection_web_api() {
        let mut serializer = HybridSerializer::new();
        let profile = create_test_volume_profile();
        let strategy = SerializationStrategy {
            format: SerializationFormat::Adaptive,
            consumer_type: ConsumerType::WebApi,
            ..Default::default()
        };

        let result = serializer.serialize_volume_profile(&profile, &strategy).unwrap();
        
        // Web API should prefer JSON
        assert_eq!(result.format, SerializationFormat::JsonSonic);
        assert_eq!(result.content_type, "application/json");
    }

    #[test]
    fn test_adaptive_format_selection_hft() {
        let mut serializer = HybridSerializer::new();
        let output = create_test_indicator_output();
        let strategy = SerializationStrategy {
            format: SerializationFormat::Adaptive,
            consumer_type: ConsumerType::HighFrequencyTrading,
            ..Default::default()
        };

        let result = serializer.serialize_technical_analysis(&output, &strategy).unwrap();
        
        // HFT should prefer binary
        assert_eq!(result.format, SerializationFormat::BinaryRkyv);
        assert_eq!(result.content_type, "application/x-rkyv");
    }

    #[test]
    fn test_performance_metrics() {
        let mut serializer = HybridSerializer::new();
        let output = create_test_indicator_output();
        let strategy = SerializationStrategy::default();

        // Perform multiple serializations
        for _ in 0..10 {
            let _ = serializer.serialize_technical_analysis(&output, &strategy);
        }

        let metrics = serializer.get_metrics();
        assert_eq!(metrics.operations_count, 10);
        assert!(metrics.average_time > std::time::Duration::ZERO);
        assert!(metrics.json_operations > 0);
    }

    #[test]
    fn test_format_caching() {
        let mut serializer = HybridSerializer::new();
        let output = create_test_indicator_output();
        let strategy = SerializationStrategy {
            format: SerializationFormat::Adaptive,
            consumer_type: ConsumerType::WebApi,
            ..Default::default()
        };

        // First serialization should populate cache
        let result1 = serializer.serialize_technical_analysis(&output, &strategy).unwrap();
        
        // Second serialization should use cached decision
        let result2 = serializer.serialize_technical_analysis(&output, &strategy).unwrap();
        
        // Both should use the same format
        assert_eq!(result1.format, result2.format);
    }

    #[test]
    fn test_serialized_data_as_string() {
        let mut serializer = HybridSerializer::new();
        let output = create_test_indicator_output();
        let strategy = SerializationStrategy {
            format: SerializationFormat::JsonSonic,
            ..Default::default()
        };

        let result = serializer.serialize_technical_analysis(&output, &strategy).unwrap();
        let json_string = result.as_string().unwrap();
        
        // Should be valid JSON
        assert!(json_string.contains("BTCUSDT"));
        assert!(json_string.contains("timestamp"));
    }
}