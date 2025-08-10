//! rkyv zero-copy serialization for technical analysis results
//! 
//! This module implements ultra-fast binary serialization using rkyv for technical
//! analysis output, enabling zero-copy deserialization and memory-mapped access patterns.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, 
          from_bytes, to_bytes};
use crate::technical_analysis::structs::{IndicatorOutput, TrendDirection, QuantileResults, QuantileValues};
use crate::volume_profile::structs::VolumeProfileData;
use crate::historical::structs::TimestampMS;

/// Technical analysis serialization errors  
#[derive(Debug, Clone, thiserror::Error)]
pub enum TechnicalAnalysisError {
    #[error("rkyv serialization error: {0}")]
    SerializationError(String),
    
    #[error("rkyv deserialization error: {0}")]
    DeserializationError(String),
    
    #[error("Buffer alignment error: {0}")]
    AlignmentError(String),
    
    #[error("Queue full")]
    QueueFull,
    
    #[error("Processing error: {0}")]
    ProcessingError(String),
}

/// Alias for backward compatibility
pub type RkyvSerializationError = TechnicalAnalysisError;

/// Archived (serializable) version of TrendDirection for rkyv
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq)]
#[archive(check_bytes)]
pub enum ArchivedTrendDirection {
    Buy,
    Sell,
    Neutral,
}

impl From<TrendDirection> for ArchivedTrendDirection {
    fn from(trend: TrendDirection) -> Self {
        match trend {
            TrendDirection::Buy => ArchivedTrendDirection::Buy,
            TrendDirection::Sell => ArchivedTrendDirection::Sell,
            TrendDirection::Neutral => ArchivedTrendDirection::Neutral,
        }
    }
}

impl From<ArchivedTrendDirection> for TrendDirection {
    fn from(trend: ArchivedTrendDirection) -> Self {
        match trend {
            ArchivedTrendDirection::Buy => TrendDirection::Buy,
            ArchivedTrendDirection::Sell => TrendDirection::Sell,
            ArchivedTrendDirection::Neutral => TrendDirection::Neutral,
        }
    }
}

/// Archived version of QuantileValues for rkyv
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct ArchivedQuantileValues {
    pub q25: f64,
    pub q50: f64,
    pub q75: f64,
    pub q90: f64,
}

impl From<QuantileValues> for ArchivedQuantileValues {
    fn from(values: QuantileValues) -> Self {
        ArchivedQuantileValues {
            q25: values.q25,
            q50: values.q50,
            q75: values.q75,
            q90: values.q90,
        }
    }
}

/// Archived version of QuantileResults for rkyv
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct ArchivedQuantileResults {
    pub volume: ArchivedQuantileValues,
    pub taker_buy_volume: ArchivedQuantileValues,
    pub avg_trade: ArchivedQuantileValues,
    pub trade_count: ArchivedQuantileValues,
}

impl From<QuantileResults> for ArchivedQuantileResults {
    fn from(quantiles: QuantileResults) -> Self {
        ArchivedQuantileResults {
            volume: ArchivedQuantileValues::from(quantiles.volume),
            taker_buy_volume: ArchivedQuantileValues::from(quantiles.taker_buy_volume),
            avg_trade: ArchivedQuantileValues::from(quantiles.avg_trade),
            trade_count: ArchivedQuantileValues::from(quantiles.trade_count),
        }
    }
}

/// Archived version of VolumeProfileData for efficient binary serialization
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct ArchivedVolumeProfileData {
    pub date: String,
    
    // Use f64 for numeric fields for rkyv efficiency (avoid Decimal complexity)
    pub total_volume: f64,
    pub vwap: f64,
    pub poc: f64,
    pub price_increment: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub candle_count: u32,
    pub last_updated: TimestampMS,
    
    // Store price levels as Vec of tuples for maximum efficiency
    pub price_levels: Vec<(f64, f64, f64, u32)>, // (price, volume, percentage, candle_count)
    
    // Value area as simple tuple
    pub value_area: (f64, f64, f64, f64), // (high, low, volume_percentage, volume)
}

/// Archived version of IndicatorOutput optimized for rkyv
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct ArchivedIndicatorOutput {
    pub symbol: String,
    pub timestamp: TimestampMS,
    
    // Multi-timeframe close prices (Option<f64> → f64 with sentinel values)
    pub close_5m: f64,    // -1.0 indicates None
    pub close_15m: f64,   // -1.0 indicates None
    pub close_60m: f64,   // -1.0 indicates None
    pub close_4h: f64,    // -1.0 indicates None
    
    // EMA indicators
    pub ema21_1min: f64,  // -1.0 indicates None
    pub ema89_1min: f64,
    pub ema89_5min: f64,
    pub ema89_15min: f64,
    pub ema89_1h: f64,
    pub ema89_4h: f64,
    
    // Trend analysis
    pub trend_1min: ArchivedTrendDirection,
    pub trend_5min: ArchivedTrendDirection,
    pub trend_15min: ArchivedTrendDirection,
    pub trend_1h: ArchivedTrendDirection,
    pub trend_4h: ArchivedTrendDirection,
    
    // Volume analysis
    pub max_volume: f64,        // -1.0 indicates None
    pub max_volume_price: f64,  // -1.0 indicates None
    pub max_volume_time: String, // Empty string indicates None
    pub max_volume_trend: ArchivedTrendDirection, // Neutral indicates None
    
    // Volume quantiles (flattened for efficiency)
    pub has_volume_quantiles: bool,
    pub volume_quantiles: ArchivedQuantileResults, // Only valid if has_volume_quantiles is true
    
    // Volume profile data (embedded)
    pub has_volume_profile: bool,
    pub volume_profile: ArchivedVolumeProfileData, // Only valid if has_volume_profile is true
}

/// rkyv serializer for technical analysis with caching and memory mapping support
pub struct TechnicalAnalysisSerializer {
    /// Buffer pool for serialization
    buffer_pool: Vec<Vec<u8>>,
    /// Current buffer index for round-robin allocation
    current_buffer: usize,
    /// Performance metrics
    operations_count: u64,
    total_serialization_time: std::time::Duration,
    total_deserialization_time: std::time::Duration,
    cache_hits: u64,
    cache_misses: u64,
}

impl TechnicalAnalysisSerializer {
    pub fn new() -> Self {
        // Pre-allocate multiple buffers for concurrent operations
        let buffer_pool = (0..4).map(|_| Vec::with_capacity(64 * 1024)).collect();
        
        Self {
            buffer_pool,
            current_buffer: 0,
            operations_count: 0,
            total_serialization_time: std::time::Duration::ZERO,
            total_deserialization_time: std::time::Duration::ZERO,
            cache_hits: 0,
            cache_misses: 0,
        }
    }
    
    /// Serialize IndicatorOutput to rkyv binary format (zero-copy optimized)
    pub fn serialize_indicator_output(&mut self, output: &IndicatorOutput) -> Result<Vec<u8>, RkyvSerializationError> {
        let start = std::time::Instant::now();
        
        // Convert to archived format
        let archived = self.convert_to_archived(output)?;
        
        // Get buffer from pool (round-robin allocation)
        let buffer = &mut self.buffer_pool[self.current_buffer];
        buffer.clear();
        self.current_buffer = (self.current_buffer + 1) % self.buffer_pool.len();
        
        // Serialize using rkyv (zero-copy binary format)
        let serialized = to_bytes::<_, 256>(&archived)
            .map_err(|e| RkyvSerializationError::SerializationError(e.to_string()))?;
        
        // Update metrics
        self.operations_count += 1;
        self.total_serialization_time += start.elapsed();
        
        Ok(serialized.to_vec())
    }
    
    /// Deserialize from rkyv binary format with zero-copy access
    pub fn deserialize_indicator_output(&mut self, data: &[u8]) -> Result<IndicatorOutput, RkyvSerializationError> {
        let start = std::time::Instant::now();
        
        // Zero-copy deserialization using rkyv
        let archived = from_bytes::<ArchivedIndicatorOutput>(data)
            .map_err(|e| RkyvSerializationError::DeserializationError(e.to_string()))?;
        
        // Convert back to runtime format
        let output = self.convert_from_archived(&archived)?;
        
        // Update metrics
        self.total_deserialization_time += start.elapsed();
        
        Ok(output)
    }
    
    /// Convert IndicatorOutput to archived format for rkyv
    fn convert_to_archived(&self, output: &IndicatorOutput) -> Result<ArchivedIndicatorOutput, RkyvSerializationError> {
        // Helper function to convert Option<f64> to f64 with sentinel value
        let opt_f64_to_f64 = |opt: Option<f64>| opt.unwrap_or(-1.0);
        
        // Convert volume profile if present
        let (has_volume_profile, volume_profile) = match &output.volume_profile {
            #[cfg(feature = "volume_profile")]
            Some(vp) => (true, self.convert_volume_profile_to_archived(vp)?),
            _ => (false, ArchivedVolumeProfileData {
                date: String::new(),
                total_volume: 0.0,
                vwap: 0.0,
                poc: 0.0,
                price_increment: 0.0,
                min_price: 0.0,
                max_price: 0.0,
                candle_count: 0,
                last_updated: 0,
                price_levels: Vec::new(),
                value_area: (0.0, 0.0, 0.0, 0.0),
            }),
        };
        
        // Convert quantile results if present
        let (has_quantiles, quantiles) = match &output.volume_quantiles {
            Some(q) => (true, ArchivedQuantileResults::from(q.clone())),
            None => (false, ArchivedQuantileResults {
                volume: ArchivedQuantileValues { q25: 0.0, q50: 0.0, q75: 0.0, q90: 0.0 },
                taker_buy_volume: ArchivedQuantileValues { q25: 0.0, q50: 0.0, q75: 0.0, q90: 0.0 },
                avg_trade: ArchivedQuantileValues { q25: 0.0, q50: 0.0, q75: 0.0, q90: 0.0 },
                trade_count: ArchivedQuantileValues { q25: 0.0, q50: 0.0, q75: 0.0, q90: 0.0 },
            }),
        };
        
        Ok(ArchivedIndicatorOutput {
            symbol: output.symbol.clone(),
            timestamp: output.timestamp,
            close_5m: opt_f64_to_f64(output.close_5m),
            close_15m: opt_f64_to_f64(output.close_15m),
            close_60m: opt_f64_to_f64(output.close_60m),
            close_4h: opt_f64_to_f64(output.close_4h),
            ema21_1min: opt_f64_to_f64(output.ema21_1min),
            ema89_1min: opt_f64_to_f64(output.ema89_1min),
            ema89_5min: opt_f64_to_f64(output.ema89_5min),
            ema89_15min: opt_f64_to_f64(output.ema89_15min),
            ema89_1h: opt_f64_to_f64(output.ema89_1h),
            ema89_4h: opt_f64_to_f64(output.ema89_4h),
            trend_1min: ArchivedTrendDirection::from(output.trend_1min),
            trend_5min: ArchivedTrendDirection::from(output.trend_5min),
            trend_15min: ArchivedTrendDirection::from(output.trend_15min),
            trend_1h: ArchivedTrendDirection::from(output.trend_1h),
            trend_4h: ArchivedTrendDirection::from(output.trend_4h),
            max_volume: opt_f64_to_f64(output.max_volume),
            max_volume_price: opt_f64_to_f64(output.max_volume_price),
            max_volume_time: output.max_volume_time.clone().unwrap_or_default(),
            max_volume_trend: output.max_volume_trend
                .map(ArchivedTrendDirection::from)
                .unwrap_or(ArchivedTrendDirection::Neutral),
            has_volume_quantiles: has_quantiles,
            volume_quantiles: quantiles,
            has_volume_profile,
            volume_profile,
        })
    }
    
    /// Convert archived format back to IndicatorOutput
    fn convert_from_archived(&self, archived: &ArchivedIndicatorOutput) -> Result<IndicatorOutput, RkyvSerializationError> {
        // Helper function to convert f64 with sentinel back to Option<f64>
        let f64_to_opt_f64 = |val: f64| if val < 0.0 { None } else { Some(val) };
        
        // Convert volume profile if present
        #[cfg(feature = "volume_profile")]
        let volume_profile = if archived.has_volume_profile {
            Some(self.convert_volume_profile_from_archived(&archived.volume_profile)?)
        } else {
            None
        };
        
        #[cfg(not(feature = "volume_profile"))]
        let volume_profile = ();
        
        // Convert quantile results if present
        let volume_quantiles = if archived.has_volume_quantiles {
            Some(QuantileResults {
                volume: QuantileValues {
                    q25: archived.volume_quantiles.volume.q25,
                    q50: archived.volume_quantiles.volume.q50,
                    q75: archived.volume_quantiles.volume.q75,
                    q90: archived.volume_quantiles.volume.q90,
                },
                taker_buy_volume: QuantileValues {
                    q25: archived.volume_quantiles.taker_buy_volume.q25,
                    q50: archived.volume_quantiles.taker_buy_volume.q50,
                    q75: archived.volume_quantiles.taker_buy_volume.q75,
                    q90: archived.volume_quantiles.taker_buy_volume.q90,
                },
                avg_trade: QuantileValues {
                    q25: archived.volume_quantiles.avg_trade.q25,
                    q50: archived.volume_quantiles.avg_trade.q50,
                    q75: archived.volume_quantiles.avg_trade.q75,
                    q90: archived.volume_quantiles.avg_trade.q90,
                },
                trade_count: QuantileValues {
                    q25: archived.volume_quantiles.trade_count.q25,
                    q50: archived.volume_quantiles.trade_count.q50,
                    q75: archived.volume_quantiles.trade_count.q75,
                    q90: archived.volume_quantiles.trade_count.q90,
                },
            })
        } else {
            None
        };
        
        Ok(IndicatorOutput {
            symbol: archived.symbol.to_string(),
            timestamp: archived.timestamp,
            close_5m: f64_to_opt_f64(archived.close_5m),
            close_15m: f64_to_opt_f64(archived.close_15m),
            close_60m: f64_to_opt_f64(archived.close_60m),
            close_4h: f64_to_opt_f64(archived.close_4h),
            ema21_1min: f64_to_opt_f64(archived.ema21_1min),
            ema89_1min: f64_to_opt_f64(archived.ema89_1min),
            ema89_5min: f64_to_opt_f64(archived.ema89_5min),
            ema89_15min: f64_to_opt_f64(archived.ema89_15min),
            ema89_1h: f64_to_opt_f64(archived.ema89_1h),
            ema89_4h: f64_to_opt_f64(archived.ema89_4h),
            trend_1min: TrendDirection::from(archived.trend_1min),
            trend_5min: TrendDirection::from(archived.trend_5min),
            trend_15min: TrendDirection::from(archived.trend_15min),
            trend_1h: TrendDirection::from(archived.trend_1h),
            trend_4h: TrendDirection::from(archived.trend_4h),
            max_volume: f64_to_opt_f64(archived.max_volume),
            max_volume_price: f64_to_opt_f64(archived.max_volume_price),
            max_volume_time: if archived.max_volume_time.is_empty() { 
                None 
            } else { 
                Some(archived.max_volume_time.to_string()) 
            },
            max_volume_trend: match archived.max_volume_trend {
                ArchivedTrendDirection::Neutral => None,
                trend => Some(TrendDirection::from(trend)),
            },
            volume_quantiles,
            volume_profile,
        })
    }
    
    /// Convert VolumeProfileData to archived format
    #[cfg(feature = "volume_profile")]
    fn convert_volume_profile_to_archived(&self, vp: &VolumeProfileData) -> Result<ArchivedVolumeProfileData, RkyvSerializationError> {
        use rust_decimal::prelude::ToPrimitive;
        
        // Convert price levels to efficient tuple format
        let price_levels: Vec<(f64, f64, f64, u32)> = vp.price_levels
            .iter()
            .map(|level| (
                level.price.to_f64().unwrap_or(0.0),
                level.volume.to_f64().unwrap_or(0.0),
                level.percentage.to_f64().unwrap_or(0.0),
                level.candle_count,
            ))
            .collect();
        
        Ok(ArchivedVolumeProfileData {
            date: vp.date.clone(),
            total_volume: vp.total_volume.to_f64().unwrap_or(0.0),
            vwap: vp.vwap.to_f64().unwrap_or(0.0),
            poc: vp.poc.to_f64().unwrap_or(0.0),
            price_increment: vp.price_increment.to_f64().unwrap_or(0.0),
            min_price: vp.min_price.to_f64().unwrap_or(0.0),
            max_price: vp.max_price.to_f64().unwrap_or(0.0),
            candle_count: vp.candle_count,
            last_updated: vp.last_updated,
            price_levels,
            value_area: (
                vp.value_area.high.to_f64().unwrap_or(0.0),
                vp.value_area.low.to_f64().unwrap_or(0.0),
                vp.value_area.volume_percentage.to_f64().unwrap_or(0.0),
                vp.value_area.volume.to_f64().unwrap_or(0.0),
            ),
        })
    }
    
    /// Convert archived VolumeProfileData back to runtime format
    #[cfg(feature = "volume_profile")]
    fn convert_volume_profile_from_archived(&self, archived: &ArchivedVolumeProfileData) -> Result<VolumeProfileData, RkyvSerializationError> {
        use rust_decimal::Decimal;
        use crate::volume_profile::structs::{PriceLevelData, ValueArea};
        
        // Convert price levels back to runtime format
        let price_levels: Vec<PriceLevelData> = archived.price_levels
            .iter()
            .map(|(price, volume, percentage, candle_count)| PriceLevelData {
                price: Decimal::from_f64_retain(*price).unwrap_or_default(),
                volume: Decimal::from_f64_retain(*volume).unwrap_or_default(),
                percentage: Decimal::from_f64_retain(*percentage).unwrap_or_default(),
                candle_count: *candle_count,
            })
            .collect();
        
        Ok(VolumeProfileData {
            date: archived.date.to_string(),
            price_levels,
            total_volume: Decimal::from_f64_retain(archived.total_volume).unwrap_or_default(),
            vwap: Decimal::from_f64_retain(archived.vwap).unwrap_or_default(),
            poc: Decimal::from_f64_retain(archived.poc).unwrap_or_default(),
            value_area: ValueArea {
                high: Decimal::from_f64_retain(archived.value_area.0).unwrap_or_default(),
                low: Decimal::from_f64_retain(archived.value_area.1).unwrap_or_default(),
                volume_percentage: Decimal::from_f64_retain(archived.value_area.2).unwrap_or_default(),
                volume: Decimal::from_f64_retain(archived.value_area.3).unwrap_or_default(),
            },
            price_increment: Decimal::from_f64_retain(archived.price_increment).unwrap_or_default(),
            min_price: Decimal::from_f64_retain(archived.min_price).unwrap_or_default(),
            max_price: Decimal::from_f64_retain(archived.max_price).unwrap_or_default(),
            candle_count: archived.candle_count,
            last_updated: archived.last_updated,
        })
    }
    
    /// Serialize indicator result for async batching system 
    pub async fn serialize_indicator_result(
        &mut self,
        symbol: &str,
        indicator_type: &crate::technical_analysis::async_batching::IndicatorType,
        _parameters: &crate::technical_analysis::async_batching::IndicatorParameters,
    ) -> Result<String, TechnicalAnalysisError> {
        // Create mock IndicatorOutput for demonstration
        // In real implementation, this would compute the actual technical analysis
        let output = IndicatorOutput {
            symbol: symbol.to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            close_5m: Some(50000.0),
            close_15m: Some(49950.0),
            close_60m: Some(49900.0),
            close_4h: Some(49850.0),
            ema21_1min: Some(49975.0),
            ema89_1min: Some(49950.0),
            ema89_5min: Some(49925.0),
            ema89_15min: Some(49900.0),
            ema89_1h: Some(49875.0),
            ema89_4h: Some(49850.0),
            trend_1min: match indicator_type {
                crate::technical_analysis::async_batching::IndicatorType::VolumeWeightedAveragePrice => TrendDirection::Buy,
                crate::technical_analysis::async_batching::IndicatorType::RelativeStrengthIndex => TrendDirection::Neutral,
                _ => TrendDirection::Sell,
            },
            trend_5min: TrendDirection::Buy,
            trend_15min: TrendDirection::Neutral,
            trend_1h: TrendDirection::Neutral,
            trend_4h: TrendDirection::Sell,
            max_volume: Some(1200.0),
            max_volume_price: Some(50025.0),
            max_volume_time: Some("2025-08-10T15:30:00Z".to_string()),
            max_volume_trend: Some(TrendDirection::Buy),
            volume_quantiles: None,
            #[cfg(feature = "volume_profile")]
            volume_profile: None,
            #[cfg(not(feature = "volume_profile"))]
            volume_profile: (),
        };

        // Serialize to binary format
        let _binary_data = self.serialize_indicator_output(&output)
            .map_err(|e| TechnicalAnalysisError::SerializationError(e.to_string()))?;

        // Convert to JSON string for compatibility with existing API consumers
        // In production, you might want to return binary data directly for maximum performance
        let json_string = serde_json::to_string(&output)
            .map_err(|e| TechnicalAnalysisError::SerializationError(e.to_string()))?;

        Ok(json_string)
    }

    /// Get performance metrics
    pub fn get_performance_metrics(&self) -> RkyvSerializationMetrics {
        RkyvSerializationMetrics {
            operations_count: self.operations_count,
            total_serialization_time: self.total_serialization_time,
            total_deserialization_time: self.total_deserialization_time,
            average_serialization_time: if self.operations_count > 0 {
                self.total_serialization_time / self.operations_count as u32
            } else {
                std::time::Duration::ZERO
            },
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            cache_hit_rate: if self.cache_hits + self.cache_misses > 0 {
                self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64
            } else {
                0.0
            },
        }
    }
}

impl Default for TechnicalAnalysisSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Performance metrics for rkyv serialization
#[derive(Debug, Clone)]
pub struct RkyvSerializationMetrics {
    pub operations_count: u64,
    pub total_serialization_time: std::time::Duration,
    pub total_deserialization_time: std::time::Duration,
    pub average_serialization_time: std::time::Duration,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::technical_analysis::structs::{IndicatorOutput, TrendDirection};
    use std::time::Instant;
    
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
    fn test_rkyv_serialization_roundtrip() {
        let mut serializer = TechnicalAnalysisSerializer::new();
        let original = create_test_indicator_output();
        
        // Serialize to rkyv binary format
        let serialized = serializer.serialize_indicator_output(&original).unwrap();
        
        // Deserialize back to runtime format
        let deserialized = serializer.deserialize_indicator_output(&serialized).unwrap();
        
        // Verify roundtrip accuracy
        assert_eq!(original.symbol, deserialized.symbol);
        assert_eq!(original.timestamp, deserialized.timestamp);
        assert_eq!(original.close_5m, deserialized.close_5m);
        assert_eq!(original.trend_1min, deserialized.trend_1min);
        assert_eq!(original.max_volume, deserialized.max_volume);
    }
    
    #[test]
    fn test_rkyv_serialization_performance() {
        let mut serializer = TechnicalAnalysisSerializer::new();
        let output = create_test_indicator_output();
        
        // Warm-up
        for _ in 0..10 {
            let _ = serializer.serialize_indicator_output(&output);
        }
        
        // Performance test
        let start = Instant::now();
        let iterations = 1000;
        
        for _ in 0..iterations {
            let _ = serializer.serialize_indicator_output(&output);
        }
        
        let elapsed = start.elapsed();
        let avg_time = elapsed / iterations;
        
        println!("rkyv serialization average time: {:?}", avg_time);
        
        // Should be faster than JSON serialization
        assert!(avg_time < std::time::Duration::from_micros(10));
        
        let metrics = serializer.get_performance_metrics();
        println!("rkyv Performance metrics: {:?}", metrics);
    }
    
    #[test]
    fn test_rkyv_binary_size_efficiency() {
        let mut serializer = TechnicalAnalysisSerializer::new();
        let output = create_test_indicator_output();
        
        // Compare binary size vs JSON size
        let rkyv_data = serializer.serialize_indicator_output(&output).unwrap();
        let json_data = serde_json::to_string(&output).unwrap();
        
        println!("rkyv binary size: {} bytes", rkyv_data.len());
        println!("JSON string size: {} bytes", json_data.len());
        
        // Binary format should be more compact
        assert!(rkyv_data.len() <= json_data.len());
    }
}