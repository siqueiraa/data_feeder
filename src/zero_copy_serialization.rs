//! Zero-copy serialization patterns for high-performance data structures
//! 
//! This module provides custom zero-copy serialization for technical analysis 
//! and volume profile data using rkyv for compatible types and optimized 
//! binary formats for Decimal types.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, archived_root, ser::{serializers::AllocSerializer, Serializer}};
use crate::technical_analysis::structs::{IndicatorOutput, TrendDirection, QuantileResults, QuantileValues};
use crate::volume_profile::structs::VolumeProfileData;
use rust_decimal::prelude::ToPrimitive;

/// Zero-copy serializable version of TrendDirection
/// Uses u8 instead of enum for rkyv compatibility
#[derive(Debug, Clone, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ZeroCopyTrendDirection(pub u8);

impl From<TrendDirection> for ZeroCopyTrendDirection {
    fn from(trend: TrendDirection) -> Self {
        match trend {
            TrendDirection::Buy => ZeroCopyTrendDirection(0),
            TrendDirection::Sell => ZeroCopyTrendDirection(1), 
            TrendDirection::Neutral => ZeroCopyTrendDirection(2),
        }
    }
}

impl From<ZeroCopyTrendDirection> for TrendDirection {
    fn from(zero_copy: ZeroCopyTrendDirection) -> Self {
        match zero_copy.0 {
            0 => TrendDirection::Buy,
            1 => TrendDirection::Sell,
            _ => TrendDirection::Neutral,
        }
    }
}

/// Zero-copy serializable version of QuantileValues
/// Replaces any complex types with simple f64 for rkyv compatibility
#[derive(Debug, Clone, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ZeroCopyQuantileValues {
    pub q25: f64,    // 25th percentile
    pub q50: f64,    // 50th percentile (median)
    pub q75: f64,    // 75th percentile
    pub q90: f64,    // 90th percentile
}

/// Zero-copy serializable version of QuantileResults
/// Replaces QuantileValues with ZeroCopyQuantileValues for rkyv compatibility
#[derive(Debug, Clone, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ZeroCopyQuantileResults {
    pub volume: ZeroCopyQuantileValues,
    pub taker_buy_volume: ZeroCopyQuantileValues,
    pub avg_trade: ZeroCopyQuantileValues,
    pub trade_count: ZeroCopyQuantileValues,
}

/// Zero-copy serializable version of IndicatorOutput
/// Replaces Decimal fields with f64 for rkyv compatibility
#[derive(Debug, Clone, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ZeroCopyIndicatorOutput {
    pub symbol: String,
    pub timestamp: i64,
    
    // Multi-timeframe close prices
    pub close_5m: Option<f64>,
    pub close_15m: Option<f64>,
    pub close_60m: Option<f64>,
    pub close_4h: Option<f64>,
    
    // EMA indicators for different timeframes
    pub ema21_1min: Option<f64>,
    pub ema89_1min: Option<f64>,
    pub ema89_5min: Option<f64>,
    pub ema89_15min: Option<f64>,
    pub ema89_1h: Option<f64>,
    pub ema89_4h: Option<f64>,
    
    // Trend analysis for each timeframe
    pub trend_1min: ZeroCopyTrendDirection,
    pub trend_5min: ZeroCopyTrendDirection,
    pub trend_15min: ZeroCopyTrendDirection,
    pub trend_1h: ZeroCopyTrendDirection,
    pub trend_4h: ZeroCopyTrendDirection,
    
    // Volume analysis
    pub max_volume: Option<f64>,
    pub max_volume_price: Option<f64>,
    pub max_volume_time: Option<String>,
    pub max_volume_trend: ZeroCopyTrendDirection,
    
    // Quantile analysis  
    pub volume_quantiles: Option<ZeroCopyQuantileResults>,
}

/// Zero-copy serializable version of VolumeProfileData
/// Replaces Decimal fields with f64 for rkyv compatibility
#[derive(Debug, Clone, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ZeroCopyVolumeProfileData {
    /// Trading date (YYYY-MM-DD format)
    pub date: String,
    /// Individual price levels with volume data (simplified to f64)
    pub price_levels: Vec<ZeroCopyPriceLevelData>,
    /// Total volume for the day
    pub total_volume: f64,
    /// Volume Weighted Average Price
    pub vwap: f64,
    /// Point of Control (price level with highest volume)
    pub poc: f64,
    /// Value area (70% of volume concentration)
    pub value_area: ZeroCopyValueArea,
    /// Price increment used for this profile
    pub price_increment: f64,
    /// Minimum price for the day
    pub min_price: f64,
    /// Maximum price for the day
    pub max_price: f64,
    /// Number of 1-minute candles processed
    pub candle_count: u32,
}

/// Zero-copy serializable price level data
#[derive(Debug, Clone, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ZeroCopyPriceLevelData {
    /// Price level
    pub price: f64,
    /// Volume at this price level
    pub volume: f64,
    /// Percentage of total daily volume
    pub percentage: f64,
    /// Number of candles at this price level
    pub candle_count: u32,
}

/// Zero-copy serializable value area
#[derive(Debug, Clone, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ZeroCopyValueArea {
    /// Highest price in value area
    pub high: f64,
    /// Lowest price in value area
    pub low: f64,
    /// Percentage of total volume in value area
    pub volume_percentage: f64,
}

/// High-performance serializer for zero-copy data structures
pub struct ZeroCopySerializer {
    buffer: Vec<u8>,
}

impl ZeroCopySerializer {
    /// Create new serializer with initial capacity
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(4096), // Pre-allocate 4KB
        }
    }

    /// Create serializer with specific capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
        }
    }

    /// Serialize IndicatorOutput to zero-copy format
    pub fn serialize_indicator_output(&mut self, output: &IndicatorOutput) -> Result<&[u8], Box<dyn std::error::Error>> {
        // Convert to zero-copy compatible format
        let zero_copy_output = ZeroCopyIndicatorOutput {
            symbol: output.symbol.clone(),
            timestamp: output.timestamp,
            close_5m: output.close_5m,
            close_15m: output.close_15m,
            close_60m: output.close_60m,
            close_4h: output.close_4h,
            ema21_1min: output.ema21_1min,
            ema89_1min: output.ema89_1min,
            ema89_5min: output.ema89_5min,
            ema89_15min: output.ema89_15min,
            ema89_1h: output.ema89_1h,
            ema89_4h: output.ema89_4h,
            trend_1min: ZeroCopyTrendDirection::from(output.trend_1min),
            trend_5min: ZeroCopyTrendDirection::from(output.trend_5min),
            trend_15min: ZeroCopyTrendDirection::from(output.trend_15min),
            trend_1h: ZeroCopyTrendDirection::from(output.trend_1h),
            trend_4h: ZeroCopyTrendDirection::from(output.trend_4h),
            max_volume: output.max_volume,
            max_volume_price: output.max_volume_price,
            max_volume_time: output.max_volume_time.clone(),
            max_volume_trend: ZeroCopyTrendDirection::from(output.max_volume_trend.unwrap_or(TrendDirection::Neutral)),
            volume_quantiles: output.volume_quantiles.as_ref().map(|q| ZeroCopyQuantileResults {
                volume: ZeroCopyQuantileValues {
                    q25: q.volume.q25,
                    q50: q.volume.q50,
                    q75: q.volume.q75,
                    q90: q.volume.q90,
                },
                taker_buy_volume: ZeroCopyQuantileValues {
                    q25: q.taker_buy_volume.q25,
                    q50: q.taker_buy_volume.q50,
                    q75: q.taker_buy_volume.q75,
                    q90: q.taker_buy_volume.q90,
                },
                avg_trade: ZeroCopyQuantileValues {
                    q25: q.avg_trade.q25,
                    q50: q.avg_trade.q50,
                    q75: q.avg_trade.q75,
                    q90: q.avg_trade.q90,
                },
                trade_count: ZeroCopyQuantileValues {
                    q25: q.trade_count.q25,
                    q50: q.trade_count.q50,
                    q75: q.trade_count.q75,
                    q90: q.trade_count.q90,
                },
            }),
        };

        // Serialize using rkyv
        self.buffer.clear();
        let mut serializer = AllocSerializer::<256>::default();
        serializer.serialize_value(&zero_copy_output)?;
        let bytes = serializer.into_serializer().into_inner();
        self.buffer.extend_from_slice(&bytes);
        
        Ok(&self.buffer)
    }

    /// Serialize VolumeProfileData to zero-copy format
    pub fn serialize_volume_profile(&mut self, profile: &VolumeProfileData) -> Result<&[u8], Box<dyn std::error::Error>> {
        // Convert to zero-copy compatible format
        let zero_copy_profile = ZeroCopyVolumeProfileData {
            date: profile.date.clone(),
            price_levels: profile.price_levels.iter().map(|level| {
                ZeroCopyPriceLevelData {
                    price: level.price.to_f64().unwrap_or(0.0),
                    volume: level.volume.to_f64().unwrap_or(0.0),
                    percentage: level.percentage.to_f64().unwrap_or(0.0),
                    candle_count: level.candle_count,
                }
            }).collect(),
            total_volume: profile.total_volume.to_f64().unwrap_or(0.0),
            vwap: profile.vwap.to_f64().unwrap_or(0.0),
            poc: profile.poc.to_f64().unwrap_or(0.0),
            value_area: ZeroCopyValueArea {
                high: profile.value_area.high.to_f64().unwrap_or(0.0),
                low: profile.value_area.low.to_f64().unwrap_or(0.0),
                volume_percentage: profile.value_area.volume_percentage.to_f64().unwrap_or(0.0),
            },
            price_increment: profile.price_increment.to_f64().unwrap_or(0.0),
            min_price: profile.min_price.to_f64().unwrap_or(0.0),
            max_price: profile.max_price.to_f64().unwrap_or(0.0),
            candle_count: profile.candle_count,
        };

        // Serialize using rkyv
        self.buffer.clear();
        let mut serializer = AllocSerializer::<256>::default();
        serializer.serialize_value(&zero_copy_profile)?;
        let bytes = serializer.into_serializer().into_inner();
        self.buffer.extend_from_slice(&bytes);
        
        Ok(&self.buffer)
    }

    /// Get buffer reference for external writing
    pub fn get_buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Clear internal buffer for reuse
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

/// High-performance deserializer for zero-copy data structures
pub struct ZeroCopyDeserializer;

impl ZeroCopyDeserializer {
    /// Deserialize IndicatorOutput from zero-copy format
    pub fn deserialize_indicator_output(bytes: &[u8]) -> Result<ZeroCopyIndicatorOutput, Box<dyn std::error::Error>> {
        let archived = unsafe { archived_root::<ZeroCopyIndicatorOutput>(bytes) };
        let deserialized: ZeroCopyIndicatorOutput = archived.deserialize(&mut rkyv::Infallible)
            .map_err(|e| format!("Deserialization failed: {}", e))?;
        Ok(deserialized)
    }

    /// Deserialize VolumeProfileData from zero-copy format
    pub fn deserialize_volume_profile(bytes: &[u8]) -> Result<ZeroCopyVolumeProfileData, Box<dyn std::error::Error>> {
        let archived = unsafe { archived_root::<ZeroCopyVolumeProfileData>(bytes) };
        let deserialized: ZeroCopyVolumeProfileData = archived.deserialize(&mut rkyv::Infallible)
            .map_err(|e| format!("Deserialization failed: {}", e))?;
        Ok(deserialized)
    }
}

/// Conversion utilities between original and zero-copy formats
impl From<ZeroCopyIndicatorOutput> for IndicatorOutput {
    fn from(zero_copy: ZeroCopyIndicatorOutput) -> Self {
        IndicatorOutput {
            symbol: zero_copy.symbol,
            timestamp: zero_copy.timestamp,
            close_5m: zero_copy.close_5m,
            close_15m: zero_copy.close_15m,
            close_60m: zero_copy.close_60m,
            close_4h: zero_copy.close_4h,
            ema21_1min: zero_copy.ema21_1min,
            ema89_1min: zero_copy.ema89_1min,
            ema89_5min: zero_copy.ema89_5min,
            ema89_15min: zero_copy.ema89_15min,
            ema89_1h: zero_copy.ema89_1h,
            ema89_4h: zero_copy.ema89_4h,
            trend_1min: TrendDirection::from(zero_copy.trend_1min),
            trend_5min: TrendDirection::from(zero_copy.trend_5min),
            trend_15min: TrendDirection::from(zero_copy.trend_15min),
            trend_1h: TrendDirection::from(zero_copy.trend_1h),
            trend_4h: TrendDirection::from(zero_copy.trend_4h),
            max_volume: zero_copy.max_volume,
            max_volume_price: zero_copy.max_volume_price,
            max_volume_time: zero_copy.max_volume_time,
            max_volume_trend: Some(TrendDirection::from(zero_copy.max_volume_trend)),
            volume_quantiles: zero_copy.volume_quantiles.map(|q| QuantileResults {
                volume: QuantileValues {
                    q25: q.volume.q25,
                    q50: q.volume.q50,
                    q75: q.volume.q75,
                    q90: q.volume.q90,
                },
                taker_buy_volume: QuantileValues {
                    q25: q.taker_buy_volume.q25,
                    q50: q.taker_buy_volume.q50,
                    q75: q.taker_buy_volume.q75,
                    q90: q.taker_buy_volume.q90,
                },
                avg_trade: QuantileValues {
                    q25: q.avg_trade.q25,
                    q50: q.avg_trade.q50,
                    q75: q.avg_trade.q75,
                    q90: q.avg_trade.q90,
                },
                trade_count: QuantileValues {
                    q25: q.trade_count.q25,
                    q50: q.trade_count.q50,
                    q75: q.trade_count.q75,
                    q90: q.trade_count.q90,
                },
            }),
            volume_profile: None, // This would need separate handling
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::technical_analysis::structs::TrendDirection;

    #[test]
    fn test_zero_copy_serialization_roundtrip() {
        let mut serializer = ZeroCopySerializer::new();
        
        // Create test indicator output
        let original = IndicatorOutput {
            symbol: "BTCUSDT".to_string(),
            timestamp: 1640995200000,
            close_5m: Some(46222.01),
            close_15m: Some(46230.45),
            close_60m: None,
            close_4h: Some(46250.00),
            ema21_1min: Some(46220.0),
            ema89_1min: Some(46210.0),
            ema89_5min: Some(46215.0),
            ema89_15min: Some(46225.0),
            ema89_1h: Some(46235.0),
            ema89_4h: Some(46240.0),
            trend_1min: TrendDirection::Buy,
            trend_5min: TrendDirection::Buy,
            trend_15min: TrendDirection::Neutral,
            trend_1h: TrendDirection::Buy,
            trend_4h: TrendDirection::Sell,
            max_volume: Some(1500.0),
            max_volume_price: Some(46220.0),
            max_volume_time: Some("2022-01-01T12:00:00Z".to_string()),
            max_volume_trend: Some(TrendDirection::Buy),
            volume_quantiles: None,
            volume_profile: None,
        };

        // Serialize
        let bytes = serializer.serialize_indicator_output(&original).unwrap();
        
        // Deserialize
        let deserialized = ZeroCopyDeserializer::deserialize_indicator_output(bytes).unwrap();
        
        // Verify key fields
        assert_eq!(deserialized.symbol, "BTCUSDT");
        assert_eq!(deserialized.timestamp, 1640995200000);
        assert_eq!(deserialized.close_5m, Some(46222.01));
        assert_eq!(TrendDirection::from(deserialized.trend_1min), TrendDirection::Buy);
    }
}