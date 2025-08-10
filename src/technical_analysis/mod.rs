pub mod actors;
pub mod async_batching;
pub mod structs; 
pub mod indicators;
pub mod utils;
pub mod simd_math;
pub mod fast_serialization;
pub mod rkyv_serialization;

// Re-export commonly used types for convenience
pub use structs::{
    TechnicalAnalysisConfig, MultiTimeFrameCandles, IncrementalEMA, 
    TrendAnalyzer, TrendDirection, MaxVolumeTracker, IndicatorOutput
};

pub use rkyv_serialization::{
    TechnicalAnalysisSerializer, RkyvSerializationError, TechnicalAnalysisError, RkyvSerializationMetrics,
    ArchivedIndicatorOutput, ArchivedTrendDirection, ArchivedQuantileResults, ArchivedQuantileValues
};

pub use async_batching::{
    AsyncBatchingSystem, BatchConfig, BatchMetrics, IndicatorType, IndicatorParameters, PriceType
};

pub use actors::{
    timeframe::{TimeFrameActor, TimeFrameTell, TimeFrameAsk, TimeFrameReply},
    indicator::{IndicatorActor, IndicatorTell, IndicatorAsk, IndicatorReply}
};