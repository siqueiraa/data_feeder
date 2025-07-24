pub mod actors;
pub mod structs; 
pub mod indicators;
pub mod utils;

// Re-export commonly used types for convenience
pub use structs::{
    TechnicalAnalysisConfig, MultiTimeFrameCandles, IncrementalEMA, 
    TrendAnalyzer, TrendDirection, MaxVolumeTracker, IndicatorOutput
};

pub use actors::{
    timeframe::{TimeFrameActor, TimeFrameTell, TimeFrameAsk, TimeFrameReply},
    indicator::{IndicatorActor, IndicatorTell, IndicatorAsk, IndicatorReply}
};