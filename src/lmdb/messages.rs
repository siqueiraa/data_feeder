use serde::{Deserialize, Serialize};
use kameo::Reply;
use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LmdbActorMessage {
    /// Validate data range and return gaps (prefix, suffix, internal)
    ValidateDataRange {
        symbol: String,
        timeframe: u64,
        start: TimestampMS,
        end: TimestampMS,
    },
    /// Store candles for a symbol/timeframe
    StoreCandles {
        symbol: String,
        timeframe: u64,
        candles: Vec<FuturesOHLCVCandle>,
    },
    /// Get data range information for symbol/timeframe
    GetDataRange {
        symbol: String,
        timeframe: u64,
    },
    /// Get candles in specified time range
    GetCandles {
        symbol: String,
        timeframe: u64,
        start: TimestampMS,
        end: TimestampMS,
        limit: Option<u32>,
    },
    /// Compact database (optional symbol for specific cleanup)
    CompactDatabase {
        symbol: Option<String>,
    },
    /// Get storage statistics
    GetStorageStats,
    /// Initialize database for symbol/timeframe
    InitializeDatabase {
        symbol: String,
        timeframe: u64,
    },
    /// Check if volume profile data exists for a key
    #[cfg(feature = "volume_profile_reprocessing")]
    CheckVolumeProfileExists {
        key: String,
    },
}

/// Fire-and-forget messages for LmdbActor (Tell messages)
#[derive(Debug, Clone)]
pub enum LmdbActorTell {
    /// Store candles asynchronously (fire-and-forget)
    StoreCandlesAsync {
        symbol: String,
        timeframe: u64,
        candles: Vec<FuturesOHLCVCandle>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Reply)]
pub enum LmdbActorResponse {
    /// Gap validation result with list of gaps (start, end) timestamps
    ValidationResult {
        gaps: Vec<(TimestampMS, TimestampMS)>,
    },
    /// Data range information
    DataRange {
        earliest: Option<TimestampMS>,
        latest: Option<TimestampMS>,
        count: u64,
    },
    /// Candles data
    Candles(Vec<FuturesOHLCVCandle>),
    /// Storage statistics
    StorageStats {
        symbols: Vec<String>,
        total_candles: u64,
        db_size_bytes: u64,
        symbol_stats: Vec<SymbolStats>,
    },
    /// Success response
    Success,
    /// Error response
    ErrorResponse(String),
    /// Volume profile existence check result
    #[cfg(feature = "volume_profile_reprocessing")]
    VolumeProfileExists {
        exists: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolStats {
    pub symbol: String,
    pub timeframe: u64,
    pub candle_count: u64,
    pub earliest: Option<TimestampMS>,
    pub latest: Option<TimestampMS>,
    pub size_bytes: u64,
}

