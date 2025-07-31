/// Database and storage constants
// LMDB Configuration
pub const LMDB_MAP_SIZE: usize = 1024 * 1024 * 1024; // 1GB per symbol-timeframe
pub const LMDB_MAX_DBS: u32 = 10;
pub const LMDB_MAX_READERS: u32 = 256;

// Database names
pub const CANDLES_DB_NAME: &str = "candles";
pub const CERTIFIED_RANGE_DB_NAME: &str = "certified_range";

// Batch processing
pub const BATCH_SIZE: usize = 1000;

// Time constants
pub const MILLISECONDS_PER_SECOND: i64 = 1000;
pub const SECONDS_PER_MINUTE: i64 = 60;
pub const SECONDS_PER_HOUR: i64 = 3600;
pub const SECONDS_PER_DAY: i64 = 86400;
pub const MINUTES_PER_HOUR: i64 = 60;
pub const HOURS_PER_DAY: i64 = 24;
pub const DAYS_PER_MONTH: i64 = 30; // Approximate for calculations

// URL and file patterns
pub const BINANCE_BASE_URL: &str = "https://data.binance.vision/";
pub const FUTURES_KLINES_PATH: &str = "data/futures/um";
pub const DAILY_PATH: &str = "daily/klines";
pub const MONTHLY_PATH: &str = "monthly/klines";
pub const AGGTRADES_PATH: &str = "daily/aggTrades";

// File extensions and suffixes
pub const ZIP_EXTENSION: &str = ".zip";
pub const CSV_EXTENSION: &str = ".csv";
pub const CHECKSUM_EXTENSION: &str = ".CHECKSUM";

// Default values
pub const DEFAULT_SYMBOL: &str = "BTCUSDT";
pub const DEFAULT_TIMEFRAME: u64 = 60; // 1 minute
pub const DEFAULT_MONTHLY_THRESHOLD_MONTHS: u32 = 3;

// Interval mappings
pub const INTERVAL_1M: &str = "1m";
pub const INTERVAL_3M: &str = "3m";
pub const INTERVAL_5M: &str = "5m";
pub const INTERVAL_15M: &str = "15m";
pub const INTERVAL_30M: &str = "30m";
pub const INTERVAL_1H: &str = "1h";
pub const INTERVAL_2H: &str = "2h";
pub const INTERVAL_4H: &str = "4h";
pub const INTERVAL_6H: &str = "6h";
pub const INTERVAL_8H: &str = "8h";
pub const INTERVAL_12H: &str = "12h";
pub const INTERVAL_1D: &str = "1d";
pub const INTERVAL_3D: &str = "3d";

// Error context messages
pub const LMDB_ENV_CREATION_CONTEXT: &str = "Failed to create LMDB environment";
pub const LMDB_TRANSACTION_CONTEXT: &str = "Failed to create LMDB transaction";
pub const LMDB_DATABASE_CONTEXT: &str = "Failed to create LMDB database";
pub const SEMAPHORE_ACQUIRE_CONTEXT: &str = "Failed to acquire semaphore";
pub const DIRECTORY_CREATION_CONTEXT: &str = "Failed to create directory";

// Topic and stream names
pub const KAFKA_DATA_TOPIC_SUFFIX: &str = "data";
pub const WEBSOCKET_KLINE_SUFFIX: &str = "@kline_1m";
pub const WEBSOCKET_STREAM_PREFIX: &str = "stream?streams=";

// Health check and monitoring
pub const DEFAULT_HEALTH_CHECK_INTERVAL_MINUTES: u32 = 5;
pub const DEFAULT_GAP_CHECK_WINDOW_MINUTES: u32 = 30;
pub const DEFAULT_RECONNECTION_THRESHOLD_MINUTES: u32 = 1;
pub const DEFAULT_RECONNECTION_DELAY_SECONDS: u32 = 5;