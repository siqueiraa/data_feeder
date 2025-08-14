use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use crate::historical::structs::{TimestampMS};

/// Supported API endpoints and data types
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ApiEndpoint {
    /// Kline/Candlestick data
    Klines,
    /// 24hr ticker statistics
    Ticker24hr,
    /// Exchange info
    ExchangeInfo,
}

impl ApiEndpoint {
    /// Get the Binance Futures API path for this endpoint
    pub fn binance_path(&self) -> &'static str {
        match self {
            ApiEndpoint::Klines => "/fapi/v1/klines",
            ApiEndpoint::Ticker24hr => "/fapi/v1/ticker/24hr",
            ApiEndpoint::ExchangeInfo => "/fapi/v1/exchangeInfo",
        }
    }

    /// Get the Gate.io Futures API path for this endpoint
    pub fn gate_io_path(&self) -> &'static str {
        match self {
            ApiEndpoint::Klines => "/api/v4/futures/usdt/candlesticks",
            ApiEndpoint::Ticker24hr => "/api/v4/futures/usdt/tickers",
            ApiEndpoint::ExchangeInfo => "/api/v4/futures/usdt/symbols",
        }
    }
}

/// API request configuration
#[derive(Debug, Clone)]
pub struct ApiRequest {
    pub endpoint: ApiEndpoint,
    pub symbol: String,
    pub interval: String,
    pub start_time: Option<TimestampMS>,
    pub end_time: Option<TimestampMS>,
    pub limit: Option<u32>,
}

impl ApiRequest {
    pub fn new_klines(symbol: String, interval: String) -> Self {
        Self {
            endpoint: ApiEndpoint::Klines,
            symbol,
            interval,
            start_time: None,
            end_time: None,
            limit: None,
        }
    }

    pub fn with_time_range(mut self, start_time: TimestampMS, end_time: TimestampMS) -> Self {
        self.start_time = Some(start_time);
        self.end_time = Some(end_time);
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn new_ticker_24hr(symbol: String) -> Self {
        Self {
            endpoint: ApiEndpoint::Ticker24hr,
            symbol,
            interval: "".to_string(), // Not used for ticker
            start_time: None,
            end_time: None,
            limit: None,
        }
    }

    pub fn new_exchange_info() -> Self {
        Self {
            endpoint: ApiEndpoint::ExchangeInfo,
            symbol: "".to_string(), // Not used for exchange info
            interval: "".to_string(), // Not used for exchange info
            start_time: None,
            end_time: None,
            limit: None,
        }
    }
}

/// API response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub data: T,
    pub timestamp: TimestampMS,
    pub rate_limit_info: Option<RateLimitInfo>,
}

/// Rate limiting information from API headers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitInfo {
    pub requests_used: u32,
    pub requests_limit: u32,
    pub reset_time: Option<TimestampMS>,
    pub retry_after: Option<u32>,
}

/// API error types
#[derive(Error, Debug, Clone)]
pub enum ApiError {
    #[error("HTTP error: {0}")]
    Http(String),
    
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Rate limit exceeded: {0}")]
    RateLimit(String),
    
    #[error("Invalid symbol: {0}")]
    InvalidSymbol(String),
    
    #[error("Invalid timeframe: {0}")]
    InvalidTimeframe(String),
    
    #[error("Authentication error: {0}")]
    Authentication(String),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Timeout error: {0}")]
    Timeout(String),
    
    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl ApiError {
    pub fn is_recoverable(&self) -> bool {
        matches!(self, 
            ApiError::Network(_) | 
            ApiError::Timeout(_) | 
            ApiError::Http(_) |
            ApiError::Unknown(_)
        )
    }

    pub fn is_rate_limit(&self) -> bool {
        matches!(self, ApiError::RateLimit(_))
    }
}

/// API statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApiStats {
    pub requests_made: u64,
    pub requests_successful: u64,
    pub requests_failed: u64,
    pub rate_limit_hits: u64,
    pub total_candles_fetched: u64,
    pub last_request_time: Option<TimestampMS>,
}

impl ApiStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_request(&mut self) {
        self.requests_made += 1;
        self.last_request_time = Some(chrono::Utc::now().timestamp_millis());
    }

    pub fn record_success(&mut self, candles_count: u64) {
        self.requests_successful += 1;
        self.total_candles_fetched += candles_count;
    }

    pub fn record_failure(&mut self) {
        self.requests_failed += 1;
    }

    pub fn record_rate_limit(&mut self) {
        self.rate_limit_hits += 1;
        self.record_failure();
    }

    pub fn success_rate(&self) -> f64 {
        if self.requests_made == 0 {
            0.0
        } else {
            self.requests_successful as f64 / self.requests_made as f64
        }
    }
}

/// Configuration for API client
#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub base_url: String,
    pub timeout_seconds: u64,
    pub max_retries: u32,
    pub rate_limit_delay_ms: u64,
    pub max_requests_per_minute: u32,
}

impl ApiConfig {
    /// Create Binance Futures API configuration
    pub fn binance_futures() -> Self {
        Self {
            base_url: "https://fapi.binance.com".to_string(),
            timeout_seconds: 30,
            max_retries: 3,
            rate_limit_delay_ms: 50,
            max_requests_per_minute: 1200, // Binance Futures limit
        }
    }

    /// Create Gate.io Futures API configuration
    pub fn gate_io_futures() -> Self {
        Self {
            base_url: "https://api.gateio.ws".to_string(),
            timeout_seconds: 30,
            max_retries: 3,
            rate_limit_delay_ms: 50,
            max_requests_per_minute: 1000, // Gate.io conservative limit
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self::binance_futures()
    }
}

impl fmt::Display for ApiEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiEndpoint::Klines => write!(f, "klines"),
            ApiEndpoint::Ticker24hr => write!(f, "ticker_24hr"),
            ApiEndpoint::ExchangeInfo => write!(f, "exchange_info"),
        }
    }
}