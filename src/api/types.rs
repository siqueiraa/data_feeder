use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
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

/// Order side enumeration for trading
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Order type enumeration for trading
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub enum OrderType {
    Market,
    Limit,
    StopLoss,
    StopLimit,
    TakeProfit,
}

/// Order status enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

/// Position side enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub enum PositionSide {
    Long,
    Short,
    None,
}

/// Order structure with zero-copy serialization support
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct Order {
    pub id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub status: OrderStatus,
    pub timestamp: i64,
}

/// Position structure with zero-copy serialization support
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct Position {
    pub symbol: String,
    pub side: PositionSide,
    pub size: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    pub margin_required: f64,
    pub timestamp: i64,
}

/// Balance structure for account balances
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct Balance {
    pub asset: String,
    pub available: f64,
    pub locked: f64,
    pub total: f64,
    pub timestamp: i64,
}

/// Trade execution structure
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct TradeExecution {
    pub id: String,
    pub order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub price: f64,
    pub fee: f64,
    pub fee_asset: String,
    pub timestamp: i64,
}

/// Order request structure for placing orders
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub time_in_force: Option<String>,
    pub client_order_id: Option<String>,
    pub reduce_only: bool,
}

/// Modify order request structure
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ModifyOrderRequest {
    pub symbol: String,
    pub order_id: String,
    pub quantity: Option<f64>,
    pub price: Option<f64>,
}

/// Order response structure
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct OrderResponse {
    pub order: Order,
    pub success: bool,
    pub message: Option<String>,
    pub timestamp: i64,
}

impl OrderRequest {
    /// Create a new market order request
    pub fn market_order(symbol: String, side: OrderSide, quantity: f64) -> Self {
        Self {
            symbol,
            side,
            order_type: OrderType::Market,
            quantity,
            price: None,
            time_in_force: Some("IOC".to_string()),
            client_order_id: None,
            reduce_only: false,
        }
    }

    /// Create a new limit order request
    pub fn limit_order(symbol: String, side: OrderSide, quantity: f64, price: f64) -> Self {
        Self {
            symbol,
            side,
            order_type: OrderType::Limit,
            quantity,
            price: Some(price),
            time_in_force: Some("GTC".to_string()),
            client_order_id: None,
            reduce_only: false,
        }
    }

    /// Set client order ID for tracking
    pub fn with_client_order_id(mut self, client_order_id: String) -> Self {
        self.client_order_id = Some(client_order_id);
        self
    }

    /// Set as reduce-only order
    pub fn reduce_only(mut self) -> Self {
        self.reduce_only = true;
        self
    }

    /// Validate order request parameters
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.symbol.is_empty() {
            return Err(ApiError::InvalidSymbol("Symbol cannot be empty".to_string()));
        }

        if self.quantity <= 0.0 {
            return Err(ApiError::InvalidTimeframe("Quantity must be positive".to_string()));
        }

        if self.order_type == OrderType::Limit && self.price.is_none() {
            return Err(ApiError::InvalidTimeframe("Limit orders require a price".to_string()));
        }

        if let Some(price) = self.price {
            if price <= 0.0 {
                return Err(ApiError::InvalidTimeframe("Price must be positive".to_string()));
            }
        }

        Ok(())
    }
}

/// Cross-exchange pricing comparison data
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct CrossExchangePrices {
    pub symbol: String,
    pub binance_price: Option<f64>,
    pub gate_io_price: Option<f64>,
    pub spread: Option<f64>,
    pub spread_percentage: Option<f64>,
    pub timestamp: i64,
}

impl CrossExchangePrices {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            binance_price: None,
            gate_io_price: None,
            spread: None,
            spread_percentage: None,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    pub fn with_binance_price(mut self, price: f64) -> Self {
        self.binance_price = Some(price);
        self.update_spread();
        self
    }

    pub fn with_gate_io_price(mut self, price: f64) -> Self {
        self.gate_io_price = Some(price);
        self.update_spread();
        self
    }

    fn update_spread(&mut self) {
        if let (Some(binance), Some(gate_io)) = (self.binance_price, self.gate_io_price) {
            self.spread = Some((binance - gate_io).abs());
            if gate_io != 0.0 {
                self.spread_percentage = Some(((binance - gate_io).abs() / gate_io) * 100.0);
            }
        }
    }

    pub fn is_complete(&self) -> bool {
        self.binance_price.is_some() && self.gate_io_price.is_some()
    }
}

/// Arbitrage opportunity detection
#[derive(Debug, Clone, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ArbitrageOpportunity {
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_price: f64,
    pub sell_price: f64,
    pub price_difference: f64,
    pub percentage_profit: f64,
    pub min_quantity: f64,
    pub max_quantity: f64,
    pub timestamp: i64,
}

impl ArbitrageOpportunity {
    pub fn new(
        symbol: String,
        buy_exchange: String,
        sell_exchange: String,
        buy_price: f64,
        sell_price: f64,
        min_quantity: f64,
        max_quantity: f64,
    ) -> Self {
        let price_difference = sell_price - buy_price;
        let percentage_profit = if buy_price != 0.0 {
            (price_difference / buy_price) * 100.0
        } else {
            0.0
        };

        Self {
            symbol,
            buy_exchange,
            sell_exchange,
            buy_price,
            sell_price,
            price_difference,
            percentage_profit,
            min_quantity,
            max_quantity,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    pub fn is_profitable(&self, min_profit_percentage: f64) -> bool {
        self.percentage_profit >= min_profit_percentage && self.price_difference > 0.0
    }
}