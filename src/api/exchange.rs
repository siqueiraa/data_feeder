use async_trait::async_trait;
use std::time::Duration;

use crate::api::types::{ApiError, ApiRequest, ApiResponse};
use crate::historical::structs::FuturesOHLCVCandle;

/// Ticker 24hr statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Ticker24hr {
    pub symbol: String,
    pub price_change: f64,
    pub price_change_percent: f64,
    pub weighted_avg_price: f64,
    pub last_price: f64,
    pub last_qty: f64,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub open_time: i64,
    pub close_time: i64,
    pub count: u64,
}

/// Exchange information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExchangeInfo {
    pub timezone: String,
    pub server_time: i64,
    pub symbols: Vec<SymbolInfo>,
    pub rate_limits: Vec<RateLimit>,
}

/// Symbol information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    pub status: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: u32,
    pub quantity_precision: u32,
}

/// Rate limit information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RateLimit {
    pub rate_limit_type: String,
    pub interval: String,
    pub interval_num: u32,
    pub limit: u32,
}

/// Unified Exchange trait for cryptocurrency exchange API clients
#[async_trait]
pub trait Exchange: Send + Sync {
    /// Get OHLCV klines data for a symbol (zero-copy)
    async fn get_klines(&mut self, request: &ApiRequest) -> Result<ApiResponse<Vec<FuturesOHLCVCandle>>, ApiError>;
    
    /// Get 24hr ticker statistics for a symbol
    async fn get_ticker_24hr(&mut self, symbol: &str) -> Result<ApiResponse<Ticker24hr>, ApiError>;
    
    /// Get exchange information (symbols, rate limits, etc.)
    async fn get_exchange_info(&mut self) -> Result<ApiResponse<ExchangeInfo>, ApiError>;
    
    /// Set the minimum interval between API requests for rate limiting
    async fn set_rate_limit(&mut self, min_interval: Duration);
    
    /// Get the exchange name for identification
    fn get_exchange_name(&self) -> &'static str;
    
    /// Check if the exchange connection is healthy
    fn is_healthy(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ticker24hr_creation() {
        let ticker = Ticker24hr {
            symbol: "BTCUSDT".to_string(),
            price_change: 100.0,
            price_change_percent: 0.5,
            weighted_avg_price: 50000.0,
            last_price: 50100.0,
            last_qty: 0.001,
            open_price: 50000.0,
            high_price: 50200.0,
            low_price: 49900.0,
            volume: 1000.0,
            quote_volume: 50000000.0,
            open_time: 1640995200000,
            close_time: 1641081600000,
            count: 12345,
        };
        
        assert_eq!(ticker.symbol, "BTCUSDT");
        assert_eq!(ticker.price_change, 100.0);
        assert_eq!(ticker.last_price, 50100.0);
    }
    
    #[test]
    fn test_exchange_info_creation() {
        let exchange_info = ExchangeInfo {
            timezone: "UTC".to_string(),
            server_time: 1641081600000,
            symbols: vec![
                SymbolInfo {
                    symbol: "BTCUSDT".to_string(),
                    status: "TRADING".to_string(),
                    base_asset: "BTC".to_string(),
                    quote_asset: "USDT".to_string(),
                    price_precision: 2,
                    quantity_precision: 6,
                }
            ],
            rate_limits: vec![
                RateLimit {
                    rate_limit_type: "REQUEST_WEIGHT".to_string(),
                    interval: "MINUTE".to_string(),
                    interval_num: 1,
                    limit: 1200,
                }
            ],
        };
        
        assert_eq!(exchange_info.timezone, "UTC");
        assert_eq!(exchange_info.symbols.len(), 1);
        assert_eq!(exchange_info.rate_limits.len(), 1);
        assert_eq!(exchange_info.symbols[0].symbol, "BTCUSDT");
    }
}