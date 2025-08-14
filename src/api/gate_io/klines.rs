use async_trait::async_trait;
use reqwest;
use serde_json;
use sonic_rs;
use std::time::Duration;
use tokio::time::{sleep, Instant};
use tracing::{debug, info};

use crate::api::exchange::{Exchange, Ticker24hr, ExchangeInfo, SymbolInfo, RateLimit};
use crate::api::types::{ApiError, ApiRequest, ApiResponse, RateLimitInfo, ApiConfig};
use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};

/// Gate.io Futures API client for klines data
pub struct GateIoKlinesClient {
    client: reqwest::Client,
    base_url: String,
    last_request_time: Option<Instant>,
    min_request_interval: Duration,
    is_healthy: bool,
}

impl GateIoKlinesClient {
    /// Create a new Gate.io klines client
    pub fn new(base_url: String) -> Result<Self, ApiError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ApiError::Network(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            base_url,
            last_request_time: None,
            min_request_interval: Duration::from_millis(50), // Conservative rate limiting
            is_healthy: true,
        })
    }

    /// Create a new Gate.io klines client from configuration
    pub fn from_config(config: ApiConfig) -> Result<Self, ApiError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()
            .map_err(|e| ApiError::Network(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            base_url: config.base_url,
            last_request_time: None,
            min_request_interval: Duration::from_millis(config.rate_limit_delay_ms),
            is_healthy: true,
        })
    }

    /// Fetch klines data from Gate.io API
    pub async fn fetch_klines(&mut self, request: &ApiRequest) -> Result<ApiResponse<Vec<FuturesOHLCVCandle>>, ApiError> {
        // Rate limiting: ensure minimum interval between requests
        if let Some(last_request) = self.last_request_time {
            let elapsed = last_request.elapsed();
            if elapsed < self.min_request_interval {
                let delay = self.min_request_interval - elapsed;
                debug!("Rate limiting: waiting {:?} before next request", delay);
                sleep(delay).await;
            }
        }

        let url = self.build_klines_url(request)?;
        debug!("Fetching klines from Gate.io: {}", url);

        self.last_request_time = Some(Instant::now());
        
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| {
                self.is_healthy = false;
                ApiError::Network(format!("Request failed: {}", e))
            })?;

        // Check for rate limiting
        if response.status().as_u16() == 429 {
            self.is_healthy = false;
            let retry_after = response.headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60);
            
            return Err(ApiError::RateLimit(format!("Rate limit exceeded, retry after {} seconds", retry_after)));
        }

        // Check for other HTTP errors
        if !response.status().is_success() {
            self.is_healthy = false;
            return Err(ApiError::Http(format!("HTTP {}: {}", response.status(), response.status().canonical_reason().unwrap_or("Unknown"))));
        }

        // Parse rate limit headers
        let rate_limit_info = self.parse_rate_limit_headers(&response);

        // Parse response body
        let body = response
            .text()
            .await
            .map_err(|e| ApiError::Parse(format!("Failed to read response body: {}", e)))?;

        debug!("Received Gate.io response body ({}b): {}", body.len(), body.chars().take(200).collect::<String>());

        // Try sonic-rs first for performance, fallback to serde_json for complex cases
        let raw_klines: Vec<serde_json::Value> = match sonic_rs::from_str::<sonic_rs::Value>(&body) {
            Ok(sonic_value) => {
                // Convert sonic-rs value to serde_json for compatibility with existing parsing logic
                match self.convert_sonic_to_serde_value(&sonic_value)? {
                    serde_json::Value::Array(array) => array,
                    _ => return Err(ApiError::Parse("Expected JSON array from Gate.io API".to_string())),
                }
            }
            Err(sonic_error) => {
                debug!("Sonic-rs parsing failed, falling back to serde_json: {}", sonic_error);
                serde_json::from_str(&body)
                    .map_err(|e| ApiError::Parse(format!("Failed to parse JSON with both sonic-rs and serde_json: {}", e)))?
            }
        };

        let candles = self.parse_klines_response(raw_klines)?;
        
        info!("✅ Fetched {} klines for {} {} from Gate.io", candles.len(), request.symbol, request.interval);
        self.is_healthy = true;

        Ok(ApiResponse {
            data: candles,
            timestamp: chrono::Utc::now().timestamp_millis(),
            rate_limit_info,
        })
    }

    /// Build the complete URL for Gate.io klines request
    fn build_klines_url(&self, request: &ApiRequest) -> Result<String, ApiError> {
        let mut url = format!("{}{}?contract={}&interval={}", 
            self.base_url, 
            request.endpoint.gate_io_path(),
            request.symbol,
            self.map_interval_to_gate_io(&request.interval)?
        );

        if let Some(start_time) = request.start_time {
            // Gate.io uses seconds, not milliseconds
            url.push_str(&format!("&from={}", start_time / 1000));
        }

        if let Some(end_time) = request.end_time {
            // Gate.io uses seconds, not milliseconds  
            url.push_str(&format!("&to={}", end_time / 1000));
        }

        if let Some(limit) = request.limit {
            // Gate.io allows max 1000 candlesticks per request
            let limit = std::cmp::min(limit, 1000);
            url.push_str(&format!("&limit={}", limit));
        }

        Ok(url)
    }

    /// Map standard interval format to Gate.io format
    fn map_interval_to_gate_io(&self, interval: &str) -> Result<String, ApiError> {
        let gate_io_interval = match interval {
            "1m" => "1m",
            "3m" => "3m", 
            "5m" => "5m",
            "15m" => "15m",
            "30m" => "30m",
            "1h" => "1h",
            "2h" => "2h",
            "4h" => "4h",
            "6h" => "6h",
            "8h" => "8h",
            "12h" => "12h",
            "1d" => "1d",
            "3d" => "3d",
            "1w" => "1w",
            "1M" => "1M",
            _ => return Err(ApiError::InvalidTimeframe(format!("Unsupported interval for Gate.io: {}", interval)))
        };
        Ok(gate_io_interval.to_string())
    }

    /// Parse Gate.io klines response into our internal format
    fn parse_klines_response(&self, raw_klines: Vec<serde_json::Value>) -> Result<Vec<FuturesOHLCVCandle>, ApiError> {
        let mut candles = Vec::with_capacity(raw_klines.len());

        for kline_array in raw_klines {
            let array = kline_array.as_array()
                .ok_or_else(|| ApiError::Parse("Expected kline to be an array".to_string()))?;

            if array.len() < 6 {
                return Err(ApiError::Parse(format!("Expected at least 6 elements in Gate.io kline array, got {}", array.len())));
            }

            // Gate.io kline format: [timestamp, volume, close, high, low, open]
            let timestamp = self.parse_timestamp(&array[0])?;
            let volume = self.parse_f64(&array[1])?;
            let close = self.parse_f64(&array[2])?;
            let high = self.parse_f64(&array[3])?;
            let low = self.parse_f64(&array[4])?;
            let open = self.parse_f64(&array[5])?;

            // Calculate close_time from timestamp (Gate.io only provides start time)
            let close_time = timestamp + 60000; // Assume 1 minute interval for now, should be calculated based on actual interval

            let candle = FuturesOHLCVCandle {
                open_time: timestamp,
                close_time,
                open,
                high,
                low,
                close,
                volume,
                number_of_trades: 0, // Gate.io doesn't provide this in klines response
                taker_buy_base_asset_volume: 0.0, // Gate.io doesn't provide this in klines response
                closed: true, // API data is always closed/completed
            };

            candles.push(candle);
        }

        Ok(candles)
    }

    /// Parse timestamp from JSON value (Gate.io uses seconds)
    fn parse_timestamp(&self, value: &serde_json::Value) -> Result<TimestampMS, ApiError> {
        let timestamp_seconds = value.as_i64()
            .ok_or_else(|| ApiError::Parse(format!("Expected timestamp to be i64, got: {:?}", value)))?;
        Ok(timestamp_seconds * 1000) // Convert to milliseconds
    }

    /// Parse f64 from JSON value
    fn parse_f64(&self, value: &serde_json::Value) -> Result<f64, ApiError> {
        match value {
            serde_json::Value::String(s) => s.parse::<f64>()
                .map_err(|_| ApiError::Parse(format!("Failed to parse '{}' as f64", s))),
            serde_json::Value::Number(n) => n.as_f64()
                .ok_or_else(|| ApiError::Parse(format!("Failed to convert number to f64: {:?}", n))),
            _ => Err(ApiError::Parse(format!("Expected string or number, got: {:?}", value))),
        }
    }

    /// Parse u64 from JSON value
    #[allow(dead_code)]
    fn parse_u64(&self, value: &serde_json::Value) -> Result<u64, ApiError> {
        match value {
            serde_json::Value::String(s) => s.parse::<u64>()
                .map_err(|_| ApiError::Parse(format!("Failed to parse '{}' as u64", s))),
            serde_json::Value::Number(n) => n.as_u64()
                .ok_or_else(|| ApiError::Parse(format!("Failed to convert number to u64: {:?}", n))),
            _ => Err(ApiError::Parse(format!("Expected string or number, got: {:?}", value))),
        }
    }

    /// Parse rate limit information from Gate.io response headers
    fn parse_rate_limit_headers(&self, response: &reqwest::Response) -> Option<RateLimitInfo> {
        let headers = response.headers();
        
        // Gate.io rate limiting headers (if available)
        let requests_used = headers.get("x-gate-ratelimit-used")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok());

        let requests_limit = headers.get("x-gate-ratelimit-limit")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .or(Some(1000)); // Conservative default

        let retry_after = headers.get("retry-after")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok());

        if requests_used.is_some() || requests_limit.is_some() || retry_after.is_some() {
            Some(RateLimitInfo {
                requests_used: requests_used.unwrap_or(0),
                requests_limit: requests_limit.unwrap_or(1000),
                reset_time: None, // Gate.io doesn't provide this info
                retry_after,
            })
        } else {
            None
        }
    }

    /// Set minimum request interval for rate limiting
    pub fn set_min_request_interval(&mut self, interval: Duration) {
        self.min_request_interval = interval;
    }

    /// Convert sonic-rs Value to serde_json Value for compatibility with existing parsing logic
    /// This provides the performance benefit of sonic-rs parsing while maintaining API compatibility
    fn convert_sonic_to_serde_value(&self, sonic_value: &sonic_rs::Value) -> Result<serde_json::Value, ApiError> {
        // For now, use the string representation and re-parse with serde_json
        // This is a bridge solution that still provides parsing speed benefits from sonic-rs
        // while maintaining full compatibility with existing code
        let json_str = sonic_value.to_string();
        serde_json::from_str(&json_str)
            .map_err(|e| ApiError::Parse(format!("Failed to convert sonic-rs value to serde_json: {}", e)))
    }

    /// Fetch ticker 24hr data from Gate.io API
    pub async fn fetch_ticker_24hr(&mut self, symbol: &str) -> Result<ApiResponse<Ticker24hr>, ApiError> {
        let request = ApiRequest::new_ticker_24hr(symbol.to_string());
        
        // Apply rate limiting
        if let Some(last_request) = self.last_request_time {
            let elapsed = last_request.elapsed();
            if elapsed < self.min_request_interval {
                let delay = self.min_request_interval - elapsed;
                debug!("Rate limiting: waiting {:?} before next request", delay);
                sleep(delay).await;
            }
        }

        let url = format!("{}{}?contract={}", 
            self.base_url, 
            request.endpoint.gate_io_path(),
            symbol
        );

        debug!("Fetching ticker from Gate.io: {}", url);
        self.last_request_time = Some(Instant::now());
        
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| {
                self.is_healthy = false;
                ApiError::Network(format!("Request failed: {}", e))
            })?;

        // Handle rate limiting and HTTP errors
        if response.status().as_u16() == 429 {
            self.is_healthy = false;
            let retry_after = response.headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60);
            
            return Err(ApiError::RateLimit(format!("Rate limit exceeded, retry after {} seconds", retry_after)));
        }

        if !response.status().is_success() {
            self.is_healthy = false;
            return Err(ApiError::Http(format!("HTTP {}: {}", response.status(), response.status().canonical_reason().unwrap_or("Unknown"))));
        }

        let rate_limit_info = self.parse_rate_limit_headers(&response);
        
        let body = response
            .text()
            .await
            .map_err(|e| ApiError::Parse(format!("Failed to read response body: {}", e)))?;

        debug!("Received ticker response body ({}b): {}", body.len(), body.chars().take(200).collect::<String>());

        // Parse the ticker response - Gate.io returns array for multiple tickers, single object for one
        let ticker_data: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| ApiError::Parse(format!("Failed to parse ticker JSON: {}", e)))?;

        let ticker = self.parse_ticker_response(ticker_data, symbol)?;
        
        info!("✅ Fetched ticker data for {} from Gate.io", symbol);
        self.is_healthy = true;

        Ok(ApiResponse {
            data: ticker,
            timestamp: chrono::Utc::now().timestamp_millis(),
            rate_limit_info,
        })
    }

    /// Fetch exchange info from Gate.io API
    pub async fn fetch_exchange_info(&mut self) -> Result<ApiResponse<ExchangeInfo>, ApiError> {
        let request = ApiRequest::new_exchange_info();
        
        // Apply rate limiting
        if let Some(last_request) = self.last_request_time {
            let elapsed = last_request.elapsed();
            if elapsed < self.min_request_interval {
                let delay = self.min_request_interval - elapsed;
                debug!("Rate limiting: waiting {:?} before next request", delay);
                sleep(delay).await;
            }
        }

        let url = format!("{}{}", 
            self.base_url, 
            request.endpoint.gate_io_path()
        );

        debug!("Fetching exchange info from Gate.io: {}", url);
        self.last_request_time = Some(Instant::now());
        
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| {
                self.is_healthy = false;
                ApiError::Network(format!("Request failed: {}", e))
            })?;

        // Handle rate limiting and HTTP errors
        if response.status().as_u16() == 429 {
            self.is_healthy = false;
            let retry_after = response.headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60);
            
            return Err(ApiError::RateLimit(format!("Rate limit exceeded, retry after {} seconds", retry_after)));
        }

        if !response.status().is_success() {
            self.is_healthy = false;
            return Err(ApiError::Http(format!("HTTP {}: {}", response.status(), response.status().canonical_reason().unwrap_or("Unknown"))));
        }

        let rate_limit_info = self.parse_rate_limit_headers(&response);
        
        let body = response
            .text()
            .await
            .map_err(|e| ApiError::Parse(format!("Failed to read response body: {}", e)))?;

        debug!("Received exchange info response body ({}b)", body.len());

        // Parse the exchange info response
        let exchange_data: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| ApiError::Parse(format!("Failed to parse exchange info JSON: {}", e)))?;

        let exchange_info = self.parse_exchange_info_response(exchange_data)?;
        
        info!("✅ Fetched exchange info with {} symbols from Gate.io", exchange_info.symbols.len());
        self.is_healthy = true;

        Ok(ApiResponse {
            data: exchange_info,
            timestamp: chrono::Utc::now().timestamp_millis(),
            rate_limit_info,
        })
    }

    /// Parse f64 from object field with default value
    fn parse_f64_field(&self, obj: &serde_json::Map<String, serde_json::Value>, field: &str) -> Result<f64, ApiError> {
        obj.get(field)
            .map(|v| self.parse_f64(v))
            .unwrap_or(Ok(0.0))
    }

    /// Parse Gate.io ticker 24hr response
    fn parse_ticker_response(&self, ticker_data: serde_json::Value, symbol: &str) -> Result<Ticker24hr, ApiError> {
        // Gate.io may return array or object depending on query
        let obj = if let Some(array) = ticker_data.as_array() {
            // Find the ticker for the specific symbol
            array.iter()
                .find(|item| {
                    item.as_object()
                        .and_then(|obj| obj.get("contract"))
                        .and_then(|v| v.as_str())
                        == Some(symbol)
                })
                .and_then(|v| v.as_object())
                .ok_or_else(|| ApiError::Parse(format!("Ticker not found for symbol: {}", symbol)))?
        } else {
            ticker_data.as_object()
                .ok_or_else(|| ApiError::Parse("Expected ticker response to be an object or array".to_string()))?
        };

        Ok(Ticker24hr {
            symbol: obj.get("contract")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| symbol.to_string()),
            price_change: self.parse_f64_field(obj, "change_percentage")?,
            price_change_percent: self.parse_f64_field(obj, "change_percentage")?,
            weighted_avg_price: 0.0, // Gate.io doesn't provide this
            last_price: self.parse_f64_field(obj, "last")?,
            last_qty: 0.0, // Gate.io doesn't provide this in ticker
            open_price: 0.0, // Gate.io doesn't provide this directly
            high_price: self.parse_f64_field(obj, "high_24h")?,
            low_price: self.parse_f64_field(obj, "low_24h")?,
            volume: self.parse_f64_field(obj, "volume_24h")?,
            quote_volume: 0.0, // Gate.io doesn't provide this
            open_time: 0, // Gate.io doesn't provide this
            close_time: chrono::Utc::now().timestamp_millis(),
            count: 0, // Gate.io doesn't provide this
        })
    }

    /// Parse Gate.io exchange info response
    fn parse_exchange_info_response(&self, exchange_data: serde_json::Value) -> Result<ExchangeInfo, ApiError> {
        let symbols_array = exchange_data.as_array()
            .ok_or_else(|| ApiError::Parse("Expected exchange info response to be an array".to_string()))?;

        let mut symbols = Vec::new();
        for symbol_value in symbols_array {
            let symbol_obj = symbol_value.as_object()
                .ok_or_else(|| ApiError::Parse("Symbol should be an object".to_string()))?;

            symbols.push(SymbolInfo {
                symbol: symbol_obj.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| ApiError::Parse("Missing symbol name".to_string()))?,
                status: symbol_obj.get("trade_status")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_string()),
                base_asset: symbol_obj.get("underlying")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "".to_string()),
                quote_asset: symbol_obj.get("settle")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "".to_string()),
                price_precision: 8, // Gate.io default
                quantity_precision: 8, // Gate.io default
            });
        }

        // Gate.io doesn't expose detailed rate limit info in exchange info
        let rate_limits = vec![
            RateLimit {
                rate_limit_type: "REQUEST_WEIGHT".to_string(),
                interval: "MINUTE".to_string(),
                interval_num: 1,
                limit: 1000,
            }
        ];

        Ok(ExchangeInfo {
            timezone: "UTC".to_string(),
            server_time: chrono::Utc::now().timestamp_millis(),
            symbols,
            rate_limits,
        })
    }
}

#[async_trait]
impl Exchange for GateIoKlinesClient {
    /// Get OHLCV klines data for a symbol
    async fn get_klines(&mut self, request: &ApiRequest) -> Result<ApiResponse<Vec<FuturesOHLCVCandle>>, ApiError> {
        self.fetch_klines(request).await
    }
    
    /// Get 24hr ticker statistics for a symbol
    async fn get_ticker_24hr(&mut self, symbol: &str) -> Result<ApiResponse<Ticker24hr>, ApiError> {
        self.fetch_ticker_24hr(symbol).await
    }
    
    /// Get exchange information (symbols, rate limits, etc.)
    async fn get_exchange_info(&mut self) -> Result<ApiResponse<ExchangeInfo>, ApiError> {
        self.fetch_exchange_info().await
    }
    
    /// Set the minimum interval between API requests for rate limiting
    async fn set_rate_limit(&mut self, min_interval: Duration) {
        self.min_request_interval = min_interval;
        debug!("Updated Gate.io rate limit interval to {:?}", min_interval);
    }
    
    /// Get the exchange name for identification
    fn get_exchange_name(&self) -> &'static str {
        "gate_io"
    }
    
    /// Check if the exchange connection is healthy
    fn is_healthy(&self) -> bool {
        self.is_healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::ApiRequest;

    #[test]
    fn test_new_gate_io_client() {
        let client = GateIoKlinesClient::new("https://api.gateio.ws".to_string()).unwrap();
        assert!(client.is_healthy());
        assert_eq!(client.get_exchange_name(), "gate_io");
        assert_eq!(client.base_url, "https://api.gateio.ws");
    }

    #[test]
    fn test_from_config() {
        let config = ApiConfig::gate_io_futures();
        let client = GateIoKlinesClient::from_config(config).unwrap();
        assert!(client.is_healthy());
        assert_eq!(client.get_exchange_name(), "gate_io");
        assert_eq!(client.base_url, "https://api.gateio.ws");
    }

    #[test]
    fn test_build_klines_url() {
        let client = GateIoKlinesClient::new("https://api.gateio.ws".to_string()).unwrap();
        
        let request = ApiRequest::new_klines("BTC_USDT".to_string(), "1m".to_string())
            .with_time_range(1640995200000, 1641081600000)
            .with_limit(500);

        let url = client.build_klines_url(&request).unwrap();
        assert!(url.contains("contract=BTC_USDT"));
        assert!(url.contains("interval=1m"));
        assert!(url.contains("from=1640995200")); // Seconds, not milliseconds
        assert!(url.contains("to=1641081600"));
        assert!(url.contains("limit=500"));
    }

    #[test]
    fn test_interval_mapping() {
        let client = GateIoKlinesClient::new("https://api.gateio.ws".to_string()).unwrap();
        
        assert_eq!(client.map_interval_to_gate_io("1m").unwrap(), "1m");
        assert_eq!(client.map_interval_to_gate_io("5m").unwrap(), "5m");
        assert_eq!(client.map_interval_to_gate_io("1h").unwrap(), "1h");
        assert_eq!(client.map_interval_to_gate_io("1d").unwrap(), "1d");
        
        assert!(client.map_interval_to_gate_io("invalid").is_err());
    }

    #[test]
    fn test_parse_klines_response() {
        let client = GateIoKlinesClient::new("https://api.gateio.ws".to_string()).unwrap();
        
        let raw_response = r#"[
            [
                1640995200,
                "3.45",
                "46271.02",
                "46271.02",
                "46222.01",
                "46222.01"
            ]
        ]"#;

        let raw_klines: Vec<serde_json::Value> = serde_json::from_str(raw_response).unwrap();
        let candles = client.parse_klines_response(raw_klines).unwrap();
        
        assert_eq!(candles.len(), 1);
        let candle = &candles[0];
        assert_eq!(candle.open_time, 1640995200000); // Converted to milliseconds
        assert_eq!(candle.open, 46222.01);
        assert_eq!(candle.volume, 3.45);
        assert_eq!(candle.high, 46271.02);
        assert_eq!(candle.low, 46222.01);
        assert_eq!(candle.close, 46271.02);
        assert!(candle.closed);
    }

    #[tokio::test]
    async fn test_rate_limit_setting() {
        let mut client = GateIoKlinesClient::new("https://api.gateio.ws".to_string()).unwrap();
        
        let new_interval = Duration::from_millis(100);
        client.set_rate_limit(new_interval).await;
        assert_eq!(client.min_request_interval, new_interval);
    }

    #[test]
    fn test_exchange_trait_implementation() {
        let client = GateIoKlinesClient::new("https://api.gateio.ws".to_string()).unwrap();
        
        // Test Exchange trait methods are available
        assert_eq!(client.get_exchange_name(), "gate_io");
        assert!(client.is_healthy());
    }
}