use reqwest;
use serde_json;
use std::time::Duration;
use tokio::time::{sleep, Instant};
use tracing::{debug, info};

use crate::api::types::{ApiError, ApiRequest, ApiResponse, RateLimitInfo};
use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};

/// Binance Futures API client for klines data
pub struct BinanceKlinesClient {
    client: reqwest::Client,
    base_url: String,
    last_request_time: Option<Instant>,
    min_request_interval: Duration,
}

impl BinanceKlinesClient {
    /// Create a new Binance klines client
    pub fn new(base_url: String) -> Result<Self, ApiError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ApiError::Network(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            base_url,
            last_request_time: None,
            min_request_interval: Duration::from_millis(50), // 20 requests per second max
        })
    }

    /// Fetch klines data from Binance API
    pub async fn fetch_klines(&mut self, request: ApiRequest) -> Result<ApiResponse<Vec<FuturesOHLCVCandle>>, ApiError> {
        // Rate limiting: ensure minimum interval between requests
        if let Some(last_request) = self.last_request_time {
            let elapsed = last_request.elapsed();
            if elapsed < self.min_request_interval {
                let delay = self.min_request_interval - elapsed;
                debug!("Rate limiting: waiting {:?} before next request", delay);
                sleep(delay).await;
            }
        }

        let url = self.build_klines_url(&request)?;
        debug!("Fetching klines from: {}", url);

        self.last_request_time = Some(Instant::now());
        
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| ApiError::Network(format!("Request failed: {}", e)))?;

        // Check for rate limiting
        if response.status().as_u16() == 429 {
            let retry_after = response.headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60);
            
            return Err(ApiError::RateLimit(format!("Rate limit exceeded, retry after {} seconds", retry_after)));
        }

        // Check for other HTTP errors
        if !response.status().is_success() {
            return Err(ApiError::Http(format!("HTTP {}: {}", response.status(), response.status().canonical_reason().unwrap_or("Unknown"))));
        }

        // Parse rate limit headers
        let rate_limit_info = self.parse_rate_limit_headers(&response);

        // Parse response body
        let body = response
            .text()
            .await
            .map_err(|e| ApiError::Parse(format!("Failed to read response body: {}", e)))?;

        debug!("Received response body ({}b): {}", body.len(), body.chars().take(200).collect::<String>());

        let raw_klines: Vec<serde_json::Value> = serde_json::from_str(&body)
            .map_err(|e| ApiError::Parse(format!("Failed to parse JSON: {}", e)))?;

        let candles = self.parse_klines_response(raw_klines)?;
        
        info!("✅ Fetched {} klines for {} {}", candles.len(), request.symbol, request.interval);

        Ok(ApiResponse {
            data: candles,
            timestamp: chrono::Utc::now().timestamp_millis(),
            rate_limit_info,
        })
    }

    /// Build the complete URL for klines request
    fn build_klines_url(&self, request: &ApiRequest) -> Result<String, ApiError> {
        let mut url = format!("{}{}?symbol={}&interval={}", 
            self.base_url, 
            request.endpoint.binance_path(),
            request.symbol,
            request.interval
        );

        if let Some(start_time) = request.start_time {
            url.push_str(&format!("&startTime={}", start_time));
        }

        if let Some(end_time) = request.end_time {
            url.push_str(&format!("&endTime={}", end_time));
        }

        if let Some(limit) = request.limit {
            // Binance allows max 1000 klines per request
            let limit = std::cmp::min(limit, 1000);
            url.push_str(&format!("&limit={}", limit));
        }

        Ok(url)
    }

    /// Parse Binance klines response into our internal format
    fn parse_klines_response(&self, raw_klines: Vec<serde_json::Value>) -> Result<Vec<FuturesOHLCVCandle>, ApiError> {
        let mut candles = Vec::with_capacity(raw_klines.len());

        for kline_array in raw_klines {
            let array = kline_array.as_array()
                .ok_or_else(|| ApiError::Parse("Expected kline to be an array".to_string()))?;

            if array.len() < 12 {
                return Err(ApiError::Parse(format!("Expected at least 12 elements in kline array, got {}", array.len())));
            }

            let open_time = self.parse_timestamp(&array[0])?;
            let close_time = self.parse_timestamp(&array[6])?;
            let open = self.parse_f64(&array[1])?;
            let high = self.parse_f64(&array[2])?;
            let low = self.parse_f64(&array[3])?;
            let close = self.parse_f64(&array[4])?;
            let volume = self.parse_f64(&array[5])?;
            let number_of_trades = self.parse_u64(&array[8])?;
            let taker_buy_base_asset_volume = self.parse_f64(&array[9])?;

            let candle = FuturesOHLCVCandle {
                open_time,
                close_time,
                open,
                high,
                low,
                close,
                volume,
                number_of_trades,
                taker_buy_base_asset_volume,
                closed: true, // API data is always closed/completed
            };

            candles.push(candle);
        }

        Ok(candles)
    }

    /// Parse timestamp from JSON value
    fn parse_timestamp(&self, value: &serde_json::Value) -> Result<TimestampMS, ApiError> {
        value.as_i64()
            .ok_or_else(|| ApiError::Parse(format!("Expected timestamp to be i64, got: {:?}", value)))
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
    fn parse_u64(&self, value: &serde_json::Value) -> Result<u64, ApiError> {
        match value {
            serde_json::Value::String(s) => s.parse::<u64>()
                .map_err(|_| ApiError::Parse(format!("Failed to parse '{}' as u64", s))),
            serde_json::Value::Number(n) => n.as_u64()
                .ok_or_else(|| ApiError::Parse(format!("Failed to convert number to u64: {:?}", n))),
            _ => Err(ApiError::Parse(format!("Expected string or number, got: {:?}", value))),
        }
    }

    /// Parse rate limit information from response headers
    fn parse_rate_limit_headers(&self, response: &reqwest::Response) -> Option<RateLimitInfo> {
        let headers = response.headers();
        
        // Binance uses these headers for rate limiting
        let requests_used = headers.get("x-mbx-used-weight-1m")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok());

        let requests_limit = headers.get("x-mbx-used-weight-1m")
            .map(|_| 1200u32); // Binance Futures limit

        let retry_after = headers.get("retry-after")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok());

        if requests_used.is_some() || requests_limit.is_some() || retry_after.is_some() {
            Some(RateLimitInfo {
                requests_used: requests_used.unwrap_or(0),
                requests_limit: requests_limit.unwrap_or(1200),
                reset_time: None, // Binance doesn't provide this info
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::ApiRequest;

    #[test]
    fn test_build_klines_url() {
        let client = BinanceKlinesClient::new("https://fapi.binance.com".to_string()).unwrap();
        
        let request = ApiRequest::new_klines("BTCUSDT".to_string(), "1m".to_string())
            .with_time_range(1640995200000, 1641081600000)
            .with_limit(500);

        let url = client.build_klines_url(&request).unwrap();
        assert!(url.contains("symbol=BTCUSDT"));
        assert!(url.contains("interval=1m"));
        assert!(url.contains("startTime=1640995200000"));
        assert!(url.contains("endTime=1641081600000"));
        assert!(url.contains("limit=500"));
    }

    #[test]
    fn test_parse_klines_response() {
        let client = BinanceKlinesClient::new("https://fapi.binance.com".to_string()).unwrap();
        
        let raw_response = r#"[
            [
                1640995200000,
                "46222.01",
                "46271.02",
                "46222.01",
                "46271.02",
                "3.45",
                1640995259999,
                "159633.38",
                10,
                "1.72",
                "79516.69",
                "0"
            ]
        ]"#;

        let raw_klines: Vec<serde_json::Value> = serde_json::from_str(raw_response).unwrap();
        let candles = client.parse_klines_response(raw_klines).unwrap();
        
        assert_eq!(candles.len(), 1);
        let candle = &candles[0];
        assert_eq!(candle.open_time, 1640995200000);
        assert_eq!(candle.open, 46222.01);
        assert_eq!(candle.volume, 3.45);
        assert_eq!(candle.number_of_trades, 10);
        assert_eq!(candle.taker_buy_base_asset_volume, 1.72);
        assert!(candle.closed);
    }
}