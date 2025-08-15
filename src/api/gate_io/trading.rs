use async_trait::async_trait;
use serde_json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{sleep, Instant};
use tracing::{debug, info};
use reqwest;

use crate::api::exchange::{Exchange, TradingExchange, Ticker24hr, ExchangeInfo};
use crate::api::types::{
    ApiError, ApiRequest, ApiResponse, ApiConfig,
    OrderRequest, OrderResponse, ModifyOrderRequest, Order, Position, Balance,
    OrderSide, OrderType, OrderStatus, PositionSide
};
use crate::api::trading_metrics::{get_trading_metrics, TradingOperation};
use crate::historical::structs::FuturesOHLCVCandle;
use super::klines::GateIoKlinesClient;

/// Gate.io Futures Trading Client with WebSocket support
pub struct GateIoTradingClient {
    /// HTTP client for trading API requests
    client: reqwest::Client,
    /// Base URL for Gate.io API
    base_url: String,
    /// API key for authenticated requests
    api_key: Option<String>,
    /// API secret for signature generation
    api_secret: Option<String>,
    /// Last request time for rate limiting
    last_request_time: Arc<RwLock<Option<Instant>>>,
    /// Minimum interval between requests
    min_request_interval: Duration,
    /// Health status of the connection
    is_healthy: Arc<RwLock<bool>>,
    /// Embedded klines client for Exchange trait implementation
    klines_client: GateIoKlinesClient,
}

impl GateIoTradingClient {
    /// Create a new Gate.io trading client
    pub fn new(base_url: String, api_key: Option<String>, api_secret: Option<String>) -> Result<Self, ApiError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ApiError::Network(format!("Failed to create HTTP client: {}", e)))?;

        let klines_client = GateIoKlinesClient::new(base_url.clone())?;

        Ok(Self {
            client,
            base_url: base_url.clone(),
            api_key,
            api_secret,
            last_request_time: Arc::new(RwLock::new(None)),
            min_request_interval: Duration::from_millis(50),
            is_healthy: Arc::new(RwLock::new(true)),
            klines_client,
        })
    }

    /// Create a new Gate.io trading client from configuration
    pub fn from_config(config: ApiConfig, api_key: Option<String>, api_secret: Option<String>) -> Result<Self, ApiError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()
            .map_err(|e| ApiError::Network(format!("Failed to create HTTP client: {}", e)))?;

        let klines_client = GateIoKlinesClient::from_config(config.clone())?;

        Ok(Self {
            client,
            base_url: config.base_url,
            api_key,
            api_secret,
            last_request_time: Arc::new(RwLock::new(None)),
            min_request_interval: Duration::from_millis(config.rate_limit_delay_ms),
            is_healthy: Arc::new(RwLock::new(true)),
            klines_client,
        })
    }

    /// Apply rate limiting before requests
    async fn apply_rate_limiting(&self) {
        let last_request = self.last_request_time.read().await;
        if let Some(last_request_time) = *last_request {
            let elapsed = last_request_time.elapsed();
            if elapsed < self.min_request_interval {
                let delay = self.min_request_interval - elapsed;
                debug!("Rate limiting: waiting {:?} before next request", delay);
                sleep(delay).await;
            }
        }
        drop(last_request);
        
        let mut last_request = self.last_request_time.write().await;
        *last_request = Some(Instant::now());
    }

    /// Generate authentication headers for Gate.io API
    fn generate_auth_headers(&self, method: &str, path: &str, body: &str) -> Result<std::collections::HashMap<String, String>, ApiError> {
        let api_key = self.api_key.as_ref()
            .ok_or_else(|| ApiError::Authentication("API key required for trading operations".to_string()))?;
        
        let api_secret = self.api_secret.as_ref()
            .ok_or_else(|| ApiError::Authentication("API secret required for trading operations".to_string()))?;

        let timestamp = chrono::Utc::now().timestamp().to_string();
        let nonce = chrono::Utc::now().timestamp_millis().to_string();

        // Gate.io signature format: METHOD\nPATH\nBODY\nTIMESTAMP\nNONCE
        let payload = format!("{}\n{}\n{}\n{}\n{}", method, path, body, timestamp, nonce);
        
        // Generate HMAC-SHA256 signature
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        
        let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes())
            .map_err(|_| ApiError::Authentication("Invalid API secret".to_string()))?;
        mac.update(payload.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let mut headers = std::collections::HashMap::new();
        headers.insert("KEY".to_string(), api_key.clone());
        headers.insert("Timestamp".to_string(), timestamp);
        headers.insert("SIGN".to_string(), signature);
        headers.insert("Content-Type".to_string(), "application/json".to_string());

        Ok(headers)
    }

    /// Send authenticated request to Gate.io API
    async fn send_authenticated_request(&self, method: &str, endpoint: &str, body: Option<&str>) -> Result<serde_json::Value, ApiError> {
        self.apply_rate_limiting().await;

        let url = format!("{}{}", self.base_url, endpoint);
        let body_str = body.unwrap_or("");
        
        // Measure authentication generation latency
        let auth_headers = if let Some(metrics) = get_trading_metrics() {
            metrics.measure_operation(TradingOperation::Authentication, || {
                self.generate_auth_headers(method, endpoint, body_str)
            }).await?
        } else {
            self.generate_auth_headers(method, endpoint, body_str)?
        };
        
        debug!("Sending {} request to Gate.io: {}", method, endpoint);
        
        let mut request_builder = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "PUT" => self.client.put(&url),
            "DELETE" => self.client.delete(&url),
            _ => return Err(ApiError::Network(format!("Unsupported HTTP method: {}", method))),
        };

        // Add authentication headers
        for (key, value) in auth_headers {
            request_builder = request_builder.header(key, value);
        }

        // Add body if provided
        if let Some(body_content) = body {
            request_builder = request_builder.body(body_content.to_string());
        }

        let response = request_builder
            .send()
            .await
            .map_err(|e| {
                tokio::spawn({
                    let is_healthy = self.is_healthy.clone();
                    async move {
                        *is_healthy.write().await = false;
                    }
                });
                ApiError::Network(format!("Request failed: {}", e))
            })?;

        // Handle rate limiting
        if response.status().as_u16() == 429 {
            tokio::spawn({
                let is_healthy = self.is_healthy.clone();
                async move {
                    *is_healthy.write().await = false;
                }
            });
            let retry_after = response.headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(60);
            
            return Err(ApiError::RateLimit(format!("Rate limit exceeded, retry after {} seconds", retry_after)));
        }

        // Check for other HTTP errors
        if !response.status().is_success() {
            let status = response.status();
            tokio::spawn({
                let is_healthy = self.is_healthy.clone();
                async move {
                    *is_healthy.write().await = false;
                }
            });
            
            let error_body = response.text().await.unwrap_or_default();
            return Err(ApiError::Http(format!("HTTP {}: {}", status, error_body)));
        }

        // Parse response
        let body = response
            .text()
            .await
            .map_err(|e| ApiError::Parse(format!("Failed to read response body: {}", e)))?;

        let json_response: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| ApiError::Parse(format!("Failed to parse JSON response: {}", e)))?;

        // Mark as healthy on successful request
        tokio::spawn({
            let is_healthy = self.is_healthy.clone();
            async move {
                *is_healthy.write().await = true;
            }
        });

        Ok(json_response)
    }

    /// Parse Gate.io order response into our Order structure
    fn parse_order_response(&self, gate_io_order: &serde_json::Value) -> Result<Order, ApiError> {
        let obj = gate_io_order.as_object()
            .ok_or_else(|| ApiError::Parse("Expected order to be an object".to_string()))?;

        let side_str = obj.get("side")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ApiError::Parse("Missing order side".to_string()))?;
        
        let side = match side_str {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            _ => return Err(ApiError::Parse(format!("Unknown order side: {}", side_str))),
        };

        let order_type_str = obj.get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("limit");
        
        let order_type = match order_type_str {
            "market" => OrderType::Market,
            "limit" => OrderType::Limit,
            _ => OrderType::Limit, // Default to limit
        };

        let status_str = obj.get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("new");
        
        let status = match status_str {
            "open" | "new" => OrderStatus::New,
            "partially_filled" => OrderStatus::PartiallyFilled,
            "filled" => OrderStatus::Filled,
            "cancelled" => OrderStatus::Cancelled,
            "rejected" => OrderStatus::Rejected,
            _ => OrderStatus::New,
        };

        let quantity: f64 = match obj.get("size") {
            Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => 0.0,
        };

        let price: Option<f64> = match obj.get("price") {
            Some(serde_json::Value::String(s)) => s.parse().ok(),
            Some(serde_json::Value::Number(n)) => n.as_f64(),
            _ => None,
        };

        Ok(Order {
            id: obj.get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            symbol: obj.get("contract")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            side,
            order_type,
            quantity,
            price,
            status,
            timestamp: obj.get("create_time")
                .and_then(|v| v.as_i64())
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis()) * 1000, // Convert to milliseconds
        })
    }

    /// Parse Gate.io position response into our Position structure
    fn parse_position_response(&self, gate_io_position: &serde_json::Value) -> Result<Position, ApiError> {
        let obj = gate_io_position.as_object()
            .ok_or_else(|| ApiError::Parse("Expected position to be an object".to_string()))?;

        let size: f64 = match obj.get("size") {
            Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => 0.0,
        };

        let side = if size > 0.0 {
            PositionSide::Long
        } else if size < 0.0 {
            PositionSide::Short
        } else {
            PositionSide::None
        };

        Ok(Position {
            symbol: obj.get("contract")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            side,
            size: size.abs(),
            entry_price: match obj.get("entry_price") {
                Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
                Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                _ => 0.0,
            },
            mark_price: match obj.get("mark_price") {
                Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
                Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                _ => 0.0,
            },
            unrealized_pnl: match obj.get("unrealised_pnl") {
                Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
                Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                _ => 0.0,
            },
            margin_required: match obj.get("margin") {
                Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
                Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                _ => 0.0,
            },
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Parse Gate.io balance response into our Balance structure
    fn parse_balance_response(&self, gate_io_balance: &serde_json::Value) -> Result<Balance, ApiError> {
        let obj = gate_io_balance.as_object()
            .ok_or_else(|| ApiError::Parse("Expected balance to be an object".to_string()))?;

        let available: f64 = match obj.get("available") {
            Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => 0.0,
        };

        let locked: f64 = match obj.get("freeze") {
            Some(serde_json::Value::String(s)) => s.parse().unwrap_or(0.0),
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => 0.0,
        };

        Ok(Balance {
            asset: obj.get("currency")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            available,
            locked,
            total: available + locked,
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }
}

#[async_trait]
impl Exchange for GateIoTradingClient {
    async fn get_klines(&mut self, request: &ApiRequest) -> Result<ApiResponse<Vec<FuturesOHLCVCandle>>, ApiError> {
        self.klines_client.get_klines(request).await
    }
    
    async fn get_ticker_24hr(&mut self, symbol: &str) -> Result<ApiResponse<Ticker24hr>, ApiError> {
        self.klines_client.get_ticker_24hr(symbol).await
    }
    
    async fn get_exchange_info(&mut self) -> Result<ApiResponse<ExchangeInfo>, ApiError> {
        self.klines_client.get_exchange_info().await
    }
    
    async fn set_rate_limit(&mut self, min_interval: Duration) {
        self.min_request_interval = min_interval;
        self.klines_client.set_rate_limit(min_interval).await;
        debug!("Updated Gate.io trading client rate limit interval to {:?}", min_interval);
    }
    
    fn get_exchange_name(&self) -> &'static str {
        "gate_io_trading"
    }
    
    fn is_healthy(&self) -> bool {
        // Use blocking read since this is a sync method
        match self.is_healthy.try_read() {
            Ok(healthy) => *healthy,
            Err(_) => true, // Default to healthy if we can't acquire lock
        }
    }
}

#[async_trait]
impl TradingExchange for GateIoTradingClient {
    async fn place_order(&mut self, order_request: &OrderRequest) -> Result<OrderResponse, ApiError> {
        let start_time = std::time::Instant::now();
        
        // Validate the order request (measure validation latency)
        if let Some(metrics) = get_trading_metrics() {
            metrics.measure_operation(TradingOperation::JsonParsing, || {
                order_request.validate()
            }).await?;
        } else {
            order_request.validate()?;
        }

        let order_type_str = match order_request.order_type {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            _ => return Err(ApiError::InvalidTimeframe("Order type not supported by Gate.io".to_string())),
        };

        let side_str = match order_request.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        let mut order_body = serde_json::json!({
            "contract": order_request.symbol,
            "side": side_str,
            "type": order_type_str,
            "size": order_request.quantity.to_string(),
            "reduce_only": order_request.reduce_only
        });

        // Add price for limit orders
        if let Some(price) = order_request.price {
            order_body["price"] = serde_json::json!(price.to_string());
        }

        // Add time in force if specified
        if let Some(ref tif) = order_request.time_in_force {
            order_body["time_in_force"] = serde_json::json!(tif);
        }

        // Add client order ID if specified
        if let Some(ref client_order_id) = order_request.client_order_id {
            order_body["text"] = serde_json::json!(client_order_id);
        }

        let body_str = serde_json::to_string(&order_body)
            .map_err(|e| ApiError::Parse(format!("Failed to serialize order request: {}", e)))?;

        debug!("Placing Gate.io order: {}", body_str);

        // Measure the complete order placement latency
        let result = if let Some(metrics) = get_trading_metrics() {
            let response = metrics.measure_async_operation(TradingOperation::OrderPlacement, || async {
                self.send_authenticated_request("POST", "/api/v4/futures/usdt/orders", Some(&body_str)).await
            }).await?;
            
            let order = metrics.measure_operation(TradingOperation::JsonParsing, || {
                self.parse_order_response(&response)
            }).await?;
            
            // Record end-to-end latency
            let end_to_end_duration = start_time.elapsed();
            metrics.record_latency(TradingOperation::EndToEndWorkflow, end_to_end_duration).await;
            
            info!("✅ Successfully placed Gate.io order: {} {:?} {} at {} ({}μs)", 
                order.symbol, order.side, order.quantity, order.price.unwrap_or(0.0), 
                end_to_end_duration.as_micros());

            OrderResponse {
                order,
                success: true,
                message: Some("Order placed successfully".to_string()),
                timestamp: chrono::Utc::now().timestamp_millis(),
            }
        } else {
            let response = self.send_authenticated_request("POST", "/api/v4/futures/usdt/orders", Some(&body_str)).await?;
            let order = self.parse_order_response(&response)?;
            
            info!("✅ Successfully placed Gate.io order: {} {:?} {} at {}", 
                order.symbol, order.side, order.quantity, order.price.unwrap_or(0.0));

            OrderResponse {
                order,
                success: true,
                message: Some("Order placed successfully".to_string()),
                timestamp: chrono::Utc::now().timestamp_millis(),
            }
        };

        Ok(result)
    }

    async fn cancel_order(&mut self, symbol: &str, order_id: &str) -> Result<OrderResponse, ApiError> {
        let endpoint = format!("/api/v4/futures/usdt/orders/{}", order_id);
        
        debug!("Cancelling Gate.io order: {} for symbol: {}", order_id, symbol);
        
        let response = self.send_authenticated_request("DELETE", &endpoint, None).await?;
        let order = self.parse_order_response(&response)?;
        
        info!("✅ Successfully cancelled Gate.io order: {}", order_id);

        Ok(OrderResponse {
            order,
            success: true,
            message: Some("Order cancelled successfully".to_string()),
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }

    async fn modify_order(&mut self, modify_request: &ModifyOrderRequest) -> Result<OrderResponse, ApiError> {
        let mut modify_body = serde_json::json!({});

        if let Some(quantity) = modify_request.quantity {
            modify_body["size"] = serde_json::json!(quantity.to_string());
        }

        if let Some(price) = modify_request.price {
            modify_body["price"] = serde_json::json!(price.to_string());
        }

        let body_str = serde_json::to_string(&modify_body)
            .map_err(|e| ApiError::Parse(format!("Failed to serialize modify request: {}", e)))?;

        let endpoint = format!("/api/v4/futures/usdt/orders/{}", modify_request.order_id);
        
        debug!("Modifying Gate.io order: {} with body: {}", modify_request.order_id, body_str);
        
        let response = self.send_authenticated_request("PUT", &endpoint, Some(&body_str)).await?;
        let order = self.parse_order_response(&response)?;
        
        info!("✅ Successfully modified Gate.io order: {}", modify_request.order_id);

        Ok(OrderResponse {
            order,
            success: true,
            message: Some("Order modified successfully".to_string()),
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }

    async fn get_open_orders(&mut self, symbol: Option<&str>) -> Result<Vec<Order>, ApiError> {
        let mut endpoint = "/api/v4/futures/usdt/orders".to_string();
        
        if let Some(symbol) = symbol {
            endpoint.push_str(&format!("?contract={}&status=open", symbol));
        } else {
            endpoint.push_str("?status=open");
        }
        
        debug!("Fetching open orders from Gate.io: {}", endpoint);
        
        let response = self.send_authenticated_request("GET", &endpoint, None).await?;
        
        let orders_array = response.as_array()
            .ok_or_else(|| ApiError::Parse("Expected orders response to be an array".to_string()))?;

        let mut orders = Vec::new();
        for order_value in orders_array {
            let order = self.parse_order_response(order_value)?;
            orders.push(order);
        }
        
        info!("✅ Fetched {} open orders from Gate.io", orders.len());
        Ok(orders)
    }

    async fn get_positions(&mut self, symbol: Option<&str>) -> Result<Vec<Position>, ApiError> {
        let mut endpoint = "/api/v4/futures/usdt/positions".to_string();
        
        if let Some(symbol) = symbol {
            endpoint.push_str(&format!("?contract={}", symbol));
        }
        
        debug!("Fetching positions from Gate.io: {}", endpoint);
        
        let response = self.send_authenticated_request("GET", &endpoint, None).await?;
        
        let positions_array = response.as_array()
            .ok_or_else(|| ApiError::Parse("Expected positions response to be an array".to_string()))?;

        let mut positions = Vec::new();
        for position_value in positions_array {
            let position = self.parse_position_response(position_value)?;
            // Only include positions with non-zero size
            if position.size > 0.0 {
                positions.push(position);
            }
        }
        
        info!("✅ Fetched {} positions from Gate.io", positions.len());
        Ok(positions)
    }

    async fn get_balance(&mut self) -> Result<Vec<Balance>, ApiError> {
        let endpoint = "/api/v4/futures/usdt/accounts";
        
        debug!("Fetching account balance from Gate.io");
        
        let response = self.send_authenticated_request("GET", endpoint, None).await?;
        
        let balances_array = response.as_array()
            .ok_or_else(|| ApiError::Parse("Expected balance response to be an array".to_string()))?;

        let mut balances = Vec::new();
        for balance_value in balances_array {
            let balance = self.parse_balance_response(balance_value)?;
            balances.push(balance);
        }
        
        info!("✅ Fetched {} account balances from Gate.io", balances.len());
        Ok(balances)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::{OrderSide, OrderType};

    #[test]
    fn test_new_trading_client() {
        let client = GateIoTradingClient::new(
            "https://api.gateio.ws".to_string(), 
            Some("test_key".to_string()),
            Some("test_secret".to_string())
        ).unwrap();
        
        assert_eq!(client.get_exchange_name(), "gate_io_trading");
        assert!(client.is_healthy());
    }

    #[test]
    fn test_parse_order_response() {
        let client = GateIoTradingClient::new(
            "https://api.gateio.ws".to_string(),
            None,
            None
        ).unwrap();

        let order_json = serde_json::json!({
            "id": "12345",
            "contract": "BTC_USDT",
            "side": "buy",
            "type": "limit",
            "size": "0.001",
            "price": "50000.0",
            "status": "open",
            "create_time": 1640995200
        });

        let order = client.parse_order_response(&order_json).unwrap();
        assert_eq!(order.id, "12345");
        assert_eq!(order.symbol, "BTC_USDT");
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.quantity, 0.001);
        assert_eq!(order.price, Some(50000.0));
        assert_eq!(order.status, OrderStatus::New);
    }

    #[test]
    fn test_parse_position_response() {
        let client = GateIoTradingClient::new(
            "https://api.gateio.ws".to_string(),
            None,
            None
        ).unwrap();

        let position_json = serde_json::json!({
            "contract": "BTC_USDT",
            "size": "0.5",
            "entry_price": "50000.0",
            "mark_price": "50100.0",
            "unrealised_pnl": "50.0",
            "margin": "1000.0"
        });

        let position = client.parse_position_response(&position_json).unwrap();
        assert_eq!(position.symbol, "BTC_USDT");
        assert_eq!(position.side, PositionSide::Long);
        assert_eq!(position.size, 0.5);
        assert_eq!(position.entry_price, 50000.0);
        assert_eq!(position.mark_price, 50100.0);
        assert_eq!(position.unrealized_pnl, 50.0);
        assert_eq!(position.margin_required, 1000.0);
    }

    #[test]
    fn test_parse_balance_response() {
        let client = GateIoTradingClient::new(
            "https://api.gateio.ws".to_string(),
            None,
            None
        ).unwrap();

        let balance_json = serde_json::json!({
            "currency": "USDT",
            "available": "1000.0",
            "freeze": "100.0"
        });

        let balance = client.parse_balance_response(&balance_json).unwrap();
        assert_eq!(balance.asset, "USDT");
        assert_eq!(balance.available, 1000.0);
        assert_eq!(balance.locked, 100.0);
        assert_eq!(balance.total, 1100.0);
    }

    #[tokio::test]
    async fn test_order_request_validation() {
        let order_request = OrderRequest::limit_order(
            "BTC_USDT".to_string(),
            OrderSide::Buy,
            0.001,
            50000.0
        );

        assert!(order_request.validate().is_ok());

        let invalid_order = OrderRequest::limit_order(
            "".to_string(),
            OrderSide::Buy,
            0.001,
            50000.0
        );

        assert!(invalid_order.validate().is_err());
    }
}