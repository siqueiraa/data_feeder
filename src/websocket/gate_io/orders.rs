//! Gate.io WebSocket orders stream implementation
//! 
//! This module handles real-time order updates from Gate.io WebSocket streams
//! using zero-copy serialization and sonic-rs for high-performance parsing.

use serde::{Deserialize, Serialize};
use crate::websocket::types::{WebSocketError, WebSocketMessage, StreamType};
use crate::api::types::{Order, OrderSide, OrderType, OrderStatus};
use tracing::debug;

/// Gate.io order WebSocket event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateIoOrderEvent {
    pub time: i64,
    pub time_ms: i64,
    pub channel: String,
    pub event: String,
    pub result: GateIoOrderData,
}

/// Gate.io order data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateIoOrderData {
    pub id: String,
    pub text: String,
    pub user: u64,
    pub create_time: i64,
    pub create_time_ms: i64,
    pub update_time: i64,
    pub update_time_ms: i64,
    pub status: String,
    pub contract: String,
    pub size: i64,
    pub left: i64,
    pub price: String,
    pub filled_total: String,
    pub fill_price: String,
    pub avg_deal_price: String,
    pub fee: String,
    pub fee_asset: String,
    pub point_fee: String,
    pub gt_fee: String,
    pub gt_discount: bool,
    pub rebated_fee: String,
    pub rebated_fee_asset: String,
    pub is_reduce_only: bool,
    pub is_close: bool,
    pub is_liq: bool,
    pub tif: String,
    pub iceberg: i64,
    pub auto_size: String,
}

impl GateIoOrderData {
    /// Convert Gate.io order data to unified Order structure
    pub fn to_order(&self) -> Result<Order, WebSocketError> {
        let side = if self.size > 0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        let order_type = match self.tif.as_str() {
            "gtc" => OrderType::Limit,
            "ioc" => OrderType::Market,
            "fok" => OrderType::Limit,
            _ => OrderType::Limit,
        };

        let status = match self.status.as_str() {
            "open" => OrderStatus::New,
            "finished" => OrderStatus::Filled,
            "cancelled" => OrderStatus::Cancelled,
            _ => OrderStatus::New,
        };

        let price = self.price.parse::<f64>()
            .map_err(|e| WebSocketError::Parse(format!("Invalid price format: {}", e)))?;

        Ok(Order {
            id: self.id.clone(),
            symbol: self.contract.clone(),
            side,
            order_type,
            quantity: self.size.abs() as f64,
            price: Some(price),
            status,
            timestamp: self.update_time_ms,
        })
    }
}

impl WebSocketMessage for GateIoOrderEvent {
    type Output = GateIoOrderEvent;
    
    fn parse(payload: &str) -> Result<Self::Output, WebSocketError> {
        parse_order_message(payload)
    }
    
    fn stream_type() -> StreamType {
        StreamType::GateOrders
    }
}

/// Parse Gate.io order WebSocket message using sonic-rs with serde_json fallback
pub fn parse_order_message(payload: &str) -> Result<GateIoOrderEvent, WebSocketError> {
    // Try sonic-rs first for zero-copy performance
    match sonic_rs::from_str::<GateIoOrderEvent>(payload) {
        Ok(event) => {
            debug!("✅ Successfully parsed Gate.io order message with sonic-rs");
            Ok(event)
        }
        Err(sonic_err) => {
            debug!("sonic-rs failed, falling back to serde_json: {}", sonic_err);
            // Fallback to serde_json for compatibility
            serde_json::from_str::<GateIoOrderEvent>(payload)
                .map_err(|e| WebSocketError::Parse(format!("Failed to parse Gate.io order message: {}", e)))
        }
    }
}

/// WebSocket order manager for Gate.io trading operations
pub struct GateIoOrderManager {
    connection: Option<crate::websocket::connection::ConnectionManager>,
    api_key: String,
    secret_key: String,
    rate_limiter: tokio::time::Interval,
}

impl GateIoOrderManager {
    /// Create a new Gate.io order manager
    pub fn new(api_key: String, secret_key: String) -> Self {
        Self {
            connection: None,
            api_key,
            secret_key,
            rate_limiter: tokio::time::interval(std::time::Duration::from_millis(100)), // 10 req/sec
        }
    }

    /// Initialize WebSocket connection for order management
    pub async fn connect(&mut self) -> Result<(), WebSocketError> {
        let connection = crate::websocket::connection::ConnectionManager::new_gate_io_futures();
        self.connection = Some(connection);
        Ok(())
    }

    /// Generate authentication signature for Gate.io API
    fn generate_signature(&self, timestamp: i64, method: &str, path: &str, body: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha512;
        
        type HmacSha512 = Hmac<Sha512>;
        
        let payload = format!("{}\n{}\n{}\n{}\n{}", 
            method, path, "", body, timestamp);
        
        let mut mac = HmacSha512::new_from_slice(self.secret_key.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(payload.as_bytes());
        
        hex::encode(mac.finalize().into_bytes())
    }

    /// Create order placement message for Gate.io WebSocket
    pub fn create_place_order_message(&self, order_request: &crate::api::types::OrderRequest, id: u64) -> Result<String, WebSocketError> {
        let timestamp = chrono::Utc::now().timestamp();
        let _side = match order_request.side {
            crate::api::types::OrderSide::Buy => "buy",
            crate::api::types::OrderSide::Sell => "sell",
        };
        
        let _order_type = match order_request.order_type {
            crate::api::types::OrderType::Market => "market",
            crate::api::types::OrderType::Limit => "limit",
            _ => "limit", // Default to limit for other types
        };
        
        let mut order_data = serde_json::json!({
            "contract": order_request.symbol,
            "size": if order_request.side == crate::api::types::OrderSide::Buy { 
                order_request.quantity as i64 
            } else { 
                -(order_request.quantity as i64) 
            },
            "tif": order_request.time_in_force.as_deref().unwrap_or("gtc"),
            "reduce_only": order_request.reduce_only
        });

        // Add price for limit orders
        if let Some(price) = order_request.price {
            order_data["price"] = serde_json::json!(price.to_string());
        }

        // Add client order ID if provided
        if let Some(client_id) = &order_request.client_order_id {
            order_data["text"] = serde_json::json!(client_id);
        }

        let body = serde_json::to_string(&order_data)
            .map_err(|e| WebSocketError::Parse(format!("Failed to serialize order data: {}", e)))?;

        let signature = self.generate_signature(timestamp, "POST", "/api/v4/futures/usdt/orders", &body);

        let message = serde_json::json!({
            "time": timestamp,
            "channel": "futures.orders",
            "event": "create",
            "payload": order_data,
            "id": id,
            "auth": {
                "method": "api_key",
                "KEY": self.api_key,
                "SIGN": signature,
                "timestamp": timestamp.to_string()
            }
        });

        serde_json::to_string(&message)
            .map_err(|e| WebSocketError::Parse(format!("Failed to serialize WebSocket message: {}", e)))
    }

    /// Create order cancellation message for Gate.io WebSocket
    pub fn create_cancel_order_message(&self, symbol: &str, order_id: &str, id: u64) -> Result<String, WebSocketError> {
        let timestamp = chrono::Utc::now().timestamp();
        
        let cancel_data = serde_json::json!({
            "contract": symbol,
            "order_id": order_id
        });

        let body = serde_json::to_string(&cancel_data)
            .map_err(|e| WebSocketError::Parse(format!("Failed to serialize cancel data: {}", e)))?;

        let signature = self.generate_signature(timestamp, "DELETE", "/api/v4/futures/usdt/orders", &body);

        let message = serde_json::json!({
            "time": timestamp,
            "channel": "futures.orders", 
            "event": "cancel",
            "payload": cancel_data,
            "id": id,
            "auth": {
                "method": "api_key",
                "KEY": self.api_key,
                "SIGN": signature,
                "timestamp": timestamp.to_string()
            }
        });

        serde_json::to_string(&message)
            .map_err(|e| WebSocketError::Parse(format!("Failed to serialize WebSocket cancel message: {}", e)))
    }

    /// Create order modification message for Gate.io WebSocket
    pub fn create_modify_order_message(&self, modify_request: &crate::api::types::ModifyOrderRequest, id: u64) -> Result<String, WebSocketError> {
        let timestamp = chrono::Utc::now().timestamp();
        
        let mut modify_data = serde_json::json!({
            "contract": modify_request.symbol,
            "order_id": modify_request.order_id
        });

        // Add quantity if provided
        if let Some(quantity) = modify_request.quantity {
            modify_data["size"] = serde_json::json!(quantity as i64);
        }

        // Add price if provided
        if let Some(price) = modify_request.price {
            modify_data["price"] = serde_json::json!(price.to_string());
        }

        let body = serde_json::to_string(&modify_data)
            .map_err(|e| WebSocketError::Parse(format!("Failed to serialize modify data: {}", e)))?;

        let signature = self.generate_signature(timestamp, "PUT", "/api/v4/futures/usdt/orders", &body);

        let message = serde_json::json!({
            "time": timestamp,
            "channel": "futures.orders",
            "event": "update", 
            "payload": modify_data,
            "id": id,
            "auth": {
                "method": "api_key",
                "KEY": self.api_key,
                "SIGN": signature,
                "timestamp": timestamp.to_string()
            }
        });

        serde_json::to_string(&message)
            .map_err(|e| WebSocketError::Parse(format!("Failed to serialize WebSocket modify message: {}", e)))
    }

    /// Apply rate limiting before sending order requests
    pub async fn rate_limit(&mut self) {
        self.rate_limiter.tick().await;
    }

    /// Validate order request before sending
    pub fn validate_order_request(&self, order_request: &crate::api::types::OrderRequest) -> Result<(), crate::api::types::ApiError> {
        // Use the validation logic from OrderRequest
        order_request.validate()?;
        
        // Additional Gate.io specific validations
        if order_request.symbol.is_empty() {
            return Err(crate::api::types::ApiError::InvalidSymbol("Symbol cannot be empty".to_string()));
        }

        // Gate.io typically requires symbols to be in format like "BTC_USDT"
        if !order_request.symbol.contains('_') {
            return Err(crate::api::types::ApiError::InvalidSymbol(
                format!("Gate.io symbol format should be 'BASE_QUOTE', got: {}", order_request.symbol)
            ));
        }

        // Check minimum quantity (this could be configurable per symbol)
        if order_request.quantity < 0.001 {
            return Err(crate::api::types::ApiError::InvalidTimeframe(
                "Quantity too small, minimum 0.001".to_string()
            ));
        }

        // For limit orders, ensure price is reasonable (not zero)
        if order_request.order_type == crate::api::types::OrderType::Limit {
            if let Some(price) = order_request.price {
                if price <= 0.0 {
                    return Err(crate::api::types::ApiError::InvalidTimeframe(
                        "Price must be greater than zero".to_string()
                    ));
                }
            }
        }

        Ok(())
    }
}

/// Order error handling with Gate.io specific error mapping
pub struct GateIoOrderErrorHandler;

impl GateIoOrderErrorHandler {
    /// Map Gate.io WebSocket error responses to ApiError
    pub fn map_gate_io_error(error_code: i32, error_message: &str) -> crate::api::types::ApiError {
        match error_code {
            1001 => crate::api::types::ApiError::Authentication("Invalid API key".to_string()),
            1002 => crate::api::types::ApiError::Authentication("Invalid signature".to_string()),
            1003 => crate::api::types::ApiError::RateLimit("Rate limit exceeded".to_string()),
            2001 => crate::api::types::ApiError::InvalidSymbol("Invalid contract".to_string()),
            2002 => crate::api::types::ApiError::InvalidTimeframe("Insufficient balance".to_string()),
            2003 => crate::api::types::ApiError::InvalidTimeframe("Invalid order size".to_string()),
            2004 => crate::api::types::ApiError::InvalidTimeframe("Invalid order price".to_string()),
            2005 => crate::api::types::ApiError::InvalidTimeframe("Order not found".to_string()),
            2006 => crate::api::types::ApiError::InvalidTimeframe("Order already cancelled".to_string()),
            3001 => crate::api::types::ApiError::Network("Market closed".to_string()),
            3002 => crate::api::types::ApiError::Network("Position limit exceeded".to_string()),
            _ => crate::api::types::ApiError::Unknown(format!("Gate.io error {}: {}", error_code, error_message))
        }
    }

    /// Check if error is recoverable and suggests retry
    pub fn is_recoverable_error(error_code: i32) -> bool {
        match error_code {
            1003 => true, // Rate limit - can retry after backoff
            3001 => false, // Market closed - not recoverable immediately
            3002 => false, // Position limit - need to reduce positions first
            2002 => false, // Insufficient balance - need to add funds
            2005 => false, // Order not found - order may have been filled/cancelled
            2006 => false, // Already cancelled - not recoverable
            1001 | 1002 => false, // Auth errors - need configuration fix
            2001 | 2003 | 2004 => false, // Invalid params - need order correction
            _ => true, // Unknown errors - might be temporary
        }
    }

    /// Get suggested retry delay for recoverable errors
    pub fn get_retry_delay(error_code: i32) -> std::time::Duration {
        match error_code {
            1003 => std::time::Duration::from_secs(60), // Rate limit - wait 1 minute
            _ => std::time::Duration::from_secs(5), // Default - wait 5 seconds
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_gate_order_message() -> &'static str {
        r#"{
            "time": 1609459200,
            "time_ms": 1609459200123,
            "channel": "futures.orders",
            "event": "update",
            "result": {
                "id": "123456789",
                "text": "t-my-custom-id",
                "user": 123456,
                "create_time": 1609459200,
                "create_time_ms": 1609459200123,
                "update_time": 1609459200,
                "update_time_ms": 1609459200456,
                "status": "open",
                "contract": "BTC_USDT",
                "size": 100,
                "left": 50,
                "price": "50000.0",
                "filled_total": "25000.0",
                "fill_price": "50000.0",
                "avg_deal_price": "50000.0",
                "fee": "0.025",
                "fee_asset": "USDT",
                "point_fee": "0",
                "gt_fee": "0",
                "gt_discount": false,
                "rebated_fee": "0",
                "rebated_fee_asset": "USDT",
                "is_reduce_only": false,
                "is_close": false,
                "is_liq": false,
                "tif": "gtc",
                "iceberg": 0,
                "auto_size": "0"
            }
        }"#
    }

    #[test]
    fn test_parse_gate_order_message() {
        let result = parse_order_message(sample_gate_order_message());
        assert!(result.is_ok());
        
        let event = result.unwrap();
        assert_eq!(event.channel, "futures.orders");
        assert_eq!(event.event, "update");
        assert_eq!(event.result.id, "123456789");
        assert_eq!(event.result.contract, "BTC_USDT");
        assert_eq!(event.result.size, 100);
        assert_eq!(event.result.status, "open");
    }

    #[test]
    fn test_gate_order_to_unified_order() {
        let message = parse_order_message(sample_gate_order_message()).unwrap();
        let order = message.result.to_order().unwrap();
        
        assert_eq!(order.id, "123456789");
        assert_eq!(order.symbol, "BTC_USDT");
        assert_eq!(order.side, OrderSide::Buy);
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.quantity, 100.0);
        assert_eq!(order.price, Some(50000.0));
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.timestamp, 1609459200456);
    }

    #[test]
    fn test_websocket_message_trait() {
        let result = GateIoOrderEvent::parse(sample_gate_order_message());
        assert!(result.is_ok());
        
        assert_eq!(GateIoOrderEvent::stream_type(), StreamType::GateOrders);
    }

    #[test]
    fn test_invalid_order_message() {
        let result = parse_order_message("invalid json");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_order_manager_creation() {
        let order_manager = GateIoOrderManager::new(
            "test_api_key".to_string(),
            "test_secret_key".to_string()
        );
        
        assert_eq!(order_manager.api_key, "test_api_key");
        assert_eq!(order_manager.secret_key, "test_secret_key");
    }

    #[test]
    fn test_order_validation() {
        let order_manager = GateIoOrderManager::new(
            "test_key".to_string(),
            "test_secret".to_string()
        );

        // Valid order
        let valid_order = crate::api::types::OrderRequest::limit_order(
            "BTC_USDT".to_string(),
            crate::api::types::OrderSide::Buy,
            1.0,
            50000.0
        );
        assert!(order_manager.validate_order_request(&valid_order).is_ok());

        // Invalid symbol format
        let invalid_symbol = crate::api::types::OrderRequest::limit_order(
            "BTCUSDT".to_string(), // Missing underscore
            crate::api::types::OrderSide::Buy,
            1.0,
            50000.0
        );
        assert!(order_manager.validate_order_request(&invalid_symbol).is_err());

        // Quantity too small
        let small_quantity = crate::api::types::OrderRequest::limit_order(
            "BTC_USDT".to_string(),
            crate::api::types::OrderSide::Buy,
            0.0001, // Too small
            50000.0
        );
        assert!(order_manager.validate_order_request(&small_quantity).is_err());

        // Invalid price for limit order
        let invalid_price = crate::api::types::OrderRequest::limit_order(
            "BTC_USDT".to_string(),
            crate::api::types::OrderSide::Buy,
            1.0,
            0.0 // Invalid price
        );
        assert!(order_manager.validate_order_request(&invalid_price).is_err());
    }

    #[test]
    fn test_error_mapping() {
        // Test authentication error
        let auth_error = GateIoOrderErrorHandler::map_gate_io_error(1001, "Invalid key");
        assert!(matches!(auth_error, crate::api::types::ApiError::Authentication(_)));

        // Test rate limit error
        let rate_limit_error = GateIoOrderErrorHandler::map_gate_io_error(1003, "Rate limit");
        assert!(matches!(rate_limit_error, crate::api::types::ApiError::RateLimit(_)));

        // Test unknown error
        let unknown_error = GateIoOrderErrorHandler::map_gate_io_error(9999, "Unknown");
        assert!(matches!(unknown_error, crate::api::types::ApiError::Unknown(_)));
    }

    #[test]
    fn test_error_recoverability() {
        // Recoverable errors
        assert!(GateIoOrderErrorHandler::is_recoverable_error(1003)); // Rate limit
        assert!(GateIoOrderErrorHandler::is_recoverable_error(9999)); // Unknown

        // Non-recoverable errors
        assert!(!GateIoOrderErrorHandler::is_recoverable_error(1001)); // Auth
        assert!(!GateIoOrderErrorHandler::is_recoverable_error(2002)); // Insufficient balance
        assert!(!GateIoOrderErrorHandler::is_recoverable_error(3001)); // Market closed
    }

    #[test]
    fn test_retry_delays() {
        // Rate limit should have longer delay
        let rate_limit_delay = GateIoOrderErrorHandler::get_retry_delay(1003);
        assert_eq!(rate_limit_delay, std::time::Duration::from_secs(60));

        // Default delay for other errors
        let default_delay = GateIoOrderErrorHandler::get_retry_delay(9999);
        assert_eq!(default_delay, std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_order_message_creation() {
        let order_manager = GateIoOrderManager::new(
            "test_key".to_string(),
            "test_secret".to_string()
        );

        let order_request = crate::api::types::OrderRequest::limit_order(
            "BTC_USDT".to_string(),
            crate::api::types::OrderSide::Buy,
            1.0,
            50000.0
        );

        let message_result = order_manager.create_place_order_message(&order_request, 1);
        assert!(message_result.is_ok());

        let message = message_result.unwrap();
        assert!(message.contains("futures.orders"));
        assert!(message.contains("create"));
        assert!(message.contains("BTC_USDT"));
    }

    #[test]
    fn test_cancel_order_message_creation() {
        let order_manager = GateIoOrderManager::new(
            "test_key".to_string(),
            "test_secret".to_string()
        );

        let message_result = order_manager.create_cancel_order_message("BTC_USDT", "12345", 1);
        assert!(message_result.is_ok());

        let message = message_result.unwrap();
        assert!(message.contains("futures.orders"));
        assert!(message.contains("cancel"));
        assert!(message.contains("BTC_USDT"));
        assert!(message.contains("12345"));
    }

    #[test]
    fn test_modify_order_message_creation() {
        let order_manager = GateIoOrderManager::new(
            "test_key".to_string(),
            "test_secret".to_string()
        );

        let modify_request = crate::api::types::ModifyOrderRequest {
            symbol: "BTC_USDT".to_string(),
            order_id: "12345".to_string(),
            quantity: Some(2.0),
            price: Some(51000.0),
        };

        let message_result = order_manager.create_modify_order_message(&modify_request, 1);
        assert!(message_result.is_ok());

        let message = message_result.unwrap();
        assert!(message.contains("futures.orders"));
        assert!(message.contains("update"));
        assert!(message.contains("BTC_USDT"));
        assert!(message.contains("12345"));
    }
}