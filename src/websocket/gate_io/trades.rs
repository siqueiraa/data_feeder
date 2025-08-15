//! Gate.io WebSocket trades stream implementation
//! 
//! This module handles real-time trade execution updates from Gate.io WebSocket streams
//! using zero-copy serialization and sonic-rs for high-performance parsing.

use serde::{Deserialize, Serialize};
use crate::websocket::types::{WebSocketError, WebSocketMessage, StreamType};
use crate::api::types::{TradeExecution, OrderSide};
use tracing::debug;

/// Gate.io trade WebSocket event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateIoTradeEvent {
    pub time: i64,
    pub time_ms: i64,
    pub channel: String,
    pub event: String,
    pub result: Vec<GateIoTradeData>,
}

/// Gate.io trade data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateIoTradeData {
    pub id: String,
    pub create_time: i64,
    pub create_time_ms: i64,
    pub contract: String,
    pub order_id: String,
    pub size: i64,
    pub price: String,
    pub fee: String,
    pub point_fee: String,
    pub gt_fee: String,
    pub role: String,
    pub text: String,
}

impl GateIoTradeData {
    /// Convert Gate.io trade data to unified TradeExecution structure
    pub fn to_trade_execution(&self) -> Result<TradeExecution, WebSocketError> {
        let side = if self.size > 0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        let price = self.price.parse::<f64>()
            .map_err(|e| WebSocketError::Parse(format!("Invalid price format: {}", e)))?;

        let fee = self.fee.parse::<f64>()
            .map_err(|e| WebSocketError::Parse(format!("Invalid fee format: {}", e)))?;

        Ok(TradeExecution {
            id: self.id.clone(),
            order_id: self.order_id.clone(),
            symbol: self.contract.clone(),
            side,
            quantity: self.size.abs() as f64,
            price,
            fee,
            fee_asset: "USDT".to_string(), // Gate.io typically uses USDT for futures fees
            timestamp: self.create_time_ms,
        })
    }
}

impl WebSocketMessage for GateIoTradeEvent {
    type Output = GateIoTradeEvent;
    
    fn parse(payload: &str) -> Result<Self::Output, WebSocketError> {
        parse_trade_message(payload)
    }
    
    fn stream_type() -> StreamType {
        StreamType::GateTrades
    }
}

/// Parse Gate.io trade WebSocket message using sonic-rs with serde_json fallback
pub fn parse_trade_message(payload: &str) -> Result<GateIoTradeEvent, WebSocketError> {
    // Try sonic-rs first for zero-copy performance
    match sonic_rs::from_str::<GateIoTradeEvent>(payload) {
        Ok(event) => {
            debug!("✅ Successfully parsed Gate.io trade message with sonic-rs");
            Ok(event)
        }
        Err(sonic_err) => {
            debug!("sonic-rs failed, falling back to serde_json: {}", sonic_err);
            // Fallback to serde_json for compatibility
            serde_json::from_str::<GateIoTradeEvent>(payload)
                .map_err(|e| WebSocketError::Parse(format!("Failed to parse Gate.io trade message: {}", e)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_gate_trade_message() -> &'static str {
        r#"{
            "time": 1609459200,
            "time_ms": 1609459200123,
            "channel": "futures.usertrades",
            "event": "update",
            "result": [
                {
                    "id": "987654321",
                    "create_time": 1609459200,
                    "create_time_ms": 1609459200456,
                    "contract": "BTC_USDT",
                    "order_id": "123456789",
                    "size": 50,
                    "price": "50000.0",
                    "fee": "1.25",
                    "point_fee": "0",
                    "gt_fee": "0",
                    "role": "taker",
                    "text": "t-my-custom-id"
                }
            ]
        }"#
    }

    #[test]
    fn test_parse_gate_trade_message() {
        let result = parse_trade_message(sample_gate_trade_message());
        assert!(result.is_ok());
        
        let event = result.unwrap();
        assert_eq!(event.channel, "futures.usertrades");
        assert_eq!(event.event, "update");
        assert_eq!(event.result.len(), 1);
        assert_eq!(event.result[0].id, "987654321");
        assert_eq!(event.result[0].contract, "BTC_USDT");
        assert_eq!(event.result[0].order_id, "123456789");
        assert_eq!(event.result[0].size, 50);
    }

    #[test]
    fn test_gate_trade_to_unified_trade() {
        let message = parse_trade_message(sample_gate_trade_message()).unwrap();
        let trade = message.result[0].to_trade_execution().unwrap();
        
        assert_eq!(trade.id, "987654321");
        assert_eq!(trade.order_id, "123456789");
        assert_eq!(trade.symbol, "BTC_USDT");
        assert_eq!(trade.side, OrderSide::Buy);
        assert_eq!(trade.quantity, 50.0);
        assert_eq!(trade.price, 50000.0);
        assert_eq!(trade.fee, 1.25);
        assert_eq!(trade.fee_asset, "USDT");
        assert_eq!(trade.timestamp, 1609459200456);
    }

    #[test]
    fn test_websocket_message_trait() {
        let result = GateIoTradeEvent::parse(sample_gate_trade_message());
        assert!(result.is_ok());
        
        assert_eq!(GateIoTradeEvent::stream_type(), StreamType::GateTrades);
    }

    #[test]
    fn test_trade_side_detection() {
        let mut trade_data = GateIoTradeData {
            id: "987654321".to_string(),
            create_time: 1609459200,
            create_time_ms: 1609459200456,
            contract: "BTC_USDT".to_string(),
            order_id: "123456789".to_string(),
            size: 50,
            price: "50000.0".to_string(),
            fee: "1.25".to_string(),
            point_fee: "0".to_string(),
            gt_fee: "0".to_string(),
            role: "taker".to_string(),
            text: "t-my-custom-id".to_string(),
        };

        // Test buy trade
        let trade = trade_data.to_trade_execution().unwrap();
        assert_eq!(trade.side, OrderSide::Buy);

        // Test sell trade
        trade_data.size = -50;
        let trade = trade_data.to_trade_execution().unwrap();
        assert_eq!(trade.side, OrderSide::Sell);
    }

    #[test]
    fn test_invalid_trade_message() {
        let result = parse_trade_message("invalid json");
        assert!(result.is_err());
    }
}