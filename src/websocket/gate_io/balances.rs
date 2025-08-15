//! Gate.io WebSocket balances stream implementation
//! 
//! This module handles real-time balance updates from Gate.io WebSocket streams
//! using zero-copy serialization and sonic-rs for high-performance parsing.

use serde::{Deserialize, Serialize};
use crate::websocket::types::{WebSocketError, WebSocketMessage, StreamType};
use crate::api::types::Balance;
use tracing::debug;

/// Gate.io balance WebSocket event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateIoBalanceEvent {
    pub time: i64,
    pub time_ms: i64,
    pub channel: String,
    pub event: String,
    pub result: Vec<GateIoBalanceData>,
}

/// Gate.io balance data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateIoBalanceData {
    pub balance: String,
    pub change: String,
    pub text: String,
    pub time: i64,
    pub time_ms: i64,
    #[serde(rename = "type")]
    pub balance_type: String,
    pub user: u64,
}

impl GateIoBalanceData {
    /// Convert Gate.io balance data to unified Balance structure
    pub fn to_balance(&self, asset: &str) -> Result<Balance, WebSocketError> {
        let balance_value = self.balance.parse::<f64>()
            .map_err(|e| WebSocketError::Parse(format!("Invalid balance format: {}", e)))?;

        let _change_value = self.change.parse::<f64>()
            .map_err(|e| WebSocketError::Parse(format!("Invalid change format: {}", e)))?;

        // For futures, typically all balance is available unless locked in positions
        let available = balance_value;
        let locked = 0.0; // Gate.io balance updates show net balance changes
        let total = available + locked;

        Ok(Balance {
            asset: asset.to_string(),
            available,
            locked,
            total,
            timestamp: self.time_ms,
        })
    }
}

impl WebSocketMessage for GateIoBalanceEvent {
    type Output = GateIoBalanceEvent;
    
    fn parse(payload: &str) -> Result<Self::Output, WebSocketError> {
        parse_balance_message(payload)
    }
    
    fn stream_type() -> StreamType {
        StreamType::GateBalances
    }
}

/// Parse Gate.io balance WebSocket message using sonic-rs with serde_json fallback
pub fn parse_balance_message(payload: &str) -> Result<GateIoBalanceEvent, WebSocketError> {
    // Try sonic-rs first for zero-copy performance
    match sonic_rs::from_str::<GateIoBalanceEvent>(payload) {
        Ok(event) => {
            debug!("✅ Successfully parsed Gate.io balance message with sonic-rs");
            Ok(event)
        }
        Err(sonic_err) => {
            debug!("sonic-rs failed, falling back to serde_json: {}", sonic_err);
            // Fallback to serde_json for compatibility
            serde_json::from_str::<GateIoBalanceEvent>(payload)
                .map_err(|e| WebSocketError::Parse(format!("Failed to parse Gate.io balance message: {}", e)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_gate_balance_message() -> &'static str {
        r#"{
            "time": 1609459200,
            "time_ms": 1609459200123,
            "channel": "futures.balances",
            "event": "update",
            "result": [
                {
                    "balance": "9999.9875",
                    "change": "-0.0125",
                    "text": "BTC_USDT:123456789",
                    "time": 1609459200,
                    "time_ms": 1609459200456,
                    "type": "fee",
                    "user": 123456
                }
            ]
        }"#
    }

    #[test]
    fn test_parse_gate_balance_message() {
        let result = parse_balance_message(sample_gate_balance_message());
        assert!(result.is_ok());
        
        let event = result.unwrap();
        assert_eq!(event.channel, "futures.balances");
        assert_eq!(event.event, "update");
        assert_eq!(event.result.len(), 1);
        assert_eq!(event.result[0].balance, "9999.9875");
        assert_eq!(event.result[0].change, "-0.0125");
        assert_eq!(event.result[0].balance_type, "fee");
        assert_eq!(event.result[0].user, 123456);
    }

    #[test]
    fn test_gate_balance_to_unified_balance() {
        let message = parse_balance_message(sample_gate_balance_message()).unwrap();
        let balance = message.result[0].to_balance("USDT").unwrap();
        
        assert_eq!(balance.asset, "USDT");
        assert_eq!(balance.available, 9999.9875);
        assert_eq!(balance.locked, 0.0);
        assert_eq!(balance.total, 9999.9875);
        assert_eq!(balance.timestamp, 1609459200456);
    }

    #[test]
    fn test_websocket_message_trait() {
        let result = GateIoBalanceEvent::parse(sample_gate_balance_message());
        assert!(result.is_ok());
        
        assert_eq!(GateIoBalanceEvent::stream_type(), StreamType::GateBalances);
    }

    #[test]
    fn test_invalid_balance_message() {
        let result = parse_balance_message("invalid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_balance_calculation() {
        let balance_data = GateIoBalanceData {
            balance: "1000.50".to_string(),
            change: "50.25".to_string(),
            text: "BTC_USDT:123456789".to_string(),
            time: 1609459200,
            time_ms: 1609459200456,
            balance_type: "trade".to_string(),
            user: 123456,
        };

        let balance = balance_data.to_balance("USDT").unwrap();
        
        assert_eq!(balance.available, 1000.50);
        assert_eq!(balance.locked, 0.0);
        assert_eq!(balance.total, 1000.50);
    }
}