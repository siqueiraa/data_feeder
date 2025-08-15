//! Gate.io WebSocket positions stream implementation
//! 
//! This module handles real-time position updates from Gate.io WebSocket streams
//! using zero-copy serialization and sonic-rs for high-performance parsing.

use serde::{Deserialize, Serialize};
use crate::websocket::types::{WebSocketError, WebSocketMessage, StreamType};
use crate::api::types::{Position, PositionSide};
use tracing::debug;

/// Gate.io position WebSocket event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateIoPositionEvent {
    pub time: i64,
    pub time_ms: i64,
    pub channel: String,
    pub event: String,
    pub result: Vec<GateIoPositionData>,
}

/// Gate.io position data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateIoPositionData {
    pub contract: String,
    pub size: i64,
    pub leverage: String,
    pub risk_limit: String,
    pub leverage_max: String,
    pub maintenance_rate: String,
    pub value: String,
    pub margin: String,
    pub entry_price: String,
    pub liq_price: String,
    pub mark_price: String,
    pub unrealised_pnl: String,
    pub realised_pnl: String,
    pub history_pnl: String,
    pub last_close_pnl: String,
    pub realised_point: String,
    pub history_point: String,
    pub adl_ranking: i32,
    pub pending_orders: i32,
    pub close_order: Option<CloseOrderInfo>,
    pub mode: String,
    pub cross_leverage_limit: String,
    pub update_time: i64,
    pub update_time_ms: i64,
    pub update_id: u64,
}

/// Close order information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloseOrderInfo {
    pub id: String,
    pub price: String,
    pub is_liq: bool,
}

impl GateIoPositionData {
    /// Convert Gate.io position data to unified Position structure
    pub fn to_position(&self) -> Result<Position, WebSocketError> {
        let side = if self.size > 0 {
            PositionSide::Long
        } else if self.size < 0 {
            PositionSide::Short
        } else {
            PositionSide::None
        };

        let entry_price = self.entry_price.parse::<f64>()
            .map_err(|e| WebSocketError::Parse(format!("Invalid entry_price format: {}", e)))?;

        let mark_price = self.mark_price.parse::<f64>()
            .map_err(|e| WebSocketError::Parse(format!("Invalid mark_price format: {}", e)))?;

        let unrealized_pnl = self.unrealised_pnl.parse::<f64>()
            .map_err(|e| WebSocketError::Parse(format!("Invalid unrealised_pnl format: {}", e)))?;

        let margin_required = self.margin.parse::<f64>()
            .map_err(|e| WebSocketError::Parse(format!("Invalid margin format: {}", e)))?;

        Ok(Position {
            symbol: self.contract.clone(),
            side,
            size: self.size.abs() as f64,
            entry_price,
            mark_price,
            unrealized_pnl,
            margin_required,
            timestamp: self.update_time_ms,
        })
    }
}

impl WebSocketMessage for GateIoPositionEvent {
    type Output = GateIoPositionEvent;
    
    fn parse(payload: &str) -> Result<Self::Output, WebSocketError> {
        parse_position_message(payload)
    }
    
    fn stream_type() -> StreamType {
        StreamType::GatePositions
    }
}

/// Parse Gate.io position WebSocket message using sonic-rs with serde_json fallback
pub fn parse_position_message(payload: &str) -> Result<GateIoPositionEvent, WebSocketError> {
    // Try sonic-rs first for zero-copy performance
    match sonic_rs::from_str::<GateIoPositionEvent>(payload) {
        Ok(event) => {
            debug!("✅ Successfully parsed Gate.io position message with sonic-rs");
            Ok(event)
        }
        Err(sonic_err) => {
            debug!("sonic-rs failed, falling back to serde_json: {}", sonic_err);
            // Fallback to serde_json for compatibility
            serde_json::from_str::<GateIoPositionEvent>(payload)
                .map_err(|e| WebSocketError::Parse(format!("Failed to parse Gate.io position message: {}", e)))
        }
    }
}

/// Position reconciliation manager for ensuring accurate position tracking
pub struct GateIoPositionReconciler {
    positions: std::collections::HashMap<String, crate::api::types::Position>,
    last_reconciliation: std::time::Instant,
    reconciliation_interval: std::time::Duration,
}

impl GateIoPositionReconciler {
    /// Create a new position reconciler
    pub fn new() -> Self {
        Self {
            positions: std::collections::HashMap::new(),
            last_reconciliation: std::time::Instant::now(),
            reconciliation_interval: std::time::Duration::from_secs(60), // Reconcile every minute
        }
    }

    /// Update position from WebSocket message
    pub fn update_position(&mut self, position: crate::api::types::Position) {
        let symbol = position.symbol.clone();
        self.positions.insert(symbol, position);
    }

    /// Get current position for a symbol
    pub fn get_position(&self, symbol: &str) -> Option<&crate::api::types::Position> {
        self.positions.get(symbol)
    }

    /// Get all positions
    pub fn get_all_positions(&self) -> Vec<&crate::api::types::Position> {
        self.positions.values().collect()
    }

    /// Check if reconciliation is needed
    pub fn needs_reconciliation(&self) -> bool {
        self.last_reconciliation.elapsed() > self.reconciliation_interval
    }

    /// Mark reconciliation as completed
    pub fn mark_reconciled(&mut self) {
        self.last_reconciliation = std::time::Instant::now();
    }

    /// Calculate total unrealized PnL across all positions
    pub fn total_unrealized_pnl(&self) -> f64 {
        self.positions.values()
            .map(|pos| pos.unrealized_pnl)
            .sum()
    }

    /// Calculate total margin required across all positions
    pub fn total_margin_required(&self) -> f64 {
        self.positions.values()
            .map(|pos| pos.margin_required)
            .sum()
    }

    /// Get positions by side (Long/Short)
    pub fn get_positions_by_side(&self, side: crate::api::types::PositionSide) -> Vec<&crate::api::types::Position> {
        self.positions.values()
            .filter(|pos| pos.side == side)
            .collect()
    }

    /// Validate position consistency
    pub fn validate_position_consistency(&self, position: &crate::api::types::Position) -> Result<(), WebSocketError> {
        // Check for impossible states
        if position.size < 0.0 {
            return Err(WebSocketError::Parse(format!(
                "Position size cannot be negative: {}", position.size
            )));
        }

        if position.side == crate::api::types::PositionSide::None && position.size != 0.0 {
            return Err(WebSocketError::Parse(format!(
                "Position with side None must have zero size, got: {}", position.size
            )));
        }

        if (position.side == crate::api::types::PositionSide::Long || position.side == crate::api::types::PositionSide::Short) 
           && position.size == 0.0 {
            debug!("Warning: Position shows Long/Short side but zero size for {}", position.symbol);
        }

        if position.entry_price <= 0.0 && position.size > 0.0 {
            return Err(WebSocketError::Parse(format!(
                "Position with non-zero size must have positive entry price: {}", position.entry_price
            )));
        }

        Ok(())
    }

    /// Calculate position-based risk metrics
    pub fn calculate_risk_metrics(&self) -> PositionRiskMetrics {
        let mut risk_metrics = PositionRiskMetrics::default();
        
        for position in self.positions.values() {
            if position.size == 0.0 {
                continue;
            }

            risk_metrics.total_exposure += position.size * position.mark_price;
            risk_metrics.total_margin += position.margin_required;
            risk_metrics.total_unrealized_pnl += position.unrealized_pnl;
            
            if position.side == crate::api::types::PositionSide::Long {
                risk_metrics.long_exposure += position.size * position.mark_price;
                risk_metrics.long_positions += 1;
            } else if position.side == crate::api::types::PositionSide::Short {
                risk_metrics.short_exposure += position.size * position.mark_price;
                risk_metrics.short_positions += 1;
            }

            // Calculate individual position risk
            let position_value = position.size * position.entry_price;
            if position_value != 0.0 {
                let pnl_percentage = position.unrealized_pnl / position_value * 100.0;
                if pnl_percentage < risk_metrics.worst_position_pnl_pct {
                    risk_metrics.worst_position_pnl_pct = pnl_percentage;
                }
                if pnl_percentage > risk_metrics.best_position_pnl_pct {
                    risk_metrics.best_position_pnl_pct = pnl_percentage;
                }
            }
        }

        risk_metrics.net_exposure = risk_metrics.long_exposure - risk_metrics.short_exposure;
        risk_metrics.total_positions = risk_metrics.long_positions + risk_metrics.short_positions;

        risk_metrics
    }
}

impl Default for GateIoPositionReconciler {
    fn default() -> Self {
        Self::new()
    }
}

/// Risk metrics calculated from position data
#[derive(Debug, Clone, Default)]
pub struct PositionRiskMetrics {
    pub total_exposure: f64,
    pub net_exposure: f64,
    pub long_exposure: f64,
    pub short_exposure: f64,
    pub total_margin: f64,
    pub total_unrealized_pnl: f64,
    pub total_positions: u32,
    pub long_positions: u32,
    pub short_positions: u32,
    pub worst_position_pnl_pct: f64,
    pub best_position_pnl_pct: f64,
}

impl PositionRiskMetrics {
    /// Check if portfolio is over-leveraged based on margin usage
    pub fn is_over_leveraged(&self, max_margin_ratio: f64) -> bool {
        if self.total_exposure == 0.0 {
            return false;
        }
        
        let margin_ratio = self.total_margin / self.total_exposure;
        margin_ratio > max_margin_ratio
    }

    /// Check if portfolio has excessive concentration in one direction
    pub fn is_over_concentrated(&self, max_concentration: f64) -> bool {
        if self.total_exposure == 0.0 {
            return false;
        }
        
        let concentration = self.net_exposure.abs() / self.total_exposure;
        concentration > max_concentration
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_gate_position_message() -> &'static str {
        r#"{
            "time": 1609459200,
            "time_ms": 1609459200123,
            "channel": "futures.positions",
            "event": "update",
            "result": [
                {
                    "contract": "BTC_USDT",
                    "size": 100,
                    "leverage": "10",
                    "risk_limit": "1000000",
                    "leverage_max": "100",
                    "maintenance_rate": "0.005",
                    "value": "5000.0",
                    "margin": "500.0",
                    "entry_price": "50000.0",
                    "liq_price": "45000.0",
                    "mark_price": "50100.0",
                    "unrealised_pnl": "10.0",
                    "realised_pnl": "0.0",
                    "history_pnl": "0.0",
                    "last_close_pnl": "0.0",
                    "realised_point": "0.0",
                    "history_point": "0.0",
                    "adl_ranking": 5,
                    "pending_orders": 0,
                    "close_order": null,
                    "mode": "single",
                    "cross_leverage_limit": "0",
                    "update_time": 1609459200,
                    "update_time_ms": 1609459200456,
                    "update_id": 123456789
                }
            ]
        }"#
    }

    #[test]
    fn test_parse_gate_position_message() {
        let result = parse_position_message(sample_gate_position_message());
        assert!(result.is_ok());
        
        let event = result.unwrap();
        assert_eq!(event.channel, "futures.positions");
        assert_eq!(event.event, "update");
        assert_eq!(event.result.len(), 1);
        assert_eq!(event.result[0].contract, "BTC_USDT");
        assert_eq!(event.result[0].size, 100);
    }

    #[test]
    fn test_gate_position_to_unified_position() {
        let message = parse_position_message(sample_gate_position_message()).unwrap();
        let position = message.result[0].to_position().unwrap();
        
        assert_eq!(position.symbol, "BTC_USDT");
        assert_eq!(position.side, PositionSide::Long);
        assert_eq!(position.size, 100.0);
        assert_eq!(position.entry_price, 50000.0);
        assert_eq!(position.mark_price, 50100.0);
        assert_eq!(position.unrealized_pnl, 10.0);
        assert_eq!(position.margin_required, 500.0);
        assert_eq!(position.timestamp, 1609459200456);
    }

    #[test]
    fn test_websocket_message_trait() {
        let result = GateIoPositionEvent::parse(sample_gate_position_message());
        assert!(result.is_ok());
        
        assert_eq!(GateIoPositionEvent::stream_type(), StreamType::GatePositions);
    }

    #[test]
    fn test_position_side_detection() {
        // Test long position
        let mut position_data = GateIoPositionData {
            contract: "BTC_USDT".to_string(),
            size: 100,
            leverage: "10".to_string(),
            risk_limit: "1000000".to_string(),
            leverage_max: "100".to_string(),
            maintenance_rate: "0.005".to_string(),
            value: "5000.0".to_string(),
            margin: "500.0".to_string(),
            entry_price: "50000.0".to_string(),
            liq_price: "45000.0".to_string(),
            mark_price: "50100.0".to_string(),
            unrealised_pnl: "10.0".to_string(),
            realised_pnl: "0.0".to_string(),
            history_pnl: "0.0".to_string(),
            last_close_pnl: "0.0".to_string(),
            realised_point: "0.0".to_string(),
            history_point: "0.0".to_string(),
            adl_ranking: 5,
            pending_orders: 0,
            close_order: None,
            mode: "single".to_string(),
            cross_leverage_limit: "0".to_string(),
            update_time: 1609459200,
            update_time_ms: 1609459200456,
            update_id: 123456789,
        };

        let position = position_data.to_position().unwrap();
        assert_eq!(position.side, PositionSide::Long);

        // Test short position
        position_data.size = -100;
        let position = position_data.to_position().unwrap();
        assert_eq!(position.side, PositionSide::Short);

        // Test no position
        position_data.size = 0;
        let position = position_data.to_position().unwrap();
        assert_eq!(position.side, PositionSide::None);
    }

    #[test]
    fn test_invalid_position_message() {
        let result = parse_position_message("invalid json");
        assert!(result.is_err());
    }
}