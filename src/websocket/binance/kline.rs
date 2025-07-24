use serde::{Deserialize, Serialize};
use crate::historical::structs::FuturesOHLCVCandle;
use crate::websocket::types::{StreamType, WebSocketError, WebSocketMessage};

/// Complete Binance kline WebSocket event structure
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinanceKlineEvent {
    /// Event type - should be "kline"
    #[serde(rename = "e")]
    pub event_type: String,
    
    /// Event time (timestamp in milliseconds)
    #[serde(rename = "E")]
    pub event_time: i64,
    
    /// Symbol
    #[serde(rename = "s")]
    pub symbol: String,
    
    /// Kline data
    #[serde(rename = "k")]
    pub kline: BinanceKlineData,
}

/// Complete Binance kline data structure with ALL fields
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinanceKlineData {
    /// Kline start time (timestamp in milliseconds)
    #[serde(rename = "t")]
    pub start_time: i64,
    
    /// Kline close time (timestamp in milliseconds)
    #[serde(rename = "T")]
    pub close_time: i64,
    
    /// Symbol
    #[serde(rename = "s")]
    pub symbol: String,
    
    /// Interval
    #[serde(rename = "i")]
    pub interval: String,
    
    /// First trade ID
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    
    /// Last trade ID
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    
    /// Open price
    #[serde(rename = "o")]
    pub open: String,
    
    /// Close price
    #[serde(rename = "c")]
    pub close: String,
    
    /// High price
    #[serde(rename = "h")]
    pub high: String,
    
    /// Low price
    #[serde(rename = "l")]
    pub low: String,
    
    /// Base asset volume
    #[serde(rename = "v")]
    pub volume: String,
    
    /// Number of trades
    #[serde(rename = "n")]
    pub number_of_trades: i64,
    
    /// Is this kline closed? (CRITICAL: true means the candle is complete)
    #[serde(rename = "x")]
    pub is_kline_closed: bool,
    
    /// Quote asset volume
    #[serde(rename = "q")]
    pub quote_asset_volume: String,
    
    /// Taker buy base asset volume
    #[serde(rename = "V")]
    pub taker_buy_base_asset_volume: String,
    
    /// Ignore field (can be ignored)
    #[serde(rename = "B")]
    pub ignore: String,
}

impl BinanceKlineData {
    /// Convert to internal FuturesOHLCVCandle structure
    pub fn to_futures_candle(&self) -> Result<FuturesOHLCVCandle, WebSocketError> {
        // Parse numeric fields
        let open = self.open.parse::<f64>()
            .map_err(|_| WebSocketError::Parse(format!("Invalid open price: {}", self.open)))?;
        
        let high = self.high.parse::<f64>()
            .map_err(|_| WebSocketError::Parse(format!("Invalid high price: {}", self.high)))?;
        
        let low = self.low.parse::<f64>()
            .map_err(|_| WebSocketError::Parse(format!("Invalid low price: {}", self.low)))?;
        
        let close = self.close.parse::<f64>()
            .map_err(|_| WebSocketError::Parse(format!("Invalid close price: {}", self.close)))?;
        
        let volume = self.volume.parse::<f64>()
            .map_err(|_| WebSocketError::Parse(format!("Invalid volume: {}", self.volume)))?;
        
        let taker_buy_base_asset_volume = self.taker_buy_base_asset_volume.parse::<f64>()
            .map_err(|_| WebSocketError::Parse(format!("Invalid taker buy base asset volume: {}", self.taker_buy_base_asset_volume)))?;
        
        Ok(FuturesOHLCVCandle {
            open_time: self.start_time,
            close_time: self.close_time,
            open,
            high,
            low,
            close,
            volume,
            number_of_trades: self.number_of_trades as u64,
            taker_buy_base_asset_volume,
            closed: self.is_kline_closed,
        })
    }
    
    /// Check if this is a completed candle (closed kline)
    pub fn is_completed(&self) -> bool {
        self.is_kline_closed
    }
    
    /// Get the trade count for this kline
    pub fn trade_count(&self) -> i64 {
        self.number_of_trades
    }
    
    
    /// Calculate taker buy ratio (indicator of buying pressure)
    pub fn taker_buy_ratio(&self) -> Result<f64, WebSocketError> {
        let total_volume = self.volume.parse::<f64>()
            .map_err(|_| WebSocketError::Parse(format!("Invalid total volume: {}", self.volume)))?;
        
        let taker_buy_volume = self.taker_buy_base_asset_volume.parse::<f64>()
            .map_err(|_| WebSocketError::Parse(format!("Invalid taker buy volume: {}", self.taker_buy_base_asset_volume)))?;
        
        if total_volume == 0.0 {
            Ok(0.5) // Neutral ratio when no volume
        } else {
            Ok(taker_buy_volume / total_volume)
        }
    }
}

impl WebSocketMessage for BinanceKlineEvent {
    type Output = BinanceKlineEvent;
    
    fn parse(payload: &str) -> Result<Self::Output, WebSocketError> {
        serde_json::from_str(payload)
            .map_err(|e| WebSocketError::Parse(format!("Failed to parse kline message: {}", e)))
    }
    
    fn stream_type() -> StreamType {
        StreamType::Kline1m
    }
}

/// Parse a Binance kline WebSocket message
pub fn parse_kline_message(payload: &str) -> Result<BinanceKlineEvent, WebSocketError> {
    BinanceKlineEvent::parse(payload)
}

/// Handle combined stream format that includes stream field
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinanceCombinedStreamMessage {
    /// Stream name (e.g., "btcusdt@kline_1m")
    pub stream: String,
    
    /// The actual kline data
    pub data: BinanceKlineEvent,
}

impl BinanceCombinedStreamMessage {
    pub fn parse(payload: &str) -> Result<Self, WebSocketError> {
        serde_json::from_str(payload)
            .map_err(|e| WebSocketError::Parse(format!("Failed to parse combined stream message: {}", e)))
    }
    
    /// Extract the symbol from the stream name
    pub fn extract_symbol(&self) -> Option<String> {
        self.stream.split('@').next().map(|symbol_part| symbol_part.to_uppercase())
    }
}

/// Parse either single stream or combined stream format
pub fn parse_any_kline_message(payload: &str) -> Result<BinanceKlineEvent, WebSocketError> {
    // Try combined stream format first
    if let Ok(combined) = BinanceCombinedStreamMessage::parse(payload) {
        Ok(combined.data)
    } else {
        // Fall back to direct kline format
        parse_kline_message(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kline_message() {
        let json = r#"{
            "e": "kline",
            "E": 1672531200000,
            "s": "BTCUSDT",
            "k": {
                "t": 1672531140000,
                "T": 1672531199999,
                "s": "BTCUSDT",
                "i": "1m",
                "f": 123456789,
                "L": 123456799,
                "o": "16800.00",
                "c": "16850.00",
                "h": "16860.00",
                "l": "16795.00",
                "v": "12.5",
                "n": 150,
                "x": true,
                "q": "210625.00",
                "V": "8.2",
                "B": "0"
            }
        }"#;
        
        let event = parse_kline_message(json).unwrap();
        assert_eq!(event.event_type, "kline");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.kline.interval, "1m");
        assert!(event.kline.is_kline_closed);
        assert_eq!(event.kline.number_of_trades, 150);
    }
    
    #[test]
    fn test_parse_combined_stream_message() {
        let json = r#"{
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1672531200000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1672531140000,
                    "T": 1672531199999,
                    "s": "BTCUSDT",
                    "i": "1m",
                    "f": 123456789,
                    "L": 123456799,
                    "o": "16800.00",
                    "c": "16850.00",
                    "h": "16860.00",
                    "l": "16795.00",
                    "v": "12.5",
                    "n": 150,
                    "x": true,
                    "q": "210625.00",
                    "V": "8.2",
                    "Q": "138112.50",
                    "B": "0"
                }
            }
        }"#;
        
        let combined = BinanceCombinedStreamMessage::parse(json).unwrap();
        assert_eq!(combined.stream, "btcusdt@kline_1m");
        assert_eq!(combined.extract_symbol(), Some("BTCUSDT".to_string()));
        assert_eq!(combined.data.symbol, "BTCUSDT");
    }
    
    #[test]
    fn test_kline_to_futures_candle() {
        let kline_data = BinanceKlineData {
            start_time: 1672531140000,
            close_time: 1672531199999,
            symbol: "BTCUSDT".to_string(),
            interval: "1m".to_string(),
            first_trade_id: 123456789,
            last_trade_id: 123456799,
            open: "16800.00".to_string(),
            close: "16850.00".to_string(),
            high: "16860.00".to_string(),
            low: "16795.00".to_string(),
            volume: "12.5".to_string(),
            number_of_trades: 150,
            is_kline_closed: true,
            quote_asset_volume: "210625.00".to_string(),
            taker_buy_base_asset_volume: "8.2".to_string(),
            ignore: "0".to_string(),
        };
        
        let candle = kline_data.to_futures_candle().unwrap();
        assert_eq!(candle.open, 16800.0);
        assert_eq!(candle.close, 16850.0);
        assert_eq!(candle.high, 16860.0);
        assert_eq!(candle.low, 16795.0);
        assert_eq!(candle.volume, 12.5);
        assert_eq!(candle.number_of_trades, 150);
    }
    
    #[test]
    fn test_kline_taker_buy_ratio() {
        let kline_data = BinanceKlineData {
            start_time: 1672531140000,
            close_time: 1672531199999,
            symbol: "BTCUSDT".to_string(),
            interval: "1m".to_string(),
            first_trade_id: 123456789,
            last_trade_id: 123456799,
            open: "16800.00".to_string(),
            close: "16850.00".to_string(),
            high: "16860.00".to_string(),
            low: "16795.00".to_string(),
            volume: "10.0".to_string(),
            number_of_trades: 150,
            is_kline_closed: true,
            quote_asset_volume: "168250.00".to_string(), // Keep for parsing compatibility
            taker_buy_base_asset_volume: "6.0".to_string(),
            ignore: "0".to_string(),
        };
        
        let taker_buy_ratio = kline_data.taker_buy_ratio().unwrap();
        assert_eq!(taker_buy_ratio, 0.6); // 6.0 / 10.0
    }
    
    #[test]
    fn test_parse_any_kline_message() {
        // Test direct format
        let direct_json = r#"{
            "e": "kline",
            "E": 1672531200000,
            "s": "BTCUSDT",
            "k": {
                "t": 1672531140000,
                "T": 1672531199999,
                "s": "BTCUSDT",
                "i": "1m",
                "f": 123456789,
                "L": 123456799,
                "o": "16800.00",
                "c": "16850.00",
                "h": "16860.00",
                "l": "16795.00",
                "v": "12.5",
                "n": 150,
                "x": true,
                "q": "210625.00",
                "V": "8.2",
                "B": "0"
            }
        }"#;
        
        let event = parse_any_kline_message(direct_json).unwrap();
        assert_eq!(event.symbol, "BTCUSDT");
        
        // Test combined format
        let combined_json = r#"{
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1672531200000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1672531140000,
                    "T": 1672531199999,
                    "s": "BTCUSDT",
                    "i": "1m",
                    "f": 123456789,
                    "L": 123456799,
                    "o": "16800.00",
                    "c": "16850.00",
                    "h": "16860.00",
                    "l": "16795.00",
                    "v": "12.5",
                    "n": 150,
                    "x": true,
                    "q": "210625.00",
                    "V": "8.2",
                    "Q": "138112.50",
                    "B": "0"
                }
            }
        }"#;
        
        let event = parse_any_kline_message(combined_json).unwrap();
        assert_eq!(event.symbol, "BTCUSDT");
    }
}