use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Supported WebSocket stream types
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamType {
    /// 1-minute kline/candlestick data
    Kline1m,
    /// 24-hour ticker statistics
    Ticker24hr,
    /// Order book depth updates
    Depth,
    /// Individual trade data
    Trade,
}

impl StreamType {
    /// Get the Binance stream suffix for this stream type
    pub fn binance_suffix(&self) -> &'static str {
        match self {
            StreamType::Kline1m => "kline_1m",
            StreamType::Ticker24hr => "ticker",
            StreamType::Depth => "depth@100ms",
            StreamType::Trade => "trade",
        }
    }

    /// Check if this stream type is currently implemented
    pub fn is_implemented(&self) -> bool {
        match self {
            StreamType::Kline1m => true,
            _ => false, // Future implementations
        }
    }
}

impl fmt::Display for StreamType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamType::Kline1m => write!(f, "kline_1m"),
            StreamType::Ticker24hr => write!(f, "ticker_24hr"),
            StreamType::Depth => write!(f, "depth"),
            StreamType::Trade => write!(f, "trade"),
        }
    }
}

/// WebSocket connection status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting { attempt: u32 },
    Failed { error: String },
}

impl fmt::Display for ConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionStatus::Disconnected => write!(f, "Disconnected"),
            ConnectionStatus::Connecting => write!(f, "Connecting"),
            ConnectionStatus::Connected => write!(f, "Connected"),
            ConnectionStatus::Reconnecting { attempt } => write!(f, "Reconnecting (attempt {})", attempt),
            ConnectionStatus::Failed { error } => write!(f, "Failed: {}", error),
        }
    }
}

/// Stream subscription information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSubscription {
    pub stream_type: StreamType,
    pub symbols: Vec<String>,
    pub is_active: bool,
}

impl StreamSubscription {
    pub fn new(stream_type: StreamType, symbols: Vec<String>) -> Self {
        Self {
            stream_type,
            symbols,
            is_active: false,
        }
    }

    /// Generate the stream identifier for this subscription
    pub fn stream_id(&self) -> String {
        format!("{}_{}", self.stream_type, self.symbols.join("_"))
    }

    /// Generate Binance WebSocket stream names for this subscription
    pub fn binance_streams(&self) -> Vec<String> {
        self.symbols
            .iter()
            .map(|symbol| format!("{}@{}", symbol.to_lowercase(), self.stream_type.binance_suffix()))
            .collect()
    }
}

/// WebSocket error types
#[derive(Error, Debug, Clone)]
pub enum WebSocketError {
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Stream type not implemented: {0}")]
    NotImplemented(StreamType),
    
    #[error("Invalid symbol: {0}")]
    InvalidSymbol(String),
    
    #[error("Subscription error: {0}")]
    Subscription(String),
    
    #[error("Timeout error: {0}")]
    Timeout(String),
    
    #[error("Authentication error: {0}")]
    Authentication(String),
    
    #[error("Rate limit exceeded")]
    RateLimit,
    
    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl WebSocketError {
    pub fn is_recoverable(&self) -> bool {
        matches!(self, WebSocketError::Connection(_) | WebSocketError::Timeout(_) | WebSocketError::RateLimit | WebSocketError::Unknown(_))
    }
}

/// Trait for parsing WebSocket messages into structured data
pub trait WebSocketMessage {
    type Output;
    
    /// Parse a raw WebSocket message into structured data
    fn parse(payload: &str) -> Result<Self::Output, WebSocketError>;
    
    /// Get the stream type this message belongs to
    fn stream_type() -> StreamType;
}

/// Connection statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectionStats {
    pub messages_received: u64,
    pub messages_parsed: u64,
    pub parse_errors: u64,
    pub connection_count: u32,
    pub last_message_time: Option<i64>,
    pub connection_established_time: Option<i64>,
    pub uptime_seconds: u64,
}

impl ConnectionStats {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn record_message(&mut self) {
        self.messages_received += 1;
        self.last_message_time = Some(chrono::Utc::now().timestamp_millis());
    }
    
    pub fn record_parsed(&mut self) {
        self.messages_parsed += 1;
    }
    
    pub fn record_parse_error(&mut self) {
        self.parse_errors += 1;
    }
    
    pub fn record_connection(&mut self) {
        self.connection_count += 1;
        self.connection_established_time = Some(chrono::Utc::now().timestamp_millis());
    }
    
    pub fn parse_success_rate(&self) -> f64 {
        if self.messages_received == 0 {
            0.0
        } else {
            self.messages_parsed as f64 / self.messages_received as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_type_binance_suffix() {
        assert_eq!(StreamType::Kline1m.binance_suffix(), "kline_1m");
        assert_eq!(StreamType::Ticker24hr.binance_suffix(), "ticker");
        assert_eq!(StreamType::Depth.binance_suffix(), "depth@100ms");
        assert_eq!(StreamType::Trade.binance_suffix(), "trade");
    }

    #[test]
    fn test_stream_subscription() {
        let subscription = StreamSubscription::new(
            StreamType::Kline1m,
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
        
        assert_eq!(subscription.stream_type, StreamType::Kline1m);
        assert_eq!(subscription.symbols, vec!["BTCUSDT", "ETHUSDT"]);
        assert!(!subscription.is_active);
        
        let streams = subscription.binance_streams();
        assert_eq!(streams, vec!["btcusdt@kline_1m", "ethusdt@kline_1m"]);
    }

    #[test]
    fn test_websocket_error_recoverable() {
        assert!(WebSocketError::Connection("test".to_string()).is_recoverable());
        assert!(WebSocketError::Timeout("test".to_string()).is_recoverable());
        assert!(!WebSocketError::Parse("test".to_string()).is_recoverable());
        assert!(!WebSocketError::InvalidSymbol("test".to_string()).is_recoverable());
    }

    #[test]
    fn test_connection_stats() {
        let mut stats = ConnectionStats::new();
        
        stats.record_message();
        stats.record_parsed();
        assert_eq!(stats.parse_success_rate(), 1.0);
        
        stats.record_message();
        stats.record_parse_error();
        assert_eq!(stats.parse_success_rate(), 0.5);
    }
}