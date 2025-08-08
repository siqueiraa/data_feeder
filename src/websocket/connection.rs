use fastwebsockets::{OpCode, Frame, WebSocket, Role};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::websocket::types::{
    ConnectionStats, ConnectionStatus, StreamSubscription, WebSocketError,
};

/// Connection manager for WebSocket streams
#[derive(Clone)]
pub struct ConnectionManager {
    base_url: String,
    stats: ConnectionStats,
    status: ConnectionStatus,
    max_reconnect_attempts: u32,
    reconnect_delay: Duration,
    ping_interval: Duration,
}

impl ConnectionManager {
    /// Create a new connection manager for Binance futures
    pub fn new_binance_futures() -> Self {
        Self {
            base_url: "wss://fstream.binance.com".to_string(),
            stats: ConnectionStats::new(),
            status: ConnectionStatus::Disconnected,
            max_reconnect_attempts: 10,
            reconnect_delay: Duration::from_secs(5),
            ping_interval: Duration::from_secs(30),
        }
    }

    /// Create a new connection manager with custom settings
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            stats: ConnectionStats::new(),
            status: ConnectionStatus::Disconnected,
            max_reconnect_attempts: 10,
            reconnect_delay: Duration::from_secs(5),
            ping_interval: Duration::from_secs(30),
        }
    }

    /// Get current connection status
    pub fn status(&self) -> &ConnectionStatus {
        &self.status
    }

    /// Get connection statistics
    pub fn stats(&self) -> &ConnectionStats {
        &self.stats
    }

    /// Build WebSocket URL for single stream subscription
    pub fn build_single_stream_url(&self, subscription: &StreamSubscription) -> Result<String, WebSocketError> {
        if subscription.symbols.len() != 1 {
            return Err(WebSocketError::Subscription(
                "Single stream URL requires exactly one symbol".to_string()
            ));
        }

        let symbol = &subscription.symbols[0];
        let stream = format!("{}@{}", symbol.to_lowercase(), subscription.stream_type.binance_suffix());
        Ok(format!("{}/ws/{}", self.base_url, stream))
    }

    /// Build WebSocket URL for multiple stream subscription
    pub fn build_multi_stream_url(&self, subscriptions: &[StreamSubscription]) -> Result<String, WebSocketError> {
        if subscriptions.is_empty() {
            return Err(WebSocketError::Subscription(
                "At least one subscription required".to_string()
            ));
        }

        let mut all_streams = Vec::new();
        for subscription in subscriptions {
            all_streams.extend(subscription.binance_streams());
        }

        if all_streams.is_empty() {
            return Err(WebSocketError::Subscription(
                "No valid streams found in subscriptions".to_string()
            ));
        }

        Ok(format!("{}/stream?streams={}", self.base_url, all_streams.join("/")))
    }

    /// Connect to WebSocket with automatic reconnection
    pub async fn connect_with_retry<F, Fut>(
        &mut self,
        url: &str,
        message_handler: F,
    ) -> Result<(), WebSocketError>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Result<(), WebSocketError>>,
    {
        self.connect_with_retry_and_callback(url, message_handler, |_| {}).await
    }

    /// Connect to WebSocket with automatic reconnection and reconnection callback
    pub async fn connect_with_retry_and_callback<F, Fut, C>(
        &mut self,
        url: &str,
        mut message_handler: F,
        reconnection_callback: C,
    ) -> Result<(), WebSocketError>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Result<(), WebSocketError>>,
        C: Fn(bool) + Clone,
    {
        let mut attempt = 0;

        loop {
            attempt += 1;
            self.status = if attempt == 1 {
                ConnectionStatus::Connecting
            } else {
                ConnectionStatus::Reconnecting { attempt }
            };

            match self.connect_once(url, &mut message_handler).await {
                Ok(_) => {
                    info!("WebSocket connection completed normally");
                    
                    // Call reconnection callback if this was a reconnection (not first connection)
                    let is_reconnection = attempt > 1;
                    reconnection_callback.clone()(is_reconnection);
                    
                    if attempt > self.max_reconnect_attempts {
                        break;
                    }
                    // Reset attempt counter on successful connection
                    attempt = 0;
                }
                Err(e) => {
                    if attempt >= self.max_reconnect_attempts {
                        let error = format!("Max reconnection attempts ({}) exceeded: {}", self.max_reconnect_attempts, e);
                        self.status = ConnectionStatus::Failed { error: error.clone() };
                        return Err(WebSocketError::Connection(error));
                    }

                    warn!(
                        "WebSocket connection failed (attempt {}/{}): {}. Retrying in {:?}",
                        attempt, self.max_reconnect_attempts, e, self.reconnect_delay
                    );

                    sleep(self.reconnect_delay).await;
                }
            }
        }

        Ok(())
    }

    /// Single connection attempt
    async fn connect_once<F, Fut>(
        &mut self,
        url: &str,
        message_handler: &mut F,
    ) -> Result<(), WebSocketError>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Result<(), WebSocketError>>,
    {
        info!("Connecting to WebSocket: {}", url);

        // Parse URL
        let url_parsed = url::Url::parse(url)
            .map_err(|e| WebSocketError::Connection(format!("Invalid URL: {}", e)))?;
        
        let host = url_parsed.host_str()
            .ok_or_else(|| WebSocketError::Connection("No host in URL".to_string()))?;
        
        let port = if url_parsed.scheme() == "wss" { 443 } else { 80 };
        let path = url_parsed.path();
        let query = url_parsed.query().map_or(String::new(), |q| format!("?{}", q));
        
        // Create TCP connection
        let stream = tokio::net::TcpStream::connect((host, port))
            .await
            .map_err(|e| WebSocketError::Connection(format!("TCP connection failed: {}", e)))?;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Handle TLS for wss:// URLs
        if url_parsed.scheme() == "wss" {
            // TLS connection
            let connector = tokio_native_tls::TlsConnector::from(
                native_tls::TlsConnector::new()
                    .map_err(|e| WebSocketError::Connection(format!("TLS setup failed: {}", e)))?
            );
            let mut tls_stream = connector
                .connect(host, stream)
                .await
                .map_err(|e| WebSocketError::Connection(format!("TLS connection failed: {}", e)))?;
            
            // Use fastwebsockets client handshake directly
            let req = format!(
                "GET {}{} HTTP/1.1\r\n\
                Host: {}\r\n\
                Upgrade: websocket\r\n\
                Connection: Upgrade\r\n\
                Sec-WebSocket-Key: {}\r\n\
                Sec-WebSocket-Version: 13\r\n\r\n",
                path, query, host, fastwebsockets::handshake::generate_key()
            );

            // Send handshake request manually
            tls_stream.write_all(req.as_bytes()).await
                .map_err(|e| WebSocketError::Connection(format!("Failed to send handshake: {}", e)))?;

            // Read response
            let mut buffer = vec![0u8; 1024];
            let n = tls_stream.read(&mut buffer).await
                .map_err(|e| WebSocketError::Connection(format!("Failed to read handshake response: {}", e)))?;
            
            let response = String::from_utf8_lossy(&buffer[..n]);
            if !response.contains("101 Switching Protocols") {
                return Err(WebSocketError::Connection(format!("Invalid handshake response: {}", response)));
            }

            // Create WebSocket from the TLS stream
            let mut websocket = WebSocket::after_handshake(tls_stream, Role::Client);
            self.run_websocket_loop(&mut websocket, message_handler).await?;
        } else {
            // Plain connection
            let req = format!(
                "GET {}{} HTTP/1.1\r\n\
                Host: {}\r\n\
                Upgrade: websocket\r\n\
                Connection: Upgrade\r\n\
                Sec-WebSocket-Key: {}\r\n\
                Sec-WebSocket-Version: 13\r\n\r\n",
                path, query, host, fastwebsockets::handshake::generate_key()
            );

            // Send handshake request manually
            let mut stream = stream;
            stream.write_all(req.as_bytes()).await
                .map_err(|e| WebSocketError::Connection(format!("Failed to send handshake: {}", e)))?;

            // Read response
            let mut buffer = vec![0u8; 1024];
            let n = stream.read(&mut buffer).await
                .map_err(|e| WebSocketError::Connection(format!("Failed to read handshake response: {}", e)))?;
            
            let response = String::from_utf8_lossy(&buffer[..n]);
            if !response.contains("101 Switching Protocols") {
                return Err(WebSocketError::Connection(format!("Invalid handshake response: {}", response)));
            }

            // Create WebSocket from the plain stream  
            let mut websocket = WebSocket::after_handshake(stream, Role::Client);
            self.run_websocket_loop(&mut websocket, message_handler).await?;
        };

        Ok(())
    }

    /// Run the main WebSocket message loop
    async fn run_websocket_loop<S, F, Fut>(
        &mut self,
        websocket: &mut WebSocket<S>, 
        message_handler: &mut F
    ) -> Result<(), WebSocketError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Result<(), WebSocketError>>,
    {

        self.status = ConnectionStatus::Connected;
        self.stats.record_connection();
        info!("✅ WebSocket connected successfully");

        // Main message loop
        info!("📡 Starting WebSocket message receiving loop...");
        let result = async {
            loop {
                match websocket.read_frame().await {
                    Ok(frame) => {
                        match frame.opcode {
                            OpCode::Text => {
                                let text = String::from_utf8(frame.payload.to_vec())
                                    .map_err(|e| WebSocketError::Connection(format!("Invalid UTF-8: {}", e)))?;
                                
                                self.stats.record_message();
                                debug!("📨 Received WebSocket message ({}b): {}", text.len(), text.chars().take(200).collect::<String>());

                                match message_handler(text).await {
                                    Ok(_) => {
                                        self.stats.record_parsed();
                                        debug!("✅ Successfully processed WebSocket message");
                                    }
                                    Err(e) => {
                                        self.stats.record_parse_error();
                                        warn!("❌ Failed to handle message: {}", e);
                                    }
                                }
                            }
                            OpCode::Pong => {
                                debug!("🏓 Received WebSocket pong");
                            }
                            OpCode::Close => {
                                info!("WebSocket closed");
                                break;
                            }
                            OpCode::Ping => {
                                debug!("Received WebSocket ping, sending pong");
                                let pong_frame = Frame::new(true, OpCode::Pong, None, frame.payload);
                                if let Err(e) = websocket.write_frame(pong_frame).await {
                                    warn!("Failed to send pong: {}", e);
                                }
                            }
                            OpCode::Binary => {
                                warn!("Received unexpected binary message");
                            }
                            OpCode::Continuation => {
                                debug!("Received continuation frame");
                            }
                        }
                    }
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        return Err(WebSocketError::Connection(format!("WebSocket error: {}", e)));
                    }
                }
            }
            Ok(())
        }.await;

        self.status = ConnectionStatus::Disconnected;
        info!("WebSocket disconnected");

        result
    }

    /// Set maximum reconnection attempts
    pub fn set_max_reconnect_attempts(&mut self, attempts: u32) {
        self.max_reconnect_attempts = attempts;
    }

    /// Set reconnection delay
    pub fn set_reconnect_delay(&mut self, delay: Duration) {
        self.reconnect_delay = delay;
    }

    /// Set ping interval
    pub fn set_ping_interval(&mut self, interval: Duration) {
        self.ping_interval = interval;
    }

    /// Reset connection statistics
    pub fn reset_stats(&mut self) {
        self.stats = ConnectionStats::new();
    }

    /// Check if connection is healthy based on recent message activity
    pub fn is_healthy(&self, max_idle_time: Duration) -> bool {
        self.is_healthy_with_grace(max_idle_time, Duration::from_secs(120))
    }
    
    /// Check if connection is healthy with configurable grace period
    pub fn is_healthy_with_grace(&self, max_idle_time: Duration, grace_period: Duration) -> bool {
        match &self.status {
            ConnectionStatus::Connected => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                
                // If we have recent messages, check idle time
                if let Some(last_message_time) = self.stats.last_message_time {
                    let idle_time = Duration::from_millis((now_ms - last_message_time) as u64);
                    return idle_time <= max_idle_time;
                }
                
                // If no messages yet, check if connection was established recently (grace period)
                if let Some(connection_time) = self.stats.connection_established_time {
                    let connection_age = Duration::from_millis((now_ms - connection_time) as u64);
                    return connection_age <= grace_period;
                }
                
                // Fallback: no connection time recorded, assume unhealthy
                false
            }
            _ => false,
        }
    }
}

/// Utility function to validate symbol format
pub fn validate_symbol(symbol: &str) -> Result<(), WebSocketError> {
    if symbol.is_empty() {
        return Err(WebSocketError::InvalidSymbol("Symbol cannot be empty".to_string()));
    }

    if !symbol.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(WebSocketError::InvalidSymbol(
            format!("Symbol '{}' contains invalid characters", symbol)
        ));
    }

    if !symbol.ends_with("USDT") && !symbol.ends_with("BUSD") {
        warn!("Symbol '{}' does not end with USDT or BUSD", symbol);
    }

    Ok(())
}

/// Utility function to validate and normalize symbols
pub fn normalize_symbols(symbols: &[String]) -> Result<Vec<String>, WebSocketError> {
    let mut normalized = Vec::new();
    
    for symbol in symbols {
        validate_symbol(symbol)?;
        normalized.push(symbol.to_uppercase());
    }
    
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::websocket::types::StreamType;

    #[test]
    fn test_connection_manager_creation() {
        let manager = ConnectionManager::new_binance_futures();
        assert_eq!(manager.base_url, "wss://fstream.binance.com");
        assert!(matches!(manager.status, ConnectionStatus::Disconnected));
    }

    #[test]
    fn test_build_single_stream_url() {
        let manager = ConnectionManager::new_binance_futures();
        let subscription = StreamSubscription::new(
            StreamType::Kline1m,
            vec!["BTCUSDT".to_string()]
        );

        let url = manager.build_single_stream_url(&subscription).unwrap();
        assert_eq!(url, "wss://fstream.binance.com/ws/btcusdt@kline_1m");
    }

    #[test]
    fn test_build_multi_stream_url() {
        let manager = ConnectionManager::new_binance_futures();
        let subscriptions = vec![
            StreamSubscription::new(
                StreamType::Kline1m,
                vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
            ),
        ];

        let url = manager.build_multi_stream_url(&subscriptions).unwrap();
        assert_eq!(url, "wss://fstream.binance.com/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m");
    }

    #[test]
    fn test_validate_symbol() {
        assert!(validate_symbol("BTCUSDT").is_ok());
        assert!(validate_symbol("ETHUSDT").is_ok());
        assert!(validate_symbol("").is_err());
        assert!(validate_symbol("BTC-USDT").is_err());
        assert!(validate_symbol("BTC/USDT").is_err());
    }

    #[test]
    fn test_normalize_symbols() {
        let symbols = vec!["btcusdt".to_string(), "ETHUSDT".to_string()];
        let normalized = normalize_symbols(&symbols).unwrap();
        assert_eq!(normalized, vec!["BTCUSDT", "ETHUSDT"]);
    }

    #[test]
    fn test_connection_health() {
        let mut manager = ConnectionManager::new_binance_futures();
        
        // Disconnected is not healthy
        assert!(!manager.is_healthy(Duration::from_secs(60)));
        
        // Connected but no messages is healthy initially
        manager.status = ConnectionStatus::Connected;
        manager.stats.record_connection(); // Set connection establishment time
        assert!(manager.is_healthy(Duration::from_secs(60)));
        
        // Recent message is healthy
        manager.stats.record_message();
        assert!(manager.is_healthy(Duration::from_secs(60)));
    }
}