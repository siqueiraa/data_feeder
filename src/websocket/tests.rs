use tempfile::TempDir;
use tokio::time::{sleep, Duration};
use kameo::request::MessageSend;

use crate::websocket::{
    actor::{WebSocketActor, WebSocketTell, WebSocketAsk, WebSocketReply},
    binance::kline::{BinanceKlineData, parse_any_kline_message, BinanceCombinedStreamMessage},
    connection::{ConnectionManager, validate_symbol, normalize_symbols},
    types::{StreamType, StreamSubscription, ConnectionStatus, WebSocketError},
};

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_stream_type_properties() {
        assert_eq!(StreamType::Kline1m.binance_suffix(), "kline_1m");
        assert!(StreamType::Kline1m.is_implemented());
        assert!(!StreamType::Ticker24hr.is_implemented());
        assert!(!StreamType::Depth.is_implemented());
        assert!(!StreamType::Trade.is_implemented());
    }

    #[test]
    fn test_stream_subscription_creation() {
        let subscription = StreamSubscription::new(
            StreamType::Kline1m,
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );

        assert_eq!(subscription.stream_type, StreamType::Kline1m);
        assert_eq!(subscription.symbols, vec!["BTCUSDT", "ETHUSDT"]);
        assert!(!subscription.is_active);

        let binance_streams = subscription.binance_streams();
        assert_eq!(binance_streams, vec!["btcusdt@kline_1m", "ethusdt@kline_1m"]);
    }

    #[test]
    fn test_symbol_validation() {
        assert!(validate_symbol("BTCUSDT").is_ok());
        assert!(validate_symbol("ETHUSDT").is_ok());
        assert!(validate_symbol("1000PEPEUSDT").is_ok());
        
        assert!(validate_symbol("").is_err());
        assert!(validate_symbol("BTC-USDT").is_err());
        assert!(validate_symbol("BTC/USDT").is_err());
        assert!(validate_symbol("BTC USDT").is_err());
    }

    #[test]
    fn test_symbol_normalization() {
        let symbols = vec![
            "btcusdt".to_string(),
            "ETHUSDT".to_string(),
            "AdaUsdt".to_string()
        ];
        
        let normalized = normalize_symbols(&symbols).unwrap();
        assert_eq!(normalized, vec!["BTCUSDT", "ETHUSDT", "ADAUSDT"]);
    }

    #[test]
    fn test_connection_manager_creation() {
        let manager = ConnectionManager::new_binance_futures();
        assert!(matches!(manager.status(), ConnectionStatus::Disconnected));
        assert_eq!(manager.stats().messages_received, 0);
    }

    #[test]
    fn test_connection_manager_url_building() {
        let manager = ConnectionManager::new_binance_futures();
        
        // Single stream URL
        let single_subscription = StreamSubscription::new(
            StreamType::Kline1m,
            vec!["BTCUSDT".to_string()]
        );
        let single_url = manager.build_single_stream_url(&single_subscription).unwrap();
        assert_eq!(single_url, "wss://fstream.binance.com/ws/btcusdt@kline_1m");

        // Multi-stream URL
        let multi_subscription = StreamSubscription::new(
            StreamType::Kline1m,
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
        let multi_url = manager.build_multi_stream_url(&[multi_subscription]).unwrap();
        assert_eq!(multi_url, "wss://fstream.binance.com/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m");
    }
}

#[cfg(test)]
mod parsing_tests {
    use super::*;

    #[test]
    fn test_parse_direct_kline_message() {
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
                "Q": "138112.50",
                "B": "0"
            }
        }"#;

        let event = parse_any_kline_message(json).unwrap();
        assert_eq!(event.event_type, "kline");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.kline.interval, "1m");
        assert!(event.kline.is_kline_closed);
        assert_eq!(event.kline.number_of_trades, 150);

        // Test conversion to futures candle
        let candle = event.kline.to_futures_candle().unwrap();
        assert_eq!(candle.open, 16800.0);
        assert_eq!(candle.close, 16850.0);
        assert_eq!(candle.high, 16860.0);
        assert_eq!(candle.low, 16795.0);
        assert_eq!(candle.volume, 12.5);
        assert_eq!(candle.number_of_trades, 150);
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

        let event = parse_any_kline_message(json).unwrap();
        assert_eq!(event.symbol, "BTCUSDT");
        assert!(event.kline.is_kline_closed);

        // Test combined stream parsing directly
        let combined = BinanceCombinedStreamMessage::parse(json).unwrap();
        assert_eq!(combined.stream, "btcusdt@kline_1m");
        assert_eq!(combined.extract_symbol(), Some("BTCUSDT".to_string()));
    }

    #[test]
    fn test_kline_metrics_calculation() {
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
            quote_asset_volume: "168250.00".to_string(),
            taker_buy_base_asset_volume: "6.0".to_string(),
            ignore: "0".to_string(),
        };

        // Test taker buy ratio
        let taker_ratio = kline_data.taker_buy_ratio().unwrap();
        assert_eq!(taker_ratio, 0.6);

        // Test completed status
        assert!(kline_data.is_completed());
        assert_eq!(kline_data.trade_count(), 150);
    }

    #[test]
    fn test_parse_invalid_messages() {
        // Invalid JSON
        let invalid_json = r#"{"invalid": json}"#;
        assert!(parse_any_kline_message(invalid_json).is_err());

        // Missing required fields
        let incomplete_json = r#"{"e": "kline"}"#;
        assert!(parse_any_kline_message(incomplete_json).is_err());

        // Invalid number format
        let invalid_numbers = r#"{
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
                "o": "invalid_price",
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
        }"#;

        let event = parse_any_kline_message(invalid_numbers).unwrap();
        assert!(event.kline.to_futures_candle().is_err());
    }
}

#[cfg(test)]
mod actor_tests {
    use super::*;
    use kameo;

    #[tokio::test]
    async fn test_websocket_actor_creation() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        
        assert_eq!(actor.env_count(), 0);
        assert_eq!(actor.candle_db_count(), 0);
        assert_eq!(actor.recent_candles_count(), 0);
    }

    #[tokio::test]
    async fn test_websocket_actor_subscription_management() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        let actor_ref = kameo::spawn(actor);

        // Test subscription
        let subscribe_msg = WebSocketTell::Subscribe {
            stream_type: StreamType::Kline1m,
            symbols: vec!["BTCUSDT".to_string()],
        };
        actor_ref.tell(subscribe_msg).send().await.unwrap();

        // Give some time for processing
        sleep(Duration::from_millis(10)).await;

        // Test getting active streams
        let active_streams_msg = WebSocketAsk::GetActiveStreams;
        match actor_ref.ask(active_streams_msg).await.unwrap() {
            WebSocketReply::ActiveStreams(streams) => {
                assert_eq!(streams.len(), 1);
                assert_eq!(streams[0].stream_type, StreamType::Kline1m);
                assert_eq!(streams[0].symbols, vec!["BTCUSDT"]);
            }
            _ => panic!("Expected ActiveStreams reply"),
        }

        // Test unsubscription
        let unsubscribe_msg = WebSocketTell::Unsubscribe {
            stream_type: StreamType::Kline1m,
            symbols: vec!["BTCUSDT".to_string()],
        };
        actor_ref.tell(unsubscribe_msg).send().await.unwrap();

        // Give some time for processing
        sleep(Duration::from_millis(10)).await;

        // Verify subscription removed
        let active_streams_msg = WebSocketAsk::GetActiveStreams;
        match actor_ref.ask(active_streams_msg).await.unwrap() {
            WebSocketReply::ActiveStreams(streams) => {
                assert_eq!(streams.len(), 0);
            }
            _ => panic!("Expected ActiveStreams reply"),
        }
    }

    #[tokio::test]
    async fn test_websocket_actor_connection_status() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        let actor_ref = kameo::spawn(actor);

        let status_msg = WebSocketAsk::GetConnectionStatus;
        match actor_ref.ask(status_msg).await.unwrap() {
            WebSocketReply::ConnectionStatus { status, stats } => {
                assert!(matches!(status, ConnectionStatus::Disconnected));
                assert_eq!(stats.messages_received, 0);
            }
            _ => panic!("Expected ConnectionStatus reply"),
        }
    }

    #[tokio::test]
    async fn test_websocket_actor_invalid_subscription() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        let actor_ref = kameo::spawn(actor);

        // Try to subscribe to unimplemented stream type
        let subscribe_msg = WebSocketTell::Subscribe {
            stream_type: StreamType::Ticker24hr, // Not implemented
            symbols: vec!["BTCUSDT".to_string()],
        };
        actor_ref.tell(subscribe_msg).send().await.unwrap();

        // Give some time for processing
        sleep(Duration::from_millis(10)).await;

        // Should have no active streams due to error
        let active_streams_msg = WebSocketAsk::GetActiveStreams;
        match actor_ref.ask(active_streams_msg).await.unwrap() {
            WebSocketReply::ActiveStreams(streams) => {
                assert_eq!(streams.len(), 0);
            }
            _ => panic!("Expected ActiveStreams reply"),
        }
    }

    #[tokio::test]
    async fn test_websocket_actor_recent_candles() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        let actor_ref = kameo::spawn(actor);

        // Test getting recent candles for non-existent symbol
        let recent_msg = WebSocketAsk::GetRecentCandles {
            symbol: "BTCUSDT".to_string(),
            limit: 10,
        };
        match actor_ref.ask(recent_msg).await.unwrap() {
            WebSocketReply::RecentCandles(candles) => {
                assert_eq!(candles.len(), 0);
            }
            _ => panic!("Expected RecentCandles reply"),
        }
    }

    #[tokio::test]
    async fn test_websocket_actor_partial_unsubscription() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        let actor_ref = kameo::spawn(actor);

        // Subscribe to multiple symbols
        let subscribe_msg = WebSocketTell::Subscribe {
            stream_type: StreamType::Kline1m,
            symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        };
        actor_ref.tell(subscribe_msg).send().await.unwrap();
        
        sleep(Duration::from_millis(10)).await;

        // Verify both symbols are subscribed
        match actor_ref.ask(WebSocketAsk::GetActiveStreams).await.unwrap() {
            WebSocketReply::ActiveStreams(streams) => {
                assert_eq!(streams.len(), 1);
                assert_eq!(streams[0].symbols.len(), 2);
                assert!(streams[0].symbols.contains(&"BTCUSDT".to_string()));
                assert!(streams[0].symbols.contains(&"ETHUSDT".to_string()));
            }
            _ => panic!("Expected ActiveStreams reply"),
        }

        // Unsubscribe from one symbol only
        let unsubscribe_msg = WebSocketTell::Unsubscribe {
            stream_type: StreamType::Kline1m,
            symbols: vec!["ETHUSDT".to_string()],
        };
        actor_ref.tell(unsubscribe_msg).send().await.unwrap();
        
        sleep(Duration::from_millis(10)).await;

        // Verify only BTCUSDT remains
        match actor_ref.ask(WebSocketAsk::GetActiveStreams).await.unwrap() {
            WebSocketReply::ActiveStreams(streams) => {
                assert_eq!(streams.len(), 1);
                assert_eq!(streams[0].symbols.len(), 1);
                assert_eq!(streams[0].symbols[0], "BTCUSDT");
            }
            _ => panic!("Expected ActiveStreams reply"),
        }

        // Unsubscribe from remaining symbol
        let unsubscribe_msg = WebSocketTell::Unsubscribe {
            stream_type: StreamType::Kline1m,
            symbols: vec!["BTCUSDT".to_string()],
        };
        actor_ref.tell(unsubscribe_msg).send().await.unwrap();
        
        sleep(Duration::from_millis(10)).await;

        // Verify no subscriptions remain
        match actor_ref.ask(WebSocketAsk::GetActiveStreams).await.unwrap() {
            WebSocketReply::ActiveStreams(streams) => {
                assert_eq!(streams.len(), 0);
            }
            _ => panic!("Expected ActiveStreams reply"),
        }
    }
}

#[cfg(test)]
mod error_handling_tests {
    use super::*;

    #[test]
    fn test_websocket_error_types() {
        let connection_error = WebSocketError::Connection("Test connection error".to_string());
        assert!(connection_error.is_recoverable());

        let parse_error = WebSocketError::Parse("Test parse error".to_string());
        assert!(!parse_error.is_recoverable());

        let not_implemented_error = WebSocketError::NotImplemented(StreamType::Ticker24hr);
        assert!(!not_implemented_error.is_recoverable());

        let timeout_error = WebSocketError::Timeout("Test timeout".to_string());
        assert!(timeout_error.is_recoverable());

        let rate_limit_error = WebSocketError::RateLimit;
        assert!(rate_limit_error.is_recoverable());
    }

    #[test]
    fn test_connection_manager_invalid_urls() {
        let manager = ConnectionManager::new_binance_futures();

        // Empty subscription should fail
        let empty_subscriptions: Vec<StreamSubscription> = vec![];
        assert!(manager.build_multi_stream_url(&empty_subscriptions).is_err());

        // Single stream with multiple symbols should fail
        let invalid_single = StreamSubscription::new(
            StreamType::Kline1m,
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
        assert!(manager.build_single_stream_url(&invalid_single).is_err());
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_full_message_processing_flow() {
        // Create a realistic kline message
        let kline_json = r#"{
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
        }"#;

        // Parse the message
        let event = parse_any_kline_message(kline_json).unwrap();
        
        // Verify parsing
        assert_eq!(event.symbol, "BTCUSDT");
        assert!(event.kline.is_completed());
        
        // Convert to candle
        let candle = event.kline.to_futures_candle().unwrap();
        
        // Verify candle data
        assert_eq!(candle.open, 16800.0);
        assert_eq!(candle.close, 16850.0);
        assert_eq!(candle.volume, 12.5);
        assert_eq!(candle.number_of_trades, 150);
        
        // Calculate additional metrics
        let taker_ratio = event.kline.taker_buy_ratio().unwrap();
        
        assert!((taker_ratio - 0.656).abs() < 0.001); // 8.2 / 12.5 ≈ 0.656
    }

    #[tokio::test]
    async fn test_actor_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        let actor_ref = kameo::spawn(actor);

        // Test initial state
        match actor_ref.ask(WebSocketAsk::GetConnectionStatus).await.unwrap() {
            WebSocketReply::ConnectionStatus { status, .. } => {
                assert!(matches!(status, ConnectionStatus::Disconnected));
            }
            _ => panic!("Expected ConnectionStatus reply"),
        }

        // Test subscription
        actor_ref.tell(WebSocketTell::Subscribe {
            stream_type: StreamType::Kline1m,
            symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        }).send().await.unwrap();

        sleep(Duration::from_millis(10)).await;

        // Verify subscriptions
        match actor_ref.ask(WebSocketAsk::GetActiveStreams).await.unwrap() {
            WebSocketReply::ActiveStreams(streams) => {
                assert_eq!(streams.len(), 1);
                assert_eq!(streams[0].symbols, vec!["BTCUSDT", "ETHUSDT"]);
            }
            _ => panic!("Expected ActiveStreams reply"),
        }

        // Test health check
        actor_ref.tell(WebSocketTell::HealthCheck).send().await.unwrap();
        sleep(Duration::from_millis(10)).await;

        // Test stats
        match actor_ref.ask(WebSocketAsk::GetStats).await.unwrap() {
            WebSocketReply::Stats(stats) => {
                assert_eq!(stats.messages_received, 0);
            }
            _ => panic!("Expected Stats reply"),
        }
    }
}

/// Performance and stress tests
#[cfg(test)]
mod performance_tests {
    use super::*;

    #[test]
    fn test_message_parsing_performance() {
        let kline_json = r#"{
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
        }"#;

        let start = std::time::Instant::now();
        
        // Parse 1000 messages
        for _ in 0..1000 {
            let event = parse_any_kline_message(kline_json).unwrap();
            let _candle = event.kline.to_futures_candle().unwrap();
        }
        
        let duration = start.elapsed();
        println!("Parsed 1000 messages in {:?} ({:.2} msg/ms)", 
                 duration, 1000.0 / duration.as_millis() as f64);
        
        // Should be able to parse at least 100 messages per millisecond
        assert!(duration.as_millis() < 100);
    }

    #[tokio::test]
    async fn test_actor_subscription_stress() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        let actor_ref = kameo::spawn(actor);

        // Add many subscriptions rapidly
        for i in 0..50 {
            let symbol = format!("SYMBOL{}USDT", i);
            actor_ref.tell(WebSocketTell::Subscribe {
                stream_type: StreamType::Kline1m,
                symbols: vec![symbol],
            }).send().await.unwrap();
        }

        sleep(Duration::from_millis(100)).await;

        // Verify all subscriptions were added
        match actor_ref.ask(WebSocketAsk::GetActiveStreams).await.unwrap() {
            WebSocketReply::ActiveStreams(streams) => {
                assert_eq!(streams.len(), 50);
            }
            _ => panic!("Expected ActiveStreams reply"),
        }
    }
}