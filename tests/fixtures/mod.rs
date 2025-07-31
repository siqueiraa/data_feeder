use data_feeder::historical::structs::FuturesOHLCVCandle;
use data_feeder::technical_analysis::structs::{IndicatorOutput, TrendDirection};
use data_feeder::kafka::KafkaConfig;

/// Create sample OHLCV candle data for testing
pub fn create_sample_candle(timestamp: i64, price: f64, volume: f64) -> FuturesOHLCVCandle {
    FuturesOHLCVCandle {
        open_time: timestamp,
        close_time: timestamp + 59999, // 1 minute candle
        open: price,
        high: price + 1.0,
        low: price - 1.0,
        close: price + 0.5,
        volume,
        number_of_trades: 100,
        taker_buy_base_asset_volume: volume * 0.6,
        closed: true,
    }
}

/// Create sample indicator output for testing
pub fn create_sample_indicator_output(symbol: &str, timestamp: i64) -> IndicatorOutput {
    IndicatorOutput {
        symbol: symbol.to_string(),
        timestamp,
        close_5m: Some(50000.0),
        close_15m: Some(50100.0),
        close_60m: Some(50200.0),
        close_4h: Some(50300.0),
        ema21_1min: Some(49950.0),
        ema89_1min: Some(49900.0),
        ema89_5min: Some(49850.0),
        ema89_15min: Some(49800.0),
        ema89_1h: Some(49750.0),
        ema89_4h: Some(49700.0),
        trend_1min: TrendDirection::Buy,
        trend_5min: TrendDirection::Neutral,
        trend_15min: TrendDirection::Sell,
        trend_1h: TrendDirection::Buy,
        trend_4h: TrendDirection::Neutral,
        max_volume: Some(1000000.0),
        max_volume_price: Some(49950.0),
        max_volume_time: Some("2025-01-01T12:00:00Z".to_string()),
        max_volume_trend: Some(TrendDirection::Buy),
        volume_quantiles: None,
    }
}

/// Create test Kafka configuration
pub fn create_test_kafka_config() -> KafkaConfig {
    KafkaConfig {
        enabled: true,
        bootstrap_servers: vec!["140.238.175.132:9092".to_string()],
        topic_prefix: "test_ta_".to_string(),
        client_id: "data_feeder_test".to_string(),
        acks: "all".to_string(),
        retries: 3,
        max_in_flight_requests_per_connection: 1,
        compression_type: "snappy".to_string(),
        batch_size: 1024,
        linger_ms: 5,
        request_timeout_ms: 10000,
        delivery_timeout_ms: 30000,
    }
}

/// Create disabled Kafka configuration for testing
#[allow(dead_code)]
pub fn create_disabled_kafka_config() -> KafkaConfig {
    KafkaConfig {
        enabled: false,
        ..create_test_kafka_config()
    }
}

/// Create multiple sample indicator outputs for performance testing
pub fn create_sample_indicators_batch(symbol: &str, count: usize) -> Vec<IndicatorOutput> {
    let base_timestamp = chrono::Utc::now().timestamp_millis();
    (0..count)
        .map(|i| create_sample_indicator_output(symbol, base_timestamp + (i as i64 * 60000)))
        .collect()
}

/// Create realistic trading candles with price movement
pub fn create_realistic_candles(_symbol: &str, count: usize, start_price: f64) -> Vec<FuturesOHLCVCandle> {
    let base_timestamp = chrono::Utc::now().timestamp_millis();
    let mut current_price = start_price;
    
    (0..count)
        .map(|i| {
            // Simulate price movement
            let price_change = (i as f64 * 0.1) % 100.0 - 50.0; // Random-ish price movement
            current_price += price_change;
            
            let timestamp = base_timestamp + (i as i64 * 60000); // 1 minute intervals
            create_sample_candle(timestamp, current_price, 1000.0 + (i as f64 * 10.0))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_sample_candle() {
        let candle = create_sample_candle(1640995200000, 50000.0, 1000.0);
        assert_eq!(candle.open, 50000.0);
        assert_eq!(candle.high, 50001.0);
        assert_eq!(candle.low, 49999.0);
        assert_eq!(candle.close, 50000.5);
        assert_eq!(candle.volume, 1000.0);
        assert!(candle.closed);
    }

    #[test]
    fn test_create_sample_indicator_output() {
        let indicators = create_sample_indicator_output("BTCUSDT", 1640995200000);
        assert_eq!(indicators.symbol, "BTCUSDT");
        assert_eq!(indicators.timestamp, 1640995200000);
        assert!(indicators.close_5m.is_some());
        assert!(matches!(indicators.trend_1min, TrendDirection::Buy));
    }

    #[test]
    fn test_create_test_kafka_config() {
        let config = create_test_kafka_config();
        assert!(config.enabled);
        assert_eq!(config.bootstrap_servers, vec!["140.238.175.132:9092"]);
        assert_eq!(config.topic_prefix, "test_ta_");
    }

    #[test]
    fn test_create_indicators_batch() {
        let indicators = create_sample_indicators_batch("BTCUSDT", 5);
        assert_eq!(indicators.len(), 5);
        
        // Check timestamps are sequential
        for i in 1..indicators.len() {
            assert!(indicators[i].timestamp > indicators[i-1].timestamp);
        }
    }

    #[test]
    fn test_create_realistic_candles() {
        let candles = create_realistic_candles("ETHUSDT", 10, 3000.0);
        assert_eq!(candles.len(), 10);
        
        // Check timestamps are sequential
        for i in 1..candles.len() {
            assert!(candles[i].open_time > candles[i-1].open_time);
        }
    }
}