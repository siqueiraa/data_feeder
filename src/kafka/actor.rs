use std::sync::Arc;
use std::time::Duration;

use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::technical_analysis::structs::IndicatorOutput;
use crate::technical_analysis::fast_serialization::SerializationBuffer;
use super::errors::KafkaProducerError;

/// Kafka producer configuration
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub enabled: bool,
    pub bootstrap_servers: Vec<String>,
    pub topic_prefix: String,
    pub client_id: String,
    pub acks: String,
    pub retries: u32,
    pub max_in_flight_requests_per_connection: u32,
    pub compression_type: String,
    pub batch_size: u32,
    pub linger_ms: u32,
    pub request_timeout_ms: u32,
    pub delivery_timeout_ms: u32,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bootstrap_servers: vec!["localhost:9092".to_string()],
            topic_prefix: "ta_".to_string(),
            client_id: "data_feeder_rust".to_string(),
            acks: "all".to_string(),
            retries: 3,
            max_in_flight_requests_per_connection: 1,
            compression_type: "snappy".to_string(),
            batch_size: 16384,
            linger_ms: 10,
            request_timeout_ms: 30000,
            delivery_timeout_ms: 120000,
        }
    }
}

/// Messages for Kafka Actor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KafkaTell {
    /// Publish technical analysis indicators
    PublishIndicators {
        indicators: Box<IndicatorOutput>,
    },
    /// Health check message
    HealthCheck,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KafkaAsk {
    /// Get producer health status
    GetHealthStatus,
    /// Get producer statistics
    GetStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KafkaReply {
    /// Health status response
    HealthStatus {
        is_healthy: bool,
        is_enabled: bool,
        last_error: Option<String>,
    },
    /// Statistics response
    Stats {
        messages_sent: u64,
        messages_failed: u64,
        last_send_time: Option<i64>,
    },
    /// Success response
    Success,
    /// Error response
    Error(String),
}

/// Kafka producer actor for publishing technical analysis data
pub struct KafkaActor {
    /// Configuration
    config: KafkaConfig,
    /// Kafka producer instance
    producer: Option<FutureProducer>,
    /// Health status
    is_healthy: bool,
    /// Last error message
    last_error: Option<Arc<String>>,
    /// Statistics
    messages_sent: u64,
    messages_failed: u64,
    last_send_time: Option<i64>,
    /// Data topic name (constructed from prefix)
    data_topic: String,
    /// Ultra-fast serialization buffer (PHASE 11 OPTIMIZATION)
    serialization_buffer: SerializationBuffer,
}

impl KafkaActor {
    /// Topic name constant for technical analysis data
    const DATA_TOPIC_SUFFIX: &'static str = "data";
    
    /// Create a new Kafka actor
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaProducerError> {
        let data_topic = format!("{}{}", config.topic_prefix, Self::DATA_TOPIC_SUFFIX);
        
        Ok(Self {
            config,
            producer: None,
            is_healthy: false,
            last_error: None,
            messages_sent: 0,
            messages_failed: 0,
            last_send_time: None,
            data_topic,
            serialization_buffer: SerializationBuffer::new(), // PHASE 11: Ultra-fast serialization
        })
    }

    /// Initialize Kafka producer
    async fn initialize_producer(&mut self) -> Result<(), KafkaProducerError> {
        if !self.config.enabled {
            info!("Kafka producer is disabled in configuration");
            return Ok(());
        }

        info!("Initializing Kafka producer...");
        info!("Kafka configuration:");
        info!("  bootstrap_servers: {:?}", self.config.bootstrap_servers);
        info!("  topic_prefix: {}", self.config.topic_prefix);
        info!("  data_topic: {}", self.data_topic);
        info!("  client_id: {}", self.config.client_id);

        let mut client_config = ClientConfig::new();
        
        // Basic configuration
        client_config
            .set("bootstrap.servers", self.config.bootstrap_servers.join(","))
            .set("client.id", &self.config.client_id)
            .set("acks", &self.config.acks)
            .set("retries", self.config.retries.to_string())
            .set("max.in.flight.requests.per.connection", self.config.max_in_flight_requests_per_connection.to_string())
            .set("compression.type", &self.config.compression_type)
            .set("batch.size", self.config.batch_size.to_string())
            .set("linger.ms", self.config.linger_ms.to_string())
            .set("request.timeout.ms", self.config.request_timeout_ms.to_string())
            .set("delivery.timeout.ms", self.config.delivery_timeout_ms.to_string())
            // Additional reliability settings
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "60000");

        let producer: FutureProducer = client_config.create()
            .map_err(KafkaProducerError::ProducerCreation)?;

        // Test the connection by getting metadata
        match producer.client().fetch_metadata(None, Timeout::After(Duration::from_secs(10))) {
            Ok(metadata) => {
                info!("✅ Kafka producer connected successfully");
                info!("   Brokers: {}", metadata.brokers().len());
                for broker in metadata.brokers() {
                    debug!("   - Broker {}: {}:{}", broker.id(), broker.host(), broker.port());
                }
            }
            Err(e) => {
                let error_msg = Arc::new(format!("Failed to connect to Kafka: {}", e));
                error!("❌ {}", error_msg);
                self.last_error = Some(error_msg.clone());
                return Err(KafkaProducerError::Generic((*error_msg).clone()));
            }
        }

        self.producer = Some(producer);
        self.is_healthy = true;
        self.last_error = None;

        info!("✅ Kafka producer initialized successfully");
        Ok(())
    }

    /// Publish indicator data to Kafka
    async fn publish_indicators(&mut self, indicators: IndicatorOutput) -> Result<(), KafkaProducerError> {
        let producer = match &self.producer {
            Some(producer) => producer,
            None => {
                if self.config.enabled {
                    warn!("Kafka producer not initialized, skipping message publication");
                    self.messages_failed += 1;
                }
                return Ok(());
            }
        };

        // Validate timestamp before publishing
        if indicators.timestamp <= 0 {
            warn!("Skipping Kafka publication for {} - invalid timestamp: {}", 
                  indicators.symbol, indicators.timestamp);
            self.messages_failed += 1;
            return Ok(());
        }

        // PHASE 11 OPTIMIZATION: Ultra-fast custom serialization (eliminates serde overhead)
        let payload_bytes = self.serialization_buffer.serialize_indicator_output(&indicators);
        let payload = std::str::from_utf8(payload_bytes)
            .map_err(|e| KafkaProducerError::SerializationError(format!("UTF-8 error: {}", e)))?
            .to_string(); // Convert to owned String for rdkafka
        
        // VALIDATION: Check for missing fields to prevent silent data loss
        if let Err(missing_fields) = self.serialization_buffer.validate_completeness(&indicators, &payload) {
            let error_msg = format!("Serialization validation failed for {}: missing fields: {:?}", 
                                   indicators.symbol, missing_fields);
            error!("🚨 SERIALIZATION ERROR: {}", error_msg);
            self.messages_failed += 1;
            return Err(KafkaProducerError::SerializationError(error_msg));
        }
        
        // Use symbol as key for partitioning
        let key = &indicators.symbol;
        
        // Use 1-minute candle timestamp (Kafka expects milliseconds)
        let timestamp = indicators.timestamp; // Keep in milliseconds as expected by Kafka

        // Create the record
        let record = FutureRecord::to(&self.data_topic)
            .key(key)
            .payload(&payload)
            .timestamp(timestamp);

        debug!("📤 Publishing to Kafka: topic={}, key={}, timestamp={}, payload_size={}", 
               self.data_topic, key, timestamp, payload.len());

        // Send the message
        match producer.send(record, Timeout::After(Duration::from_millis(self.config.request_timeout_ms as u64))).await {
            Ok((partition, offset)) => {
                self.messages_sent += 1;
                self.last_send_time = Some(chrono::Utc::now().timestamp_millis());
                self.is_healthy = true;
                self.last_error = None;
                
                info!("✅ Published {} indicators to Kafka: partition={}, offset={}, timestamp={}", 
                      key, partition, offset, timestamp);
            }
            Err((kafka_error, _)) => {
                self.messages_failed += 1;
                self.is_healthy = false;
                let error_msg = Arc::new(format!("Failed to send message: {}", kafka_error));
                self.last_error = Some(error_msg.clone());
                
                error!("❌ Failed to publish {} indicators: {}", key, error_msg);
                return Err(KafkaProducerError::Delivery((*error_msg).clone()));
            }
        }

        Ok(())
    }

    /// Get health status
    fn get_health_status(&self) -> KafkaReply {
        KafkaReply::HealthStatus {
            is_healthy: self.is_healthy && self.producer.is_some(),
            is_enabled: self.config.enabled,
            last_error: self.last_error.as_ref().map(|s| (**s).clone()),
        }
    }

    /// Get statistics
    fn get_stats(&self) -> KafkaReply {
        KafkaReply::Stats {
            messages_sent: self.messages_sent,
            messages_failed: self.messages_failed,
            last_send_time: self.last_send_time,
        }
    }
}

impl Actor for KafkaActor {
    type Mailbox = UnboundedMailbox<Self>;

    fn name() -> &'static str {
        "KafkaActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        info!("🚀 Starting Kafka Actor");
        
        if let Err(e) = self.initialize_producer().await {
            let error_msg = Arc::new(format!("Failed to initialize Kafka producer: {}", e));
            error!("{}",error_msg);
            self.last_error = Some(error_msg);
            
            if self.config.enabled {
                warn!("Kafka enabled but initialization failed - continuing without Kafka publishing");
            }
        }
        
        info!("📊 Kafka actor started (enabled: {}, healthy: {})", 
              self.config.enabled, self.is_healthy);
        Ok(())
    }

    async fn on_stop(&mut self, _actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> Result<(), BoxError> {
        info!("🛑 Stopping Kafka Actor: {:?}", reason);
        
        // Flush any pending messages
        if let Some(producer) = &self.producer {
            info!("📤 Flushing pending Kafka messages...");
            match producer.flush(Timeout::After(Duration::from_secs(10))) {
                Ok(_) => info!("✅ Kafka messages flushed successfully"),
                Err(e) => error!("❌ Failed to flush Kafka messages: {}", e),
            }
        }
        
        // Log final statistics
        info!("📊 Final Kafka statistics: sent={}, failed={}", 
              self.messages_sent, self.messages_failed);

        Ok(())
    }
}

impl Message<KafkaTell> for KafkaActor {
    type Reply = ();

    async fn handle(&mut self, msg: KafkaTell, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            KafkaTell::PublishIndicators { indicators } => {
                info!("📨 KafkaActor received PublishIndicators request for symbol: {}", indicators.symbol);
                if let Err(e) = self.publish_indicators(*indicators).await {
                    error!("❌ KafkaActor failed to publish indicators: {}", e);
                } else {
                    info!("✅ KafkaActor successfully published indicators");
                }
            }
            KafkaTell::HealthCheck => {
                // Periodic health check - could implement producer health verification here
                debug!("🔍 Kafka health check: healthy={}, enabled={}", 
                       self.is_healthy, self.config.enabled);
            }
        }
    }
}

impl Message<KafkaAsk> for KafkaActor {
    type Reply = Result<KafkaReply, String>;

    async fn handle(&mut self, msg: KafkaAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            KafkaAsk::GetHealthStatus => Ok(self.get_health_status()),
            KafkaAsk::GetStats => Ok(self.get_stats()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::technical_analysis::structs::{IndicatorOutput, TrendDirection};
    
    fn create_test_indicator_output() -> IndicatorOutput {
        IndicatorOutput {
            symbol: "BTCUSDT".to_string(),
            timestamp: chrono::Utc::now().timestamp_millis(),
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
            volume_profile: None,
        }
    }

    #[test]
    fn test_kafka_actor_creation() {
        let config = KafkaConfig::default();
        let actor = KafkaActor::new(config.clone()).unwrap();
        
        assert_eq!(actor.data_topic, format!("{}{}", config.topic_prefix, KafkaActor::DATA_TOPIC_SUFFIX));
        assert!(!actor.is_healthy);
        assert_eq!(actor.messages_sent, 0);
        assert_eq!(actor.messages_failed, 0);
        assert!(actor.producer.is_none());
        assert!(actor.last_error.is_none());
    }

    #[test]
    fn test_kafka_config_default() {
        let config = KafkaConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bootstrap_servers, vec!["localhost:9092"]);
        assert_eq!(config.topic_prefix, "ta_");
        assert_eq!(config.client_id, "data_feeder_rust");
        assert_eq!(config.acks, "all");
        assert_eq!(config.retries, 3);
        assert_eq!(config.compression_type, "snappy");
        assert_eq!(config.batch_size, 16384);
        assert_eq!(config.linger_ms, 10);
    }

    #[test]
    fn test_kafka_config_custom() {
        let config = KafkaConfig {
            enabled: true,
            bootstrap_servers: vec!["test-broker:9092".to_string(), "test-broker2:9092".to_string()],
            topic_prefix: "custom_".to_string(),
            client_id: "test_client".to_string(),
            acks: "1".to_string(),
            retries: 5,
            compression_type: "gzip".to_string(),
            batch_size: 32768,
            linger_ms: 20,
            ..KafkaConfig::default()
        };
        
        assert!(config.enabled);
        assert_eq!(config.bootstrap_servers.len(), 2);
        assert_eq!(config.topic_prefix, "custom_");
        assert_eq!(config.acks, "1");
        assert_eq!(config.retries, 5);
    }

    #[test]
    fn test_data_topic_construction() {
        let test_cases = vec![
            ("ta_", "ta_data"),
            ("prod_", "prod_data"),
            ("test_ta_", "test_ta_data"),
            ("", "data"),
        ];
        
        for (prefix, expected) in test_cases {
            let config = KafkaConfig {
                topic_prefix: prefix.to_string(),
                ..KafkaConfig::default()
            };
            let actor = KafkaActor::new(config).unwrap();
            assert_eq!(actor.data_topic, expected, "Failed for prefix: {}", prefix);
        }
    }

    #[test]
    fn test_health_status_disabled() {
        let config = KafkaConfig {
            enabled: false,
            ..KafkaConfig::default()
        };
        let actor = KafkaActor::new(config).unwrap();
        
        let health_status = actor.get_health_status();
        match health_status {
            KafkaReply::HealthStatus { is_healthy, is_enabled, last_error } => {
                assert!(!is_enabled);
                assert!(!is_healthy); // Not healthy because producer is None
                assert!(last_error.is_none());
            }
            _ => panic!("Expected HealthStatus reply"),
        }
    }

    #[test]
    fn test_health_status_enabled_no_producer() {
        let config = KafkaConfig {
            enabled: true,
            ..KafkaConfig::default()
        };
        let actor = KafkaActor::new(config).unwrap();
        
        let health_status = actor.get_health_status();
        match health_status {
            KafkaReply::HealthStatus { is_healthy, is_enabled, last_error } => {
                assert!(is_enabled);
                assert!(!is_healthy); // Not healthy because producer is None
                assert!(last_error.is_none());
            }
            _ => panic!("Expected HealthStatus reply"),
        }
    }

    #[test]
    fn test_stats_initial() {
        let config = KafkaConfig::default();
        let actor = KafkaActor::new(config).unwrap();
        
        let stats = actor.get_stats();
        match stats {
            KafkaReply::Stats { messages_sent, messages_failed, last_send_time } => {
                assert_eq!(messages_sent, 0);
                assert_eq!(messages_failed, 0);
                assert!(last_send_time.is_none());
            }
            _ => panic!("Expected Stats reply"),
        }
    }
    
    #[test]
    fn test_indicator_output_serialization() {
        let indicators = create_test_indicator_output();
        
        // Test that the indicator output can be serialized to JSON
        let json_result = serde_json::to_string(&indicators);
        assert!(json_result.is_ok(), "Should be able to serialize IndicatorOutput");
        
        let json_str = json_result.unwrap();
        assert!(!json_str.is_empty());
        assert!(json_str.contains("BTCUSDT"));
        assert!(json_str.contains("close_5m"));
        assert!(json_str.contains("trend_1min"));
        
        // Test deserialization
        let deserialized: IndicatorOutput = serde_json::from_str(&json_str)
            .expect("Should be able to deserialize");
        assert_eq!(deserialized.symbol, indicators.symbol);
        assert_eq!(deserialized.timestamp, indicators.timestamp);
    }

    #[test]
    fn test_message_types() {
        let indicators = create_test_indicator_output();
        
        // Test KafkaTell message creation
        let tell_msg = KafkaTell::PublishIndicators {
            indicators: Box::new(indicators.clone()),
        };
        
        // Verify message structure
        match tell_msg {
            KafkaTell::PublishIndicators { indicators: msg_indicators } => {
                assert_eq!(msg_indicators.symbol, "BTCUSDT");
            }
            _ => panic!("Unexpected message type"),
        }
        
        // Test health check message
        let health_msg = KafkaTell::HealthCheck;
        match health_msg {
            KafkaTell::HealthCheck => {}, // Expected
            _ => panic!("Unexpected health check message type"),
        }
    }

    #[test]
    fn test_ask_message_types() {
        // Test KafkaAsk message types
        let health_ask = KafkaAsk::GetHealthStatus;
        let stats_ask = KafkaAsk::GetStats;
        
        // Verify they can be constructed and are the right types
        match health_ask {
            KafkaAsk::GetHealthStatus => {},
            _ => panic!("Unexpected health ask type"),
        }
        
        match stats_ask {
            KafkaAsk::GetStats => {},
            _ => panic!("Unexpected stats ask type"),
        }
    }

    #[test]
    fn test_kafka_config_validation() {
        // Test that invalid configurations can be detected
        let config = KafkaConfig {
            enabled: true,
            bootstrap_servers: vec![], // Empty servers - should be invalid
            ..KafkaConfig::default()
        };
        
        // Creating the actor should still succeed, but connection will fail
        let actor_result = KafkaActor::new(config);
        assert!(actor_result.is_ok(), "Actor creation should succeed even with invalid config");
    }

    #[test]
    fn test_topic_suffix_constant() {
        assert_eq!(KafkaActor::DATA_TOPIC_SUFFIX, "data");
    }

    #[test]
    fn test_multiple_bootstrap_servers() {
        let config = KafkaConfig {
            bootstrap_servers: vec![
                "broker1:9092".to_string(),
                "broker2:9092".to_string(),
                "broker3:9092".to_string(),
            ],
            ..KafkaConfig::default()
        };
        
        let actor = KafkaActor::new(config.clone()).unwrap();
        assert_eq!(actor.config.bootstrap_servers.len(), 3);
        assert_eq!(actor.config.bootstrap_servers, config.bootstrap_servers);
    }
}