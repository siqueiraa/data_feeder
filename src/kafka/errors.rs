use thiserror::Error;
use rdkafka::error::KafkaError;

/// Custom error types for Kafka operations
#[derive(Error, Debug)]
pub enum KafkaProducerError {
    /// Kafka client configuration error
    #[error("Kafka configuration error: {0}")]
    Config(String),
    
    /// Producer creation error
    #[error("Failed to create Kafka producer: {0}")]
    ProducerCreation(#[from] KafkaError),
    
    /// Message serialization error (serde_json)
    #[error("Failed to serialize message: {0}")]
    Serialization(#[from] serde_json::Error),
    
    /// Custom serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    /// Message delivery error
    #[error("Failed to deliver message: {0}")]
    Delivery(String),
    
    /// Connection timeout error
    #[error("Kafka connection timeout: {0}")]
    Timeout(String),
    
    /// Generic Kafka error
    #[error("Kafka error: {0}")]
    Generic(String),
}