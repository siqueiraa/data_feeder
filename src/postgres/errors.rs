use thiserror::Error;

#[derive(Debug, Error)]
pub enum PostgresError {
    #[error("Database connection error: {0}")]
    Connection(#[from] tokio_postgres::Error),
    
    #[error("Connection pool error: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    
    #[error("Database configuration error: {0}")]
    Config(String),
    
    #[error("Data conversion error: {0}")]
    Conversion(String),
    
    #[error("Database operation timeout")]
    Timeout,
    
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
}