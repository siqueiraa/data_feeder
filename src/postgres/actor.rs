
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod};
use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use tracing::{error, info, warn};

use crate::historical::structs::FuturesOHLCVCandle;
use super::errors::PostgresError;

/// PostgreSQL configuration
#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub max_connections: usize,
    pub connection_timeout_seconds: u64,
    pub batch_size: usize,
    pub batch_timeout_seconds: u64,
    pub enabled: bool,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: "crypto_data".to_string(),
            username: "postgres".to_string(),
            password: "password".to_string(),
            max_connections: 10,
            connection_timeout_seconds: 30,
            batch_size: 1000,
            batch_timeout_seconds: 5,
            enabled: false,
        }
    }
}

/// PostgreSQL actor messages for telling (fire-and-forget)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PostgresTell {
    /// Store a single candle
    StoreCandle {
        symbol: String,
        candle: FuturesOHLCVCandle,
    },
    /// Store multiple candles in batch
    StoreBatch {
        candles: Vec<(String, FuturesOHLCVCandle)>,
    },
    /// Health check message
    HealthCheck,
}

/// PostgreSQL actor messages for asking (request-response)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PostgresAsk {
    /// Get recent candles for a symbol
    GetRecentCandles {
        symbol: String,
        limit: usize,
    },
    /// Get connection health status
    GetHealthStatus,
}

/// PostgreSQL actor responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PostgresReply {
    /// Recent candles response
    RecentCandles(Vec<FuturesOHLCVCandle>),
    /// Health status response
    HealthStatus {
        is_healthy: bool,
        connection_count: usize,
        last_error: Option<String>,
    },
    /// Success confirmation
    Success,
    /// Error response
    Error(String),
}

/// PostgreSQL actor for storing candle data
pub struct PostgresActor {
    pool: Option<Pool>,
    config: PostgresConfig,
    last_error: Option<String>,
    candle_batch: Vec<(String, FuturesOHLCVCandle)>,
    last_batch_time: Option<std::time::Instant>,
}

impl PostgresActor {
    /// SQL statement for upserting candle data
    const UPSERT_CANDLE_SQL: &'static str = r#"
        INSERT INTO candles_1m (time, open, high, low, close, volume, symbol, time_frame, trade_count, taker_buy_volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (time, symbol, time_frame) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            trade_count = EXCLUDED.trade_count,
            taker_buy_volume = EXCLUDED.taker_buy_volume
    "#;

    /// Create a new PostgresActor
    pub fn new(config: PostgresConfig) -> Result<Self, PostgresError> {
        Ok(Self {
            pool: None,
            config,
            last_error: None,
            candle_batch: Vec::new(),
            last_batch_time: None,
        })
    }

    /// Initialize database connection pool
    async fn initialize_pool(&mut self) -> Result<(), PostgresError> {
        if !self.config.enabled {
            info!("PostgreSQL storage is disabled in configuration");
            return Ok(());
        }

        info!("Initializing PostgreSQL connection pool...");

        // Log connection parameters for debugging
        info!("PostgreSQL connection parameters:");
        info!("  host: {}", self.config.host);
        info!("  port: {}", self.config.port);
        info!("  database: {}", self.config.database);
        info!("  username: {}", self.config.username);
        info!("  password: [HIDDEN]");

        // Manual URL encoding for special characters (like + in password)
        let url_encoded_password = self.config.password.replace("+", "%2B");

        info!("🔄 Testing deadpool with individual parameters (URI not supported)");

        // Deadpool doesn't support URI parsing directly, use individual parameters
        let mut cfg = Config::new();
        cfg.host = Some(self.config.host.clone());
        cfg.port = Some(self.config.port);
        cfg.dbname = Some(self.config.database.clone());
        cfg.user = Some(self.config.username.clone());
        cfg.password = Some(self.config.password.clone());
        cfg.ssl_mode = Some(deadpool_postgres::SslMode::Disable);

        // Debug: PostgreSQL libraries use different source IP than TCP connections
        // Need to understand why and how to control the source IP selection
        info!("🔧 Investigating why PostgreSQL libraries use different source IP than TCP connections");

        // Try direct connection test to compare with deadpool behavior
        info!("🧪 Testing direct PostgreSQL connection...");

        // Use direct URI connection like Python asyncpg, not Config object
        let postgresql_uri = format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode=disable&application_name=data_feeder_rust",
            self.config.username,
            url_encoded_password,
            self.config.host,
            self.config.port,
            self.config.database
        );

        info!("🔄 Testing direct URI connection (like Python asyncpg)");
        info!("    postgresql://{}:{}@{}:{}/{}?sslmode=disable",
              self.config.username, "[ENCODED_PASSWORD]",
              self.config.host, self.config.port, self.config.database);

        // Use tokio_postgres::connect() directly with URI string (not Config object)
        match tokio_postgres::connect(&postgresql_uri, tokio_postgres::NoTls).await {
            Ok((client, connection)) => {
                info!("✅ Direct tokio-postgres connection successful!");
                // Clean up
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                // Test query
                match client.query_one("SELECT 1 as test", &[]).await {
                    Ok(_) => info!("✅ Direct connection query successful!"),
                    Err(e) => error!("❌ Direct connection query failed: {}", e),
                }
            }
            Err(e) => {
                error!("❌ Direct tokio-postgres connection failed: {}", e);
            }
        }

        info!("Using individual parameter approach (matching working config format)");

        // Add detailed debugging for the connection process
        info!("Creating deadpool configuration with SSL mode: {:?}", deadpool_postgres::SslMode::Disable);

        // Debug network routing - see what local IP will be used
        info!("Checking local network configuration for PostgreSQL connection...");

        // Try to determine what local IP will be used for this connection
        let target_addr = format!("{}:{}", self.config.host, self.config.port);
        info!("Target PostgreSQL server: {}", target_addr);

        // Test connection to see actual source IP being used
        match std::net::TcpStream::connect(&target_addr) {
            Ok(stream) => {
                if let Ok(local_addr) = stream.local_addr() {
                    info!("🔍 Local IP that will be used for PostgreSQL connection: {:?}", local_addr.ip());
                    info!("🔍 Local port: {:?}", local_addr.port());
                } else {
                    warn!("Could not determine local address for connection");
                }
                if let Ok(peer_addr) = stream.peer_addr() {
                    info!("🔍 Connected to PostgreSQL server at: {:?}", peer_addr);
                }
            }
            Err(e) => {
                warn!("Could not establish test connection to determine source IP: {}", e);
            }
        }

        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: self.config.max_connections,
            timeouts: deadpool_postgres::Timeouts::default(),
            // Note: Using default timeouts to avoid runtime context issues
            // The connection_timeout_seconds config is kept for potential future use
        });

        let pool = cfg.create_pool(None, NoTls)
            .map_err(|e| PostgresError::Config(format!("Failed to create connection pool: {}", e)))?;

        // Test the connection
        let client = pool.get().await?;
        let row = client.query_one("SELECT 1 as test", &[]).await?;
        let test: i32 = row.get("test");

        if test == 1 {
            info!("PostgreSQL connection test successful");
            self.pool = Some(pool);
            self.last_error = None;
        } else {
            return Err(PostgresError::Config("Connection test failed".to_string()));
        }

        Ok(())
    }

    /// Convert milliseconds timestamp to PostgreSQL timestamp with time zone
    fn millis_to_timestamp(&self, millis: i64) -> Result<chrono::DateTime<chrono::FixedOffset>, PostgresError> {
        chrono::DateTime::from_timestamp_millis(millis)
            .ok_or(PostgresError::InvalidTimestamp(millis))
            .map(|dt| dt.with_timezone(&chrono::FixedOffset::east_opt(0).unwrap()))
    }


    /// Store multiple candles in batch
    async fn store_batch(&mut self, candles: Vec<(String, FuturesOHLCVCandle)>) -> Result<(), PostgresError> {
        if candles.is_empty() {
            return Ok(());
        }

        let pool = match &self.pool {
            Some(pool) => pool,
            None => {
                if self.config.enabled {
                    warn!("PostgreSQL pool not initialized, skipping batch storage");
                }
                return Ok(());
            }
        };

        let mut client = pool.get().await?;
        let transaction = client.transaction().await?;

        let stmt = transaction.prepare(Self::UPSERT_CANDLE_SQL).await?;

        let mut rows_affected = 0;
        let candles_len = candles.len();
        for (symbol, candle) in candles {
            let timestamp = self.millis_to_timestamp(candle.close_time)?;
            let result = transaction.execute(
                &stmt,
                &[
                    &timestamp,
                    &candle.open,
                    &candle.high,
                    &candle.low,
                    &candle.close,
                    &candle.volume,
                    &symbol,
                    &"1m",
                    &(candle.number_of_trades as i32),
                    &candle.taker_buy_base_asset_volume,
                ],
            ).await?;
            rows_affected += result;
        }

        transaction.commit().await?;
        info!("Batch stored {} candles (rows affected: {})", candles_len, rows_affected);
        Ok(())
    }

    /// Add candle to batch and flush if needed
    async fn add_to_batch(&mut self, symbol: String, candle: FuturesOHLCVCandle) -> Result<(), PostgresError> {
        self.candle_batch.push((symbol, candle));
        
        if self.last_batch_time.is_none() {
            self.last_batch_time = Some(std::time::Instant::now());
        }

        // Check if we should flush the batch
        let should_flush = self.candle_batch.len() >= self.config.batch_size
            || self.last_batch_time
                .map(|t| t.elapsed().as_secs() >= self.config.batch_timeout_seconds)
                .unwrap_or(false);

        if should_flush {
            self.flush_batch().await?;
        }

        Ok(())
    }

    /// Flush the current batch to database
    async fn flush_batch(&mut self) -> Result<(), PostgresError> {
        if self.candle_batch.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(&mut self.candle_batch);
        self.last_batch_time = None;

        match self.store_batch(batch).await {
            Ok(()) => {
                self.last_error = None;
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Batch storage failed: {}", e);
                error!("{}", error_msg);
                self.last_error = Some(error_msg);
                Err(e)
            }
        }
    }

    /// Get connection health status
    fn get_health_status(&self) -> PostgresReply {
        let connection_count = self.pool
            .as_ref()
            .map(|p| p.status().size)
            .unwrap_or(0);

        PostgresReply::HealthStatus {
            is_healthy: self.pool.is_some() && self.last_error.is_none(),
            connection_count,
            last_error: self.last_error.clone(),
        }
    }
}

impl Actor for PostgresActor {
    type Mailbox = UnboundedMailbox<Self>;

    fn name() -> &'static str {
        "PostgresActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        info!("PostgresActor starting...");
        
        if let Err(e) = self.initialize_pool().await {
            let error_msg = format!("Failed to initialize PostgreSQL pool: {}", e);
            error!("{}", error_msg);
            self.last_error = Some(error_msg);
            
            if self.config.enabled {
                warn!("PostgreSQL enabled but initialization failed - continuing without PostgreSQL storage");
            }
        }
        
        info!("PostgresActor started (enabled: {})", self.config.enabled);
        Ok(())
    }

    async fn on_stop(&mut self, _actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> Result<(), BoxError> {
        info!("PostgresActor stopping: {:?}", reason);
        
        // Flush any remaining batch
        if let Err(e) = self.flush_batch().await {
            error!("Failed to flush final batch: {}", e);
        }
        
        Ok(())
    }
}

impl Message<PostgresTell> for PostgresActor {
    type Reply = ();

    async fn handle(&mut self, msg: PostgresTell, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            PostgresTell::StoreCandle { symbol, candle } => {
                if let Err(e) = self.add_to_batch(symbol, candle).await {
                    error!("Failed to add candle to batch: {}", e);
                }
            }
            PostgresTell::StoreBatch { candles } => {
                if let Err(e) = self.store_batch(candles).await {
                    error!("Failed to store batch: {}", e);
                }
            }
            PostgresTell::HealthCheck => {
                // Periodic health check - flush batch if needed
                if let Err(e) = self.flush_batch().await {
                    error!("Health check batch flush failed: {}", e);
                }
            }
        }
    }
}

impl Message<PostgresAsk> for PostgresActor {
    type Reply = Result<PostgresReply, PostgresError>;

    async fn handle(&mut self, msg: PostgresAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            PostgresAsk::GetRecentCandles { symbol: _symbol, limit: _limit } => {
                // TODO: Implement querying recent candles from database
                // For now, return empty list
                Ok(PostgresReply::RecentCandles(Vec::new()))
            }
            PostgresAsk::GetHealthStatus => {
                Ok(self.get_health_status())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_config_default() {
        let config = PostgresConfig::default();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.database, "crypto_data");
        assert!(!config.enabled); // Should be disabled by default
    }

    #[test]
    fn test_millis_to_timestamp() {
        let actor = PostgresActor::new(PostgresConfig::default()).unwrap();
        
        // Test valid timestamp
        let millis = 1640995200000; // 2022-01-01 00:00:00 UTC
        let timestamp = actor.millis_to_timestamp(millis).unwrap();
        assert_eq!(timestamp.timestamp_millis(), millis);
        
        // Test invalid timestamp (extremely large value that would overflow)
        let invalid_millis = i64::MAX;
        assert!(actor.millis_to_timestamp(invalid_millis).is_err());
    }
}