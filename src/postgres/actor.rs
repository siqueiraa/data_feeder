
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
        source: String, // Track which actor sent this candle
    },
    /// Store multiple candles in batch
    StoreBatch {
        candles: Vec<(String, FuturesOHLCVCandle)>,
        source: String, // Track which actor sent this batch
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
    last_activity_time: std::time::Instant,
}

impl PostgresActor {

    /// SQL statement for upserting candle data (kept for potential future real-time use)
    #[allow(dead_code)]
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
            last_activity_time: std::time::Instant::now(),
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


    /// Store multiple candles using ultra-high performance multi-row INSERT
    async fn store_batch(&mut self, candles: Vec<(String, FuturesOHLCVCandle)>, source: &str) -> Result<(), PostgresError> {
        info!("🚀 [PostgresActor] Starting ULTRA-HIGH PERFORMANCE store_batch from {}: {} candles", source, candles.len());
        
        if candles.is_empty() {
            info!("📭 [PostgresActor] Empty candles batch, skipping");
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

        // Optimized batch size - balance performance vs PostgreSQL message limits
        // PostgreSQL message size limit ~1MB, so use smaller batches to stay within limits
        const ULTRA_BATCH_SIZE: usize = 1000;
        let total_candles = candles.len();
        let num_batches = total_candles.div_ceil(ULTRA_BATCH_SIZE);
        
        info!("⚡ [PostgresActor] Using MULTI-ROW UPSERT: {} candles in {} ultra-batches of ~{} candles each", 
              total_candles, num_batches, ULTRA_BATCH_SIZE);
        
        let mut total_rows_affected = 0;
        let batch_start_time = std::time::Instant::now();
        
        // Process candles in ultra-high performance batches
        for (batch_idx, batch_candles) in candles.chunks(ULTRA_BATCH_SIZE).enumerate() {
            let batch_num = batch_idx + 1;
            let batch_size = batch_candles.len();
            let batch_start = std::time::Instant::now();
            
            info!("🚀 [PostgresActor] Processing ultra-batch {}/{}: {} candles with MULTI-ROW UPSERT", 
                  batch_num, num_batches, batch_size);
            
            // Get fresh connection for each batch to avoid connection timeouts
            let mut client = match pool.get().await {
                Ok(client) => client,
                Err(e) => {
                    error!("❌ [PostgresActor] Failed to acquire database client for ultra-batch {}/{}: {}", batch_num, num_batches, e);
                    return Err(e.into());
                }
            };
            
            // Start transaction for this batch
            let transaction = match client.transaction().await {
                Ok(transaction) => transaction,
                Err(e) => {
                    error!("❌ [PostgresActor] Failed to start transaction for ultra-batch {}/{}: {}", batch_num, num_batches, e);
                    return Err(e.into());
                }
            };

            // Execute ultra-high performance multi-row UPSERT
            let batch_rows_affected = match self.execute_multi_row_upsert(&transaction, batch_candles, batch_num, num_batches).await {
                Ok(rows) => rows,
                Err(e) => {
                    error!("❌ [PostgresActor] Multi-row UPSERT failed for ultra-batch {}/{}: {}", batch_num, num_batches, e);
                    return Err(e);
                }
            };

            // Commit this batch
            match transaction.commit().await {
                Ok(()) => {
                    let batch_elapsed = batch_start.elapsed();
                    let candles_per_sec = batch_size as f64 / batch_elapsed.as_secs_f64();
                    info!("✅ [PostgresActor] Committed ultra-batch {}/{} with {} rows in {:?} ({:.0} candles/sec)", 
                          batch_num, num_batches, batch_rows_affected, batch_elapsed, candles_per_sec);
                }
                Err(e) => {
                    error!("❌ [PostgresActor] Failed to commit ultra-batch {}/{}: {}", batch_num, num_batches, e);
                    return Err(e.into());
                }
            }
            
            total_rows_affected += batch_rows_affected;
        }
        
        let total_elapsed = batch_start_time.elapsed();
        let total_candles_per_sec = total_candles as f64 / total_elapsed.as_secs_f64();
        
        info!("🎯 [{}] ULTRA-HIGH PERFORMANCE COMPLETED: {} candles in {} ultra-batches, {} total rows", 
              source, total_candles, num_batches, total_rows_affected);
        info!("⚡ [{}] PERFORMANCE: {:?} total time, {:.0} candles/sec throughput (TARGET: 20,000+)", 
              source, total_elapsed, total_candles_per_sec);
        
        // Log warning if rows_affected doesn't match expected count
        if total_rows_affected as usize != total_candles {
            warn!("⚠️  Expected {} rows but affected {} rows - potential duplicates", 
                  total_candles, total_rows_affected);
        }
        
        Ok(())
    }

    /// Execute ultra-high performance multi-row UPSERT with dynamic SQL generation
    async fn execute_multi_row_upsert(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        batch_candles: &[(String, FuturesOHLCVCandle)],
        batch_num: usize,
        total_batches: usize,
    ) -> Result<u64, PostgresError> {
        let batch_size = batch_candles.len();
        
        info!("🏗️  [PostgresActor] Building multi-row UPSERT SQL for {} candles in batch {}/{}", 
              batch_size, batch_num, total_batches);
        
        // Build dynamic multi-row UPSERT statement to handle duplicate data
        let mut sql = String::with_capacity(batch_size * 120); // Pre-allocate for performance (~120 chars per row with UPSERT)
        sql.push_str("INSERT INTO candles_1m (time, open, high, low, close, volume, symbol, time_frame, trade_count, taker_buy_volume) VALUES ");
        
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::with_capacity(batch_size * 10);
        let mut timestamps: Vec<chrono::DateTime<chrono::FixedOffset>> = Vec::with_capacity(batch_size);
        let mut trade_counts: Vec<i32> = Vec::with_capacity(batch_size);
        
        // Pre-process all data for performance
        for (i, (_symbol, candle)) in batch_candles.iter().enumerate() {
            let timestamp = self.millis_to_timestamp(candle.open_time)
                .map_err(|e| {
                    error!("❌ [PostgresActor] Failed to convert timestamp for candle {} in batch {}: {}", i + 1, batch_num, e);
                    e
                })?;
            timestamps.push(timestamp);
            trade_counts.push(candle.number_of_trades as i32);
        }
        
        // Build VALUES clause with parameter placeholders
        for i in 0..batch_size {
            if i > 0 {
                sql.push_str(", ");
            }
            
            let base_param = i * 10 + 1;
            sql.push_str(&format!("(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})", 
                base_param, base_param + 1, base_param + 2, base_param + 3, base_param + 4,
                base_param + 5, base_param + 6, base_param + 7, base_param + 8, base_param + 9));
        }
        
        // Flatten all parameters in correct order
        for (i, (symbol, candle)) in batch_candles.iter().enumerate() {
            params.push(&timestamps[i]);           // time
            params.push(&candle.open);             // open
            params.push(&candle.high);             // high
            params.push(&candle.low);              // low
            params.push(&candle.close);            // close
            params.push(&candle.volume);           // volume
            params.push(symbol);                   // symbol
            params.push(&"1m");                    // time_frame
            params.push(&trade_counts[i]);         // trade_count
            params.push(&candle.taker_buy_base_asset_volume); // taker_buy_volume
        }
        
        // Add UPSERT clause to handle duplicates efficiently
        sql.push_str(" ON CONFLICT (time, symbol, time_frame) DO UPDATE SET ");
        sql.push_str("open = EXCLUDED.open, ");
        sql.push_str("high = EXCLUDED.high, ");
        sql.push_str("low = EXCLUDED.low, ");
        sql.push_str("close = EXCLUDED.close, ");
        sql.push_str("volume = EXCLUDED.volume, ");
        sql.push_str("trade_count = EXCLUDED.trade_count, ");
        sql.push_str("taker_buy_volume = EXCLUDED.taker_buy_volume");
        
        info!("📝 [PostgresActor] Generated UPSERT SQL with {} parameters for {} candles in batch {}/{}", 
              params.len(), batch_size, batch_num, total_batches);
        
        // Execute the ultra-high performance multi-row INSERT
        let sql_start = std::time::Instant::now();
        let rows_affected = match transaction.execute(&sql, &params).await {
            Ok(rows) => {
                let sql_elapsed = sql_start.elapsed();
                info!("⚡ [PostgresActor] Multi-row UPSERT completed: {} candles in {:?} ({:.0} candles/sec)", 
                      batch_size, sql_elapsed, batch_size as f64 / sql_elapsed.as_secs_f64());
                rows
            }
            Err(e) => {
                error!("❌ [PostgresActor] Multi-row UPSERT failed for batch {}/{}: {}", batch_num, total_batches, e);
                error!("🔍 [PostgresActor] SQL length: {} chars, Parameters: {}", sql.len(), params.len());
                if batch_size > 0 {
                    let first_candle = &batch_candles[0];
                    error!("🔍 [PostgresActor] First candle: {} at {}", first_candle.0, first_candle.1.open_time);
                }
                return Err(e.into());
            }
        };
        
        Ok(rows_affected)
    }

    /// Add candle to batch and flush if needed
    async fn add_to_batch(&mut self, symbol: String, candle: FuturesOHLCVCandle, source: &str) -> Result<(), PostgresError> {
        // Log individual candle addition
        let timestamp = chrono::DateTime::from_timestamp_millis(candle.open_time)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| format!("INVALID_TIME({})", candle.open_time));
        info!("➕ [{}] Adding candle to batch: {} - {} (close: {})", source, symbol, timestamp, candle.close);
        
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
            self.flush_batch(source).await?;
        }

        Ok(())
    }

    /// Flush the current batch to database
    async fn flush_batch(&mut self, source: &str) -> Result<(), PostgresError> {
        if self.candle_batch.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(&mut self.candle_batch);
        self.last_batch_time = None;

        match self.store_batch(batch, source).await {
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
        if let Err(e) = self.flush_batch("Shutdown").await {
            error!("Failed to flush final batch: {}", e);
        }
        
        Ok(())
    }
}

impl Message<PostgresTell> for PostgresActor {
    type Reply = ();

    async fn handle(&mut self, msg: PostgresTell, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        // Update activity time for all messages
        self.last_activity_time = std::time::Instant::now();
        
        match msg {
            PostgresTell::StoreCandle { symbol, candle, source } => {
                info!("📥 [PostgresActor] Received StoreCandle message from {}: {} candle", source, symbol);
                if let Err(e) = self.add_to_batch(symbol, candle, &source).await {
                    error!("Failed to add candle to batch: {}", e);
                }
            }
            PostgresTell::StoreBatch { candles, source } => {
                info!("📦 [PostgresActor] Received StoreBatch message from {}: {} candles", source, candles.len());
                let batch_start = std::time::Instant::now();
                match self.store_batch(candles.clone(), &source).await {
                    Ok(()) => {
                        let batch_elapsed = batch_start.elapsed();
                        info!("✅ [PostgresActor] Successfully processed StoreBatch from {} in {:?}", source, batch_elapsed);
                    }
                    Err(e) => {
                        let batch_elapsed = batch_start.elapsed();
                        error!("❌ [PostgresActor] Failed to store batch from {} after {:?}: {}", source, batch_elapsed, e);
                        error!("🔍 [PostgresActor] Batch details - {} candles from {}", candles.len(), source);
                        if !candles.is_empty() {
                            let first_candle = &candles[0];
                            let last_candle = &candles[candles.len() - 1];
                            error!("🔍 [PostgresActor] Batch range - first: {} at {}, last: {} at {}", 
                                   first_candle.0, first_candle.1.open_time, 
                                   last_candle.0, last_candle.1.open_time);
                        }
                        error!("🔍 [PostgresActor] Check PostgreSQL connection, table schema, and data validity");
                    }
                }
            }
            PostgresTell::HealthCheck => {
                let elapsed_since_activity = self.last_activity_time.elapsed();
                info!("🏥 [PostgresActor] Received HealthCheck message - alive and responsive (last activity: {:?} ago)", elapsed_since_activity);
                
                // Log health status
                let health_status = self.get_health_status();
                if let PostgresReply::HealthStatus { is_healthy, connection_count, last_error } = health_status {
                    info!("💊 [PostgresActor] Health Status: healthy={}, connections={}, last_error={:?}", 
                          is_healthy, connection_count, last_error);
                }
                
                // Periodic health check - flush batch if needed
                if let Err(e) = self.flush_batch("HealthCheck").await {
                    error!("Health check batch flush failed: {}", e);
                } else {
                    info!("✅ [PostgresActor] HealthCheck completed successfully");
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