use std::time::Duration;

use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::request::MessageSend;
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::api::binance::BinanceKlinesClient;
use crate::api::types::{ApiConfig, ApiError, ApiRequest, ApiStats};
use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};
use crate::lmdb::{LmdbActor, LmdbActorMessage, LmdbActorResponse};
use crate::postgres::{PostgresActor, PostgresTell};

/// API actor messages for telling (fire-and-forget)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApiTell {
    /// Fill gap for a specific symbol and timeframe
    FillGap {
        symbol: String,
        interval: String,
        start_time: TimestampMS,
        end_time: TimestampMS,
    },
    /// Fetch recent data to bridge to real-time
    FetchRecent {
        symbol: String,
        interval: String,
        limit: u32,
    },
}

/// API actor messages for asking (request-response)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApiAsk {
    /// Get API statistics
    GetStats,
    /// Get available data range for symbol/interval
    GetDataRange {
        symbol: String,
        interval: String,
    },
    /// Fetch klines with custom parameters
    FetchKlines {
        symbol: String,
        interval: String,
        start_time: Option<TimestampMS>,
        end_time: Option<TimestampMS>,
        limit: Option<u32>,
    },
}

/// API actor replies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApiReply {
    /// Statistics response
    Stats(ApiStats),
    /// Data range response
    DataRange {
        symbol: String,
        interval: String,
        earliest: Option<TimestampMS>,
        latest: Option<TimestampMS>,
        count: u64,
    },
    /// Klines fetch response
    Klines(Vec<FuturesOHLCVCandle>),
    /// Success response
    Success,
    /// Error response
    Error(String),
}

/// API actor for fetching data from REST endpoints
pub struct ApiActor {
    /// Binance klines client
    klines_client: BinanceKlinesClient,
    /// API statistics
    stats: ApiStats,
    /// Configuration
    config: ApiConfig,
    /// LmdbActor reference for storage operations
    lmdb_actor: ActorRef<LmdbActor>,
    /// PostgreSQL actor reference for dual storage
    postgres_actor: Option<ActorRef<PostgresActor>>,
}

impl ApiActor {
    /// Create a new API actor
    pub fn new(lmdb_actor: ActorRef<LmdbActor>) -> Result<Self, ApiError> {
        Self::new_with_config(lmdb_actor, ApiConfig::default())
    }

    /// Create a new API actor with custom configuration
    pub fn new_with_config(lmdb_actor: ActorRef<LmdbActor>, config: ApiConfig) -> Result<Self, ApiError> {
        let klines_client = BinanceKlinesClient::new(config.base_url.clone())?;

        Ok(Self {
            klines_client,
            stats: ApiStats::new(),
            config,
            lmdb_actor,
            postgres_actor: None,
        })
    }

    /// Set the PostgreSQL actor reference for dual storage
    pub fn set_postgres_actor(&mut self, postgres_actor: ActorRef<PostgresActor>) {
        self.postgres_actor = Some(postgres_actor);
    }

    /// Initialize LMDB database for a symbol-interval combination through LmdbActor
    async fn init_database(&self, symbol: &str, interval: &str) -> Result<(), ApiError> {
        let timeframe_seconds = self.interval_to_seconds(interval);
        
        let msg = LmdbActorMessage::InitializeDatabase {
            symbol: symbol.to_string(),
            timeframe: timeframe_seconds,
        };
        
        match self.lmdb_actor.ask(msg).send().await {
            Ok(LmdbActorResponse::Success) => {
                info!("✅ Initialized LMDB database for: {}_{}", symbol, timeframe_seconds);
                Ok(())
            }
            Ok(LmdbActorResponse::ErrorResponse(err)) => {
                Err(ApiError::Unknown(format!("Failed to initialize database: {}", err)))
            }
            Ok(_) => {
                Err(ApiError::Unknown("Unexpected response from LmdbActor".to_string()))
            }
            Err(e) => {
                Err(ApiError::Unknown(format!("Failed to communicate with LmdbActor: {}", e)))
            }
        }
    }

    /// Store candles through LmdbActor
    async fn store_candles(&self, symbol: &str, interval: &str, candles: Vec<FuturesOHLCVCandle>) -> Result<(), ApiError> {
        info!("🎯 [ApiActor] store_candles called for {}: {} candles", symbol, candles.len());
        if candles.is_empty() {
            return Ok(());
        }

        let timeframe_seconds = self.interval_to_seconds(interval);
        
        // Initialize database if needed
        self.init_database(symbol, interval).await?;
        
        // Prepare PostgreSQL data before LMDB operations
        let postgres_data = if self.postgres_actor.is_some() {
            let candles_len = candles.len();
            let candles_vec: Vec<(String, FuturesOHLCVCandle)> = candles
                .iter()
                .map(|candle| (symbol.to_string(), candle.clone()))
                .collect();
            Some((candles_len, candles_vec))
        } else {
            None
        };

        // Get range info for logging and PostgreSQL
        let start_time = candles.first().unwrap().open_time;
        let end_time = candles.last().unwrap().close_time;

        // Store candles through LmdbActor
        let msg = LmdbActorMessage::StoreCandles {
            symbol: symbol.to_string(),
            timeframe: timeframe_seconds,
            candles: candles.clone(),
        };
        
        match self.lmdb_actor.ask(msg).send().await {
            Ok(LmdbActorResponse::Success) => {
                info!("💾 Stored {} candles for {} {} (range: {} - {})", 
                      candles.len(), symbol, interval, start_time, end_time);
            }
            Ok(LmdbActorResponse::ErrorResponse(err)) => {
                return Err(ApiError::Unknown(format!("Failed to store candles: {}", err)));
            }
            Ok(_) => {
                return Err(ApiError::Unknown("Unexpected response from LmdbActor".to_string()));
            }
            Err(e) => {
                return Err(ApiError::Unknown(format!("Failed to communicate with LmdbActor: {}", e)));
            }
        }

        // Store to PostgreSQL for dual storage strategy (after LMDB commit)
        if let Some(postgres_actor) = &self.postgres_actor {
            if let Some((candles_len, candles_vec)) = postgres_data {
                let start_timestamp = chrono::DateTime::from_timestamp_millis(start_time)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| format!("INVALID_TIME({})", start_time));
                let end_timestamp = chrono::DateTime::from_timestamp_millis(end_time)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| format!("INVALID_TIME({})", end_time));
                    
                info!("🔄 [ApiActor] Sending batch of {} candles to PostgreSQL for {} {} (range: {} - {})", 
                      candles_len, symbol, interval, start_timestamp, end_timestamp);
                
                // First send a health check to verify the PostgreSQL actor is responsive
                info!("🏥 [ApiActor] Sending health check to PostgreSQL actor first");
                if let Err(e) = postgres_actor.tell(PostgresTell::HealthCheck).send().await {
                    error!("❌ [ApiActor] PostgreSQL health check failed: {}", e);
                    return Err(ApiError::Network(format!("PostgreSQL health check failed: {}", e)));
                }
                
                let postgres_msg = PostgresTell::StoreBatch {
                    candles: candles_vec,
                    source: "ApiActor".to_string(),
                };
                
                if let Err(e) = postgres_actor.tell(postgres_msg).send().await {
                    warn!("❌ [ApiActor] Failed to store API batch to PostgreSQL for {}: {}", symbol, e);
                } else {
                    info!("✅ [ApiActor] Successfully sent {} API candles to PostgreSQL for {} (range: {} - {})", 
                          candles_len, symbol, start_timestamp, end_timestamp);
                }
            }
        } else {
            warn!("⚠️  [ApiActor] PostgreSQL actor not available for storing {} candles", candles.len());
        }

        Ok(())
    }

    /// Convert interval string to seconds
    fn interval_to_seconds(&self, interval: &str) -> u64 {
        match interval {
            "1m" => 60,
            "3m" => 180,
            "5m" => 300,
            "15m" => 900,
            "30m" => 1800,
            "1h" => 3600,
            "2h" => 7200,
            "4h" => 14400,
            "6h" => 21600,
            "8h" => 28800,
            "12h" => 43200,
            "1d" => 86400,
            _ => 60, // Default to 1 minute
        }
    }

    /// Fetch and store klines data
    async fn fetch_and_store_klines(&mut self, symbol: &str, interval: &str, request: ApiRequest) -> Result<Vec<FuturesOHLCVCandle>, ApiError> {
        self.stats.record_request();

        let mut retries = 0;
        let max_retries = self.config.max_retries;

        loop {
            match self.klines_client.fetch_klines(request.clone()).await {
                Ok(response) => {
                    let candles = response.data;
                    self.stats.record_success(candles.len() as u64);
                    
                    // Store in database
                    if let Err(e) = self.store_candles(symbol, interval, candles.clone()).await {
                        warn!("Failed to store candles: {}", e);
                    }

                    return Ok(candles);
                }
                Err(e) => {
                    if e.is_rate_limit() {
                        self.stats.record_rate_limit();
                        warn!("⚠️ Rate limit hit, waiting before retry...");
                        sleep(Duration::from_millis(self.config.rate_limit_delay_ms * 2)).await;
                    } else if e.is_recoverable() && retries < max_retries {
                        retries += 1;
                        warn!("🔄 Retrying request ({}/{}): {}", retries, max_retries, e);
                        sleep(Duration::from_millis(1000 * retries as u64)).await;
                    } else {
                        self.stats.record_failure();
                        return Err(e);
                    }
                }
            }
        }
    }
}

impl Actor for ApiActor {
    type Mailbox = UnboundedMailbox<Self>;

    fn name() -> &'static str {
        "ApiActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        info!("🚀 Starting API Actor");
        info!("🌐 Target API: {}", self.config.base_url);
        info!("🎭 LMDB operations delegated to LmdbActor");
        Ok(())
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), BoxError> {
        info!("🛑 Stopping API Actor");
        info!("📊 Final stats: {} requests, {:.1}% success rate, {} candles fetched",
            self.stats.requests_made,
            self.stats.success_rate() * 100.0,
            self.stats.total_candles_fetched
        );
        Ok(())
    }
}

impl Message<ApiTell> for ApiActor {
    type Reply = ();

    async fn handle(&mut self, msg: ApiTell, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            ApiTell::FillGap { symbol, interval, start_time, end_time } => {
                info!("🔍 Filling gap for {} {} from {} to {}", symbol, interval, start_time, end_time);
                
                let request = ApiRequest::new_klines(symbol.clone(), interval.clone())
                    .with_time_range(start_time, end_time)
                    .with_limit(1000);

                match self.fetch_and_store_klines(&symbol, &interval, request).await {
                    Ok(candles) => {
                        info!("✅ Successfully filled gap: {} candles", candles.len());
                    }
                    Err(e) => {
                        error!("❌ Failed to fill gap: {}", e);
                    }
                }
            }
            ApiTell::FetchRecent { symbol, interval, limit } => {
                info!("📊 Fetching {} recent {} candles for {}", limit, interval, symbol);
                
                let request = ApiRequest::new_klines(symbol.clone(), interval.clone())
                    .with_limit(limit);

                match self.fetch_and_store_klines(&symbol, &interval, request).await {
                    Ok(candles) => {
                        info!("✅ Successfully fetched recent data: {} candles", candles.len());
                    }
                    Err(e) => {
                        error!("❌ Failed to fetch recent data: {}", e);
                    }
                }
            }
        }
    }
}

impl Message<ApiAsk> for ApiActor {
    type Reply = Result<ApiReply, String>;

    async fn handle(&mut self, msg: ApiAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            ApiAsk::GetStats => {
                Ok(ApiReply::Stats(self.stats.clone()))
            }
            ApiAsk::GetDataRange { symbol, interval } => {
                let timeframe_seconds = self.interval_to_seconds(&interval);

                // Get data range from LmdbActor (will initialize if needed through ensure_database_initialized)
                let msg = LmdbActorMessage::GetDataRange {
                    symbol: symbol.clone(),
                    timeframe: timeframe_seconds,
                };
                
                match self.lmdb_actor.ask(msg).send().await {
                    Ok(LmdbActorResponse::DataRange { earliest, latest, count }) => {
                        Ok(ApiReply::DataRange {
                            symbol,
                            interval,
                            earliest,
                            latest,
                            count,
                        })
                    }
                    Ok(LmdbActorResponse::ErrorResponse(err)) => {
                        Ok(ApiReply::Error(format!("Failed to get data range: {}", err)))
                    }
                    Ok(_) => {
                        Ok(ApiReply::Error("Unexpected response from LmdbActor".to_string()))
                    }
                    Err(e) => {
                        Ok(ApiReply::Error(format!("Failed to communicate with LmdbActor: {}", e)))
                    }
                }
            }
            ApiAsk::FetchKlines { symbol, interval, start_time, end_time, limit } => {
                let mut request = ApiRequest::new_klines(symbol.clone(), interval.clone());
                
                if let (Some(start), Some(end)) = (start_time, end_time) {
                    request = request.with_time_range(start, end);
                }
                
                if let Some(limit) = limit {
                    request = request.with_limit(limit);
                }

                match self.fetch_and_store_klines(&symbol, &interval, request).await {
                    Ok(candles) => Ok(ApiReply::Klines(candles)),
                    Err(e) => Ok(ApiReply::Error(format!("Failed to fetch klines: {}", e))),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_api_actor_creation() {
        // Create a dummy LmdbActor for testing
        let temp_dir = TempDir::new().unwrap();
        let lmdb_actor = kameo::spawn(LmdbActor::new(temp_dir.path(), 60).unwrap());
        
        let actor = ApiActor::new(lmdb_actor.clone()).unwrap();
        
        assert_eq!(actor.stats.requests_made, 0);
    }

    #[tokio::test]
    async fn test_interval_to_seconds() {
        // Create a dummy LmdbActor for testing
        let temp_dir = TempDir::new().unwrap();
        let lmdb_actor = kameo::spawn(LmdbActor::new(temp_dir.path(), 60).unwrap());
        
        let actor = ApiActor::new(lmdb_actor.clone()).unwrap();
        
        assert_eq!(actor.interval_to_seconds("1m"), 60);
        assert_eq!(actor.interval_to_seconds("5m"), 300);
        assert_eq!(actor.interval_to_seconds("1h"), 3600);
        assert_eq!(actor.interval_to_seconds("1d"), 86400);
    }
}