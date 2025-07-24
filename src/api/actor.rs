use std::path::PathBuf;
use std::time::Duration;

use heed::{Database, Env, EnvOpenOptions};
use heed::types::{SerdeBincode, Str};
use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::request::MessageSend;
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::api::binance::BinanceKlinesClient;
use crate::api::types::{ApiConfig, ApiError, ApiRequest, ApiStats};
use crate::historical::structs::{FuturesOHLCVCandle, TimeRange, TimestampMS};
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
    /// LMDB environments per symbol-interval
    envs: FxHashMap<String, Env>,
    /// Candle databases per symbol-interval
    candle_dbs: FxHashMap<String, Database<Str, SerdeBincode<FuturesOHLCVCandle>>>,
    /// Certified range databases per symbol-interval
    range_dbs: FxHashMap<String, Database<Str, SerdeBincode<TimeRange>>>,
    /// Base path for LMDB storage
    base_path: PathBuf,
    /// PostgreSQL actor reference for dual storage
    postgres_actor: Option<ActorRef<PostgresActor>>,
}

impl ApiActor {
    /// Create a new API actor
    pub fn new(base_path: PathBuf) -> Result<Self, ApiError> {
        Self::new_with_config(base_path, ApiConfig::default())
    }

    /// Create a new API actor with custom configuration
    pub fn new_with_config(base_path: PathBuf, config: ApiConfig) -> Result<Self, ApiError> {
        let klines_client = BinanceKlinesClient::new(config.base_url.clone())?;

        if !base_path.exists() {
            std::fs::create_dir_all(&base_path)
                .map_err(|e| ApiError::Unknown(format!("Failed to create base path: {}", e)))?;
        }

        Ok(Self {
            klines_client,
            stats: ApiStats::new(),
            config,
            envs: FxHashMap::default(),
            candle_dbs: FxHashMap::default(),
            range_dbs: FxHashMap::default(),
            base_path,
            postgres_actor: None,
        })
    }

    /// Set the PostgreSQL actor reference for dual storage
    pub fn set_postgres_actor(&mut self, postgres_actor: ActorRef<PostgresActor>) {
        self.postgres_actor = Some(postgres_actor);
    }

    /// Initialize LMDB database for a symbol-interval combination
    fn init_database(&mut self, symbol: &str, interval: &str) -> Result<(), ApiError> {
        let timeframe_seconds = self.interval_to_seconds(interval);
        let db_key = format!("{}_{}", symbol, timeframe_seconds);
        
        if self.envs.contains_key(&db_key) {
            return Ok(()); // Already initialized
        }

        let db_path = self.base_path.join(&db_key);
        std::fs::create_dir_all(&db_path)
            .map_err(|e| ApiError::Unknown(format!("Failed to create DB path for {}: {}", db_key, e)))?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1GB per symbol-interval
                .max_dbs(10)
                .max_readers(256)
                .open(&db_path)
                .map_err(|e| ApiError::Unknown(format!("Failed to open LMDB for {}: {}", db_key, e)))?
        };

        let mut wtxn = env.write_txn()
            .map_err(|e| ApiError::Unknown(format!("Failed to create write transaction for {}: {}", db_key, e)))?;

        let candle_db = env.create_database::<Str, SerdeBincode<FuturesOHLCVCandle>>(&mut wtxn, Some("candles"))
            .map_err(|e| ApiError::Unknown(format!("Failed to create candle DB for {}: {}", db_key, e)))?;

        let range_db = env.create_database::<Str, SerdeBincode<TimeRange>>(&mut wtxn, Some("certified_ranges"))
            .map_err(|e| ApiError::Unknown(format!("Failed to create range DB for {}: {}", db_key, e)))?;

        wtxn.commit()
            .map_err(|e| ApiError::Unknown(format!("Failed to commit creation transaction for {}: {}", db_key, e)))?;

        self.envs.insert(db_key.clone(), env);
        self.candle_dbs.insert(db_key.clone(), candle_db);
        self.range_dbs.insert(db_key.clone(), range_db);

        info!("✅ Initialized LMDB database for: {}", db_key);
        Ok(())
    }

    /// Store candles in LMDB and update certified ranges
    async fn store_candles(&mut self, symbol: &str, interval: &str, candles: Vec<FuturesOHLCVCandle>) -> Result<(), ApiError> {
        if candles.is_empty() {
            return Ok(());
        }

        let timeframe_seconds = self.interval_to_seconds(interval);
        let db_key = format!("{}_{}", symbol, timeframe_seconds);
        self.init_database(symbol, interval)?;

        let env = self.envs.get(&db_key).unwrap();
        let candle_db = self.candle_dbs.get(&db_key).unwrap();
        let range_db = self.range_dbs.get(&db_key).unwrap();

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

        // Get range info before LMDB operations
        let start_time = candles.first().unwrap().open_time;
        let end_time = candles.last().unwrap().close_time;

        // Perform LMDB operations in a scoped block
        {
            let mut wtxn = env.write_txn()
                .map_err(|e| ApiError::Unknown(format!("Failed to create write transaction: {}", e)))?;

            // Store candles
            for candle in &candles {
                let key = format!("{}:{}", timeframe_seconds, candle.close_time);
                candle_db.put(&mut wtxn, &key, candle)
                    .map_err(|e| ApiError::Unknown(format!("Failed to store candle: {}", e)))?;
            }

            // Update certified range
            let range = TimeRange { start: start_time, end: end_time };
            let range_key = format!("{}_range", interval);
            
            // Get existing range to potentially merge
            let existing_range = range_db.get(&wtxn, &range_key)
                .map_err(|e| ApiError::Unknown(format!("Failed to read existing range: {}", e)))?;

            let merged_range = if let Some(existing) = existing_range {
                TimeRange {
                    start: std::cmp::min(existing.start, range.start),
                    end: std::cmp::max(existing.end, range.end),
                }
            } else {
                range
            };

            range_db.put(&mut wtxn, &range_key, &merged_range)
                .map_err(|e| ApiError::Unknown(format!("Failed to store range: {}", e)))?;

            wtxn.commit()
                .map_err(|e| ApiError::Unknown(format!("Failed to commit storage transaction: {}", e)))?;
        } // wtxn goes out of scope here

        info!("💾 Stored {} candles for {} {} (range: {} - {})", 
              candles.len(), symbol, interval, start_time, end_time);

        // Store to PostgreSQL for dual storage strategy (after LMDB commit)
        if let Some(postgres_actor) = &self.postgres_actor {
            if let Some((candles_len, candles_vec)) = postgres_data {
                let postgres_msg = PostgresTell::StoreBatch {
                    candles: candles_vec,
                };
                
                if let Err(e) = postgres_actor.tell(postgres_msg).send().await {
                    warn!("Failed to store API batch to PostgreSQL for {}: {}", symbol, e);
                } else {
                    info!("🐘 Stored {} API candles to PostgreSQL for {}", candles_len, symbol);
                }
            }
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
        info!("📁 LMDB storage path: {}", self.base_path.display());
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
                let db_key = format!("{}_{}", symbol, timeframe_seconds);
                
                if let Err(e) = self.init_database(&symbol, &interval) {
                    return Ok(ApiReply::Error(format!("Failed to initialize database: {}", e)));
                }

                let env = self.envs.get(&db_key).unwrap();
                let candle_db = self.candle_dbs.get(&db_key).unwrap();

                match env.read_txn() {
                    Ok(rtxn) => {
                        let mut earliest = None;
                        let mut latest = None;
                        let mut count = 0;

                        let iter = candle_db.iter(&rtxn);
                        if let Ok(iter) = iter {
                            for result in iter {
                                if let Ok((_, candle)) = result {
                                    count += 1;
                                    if earliest.is_none() || candle.open_time < earliest.unwrap() {
                                        earliest = Some(candle.open_time);
                                    }
                                    if latest.is_none() || candle.close_time > latest.unwrap() {
                                        latest = Some(candle.close_time);
                                    }
                                }
                            }
                        }

                        Ok(ApiReply::DataRange {
                            symbol,
                            interval,
                            earliest,
                            latest,
                            count,
                        })
                    }
                    Err(e) => Ok(ApiReply::Error(format!("Failed to read database: {}", e))),
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
        let temp_dir = TempDir::new().unwrap();
        let actor = ApiActor::new(temp_dir.path().to_path_buf()).unwrap();
        
        assert_eq!(actor.envs.len(), 0);
        assert_eq!(actor.candle_dbs.len(), 0);
        assert_eq!(actor.stats.requests_made, 0);
    }

    #[test]
    fn test_interval_to_seconds() {
        let temp_dir = TempDir::new().unwrap();
        let actor = ApiActor::new(temp_dir.path().to_path_buf()).unwrap();
        
        assert_eq!(actor.interval_to_seconds("1m"), 60);
        assert_eq!(actor.interval_to_seconds("5m"), 300);
        assert_eq!(actor.interval_to_seconds("1h"), 3600);
        assert_eq!(actor.interval_to_seconds("1d"), 86400);
    }
}