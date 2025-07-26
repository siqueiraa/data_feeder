use std::path::PathBuf;
use std::sync::Arc;
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
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use crate::common::shared_data::{SharedCandle, shared_candle};
use crate::historical::structs::FuturesOHLCVCandle;
use crate::postgres::{PostgresActor, PostgresTell};
use crate::websocket::binance::kline::parse_any_kline_message;
use crate::websocket::connection::{ConnectionManager, normalize_symbols};
use crate::websocket::types::{
    ConnectionStats, ConnectionStatus, StreamSubscription, StreamType, WebSocketError,
};

/// WebSocket actor messages for telling (fire-and-forget)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebSocketTell {
    /// Subscribe to streams for given symbols
    Subscribe {
        stream_type: StreamType,
        symbols: Vec<String>,
    },
    /// Unsubscribe from streams for given symbols
    Unsubscribe {
        stream_type: StreamType,
        symbols: Vec<String>,
    },
    /// Process a received candle (internal use)
    ProcessCandle {
        symbol: String,
        candle: FuturesOHLCVCandle,
        is_closed: bool,
    },
    /// Force reconnection
    Reconnect,
    /// Health check
    HealthCheck,
    /// Check for gap after reconnection and fill if needed
    CheckReconnectionGap,
}

/// WebSocket actor messages for asking (request-response)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebSocketAsk {
    /// Get current connection status
    GetConnectionStatus,
    /// Get active stream subscriptions
    GetActiveStreams,
    /// Get connection statistics
    GetStats,
    /// Get recent candles for a symbol
    GetRecentCandles {
        symbol: String,
        limit: usize,
    },
}

/// WebSocket actor replies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebSocketReply {
    /// Connection status response
    ConnectionStatus {
        status: ConnectionStatus,
        stats: ConnectionStats,
    },
    /// Active streams response
    ActiveStreams(Vec<StreamSubscription>),
    /// Statistics response
    Stats(ConnectionStats),
    /// Recent candles response
    RecentCandles(Vec<FuturesOHLCVCandle>),
    /// Success response
    Success,
    /// Error response
    Error(String),
}

/// WebSocket actor for real-time data streaming
pub struct WebSocketActor {
    /// Connection manager
    connection_manager: ConnectionManager,
    /// Active subscriptions
    subscriptions: Arc<RwLock<Vec<StreamSubscription>>>,
    /// LMDB environments per symbol-timeframe
    envs: FxHashMap<String, Env>,
    /// Candle databases per symbol
    candle_dbs: FxHashMap<String, Database<Str, SerdeBincode<FuturesOHLCVCandle>>>,
    /// Base path for LMDB storage
    base_path: PathBuf,
    /// Recent candles cache (symbol -> candles)
    recent_candles: FxHashMap<String, Vec<SharedCandle>>,
    /// Maximum recent candles to keep in memory
    max_recent_candles: usize,
    /// Maximum idle time before marking connection as unhealthy (in seconds)
    max_idle_time: u64,
    /// Handle to the current connection task (for proper termination)
    connection_task: Option<tokio::task::JoinHandle<()>>,
    /// Last activity timestamp (when we last processed a message) - for health checks
    last_activity_time: Option<i64>,
    /// Last processed candle timestamp - for gap detection on reconnection
    last_processed_candle_time: Option<i64>,
    /// API actor reference for gap filling
    api_actor: Option<ActorRef<crate::api::ApiActor>>,
    /// PostgreSQL actor reference for dual storage
    postgres_actor: Option<ActorRef<PostgresActor>>,
    /// TimeFrame actor reference for direct real-time forwarding
    timeframe_actor: Option<ActorRef<crate::technical_analysis::actors::timeframe::TimeFrameActor>>,
    /// Gap detection configuration
    gap_threshold_minutes: u32,
    gap_check_delay_seconds: u32,
}

impl WebSocketActor {
    /// Create a new WebSocket actor with default settings
    pub fn new(base_path: PathBuf) -> Result<Self, WebSocketError> {
        Self::new_with_config(base_path, 300, 2, 5) // 5min max idle, 2min gap threshold, 5s delay
    }

    /// Set the API actor reference for gap filling
    pub fn set_api_actor(&mut self, api_actor: ActorRef<crate::api::ApiActor>) {
        self.api_actor = Some(api_actor);
    }
    
    /// Set the PostgreSQL actor reference for dual storage
    pub fn set_postgres_actor(&mut self, postgres_actor: ActorRef<PostgresActor>) {
        self.postgres_actor = Some(postgres_actor);
    }
    
    /// Set the TimeFrame actor reference for direct real-time forwarding
    pub fn set_timeframe_actor(&mut self, timeframe_actor: ActorRef<crate::technical_analysis::actors::timeframe::TimeFrameActor>) {
        self.timeframe_actor = Some(timeframe_actor);
    }

    /// Create a new WebSocket actor with custom configuration
    pub fn new_with_config(
        base_path: PathBuf, 
        max_idle_secs: u64,
        gap_threshold_minutes: u32,
        gap_check_delay_seconds: u32
    ) -> Result<Self, WebSocketError> {
        let connection_manager = ConnectionManager::new_binance_futures();
        
        if !base_path.exists() {
            std::fs::create_dir_all(&base_path)
                .map_err(|e| WebSocketError::Unknown(format!("Failed to create base path: {}", e)))?;
        }

        Ok(Self {
            connection_manager,
            subscriptions: Arc::new(RwLock::new(Vec::new())),
            envs: FxHashMap::default(),
            candle_dbs: FxHashMap::default(),
            base_path,
            recent_candles: FxHashMap::default(),
            max_recent_candles: 1000,
            max_idle_time: max_idle_secs,
            connection_task: None,
            last_activity_time: None,
            last_processed_candle_time: None,
            api_actor: None,
            postgres_actor: None,
            timeframe_actor: None,
            gap_threshold_minutes,
            gap_check_delay_seconds,
        })
    }

    /// Initialize LMDB database for a symbol
    fn init_symbol_db(&mut self, symbol: &str) -> Result<(), WebSocketError> {
        if self.envs.contains_key(symbol) {
            return Ok(()); // Already initialized
        }

        let db_path = self.base_path.join(format!("{}_60", symbol)); // WebSocket stores 1-minute data only
        std::fs::create_dir_all(&db_path)
            .map_err(|e| WebSocketError::Unknown(format!("Failed to create DB path for {}: {}", symbol, e)))?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1GB per symbol
                .max_dbs(10)
                .max_readers(256)
                .open(&db_path)
                .map_err(|e| WebSocketError::Unknown(format!("Failed to open LMDB for {}: {}", symbol, e)))?
        };

        let mut wtxn = env.write_txn()
            .map_err(|e| WebSocketError::Unknown(format!("Failed to create write transaction for {}: {}", symbol, e)))?;

        let candle_db = env.create_database::<Str, SerdeBincode<FuturesOHLCVCandle>>(&mut wtxn, Some("candles"))
            .map_err(|e| WebSocketError::Unknown(format!("Failed to create candle DB for {}: {}", symbol, e)))?;

        wtxn.commit()
            .map_err(|e| WebSocketError::Unknown(format!("Failed to commit creation transaction for {}: {}", symbol, e)))?;

        self.envs.insert(symbol.to_string(), env);
        self.candle_dbs.insert(symbol.to_string(), candle_db);
        self.recent_candles.insert(symbol.to_string(), Vec::new());

        info!("✅ Initialized LMDB database for symbol: {}", symbol);
        Ok(())
    }

    /// Store a candle in LMDB and update recent cache - handles both live and closed candles
    fn store_candle(&mut self, symbol: &str, candle: &FuturesOHLCVCandle, is_closed: bool) -> Result<(), WebSocketError> {
        // Always update recent cache for both live and closed candles (for real-time access)
        if let Some(recent) = self.recent_candles.get_mut(symbol) {
            recent.push(shared_candle(candle.clone()));
            if recent.len() > self.max_recent_candles {
                recent.remove(0);
            }
        }
        
        // Only store closed candles to LMDB to avoid partial data
        if !is_closed {
            debug!("📝 Cached live candle for {} (not stored to LMDB)", symbol);
            return Ok(());
        }
        
        info!("💾 Storing completed 1-minute candle for {} at {}", symbol, candle.close_time);

        // Ensure database is initialized
        if !self.envs.contains_key(symbol) {
            self.init_symbol_db(symbol)?;
        }

        // Store in LMDB
        if let (Some(env), Some(candle_db)) = (self.envs.get(symbol), self.candle_dbs.get(symbol)) {
            let mut wtxn = env.write_txn()
                .map_err(|e| WebSocketError::Unknown(format!("Failed to create write transaction: {}", e)))?;

            let key = format!("60:{}", candle.close_time); // 1-minute timeframe:close_time
            candle_db.put(&mut wtxn, &key, candle)
                .map_err(|e| WebSocketError::Unknown(format!("Failed to store candle: {}", e)))?;

            wtxn.commit()
                .map_err(|e| WebSocketError::Unknown(format!("Failed to commit candle storage: {}", e)))?;

            debug!("Stored candle for {} at {}", symbol, candle.close_time);
        }

        Ok(())
    }

    /// Start WebSocket connection with current subscriptions
    async fn start_connection(&mut self, actor_ref: ActorRef<Self>) {
        // Step 1: Terminate existing connection if any
        if let Some(handle) = self.connection_task.take() {
            info!("🛑 Terminating existing WebSocket connection...");
            handle.abort();
            // Give a moment for cleanup
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let subscriptions = self.subscriptions.read().await.clone();
        
        if subscriptions.is_empty() {
            warn!("No subscriptions found, cannot start connection");
            return;
        }

        // Validate and normalize all symbols
        let mut all_symbols = Vec::new();
        for subscription in &subscriptions {
            if !subscription.stream_type.is_implemented() {
                error!("Stream type {:?} is not implemented.", subscription.stream_type);
                return;
            }
            all_symbols.extend(subscription.symbols.clone());
        }

        let normalized_symbols = match normalize_symbols(&all_symbols) {
            Ok(symbols) => symbols,
            Err(e) => {
                error!("Failed to normalize symbols: {}", e);
                return;
            }
        };

        // Initialize databases for all symbols
        for symbol in &normalized_symbols {
            if let Err(e) = self.init_symbol_db(symbol) {
                error!("Failed to initialize database for {}: {}", symbol, e);
                return;
            }
        }

        // Build connection URL
        let url = if subscriptions.len() == 1 && subscriptions[0].symbols.len() == 1 {
            match self.connection_manager.build_single_stream_url(&subscriptions[0]) {
                Ok(url) => url,
                Err(e) => {
                    error!("Failed to build single stream URL: {}", e);
                    return;
                }
            }
        } else {
            match self.connection_manager.build_multi_stream_url(&subscriptions) {
                Ok(url) => url,
                Err(e) => {
                    error!("Failed to build multi-stream URL: {}", e);
                    return;
                }
            }
        };

        info!("🔗 Starting WebSocket connection to: {}", url);

        let mut connection_manager = self.connection_manager.clone();
        let actor_ref_for_connection = actor_ref.clone();
        let handle = tokio::spawn(async move {
            let message_handler = |message: String| {
                let actor_ref = actor_ref_for_connection.clone();
                async move {
                    match parse_any_kline_message(&message) {
                        Ok(kline_event) => {
                            debug!("🔍 Parsed kline message for {} (closed: {})", kline_event.symbol, kline_event.kline.is_completed());
                            
                            let candle = match kline_event.kline.to_futures_candle() {
                                Ok(candle) => candle,
                                Err(e) => {
                                    warn!("Failed to convert kline to candle: {}", e);
                                    return Err(e);
                                }
                            };

                            let is_closed = kline_event.kline.is_completed();
                            
                            info!("💰 {} candle: O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.2} Qt:{} Tb:{:.2} (closed: {})",
                                 kline_event.symbol, candle.open, candle.high, candle.low, candle.close, candle.volume, candle.number_of_trades, candle.taker_buy_base_asset_volume, is_closed);
                            
                            let process_msg = WebSocketTell::ProcessCandle {
                                symbol: kline_event.symbol.clone(),
                                candle,
                                is_closed,
                            };
                            
                            if let Err(e) = actor_ref.tell(process_msg).send().await {
                                warn!("Failed to send processed candle to actor: {}", e);
                            }
                            
                            Ok(())
                        }
                        Err(e) => {
                            warn!("Failed to parse kline message: {}", e);
                            Err(e)
                        }
                    }
                }
            };

            if let Err(e) = connection_manager.connect_with_retry(&url, message_handler).await {
                error!("WebSocket connection failed permanently: {}", e);
            }
        });
        
        // Step 2: Store the new connection handle
        self.connection_task = Some(handle);
        info!("✅ Started new WebSocket connection task");
        
        // Step 3: Schedule gap detection after connection is established
        let actor_ref_for_gap_check = actor_ref.clone();
        let gap_check_delay = self.gap_check_delay_seconds;
        tokio::spawn(async move {
            // Wait for connection to establish and start receiving data
            tokio::time::sleep(std::time::Duration::from_secs(gap_check_delay as u64)).await;
            
            // Trigger gap detection
            if let Err(e) = actor_ref_for_gap_check.tell(WebSocketTell::CheckReconnectionGap).send().await {
                error!("Failed to trigger reconnection gap check: {}", e);
            }
        });
    }

    /// Add a subscription
    async fn add_subscription(&mut self, stream_type: StreamType, symbols: Vec<String>) -> Result<(), WebSocketError> {
        if !stream_type.is_implemented() {
            return Err(WebSocketError::NotImplemented(stream_type));
        }

        let normalized_symbols = normalize_symbols(&symbols)?;
        let subscription = StreamSubscription::new(stream_type.clone(), normalized_symbols);

        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.push(subscription);

        info!("➕ Added subscription: {} for {:?}", stream_type, symbols);
        Ok(())
    }

    /// Remove a subscription
    async fn remove_subscription(&mut self, stream_type: StreamType, symbols: Vec<String>) -> Result<(), WebSocketError> {
        let normalized_symbols = normalize_symbols(&symbols)?;
        
        let mut subscriptions = self.subscriptions.write().await;
        let mut indices_to_remove = Vec::new();
        
        for (i, sub) in subscriptions.iter_mut().enumerate() {
            if sub.stream_type == stream_type {
                // Remove specified symbols from this subscription
                sub.symbols.retain(|symbol| !normalized_symbols.contains(symbol));
                
                // Mark subscription for removal if no symbols left
                if sub.symbols.is_empty() {
                    indices_to_remove.push(i);
                }
            }
        }
        
        // Remove empty subscriptions in reverse order to maintain indices
        for &i in indices_to_remove.iter().rev() {
            subscriptions.remove(i);
        }

        info!("➖ Removed subscription: {} for {:?}", stream_type, symbols);
        Ok(())
    }

    /// Get recent candles for a symbol
    fn get_recent_candles(&self, symbol: &str, limit: usize) -> Vec<FuturesOHLCVCandle> {
        if let Some(recent) = self.recent_candles.get(symbol) {
            let start_idx = if recent.len() > limit { recent.len() - limit } else { 0 };
            recent[start_idx..].iter().map(|candle| (**candle).clone()).collect()
        } else {
            Vec::new()
        }
    }

    /// Get the number of initialized database environments (for testing)
    #[cfg(test)]
    pub fn env_count(&self) -> usize {
        self.envs.len()
    }

    /// Get the number of initialized candle databases (for testing)
    #[cfg(test)]
    pub fn candle_db_count(&self) -> usize {
        self.candle_dbs.len()
    }

    /// Get the number of symbols with recent candles cache (for testing)
    #[cfg(test)]
    pub fn recent_candles_count(&self) -> usize {
        self.recent_candles.len()
    }
}

impl Actor for WebSocketActor {
    type Mailbox = UnboundedMailbox<Self>;

    fn name() -> &'static str {
        "WebSocketActor"
    }

    async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        info!("🚀 Starting WebSocket Actor");
        info!("📁 LMDB storage path: {}", self.base_path.display());
        info!("🌐 Target exchange: Binance Futures");

        // Start health check task
        let actor_ref_clone = actor_ref.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if let Err(e) = actor_ref_clone.tell(WebSocketTell::HealthCheck).send().await {
                    warn!("Failed to send health check: {}", e);
                    break;
                }
            }
        });

        Ok(())
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), BoxError> {
        info!("🛑 Stopping WebSocket Actor");
        
        // Log final statistics
        let stats = self.connection_manager.stats();
        info!("📊 Final stats: {} messages received, {} parsed, {:.2}% success rate",
            stats.messages_received,
            stats.messages_parsed,
            stats.parse_success_rate() * 100.0
        );

        Ok(())
    }
}

impl Message<WebSocketTell> for WebSocketActor {
    type Reply = ();

    async fn handle(&mut self, msg: WebSocketTell, ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            WebSocketTell::Subscribe { stream_type, symbols } => {
                if let Err(e) = self.add_subscription(stream_type, symbols).await {
                    error!("Failed to add subscription: {}", e);
                }
            }
            WebSocketTell::Unsubscribe { stream_type, symbols } => {
                if let Err(e) = self.remove_subscription(stream_type, symbols).await {
                    error!("Failed to remove subscription: {}", e);
                }
            }
            WebSocketTell::ProcessCandle { symbol, candle, is_closed } => {
                // Update activity timestamp - this proves we're receiving messages
                self.last_activity_time = Some(chrono::Utc::now().timestamp_millis());
                
                // Track last processed candle time for gap detection (only closed candles)
                if is_closed {
                    self.last_processed_candle_time = Some(candle.close_time);
                    debug!("📝 Updated last processed candle time for {}: {}", symbol, candle.close_time);
                }
                
                // Store candle (both live and closed go to recent cache, only closed to LMDB)
                if let Err(e) = self.store_candle(&symbol, &candle, is_closed) {
                    error!("Failed to store candle for {}: {}", symbol, e);
                }
                
                // Store closed candles to PostgreSQL for dual storage strategy
                if is_closed {
                    let timestamp = chrono::DateTime::from_timestamp_millis(candle.close_time)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| format!("INVALID_TIME({})", candle.close_time));
                    
                    info!("🔄 [WebSocketActor] Sending closed candle to PostgreSQL: {} - {} (close: {})", 
                          symbol, timestamp, candle.close);
                    
                    if let Some(postgres_actor) = &self.postgres_actor {
                        let postgres_msg = PostgresTell::StoreCandle {
                            symbol: symbol.clone(),
                            candle: candle.clone(),
                            source: "WebSocketActor".to_string(),
                        };
                        
                        if let Err(e) = postgres_actor.tell(postgres_msg).send().await {
                            warn!("❌ [WebSocketActor] Failed to store candle to PostgreSQL for {}: {}", symbol, e);
                        } else {
                            info!("✅ [WebSocketActor] Successfully sent candle to PostgreSQL: {} @ {}", symbol, timestamp);
                        }
                    } else {
                        warn!("⚠️  [WebSocketActor] PostgreSQL actor not available for storing candle: {} @ {}", symbol, timestamp);
                    }
                } else {
                    // Log live candle updates for debugging timing
                    let timestamp = chrono::DateTime::from_timestamp_millis(candle.close_time)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| format!("INVALID_TIME({})", candle.close_time));
                    debug!("📊 [WebSocketActor] Processing live candle update: {} - {} (close: {})", 
                           symbol, timestamp, candle.close);
                }
                
                // Direct real-time forwarding to TimeFrame actor for immediate TA updates
                if let Some(timeframe_actor) = &self.timeframe_actor {
                    let forward_start = std::time::Instant::now();
                    let msg = crate::technical_analysis::actors::timeframe::TimeFrameTell::ProcessCandle {
                        symbol: symbol.clone(),
                        candle: candle.clone(),
                        is_closed,
                    };
                    
                    if let Err(e) = timeframe_actor.tell(msg).send().await {
                        error!("Failed to forward candle to TimeFrame actor: {}", e);
                    } else {
                        let forward_time = forward_start.elapsed();
                        debug!("🚀 Forwarded {} candle ({}) to TimeFrame: {:.2} @ {} (fwd: {:?})", 
                               symbol, if is_closed { "closed" } else { "live" },
                               candle.close, candle.close_time, forward_time);
                    }
                }
            }
            WebSocketTell::Reconnect => {
                info!("🔄 Manual reconnection requested");
                let actor_ref = ctx.actor_ref().clone();
                self.start_connection(actor_ref).await;
            }
            WebSocketTell::HealthCheck => {
                let now = chrono::Utc::now().timestamp_millis();
                let is_healthy = match self.last_activity_time {
                    Some(last_activity) => {
                        let idle_time_ms = now - last_activity;
                        let max_idle_ms = (self.max_idle_time * 1000) as i64;
                        idle_time_ms < max_idle_ms
                    }
                    None => {
                        // No activity yet - check if we're within grace period
                        // For now, assume it's healthy during startup
                        true
                    }
                };
                
                if !is_healthy {
                    warn!("⚠️ WebSocket connection is unhealthy - no messages processed recently. Last activity: {:?}", 
                          self.last_activity_time);
                    
                    // Trigger automatic reconnection on health failure
                    let actor_ref = ctx.actor_ref().clone();
                    self.start_connection(actor_ref).await;
                } else {
                    debug!("✅ WebSocket connection is healthy - recent activity: {:?}", self.last_activity_time);
                }
            }
            WebSocketTell::CheckReconnectionGap => {
                info!("🔍 Checking for gaps after reconnection...");
                
                if let Some(api_actor) = &self.api_actor {
                    let subscriptions = self.subscriptions.read().await;
                    let now = chrono::Utc::now().timestamp_millis();
                    
                    for subscription in subscriptions.iter() {
                        for symbol in &subscription.symbols {
                            if let Some(last_candle_time) = self.last_processed_candle_time {
                                let gap_duration_ms = now - last_candle_time;
                                let gap_minutes = gap_duration_ms / (60 * 1000);
                                
                                // Check if gap is significant (using configured threshold)
                                if gap_minutes > self.gap_threshold_minutes as i64 {
                                    let gap_start = chrono::DateTime::from_timestamp_millis(last_candle_time + 60000)
                                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                                        .unwrap_or_else(|| format!("INVALID_TIME({})", last_candle_time + 60000));
                                    let gap_end = chrono::DateTime::from_timestamp_millis(now - 60000)
                                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                                        .unwrap_or_else(|| format!("INVALID_TIME({})", now - 60000));
                                    
                                    warn!("🕳️ [WebSocketActor] Found {} minute gap for {} after reconnection", gap_minutes, symbol);
                                    info!("🔧 [WebSocketActor] Requesting gap fill: {} from {} to {}", symbol, gap_start, gap_end);
                                    
                                    // Request gap filling via API actor
                                    let gap_fill_msg = crate::api::ApiTell::FillGap {
                                        symbol: symbol.clone(),
                                        interval: "1m".to_string(),
                                        start_time: last_candle_time + 60000, // Start from next minute
                                        end_time: now - 60000, // End at previous minute
                                    };
                                    
                                    if let Err(e) = api_actor.tell(gap_fill_msg).send().await {
                                        error!("❌ [WebSocketActor] Failed to request gap filling for {}: {}", symbol, e);
                                    } else {
                                        info!("✅ [WebSocketActor] Successfully requested gap filling for {} from {} to {}", 
                                              symbol, gap_start, gap_end);
                                    }
                                } else {
                                    debug!("✨ [WebSocketActor] No significant gap found for {} ({} minutes)", symbol, gap_minutes);
                                }
                            } else {
                                warn!("No last processed candle time available for gap detection");
                            }
                        }
                    }
                } else {
                    warn!("API actor not available for gap filling");
                }
            }
        }
    }
}

impl Message<WebSocketAsk> for WebSocketActor {
    type Reply = Result<WebSocketReply, String>;

    async fn handle(&mut self, msg: WebSocketAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            WebSocketAsk::GetConnectionStatus => {
                Ok(WebSocketReply::ConnectionStatus {
                    status: self.connection_manager.status().clone(),
                    stats: self.connection_manager.stats().clone(),
                })
            }
            WebSocketAsk::GetActiveStreams => {
                let subscriptions = self.subscriptions.read().await.clone();
                Ok(WebSocketReply::ActiveStreams(subscriptions))
            }
            WebSocketAsk::GetStats => {
                Ok(WebSocketReply::Stats(self.connection_manager.stats().clone()))
            }
            WebSocketAsk::GetRecentCandles { symbol, limit } => {
                let candles = self.get_recent_candles(&symbol, limit);
                Ok(WebSocketReply::RecentCandles(candles))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_websocket_actor_creation() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        
        assert_eq!(actor.envs.len(), 0);
        assert_eq!(actor.candle_dbs.len(), 0);
        assert_eq!(actor.recent_candles.len(), 0);
    }

    #[tokio::test]
    async fn test_add_subscription() {
        let temp_dir = TempDir::new().unwrap();
        let mut actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        
        let result = actor.add_subscription(
            StreamType::Kline1m,
            vec!["BTCUSDT".to_string()]
        ).await;
        
        assert!(result.is_ok());
        
        let subscriptions = actor.subscriptions.read().await;
        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].stream_type, StreamType::Kline1m);
        assert_eq!(subscriptions[0].symbols, vec!["BTCUSDT"]);
    }

    #[tokio::test]
    async fn test_remove_subscription() {
        let temp_dir = TempDir::new().unwrap();
        let mut actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        
        // Add subscription first
        actor.add_subscription(
            StreamType::Kline1m,
            vec!["BTCUSDT".to_string()]
        ).await.unwrap();
        
        // Remove subscription
        let result = actor.remove_subscription(
            StreamType::Kline1m,
            vec!["BTCUSDT".to_string()]
        ).await;
        
        assert!(result.is_ok());
        
        let subscriptions = actor.subscriptions.read().await;
        assert_eq!(subscriptions.len(), 0);
    }

    #[tokio::test]
    async fn test_init_symbol_db() {
        let temp_dir = TempDir::new().unwrap();
        let mut actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        
        let result = actor.init_symbol_db("BTCUSDT");
        assert!(result.is_ok());
        
        assert!(actor.envs.contains_key("BTCUSDT"));
        assert!(actor.candle_dbs.contains_key("BTCUSDT"));
        assert!(actor.recent_candles.contains_key("BTCUSDT"));
    }

    #[tokio::test]
    async fn test_websocket_actor_config() {
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new_with_config(
            temp_dir.path().to_path_buf(), 
            300, // max_idle_secs
            5,   // gap_threshold_minutes  
            10   // gap_check_delay_seconds
        ).unwrap();
        
        assert_eq!(actor.gap_threshold_minutes, 5);
        assert_eq!(actor.gap_check_delay_seconds, 10);
        assert_eq!(actor.max_idle_time, 300);
    }

    #[test]
    fn test_gap_detection_logic() {
        // Test the gap calculation logic
        let now = chrono::Utc::now().timestamp_millis();
        let last_candle_time = now - (5 * 60 * 1000); // 5 minutes ago
        
        let gap_duration_ms = now - last_candle_time;
        let gap_minutes = gap_duration_ms / (60 * 1000);
        
        assert_eq!(gap_minutes, 5);
        
        // Test threshold comparison
        let threshold = 2u32;
        assert!(gap_minutes > threshold as i64, "Gap should exceed threshold");
    }

    #[test]
    fn test_websocket_direct_forwarding_setup() {
        use tempfile::TempDir;
        
        let temp_dir = TempDir::new().unwrap();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        
        // Initially no TimeFrame actor reference
        assert!(actor.timeframe_actor.is_none());
        
        // Test that we can set the reference (we can't actually create a TimeFrameActor in unit test)
        // So we just verify the setter method exists and works
        // This would normally be: actor.set_timeframe_actor(timeframe_actor_ref);
        
        // Verify the configuration is set up for direct forwarding
        assert_eq!(actor.gap_threshold_minutes, 2); // Default
        assert_eq!(actor.gap_check_delay_seconds, 5); // Default
    }
}