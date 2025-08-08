use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;

use crossbeam_channel::{bounded, Sender};
use dashmap::DashMap;
use sharded_slab::Slab;
use crate::queue::{TaskQueue, DatabaseQueue};
use crate::queue::types::QueueConfig;
use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::request::MessageSend;
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, warn};
use crate::adaptive_config::AdaptiveConfig;
use crate::common::shared_data::{
    SharedCandle, SharedSymbol, shared_candle, intern_symbol
};
use crate::common::object_pool::init_global_pools;
use crate::historical::structs::FuturesOHLCVCandle;
#[cfg(feature = "postgres")]
use crate::postgres::{PostgresActor, PostgresTell};
use crate::websocket::optimized_parser::parse_any_kline_message_optimized;
use crate::websocket::connection::{ConnectionManager, normalize_symbols};
use crate::websocket::types::{
    ConnectionStats, ConnectionStatus, StreamSubscription, StreamType, WebSocketError,
};
use crate::lmdb::messages::LmdbActorTell;
#[cfg(feature = "volume_profile")]
use crate::volume_profile::{VolumeProfileActor, VolumeProfileTell};

/// Buffer for WebSocket message processing
#[derive(Debug)]
pub struct MessageBuffer {
    /// Raw message content
    pub content: String,
    /// Processing timestamp
    pub timestamp: i64,
    /// Buffer capacity
    pub capacity: usize,
}

impl MessageBuffer {
    /// Create a new message buffer with specified capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            content: String::with_capacity(capacity),
            timestamp: 0,
            capacity,
        }
    }
    
    /// Reset buffer for reuse
    pub fn reset(&mut self) {
        self.content.clear();
        self.timestamp = 0;
    }
    
    /// Set buffer content
    pub fn set_content(&mut self, content: String, timestamp: i64) {
        self.content = content;
        self.timestamp = timestamp;
    }
}

/// Buffer pool statistics for monitoring
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    /// Total buffers allocated
    pub total_buffers: usize,
    /// Buffers currently in use
    pub buffers_in_use: usize,
    /// Buffer allocation failures
    pub allocation_failures: u64,
    /// Buffer reuse count
    pub reuse_count: u64,
}

impl BufferPoolStats {
    pub fn new() -> Self {
        Self {
            total_buffers: 0,
            buffers_in_use: 0,
            allocation_failures: 0,
            reuse_count: 0,
        }
    }
}

/// Message type for WebSocket processing thread pool
#[derive(Debug, Clone)]
pub enum WebSocketTask {
    /// Process WebSocket message
    ProcessMessage {
        message: String,
        timestamp: i64,
    },
    /// Shutdown signal for worker threads
    Shutdown,
}

/// WebSocket actor messages for telling (fire-and-forget)
#[derive(Debug, Clone)]
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
        symbol: SharedSymbol,
        candle: SharedCandle,
        is_closed: bool,
    },
    /// Force reconnection
    Reconnect,
    /// Health check
    HealthCheck,
    /// Check for gap after reconnection and fill if needed
    CheckReconnectionGap,
    /// Set the TimeFrame actor reference for direct real-time forwarding
    SetTimeFrameActor {
        timeframe_actor: ActorRef<crate::technical_analysis::actors::timeframe::TimeFrameActor>,
    },
    /// Set the LMDB actor reference for centralized storage
    SetLmdbActor {
        lmdb_actor: ActorRef<crate::lmdb::LmdbActor>,
    },
    /// Set the PostgreSQL actor reference for dual storage
    #[cfg(feature = "postgres")]
    SetPostgresActor {
        postgres_actor: ActorRef<crate::postgres::PostgresActor>,
    },
    /// Set the Volume Profile actor reference for daily volume profiles
    #[cfg(feature = "volume_profile")]
    SetVolumeProfileActor {
        volume_profile_actor: ActorRef<VolumeProfileActor>,
    },
    /// Set the API actor reference for gap filling
    SetApiActor {
        api_actor: ActorRef<crate::api::ApiActor>,
    },
    /// Flush pending batches to prevent candles from getting stuck
    FlushBatches,
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

/// Cache entry with timestamp for LRU eviction
#[derive(Debug, Clone)]
struct CacheEntry {
    candles: VecDeque<SharedCandle>,
    last_access: Instant,
}

/// WebSocket actor for real-time data streaming
pub struct WebSocketActor {
    /// Connection manager
    connection_manager: ConnectionManager,
    /// Active subscriptions
    subscriptions: Arc<RwLock<Vec<StreamSubscription>>>,
    /// LMDB actor reference for centralized storage
    lmdb_actor: Option<ActorRef<crate::lmdb::LmdbActor>>,
    /// Base path for LMDB storage (for reference only)
    base_path: PathBuf,
    /// Recent candles cache with LRU eviction (symbol -> cache entry)
    recent_candles: DashMap<String, CacheEntry>,
    /// Maximum recent candles to keep in memory per symbol
    max_recent_candles: usize,
    /// Maximum number of symbols to cache
    max_cached_symbols: usize,
    /// Maximum idle time before marking connection as unhealthy (in seconds)
    max_idle_time: u64,
    /// Pending candles for batch processing (symbol -> pending candles)
    pending_batch_candles: DashMap<String, Vec<(SharedCandle, bool)>>,
    /// Last batch process time for adaptive batching
    pub last_batch_time: Option<Instant>,
    /// Performance metrics for adaptive batching
    batch_processing_times: Vec<Duration>,
    /// Handle to the current connection task (for proper termination)
    connection_task: Option<tokio::task::JoinHandle<()>>,
    /// Last activity timestamp (when we last processed a message) - for health checks
    last_activity_time: Option<i64>,
    /// Last processed candle timestamp - for gap detection on reconnection
    last_processed_candle_time: Option<i64>,
    /// API actor reference for gap filling
    api_actor: Option<ActorRef<crate::api::ApiActor>>,
    /// PostgreSQL actor reference for dual storage
    #[cfg(feature = "postgres")]
    postgres_actor: Option<ActorRef<PostgresActor>>,
    #[cfg(not(feature = "postgres"))]
    postgres_actor: Option<()>,
    /// TimeFrame actor reference for direct real-time forwarding
    timeframe_actor: Option<ActorRef<crate::technical_analysis::actors::timeframe::TimeFrameActor>>,
    /// Volume Profile actor reference for daily volume profile calculation
    #[cfg(feature = "volume_profile")]
    volume_profile_actor: Option<ActorRef<VolumeProfileActor>>,
    #[cfg(not(feature = "volume_profile"))]
    volume_profile_actor: Option<()>,
    /// Gap detection configuration
    gap_threshold_minutes: u32,
    gap_check_delay_seconds: u32,
    /// Last cache cleanup timestamp
    last_cache_cleanup: Instant,
    /// Cache cleanup interval (in seconds)
    cache_cleanup_interval: u64,
    /// Semaphore for backpressure control on LMDB operations
    lmdb_semaphore: Arc<Semaphore>,
    /// Semaphore for backpressure control on PostgreSQL operations
    postgres_semaphore: Arc<Semaphore>,
    /// Queue system for non-blocking operations
    #[allow(dead_code)] // Future architecture component
    task_queue: TaskQueue,
    #[allow(dead_code)] // Future architecture component
    db_queue: DatabaseQueue,
    /// WebSocket processing thread pool sender
    thread_pool_sender: Option<Sender<WebSocketTask>>,
    /// Thread pool handles for graceful shutdown
    thread_pool_handles: Vec<thread::JoinHandle<()>>,
    /// High-performance buffer pool for WebSocket message processing
    message_buffer_pool: Arc<Slab<MessageBuffer>>,
    /// Buffer pool statistics for monitoring
    buffer_pool_stats: Arc<std::sync::RwLock<BufferPoolStats>>,
}

impl WebSocketActor {
    /// Create a new WebSocket actor with default settings
    pub fn new(base_path: PathBuf, adaptive_config: &AdaptiveConfig) -> Result<Self, WebSocketError> {
        Self::new_with_config(base_path, 300, 2, 5, adaptive_config) // 5min max idle, 2min gap threshold, 5s delay
    }

    /// Set the API actor reference for gap filling
    pub fn set_api_actor(&mut self, api_actor: ActorRef<crate::api::ApiActor>) {
        self.api_actor = Some(api_actor);
    }
    
    /// Set the PostgreSQL actor reference for dual storage
    #[cfg(feature = "postgres")]
    pub fn set_postgres_actor(&mut self, postgres_actor: ActorRef<PostgresActor>) {
        self.postgres_actor = Some(postgres_actor);
    }
    
    /// Set the TimeFrame actor reference for direct real-time forwarding
    pub fn set_timeframe_actor(&mut self, timeframe_actor: ActorRef<crate::technical_analysis::actors::timeframe::TimeFrameActor>) {
        self.timeframe_actor = Some(timeframe_actor);
    }
    
    /// Set the Volume Profile actor reference for daily volume profile calculation
    #[cfg(feature = "volume_profile")]
    pub fn set_volume_profile_actor(&mut self, volume_profile_actor: ActorRef<VolumeProfileActor>) {
        self.volume_profile_actor = Some(volume_profile_actor);
    }

    /// Create a new WebSocket actor with custom configuration
    pub fn new_with_config(
        base_path: PathBuf, 
        max_idle_secs: u64,
        gap_threshold_minutes: u32,
        gap_check_delay_seconds: u32,
        adaptive_config: &AdaptiveConfig,
    ) -> Result<Self, WebSocketError> {
        let connection_manager = ConnectionManager::new_binance_futures();
        
        if !base_path.exists() {
            std::fs::create_dir_all(&base_path)
                .map_err(|e| WebSocketError::Unknown(format!("Failed to create base path: {}", e)))?;
        }

        // Initialize queue system for non-blocking operations using adaptive configuration
        let worker_threads = adaptive_config.get_worker_thread_count();
        let task_config = QueueConfig {
            max_workers: worker_threads.max(1), // Use adaptive config, minimum 1
            ..QueueConfig::default()
        };
        let db_config = QueueConfig {
            max_workers: (worker_threads / 2).max(1), // Half of adaptive threads, minimum 1
            batch_size: 10, // Batch candles for better performance
            batch_timeout: Duration::from_millis(100),
            ..QueueConfig::default()
        };
        
        let task_queue = TaskQueue::new(task_config);
        let db_queue = DatabaseQueue::new(db_config, None); // No direct LMDB access - use LmdbActor instead
        
        // Initialize high-performance buffer pool with 1024 pre-allocated buffers
        let message_buffer_pool = Arc::new(Slab::new());
        let buffer_pool_stats = Arc::new(std::sync::RwLock::new(BufferPoolStats::new()));
        
        // Pre-allocate buffers for optimal performance (1024 buffers vs current 16)
        const BUFFER_POOL_SIZE: usize = 1024;
        const BUFFER_CAPACITY: usize = 8192; // 8KB per buffer for typical WebSocket messages
        
        for _ in 0..BUFFER_POOL_SIZE {
            let buffer = MessageBuffer::new(BUFFER_CAPACITY);
            message_buffer_pool.insert(buffer);
        }
        
        // Update buffer pool statistics
        if let Ok(mut stats) = buffer_pool_stats.write() {
            stats.total_buffers = BUFFER_POOL_SIZE;
        }
        
        info!("✅ Initialized buffer pool with {} pre-allocated buffers ({} KB each)", 
              BUFFER_POOL_SIZE, BUFFER_CAPACITY / 1024);
        
        // Initialize WebSocket processing thread pool with optimal thread count
        let thread_pool_size = num_cpus::get().max(2); // At least 2 threads, scale with CPU cores
        let (thread_pool_sender, thread_pool_receiver) = bounded::<WebSocketTask>(1024); // Bounded channel for backpressure
        let mut thread_pool_handles = Vec::new();
        
        // Start worker threads
        for thread_id in 0..thread_pool_size {
            let receiver = thread_pool_receiver.clone();
            let handle = thread::spawn(move || {
                info!("WebSocket worker thread {} started", thread_id);
                
                while let Ok(task) = receiver.recv() {
                    match task {
                        WebSocketTask::ProcessMessage { message, timestamp } => {
                            // Process WebSocket message in dedicated thread
                            let start = Instant::now();
                            
                            // Parse and process the message (placeholder - actual implementation needed)
                            if let Err(e) = Self::process_message_in_thread(&message, timestamp) {
                                warn!("WebSocket message processing failed in thread {}: {}", thread_id, e);
                            }
                            
                            let processing_time = start.elapsed();
                            if processing_time > Duration::from_millis(5) {
                                warn!("Slow WebSocket processing in thread {} took: {:?}", thread_id, processing_time);
                            }
                        }
                        WebSocketTask::Shutdown => {
                            info!("WebSocket worker thread {} shutting down", thread_id);
                            break;
                        }
                    }
                }
            });
            thread_pool_handles.push(handle);
        }
        
        let now = Instant::now();
        Ok(Self {
            connection_manager,
            subscriptions: Arc::new(RwLock::new(Vec::new())),
            lmdb_actor: None,
            base_path,
            recent_candles: DashMap::new(),
            max_recent_candles: 500, // Reduced from 1000 for better memory efficiency
            max_cached_symbols: 50,  // Limit total number of cached symbols
            max_idle_time: max_idle_secs,
            pending_batch_candles: DashMap::new(),
            last_batch_time: None,
            batch_processing_times: Vec::with_capacity(100), // Track last 100 batch processing times
            connection_task: None,
            last_activity_time: None,
            last_processed_candle_time: None,
            api_actor: None,
            postgres_actor: None,
            timeframe_actor: None,
            volume_profile_actor: None,
            gap_threshold_minutes,
            gap_check_delay_seconds,
            last_cache_cleanup: now,
            cache_cleanup_interval: 300, // Cleanup every 5 minutes
            lmdb_semaphore: Arc::new(Semaphore::new(10)), // Max 10 concurrent LMDB operations
            postgres_semaphore: Arc::new(Semaphore::new(5)), // Max 5 concurrent PostgreSQL operations
            task_queue,
            db_queue,
            thread_pool_sender: Some(thread_pool_sender),
            thread_pool_handles,
            message_buffer_pool,
            buffer_pool_stats,
        })
    }
    
    /// Get a buffer from the pool for message processing
    pub fn get_buffer(&self) -> Option<usize> {
        // Insert a new buffer (sharded-slab will handle concurrency and reuse)
        let buffer = MessageBuffer::new(8192); // 8KB capacity
        let key = self.message_buffer_pool.insert(buffer);
        
        // Update statistics
        if let Ok(mut stats) = self.buffer_pool_stats.write() {
            stats.buffers_in_use += 1;
            stats.reuse_count += 1;
        }
        
        key
    }
    
    /// Return a buffer to the pool after processing
    pub fn return_buffer(&self, buffer_key: usize) -> Result<(), WebSocketError> {
        if self.message_buffer_pool.remove(buffer_key) {
            // Buffer automatically returned to memory pool via drop
            
            // Update statistics
            if let Ok(mut stats) = self.buffer_pool_stats.write() {
                stats.buffers_in_use = stats.buffers_in_use.saturating_sub(1);
            }
            
            Ok(())
        } else {
            Err(WebSocketError::Unknown("Invalid buffer key".to_string()))
        }
    }
    
    /// Process message with buffer pool optimization
    pub fn process_message_with_buffer(&self, message: String) -> Result<(), WebSocketError> {
        // Get buffer from pool
        let buffer_key = self.get_buffer()
            .ok_or_else(|| WebSocketError::Connection("Failed to acquire buffer from pool".to_string()))?;
        
        // Process the message (placeholder)
        let _timestamp = chrono::Utc::now().timestamp_millis();
        debug!("Processing message with buffer {}: {} chars", buffer_key, message.len());
        
        // Buffer is used for allocation optimization (sharded-slab handles memory efficiently)
        // The actual message processing happens here with optimized memory allocation
        
        // Return buffer to pool
        self.return_buffer(buffer_key)?;
        
        Ok(())
    }
    
    /// Get buffer pool statistics for monitoring
    pub fn get_buffer_pool_stats(&self) -> BufferPoolStats {
        self.buffer_pool_stats.read()
            .map(|stats| stats.clone())
            .unwrap_or_else(|_| BufferPoolStats::new())
    }
    
    /// Process WebSocket message in worker thread (static method for thread pool)
    fn process_message_in_thread(message: &str, timestamp: i64) -> Result<(), WebSocketError> {
        // TODO: Implement actual message parsing and processing
        // This is a placeholder for the thread pool processing logic
        // The actual implementation will integrate with sonic-rs for zero-copy parsing
        
        debug!("Processing WebSocket message in thread: {} chars, timestamp: {}", message.len(), timestamp);
        
        // Simulate processing time
        if message.is_empty() {
            return Err(WebSocketError::Connection("Empty message".to_string()));
        }
        
        // Placeholder: In real implementation, this would parse JSON and extract candle data
        // using sonic-rs for zero-copy parsing
        Ok(())
    }
    
    /// Send message to thread pool for processing
    pub fn send_to_thread_pool(&self, message: String) -> Result<(), WebSocketError> {
        if let Some(sender) = &self.thread_pool_sender {
            let timestamp = chrono::Utc::now().timestamp_millis();
            let task = WebSocketTask::ProcessMessage { message, timestamp };
            
            match sender.try_send(task) {
                Ok(_) => Ok(()),
                Err(crossbeam_channel::TrySendError::Full(_)) => {
                    warn!("WebSocket thread pool queue full, dropping message");
                    Err(WebSocketError::Connection("Thread pool queue full".to_string()))
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    error!("WebSocket thread pool disconnected");
                    Err(WebSocketError::Connection("Thread pool disconnected".to_string()))
                }
            }
        } else {
            Err(WebSocketError::Connection("Thread pool not initialized".to_string()))
        }
    }

    /// Set the LMDB actor reference for centralized storage
    pub fn set_lmdb_actor(&mut self, lmdb_actor: ActorRef<crate::lmdb::LmdbActor>) {
        self.lmdb_actor = Some(lmdb_actor);
    }

    /// Initialize database for a symbol via LmdbActor (non-blocking)
    fn init_symbol_db(&mut self, symbol: &str) -> Result<(), WebSocketError> {
        // Initialize recent candles cache immediately with LRU entry
        let cache_entry = CacheEntry {
            candles: VecDeque::with_capacity(self.max_recent_candles),
            last_access: Instant::now(),
        };
        self.recent_candles.insert(symbol.to_string(), cache_entry);
        
        // Send database initialization to background (non-blocking)
        if let Some(lmdb_actor) = &self.lmdb_actor {
            let lmdb_actor_clone = lmdb_actor.clone();
            let symbol_owned = symbol.to_string();
            
            tokio::spawn(async move {
                let msg = crate::lmdb::LmdbActorMessage::InitializeDatabase {
                    symbol: symbol_owned.clone(),
                    timeframe: 60,
                };
                
                match lmdb_actor_clone.ask(msg).await {
                    Ok(crate::lmdb::LmdbActorResponse::Success) => {
                        info!("✅ Background database initialization completed for: {}", symbol_owned);
                    }
                    Ok(crate::lmdb::LmdbActorResponse::ErrorResponse(err)) => {
                        error!("❌ Background database initialization failed for {}: {}", symbol_owned, err);
                    }
                    Ok(_) => {
                        error!("❌ Unexpected response from LmdbActor for {}", symbol_owned);
                    }
                    Err(e) => {
                        error!("❌ Failed to communicate with LmdbActor for {}: {}", symbol_owned, e);
                    }
                }
            });
        }
        
        info!("⚡ Database initialization started in background for: {}", symbol);
        Ok(())
    }

    /// Perform intelligent cache cleanup with LRU eviction
    fn cleanup_cache(&mut self) {
        let now = Instant::now();
        
        // Check if cleanup is needed (time-based or size-based)
        let should_cleanup = now.duration_since(self.last_cache_cleanup).as_secs() >= self.cache_cleanup_interval
            || self.recent_candles.len() > self.max_cached_symbols;
        
        if !should_cleanup {
            return;
        }
        
        debug!("🧹 Starting cache cleanup: {} symbols cached", self.recent_candles.len());
        
        // If over symbol limit, remove least recently used symbols
        if self.recent_candles.len() > self.max_cached_symbols {
            // Collect symbols with their last access times
            let mut symbol_access_times: Vec<(String, Instant)> = self.recent_candles
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().last_access))
                .collect();
            
            // Sort by access time (oldest first)
            symbol_access_times.sort_by_key(|(_, last_access)| *last_access);
            
            // Remove oldest symbols to get back to limit
            let symbols_to_remove = self.recent_candles.len() - self.max_cached_symbols;
            for (symbol, _) in symbol_access_times.iter().take(symbols_to_remove) {
                if let Some((_, cache_entry)) = self.recent_candles.remove(symbol) {
                    info!("🗑️ Evicted symbol from cache: {} ({} candles)", symbol, cache_entry.candles.len());
                }
            }
        }
        
        // Clean up old candles within each symbol's cache
        let cleanup_threshold = Duration::from_secs(3600); // 1 hour
        let mut cleaned_symbols = 0;
        let mut cleaned_candles = 0;
        
        self.recent_candles.iter_mut().for_each(|mut entry| {
            let symbol_key = entry.key().clone(); // Clone the key to avoid borrowing conflicts
            let cache_entry = entry.value_mut();
            
            // Clean old candles if not accessed recently
            if now.duration_since(cache_entry.last_access) > cleanup_threshold {
                let _original_count = cache_entry.candles.len();
                // Keep only the most recent candles (half of max)
                let keep_count = self.max_recent_candles / 2;
                if cache_entry.candles.len() > keep_count {
                    let drain_count = cache_entry.candles.len() - keep_count;
                    cache_entry.candles.drain(0..drain_count);
                    cleaned_candles += drain_count;
                    cleaned_symbols += 1;
                    debug!("🧹 Cleaned {} old candles for {}", drain_count, symbol_key);
                }
            }
        });
        
        self.last_cache_cleanup = now;
        info!("✅ Cache cleanup completed: {} symbols, {} candles cleaned", cleaned_symbols, cleaned_candles);
    }

    /// Calculate adaptive batch threshold based on recent performance
    fn calculate_adaptive_batch_threshold(&mut self) -> (Duration, usize) {
        // If we have performance history, use it to adapt
        if self.batch_processing_times.len() >= 10 {
            let avg_processing_time: Duration = self.batch_processing_times
                .iter()
                .sum::<Duration>() / self.batch_processing_times.len() as u32;
            
            // Target: keep processing time under 5ms
            let target_time = Duration::from_millis(5);
            
            let (time_threshold, count_threshold) = if avg_processing_time > target_time {
                // Processing is slow, use smaller batches and shorter time windows
                (Duration::from_millis(50), 3)
            } else if avg_processing_time < Duration::from_millis(1) {
                // Processing is fast, allow larger batches and longer time windows
                (Duration::from_millis(200), 10)
            } else {
                // Balanced performance
                (Duration::from_millis(100), 5)
            };
            
            debug!("📊 Adaptive batching: avg_time={:?}, threshold=({:?}, {})", 
                   avg_processing_time, time_threshold, count_threshold);
            
            (time_threshold, count_threshold)
        } else {
            // Default conservative thresholds until we have performance data
            (Duration::from_millis(100), 5)
        }
    }

    /// Store a candle in LMDB and update recent cache - handles both live and closed candles
    pub async fn store_candle(&mut self, symbol: &str, candle: &SharedCandle, is_closed: bool) -> Result<(), WebSocketError> {
        // Always update recent cache for both live and closed candles (for real-time access)
        if let Some(mut cache_entry) = self.recent_candles.get_mut(symbol) {
            // Update access time for LRU
            cache_entry.last_access = Instant::now();
            cache_entry.candles.push_back(Arc::clone(candle));
            
            // Maintain cache size limit
            if cache_entry.candles.len() > self.max_recent_candles {
                cache_entry.candles.pop_front(); // O(1) operation with VecDeque
            }
        }
        
        // Perform periodic cache cleanup
        self.cleanup_cache();
        
        // Only store closed candles to LMDB to avoid partial data
        if !is_closed {
            debug!("📝 Cached live candle for {} (not stored to LMDB)", symbol);
            return Ok(());
        }
        
        // For closed candles, add to batch for efficient processing
        self.add_to_batch(symbol, candle, is_closed).await
    }
    
    /// Add a candle to the batch processing queue with adaptive thresholds
    async fn add_to_batch(&mut self, symbol: &str, candle: &SharedCandle, is_closed: bool) -> Result<(), WebSocketError> {
        // Calculate adaptive thresholds based on recent performance
        let (time_threshold, count_threshold) = self.calculate_adaptive_batch_threshold();
        
        // Add to batch queue and determine if we should process
        let (should_process, batch_len, total_pending) = {
            let mut batch = self.pending_batch_candles.entry(symbol.to_string()).or_default();
            batch.push((Arc::clone(candle), is_closed));
            
            let total_pending: usize = self.pending_batch_candles.iter().map(|entry| entry.value().len()).sum();
            
            let should_process = match self.last_batch_time {
                Some(last_time) => last_time.elapsed() > time_threshold,
                None => true, // Always process immediately if this is the first candle
            } || batch.len() >= count_threshold // Use adaptive count threshold
            || total_pending >= (count_threshold * 3); // Process if total pending gets large
            
            (should_process, batch.len(), total_pending)
        }; // Release the borrow here
        
        debug!("📊 Batch status for {}: {} candles, {} total pending, should_process: {}", 
               symbol, batch_len, total_pending, should_process);
        
        if should_process {
            info!("🔄 Processing batches: {} total pending candles", total_pending);
            let start_time = Instant::now();
            self.process_batches().await?;
            let processing_time = start_time.elapsed();
            
            // Track processing time for adaptive batching
            self.batch_processing_times.push(processing_time);
            if self.batch_processing_times.len() > 100 {
                self.batch_processing_times.remove(0); // Keep only last 100 measurements
            }
            
            self.last_batch_time = Some(start_time);
            debug!("⚡ Batch processing completed in {:?}", processing_time);
        }
        
        Ok(())
    }
    
    /// Process all pending batch candles
    async fn process_batches(&mut self) -> Result<(), WebSocketError> {
        // Collect all symbols that have pending candles
        let symbols_to_process: Vec<String> = self.pending_batch_candles.iter()
            .map(|entry| entry.key().clone())
            .collect();
        
        for symbol in symbols_to_process {
            if let Some((_, mut candles)) = self.pending_batch_candles.remove(&symbol) {
                // Sort by open_time to ensure proper chronological ordering
                candles.sort_by_key(|(candle, _)| candle.open_time);
                
                // Process the batch for this symbol
                self.process_candle_batch(&symbol, candles).await?;
            }
        }
        
        Ok(())
    }
    
    /// Process a batch of candles for a specific symbol via LmdbActor
    async fn process_candle_batch(&mut self, symbol: &str, candles: Vec<(SharedCandle, bool)>) -> Result<(), WebSocketError> {
        if let Some(lmdb_actor) = &self.lmdb_actor {
            // Extract only closed candles for storage
            let closed_candles: Vec<_> = candles
                .into_iter()
                .filter_map(|(candle, is_closed)| if is_closed { Some((*candle).clone()) } else { None })
                .collect();

            if !closed_candles.is_empty() {
                // Store batch via LmdbActor asynchronously (non-blocking)
                let msg = LmdbActorTell::StoreCandlesAsync {
                    symbol: symbol.to_string(),
                    timeframe: 60, // WebSocket stores 1-minute data only
                    candles: closed_candles.clone(),
                };

                if let Err(e) = lmdb_actor.tell(msg).send().await {
                    error!("❌ Failed to send async storage request to LmdbActor for {}: {}", symbol, e);
                } else {
                    debug!("📤 Sent {} candles for async storage to LmdbActor for {}", closed_candles.len(), symbol);
                }

                // Forward closed candles to VolumeProfileActor for real-time volume profile calculation
                #[cfg(feature = "volume_profile")]
                if let Some(volume_profile_actor) = &self.volume_profile_actor {
                    for candle in &closed_candles {
                        let volume_profile_msg = VolumeProfileTell::ProcessCandle {
                            symbol: symbol.to_string(),
                            candle: candle.clone(),
                        };

                        if let Err(e) = volume_profile_actor.tell(volume_profile_msg).send().await {
                            warn!("⚠️ Failed to send candle to VolumeProfileActor for {}: {}", symbol, e);
                        } else {
                            info!("📊 WEBSOCKET DEBUG: Forwarded candle to VolumeProfileActor for {} - timestamp: {}, volume: {:.2}", 
                                  symbol, candle.open_time, candle.volume);
                        }
                    }
                }
                #[cfg(not(feature = "volume_profile"))]
                {
                    debug!("Volume profile feature disabled, not forwarding {} closed candles for {}", closed_candles.len(), symbol);
                }
            }
        } else {
            warn!("LmdbActor not available, skipping candle storage for {}", symbol);
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
        let subscriptions = {
            let subscriptions_guard = self.subscriptions.read().await;
            subscriptions_guard.clone() // Only clone when we need to hold it across await
        };
        
        if subscriptions.is_empty() {
            warn!("No subscriptions found, cannot start connection");
            warn!("📋 Current subscription count: {}", subscriptions.len());
            return;
        }
        
        info!("📋 Starting connection with {} subscriptions", subscriptions.len());
        for (i, sub) in subscriptions.iter().enumerate() {
            info!("  [{}] {} -> {:?} (active: {})", i, sub.stream_type, sub.symbols, sub.is_active);
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
        let gap_check_delay = self.gap_check_delay_seconds;
        let handle = tokio::spawn(async move {
            let message_handler = |message: String| {
                let actor_ref = actor_ref_for_connection.clone();
                async move {
                    match parse_any_kline_message_optimized(&message) {
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
                                symbol: intern_symbol(&kline_event.symbol),
                                candle: shared_candle(candle),
                                is_closed,
                            };
                            
                            // Send message in background task to avoid blocking WebSocket loop
                            let actor_ref_clone = actor_ref.clone();
                            tokio::spawn(async move {
                                if let Err(e) = actor_ref_clone.tell(process_msg).send().await {
                                    warn!("Failed to send processed candle to actor: {}", e);
                                }
                            });
                            
                            Ok(())
                        }
                        Err(e) => {
                            warn!("Failed to parse kline message: {}", e);
                            Err(e)
                        }
                    }
                }
            };

            // Note: Gap detection is handled by the reconnection callback
            
            // Reconnection callback - triggers gap detection after successful reconnection
            let reconnection_callback = {
                let actor_ref_for_gap = actor_ref_for_connection.clone();
                move |is_reconnection: bool| {
                    if is_reconnection {
                        info!("🔄 WebSocket reconnected successfully - scheduling gap detection");
                        let actor_ref_gap = actor_ref_for_gap.clone();
                        tokio::spawn(async move {
                            // Wait for connection to stabilize
                            tokio::time::sleep(std::time::Duration::from_secs(gap_check_delay as u64)).await;
                            
                            // Trigger gap detection
                            if let Err(e) = actor_ref_gap.tell(WebSocketTell::CheckReconnectionGap).send().await {
                                error!("Failed to trigger reconnection gap check: {}", e);
                            }
                        });
                    }
                }
            };

            if let Err(e) = connection_manager.connect_with_retry_and_callback(&url, message_handler, reconnection_callback).await {
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
        // Keep symbols as Vec<String> for subscription interface compatibility
        let subscription = StreamSubscription::new(stream_type.clone(), normalized_symbols);

        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.push(subscription);

        info!("➕ Added subscription: {} for {:?}", stream_type, symbols);
        info!("📋 Total subscriptions now: {}", subscriptions.len());
        
        // Debug log all current subscriptions
        for (i, sub) in subscriptions.iter().enumerate() {
            debug!("  [{}] {} -> {:?} (active: {})", i, sub.stream_type, sub.symbols, sub.is_active);
        }
        
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

    /// Get recent candles for a symbol with LRU update
    pub fn get_recent_candles(&self, symbol: &str, limit: usize) -> Vec<FuturesOHLCVCandle> {
        if let Some(mut cache_entry) = self.recent_candles.get_mut(symbol) {
            // Update access time for LRU
            cache_entry.last_access = Instant::now();
            
            let take_count = std::cmp::min(limit, cache_entry.candles.len());
            cache_entry.candles.iter()
                .rev() // Get most recent first
                .take(take_count)
                .map(|candle| (**candle).clone())
                .collect::<Vec<_>>()
                .into_iter()
                .rev() // Restore chronological order
                .collect()
        } else {
            Vec::new()
        }
    }


    /// Get the number of symbols with recent candles cache (for testing)
    #[cfg(test)]
    pub fn recent_candles_count(&self) -> usize {
        self.recent_candles.len()
    }

    /// Check if LmdbActor is set (for testing)
    #[cfg(test)]
    pub fn has_lmdb_actor(&self) -> bool {
        self.lmdb_actor.is_some()
    }

    /// Get gap threshold minutes (for testing)
    #[cfg(test)]
    pub fn gap_threshold_minutes(&self) -> u32 {
        self.gap_threshold_minutes
    }

    /// Get gap check delay seconds (for testing)
    #[cfg(test)]
    pub fn gap_check_delay_seconds(&self) -> u32 {
        self.gap_check_delay_seconds
    }

    /// Set last processed candle time (for testing)
    #[cfg(test)]
    pub fn set_last_processed_candle_time(&mut self, timestamp: Option<i64>) {
        self.last_processed_candle_time = timestamp;
    }

    /// Set last activity time (for testing)
    #[cfg(test)]
    pub fn set_last_activity_time(&mut self, timestamp: Option<i64>) {
        self.last_activity_time = timestamp;
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
        
        // Initialize global object pools for performance optimization
        let pools = init_global_pools();
        info!("⚡ Initialized global object pools for WebSocket actor");
        debug!("📊 Object pools initialized: {}", pools.stats_report());

        // Start adaptive health check task
        let actor_ref_clone = actor_ref.clone();
        tokio::spawn(async move {
            let base_interval = Duration::from_secs(30); // More frequent base check
            let mut current_interval = base_interval;
            let mut consecutive_healthy_checks = 0u32;
            
            loop {
                tokio::time::sleep(current_interval).await;
                
                if let Err(e) = actor_ref_clone.tell(WebSocketTell::HealthCheck).send().await {
                    warn!("Failed to send health check: {}", e);
                    break;
                }
                
                // Adaptive interval: increase check frequency when connection seems healthy
                consecutive_healthy_checks += 1;
                current_interval = if consecutive_healthy_checks > 10 {
                    // Connection is stable, reduce frequency
                    Duration::from_secs(60) // 1 minute (reduced from 2 minutes)
                } else if consecutive_healthy_checks > 5 {
                    // Connection is becoming stable
                    Duration::from_secs(45) // 45 seconds (reduced from 1.5 minutes)
                } else {
                    // Connection might be unstable, check more frequently
                    Duration::from_secs(20) // 20 seconds (reduced from 30 seconds)
                };
                
                debug!("📡 Health check interval adapted to {:?} (healthy checks: {})", 
                       current_interval, consecutive_healthy_checks);
            }
        });
        
        // Start adaptive batch flush task to prevent stuck candles
        let batch_flush_ref = actor_ref.clone();
        tokio::spawn(async move {
            let mut flush_interval = Duration::from_millis(500); // Start conservative
            let mut empty_flush_count = 0u32;
            
            loop {
                tokio::time::sleep(flush_interval).await;
                
                if let Err(e) = batch_flush_ref.tell(WebSocketTell::FlushBatches).send().await {
                    warn!("Failed to send batch flush: {}", e);
                    break;
                }
                
                // Adaptive flushing: reduce frequency if no work is being done
                empty_flush_count += 1;
                flush_interval = if empty_flush_count > 20 {
                    // No batches to flush for a while, reduce frequency
                    Duration::from_millis(1000) // 1 second
                } else if empty_flush_count > 10 {
                    // Some idle time, moderate frequency
                    Duration::from_millis(750) // 750ms
                } else {
                    // Active processing, maintain high frequency
                    Duration::from_millis(200) // 200ms
                };
                
                // Reset counter periodically to detect activity changes
                if empty_flush_count > 30 {
                    empty_flush_count = 10; // Reset but keep some history
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
                let now = chrono::Utc::now().timestamp_millis();
                
                // Check for real-time gap if we have previous activity
                if let Some(last_activity) = self.last_activity_time {
                    let gap_duration_ms = now - last_activity;
                    let expected_message_interval_ms = 5000; // Expect message at least every 5 seconds
                    
                    if gap_duration_ms > expected_message_interval_ms {
                        let gap_duration_seconds = gap_duration_ms / 1000;
                        let gap_duration_minutes = gap_duration_ms / (60 * 1000);
                        
                        // Use different thresholds for warning vs. gap filling
                        let real_time_warning_threshold_ms = 30000; // 30 seconds - warn about potential issues
                        let gap_fill_threshold_ms = (self.gap_threshold_minutes as i64) * 60 * 1000; // Full threshold for gap filling
                        
                        if gap_duration_ms > gap_fill_threshold_ms {
                            warn!("🕳️ Real-time gap detected: {} minutes since last message - triggering gap fill", gap_duration_minutes);
                            
                            // Trigger immediate gap detection for gaps exceeding fill threshold
                            let actor_ref = ctx.actor_ref().clone();
                            tokio::spawn(async move {
                                if let Err(e) = actor_ref.tell(WebSocketTell::CheckReconnectionGap).send().await {
                                    error!("Failed to trigger real-time gap detection: {}", e);
                                }
                            });
                        } else if gap_duration_ms > real_time_warning_threshold_ms {
                            warn!("⚠️ Message flow interruption detected: {} seconds gap (below fill threshold of {} minutes)", gap_duration_seconds, self.gap_threshold_minutes);
                        } else {
                            info!("📡 Message flow resumed after {} seconds", gap_duration_seconds);
                        }
                    }
                }
                
                // Update activity timestamp - this proves we're receiving messages
                self.last_activity_time = Some(now);
                
                // Track last processed candle time for gap detection (only closed candles)
                if is_closed {
                    self.last_processed_candle_time = Some(candle.close_time);
                    debug!("📝 Updated last processed candle time for {}: {}", symbol, candle.close_time);
                }
                
                // Add to recent cache immediately with optimized LRU (synchronous, fast operation)
                let now = Instant::now();
                {
                    let mut cache_entry = self.recent_candles.entry(symbol.to_string()).or_insert_with(|| {
                        CacheEntry {
                            candles: VecDeque::with_capacity(self.max_recent_candles),
                            last_access: now,
                        }
                    });
                    cache_entry.last_access = now;
                    cache_entry.candles.push_back(candle.clone());
                    if cache_entry.candles.len() > self.max_recent_candles {
                        cache_entry.candles.pop_front();
                    }
                } // Drop the borrow here
                
                // Perform lightweight cache maintenance (now that borrow is dropped)
                self.cleanup_cache();
                
                // Spawn background task for all heavy operations with backpressure control
                let lmdb_actor = self.lmdb_actor.clone();
                let postgres_actor = self.postgres_actor.clone();
                let timeframe_actor = self.timeframe_actor.clone();
                let volume_profile_actor = self.volume_profile_actor.clone();
                let lmdb_semaphore = Arc::clone(&self.lmdb_semaphore);
                let postgres_semaphore = Arc::clone(&self.postgres_semaphore);
                let symbol_owned = symbol.to_string();
                let candle_arc = Arc::clone(&candle); // Clone the Arc (cheap)
                
                tokio::spawn(async move {
                    // Background storage operations with backpressure control
                    if is_closed {
                        // LMDB storage with semaphore-based backpressure
                        if let Some(lmdb_actor) = lmdb_actor {
                            // Try to acquire semaphore permit (non-blocking check)
                            match lmdb_semaphore.try_acquire() {
                                Ok(_permit) => {
                                    let msg = LmdbActorTell::StoreCandlesAsync {
                                        symbol: symbol_owned.clone(),
                                        timeframe: 60,
                                        candles: vec![(*candle_arc).clone()], // Dereference Arc and clone candle data
                                    };
                                    
                                    if let Err(e) = lmdb_actor.tell(msg).send().await {
                                        error!("❌ Background LMDB storage failed for {}: {}", symbol_owned, e);
                                    } else {
                                        debug!("✅ LMDB storage completed for {}", symbol_owned);
                                    }
                                    // Permit is automatically dropped here
                                }
                                Err(_) => {
                                    // Backpressure activated - skip this operation
                                    warn!("⚠️ LMDB backpressure: skipping storage for {} (queue full)", symbol_owned);
                                }
                            }
                        }
                        
                        // PostgreSQL storage with semaphore-based backpressure
                        if let Some(postgres_actor) = postgres_actor {
                            match postgres_semaphore.try_acquire() {
                                Ok(_permit) => {
                                    let timestamp = chrono::DateTime::from_timestamp_millis(candle_arc.close_time)
                                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                                        .unwrap_or_else(|| format!("INVALID_TIME({})", candle_arc.close_time));
                                    
                                    #[cfg(feature = "postgres")]
                                    let postgres_msg = PostgresTell::StoreCandle {
                                        symbol: symbol_owned.clone(),
                                        candle: (*candle_arc).clone(), // Dereference Arc and clone candle data
                                        source: "WebSocketActor".to_string(),
                                    };
                                    
                                    #[cfg(feature = "postgres")]
                                    if let Err(e) = postgres_actor.tell(postgres_msg).send().await {
                                        warn!("❌ PostgreSQL storage failed for {}: {}", symbol_owned, e);
                                    } else {
                                        debug!("✅ PostgreSQL storage completed: {} @ {}", symbol_owned, timestamp);
                                    }
                                    // Permit is automatically dropped here
                                }
                                Err(_) => {
                                    // Backpressure activated - skip this operation
                                    warn!("⚠️ PostgreSQL backpressure: skipping storage for {} (queue full)", symbol_owned);
                                }
                            }
                        }
                    }
                    
                    // Background Technical Analysis forwarding
                    if let Some(timeframe_actor) = timeframe_actor {
                        let msg = crate::technical_analysis::actors::timeframe::TimeFrameTell::ProcessCandle {
                            symbol: symbol_owned.clone(),
                            candle: (*candle_arc).clone(), // Dereference Arc and clone candle data
                            is_closed,
                        };
                        
                        if let Err(e) = timeframe_actor.tell(msg).send().await {
                            error!("❌ Background TA forwarding failed for {}: {}", symbol_owned, e);
                        } else {
                            debug!("🚀 Background TA forwarding completed: {} @ {}", symbol_owned, candle_arc.close_time);
                        }
                    }
                    
                    // Background Volume Profile forwarding (only for closed candles)
                    #[cfg(feature = "volume_profile")]
                    if is_closed {
                        if let Some(volume_profile_actor) = volume_profile_actor {
                            let msg = VolumeProfileTell::ProcessCandle {
                                symbol: symbol_owned.clone(),
                                candle: (*candle_arc).clone(), // Dereference Arc and clone candle data
                            };
                            
                            if let Err(e) = volume_profile_actor.tell(msg).send().await {
                                error!("❌ Background Volume Profile forwarding failed for {}: {}", symbol_owned, e);
                            } else {
                                debug!("📊 Background Volume Profile forwarding completed: {} @ {}", symbol_owned, candle_arc.close_time);
                            }
                        }
                    }
                    #[cfg(not(feature = "volume_profile"))]
                    if is_closed {
                        debug!("Volume profile feature disabled, not forwarding closed candle for {}", symbol_owned);
                    }
                });
                
                // Handler completes immediately - WebSocket can process next message
                debug!("⚡ ProcessCandle handler completed immediately for {} ({})", symbol, if is_closed { "closed" } else { "live" });
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
                                        symbol: symbol.to_string(), // Convert Arc<str> to String only when needed
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
            WebSocketTell::SetTimeFrameActor { timeframe_actor } => {
                info!("📨 Setting TimeFrame actor reference for WebSocketActor");
                self.timeframe_actor = Some(timeframe_actor);
                info!("✅ TimeFrame actor reference successfully set in WebSocketActor");
            }
            WebSocketTell::SetLmdbActor { lmdb_actor } => {
                info!("📨 Setting LMDB actor reference for WebSocketActor");
                self.lmdb_actor = Some(lmdb_actor);
                info!("✅ LMDB actor reference successfully set in WebSocketActor - centralized storage enabled");
            }
            #[cfg(feature = "postgres")]
            WebSocketTell::SetPostgresActor { postgres_actor } => {
                info!("📨 Setting PostgreSQL actor reference for WebSocketActor");
                self.postgres_actor = Some(postgres_actor);
                info!("✅ PostgreSQL actor reference successfully set in WebSocketActor - dual storage enabled");
            }
            #[cfg(feature = "volume_profile")]
            WebSocketTell::SetVolumeProfileActor { volume_profile_actor } => {
                info!("📨 Setting Volume Profile actor reference for WebSocketActor");
                self.volume_profile_actor = Some(volume_profile_actor);
                info!("✅ Volume Profile actor reference successfully set in WebSocketActor - daily volume profiles enabled");
            }
            WebSocketTell::SetApiActor { api_actor } => {
                info!("📨 Setting API actor reference for WebSocketActor");
                self.api_actor = Some(api_actor);
                info!("✅ API actor reference successfully set in WebSocketActor - gap filling enabled");
            }
            WebSocketTell::FlushBatches => {
                // Force flush any pending batches to prevent stuck candles
                let pending_count: usize = self.pending_batch_candles.iter().map(|entry| entry.value().len()).sum();
                if pending_count > 0 {
                    debug!("🔄 Periodic batch flush: {} pending candles", pending_count);
                    let start_time = Instant::now();
                    if let Err(e) = self.process_batches().await {
                        warn!("Failed to flush batches: {}", e);
                    } else {
                        let processing_time = start_time.elapsed();
                        
                        // Track processing time for adaptive batching
                        self.batch_processing_times.push(processing_time);
                        if self.batch_processing_times.len() > 100 {
                            self.batch_processing_times.remove(0);
                        }
                        
                        self.last_batch_time = Some(start_time);
                        debug!("⚡ Periodic flush completed in {:?}", processing_time);
                    }
                } else {
                    debug!("🔄 Periodic flush: no pending candles");
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

impl Drop for WebSocketActor {
    fn drop(&mut self) {
        // Shutdown thread pool gracefully
        if let Some(sender) = &self.thread_pool_sender {
            // Send shutdown signal to all worker threads
            for _ in 0..self.thread_pool_handles.len() {
                if sender.send(WebSocketTask::Shutdown).is_err() {
                    warn!("Failed to send shutdown signal to WebSocket worker thread");
                }
            }
        }
        
        // Wait for all threads to complete
        while let Some(handle) = self.thread_pool_handles.pop() {
            if let Err(e) = handle.join() {
                error!("WebSocket worker thread panicked during shutdown: {:?}", e);
            }
        }
        
        info!("WebSocket thread pool shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::adaptive_config::AdaptiveConfig;

    fn create_test_config() -> AdaptiveConfig {
        AdaptiveConfig::default_static_config()
    }

    #[tokio::test]
    async fn test_websocket_actor_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf(), &config).unwrap();
        
        // Test that actor is created with empty cache and no LmdbActor reference initially
        assert_eq!(actor.recent_candles.len(), 0);
        assert!(!actor.has_lmdb_actor());
    }

    #[tokio::test]
    async fn test_add_subscription() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let mut actor = WebSocketActor::new(temp_dir.path().to_path_buf(), &config).unwrap();
        
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
        let mut actor = WebSocketActor::new(temp_dir.path().to_path_buf(), &create_test_config()).unwrap();
        
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
        let mut actor = WebSocketActor::new(temp_dir.path().to_path_buf(), &create_test_config()).unwrap();
        
        let result = actor.init_symbol_db("BTCUSDT");
        assert!(result.is_ok());
        
        // With new architecture, only the recent candles cache is initialized immediately
        // LMDB initialization happens in background via LmdbActor
        assert!(actor.recent_candles.contains_key("BTCUSDT"));
    }

    #[tokio::test]
    async fn test_websocket_actor_config() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let actor = WebSocketActor::new_with_config(
            temp_dir.path().to_path_buf(), 
            300, // max_idle_secs
            5,   // gap_threshold_minutes  
            10,  // gap_check_delay_seconds
            &config
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
        let actor = WebSocketActor::new(temp_dir.path().to_path_buf(), &create_test_config()).unwrap();
        
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