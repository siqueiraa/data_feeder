use std::collections::HashMap;
use std::sync::Arc;

use chrono::{NaiveDate, Utc};
use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
// use kameo::request::MessageSend; // Removed - no longer needed after Kafka publishing removal
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::historical::structs::FuturesOHLCVCandle;
use crate::lmdb::{LmdbActor, LmdbActorMessage, LmdbActorResponse};
use crate::postgres::PostgresActor;
// Kafka publishing is now handled by IndicatorActor to avoid duplicate messages
// use crate::kafka::{KafkaActor, KafkaTell};
// use crate::technical_analysis::structs::IndicatorOutput;

use super::calculator::{DailyVolumeProfile, VolumeProfileStatistics};
use super::structs::{VolumeProfileConfig, VolumeProfileData, UpdateFrequency};

/// Serializable Volume Profile Actor messages for telling (fire-and-forget)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeProfileTellSerializable {
    /// Process new 1-minute candle (real-time)
    ProcessCandle {
        symbol: String,
        candle: FuturesOHLCVCandle,
    },
    /// Rebuild volume profile for specific date (startup/recovery)
    RebuildDay {
        symbol: String,
        date: NaiveDate,
    },
    /// Health check message
    HealthCheck,
    /// Trigger batch processing of pending updates
    ProcessBatch,
}

/// All Volume Profile Actor messages for telling (including non-serializable ones)
#[derive(Debug, Clone)]
pub enum VolumeProfileTell {
    /// Process new 1-minute candle (real-time)
    ProcessCandle {
        symbol: String,
        candle: FuturesOHLCVCandle,
    },
    /// Rebuild volume profile for specific date (startup/recovery)
    RebuildDay {
        symbol: String,
        date: NaiveDate,
    },
    /// Health check message
    HealthCheck,
    /// Trigger batch processing of pending updates
    ProcessBatch,
    /// Set actor references for PostgreSQL and LMDB access (not serializable)
    SetActorReferences {
        postgres_actor: Option<ActorRef<PostgresActor>>,
        lmdb_actor: ActorRef<LmdbActor>,
    },
}

// Conversion from serializable to full enum
impl From<VolumeProfileTellSerializable> for VolumeProfileTell {
    fn from(serializable: VolumeProfileTellSerializable) -> Self {
        match serializable {
            VolumeProfileTellSerializable::ProcessCandle { symbol, candle } => {
                VolumeProfileTell::ProcessCandle { symbol, candle }
            }
            VolumeProfileTellSerializable::RebuildDay { symbol, date } => {
                VolumeProfileTell::RebuildDay { symbol, date }
            }
            VolumeProfileTellSerializable::HealthCheck => VolumeProfileTell::HealthCheck,
            VolumeProfileTellSerializable::ProcessBatch => VolumeProfileTell::ProcessBatch,
        }
    }
}

/// Volume Profile Actor messages for asking (request-response)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeProfileAsk {
    /// Get current volume profile for symbol/date
    GetVolumeProfile {
        symbol: String,
        date: NaiveDate,
    },
    /// Get health status
    GetHealthStatus,
    /// Get performance statistics
    GetStatistics,
}

/// Volume Profile Actor responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeProfileReply {
    /// Volume profile data response
    VolumeProfile(Option<VolumeProfileData>),
    /// Health status response
    HealthStatus {
        is_healthy: bool,
        profiles_count: usize,
        last_error: Option<String>,
        memory_usage_mb: f64,
    },
    /// Statistics response
    Statistics {
        profiles: Vec<VolumeProfileStatistics>,
        total_candles_processed: u64,
        total_database_writes: u64,
        // Kafka publishing removed - now handled by IndicatorActor
        // total_kafka_publishes: u64,
    },
    /// Success confirmation
    Success,
    /// Error response
    Error(String),
}

/// Volume Profile Actor for calculating and managing daily volume profiles
pub struct VolumeProfileActor {
    /// Configuration
    config: VolumeProfileConfig,
    /// Active volume profiles (one per symbol-date combination)
    profiles: HashMap<ProfileKey, DailyVolumeProfile>,
    /// Reference to PostgreSQL actor
    postgres_actor: Option<ActorRef<PostgresActor>>,
    /// Kafka publishing removed - now handled by IndicatorActor to avoid duplicate messages
    // kafka_actor: Option<ActorRef<KafkaActor>>,
    /// Reference to LMDB actor for historical data
    lmdb_actor: Option<ActorRef<LmdbActor>>,
    /// Health status
    is_healthy: bool,
    /// Last error message
    last_error: Option<Arc<String>>,
    /// Performance metrics
    total_candles_processed: u64,
    total_database_writes: u64,
    // Kafka publishing removed - now handled by IndicatorActor
    // total_kafka_publishes: u64,
    /// Batch processing queue for UpdateFrequency::Batched mode
    batch_queue: Vec<(String, FuturesOHLCVCandle)>,
    /// Last batch processing time
    last_batch_time: Option<std::time::Instant>,
}

/// Key for identifying unique volume profiles (symbol + date)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProfileKey {
    symbol: String,
    date: NaiveDate,
}

impl ProfileKey {
    fn new(symbol: String, date: NaiveDate) -> Self {
        Self { symbol, date }
    }

    fn from_candle(symbol: String, candle: &FuturesOHLCVCandle) -> Option<Self> {
        // Convert candle timestamp to date
        if let Some(datetime) = chrono::DateTime::from_timestamp_millis(candle.open_time) {
            let date = datetime.date_naive();
            Some(Self::new(symbol, date))
        } else {
            None
        }
    }
}

impl VolumeProfileActor {
    /// Create a new Volume Profile Actor
    pub fn new(config: VolumeProfileConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            config,
            profiles: HashMap::new(),
            postgres_actor: None,
            // kafka_actor: None, // Removed - now handled by IndicatorActor
            lmdb_actor: None,
            is_healthy: false,
            last_error: None,
            total_candles_processed: 0,
            total_database_writes: 0,
            // total_kafka_publishes: 0, // Removed - now handled by IndicatorActor
            batch_queue: Vec::new(),
            last_batch_time: None,
        })
    }

    /// Set actor references after creation
    pub fn set_actors(
        &mut self,
        postgres_actor: Option<ActorRef<PostgresActor>>,
        // kafka_actor: Option<ActorRef<KafkaActor>>, // Removed - now handled by IndicatorActor
        lmdb_actor: Option<ActorRef<LmdbActor>>,
    ) {
        self.postgres_actor = postgres_actor;
        // self.kafka_actor = kafka_actor; // Removed - now handled by IndicatorActor
        self.lmdb_actor = lmdb_actor;
        self.is_healthy = true;
        info!("VolumeProfileActor actor references set");
    }

    /// Process new 1-minute candle
    async fn process_candle(&mut self, symbol: String, candle: FuturesOHLCVCandle) -> Result<(), String> {
        debug!("Processing candle for volume profile: {} at {}", symbol, candle.open_time);

        if !self.config.enabled {
            return Ok(());
        }

        // Determine processing mode
        match self.config.update_frequency {
            UpdateFrequency::EveryCandle => {
                self.process_candle_immediate(symbol, candle).await
            }
            UpdateFrequency::Batched => {
                self.process_candle_batched(symbol, candle).await
            }
        }
    }

    /// Process candle immediately (real-time mode)
    async fn process_candle_immediate(&mut self, symbol: String, candle: FuturesOHLCVCandle) -> Result<(), String> {
        // Get or create profile for this symbol-date
        let profile_key = ProfileKey::from_candle(symbol.clone(), &candle)
            .ok_or_else(|| "Invalid candle timestamp".to_string())?;

        // Ensure profile exists - CRITICAL FIX: Initialize with historical data first
        if !self.profiles.contains_key(&profile_key) {
            info!("🔄 NEW DAILY PROFILE: Creating volume profile for {} on {} - rebuilding historical data first", 
                  profile_key.symbol, profile_key.date);
            
            // CRITICAL FIX: Rebuild historical data for the day BEFORE creating empty profile
            if let Err(e) = self.rebuild_day(profile_key.symbol.clone(), profile_key.date).await {
                warn!("⚠️ Failed to rebuild historical data for {} on {}: {}. Creating empty profile.", 
                      profile_key.symbol, profile_key.date, e);
                
                // Fallback: create empty profile if historical rebuild fails
                let profile = DailyVolumeProfile::new(
                    profile_key.symbol.clone(),
                    profile_key.date,
                    &self.config,
                );
                self.profiles.insert(profile_key.clone(), profile);
                info!("📊 Created fallback empty volume profile for {} on {}", profile_key.symbol, profile_key.date);
            } else {
                info!("✅ HISTORICAL REBUILD: Successfully initialized {} on {} with historical data", 
                      profile_key.symbol, profile_key.date);
            }
        }

        // Update profile with new candle and get profile data
        let profile_data = if let Some(profile) = self.profiles.get_mut(&profile_key) {
            profile.add_candle(&candle);
            self.total_candles_processed += 1;

            // Get updated profile data (clone to avoid borrow issues)
            let profile_data = profile.get_profile_data();
            
            debug!("Volume profile updated for {} on {}: {} candles, {:.2} total volume", 
                   profile_key.symbol, profile_key.date, profile.candle_count, profile.total_volume);
                   
            Some(profile_data)
        } else {
            None
        };

        // Handle storage and publishing outside the mutable borrow
        if let Some(profile_data) = profile_data {
            // Store in database
            if let Err(e) = self.store_profile_in_database(&profile_key, &profile_data).await {
                warn!("Failed to store volume profile in database: {}", e);
            } else {
                self.total_database_writes += 1;
            }

            // Kafka publishing removed - now handled by IndicatorActor to avoid duplicate messages
            // Volume profile data will be included in main technical analysis messages
        }

        Ok(())
    }

    /// Process candle in batched mode
    async fn process_candle_batched(&mut self, symbol: String, candle: FuturesOHLCVCandle) -> Result<(), String> {
        // Add to batch queue
        self.batch_queue.push((symbol, candle));

        if self.last_batch_time.is_none() {
            self.last_batch_time = Some(std::time::Instant::now());
        }

        // Check if we should process batch
        let should_process = self.batch_queue.len() >= self.config.batch_size
            || self.last_batch_time
                .map(|t| t.elapsed().as_secs() >= 60) // Process batch every minute
                .unwrap_or(false);

        if should_process {
            self.process_batch().await?;
        }

        Ok(())
    }

    /// Process batch of candles
    async fn process_batch(&mut self) -> Result<(), String> {
        if self.batch_queue.is_empty() {
            return Ok(());
        }

        info!("Processing batch of {} candles for volume profiles", self.batch_queue.len());

        let batch = std::mem::take(&mut self.batch_queue);
        self.last_batch_time = None;

        // Group candles by profile key
        let mut grouped_candles: HashMap<ProfileKey, Vec<FuturesOHLCVCandle>> = HashMap::new();

        for (symbol, candle) in batch {
            if let Some(profile_key) = ProfileKey::from_candle(symbol, &candle) {
                grouped_candles.entry(profile_key).or_default().push(candle);
            }
        }

        // Process each group
        for (profile_key, candles) in grouped_candles {
            // Ensure profile exists
            if !self.profiles.contains_key(&profile_key) {
                let profile = DailyVolumeProfile::new(
                    profile_key.symbol.clone(),
                    profile_key.date,
                    &self.config,
                );
                self.profiles.insert(profile_key.clone(), profile);
            }

            // Update profile with all candles
            if let Some(profile) = self.profiles.get_mut(&profile_key) {
                for candle in &candles {
                    profile.add_candle(candle);
                    self.total_candles_processed += 1;
                }

                // Get updated profile data
                let profile_data = profile.get_profile_data();

                // Store in database
                if let Err(e) = self.store_profile_in_database(&profile_key, &profile_data).await {
                    warn!("Failed to store volume profile in database: {}", e);
                } else {
                    self.total_database_writes += 1;
                }

                // Kafka publishing removed - now handled by IndicatorActor to avoid duplicate messages
                // Volume profile data will be included in main technical analysis messages

                info!("Batch processed for {} on {}: {} candles added", 
                      profile_key.symbol, profile_key.date, candles.len());
            }
        }

        Ok(())
    }

    /// Rebuild volume profile for specific day from LMDB data
    async fn rebuild_day(&mut self, symbol: String, date: NaiveDate) -> Result<(), String> {
        info!("Rebuilding volume profile for {} on {}", symbol, date);

        // Retrieve historical candles from LMDB for this date
        if let Some(lmdb_actor) = &self.lmdb_actor {
            // Calculate timestamp range for the date
            let start_of_day = date.and_hms_opt(0, 0, 0)
                .ok_or("Invalid date")?
                .and_utc()
                .timestamp_millis();
            let end_of_day = date.and_hms_opt(23, 59, 59)
                .ok_or("Invalid date")?
                .and_utc()
                .timestamp_millis();

            // Request candles from LMDB
            let message = LmdbActorMessage::GetCandles {
                symbol: symbol.clone(),
                timeframe: 60, // 1-minute candles
                start: start_of_day,
                end: end_of_day,
                limit: None,
            };

            match lmdb_actor.ask(message).await {
                Ok(LmdbActorResponse::Candles(candles)) => {
                    if candles.is_empty() {
                        warn!("No candles found for {} on {}", symbol, date);
                        return Ok(());
                    }

                    info!("📊 HISTORICAL DATA: Retrieved {} candles for {} on {}", candles.len(), symbol, date);

                    // Create or update profile
                    let profile_key = ProfileKey::new(symbol.clone(), date);
                    let mut profile = DailyVolumeProfile::new(symbol, date, &self.config);
                    
                    // Rebuild from candles with timing
                    let rebuild_start = std::time::Instant::now();
                    profile.rebuild_from_candles(&candles);
                    let rebuild_duration = rebuild_start.elapsed();
                    
                    // Store in memory
                    self.profiles.insert(profile_key.clone(), profile.clone());

                    // Get profile data for storage and publishing
                    let profile_data = profile.get_profile_data();

                    // Store in database
                    if let Err(e) = self.store_profile_in_database(&profile_key, &profile_data).await {
                        error!("Failed to store rebuilt volume profile in database: {}", e);
                        return Err(e);
                    } else {
                        self.total_database_writes += 1;
                    }

                    // Kafka publishing removed - now handled by IndicatorActor to avoid duplicate messages
                    // Volume profile data will be included in main technical analysis messages

                    info!("✅ REBUILD COMPLETE: {} on {} - {} candles processed in {:.2}ms", 
                          profile_key.symbol, profile_key.date, candles.len(), rebuild_duration.as_millis());
                    info!("📈 VOLUME PROFILE STATS: total_volume={:.2}, VWAP={:.2}, POC={:.2}, price_levels={}, value_area={:.1}%", 
                          profile_data.total_volume, profile_data.vwap, profile_data.poc, 
                          profile_data.price_levels.len(), profile_data.value_area.volume_percentage);
                }
                Ok(_) => {
                    error!("Unexpected response from LMDB actor");
                    return Err("Unexpected LMDB response".to_string());
                }
                Err(e) => {
                    error!("Failed to retrieve candles from LMDB: {}", e);
                    return Err(format!("LMDB query failed: {}", e));
                }
            }
        } else {
            error!("❌ CRITICAL: LMDB actor is None - cannot rebuild historical data for {} on {}", symbol, date);
            error!("💡 FIX: Ensure VolumeProfileActor.SetActorReferences message is sent during startup");
            error!("🔍 DEBUG: This causes volume profiles to show only single candle data instead of full daily data");
            return Err("LMDB actor not available - SetActorReferences message not sent during startup".to_string());
        }

        Ok(())
    }

    /// Store volume profile in PostgreSQL database
    async fn store_profile_in_database(&self, profile_key: &ProfileKey, profile_data: &VolumeProfileData) -> Result<(), String> {
        if let Some(_postgres_actor) = &self.postgres_actor {
            // Convert profile data to JSON for storage
            let _json_data = serde_json::to_value(profile_data)
                .map_err(|e| format!("Failed to serialize profile data: {}", e))?;

            // Create SQL for upsert operation (this would need to be implemented in the database module)
            // For now, we'll use a placeholder approach
            debug!("Storing volume profile in database: {} on {} ({} price levels)", 
                   profile_key.symbol, profile_key.date, profile_data.price_levels.len());

            // TODO: Implement actual database storage
            // The database module will handle the UPSERT operation with proper SQL
            
            Ok(())
        } else {
            Err("PostgreSQL actor not available".to_string())
        }
    }

    // Kafka publishing method removed - now handled by IndicatorActor to avoid duplicate messages
    // Volume profile data is now included in the main technical analysis messages

    /// Get volume profile for specific symbol and date
    fn get_volume_profile(&mut self, symbol: String, date: NaiveDate) -> Option<VolumeProfileData> {
        let profile_key = ProfileKey::new(symbol, date);
        self.profiles.get_mut(&profile_key).map(|profile| profile.get_profile_data())
    }

    /// Get health status
    fn get_health_status(&self) -> VolumeProfileReply {
        let memory_usage_mb = self.estimate_memory_usage() as f64 / (1024.0 * 1024.0);
        
        VolumeProfileReply::HealthStatus {
            is_healthy: self.is_healthy && self.last_error.is_none(),
            profiles_count: self.profiles.len(),
            last_error: self.last_error.as_ref().map(|s| (**s).clone()),
            memory_usage_mb,
        }
    }

    /// Get performance statistics
    fn get_statistics(&self) -> VolumeProfileReply {
        let profile_stats: Vec<VolumeProfileStatistics> = self.profiles
            .values()
            .map(|profile| profile.get_statistics())
            .collect();

        VolumeProfileReply::Statistics {
            profiles: profile_stats,
            total_candles_processed: self.total_candles_processed,
            total_database_writes: self.total_database_writes,
            // total_kafka_publishes: self.total_kafka_publishes, // Removed - now handled by IndicatorActor
        }
    }

    /// Estimate memory usage of all profiles
    fn estimate_memory_usage(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let profiles_size: usize = self.profiles.values()
            .map(|profile| profile.estimate_memory_usage())
            .sum();
        let queue_size = self.batch_queue.len() * std::mem::size_of::<(String, FuturesOHLCVCandle)>();
        
        base_size + profiles_size + queue_size
    }

    /// Clean up old profiles (called periodically)
    fn cleanup_old_profiles(&mut self) {
        let today = Utc::now().date_naive();
        let cutoff_date = today - chrono::Duration::days(7); // Keep profiles for 7 days

        let initial_count = self.profiles.len();
        self.profiles.retain(|key, _| key.date >= cutoff_date);
        let removed_count = initial_count - self.profiles.len();

        if removed_count > 0 {
            info!("Cleaned up {} old volume profiles (older than {})", removed_count, cutoff_date);
        }
    }
}

impl Actor for VolumeProfileActor {
    type Mailbox = UnboundedMailbox<Self>;

    fn name() -> &'static str {
        "VolumeProfileActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        info!("🚀 Starting Volume Profile Actor");
        
        if self.config.enabled {
            info!("Volume profile calculation enabled");
            info!("  Price increment mode: {:?}", self.config.price_increment_mode);
            info!("  Update frequency: {:?}", self.config.update_frequency);
            info!("  Value area percentage: {:.1}%", self.config.value_area_percentage);
            
            // STARTUP FIX: Initialize daily profiles for current trading day
            let today = chrono::Utc::now().date_naive();
            info!("🔄 STARTUP RECONSTRUCTION: Initializing volume profiles for current day: {}", today);
            
            // For now, we'll initialize for BTCUSDT (this could be extended to multiple symbols)
            // In a production system, this list would come from configuration or active trading symbols
            let active_symbols = vec!["BTCUSDT"]; // TODO: Make this configurable
            
            for symbol in active_symbols {
                info!("🔄 STARTUP: Rebuilding volume profile for {} on {}", symbol, today);
                if let Err(e) = self.rebuild_day(symbol.to_string(), today).await {
                    warn!("⚠️ STARTUP: Failed to rebuild volume profile for {} on {}: {}", symbol, today, e);
                } else {
                    info!("✅ STARTUP: Successfully initialized volume profile for {} on {}", symbol, today);
                }
            }
        } else {
            info!("Volume profile calculation disabled");
        }

        self.is_healthy = true;
        info!("📊 Volume Profile Actor started successfully");
        Ok(())
    }

    async fn on_stop(&mut self, _actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> Result<(), BoxError> {
        info!("🛑 Stopping Volume Profile Actor: {:?}", reason);
        
        // Process any remaining batch items
        if !self.batch_queue.is_empty() {
            info!("Processing {} remaining candles in batch queue", self.batch_queue.len());
            if let Err(e) = self.process_batch().await {
                error!("Failed to process final batch: {}", e);
            }
        }
        
        // Log final statistics
        info!("📊 Final Volume Profile Actor statistics:");
        info!("  Profiles created: {}", self.profiles.len());
        info!("  Candles processed: {}", self.total_candles_processed);
        info!("  Database writes: {}", self.total_database_writes);
        // info!("  Kafka publishes: {}", self.total_kafka_publishes); // Removed - now handled by IndicatorActor
        info!("  (Kafka publishing now handled by IndicatorActor to avoid duplicate messages)");

        Ok(())
    }
}

impl Message<VolumeProfileTell> for VolumeProfileActor {
    type Reply = ();

    async fn handle(&mut self, msg: VolumeProfileTell, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            VolumeProfileTell::ProcessCandle { symbol, candle } => {
                debug!("📈 VolumeProfileActor received ProcessCandle: {} at {}", symbol, candle.open_time);
                if let Err(e) = self.process_candle(symbol, candle).await {
                    error!("❌ Failed to process candle for volume profile: {}", e);
                    self.last_error = Some(Arc::new(e));
                }
            }
            VolumeProfileTell::RebuildDay { symbol, date } => {
                info!("🔄 VolumeProfileActor received RebuildDay: {} on {}", symbol, date);
                if let Err(e) = self.rebuild_day(symbol, date).await {
                    error!("❌ Failed to rebuild volume profile for day: {}", e);
                    self.last_error = Some(Arc::new(e));
                }
            }
            VolumeProfileTell::HealthCheck => {
                debug!("🔍 Volume Profile Actor health check");
                self.cleanup_old_profiles();
            }
            VolumeProfileTell::ProcessBatch => {
                debug!("📦 Processing volume profile batch");
                if let Err(e) = self.process_batch().await {
                    error!("❌ Failed to process volume profile batch: {}", e);
                    self.last_error = Some(Arc::new(e));
                }
            }
            VolumeProfileTell::SetActorReferences { postgres_actor, lmdb_actor } => {
                info!("🔧 CRITICAL FIX: Setting actor references for VolumeProfileActor");
                self.postgres_actor = postgres_actor;
                self.lmdb_actor = Some(lmdb_actor);
                self.is_healthy = true;
                info!("✅ CRITICAL FIX: VolumeProfileActor references set - LMDB access enabled for historical data reconstruction");
            }
        }
    }
}

impl Message<VolumeProfileAsk> for VolumeProfileActor {
    type Reply = Result<VolumeProfileReply, String>;

    async fn handle(&mut self, msg: VolumeProfileAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            VolumeProfileAsk::GetVolumeProfile { symbol, date } => {
                let profile_data = self.get_volume_profile(symbol, date);
                Ok(VolumeProfileReply::VolumeProfile(profile_data))
            }
            VolumeProfileAsk::GetHealthStatus => {
                Ok(self.get_health_status())
            }
            VolumeProfileAsk::GetStatistics => {
                Ok(self.get_statistics())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use crate::volume_profile::structs::{PriceIncrementMode, UpdateFrequency, VolumeDistributionMode, ValueAreaCalculationMode};

    fn create_test_config() -> VolumeProfileConfig {
        VolumeProfileConfig {
            enabled: true,
            price_increment_mode: PriceIncrementMode::Fixed,
            target_price_levels: 200,
            fixed_price_increment: 0.01,
            min_price_increment: 0.001,
            max_price_increment: 1.0,
            update_frequency: UpdateFrequency::EveryCandle,
            batch_size: 5,
            value_area_percentage: 70.0,
            volume_distribution_mode: VolumeDistributionMode::ClosingPrice,
            value_area_calculation_mode: ValueAreaCalculationMode::Traditional,
            asset_overrides: std::collections::HashMap::new(),
        }
    }

    fn create_test_candle(timestamp: i64, volume: f64) -> FuturesOHLCVCandle {
        FuturesOHLCVCandle {
            open_time: timestamp,
            close_time: timestamp + 59999,
            open: 50000.0,
            high: 50100.0,
            low: 49950.0,
            close: 50050.0,
            volume,
            number_of_trades: 100,
            taker_buy_base_asset_volume: volume * 0.6,
            closed: true,
        }
    }

    #[test]
    fn test_profile_key_creation() {
        let symbol = "BTCUSDT".to_string();
        let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        
        let key1 = ProfileKey::new(symbol.clone(), date);
        let key2 = ProfileKey::new(symbol.clone(), date);
        let key3 = ProfileKey::new("ETHUSDT".to_string(), date);
        
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_profile_key_from_candle() {
        let timestamp = 1736985600000; // 2025-01-15 12:00:00 UTC
        let candle = create_test_candle(timestamp, 1000.0);
        
        
        let key = ProfileKey::from_candle("BTCUSDT".to_string(), &candle);
        assert!(key.is_some());
        
        let key = key.unwrap();
        assert_eq!(key.symbol, "BTCUSDT");
        // The timestamp 1736985600000 actually converts to 2025-01-16, let's fix the test
        assert_eq!(key.date, NaiveDate::from_ymd_opt(2025, 1, 16).unwrap());
    }

    #[tokio::test]
    async fn test_volume_profile_actor_creation() {
        let config = create_test_config();
        let actor = VolumeProfileActor::new(config).unwrap();
        
        assert!(actor.profiles.is_empty());
        assert_eq!(actor.total_candles_processed, 0);
        assert!(actor.batch_queue.is_empty());
    }

    #[tokio::test]
    async fn test_memory_usage_estimation() {
        let config = create_test_config();
        let actor = VolumeProfileActor::new(config).unwrap();
        
        let initial_memory = actor.estimate_memory_usage();
        assert!(initial_memory > 0);
    }
}