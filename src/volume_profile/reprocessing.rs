use std::collections::VecDeque;
use chrono::{NaiveDate, Utc, DateTime};
use kameo::actor::ActorRef;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn, error};

use crate::lmdb::LmdbActor;
use crate::historical::actor::{HistoricalActor, HistoricalAsk, HistoricalReply};
#[cfg(feature = "postgres")]
use crate::postgres::PostgresActor;

use super::gap_detection::GapDetector;

/// Volume Profile Reprocessing Mode
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReprocessingMode {
    /// Reprocess complete historical data
    ReprocessWholeHistory,
    /// Process only identified gaps (default)
    MissingDaysOnly,
    /// Process current day only  
    TodayOnly,
}

impl Default for ReprocessingMode {
    fn default() -> Self {
        Self::MissingDaysOnly
    }
}

impl std::fmt::Display for ReprocessingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReprocessWholeHistory => write!(f, "reprocess_whole_history"),
            Self::MissingDaysOnly => write!(f, "missing_days_only"),
            Self::TodayOnly => write!(f, "today_only"),
        }
    }
}

impl std::str::FromStr for ReprocessingMode {
    type Err = ReprocessingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "reprocess_whole_history" => Ok(Self::ReprocessWholeHistory),
            "missing_days_only" => Ok(Self::MissingDaysOnly),
            "today_only" => Ok(Self::TodayOnly),
            _ => Err(ReprocessingError::InvalidMode(s.to_string())),
        }
    }
}

/// Volume Profile Reprocessing Configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolumeProfileReprocessingConfig {
    /// Whether reprocessing is enabled
    pub enabled: bool,
    /// Reprocessing mode
    pub mode: ReprocessingMode,
    /// Optional batch size for processing optimization
    pub batch_size: Option<usize>,
}

impl Default for VolumeProfileReprocessingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: ReprocessingMode::default(),
            batch_size: Some(10), // Process 10 days at a time by default
        }
    }
}

impl VolumeProfileReprocessingConfig {
    /// Create new reprocessing config
    pub fn new(enabled: bool, mode: ReprocessingMode, batch_size: Option<usize>) -> Self {
        Self {
            enabled,
            mode,
            batch_size,
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), ReprocessingError> {
        if self.enabled {
            if let Some(batch_size) = self.batch_size {
                if batch_size == 0 {
                    return Err(ReprocessingError::InvalidConfiguration(
                        "Batch size cannot be zero".to_string()
                    ));
                }
                if batch_size > 1000 {
                    return Err(ReprocessingError::InvalidConfiguration(
                        "Batch size cannot exceed 1000 days".to_string()
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Reprocessing status and progress information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReprocessingStatus {
    /// Whether reprocessing is currently active
    pub active: bool,
    /// Current mode being used
    pub mode: Option<ReprocessingMode>,
    /// Symbol being processed
    pub symbol: Option<String>,
    /// Total days to process
    pub total_days: u32,
    /// Days completed
    pub days_completed: u32,
    /// Current date being processed
    pub current_date: Option<NaiveDate>,
    /// Start time of reprocessing
    pub started_at: Option<DateTime<Utc>>,
    /// Last update time
    pub updated_at: DateTime<Utc>,
}

impl Default for ReprocessingStatus {
    fn default() -> Self {
        Self {
            active: false,
            mode: None,
            symbol: None,
            total_days: 0,
            days_completed: 0,
            current_date: None,
            started_at: None,
            updated_at: Utc::now(),
        }
    }
}

impl ReprocessingStatus {
    /// Calculate progress percentage
    pub fn progress_percentage(&self) -> f64 {
        if self.total_days == 0 {
            0.0
        } else {
            (self.days_completed as f64 / self.total_days as f64) * 100.0
        }
    }

    /// Check if reprocessing is complete
    pub fn is_complete(&self) -> bool {
        self.total_days > 0 && self.days_completed >= self.total_days
    }
}

/// Errors that can occur during reprocessing
#[derive(Debug, Error)]
pub enum ReprocessingError {
    #[error("Invalid reprocessing mode: {0}")]
    InvalidMode(String),
    #[error("Invalid reprocessing configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Gap detection failed: {0}")]
    GapDetectionError(#[from] super::gap_detection::GapDetectionError),
    #[error("LMDB communication failed: {0}")]
    LmdbError(String),
    #[error("Historical actor communication failed: {0}")]
    HistoricalActorError(String),
    #[cfg(feature = "postgres")]
    #[error("PostgreSQL communication failed: {0}")]
    PostgresError(String),
    #[error("No reprocessing currently active")]
    NoActiveReprocessing,
    #[error("Reprocessing already active for symbol {0}")]
    AlreadyActive(String),
}

/// Coordinator for volume profile reprocessing workflow
#[derive(Debug)]
pub struct ReprocessingCoordinator {
    /// Current reprocessing configuration
    config: VolumeProfileReprocessingConfig,
    /// Current status and progress
    status: ReprocessingStatus,
    /// Gap detector for identifying missing data
    gap_detector: GapDetector,
    /// Queue of dates to process
    processing_queue: VecDeque<NaiveDate>,
}

impl ReprocessingCoordinator {
    /// Create new reprocessing coordinator
    pub fn new(config: VolumeProfileReprocessingConfig) -> Result<Self, ReprocessingError> {
        config.validate()?;
        
        Ok(Self {
            config,
            status: ReprocessingStatus::default(),
            gap_detector: GapDetector::new(),
            processing_queue: VecDeque::new(),
        })
    }

    /// Update reprocessing configuration
    pub fn update_config(&mut self, config: VolumeProfileReprocessingConfig) -> Result<(), ReprocessingError> {
        config.validate()?;
        self.config = config;
        info!("📝 Reprocessing configuration updated: enabled={}, mode={}", 
              self.config.enabled, self.config.mode);
        Ok(())
    }

    /// Get current reprocessing status
    pub fn get_status(&self) -> &ReprocessingStatus {
        &self.status
    }

    /// Get current configuration
    pub fn get_config(&self) -> &VolumeProfileReprocessingConfig {
        &self.config
    }

    /// Start reprocessing for a symbol based on the configured mode
    pub async fn start_reprocessing(
        &mut self,
        symbol: &str,
        lmdb_actor: &ActorRef<LmdbActor>,
        historical_actor: &ActorRef<HistoricalActor>,
        #[cfg(feature = "postgres")]
        postgres_actor: Option<&ActorRef<PostgresActor>>,
    ) -> Result<(), ReprocessingError> {
        if !self.config.enabled {
            return Err(ReprocessingError::InvalidConfiguration(
                "Reprocessing is disabled".to_string()
            ));
        }

        if self.status.active {
            return Err(ReprocessingError::AlreadyActive(
                self.status.symbol.as_ref().map_or("unknown".to_string(), |s| s.clone())
            ));
        }

        info!("🔄 Starting volume profile reprocessing for {} in mode: {}", 
              symbol, self.config.mode);

        // Initialize status
        self.status = ReprocessingStatus {
            active: true,
            mode: Some(self.config.mode.clone()),
            symbol: Some(symbol.to_string()),
            total_days: 0,
            days_completed: 0,
            current_date: None,
            started_at: Some(Utc::now()),
            updated_at: Utc::now(),
        };

        // Determine date range and processing strategy based on mode
        match self.config.mode {
            ReprocessingMode::TodayOnly => {
                self.start_today_only_reprocessing(symbol).await
            }
            ReprocessingMode::MissingDaysOnly => {
                self.start_missing_days_reprocessing(symbol, lmdb_actor, #[cfg(feature = "postgres")] postgres_actor).await
            }
            ReprocessingMode::ReprocessWholeHistory => {
                self.start_whole_history_reprocessing(symbol, historical_actor).await
            }
        }
    }

    /// Stop active reprocessing
    pub async fn stop_reprocessing(&mut self) -> Result<(), ReprocessingError> {
        if !self.status.active {
            return Err(ReprocessingError::NoActiveReprocessing);
        }

        let symbol = self.status.symbol.as_ref().unwrap().clone();
        info!("⏹️ Stopping volume profile reprocessing for {}", symbol);

        // Clear processing queue and reset status
        self.processing_queue.clear();
        self.status = ReprocessingStatus::default();

        info!("✅ Reprocessing stopped for {}", symbol);
        Ok(())
    }

    /// Process today only
    async fn start_today_only_reprocessing(&mut self, symbol: &str) -> Result<(), ReprocessingError> {
        let today = Utc::now().date_naive();
        
        info!("📅 Today-only reprocessing for {} on {}", symbol, today);
        
        self.processing_queue.push_back(today);
        self.status.total_days = 1;
        self.status.current_date = Some(today);

        // In a full implementation, this would trigger volume profile calculation
        // for today's data using the VolumeProfileActor
        info!("✅ Today-only reprocessing queued for {}", symbol);
        Ok(())
    }

    /// Process missing days only (gap detection)
    async fn start_missing_days_reprocessing(
        &mut self,
        symbol: &str,
        lmdb_actor: &ActorRef<LmdbActor>,
        #[cfg(feature = "postgres")]
        postgres_actor: Option<&ActorRef<PostgresActor>>,
    ) -> Result<(), ReprocessingError> {
        info!("🔍 Starting gap detection for missing days reprocessing: {}", symbol);

        // Use a reasonable historical window for gap detection (e.g., last 60 days)
        let end_date = Utc::now().date_naive();
        let start_date = end_date - chrono::Duration::days(60);

        info!("📅 Gap detection range: {} to {} ({} days)", 
              start_date, end_date, (end_date - start_date).num_days());

        // Perform gap detection
        let gap_result = self.gap_detector.detect_gaps_comprehensive(
            symbol,
            start_date,
            end_date,
            lmdb_actor,
            #[cfg(feature = "postgres")]
            postgres_actor,
        ).await?;

        if !gap_result.has_gaps() {
            info!("✅ No gaps found for {} - reprocessing complete", symbol);
            self.status.active = false;
            return Ok(());
        }

        // Queue missing dates for processing
        let mut total_days = 0;
        for range in &gap_result.missing_date_ranges {
            let mut current_date = range.start;
            while current_date <= range.end {
                self.processing_queue.push_back(current_date);
                total_days += 1;
                current_date = current_date.succ_opt().unwrap_or(current_date);
            }
        }

        self.status.total_days = total_days;
        info!("📊 Missing days reprocessing: {} gaps found, {} total days queued", 
              gap_result.total_gaps_found, total_days);

        // Process in batches if configured
        if let Some(batch_size) = self.config.batch_size {
            info!("🔄 Processing in batches of {} days", batch_size);
        }

        Ok(())
    }

    /// Process whole history
    async fn start_whole_history_reprocessing(
        &mut self,
        symbol: &str,
        historical_actor: &ActorRef<HistoricalActor>,
    ) -> Result<(), ReprocessingError> {
        info!("🏛️ Starting whole history reprocessing for {}", symbol);

        // For whole history, we need to determine the full historical range
        // This could be configured or based on available data
        let end_date = Utc::now().date_naive();
        let start_date = end_date - chrono::Duration::days(365); // Last year by default

        info!("📅 Whole history range: {} to {} ({} days)", 
              start_date, end_date, (end_date - start_date).num_days());

        // Queue all dates in the range
        let mut current_date = start_date;
        let mut total_days = 0;
        while current_date <= end_date {
            self.processing_queue.push_back(current_date);
            total_days += 1;
            current_date = current_date.succ_opt().unwrap_or(current_date);
        }

        self.status.total_days = total_days;
        info!("📊 Whole history reprocessing: {} total days queued", total_days);

        // Trigger historical data processing
        // This would typically coordinate with the HistoricalActor
        match historical_actor.ask(HistoricalAsk::GetCandles {
            symbol: symbol.to_string(),
            timeframe: 60, // 1-minute candles for volume profile
            start_time: start_date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis(),
            end_time: end_date.and_hms_opt(23, 59, 59).unwrap().and_utc().timestamp_millis(),
        }).await {
            Ok(HistoricalReply::Candles(_candles)) => {
                info!("✅ Historical data request completed for whole history reprocessing");
            }
            Ok(_) => {
                warn!("⚠️ Unexpected reply from historical actor");
            }
            Err(e) => {
                error!("❌ Historical data request failed: {}", e);
                return Err(ReprocessingError::HistoricalActorError(e.to_string()));
            }
        }

        Ok(())
    }

    /// Process next batch of dates from the queue
    pub async fn process_next_batch(&mut self) -> Result<bool, ReprocessingError> {
        if !self.status.active || self.processing_queue.is_empty() {
            if self.status.active {
                // Mark as complete
                self.status.active = false;
                info!("✅ Reprocessing completed for {}", 
                      self.status.symbol.as_ref().unwrap());
            }
            return Ok(false); // No more work
        }

        let batch_size = self.config.batch_size.unwrap_or(1);
        let mut processed_count = 0;

        info!("🔄 Processing next batch: {} days remaining, batch size: {}", 
              self.processing_queue.len(), batch_size);

        while processed_count < batch_size && !self.processing_queue.is_empty() {
            let date = self.processing_queue.pop_front().unwrap();
            self.status.current_date = Some(date);
            
            // In a full implementation, this would:
            // 1. Retrieve historical candle data for the date
            // 2. Calculate volume profile for the date  
            // 3. Store the volume profile data
            // 4. Update progress
            
            debug!("📊 Processing volume profile for date: {}", date);
            
            self.status.days_completed += 1;
            processed_count += 1;
            self.status.updated_at = Utc::now();
        }

        info!("✅ Processed batch: {} days, {:.1}% complete ({}/{})", 
              processed_count, self.status.progress_percentage(), 
              self.status.days_completed, self.status.total_days);

        Ok(!self.processing_queue.is_empty()) // More work remaining
    }
}

impl Default for ReprocessingCoordinator {
    fn default() -> Self {
        Self::new(VolumeProfileReprocessingConfig::default())
            .expect("Default configuration should be valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reprocessing_mode_display() {
        assert_eq!(ReprocessingMode::ReprocessWholeHistory.to_string(), "reprocess_whole_history");
        assert_eq!(ReprocessingMode::MissingDaysOnly.to_string(), "missing_days_only");
        assert_eq!(ReprocessingMode::TodayOnly.to_string(), "today_only");
    }

    #[test]
    fn test_reprocessing_mode_from_str() {
        assert_eq!(
            "reprocess_whole_history".parse::<ReprocessingMode>().unwrap(),
            ReprocessingMode::ReprocessWholeHistory
        );
        assert_eq!(
            "missing_days_only".parse::<ReprocessingMode>().unwrap(),
            ReprocessingMode::MissingDaysOnly
        );
        assert_eq!(
            "today_only".parse::<ReprocessingMode>().unwrap(),
            ReprocessingMode::TodayOnly
        );

        assert!("invalid_mode".parse::<ReprocessingMode>().is_err());
    }

    #[test]
    fn test_reprocessing_config_validation() {
        // Valid configurations
        let valid_config = VolumeProfileReprocessingConfig::new(true, ReprocessingMode::MissingDaysOnly, Some(10));
        assert!(valid_config.validate().is_ok());

        let disabled_config = VolumeProfileReprocessingConfig::new(false, ReprocessingMode::TodayOnly, Some(0));
        assert!(disabled_config.validate().is_ok()); // Disabled configs don't validate batch size

        // Invalid configurations
        let zero_batch_config = VolumeProfileReprocessingConfig::new(true, ReprocessingMode::MissingDaysOnly, Some(0));
        assert!(zero_batch_config.validate().is_err());

        let large_batch_config = VolumeProfileReprocessingConfig::new(true, ReprocessingMode::MissingDaysOnly, Some(1001));
        assert!(large_batch_config.validate().is_err());
    }

    #[test]
    fn test_reprocessing_status_progress() {
        let mut status = ReprocessingStatus::default();
        assert_eq!(status.progress_percentage(), 0.0);
        assert!(!status.is_complete());

        status.total_days = 100;
        status.days_completed = 25;
        assert_eq!(status.progress_percentage(), 25.0);
        assert!(!status.is_complete());

        status.days_completed = 100;
        assert_eq!(status.progress_percentage(), 100.0);
        assert!(status.is_complete());
    }

    #[test]
    fn test_reprocessing_coordinator_creation() {
        let config = VolumeProfileReprocessingConfig::default();
        let coordinator = ReprocessingCoordinator::new(config);
        assert!(coordinator.is_ok());

        let invalid_config = VolumeProfileReprocessingConfig::new(true, ReprocessingMode::MissingDaysOnly, Some(0));
        let invalid_coordinator = ReprocessingCoordinator::new(invalid_config);
        assert!(invalid_coordinator.is_err());
    }

    #[test]
    fn test_reprocessing_coordinator_config_update() {
        let mut coordinator = ReprocessingCoordinator::default();
        
        let new_config = VolumeProfileReprocessingConfig::new(true, ReprocessingMode::TodayOnly, Some(5));
        assert!(coordinator.update_config(new_config).is_ok());
        assert_eq!(coordinator.get_config().mode, ReprocessingMode::TodayOnly);
        assert_eq!(coordinator.get_config().batch_size, Some(5));

        let invalid_config = VolumeProfileReprocessingConfig::new(true, ReprocessingMode::MissingDaysOnly, Some(0));
        assert!(coordinator.update_config(invalid_config).is_err());
    }
}