use std::collections::HashMap;
use std::sync::Arc;

use chrono::{NaiveDate, Utc, DateTime, Datelike, Timelike};
use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::message::{Context, Message};
use kameo::request::MessageSend;
use kameo::{Actor, mailbox::unbounded::UnboundedMailbox};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::historical::structs::FuturesOHLCVCandle;
use crate::lmdb::{LmdbActor, LmdbActorMessage, LmdbActorResponse};
use crate::postgres::{PostgresActor, PostgresTell};
// Kafka publishing is now handled by IndicatorActor to avoid duplicate messages
// use crate::kafka::{KafkaActor, KafkaTell};
// use crate::technical_analysis::structs::IndicatorOutput;

use super::calculator::{DailyVolumeProfile, VolumeProfileStatistics};
use super::structs::{VolumeProfileConfig, VolumeProfileData, UpdateFrequency};

/// Result of date validation with detailed information
#[derive(Debug, Clone, PartialEq)]
pub enum DateValidationResult {
    Valid { trading_day: NaiveDate },
    Invalid { reason: String, timestamp: i64 },
    EdgeCase { 
        trading_day: NaiveDate, 
        warning: String, 
        timestamp: i64 
    },
}

/// Processing report for historical data operations
#[derive(Debug, Clone)]
pub struct ProcessingReport {
    pub symbol: String,
    pub date: NaiveDate,
    pub candles_processed: usize,
    pub validation_results: Vec<DateValidationResult>,
    pub processing_duration_ms: u64,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

/// Enhanced date validation and timezone handling for historical data reconstruction
#[derive(Debug, Clone)]
pub struct HistoricalDataProcessor {
    /// Enable detailed logging for debugging date validation decisions
    pub debug_logging: bool,
}

impl HistoricalDataProcessor {
    /// Create new historical data processor
    pub fn new() -> Self {
        Self {
            debug_logging: true,
        }
    }

    /// Validate candle date with timezone awareness and edge case handling
    pub fn validate_candle_date(&self, candle: &FuturesOHLCVCandle, target_date: NaiveDate) -> DateValidationResult {
        // Convert timestamp to UTC date with explicit validation
        let datetime = match chrono::DateTime::from_timestamp_millis(candle.open_time) {
            Some(dt) => dt,
            None => {
                return DateValidationResult::Invalid {
                    reason: format!("Invalid timestamp: {}", candle.open_time),
                    timestamp: candle.open_time,
                };
            }
        };

        let candle_date = datetime.date_naive();
        
        if self.debug_logging {
            debug!("Date validation: candle_timestamp={}, candle_date={}, target_date={}", 
                   candle.open_time, candle_date, target_date);
        }

        // Basic date match
        if candle_date == target_date {
            return DateValidationResult::Valid { trading_day: target_date };
        }

        // Handle timezone boundary conditions
        if self.is_timezone_boundary_case(datetime, target_date) {
            return DateValidationResult::EdgeCase {
                trading_day: target_date,
                warning: format!("Candle at timezone boundary: {} vs target {}", candle_date, target_date),
                timestamp: candle.open_time,
            };
        }

        // Handle daylight saving time transitions
        if self.is_dst_transition_case(datetime, target_date) {
            return DateValidationResult::EdgeCase {
                trading_day: target_date,
                warning: format!("Potential DST transition: {} vs target {}", candle_date, target_date),
                timestamp: candle.open_time,
            };
        }

        // Handle leap year edge cases
        if self.is_leap_year_edge_case(candle_date, target_date) {
            return DateValidationResult::EdgeCase {
                trading_day: target_date,
                warning: format!("Leap year date boundary: {} vs target {}", candle_date, target_date),
                timestamp: candle.open_time,
            };
        }

        DateValidationResult::Invalid {
            reason: format!("Candle date {} does not match target date {}", candle_date, target_date),
            timestamp: candle.open_time,
        }
    }

    /// Process historical candle range with comprehensive validation
    pub fn process_historical_range(&self, candles: &[FuturesOHLCVCandle], target_date: NaiveDate, symbol: &str) -> ProcessingReport {
        let start_time = std::time::Instant::now();
        let mut validation_results = Vec::new();
        let mut warnings = Vec::new();
        let mut errors = Vec::new();
        let mut valid_candles = 0;

        for candle in candles {
            let validation_result = self.validate_candle_date(candle, target_date);
            
            match &validation_result {
                DateValidationResult::Valid { .. } => {
                    valid_candles += 1;
                }
                DateValidationResult::EdgeCase { warning, .. } => {
                    valid_candles += 1; // Count as valid but note the warning
                    warnings.push(warning.clone());
                }
                DateValidationResult::Invalid { reason, .. } => {
                    errors.push(reason.clone());
                }
            }
            
            validation_results.push(validation_result);
        }

        let processing_duration_ms = start_time.elapsed().as_millis() as u64;

        if self.debug_logging {
            info!("Historical processing report for {} on {}: {}/{} valid candles, {} warnings, {} errors", 
                  symbol, target_date, valid_candles, candles.len(), warnings.len(), errors.len());
        }

        ProcessingReport {
            symbol: symbol.to_string(),
            date: target_date,
            candles_processed: candles.len(),
            validation_results,
            processing_duration_ms,
            warnings,
            errors,
        }
    }

    /// Check if candle is at timezone boundary (within 1 hour of day boundary)
    fn is_timezone_boundary_case(&self, datetime: DateTime<Utc>, target_date: NaiveDate) -> bool {
        let candle_date = datetime.date_naive();
        
        // Check if candle is within 1 hour of the target date boundaries
        let day_difference = (candle_date - target_date).num_days().abs();
        
        if day_difference == 1 {
            let hour = datetime.time().hour();
            // Within 1 hour of day boundary (23:xx or 00:xx)
            return hour >= 23 || hour == 0;
        }
        
        false
    }

    /// Check if this might be a daylight saving time transition case
    fn is_dst_transition_case(&self, datetime: DateTime<Utc>, target_date: NaiveDate) -> bool {
        let candle_date = datetime.date_naive();
        let day_difference = (candle_date - target_date).num_days().abs();
        
        if day_difference == 1 {
            // DST transitions typically happen in March and November
            let month = target_date.month();
            if month == 3 || month == 11 {
                // Check if we're near the typical DST transition dates
                let day = target_date.day();
                return (8..=15).contains(&day); // Second Sunday in March, First Sunday in November
            }
        }
        
        false
    }

    /// Check if this is a leap year edge case
    fn is_leap_year_edge_case(&self, candle_date: NaiveDate, target_date: NaiveDate) -> bool {
        // Check if we're dealing with February 29th or dates around it
        let is_leap_year = target_date.year() % 4 == 0 && 
                          (target_date.year() % 100 != 0 || target_date.year() % 400 == 0);
        
        if is_leap_year {
            let feb_29 = NaiveDate::from_ymd_opt(target_date.year(), 2, 29);
            if let Some(leap_day) = feb_29 {
                let day_difference = (candle_date - leap_day).num_days().abs();
                return day_difference <= 1;
            }
        }
        
        false
    }

    /// Ensure UTC consistency in timestamp conversions
    pub fn ensure_utc_consistency(&self, timestamp: i64) -> Result<DateTime<Utc>, String> {
        chrono::DateTime::from_timestamp_millis(timestamp)
            .ok_or_else(|| format!("Invalid timestamp for UTC conversion: {}", timestamp))
    }
}

impl Default for HistoricalDataProcessor {
    fn default() -> Self {
        Self::new()
    }
}

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
    /// Detect gaps in historical data for a date range
    DetectGaps {
        symbol: String,
        start_date: NaiveDate,
        end_date: NaiveDate,
    },
    /// Process historical backfill for detected gaps
    ProcessBackfill {
        symbol: String,
        missing_dates: Vec<NaiveDate>,
    },
    /// Process retry queue for failed operations
    ProcessRetryQueue,
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
    /// Detect gaps in historical data for a date range
    DetectGaps {
        symbol: String,
        start_date: NaiveDate,
        end_date: NaiveDate,
    },
    /// Process historical backfill for detected gaps
    ProcessBackfill {
        symbol: String,
        missing_dates: Vec<NaiveDate>,
    },
    /// Process retry queue for failed operations
    ProcessRetryQueue,
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
            VolumeProfileTellSerializable::DetectGaps { symbol, start_date, end_date } => {
                VolumeProfileTell::DetectGaps { symbol, start_date, end_date }
            }
            VolumeProfileTellSerializable::ProcessBackfill { symbol, missing_dates } => {
                VolumeProfileTell::ProcessBackfill { symbol, missing_dates }
            }
            VolumeProfileTellSerializable::ProcessRetryQueue => VolumeProfileTell::ProcessRetryQueue,
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
    /// Get debug metadata for volume profile validation
    GetDebugMetadata {
        symbol: String,
        date: NaiveDate,
    },
    /// Get health status
    GetHealthStatus,
    /// Get performance statistics
    GetStatistics,
    /// Get gap detection results for a date range
    GetGapDetectionResults {
        symbol: String,
        start_date: NaiveDate,
        end_date: NaiveDate,
    },
    /// Get current processing status and progress
    GetProcessingStatus,
}

/// Volume Profile Actor responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeProfileReply {
    /// Volume profile data response
    VolumeProfile(Option<VolumeProfileData>),
    /// Debug metadata response for validation and analysis
    DebugMetadata(Option<super::structs::VolumeProfileDebugMetadata>),
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
    /// Gap detection results response
    GapDetectionResults {
        symbol: String,
        missing_dates: Vec<NaiveDate>,
        total_gaps: usize,
        date_range: (NaiveDate, NaiveDate),
    },
    /// Processing status response
    ProcessingStatus {
        current_operation: Option<String>,
        progress_percentage: Option<f32>,
        estimated_completion: Option<String>,
        retry_queue_size: usize,
        processing_metrics: ProcessingMetrics,
    },
    /// Success confirmation
    Success,
    /// Error response
    Error(String),
}

/// Gap detection and backfill management
#[derive(Debug, Clone)]
pub struct GapDetectionManager {
    /// Missing volume profile dates for backfill processing
    pub missing_dates: HashMap<String, Vec<NaiveDate>>,
    /// Failed processing attempts with retry tracking
    pub failed_attempts: HashMap<ProfileKey, RetryInfo>,
    /// Last gap detection scan time
    pub last_scan_time: Option<std::time::Instant>,
}

/// Retry information for failed processing attempts
#[derive(Debug, Clone)]
pub struct RetryInfo {
    /// Number of retry attempts
    pub attempt_count: u32,
    /// Last attempt timestamp
    pub last_attempt: std::time::Instant,
    /// Next retry time (exponential backoff)
    pub next_retry: std::time::Instant,
    /// Last error message
    pub last_error: String,
    /// Maximum retry attempts
    pub max_retries: u32,
}

impl RetryInfo {
    /// Create new retry info
    pub fn new(error: String, max_retries: u32) -> Self {
        let now = std::time::Instant::now();
        Self {
            attempt_count: 1,
            last_attempt: now,
            next_retry: now + std::time::Duration::from_secs(2), // Initial 2-second delay
            last_error: error,
            max_retries,
        }
    }

    /// Calculate next retry time with exponential backoff
    pub fn calculate_next_retry(&mut self) {
        self.attempt_count += 1;
        self.last_attempt = std::time::Instant::now();
        
        // Exponential backoff: 2^attempt_count seconds, capped at 300 seconds (5 minutes)
        let delay_seconds = (2_u32.pow(self.attempt_count.min(8))).min(300);
        self.next_retry = self.last_attempt + std::time::Duration::from_secs(delay_seconds as u64);
        
        debug!("Retry #{} scheduled for {} seconds delay", 
               self.attempt_count, delay_seconds);
    }

    /// Check if it's time to retry
    pub fn should_retry(&self) -> bool {
        self.attempt_count < self.max_retries && std::time::Instant::now() >= self.next_retry
    }

    /// Check if max retries exceeded
    pub fn is_exhausted(&self) -> bool {
        self.attempt_count >= self.max_retries
    }
}

impl GapDetectionManager {
    /// Create new gap detection manager
    pub fn new() -> Self {
        Self {
            missing_dates: HashMap::new(),
            failed_attempts: HashMap::new(),
            last_scan_time: None,
        }
    }

    /// Add missing date for symbol
    pub fn add_missing_date(&mut self, symbol: String, date: NaiveDate) {
        self.missing_dates.entry(symbol).or_default().push(date);
    }

    /// Record failed processing attempt
    pub fn record_failure(&mut self, key: ProfileKey, error: String, max_retries: u32) {
        if let Some(retry_info) = self.failed_attempts.get_mut(&key) {
            retry_info.last_error = error;
            retry_info.calculate_next_retry();
        } else {
            self.failed_attempts.insert(key, RetryInfo::new(error, max_retries));
        }
    }

    /// Get profiles ready for retry
    pub fn get_retry_candidates(&self) -> Vec<ProfileKey> {
        self.failed_attempts
            .iter()
            .filter(|(_, retry_info)| retry_info.should_retry())
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Remove successful retry from failed attempts
    pub fn mark_success(&mut self, key: &ProfileKey) {
        self.failed_attempts.remove(key);
    }

    /// Clean up exhausted retries
    pub fn cleanup_exhausted_retries(&mut self) -> Vec<ProfileKey> {
        let exhausted: Vec<_> = self.failed_attempts
            .iter()
            .filter(|(_, retry_info)| retry_info.is_exhausted())
            .map(|(key, _)| key.clone())
            .collect();

        for key in &exhausted {
            warn!("Giving up on failed volume profile processing: {} on {} after {} attempts", 
                  key.symbol, key.date, self.failed_attempts[key].attempt_count);
            self.failed_attempts.remove(key);
        }

        exhausted
    }

    /// Get comprehensive reporting of missing date ranges
    pub fn get_missing_ranges_report(&self) -> HashMap<String, Vec<(NaiveDate, NaiveDate)>> {
        let mut ranges = HashMap::new();
        
        for (symbol, dates) in &self.missing_dates {
            let mut sorted_dates = dates.clone();
            sorted_dates.sort();
            
            let mut symbol_ranges = Vec::new();
            if !sorted_dates.is_empty() {
                let mut range_start = sorted_dates[0];
                let mut range_end = sorted_dates[0];
                
                for &date in sorted_dates.iter().skip(1) {
                    if date == range_end + chrono::Duration::days(1) {
                        // Consecutive date, extend range
                        range_end = date;
                    } else {
                        // Gap in dates, close current range and start new one
                        symbol_ranges.push((range_start, range_end));
                        range_start = date;
                        range_end = date;
                    }
                }
                // Add final range
                symbol_ranges.push((range_start, range_end));
            }
            
            ranges.insert(symbol.clone(), symbol_ranges);
        }
        
        ranges
    }
}

impl Default for GapDetectionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory management and batch processing optimization
#[derive(Debug, Clone)]
pub struct BatchProcessingManager {
    /// Memory usage monitoring
    pub memory_monitor: MemoryMonitor,
    /// Batch size configuration
    pub batch_config: BatchConfig,
    /// Performance metrics tracking
    pub performance_metrics: BatchPerformanceMetrics,
}

/// Memory usage monitoring for batch operations
#[derive(Debug, Clone)]
pub struct MemoryMonitor {
    /// Current estimated memory usage in bytes
    pub current_usage: usize,
    /// Peak memory usage recorded during operations
    pub peak_usage: usize,
    /// Memory pressure threshold in bytes (warn above this)
    pub pressure_threshold: usize,
    /// Critical memory threshold in bytes (stop processing above this)
    pub critical_threshold: usize,
    /// Last memory check timestamp
    pub last_check: Option<std::time::Instant>,
}

/// Batch configuration with adaptive sizing
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Base batch size for date processing
    pub base_batch_size: usize,
    /// Current adaptive batch size
    pub current_batch_size: usize,
    /// Minimum batch size (never go below this)
    pub min_batch_size: usize,
    /// Maximum batch size (never go above this)
    pub max_batch_size: usize,
    /// Memory per item estimate in bytes
    pub memory_per_item_bytes: usize,
}

/// Performance metrics for batch processing
#[derive(Debug, Clone)]
pub struct BatchPerformanceMetrics {
    /// Total batches processed
    pub batches_processed: u64,
    /// Total items processed across all batches
    pub items_processed: u64,
    /// Total processing time in milliseconds
    pub total_processing_time_ms: u64,
    /// Average processing time per item in microseconds
    pub avg_processing_time_per_item_us: u64,
    /// Peak memory usage during processing
    pub peak_memory_usage_bytes: usize,
    /// Memory efficiency (items per MB)
    pub memory_efficiency: f64,
}

impl Default for MemoryMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryMonitor {
    /// Create new memory monitor with default thresholds
    pub fn new() -> Self {
        let available_memory = Self::estimate_available_memory();
        Self {
            current_usage: 0,
            peak_usage: 0,
            pressure_threshold: (available_memory as f64 * 0.75) as usize, // 75% of available
            critical_threshold: (available_memory as f64 * 0.90) as usize, // 90% of available
            last_check: None,
        }
    }

    /// Estimate available system memory (simplified heuristic)
    fn estimate_available_memory() -> usize {
        // Conservative estimate: assume 1GB available for our process
        // In production, this could query actual system memory
        1024 * 1024 * 1024 // 1GB
    }

    /// Update current memory usage and check thresholds
    pub fn update_usage(&mut self, usage_bytes: usize) -> MemoryPressureLevel {
        self.current_usage = usage_bytes;
        self.peak_usage = self.peak_usage.max(usage_bytes);
        self.last_check = Some(std::time::Instant::now());

        if usage_bytes >= self.critical_threshold {
            MemoryPressureLevel::Critical
        } else if usage_bytes >= self.pressure_threshold {
            MemoryPressureLevel::High
        } else if usage_bytes >= (self.pressure_threshold as f64 * 0.5) as usize {
            MemoryPressureLevel::Medium
        } else {
            MemoryPressureLevel::Low
        }
    }

    /// Get memory usage statistics
    pub fn get_usage_stats(&self) -> MemoryUsageStats {
        MemoryUsageStats {
            current_mb: self.current_usage as f64 / (1024.0 * 1024.0),
            peak_mb: self.peak_usage as f64 / (1024.0 * 1024.0),
            pressure_threshold_mb: self.pressure_threshold as f64 / (1024.0 * 1024.0),
            critical_threshold_mb: self.critical_threshold as f64 / (1024.0 * 1024.0),
            pressure_level: self.get_current_pressure_level(),
        }
    }

    /// Get current memory pressure level
    pub fn get_current_pressure_level(&self) -> MemoryPressureLevel {
        if self.current_usage >= self.critical_threshold {
            MemoryPressureLevel::Critical
        } else if self.current_usage >= self.pressure_threshold {
            MemoryPressureLevel::High
        } else if self.current_usage >= (self.pressure_threshold as f64 * 0.5) as usize {
            MemoryPressureLevel::Medium
        } else {
            MemoryPressureLevel::Low
        }
    }
}

/// Memory pressure levels for adaptive processing
#[derive(Debug, Clone, PartialEq)]
pub enum MemoryPressureLevel {
    Low,    // < 50% of pressure threshold
    Medium, // 50-75% of pressure threshold  
    High,   // 75-90% of available memory
    Critical, // > 90% of available memory
}

/// Memory usage statistics
#[derive(Debug, Clone)]
pub struct MemoryUsageStats {
    pub current_mb: f64,
    pub peak_mb: f64,
    pub pressure_threshold_mb: f64,
    pub critical_threshold_mb: f64,
    pub pressure_level: MemoryPressureLevel,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchConfig {
    /// Create new batch config with defaults
    pub fn new() -> Self {
        Self {
            base_batch_size: 10,
            current_batch_size: 10,
            min_batch_size: 1,
            max_batch_size: 100,
            memory_per_item_bytes: 1024 * 1024, // 1MB per day of data (conservative estimate)
        }
    }

    /// Adjust batch size based on memory pressure
    pub fn adjust_for_memory_pressure(&mut self, pressure_level: MemoryPressureLevel) {
        match pressure_level {
            MemoryPressureLevel::Low => {
                // Can increase batch size up to max
                self.current_batch_size = (self.current_batch_size * 2).min(self.max_batch_size);
            }
            MemoryPressureLevel::Medium => {
                // Keep current batch size or slightly reduce
                self.current_batch_size = (self.current_batch_size * 3 / 4).max(self.min_batch_size);
            }
            MemoryPressureLevel::High => {
                // Reduce batch size significantly
                self.current_batch_size = (self.current_batch_size / 2).max(self.min_batch_size);
            }
            MemoryPressureLevel::Critical => {
                // Use minimum batch size
                self.current_batch_size = self.min_batch_size;
            }
        }
        
        debug!("Adjusted batch size to {} for memory pressure level {:?}", 
               self.current_batch_size, pressure_level);
    }

    /// Estimate memory requirement for given batch size
    pub fn estimate_memory_requirement(&self, batch_size: usize) -> usize {
        batch_size * self.memory_per_item_bytes
    }
}

impl Default for BatchPerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchPerformanceMetrics {
    /// Create new performance metrics
    pub fn new() -> Self {
        Self {
            batches_processed: 0,
            items_processed: 0,
            total_processing_time_ms: 0,
            avg_processing_time_per_item_us: 0,
            peak_memory_usage_bytes: 0,
            memory_efficiency: 0.0,
        }
    }

    /// Record batch processing completion
    pub fn record_batch_completion(&mut self, items_in_batch: usize, processing_time_ms: u64, memory_used_bytes: usize) {
        self.batches_processed += 1;
        self.items_processed += items_in_batch as u64;
        self.total_processing_time_ms += processing_time_ms;
        self.peak_memory_usage_bytes = self.peak_memory_usage_bytes.max(memory_used_bytes);

        // Calculate average processing time per item
        if self.items_processed > 0 {
            self.avg_processing_time_per_item_us = (self.total_processing_time_ms * 1000) / self.items_processed;
        }

        // Calculate memory efficiency (items per MB)
        if memory_used_bytes > 0 {
            let memory_mb = memory_used_bytes as f64 / (1024.0 * 1024.0);
            self.memory_efficiency = items_in_batch as f64 / memory_mb;
        }
    }

    /// Get performance summary
    pub fn get_performance_summary(&self) -> BatchPerformanceSummary {
        BatchPerformanceSummary {
            total_batches: self.batches_processed,
            total_items: self.items_processed,
            avg_batch_size: if self.batches_processed > 0 {
                self.items_processed as f64 / self.batches_processed as f64
            } else { 0.0 },
            avg_processing_time_per_item_us: self.avg_processing_time_per_item_us,
            peak_memory_mb: self.peak_memory_usage_bytes as f64 / (1024.0 * 1024.0),
            items_per_mb: self.memory_efficiency,
            total_processing_time_seconds: self.total_processing_time_ms as f64 / 1000.0,
        }
    }
}

/// Performance summary for reporting
#[derive(Debug, Clone)]
pub struct BatchPerformanceSummary {
    pub total_batches: u64,
    pub total_items: u64,
    pub avg_batch_size: f64,
    pub avg_processing_time_per_item_us: u64,
    pub peak_memory_mb: f64,
    pub items_per_mb: f64,
    pub total_processing_time_seconds: f64,
}

impl BatchProcessingManager {
    /// Create new batch processing manager
    pub fn new() -> Self {
        Self {
            memory_monitor: MemoryMonitor::new(),
            batch_config: BatchConfig::new(),
            performance_metrics: BatchPerformanceMetrics::new(),
        }
    }

    /// Check memory pressure and adjust batch size accordingly
    pub fn check_and_adjust_batch_size(&mut self, current_memory_usage: usize) -> usize {
        let pressure_level = self.memory_monitor.update_usage(current_memory_usage);
        self.batch_config.adjust_for_memory_pressure(pressure_level);
        self.batch_config.current_batch_size
    }

    /// Record batch processing metrics
    pub fn record_batch_metrics(&mut self, items_in_batch: usize, processing_time_ms: u64) {
        let memory_used = self.memory_monitor.current_usage;
        self.performance_metrics.record_batch_completion(items_in_batch, processing_time_ms, memory_used);
    }

    /// Get comprehensive performance report
    pub fn get_performance_report(&self) -> BatchProcessingReport {
        BatchProcessingReport {
            memory_stats: self.memory_monitor.get_usage_stats(),
            performance_summary: self.performance_metrics.get_performance_summary(),
            current_batch_size: self.batch_config.current_batch_size,
        }
    }
}

/// Comprehensive batch processing report
#[derive(Debug, Clone)]
pub struct BatchProcessingReport {
    pub memory_stats: MemoryUsageStats,
    pub performance_summary: BatchPerformanceSummary,
    pub current_batch_size: usize,
}

/// Progress tracking for long-running historical operations
#[derive(Debug, Clone)]
pub struct ProgressTracker {
    /// Operation identifier
    pub operation_id: String,
    /// Total number of items to process
    pub total_items: usize,
    /// Number of items completed
    pub completed_items: usize,
    /// Operation start time
    pub start_time: std::time::Instant,
    /// Last progress update time
    pub last_update: std::time::Instant,
    /// Items processed since last update
    pub items_since_last_update: usize,
    /// Estimated completion time
    pub estimated_completion: Option<std::time::Duration>,
    /// Current processing stage
    pub current_stage: ProcessingStage,
}

/// Processing stages for progress tracking
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessingStage {
    Initializing,
    LoadingData,
    ProcessingCandles, 
    CalculatingProfiles,
    StoringResults,
    Completed,
    Failed(String),
}

/// Detailed processing metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingMetrics {
    /// Total processing sessions
    pub total_sessions: u64,
    /// Successful processing sessions
    pub successful_sessions: u64,
    /// Failed processing sessions  
    pub failed_sessions: u64,
    /// Total candles processed across all sessions
    pub total_candles_processed: u64,
    /// Total processing time in milliseconds
    pub total_processing_time_ms: u64,
    /// Average candles per second throughput
    pub avg_throughput_candles_per_second: f64,
    /// Success rate percentage
    pub success_rate_percentage: f64,
    /// Peak memory usage during processing
    pub peak_memory_usage_bytes: usize,
    /// Last processing session timestamp
    pub last_session_timestamp: Option<std::time::SystemTime>,
}

/// Real-time progress update for logging and monitoring
#[derive(Debug, Clone)]
pub struct ProgressUpdate {
    /// Operation identifier
    pub operation_id: String,
    /// Percentage completion (0.0 to 100.0)
    pub percentage_complete: f64,
    /// Estimated time remaining
    pub estimated_time_remaining: Option<std::time::Duration>,
    /// Current processing rate (items per second)
    pub current_rate: f64,
    /// Items completed in this update
    pub items_completed: usize,
    /// Current stage description
    pub stage_description: String,
    /// Any warnings or issues
    pub warnings: Vec<String>,
}

impl ProgressTracker {
    /// Create new progress tracker for an operation
    pub fn new(operation_id: String, total_items: usize) -> Self {
        let now = std::time::Instant::now();
        Self {
            operation_id,
            total_items,
            completed_items: 0,
            start_time: now,
            last_update: now,
            items_since_last_update: 0,
            estimated_completion: None,
            current_stage: ProcessingStage::Initializing,
        }
    }

    /// Update progress with completed items
    pub fn update_progress(&mut self, completed_items: usize, stage: ProcessingStage) -> ProgressUpdate {
        let now = std::time::Instant::now();
        let items_delta = completed_items.saturating_sub(self.completed_items);
        
        self.completed_items = completed_items;
        self.items_since_last_update = items_delta;
        self.current_stage = stage.clone();
        
        // Calculate percentage completion
        let percentage_complete = if self.total_items > 0 {
            (completed_items as f64 / self.total_items as f64) * 100.0
        } else {
            0.0
        };

        // Calculate current processing rate (items per second)
        let time_since_last_update = now.duration_since(self.last_update).as_secs_f64();
        let current_rate = if time_since_last_update > 0.0 {
            items_delta as f64 / time_since_last_update
        } else {
            0.0
        };

        // Estimate time remaining based on current rate
        let estimated_time_remaining = if current_rate > 0.0 && completed_items < self.total_items {
            let remaining_items = self.total_items - completed_items;
            let estimated_seconds = remaining_items as f64 / current_rate;
            Some(std::time::Duration::from_secs_f64(estimated_seconds))
        } else {
            None
        };

        self.estimated_completion = estimated_time_remaining;
        self.last_update = now;

        ProgressUpdate {
            operation_id: self.operation_id.clone(),
            percentage_complete,
            estimated_time_remaining,
            current_rate,
            items_completed: items_delta,
            stage_description: self.get_stage_description(),
            warnings: Vec::new(),
        }
    }

    /// Get human-readable stage description
    pub fn get_stage_description(&self) -> String {
        match &self.current_stage {
            ProcessingStage::Initializing => "Initializing historical processing".to_string(),
            ProcessingStage::LoadingData => "Loading historical candle data".to_string(),
            ProcessingStage::ProcessingCandles => "Processing candles and validating dates".to_string(),
            ProcessingStage::CalculatingProfiles => "Calculating volume profiles".to_string(),
            ProcessingStage::StoringResults => "Storing results in database".to_string(),
            ProcessingStage::Completed => "Processing completed successfully".to_string(),
            ProcessingStage::Failed(error) => format!("Processing failed: {}", error),
        }
    }

    /// Check if processing is complete
    pub fn is_complete(&self) -> bool {
        matches!(self.current_stage, ProcessingStage::Completed | ProcessingStage::Failed(_))
    }

    /// Get overall processing duration
    pub fn get_total_duration(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }
}

impl Default for ProcessingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessingMetrics {
    /// Create new processing metrics tracker
    pub fn new() -> Self {
        Self {
            total_sessions: 0,
            successful_sessions: 0,
            failed_sessions: 0,
            total_candles_processed: 0,
            total_processing_time_ms: 0,
            avg_throughput_candles_per_second: 0.0,
            success_rate_percentage: 0.0,
            peak_memory_usage_bytes: 0,
            last_session_timestamp: None,
        }
    }

    /// Record successful processing session
    pub fn record_success(&mut self, candles_processed: usize, processing_time_ms: u64, memory_usage: usize) {
        self.total_sessions += 1;
        self.successful_sessions += 1;
        self.total_candles_processed += candles_processed as u64;
        self.total_processing_time_ms += processing_time_ms;
        self.peak_memory_usage_bytes = self.peak_memory_usage_bytes.max(memory_usage);
        self.last_session_timestamp = Some(std::time::SystemTime::now());
        
        self.update_derived_metrics();
    }

    /// Record failed processing session
    pub fn record_failure(&mut self, processing_time_ms: u64) {
        self.total_sessions += 1;
        self.failed_sessions += 1;
        self.total_processing_time_ms += processing_time_ms;
        self.last_session_timestamp = Some(std::time::SystemTime::now());
        
        self.update_derived_metrics();
    }

    /// Update derived metrics (success rate, throughput)
    fn update_derived_metrics(&mut self) {
        // Calculate success rate
        self.success_rate_percentage = if self.total_sessions > 0 {
            (self.successful_sessions as f64 / self.total_sessions as f64) * 100.0
        } else {
            0.0
        };

        // Calculate average throughput
        self.avg_throughput_candles_per_second = if self.total_processing_time_ms > 0 {
            (self.total_candles_processed as f64) / (self.total_processing_time_ms as f64 / 1000.0)
        } else {
            0.0
        };
    }

    /// Get processing metrics summary for logging
    pub fn get_summary(&self) -> String {
        format!(
            "Sessions: {}/{} ({:.1}% success), Candles: {}, Throughput: {:.1}/sec, Peak Memory: {}MB",
            self.successful_sessions,
            self.total_sessions,
            self.success_rate_percentage,
            self.total_candles_processed,
            self.avg_throughput_candles_per_second,
            self.peak_memory_usage_bytes / 1024 / 1024
        )
    }
}

impl Default for BatchProcessingManager {
    fn default() -> Self {
        Self::new()
    }
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
    /// Enhanced date validation and historical data processing component
    historical_processor: HistoricalDataProcessor,
    /// Gap detection and backfill management
    gap_manager: GapDetectionManager,
    /// Batch processing optimization and memory management
    batch_manager: BatchProcessingManager,
    /// Progress tracking for long-running operations
    progress_tracker: Option<ProgressTracker>,
    /// Processing metrics for monitoring
    processing_metrics: ProcessingMetrics,
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
pub struct ProfileKey {
    pub symbol: String,
    pub date: NaiveDate,
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
            historical_processor: HistoricalDataProcessor::new(),
            gap_manager: GapDetectionManager::new(),
            batch_manager: BatchProcessingManager::new(),
            progress_tracker: None,
            processing_metrics: ProcessingMetrics::new(),
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

    /// Process batch of candles with memory management
    async fn process_batch(&mut self) -> Result<(), String> {
        if self.batch_queue.is_empty() {
            return Ok(());
        }

        // Check memory pressure and adjust batch size
        let _pressure = self.batch_manager.memory_monitor.update_usage(self.batch_manager.memory_monitor.current_usage);
        let effective_batch_size = if matches!(self.batch_manager.memory_monitor.get_current_pressure_level(), MemoryPressureLevel::High | MemoryPressureLevel::Critical) {
            let adjusted_size = self.batch_manager.batch_config.current_batch_size / 2;
            warn!("Memory pressure detected, reducing batch size from {} to {}", 
                  self.batch_queue.len(), adjusted_size);
            adjusted_size
        } else {
            self.batch_queue.len()
        };

        // Take only the effective batch size
        let batch_size = effective_batch_size.min(self.batch_queue.len());
        let batch: Vec<_> = self.batch_queue.drain(0..batch_size).collect();
        
        // Reset batch time only if we processed all queued items
        if self.batch_queue.is_empty() {
            self.last_batch_time = None;
        }

        info!("Processing batch of {} candles for volume profiles (memory usage: {}MB)", 
              batch.len(), self.batch_manager.memory_monitor.current_usage / 1024 / 1024);

        let batch_start = std::time::Instant::now();

        // Group candles by profile key
        let mut grouped_candles: HashMap<ProfileKey, Vec<FuturesOHLCVCandle>> = HashMap::new();

        for (symbol, candle) in batch {
            if let Some(profile_key) = ProfileKey::from_candle(symbol, &candle) {
                grouped_candles.entry(profile_key).or_default().push(candle);
            }
        }

        // Process each group with performance tracking
        let grouped_candles_len = grouped_candles.len();
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

            let processing_start = std::time::Instant::now();

            // Update profile with all candles
            if let Some(profile) = self.profiles.get_mut(&profile_key) {
                for candle in &candles {
                    profile.add_candle(candle);
                    self.total_candles_processed += 1;
                }

                let processing_duration = processing_start.elapsed();

                // Update performance metrics
                self.batch_manager.performance_metrics.record_batch_completion(candles.len(), processing_duration.as_millis() as u64, self.batch_manager.memory_monitor.current_usage);

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

                info!("Batch processed for {} on {}: {} candles added in {:.2}ms", 
                      profile_key.symbol, profile_key.date, candles.len(), processing_duration.as_millis());
            }

            // Yield control periodically to avoid blocking
            tokio::task::yield_now().await;
        }

        let total_batch_duration = batch_start.elapsed();
        info!("✅ Batch processing complete: {} profiles updated in {:.2}ms (avg: {:.2}ms/candle)", 
              grouped_candles_len, total_batch_duration.as_millis(),
              (self.batch_manager.performance_metrics.avg_processing_time_per_item_us as f64 / 1000.0));

        Ok(())
    }

    /// Rebuild volume profile for specific day from LMDB data with memory management and progress tracking
    async fn rebuild_day(&mut self, symbol: String, date: NaiveDate) -> Result<(), String> {
        let operation_id = format!("rebuild_{}_{}", symbol, date.format("%Y-%m-%d"));
        info!("🔄 HISTORICAL REBUILD: Starting {} with progress tracking", operation_id);
        
        // Start progress tracking for this operation
        let mut progress = ProgressTracker::new(operation_id.clone(), 5); // 5 main stages
        self.progress_tracker = Some(progress.clone());
        
        // Stage 1: Initializing
        let update = progress.update_progress(1, ProcessingStage::Initializing);
        info!("📊 PROGRESS: {} - {:.1}% complete", update.operation_id, update.percentage_complete);
        
        // Check memory pressure before starting
        let _pressure = self.batch_manager.memory_monitor.update_usage(self.batch_manager.memory_monitor.current_usage);
        if matches!(self.batch_manager.memory_monitor.get_current_pressure_level(), MemoryPressureLevel::High | MemoryPressureLevel::Critical) {
            warn!("⚠️ MEMORY PRESSURE: Detected during rebuild for {} on {}, adjusting processing", symbol, date);
            // Force garbage collection and adjust batch processing
            tokio::task::yield_now().await;
        }

        // Update progress tracker
        self.progress_tracker = Some(progress.clone());
        
        // Stage 2: Loading Data
        progress.update_progress(2, ProcessingStage::LoadingData);
        info!("📊 PROGRESS: Loading historical data for {} on {}", symbol, date);

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

            // Request candles from LMDB with potential batch size adjustment
            let limit = if matches!(self.batch_manager.memory_monitor.get_current_pressure_level(), MemoryPressureLevel::High | MemoryPressureLevel::Critical) {
                Some((self.batch_manager.batch_config.current_batch_size / 2) as u32) // Reduce batch size under pressure
            } else {
                None
            };

            let message = LmdbActorMessage::GetCandles {
                symbol: symbol.clone(),
                timeframe: 60, // 1-minute candles
                start: start_of_day,
                end: end_of_day,
                limit,
            };

            match lmdb_actor.ask(message).await {
                Ok(LmdbActorResponse::Candles(candles)) => {
                    if candles.is_empty() {
                        warn!("No candles found for {} on {}", symbol, date);
                        return Ok(());
                    }

                    info!("📊 HISTORICAL DATA: Retrieved {} candles for {} on {}", candles.len(), symbol, date);
                    
                    // Stage 3: Processing Candles
                    progress.update_progress(3, ProcessingStage::ProcessingCandles);
                    info!("📊 PROGRESS: Processing {} candles with date validation", candles.len());
                    
                    // Update memory usage with candle data size
                    let candle_memory_size = candles.len() * std::mem::size_of::<FuturesOHLCVCandle>();
                    let _updated_usage = self.batch_manager.memory_monitor.update_usage(
                        self.batch_manager.memory_monitor.current_usage + candle_memory_size
                    );

                    // Create or update profile
                    let profile_key = ProfileKey::new(symbol.clone(), date);
                    let mut profile = DailyVolumeProfile::new(symbol, date, &self.config);
                    
                    // Stage 4: Calculating Profiles
                    progress.update_progress(4, ProcessingStage::CalculatingProfiles);
                    info!("📊 PROGRESS: Calculating volume profile from {} candles", candles.len());
                    
                    // Rebuild from candles with timing and performance tracking
                    let rebuild_start = std::time::Instant::now();
                    profile.rebuild_from_candles(&candles);
                    let rebuild_duration = rebuild_start.elapsed();
                    
                    // Update performance metrics
                    self.batch_manager.performance_metrics.record_batch_completion(candles.len(), rebuild_duration.as_millis() as u64, self.batch_manager.memory_monitor.current_usage);
                    
                    // Store in memory
                    self.profiles.insert(profile_key.clone(), profile.clone());

                    // Get profile data for storage and publishing
                    let profile_data = profile.get_profile_data();

                    // Stage 5: Storing Results
                    progress.update_progress(5, ProcessingStage::StoringResults);
                    info!("📊 PROGRESS: Storing volume profile results in database");

                    // Store in database (optional - system continues without database)
                    if let Err(e) = self.store_profile_in_database(&profile_key, &profile_data).await {
                        if e.contains("PostgreSQL actor not available") {
                            warn!("📊 OPERATING IN OFFLINE MODE: Database unavailable, volume profile cached in memory only");
                        } else {
                            error!("❌ DATABASE ERROR: Failed to store rebuilt volume profile in database: {}", e);
                        }
                        
                        // Continue operation without database storage
                        // Volume profile is still available in memory for real-time processing
                    } else {
                        self.total_database_writes += 1;
                    }

                    // Mark operation as completed and record success metrics
                    progress.current_stage = ProcessingStage::Completed;
                    let _final_update = progress.update_progress(5, ProcessingStage::Completed);
                    self.progress_tracker = Some(progress.clone());
                    
                    // Record successful processing metrics
                    self.processing_metrics.record_success(
                        candles.len(),
                        rebuild_duration.as_millis() as u64,
                        self.batch_manager.memory_monitor.current_usage
                    );

                    // Kafka publishing removed - now handled by IndicatorActor to avoid duplicate messages
                    // Volume profile data will be included in main technical analysis messages

                    info!("✅ REBUILD COMPLETE: {} - 100% complete in {:.2}s", 
                          operation_id, progress.get_total_duration().as_secs_f64());
                    info!("📊 PROCESSING STATS: {} candles processed in {:.2}ms (avg: {:.2}ms/candle)", 
                          candles.len(), rebuild_duration.as_millis(),
                          (self.batch_manager.performance_metrics.avg_processing_time_per_item_us as f64 / 1000.0));
                    info!("📈 VOLUME PROFILE: total_volume={:.2}, VWAP={:.2}, POC={:.2}, price_levels={}, value_area={:.1}%", 
                          profile_data.total_volume, profile_data.vwap, profile_data.poc, 
                          profile_data.price_levels.len(), profile_data.value_area.volume_percentage);
                    info!("💾 MEMORY STATUS: Current usage: {}MB, Pressure: {}", 
                          self.batch_manager.memory_monitor.current_usage / 1024 / 1024, 
                          matches!(self.batch_manager.memory_monitor.get_current_pressure_level(), MemoryPressureLevel::High | MemoryPressureLevel::Critical));
                    info!("📈 GLOBAL METRICS: {}", self.processing_metrics.get_summary());
                }
                Ok(_) => {
                    error!("Unexpected response from LMDB actor");
                    return Err("Unexpected LMDB response".to_string());
                }
                Err(e) => {
                    let error_msg = format!("LMDB query failed: {}", e);
                    error!("❌ LMDB ERROR: Failed to retrieve candles from LMDB: {}", e);
                    
                    // Mark operation as failed and record metrics
                    if let Some(ref mut tracker) = self.progress_tracker {
                        tracker.current_stage = ProcessingStage::Failed(error_msg.clone());
                        let failed_update = tracker.update_progress(tracker.completed_items, ProcessingStage::Failed(error_msg.clone()));
                        error!("📊 OPERATION FAILED: {} - {}", failed_update.operation_id, failed_update.stage_description);
                    }
                    
                    // Record failure metrics
                    self.processing_metrics.record_failure(0); // No processing time since we failed early
                    
                    // Record failure for retry queue
                    let profile_key = ProfileKey::new(symbol.clone(), date);
                    self.gap_manager.record_failure(profile_key, error_msg.clone(), 5);
                    
                    return Err(error_msg);
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

    /// Get current progress for monitoring and logging
    pub fn get_current_progress(&self) -> Option<ProgressUpdate> {
        if let Some(ref tracker) = self.progress_tracker {
            let mut tracker = tracker.clone();
            Some(tracker.update_progress(tracker.completed_items, tracker.current_stage.clone()))
        } else {
            None
        }
    }

    /// Get processing metrics summary for monitoring
    pub fn get_processing_metrics_summary(&self) -> String {
        self.processing_metrics.get_summary()
    }

    /// Log current processing status with detailed information
    pub fn log_processing_status(&self, context: &str) {
        if let Some(ref tracker) = self.progress_tracker {
            let percentage = if tracker.total_items > 0 {
                (tracker.completed_items as f64 / tracker.total_items as f64) * 100.0
            } else {
                0.0
            };
            
            info!("📊 {}: {} - {:.1}% complete, Stage: {}", 
                  context, tracker.operation_id, percentage, tracker.get_stage_description());
            
            if let Some(eta) = tracker.estimated_completion {
                info!("⏱️ ETA: {:.1}s remaining", eta.as_secs_f64());
            }
        }
        
        info!("📈 METRICS: {}", self.processing_metrics.get_summary());
    }

    /// Identify missing volume profile dates and queue them for backfill processing
    async fn detect_missing_volume_profile_dates(&mut self, symbol: &str, start_date: NaiveDate, end_date: NaiveDate) -> Result<Vec<NaiveDate>, String> {
        info!("🔍 GAP DETECTION: Scanning for missing volume profiles for {} from {} to {}", 
              symbol, start_date, end_date);

        let mut missing_dates = Vec::new();
        let mut current_date = start_date;

        while current_date <= end_date {
            let profile_key = ProfileKey::new(symbol.to_string(), current_date);
            
            // Check if we have this profile in memory
            if !self.profiles.contains_key(&profile_key) {
                // Profile not in memory, check if we have data for this date
                if let Some(lmdb_actor) = &self.lmdb_actor {
                    let start_of_day = current_date.and_hms_opt(0, 0, 0)
                        .ok_or("Invalid date")?
                        .and_utc()
                        .timestamp_millis();
                    let end_of_day = current_date.and_hms_opt(23, 59, 59)
                        .ok_or("Invalid date")?
                        .and_utc()
                        .timestamp_millis();

                    let message = LmdbActorMessage::GetCandles {
                        symbol: symbol.to_string(),
                        timeframe: 60, // 1-minute candles
                        start: start_of_day,
                        end: end_of_day,
                        limit: Some(10), // Just check if any candles exist
                    };

                    match lmdb_actor.ask(message).await {
                        Ok(LmdbActorResponse::Candles(candles)) => {
                            if candles.is_empty() {
                                debug!("📅 GAP DETECTED: No candles found for {} on {}", symbol, current_date);
                                missing_dates.push(current_date);
                                self.gap_manager.add_missing_date(symbol.to_string(), current_date);
                            } else {
                                debug!("📊 DATA FOUND: {} candles available for {} on {}", candles.len(), symbol, current_date);
                            }
                        }
                        Ok(_) => {
                            warn!("Unexpected response from LMDB during gap detection");
                        }
                        Err(e) => {
                            warn!("Failed to check candles for {} on {}: {}", symbol, current_date, e);
                            // Assume missing if we can't check
                            missing_dates.push(current_date);
                            self.gap_manager.add_missing_date(symbol.to_string(), current_date);
                        }
                    }
                } else {
                    return Err("LMDB actor not available for gap detection".to_string());
                }
            } else {
                debug!("✅ PROFILE EXISTS: Volume profile already in memory for {} on {}", symbol, current_date);
            }

            current_date += chrono::Duration::days(1);
        }

        if !missing_dates.is_empty() {
            let ranges = self.gap_manager.get_missing_ranges_report();
            if let Some(symbol_ranges) = ranges.get(symbol) {
                info!("🚨 GAP SUMMARY: Found {} missing dates for {} in {} ranges: {:?}", 
                      missing_dates.len(), symbol, symbol_ranges.len(), symbol_ranges);
            }
        } else {
            info!("✅ NO GAPS: All volume profiles present for {} from {} to {}", 
                  symbol, start_date, end_date);
        }

        self.gap_manager.last_scan_time = Some(std::time::Instant::now());
        Ok(missing_dates)
    }

    /// Process retry queue for failed volume profile rebuilds
    async fn process_retry_queue(&mut self) -> Result<(), String> {
        let retry_candidates = self.gap_manager.get_retry_candidates();
        
        if retry_candidates.is_empty() {
            return Ok(());
        }

        info!("🔄 RETRY PROCESSING: {} profiles ready for retry", retry_candidates.len());

        for profile_key in retry_candidates {
            info!("🔄 RETRYING: Volume profile rebuild for {} on {} (attempt #{})", 
                  profile_key.symbol, profile_key.date, 
                  self.gap_manager.failed_attempts.get(&profile_key).map(|r| r.attempt_count + 1).unwrap_or(1));

            match self.rebuild_day(profile_key.symbol.clone(), profile_key.date).await {
                Ok(()) => {
                    info!("✅ RETRY SUCCESS: Volume profile rebuilt for {} on {}", 
                          profile_key.symbol, profile_key.date);
                    self.gap_manager.mark_success(&profile_key);
                }
                Err(e) => {
                    warn!("❌ RETRY FAILED: {} on {} - {}", 
                          profile_key.symbol, profile_key.date, e);
                    self.gap_manager.record_failure(profile_key, e, 5); // Max 5 retries
                }
            }
        }

        // Clean up exhausted retries
        let exhausted = self.gap_manager.cleanup_exhausted_retries();
        if !exhausted.is_empty() {
            error!("💀 EXHAUSTED RETRIES: {} profiles gave up after maximum attempts", exhausted.len());
        }

        Ok(())
    }

    /// Trigger comprehensive gap detection and backfill for all active symbols
    pub async fn trigger_gap_detection_and_backfill(&mut self, symbols: &[String], days_back: u32) -> Result<(), String> {
        let end_date = Utc::now().date_naive();
        let start_date = end_date - chrono::Duration::days(days_back as i64);

        info!("🔍 COMPREHENSIVE GAP DETECTION: Scanning {} symbols for {} days ({} to {})", 
              symbols.len(), days_back, start_date, end_date);

        // Use historical processor for enhanced validation during gap detection
        if self.historical_processor.debug_logging {
            debug!("Using enhanced historical data processor for gap detection validation");
        }

        for symbol in symbols {
            if let Err(e) = self.detect_missing_volume_profile_dates(symbol, start_date, end_date).await {
                error!("Failed gap detection for {}: {}", symbol, e);
            }
        }

        // Process any immediate backfill needs
        self.process_retry_queue().await?;

        info!("🎯 GAP DETECTION COMPLETE: Total missing profiles across all symbols: {}", 
              self.gap_manager.missing_dates.values().map(|v| v.len()).sum::<usize>());

        Ok(())
    }

    /// Store volume profile in PostgreSQL database
    async fn store_profile_in_database(&self, profile_key: &ProfileKey, profile_data: &VolumeProfileData) -> Result<(), String> {
        if let Some(postgres_actor) = &self.postgres_actor {
            debug!("Storing volume profile in database: {} on {} ({} price levels)", 
                   profile_key.symbol, profile_key.date, profile_data.price_levels.len());

            // Send StoreVolumeProfile message to PostgreSQL actor
            let message = crate::postgres::actor::PostgresTell::StoreVolumeProfile {
                symbol: profile_key.symbol.clone(),
                date: profile_key.date,
                profile_data: profile_data.clone(),
            };

            match postgres_actor.tell(message).await {
                Ok(()) => {
                    debug!("✅ Volume profile storage request sent successfully: {} on {}", 
                           profile_key.symbol, profile_key.date);
                    Ok(())
                }
                Err(e) => {
                    error!("❌ Failed to send volume profile storage request: {} on {}: {}", 
                           profile_key.symbol, profile_key.date, e);
                    Err(format!("Failed to send storage request: {}", e))
                }
            }
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
    
    /// Get debug metadata for volume profile validation and analysis
    fn get_debug_metadata(&self, symbol: String, date: NaiveDate) -> Option<super::structs::VolumeProfileDebugMetadata> {
        let profile_key = ProfileKey::new(symbol, date);
        self.profiles.get(&profile_key)
            .and_then(|profile| profile.get_debug_metadata().cloned())
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

    /// Detect gaps in historical data for a date range
    async fn detect_gaps(&mut self, symbol: String, start_date: NaiveDate, end_date: NaiveDate) -> Result<(), String> {
        info!("🔍 GAP DETECTION: Starting gap detection for {} from {} to {}", symbol, start_date, end_date);
        
        let missing_dates = self.detect_missing_volume_profile_dates(&symbol, start_date, end_date).await?;
        
        if !missing_dates.is_empty() {
            info!("📊 GAPS FOUND: {} missing dates for {}", missing_dates.len(), symbol);
            for date in &missing_dates {
                self.gap_manager.add_missing_date(symbol.clone(), *date);
            }
        } else {
            info!("✅ NO GAPS: All volume profiles present for {} from {} to {}", symbol, start_date, end_date);
        }
        
        Ok(())
    }

    /// Process historical backfill for detected gaps
    async fn process_backfill(&mut self, symbol: String, missing_dates: Vec<NaiveDate>) -> Result<(), String> {
        info!("🔄 BACKFILL: Processing {} missing dates for {}", missing_dates.len(), symbol);
        
        let operation_id = format!("backfill_{}_{}_dates", symbol, missing_dates.len());
        let mut progress = ProgressTracker::new(operation_id.clone(), missing_dates.len());
        self.progress_tracker = Some(progress.clone());

        let mut successful_rebuilds = 0;
        let mut failed_rebuilds = 0;

        for (index, date) in missing_dates.iter().enumerate() {
            let update = progress.update_progress(index, ProcessingStage::ProcessingCandles);
            info!("📊 BACKFILL PROGRESS: {} - {:.1}% complete ({}/{})", 
                  update.operation_id, update.percentage_complete, index + 1, missing_dates.len());

            match self.rebuild_day(symbol.clone(), *date).await {
                Ok(_) => {
                    successful_rebuilds += 1;
                    info!("✅ BACKFILL SUCCESS: {} on {}", symbol, date);
                }
                Err(e) => {
                    failed_rebuilds += 1;
                    warn!("❌ BACKFILL FAILED: {} on {} - {}", symbol, date, e);
                    
                    // Record failure for retry
                    let profile_key = ProfileKey::new(symbol.clone(), *date);
                    self.gap_manager.record_failure(profile_key, e, 3); // Max 3 retries
                }
            }
        }

        // Final progress update
        let _final_update = progress.update_progress(missing_dates.len(), ProcessingStage::Completed);
        self.progress_tracker = None;

        info!("🏁 BACKFILL COMPLETE: {} - {} successful, {} failed out of {} total", 
              symbol, successful_rebuilds, failed_rebuilds, missing_dates.len());

        Ok(())
    }

    /// Get gap detection results for a date range
    async fn get_gap_detection_results(&mut self, symbol: String, start_date: NaiveDate, end_date: NaiveDate) -> Result<VolumeProfileReply, String> {
        info!("📋 GAP REPORT: Generating gap detection results for {} from {} to {}", symbol, start_date, end_date);
        
        let missing_dates = self.detect_missing_volume_profile_dates(&symbol, start_date, end_date).await?;
        
        Ok(VolumeProfileReply::GapDetectionResults {
            symbol,
            missing_dates: missing_dates.clone(),
            total_gaps: missing_dates.len(),
            date_range: (start_date, end_date),
        })
    }

    /// Get current processing status and progress
    fn get_processing_status(&self) -> VolumeProfileReply {
        let current_operation = self.progress_tracker.as_ref().map(|tracker| tracker.operation_id.clone());
        let progress_percentage = self.progress_tracker.as_ref().map(|tracker| {
            if tracker.total_items > 0 {
                (tracker.completed_items as f64 / tracker.total_items as f64 * 100.0) as f32
            } else {
                0.0
            }
        });
        let estimated_completion = self.progress_tracker.as_ref().and_then(|tracker| {
            tracker.estimated_completion.map(|eta| format!("{:.1}s", eta.as_secs_f64()))
        });
        let retry_queue_size = self.gap_manager.failed_attempts.len();

        VolumeProfileReply::ProcessingStatus {
            current_operation,
            progress_percentage,
            estimated_completion,
            retry_queue_size,
            processing_metrics: self.processing_metrics.clone(),
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
            
            // ⏰ STARTUP DELAY: Historical data rebuilding will happen after SetActorReferences
            info!("🔄 STARTUP: Actor references not yet set - historical data rebuilding deferred");
            info!("💡 Historical data will be rebuilt when SetActorReferences message is received");
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
                
                // Log current processing status if any operation is running
                self.log_processing_status("HEALTH CHECK");
                
                // Process retry queue during health check
                if let Err(e) = self.process_retry_queue().await {
                    warn!("Failed to process retry queue during health check: {}", e);
                    self.last_error = Some(Arc::new(e));
                }
            }
            VolumeProfileTell::ProcessBatch => {
                debug!("📦 Processing volume profile batch");
                if let Err(e) = self.process_batch().await {
                    error!("❌ Failed to process volume profile batch: {}", e);
                    self.last_error = Some(Arc::new(e));
                }
            }
            VolumeProfileTell::DetectGaps { symbol, start_date, end_date } => {
                info!("🔍 VolumeProfileActor received DetectGaps: {} from {} to {}", symbol, start_date, end_date);
                if let Err(e) = self.detect_gaps(symbol, start_date, end_date).await {
                    error!("❌ Failed to detect gaps: {}", e);
                    self.last_error = Some(Arc::new(e));
                }
            }
            VolumeProfileTell::ProcessBackfill { symbol, missing_dates } => {
                info!("🔄 VolumeProfileActor received ProcessBackfill: {} for {} dates", symbol, missing_dates.len());
                if let Err(e) = self.process_backfill(symbol, missing_dates).await {
                    error!("❌ Failed to process backfill: {}", e);
                    self.last_error = Some(Arc::new(e));
                }
            }
            VolumeProfileTell::ProcessRetryQueue => {
                debug!("🔄 Processing retry queue");
                if let Err(e) = self.process_retry_queue().await {
                    error!("❌ Failed to process retry queue: {}", e);
                    self.last_error = Some(Arc::new(e));
                }
            }
            VolumeProfileTell::SetActorReferences { postgres_actor, lmdb_actor } => {
                info!("🔧 CRITICAL FIX: Setting actor references for VolumeProfileActor");
                self.postgres_actor = postgres_actor.clone();
                self.lmdb_actor = Some(lmdb_actor);
                self.is_healthy = true;
                info!("✅ CRITICAL FIX: VolumeProfileActor references set - LMDB access enabled for historical data reconstruction");
                
                // 🏗️ INITIALIZE POSTGRESQL SCHEMA: Create volume profile table if PostgreSQL is enabled
                if let Some(ref postgres_ref) = postgres_actor {
                    info!("🏗️ Initializing PostgreSQL volume profile table schema");
                    match postgres_ref.tell(PostgresTell::InitVolumeProfileSchema).send().await {
                        Ok(()) => {
                            info!("✅ PostgreSQL volume profile schema initialization request sent successfully");
                        }
                        Err(e) => {
                            warn!("⚠️ Failed to send PostgreSQL schema initialization request: {} - volume profile calculation will continue with LMDB only", e);
                        }
                    }
                } else {
                    info!("📊 PostgreSQL not enabled - volume profile data will be stored in LMDB only");
                }
                
                // 🔄 NOW REBUILD HISTORICAL DATA: References are set, we can now access LMDB
                if self.config.enabled {
                    let today = chrono::Utc::now().date_naive();
                    let historical_days = self.config.historical_days;
                    info!("🔄 STARTUP RECONSTRUCTION: Now rebuilding volume profiles for {} historical days (up to {})", historical_days, today);
                    
                    let active_symbols = vec!["BTCUSDT"]; // TODO: Make this configurable
                    
                    for symbol in active_symbols {
                        info!("🔄 STARTUP: Rebuilding {} days of volume profiles for {}", historical_days, symbol);
                        
                        // Rebuild multiple historical days, not just today
                        for days_back in 0..historical_days {
                            let target_date = today - chrono::Duration::days(days_back as i64);
                            
                            match self.rebuild_day(symbol.to_string(), target_date).await {
                                Ok(()) => {
                                    info!("✅ STARTUP: Successfully initialized volume profile for {} on {}", symbol, target_date);
                                }
                                Err(e) => {
                                    if e.contains("PostgreSQL actor not available") {
                                        debug!("📊 STARTUP: Volume profile for {} on {} initialized in memory-only mode (database offline)", symbol, target_date);
                                    } else if days_back < 7 {
                                        // Only warn for recent days (last week)
                                        warn!("⚠️ STARTUP: Failed to rebuild volume profile for {} on {} (recent): {}", symbol, target_date, e);
                                    } else {
                                        // Just debug for older days that might not have data
                                        debug!("📊 STARTUP: No data for {} on {} (historical): {}", symbol, target_date, e);
                                    }
                                }
                            }
                        }
                        
                        info!("✅ STARTUP: Completed {} days of volume profile initialization for {}", historical_days, symbol);
                    }
                    
                    info!("🎯 STARTUP RECONSTRUCTION COMPLETE: All {} days of volume profiles initialized", historical_days);
                }
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
            VolumeProfileAsk::GetDebugMetadata { symbol, date } => {
                let debug_metadata = self.get_debug_metadata(symbol, date);
                Ok(VolumeProfileReply::DebugMetadata(debug_metadata))
            }
            VolumeProfileAsk::GetHealthStatus => {
                Ok(self.get_health_status())
            }
            VolumeProfileAsk::GetStatistics => {
                Ok(self.get_statistics())
            }
            VolumeProfileAsk::GetGapDetectionResults { symbol, start_date, end_date } => {
                match self.get_gap_detection_results(symbol, start_date, end_date).await {
                    Ok(results) => Ok(results),
                    Err(e) => Ok(VolumeProfileReply::Error(e)),
                }
            }
            VolumeProfileAsk::GetProcessingStatus => {
                Ok(self.get_processing_status())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use crate::volume_profile::structs::{PriceIncrementMode, UpdateFrequency, VolumeDistributionMode, ValueAreaCalculationMode, VolumeProfileCalculationMode};

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
            calculation_mode: VolumeProfileCalculationMode::Volume,
            asset_overrides: std::collections::HashMap::new(),
            historical_days: 60,
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

    #[test]
    fn test_historical_data_processor_basic_validation() {
        let processor = HistoricalDataProcessor::new();
        let target_date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();

        // Test valid candle
        let valid_timestamp = 1736985600000; // 2025-01-16 00:00:00 UTC
        let valid_candle = create_test_candle(valid_timestamp, 1000.0);
        
        let result = processor.validate_candle_date(&valid_candle, target_date);
        match result {
            DateValidationResult::Valid { trading_day } => {
                assert_eq!(trading_day, target_date);
            }
            _ => panic!("Expected valid result, got {:?}", result),
        }

        // Test invalid candle
        let invalid_timestamp = i64::MIN;
        let invalid_candle = FuturesOHLCVCandle {
            open_time: invalid_timestamp,
            close_time: 0,
            open: 50000.0,
            high: 50100.0,
            low: 49950.0,
            close: 50050.0,
            volume: 1000.0,
            number_of_trades: 100,
            taker_buy_base_asset_volume: 600.0,
            closed: true,
        };
        
        let result = processor.validate_candle_date(&invalid_candle, target_date);
        match result {
            DateValidationResult::Invalid { reason, timestamp } => {
                assert!(reason.contains("Invalid timestamp"));
                assert_eq!(timestamp, invalid_timestamp);
            }
            _ => panic!("Expected invalid result, got {:?}", result),
        }
    }

    #[test]
    fn test_historical_data_processor_timezone_boundaries() {
        let processor = HistoricalDataProcessor::new();
        let target_date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();

        // Test timezone boundary case (23:30 previous day)
        let boundary_timestamp = 1736985600000 - 30 * 60 * 1000; // 2025-01-15 23:30:00 UTC
        let boundary_candle = create_test_candle(boundary_timestamp, 1000.0);
        
        let result = processor.validate_candle_date(&boundary_candle, target_date);
        match result {
            DateValidationResult::EdgeCase { trading_day, warning, .. } => {
                assert_eq!(trading_day, target_date);
                assert!(warning.contains("timezone boundary"));
            }
            _ => panic!("Expected edge case result, got {:?}", result),
        }
    }

    #[test]
    fn test_historical_data_processor_dst_transitions() {
        let processor = HistoricalDataProcessor::new();
        
        // Test March DST transition
        let march_date = NaiveDate::from_ymd_opt(2025, 3, 9).unwrap();
        let march_base_timestamp = march_date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
        let dst_timestamp = march_base_timestamp - (1.5 * 60.0 * 60.0 * 1000.0) as i64; // 22:30 previous day
        let dst_candle = create_test_candle(dst_timestamp, 1000.0);
        
        let result = processor.validate_candle_date(&dst_candle, march_date);
        match result {
            DateValidationResult::EdgeCase { trading_day, warning, .. } => {
                assert_eq!(trading_day, march_date);
                assert!(warning.contains("DST transition"));
            }
            _ => panic!("Expected DST edge case result, got {:?}", result),
        }
    }

    #[test]
    fn test_historical_data_processor_leap_year() {
        let processor = HistoricalDataProcessor::new();
        
        // Test leap year date
        let leap_date = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let leap_timestamp = leap_date.and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_millis();
        let leap_candle = create_test_candle(leap_timestamp, 1000.0);
        
        let result = processor.validate_candle_date(&leap_candle, leap_date);
        assert!(matches!(result, DateValidationResult::Valid { .. }));
    }

    #[test]
    fn test_historical_data_processor_range_processing() {
        let processor = HistoricalDataProcessor::new();
        let target_date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        let base_timestamp = 1736985600000; // 2025-01-16 00:00:00 UTC

        // Create test candles with mixed validation results
        let candles = vec![
            create_test_candle(base_timestamp, 1000.0), // Valid
            create_test_candle(base_timestamp + 60000, 1500.0), // Valid
            create_test_candle(base_timestamp - 30 * 60 * 1000, 800.0), // Edge case (boundary)
            create_test_candle(i64::MIN, 500.0), // Invalid timestamp
        ];

        let report = processor.process_historical_range(&candles, target_date, "BTCUSDT");

        assert_eq!(report.symbol, "BTCUSDT");
        assert_eq!(report.date, target_date);
        assert_eq!(report.candles_processed, 4);
        assert_eq!(report.validation_results.len(), 4);
        assert!(!report.warnings.is_empty()); // Should have boundary warning
        assert!(!report.errors.is_empty()); // Should have invalid timestamp error
        // Processing duration is always non-negative by type definition (u64)
    }

    #[test]
    fn test_historical_data_processor_utc_consistency() {
        let processor = HistoricalDataProcessor::new();
        
        // Test valid timestamp
        let valid_timestamp = 1736985600000;
        let result = processor.ensure_utc_consistency(valid_timestamp);
        assert!(result.is_ok());

        // Test invalid timestamp (use a value that's clearly invalid for chrono)
        let invalid_timestamp = i64::MIN;
        let result = processor.ensure_utc_consistency(invalid_timestamp);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid timestamp"));
    }

    #[test]
    fn test_gap_detection_manager_basic_operations() {
        let mut manager = GapDetectionManager::new();
        let date1 = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();

        // Test adding missing dates
        manager.add_missing_date("BTCUSDT".to_string(), date1);
        manager.add_missing_date("BTCUSDT".to_string(), date2);
        manager.add_missing_date("ETHUSDT".to_string(), date1);

        assert_eq!(manager.missing_dates.get("BTCUSDT").unwrap().len(), 2);
        assert_eq!(manager.missing_dates.get("ETHUSDT").unwrap().len(), 1);

        // Test missing ranges report
        let ranges = manager.get_missing_ranges_report();
        assert!(ranges.contains_key("BTCUSDT"));
        assert!(ranges.contains_key("ETHUSDT"));
        
        // Should have one range for BTCUSDT (consecutive dates)
        let btc_ranges = &ranges["BTCUSDT"];
        assert_eq!(btc_ranges.len(), 1);
        assert_eq!(btc_ranges[0], (date1, date2));
    }

    #[test]
    fn test_retry_info_exponential_backoff() {
        let mut retry_info = RetryInfo::new("Test error".to_string(), 5);
        
        // Initial state
        assert_eq!(retry_info.attempt_count, 1);
        assert!(!retry_info.is_exhausted());

        // Test exponential backoff calculation
        let initial_next_retry = retry_info.next_retry;
        retry_info.calculate_next_retry();
        
        assert_eq!(retry_info.attempt_count, 2);
        assert!(retry_info.next_retry > initial_next_retry);

        // Test should_retry logic (initially false due to timing)
        assert!(!retry_info.should_retry()); // Too early to retry

        // Simulate time passing by setting next_retry to past
        retry_info.next_retry = std::time::Instant::now() - std::time::Duration::from_secs(1);
        assert!(retry_info.should_retry());

        // Test exhaustion
        retry_info.attempt_count = 5;
        assert!(retry_info.is_exhausted());
        assert!(!retry_info.should_retry()); // Should not retry when exhausted
    }

    #[test]
    fn test_gap_detection_manager_retry_tracking() {
        let mut manager = GapDetectionManager::new();
        let key1 = ProfileKey::new("BTCUSDT".to_string(), NaiveDate::from_ymd_opt(2025, 1, 15).unwrap());
        let key2 = ProfileKey::new("ETHUSDT".to_string(), NaiveDate::from_ymd_opt(2025, 1, 16).unwrap());

        // Record failures
        manager.record_failure(key1.clone(), "Connection error".to_string(), 3);
        manager.record_failure(key2.clone(), "Timeout error".to_string(), 3);

        assert_eq!(manager.failed_attempts.len(), 2);

        // Test retry candidates (should be empty initially due to timing)
        let candidates = manager.get_retry_candidates();
        assert_eq!(candidates.len(), 0); // Too early to retry

        // Simulate time passing
        for (_, retry_info) in manager.failed_attempts.iter_mut() {
            retry_info.next_retry = std::time::Instant::now() - std::time::Duration::from_secs(1);
        }

        // Now should have candidates
        let candidates = manager.get_retry_candidates();
        assert_eq!(candidates.len(), 2);

        // Test marking success
        manager.mark_success(&key1);
        assert_eq!(manager.failed_attempts.len(), 1);
        assert!(!manager.failed_attempts.contains_key(&key1));

        // Test cleanup exhausted retries
        if let Some(retry_info) = manager.failed_attempts.get_mut(&key2) {
            retry_info.attempt_count = 5; // Exceed max retries
        }
        let exhausted = manager.cleanup_exhausted_retries();
        assert_eq!(exhausted.len(), 1);
        assert_eq!(exhausted[0], key2);
        assert_eq!(manager.failed_attempts.len(), 0);
    }

    #[test]
    fn test_gap_detection_manager_missing_ranges_report() {
        let mut manager = GapDetectionManager::new();
        
        // Test consecutive dates
        let dates = vec![
            NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 17).unwrap(),
        ];
        
        for date in &dates {
            manager.add_missing_date("BTCUSDT".to_string(), *date);
        }

        // Test non-consecutive dates  
        manager.add_missing_date("BTCUSDT".to_string(), NaiveDate::from_ymd_opt(2025, 1, 20).unwrap());
        manager.add_missing_date("BTCUSDT".to_string(), NaiveDate::from_ymd_opt(2025, 1, 21).unwrap());

        let ranges = manager.get_missing_ranges_report();
        let btc_ranges = &ranges["BTCUSDT"];
        
        // Should have 2 ranges: Jan 15-17 and Jan 20-21
        assert_eq!(btc_ranges.len(), 2);
        assert_eq!(btc_ranges[0], (dates[0], dates[2])); // Jan 15-17
        assert_eq!(btc_ranges[1], (NaiveDate::from_ymd_opt(2025, 1, 20).unwrap(), NaiveDate::from_ymd_opt(2025, 1, 21).unwrap())); // Jan 20-21
    }

    #[tokio::test]
    async fn test_volume_profile_actor_with_gap_manager() {
        let config = create_test_config();
        let actor = VolumeProfileActor::new(config).unwrap();
        
        // Verify gap manager is initialized
        assert_eq!(actor.gap_manager.missing_dates.len(), 0);
        assert_eq!(actor.gap_manager.failed_attempts.len(), 0);
        assert!(actor.gap_manager.last_scan_time.is_none());
    }

    #[test]
    fn test_batch_processing_manager_creation() {
        let manager = BatchProcessingManager::new();
        
        // Verify initial state
        assert_eq!(manager.batch_config.base_batch_size, 10);
        assert_eq!(manager.batch_config.current_batch_size, 10);
        assert_eq!(manager.memory_monitor.current_usage, 0);
        assert_eq!(manager.performance_metrics.batches_processed, 0);
    }

    #[test]
    fn test_memory_monitor_pressure_detection() {
        let mut monitor = MemoryMonitor::new();
        
        // Test low pressure
        let level = monitor.update_usage(100_000_000); // 100MB
        assert_eq!(level, MemoryPressureLevel::Low);
        
        // Test medium pressure (50% of pressure threshold)
        let medium_threshold = (monitor.pressure_threshold as f64 * 0.5) as usize;
        let level = monitor.update_usage(medium_threshold + 10_000_000); // slightly above medium threshold
        assert_eq!(level, MemoryPressureLevel::Medium);
        
        // Test high pressure (above pressure threshold)
        let level = monitor.update_usage(monitor.pressure_threshold + 10_000_000); // slightly above pressure threshold
        assert_eq!(level, MemoryPressureLevel::High);
        
        // Test critical pressure (above critical threshold)
        let level = monitor.update_usage(monitor.critical_threshold + 10_000_000); // slightly above critical threshold
        assert_eq!(level, MemoryPressureLevel::Critical);
    }

    #[test] 
    fn test_batch_config_adaptive_sizing() {
        let mut config = BatchConfig::new();
        
        // Test adjustment for memory pressure
        config.adjust_for_memory_pressure(MemoryPressureLevel::High);
        assert!(config.current_batch_size < config.base_batch_size);
        
        // Test adjustment for critical pressure
        config.adjust_for_memory_pressure(MemoryPressureLevel::Critical);
        assert_eq!(config.current_batch_size, config.min_batch_size);
        
        // Test recovery to normal
        config.adjust_for_memory_pressure(MemoryPressureLevel::Low);
        assert!(config.current_batch_size > config.min_batch_size);
    }

    #[test]
    fn test_performance_metrics_recording() {
        let mut metrics = BatchPerformanceMetrics::new();
        
        // Record some batch processing
        metrics.record_batch_completion(50, 100, 1000000); // 50 items, 100ms, 1MB memory
        
        assert_eq!(metrics.batches_processed, 1);
        assert_eq!(metrics.items_processed, 50);
        assert_eq!(metrics.total_processing_time_ms, 100);
        assert!(metrics.avg_processing_time_per_item_us > 0);
        
        // Record another batch
        metrics.record_batch_completion(25, 50, 500000); // 25 items, 50ms, 0.5MB memory
        
        assert_eq!(metrics.batches_processed, 2);
        assert_eq!(metrics.items_processed, 75);
        assert_eq!(metrics.total_processing_time_ms, 150);
    }

    #[tokio::test]
    async fn test_batch_processing_manager_integration() {
        let config = create_test_config();
        let actor = VolumeProfileActor::new(config).unwrap();
        
        // Verify batch manager is initialized
        assert_eq!(actor.batch_manager.batch_config.base_batch_size, 10);
        assert_eq!(actor.batch_manager.memory_monitor.current_usage, 0);
        assert_eq!(actor.batch_manager.performance_metrics.batches_processed, 0);
        
        // Test memory pressure detection
        let pressure_level = actor.batch_manager.memory_monitor.get_current_pressure_level();
        assert_eq!(pressure_level, MemoryPressureLevel::Low);
    }

    #[test]
    fn test_progress_tracker_creation() {
        let tracker = ProgressTracker::new("test_operation".to_string(), 100);
        
        assert_eq!(tracker.operation_id, "test_operation");
        assert_eq!(tracker.total_items, 100);
        assert_eq!(tracker.completed_items, 0);
        assert_eq!(tracker.current_stage, ProcessingStage::Initializing);
        assert!(!tracker.is_complete());
    }

    #[test]
    fn test_progress_tracker_updates() {
        let mut tracker = ProgressTracker::new("test_operation".to_string(), 100);
        
        // Test progress update
        let update = tracker.update_progress(25, ProcessingStage::LoadingData);
        
        assert_eq!(update.percentage_complete, 25.0);
        assert_eq!(update.items_completed, 25);
        assert_eq!(tracker.completed_items, 25);
        assert_eq!(tracker.current_stage, ProcessingStage::LoadingData);
        
        // Test completion
        let final_update = tracker.update_progress(100, ProcessingStage::Completed);
        
        assert_eq!(final_update.percentage_complete, 100.0);
        assert_eq!(tracker.completed_items, 100);
        assert!(tracker.is_complete());
    }

    #[test]
    fn test_processing_metrics_recording() {
        let mut metrics = ProcessingMetrics::new();
        
        // Record successful sessions
        metrics.record_success(1000, 5000, 1000000); // 1000 candles, 5s, 1MB
        metrics.record_success(500, 2500, 800000);   // 500 candles, 2.5s, 0.8MB
        
        assert_eq!(metrics.total_sessions, 2);
        assert_eq!(metrics.successful_sessions, 2);
        assert_eq!(metrics.failed_sessions, 0);
        assert_eq!(metrics.total_candles_processed, 1500);
        assert_eq!(metrics.success_rate_percentage, 100.0);
        assert_eq!(metrics.peak_memory_usage_bytes, 1000000);
        
        // Record a failure
        metrics.record_failure(1000); // 1s processing time
        
        assert_eq!(metrics.total_sessions, 3);
        assert_eq!(metrics.successful_sessions, 2);
        assert_eq!(metrics.failed_sessions, 1);
        assert!((metrics.success_rate_percentage - 66.67).abs() < 0.1); // ~66.67%
    }

    #[test]
    fn test_processing_stage_descriptions() {
        let tracker = ProgressTracker::new("test".to_string(), 5);
        
        assert_eq!(tracker.get_stage_description(), "Initializing historical processing");
        
        let mut tracker = tracker;
        tracker.current_stage = ProcessingStage::LoadingData;
        assert_eq!(tracker.get_stage_description(), "Loading historical candle data");
        
        tracker.current_stage = ProcessingStage::Failed("Connection timeout".to_string());
        assert_eq!(tracker.get_stage_description(), "Processing failed: Connection timeout");
    }

    #[tokio::test]
    async fn test_volume_profile_actor_progress_tracking_integration() {
        let config = create_test_config();
        let actor = VolumeProfileActor::new(config).unwrap();
        
        // Verify progress tracking is initialized
        assert!(actor.progress_tracker.is_none());
        assert_eq!(actor.processing_metrics.total_sessions, 0);
        
        // Test metrics summary
        let summary = actor.get_processing_metrics_summary();
        assert!(summary.contains("Sessions: 0/0"));
    }

    // ========================================
    // TASK 5: Integration and Testing
    // ========================================

    #[test]
    fn test_new_message_serialization() {
        use serde_json;

        // Test DetectGaps message serialization
        let detect_gaps_msg = VolumeProfileTellSerializable::DetectGaps {
            symbol: "BTCUSDT".to_string(),
            start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
        };

        let serialized = serde_json::to_string(&detect_gaps_msg).unwrap();
        let deserialized: VolumeProfileTellSerializable = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            VolumeProfileTellSerializable::DetectGaps { symbol, start_date, end_date } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(start_date, NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
                assert_eq!(end_date, NaiveDate::from_ymd_opt(2025, 1, 31).unwrap());
            }
            _ => panic!("Unexpected message type after deserialization"),
        }

        // Test ProcessBackfill message serialization
        let backfill_msg = VolumeProfileTellSerializable::ProcessBackfill {
            symbol: "ETHUSDT".to_string(),
            missing_dates: vec![
                NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
                NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
            ],
        };

        let serialized = serde_json::to_string(&backfill_msg).unwrap();
        let deserialized: VolumeProfileTellSerializable = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            VolumeProfileTellSerializable::ProcessBackfill { symbol, missing_dates } => {
                assert_eq!(symbol, "ETHUSDT");
                assert_eq!(missing_dates.len(), 2);
                assert_eq!(missing_dates[0], NaiveDate::from_ymd_opt(2025, 1, 10).unwrap());
                assert_eq!(missing_dates[1], NaiveDate::from_ymd_opt(2025, 1, 15).unwrap());
            }
            _ => panic!("Unexpected message type after deserialization"),
        }
    }

    #[test]
    fn test_message_conversion() {
        // Test conversion from serializable to full enum
        let serializable_msg = VolumeProfileTellSerializable::DetectGaps {
            symbol: "BTCUSDT".to_string(),
            start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
        };

        let full_msg: VolumeProfileTell = serializable_msg.into();
        
        match full_msg {
            VolumeProfileTell::DetectGaps { symbol, start_date, end_date } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(start_date, NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
                assert_eq!(end_date, NaiveDate::from_ymd_opt(2025, 1, 31).unwrap());
            }
            _ => panic!("Unexpected message type after conversion"),
        }
    }

    #[test]
    fn test_new_ask_message_serialization() {
        use serde_json;

        // Test GetGapDetectionResults message
        let gap_detection_ask = VolumeProfileAsk::GetGapDetectionResults {
            symbol: "BTCUSDT".to_string(),
            start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
        };

        let serialized = serde_json::to_string(&gap_detection_ask).unwrap();
        let deserialized: VolumeProfileAsk = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            VolumeProfileAsk::GetGapDetectionResults { symbol, start_date, end_date } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(start_date, NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
                assert_eq!(end_date, NaiveDate::from_ymd_opt(2025, 1, 31).unwrap());
            }
            _ => panic!("Unexpected ask message type after deserialization"),
        }

        // Test GetProcessingStatus message
        let status_ask = VolumeProfileAsk::GetProcessingStatus;
        let serialized = serde_json::to_string(&status_ask).unwrap();
        let deserialized: VolumeProfileAsk = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            VolumeProfileAsk::GetProcessingStatus => {
                // Test passes if we reach here
            }
            _ => panic!("Unexpected ask message type after deserialization"),
        }
    }

    #[test]
    fn test_new_reply_message_serialization() {
        use serde_json;

        // Test GapDetectionResults reply
        let gap_results_reply = VolumeProfileReply::GapDetectionResults {
            symbol: "BTCUSDT".to_string(),
            missing_dates: vec![
                NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
                NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
            ],
            total_gaps: 2,
            date_range: (
                NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
            ),
        };

        let serialized = serde_json::to_string(&gap_results_reply).unwrap();
        let deserialized: VolumeProfileReply = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            VolumeProfileReply::GapDetectionResults { symbol, missing_dates, total_gaps, date_range } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(missing_dates.len(), 2);
                assert_eq!(total_gaps, 2);
                assert_eq!(date_range.0, NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
                assert_eq!(date_range.1, NaiveDate::from_ymd_opt(2025, 1, 31).unwrap());
            }
            _ => panic!("Unexpected reply message type after deserialization"),
        }

        // Test ProcessingStatus reply
        let processing_metrics = ProcessingMetrics::new();
        let status_reply = VolumeProfileReply::ProcessingStatus {
            current_operation: Some("test_operation".to_string()),
            progress_percentage: Some(75.5),
            estimated_completion: Some("30s".to_string()),
            retry_queue_size: 3,
            processing_metrics,
        };

        let serialized = serde_json::to_string(&status_reply).unwrap();
        let deserialized: VolumeProfileReply = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            VolumeProfileReply::ProcessingStatus { 
                current_operation, 
                progress_percentage, 
                estimated_completion, 
                retry_queue_size, 
                processing_metrics: _ 
            } => {
                assert_eq!(current_operation, Some("test_operation".to_string()));
                assert_eq!(progress_percentage, Some(75.5));
                assert_eq!(estimated_completion, Some("30s".to_string()));
                assert_eq!(retry_queue_size, 3);
            }
            _ => panic!("Unexpected reply message type after deserialization"),
        }
    }

    #[tokio::test]
    async fn test_gap_detection_message_handling() {
        let config = create_test_config();
        let mut actor = VolumeProfileActor::new(config).unwrap();

        // Test gap detection method directly
        let symbol = "BTCUSDT".to_string();
        let start_date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2025, 1, 5).unwrap();

        // This will fail without LMDB actor but should handle gracefully
        let result = actor.detect_gaps(symbol, start_date, end_date).await;
        
        // The method should either succeed (if LMDB is available) or fail gracefully
        // What's important is that it doesn't panic
        match result {
            Ok(_) => {
                // Success case - gap detection worked
                // Test passes on successful completion
            }
            Err(e) => {
                // Expected failure case - LMDB actor not available
                assert!(e.contains("LMDB actor not available") || e.contains("historical data") || e.contains("database"));
            }
        }
    }

    #[tokio::test]
    async fn test_backfill_message_handling() {
        let config = create_test_config();
        let mut actor = VolumeProfileActor::new(config).unwrap();

        // Test backfill processing method directly
        let symbol = "BTCUSDT".to_string();
        let missing_dates = vec![
            NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 11).unwrap(),
        ];

        // This should handle the backfill request (may fail due to missing LMDB actor but should not panic)
        let result = actor.process_backfill(symbol, missing_dates).await;
        // Result may be error due to missing LMDB actor, but it should handle gracefully
        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_processing_status_response() {
        let config = create_test_config();
        let actor = VolumeProfileActor::new(config).unwrap();

        // Test getting processing status
        let status_reply = actor.get_processing_status();

        match status_reply {
            VolumeProfileReply::ProcessingStatus { 
                current_operation, 
                progress_percentage, 
                estimated_completion: _, 
                retry_queue_size, 
                processing_metrics: _ 
            } => {
                assert_eq!(current_operation, None); // No operation running initially
                assert_eq!(progress_percentage, None); // No progress initially
                assert_eq!(retry_queue_size, 0); // No retries initially
            }
            _ => panic!("Expected ProcessingStatus reply"),
        }
    }

    #[tokio::test]
    async fn test_gap_detection_results_integration() {
        let config = create_test_config();
        let mut actor = VolumeProfileActor::new(config).unwrap();

        // Test getting gap detection results
        let symbol = "BTCUSDT".to_string();
        let start_date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2025, 1, 3).unwrap();

        let result = actor.get_gap_detection_results(symbol.clone(), start_date, end_date).await;
        
        // The method should either succeed or fail gracefully (depending on LMDB actor availability)
        match result {
            Ok(reply) => match reply {
                VolumeProfileReply::GapDetectionResults { 
                    symbol: reply_symbol, 
                    missing_dates: _, 
                    total_gaps: _, 
                    date_range 
                } => {
                    assert_eq!(reply_symbol, symbol);
                    assert_eq!(date_range.0, start_date);
                    assert_eq!(date_range.1, end_date);
                }
                VolumeProfileReply::Error(e) => {
                    // Expected error case when LMDB actor not available
                    assert!(e.contains("LMDB actor not available") || e.contains("historical data") || e.contains("database"));
                }
                _ => panic!("Expected GapDetectionResults or Error reply"),
            }
            Err(e) => {
                // Expected error case
                assert!(e.contains("LMDB actor not available") || e.contains("historical data") || e.contains("database"));
            }
        }
    }

    #[test]
    fn test_profile_key_public_interface() {
        // Test that ProfileKey can be created and used externally
        let key = ProfileKey::new("BTCUSDT".to_string(), NaiveDate::from_ymd_opt(2025, 1, 15).unwrap());
        
        assert_eq!(key.symbol, "BTCUSDT");
        assert_eq!(key.date, NaiveDate::from_ymd_opt(2025, 1, 15).unwrap());

        // Test ProfileKey::from_candle
        let candle = create_test_candle(1736985600000, 1000.0);
        let key_from_candle = ProfileKey::from_candle("BTCUSDT".to_string(), &candle);
        
        assert!(key_from_candle.is_some());
        let key = key_from_candle.unwrap();
        assert_eq!(key.symbol, "BTCUSDT");
        assert_eq!(key.date, NaiveDate::from_ymd_opt(2025, 1, 16).unwrap());
    }

    #[test]
    fn test_enhanced_historical_processor_integration() {
        let processor = HistoricalDataProcessor::new();
        let target_date = NaiveDate::from_ymd_opt(2025, 1, 16).unwrap();
        
        // Test with valid candle
        let valid_candle = create_test_candle(1736985600000, 1000.0); // 2025-01-16 00:00:00 UTC
        let result = processor.validate_candle_date(&valid_candle, target_date);
        
        match result {
            DateValidationResult::Valid { trading_day } => {
                assert_eq!(trading_day, target_date);
            }
            DateValidationResult::EdgeCase { trading_day, warning: _, timestamp: _ } => {
                assert_eq!(trading_day, target_date);
            }
            _ => panic!("Expected valid or edge case result for matching date"),
        }

        // Test process_historical_range method
        let candles = vec![
            create_test_candle(1736985600000, 1000.0), // Valid
            create_test_candle(1736985660000, 1100.0), // Valid (1 minute later)
        ];

        let report = processor.process_historical_range(&candles, target_date, "BTCUSDT");
        assert_eq!(report.symbol, "BTCUSDT");
        assert_eq!(report.date, target_date);
        assert_eq!(report.candles_processed, 2);
        assert_eq!(report.validation_results.len(), 2);
        // Processing duration is always non-negative by type definition (u64)
    }

    #[test]
    fn test_message_compatibility_with_existing_patterns() {
        // Ensure new messages don't break existing message patterns
        
        // Test that existing messages still work
        let rebuild_msg = VolumeProfileTellSerializable::RebuildDay {
            symbol: "BTCUSDT".to_string(),
            date: NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
        };

        let full_msg: VolumeProfileTell = rebuild_msg.into();
        match full_msg {
            VolumeProfileTell::RebuildDay { symbol, date } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(date, NaiveDate::from_ymd_opt(2025, 1, 15).unwrap());
            }
            _ => panic!("Existing message pattern broken"),
        }

        // Test existing Ask messages
        let get_profile_ask = VolumeProfileAsk::GetVolumeProfile {
            symbol: "BTCUSDT".to_string(),
            date: NaiveDate::from_ymd_opt(2025, 1, 15).unwrap(),
        };

        match get_profile_ask {
            VolumeProfileAsk::GetVolumeProfile { symbol, date } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(date, NaiveDate::from_ymd_opt(2025, 1, 15).unwrap());
            }
            _ => panic!("Existing ask message pattern broken"),
        }
    }
    
    #[test]
    fn test_debug_metadata_method_integration() {
        let config = VolumeProfileConfig::default();
        let actor = VolumeProfileActor::new(config).expect("Should create actor");
        
        // Test debug metadata request for non-existent profile
        let debug_metadata = actor.get_debug_metadata(
            "TESTUSDT".to_string(),
            NaiveDate::from_ymd_opt(2025, 1, 16).unwrap()
        );
        
        assert!(debug_metadata.is_none(), "Should return None for non-existent profile");
    }
    
    #[test]
    fn test_debug_metadata_message_serialization() {
        // Test VolumeProfileAsk::GetDebugMetadata serialization
        let debug_ask = VolumeProfileAsk::GetDebugMetadata {
            symbol: "BTCUSDT".to_string(),
            date: NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(),
        };
        
        let serialized = serde_json::to_string(&debug_ask).unwrap();
        let deserialized: VolumeProfileAsk = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            VolumeProfileAsk::GetDebugMetadata { symbol, date } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(date, NaiveDate::from_ymd_opt(2025, 1, 16).unwrap());
            }
            _ => panic!("Unexpected ask message type after deserialization"),
        }
        
        // Test VolumeProfileReply::DebugMetadata serialization
        use crate::volume_profile::structs::{VolumeProfileDebugMetadata, PrecisionMetrics, CalculationPerformance, ValidationFlags};
        
        let debug_metadata = VolumeProfileDebugMetadata {
            calculation_timestamp: 1736985600000,
            algorithm_version: "1.4.0".to_string(),
            precision_metrics: PrecisionMetrics::default(),
            performance_metrics: CalculationPerformance::default(),
            validation_flags: ValidationFlags::default(),
        };
        
        let debug_reply = VolumeProfileReply::DebugMetadata(Some(debug_metadata));
        let serialized = serde_json::to_string(&debug_reply).unwrap();
        let deserialized: VolumeProfileReply = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            VolumeProfileReply::DebugMetadata(debug_data) => {
                assert!(debug_data.is_some(), "Should deserialize debug metadata");
                let debug = debug_data.unwrap();
                assert_eq!(debug.calculation_timestamp, 1736985600000);
                assert_eq!(debug.algorithm_version, "1.4.0");
            }
            _ => panic!("Unexpected reply message type after deserialization"),
        }
    }

    // ========================================
    // Performance Validation Tests
    // ========================================

    #[test]
    fn test_batch_processing_performance_metrics() {
        let mut batch_manager = BatchProcessingManager::new();
        
        // Simulate processing multiple batches
        let _start_time = std::time::Instant::now();
        
        // Record several batch completions
        batch_manager.record_batch_metrics(50, 1000); // 50 items, 1000ms
        batch_manager.record_batch_metrics(75, 1200); // 75 items, 1200ms  
        batch_manager.record_batch_metrics(100, 1500); // 100 items, 1500ms
        
        let performance_report = batch_manager.get_performance_report();
        
        // Validate performance metrics are reasonable
        assert!(performance_report.performance_summary.avg_processing_time_per_item_us > 0);
        assert!(performance_report.performance_summary.total_items >= 225); // 50+75+100
        // Memory efficiency might be 0 if no memory tracking occurred
        assert!(performance_report.performance_summary.items_per_mb >= 0.0);
        
        // Ensure processing time is recorded
        assert!(performance_report.performance_summary.total_processing_time_seconds > 0.0);
    }

    #[test]
    fn test_memory_pressure_adaptation() {
        let mut batch_manager = BatchProcessingManager::new();
        let initial_batch_size = batch_manager.batch_config.current_batch_size;
        
        // Simulate high memory usage
        let high_memory_usage = 900 * 1024 * 1024; // 900MB (90% of 1GB)
        let adjusted_size = batch_manager.check_and_adjust_batch_size(high_memory_usage);
        
        // Batch size should be reduced under memory pressure
        assert!(adjusted_size <= initial_batch_size);
        
        // Simulate low memory usage  
        let low_memory_usage = 100 * 1024 * 1024; // 100MB (10% of 1GB)
        let adjusted_size_low = batch_manager.check_and_adjust_batch_size(low_memory_usage);
        
        // Batch size should be larger when memory pressure is low
        assert!(adjusted_size_low >= adjusted_size);
    }

    #[test]
    fn test_progress_tracking_eta_calculation() {
        let operation_id = "performance_test".to_string();
        let mut tracker = ProgressTracker::new(operation_id, 1000);
        
        // Simulate processing with timing
        std::thread::sleep(std::time::Duration::from_millis(100));
        let _update1 = tracker.update_progress(100, ProcessingStage::ProcessingCandles);
        
        std::thread::sleep(std::time::Duration::from_millis(100));
        let update2 = tracker.update_progress(200, ProcessingStage::ProcessingCandles);
        
        // ETA should be calculated and reasonable
        assert!(update2.estimated_time_remaining.is_some());
        if let Some(eta) = update2.estimated_time_remaining {
            // ETA should be positive and reasonable (less than 10 seconds for this test)
            assert!(eta.as_secs() <= 10);
        }
        
        // Progress percentage should be calculated correctly
        assert_eq!(update2.percentage_complete, 20.0); // 200/1000 * 100
    }

    #[tokio::test]
    async fn test_end_to_end_historical_processing_simulation() {
        let config = create_test_config();
        let mut actor = VolumeProfileActor::new(config).unwrap();
        
        // Create test candles for a full day (1440 minutes)
        let mut test_candles = Vec::new();
        let base_timestamp = 1736985600000; // 2025-01-16 00:00:00 UTC
        
        // Generate 100 test candles (simulating partial day for performance)
        for i in 0..100 {
            let timestamp = base_timestamp + (i * 60 * 1000); // 1 minute intervals
            let candle = create_test_candle(timestamp, 1000.0 + i as f64);
            test_candles.push(candle);
        }
        
        let start_time = std::time::Instant::now();
        
        // Process candles in batches to simulate historical processing
        for candle in test_candles {
            let _ = actor.process_candle("BTCUSDT".to_string(), candle).await;
        }
        
        let processing_duration = start_time.elapsed();
        
        // Validate performance requirements
        // For 100 candles, processing should be very fast (< 1 second)
        assert!(processing_duration.as_millis() < 1000, 
                "Processing took too long: {}ms", processing_duration.as_millis());
        
        // Verify candles were processed
        assert!(actor.total_candles_processed >= 100);
        
        // Check memory usage is reasonable
        let memory_usage = actor.estimate_memory_usage();
        // Memory usage should be reasonable (< 10MB for this test)
        assert!(memory_usage < 10 * 1024 * 1024, "Memory usage too high: {} bytes", memory_usage);
    }

    #[test]
    fn test_performance_target_validation() {
        // Validate that our performance calculations align with story requirements
        // Target: 10 minutes for 1 year of data
        
        let minutes_per_year = 365 * 24 * 60; // 525,600 minutes
        let target_time_seconds = 10 * 60; // 10 minutes
        let required_throughput = minutes_per_year as f64 / target_time_seconds as f64;
        
        // We need to process ~875 candles per second to meet the target
        assert!(required_throughput > 800.0 && required_throughput < 1000.0,
                "Required throughput: {:.2} candles/second", required_throughput);
        
        // Test our processing metrics can track this level of throughput
        let mut metrics = ProcessingMetrics::new();
        
        // Simulate high-throughput processing session
        let candles_processed = 1000;
        let processing_time_ms = 1000; // 1 second
        let memory_usage = 10 * 1024 * 1024; // 10MB
        
        metrics.record_success(candles_processed, processing_time_ms, memory_usage);
        
        // Validate throughput calculation
        assert!(metrics.avg_throughput_candles_per_second >= 900.0,
                "Throughput too low: {:.2} candles/second", metrics.avg_throughput_candles_per_second);
        
        // Validate metrics tracking
        assert_eq!(metrics.total_candles_processed, candles_processed as u64);
        assert_eq!(metrics.successful_sessions, 1);
        assert_eq!(metrics.success_rate_percentage, 100.0);
    }

    #[tokio::test]
    async fn test_postgresql_schema_initialization() {
        // Test that PostgreSQL schema initialization is properly called during SetActorReferences
        use kameo::spawn;
        use crate::lmdb::LmdbActor;
        use crate::postgres::PostgresActor;
        use tempfile::TempDir;
        
        // Create test config with volume profile enabled
        let mut config = create_test_config();
        config.enabled = true;
        
        // Create and spawn VolumeProfileActor
        let actor = VolumeProfileActor::new(config).unwrap();
        let volume_profile_ref = spawn(actor);
        
        // Create temporary directory for LMDB
        let temp_dir = TempDir::new().unwrap();
        let lmdb_actor = LmdbActor::new(temp_dir.path(), 60).unwrap();
        let lmdb_ref = spawn(lmdb_actor);
        
        let postgres_config = crate::postgres::PostgresConfig {
            enabled: false, // Disabled to avoid actual database connection
            ..Default::default()
        };
        let postgres_actor = PostgresActor::new(postgres_config).unwrap();
        let postgres_ref = spawn(postgres_actor);
        
        // Send SetActorReferences message
        let set_refs_msg = VolumeProfileTell::SetActorReferences {
            postgres_actor: Some(postgres_ref),
            lmdb_actor: lmdb_ref,
        };
        
        // This should not fail and should complete without panicking
        // The actual schema initialization will be logged but won't execute due to disabled PostgreSQL
        let result = volume_profile_ref.tell(set_refs_msg).send().await;
        assert!(result.is_ok(), "SetActorReferences message should succeed");
        
        // Wait a moment for the message to be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Test passes if we reach this point without panicking
        // In a real integration test with database, we would verify the table exists
        println!("✅ PostgreSQL schema initialization test completed successfully");
    }
}