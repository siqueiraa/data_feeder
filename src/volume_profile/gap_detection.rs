use chrono::{NaiveDate, Utc, DateTime};
use kameo::actor::ActorRef;
use kameo::request::MessageSend;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn, error};

use crate::lmdb::{LmdbActor, LmdbActorMessage, LmdbActorResponse};
#[cfg(feature = "postgres")]
use crate::postgres::{PostgresActor};

/// Date range for gap detection
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DateRange {
    pub start: NaiveDate,
    pub end: NaiveDate,
}

impl DateRange {
    pub fn new(start: NaiveDate, end: NaiveDate) -> Self {
        Self { start, end }
    }

    /// Get the number of days in this range (inclusive)
    pub fn days_count(&self) -> u32 {
        (self.end - self.start).num_days() as u32 + 1
    }

    /// Check if this range contains the given date
    pub fn contains(&self, date: NaiveDate) -> bool {
        date >= self.start && date <= self.end
    }
}

/// Result of gap detection analysis
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GapDetectionResult {
    /// Identified gaps in volume profile data
    pub missing_date_ranges: Vec<DateRange>,
    /// Count of missing periods
    pub total_gaps_found: usize,
    /// When gap detection was performed
    pub analysis_timestamp: DateTime<Utc>,
}

impl Default for GapDetectionResult {
    fn default() -> Self {
        Self::new()
    }
}

impl GapDetectionResult {
    pub fn new() -> Self {
        Self {
            missing_date_ranges: Vec::new(),
            total_gaps_found: 0,
            analysis_timestamp: Utc::now(),
        }
    }

    /// Check if any gaps were found
    pub fn has_gaps(&self) -> bool {
        self.total_gaps_found > 0
    }

    /// Get total days missing across all gaps
    pub fn total_days_missing(&self) -> u32 {
        self.missing_date_ranges
            .iter()
            .map(|range| range.days_count())
            .sum()
    }
}

/// Errors that can occur during gap detection
#[derive(Debug, Error)]
pub enum GapDetectionError {
    #[error("LMDB communication failed: {0}")]
    LmdbError(String),
    #[cfg(feature = "postgres")]
    #[error("PostgreSQL communication failed: {0}")]
    PostgresError(String),
    #[error("Invalid date range: start date {start} is after end date {end}")]
    InvalidDateRange { start: NaiveDate, end: NaiveDate },
    #[error("No storage backend available")]
    NoStorageBackend,
}

/// Component to identify missing volume profile data periods
#[derive(Debug, Clone)]
pub struct GapDetector {
    /// Enable detailed logging for gap detection operations
    pub debug_logging: bool,
}

impl GapDetector {
    /// Create new gap detector
    pub fn new() -> Self {
        Self {
            debug_logging: true,
        }
    }

    /// Identify missing volume profile data periods using LMDB storage
    pub async fn detect_gaps_lmdb(
        &self,
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lmdb_actor: &ActorRef<LmdbActor>,
    ) -> Result<GapDetectionResult, GapDetectionError> {
        if start_date > end_date {
            return Err(GapDetectionError::InvalidDateRange {
                start: start_date,
                end: end_date,
            });
        }

        info!("🔍 Gap detection for {} from {} to {} via LMDB", 
              symbol, start_date, end_date);

        let mut result = GapDetectionResult::new();
        let mut missing_ranges: Vec<DateRange> = Vec::new();

        // Iterate through each date in the range
        let mut current_date = start_date;
        let mut gap_start: Option<NaiveDate> = None;

        while current_date <= end_date {
            if self.debug_logging {
                debug!("Checking volume profile data for {} on {}", symbol, current_date);
            }

            // Check if volume profile data exists for this date
            let has_data = self.check_date_has_data_lmdb(symbol, current_date, lmdb_actor).await?;

            if has_data {
                // Data exists - close any open gap
                if let Some(gap_start_date) = gap_start {
                    let gap_end_date = current_date.pred_opt().unwrap_or(current_date);
                    if gap_start_date <= gap_end_date {
                        missing_ranges.push(DateRange::new(gap_start_date, gap_end_date));
                        if self.debug_logging {
                            debug!("Closed gap: {} to {}", gap_start_date, gap_end_date);
                        }
                    }
                    gap_start = None;
                }
            } else {
                // Data missing - start or continue gap
                if gap_start.is_none() {
                    gap_start = Some(current_date);
                    if self.debug_logging {
                        debug!("Started gap at {}", current_date);
                    }
                }
            }

            current_date = current_date.succ_opt().unwrap_or(current_date);
            if current_date == current_date.pred_opt().unwrap_or(current_date) {
                break; // Prevent infinite loop
            }
        }

        // Close any remaining gap
        if let Some(gap_start_date) = gap_start {
            missing_ranges.push(DateRange::new(gap_start_date, end_date));
            if self.debug_logging {
                debug!("Closed final gap: {} to {}", gap_start_date, end_date);
            }
        }

        result.missing_date_ranges = missing_ranges;
        result.total_gaps_found = result.missing_date_ranges.len();

        if result.has_gaps() {
            warn!("📊 Gap detection completed: {} gaps found, {} total days missing", 
                  result.total_gaps_found, result.total_days_missing());
        } else {
            info!("✅ Gap detection completed: No gaps found");
        }

        Ok(result)
    }

    /// Check if volume profile data exists for a specific date in LMDB
    async fn check_date_has_data_lmdb(
        &self,
        symbol: &str,
        date: NaiveDate,
        lmdb_actor: &ActorRef<LmdbActor>,
    ) -> Result<bool, GapDetectionError> {
        // Use the daily volume profile key format: "{symbol}:{date}"
        let key = format!("{}:{}", symbol, date.format("%Y-%m-%d"));

        match lmdb_actor.ask(LmdbActorMessage::CheckVolumeProfileExists { key }).send().await {
            Ok(LmdbActorResponse::VolumeProfileExists { exists }) => {
                if self.debug_logging && exists {
                    debug!("✅ Volume profile data exists for {} on {}", symbol, date);
                } else if self.debug_logging {
                    debug!("❌ Volume profile data missing for {} on {}", symbol, date);
                }
                Ok(exists)
            }
            Ok(LmdbActorResponse::ErrorResponse(error)) => {
                error!("LMDB error checking volume profile for {} on {}: {}", symbol, date, error);
                Err(GapDetectionError::LmdbError(error))
            }
            Ok(_) => {
                error!("Unexpected response from LMDB actor");
                Err(GapDetectionError::LmdbError("Unexpected response".to_string()))
            }
            Err(e) => {
                error!("Failed to communicate with LMDB actor: {}", e);
                Err(GapDetectionError::LmdbError(e.to_string()))
            }
        }
    }

    /// Identify missing volume profile data periods using PostgreSQL storage  
    #[cfg(feature = "postgres")]
    pub async fn detect_gaps_postgres(
        &self,
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        _postgres_actor: &ActorRef<PostgresActor>,
    ) -> Result<GapDetectionResult, GapDetectionError> {
        if start_date > end_date {
            return Err(GapDetectionError::InvalidDateRange {
                start: start_date,
                end: end_date,
            });
        }

        info!("🔍 Gap detection for {} from {} to {} via PostgreSQL", 
              symbol, start_date, end_date);

        let mut result = GapDetectionResult::new();

        // For now, PostgreSQL volume profile queries are not implemented
        // This would require adding GetVolumeProfileDates to PostgresAsk enum
        // For this implementation, return no gaps found
        warn!("📊 PostgreSQL gap detection not yet implemented - assuming no gaps");
        
        result.missing_date_ranges = Vec::new();
        result.total_gaps_found = 0;
        
        info!("✅ PostgreSQL gap detection completed: No gaps found (not implemented)");
        Ok(result)
    }

    /// Analyze existing dates and find missing ranges
    pub fn find_missing_dates(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
        existing_dates: &[NaiveDate],
    ) -> GapDetectionResult {
        let mut result = GapDetectionResult::new();
        let mut missing_ranges: Vec<DateRange> = Vec::new();

        // Convert existing dates to a set for faster lookup
        let existing_set: std::collections::HashSet<NaiveDate> = existing_dates.iter().copied().collect();

        // Iterate through the expected date range and find gaps
        let mut current_date = start_date;
        let mut gap_start: Option<NaiveDate> = None;

        while current_date <= end_date {
            if existing_set.contains(&current_date) {
                // Data exists - close any open gap
                if let Some(gap_start_date) = gap_start {
                    let gap_end_date = current_date.pred_opt().unwrap_or(current_date);
                    if gap_start_date <= gap_end_date {
                        missing_ranges.push(DateRange::new(gap_start_date, gap_end_date));
                        if self.debug_logging {
                            debug!("Closed gap: {} to {}", gap_start_date, gap_end_date);
                        }
                    }
                    gap_start = None;
                }
            } else {
                // Data missing - start or continue gap
                if gap_start.is_none() {
                    gap_start = Some(current_date);
                    if self.debug_logging {
                        debug!("Started gap at {}", current_date);
                    }
                }
            }

            current_date = current_date.succ_opt().unwrap_or(current_date);
            if current_date == current_date.pred_opt().unwrap_or(current_date) {
                break; // Prevent infinite loop
            }
        }

        // Close any remaining gap
        if let Some(gap_start_date) = gap_start {
            missing_ranges.push(DateRange::new(gap_start_date, end_date));
            if self.debug_logging {
                debug!("Closed final gap: {} to {}", gap_start_date, end_date);
            }
        }

        result.missing_date_ranges = missing_ranges;
        result.total_gaps_found = result.missing_date_ranges.len();

        result
    }

    /// Detect gaps using both LMDB and PostgreSQL (if available) and merge results
    pub async fn detect_gaps_comprehensive(
        &self,
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
        lmdb_actor: &ActorRef<LmdbActor>,
        #[cfg(feature = "postgres")]
        postgres_actor: Option<&ActorRef<PostgresActor>>,
    ) -> Result<GapDetectionResult, GapDetectionError> {
        info!("🔍 Comprehensive gap detection for {} from {} to {}", 
              symbol, start_date, end_date);

        // Always check LMDB as primary storage
        let lmdb_result = self.detect_gaps_lmdb(symbol, start_date, end_date, lmdb_actor).await?;

        #[cfg(feature = "postgres")]
        if let Some(postgres_ref) = postgres_actor {
            // Also check PostgreSQL if available
            let postgres_result = self.detect_gaps_postgres(symbol, start_date, end_date, postgres_ref).await?;
            
            // Merge results (intersection - gaps that exist in both storages)
            let merged_result = self.merge_gap_results(&lmdb_result, &postgres_result);
            info!("📊 Comprehensive gap detection completed: LMDB={} gaps, PostgreSQL={} gaps, merged={} gaps", 
                  lmdb_result.total_gaps_found, postgres_result.total_gaps_found, merged_result.total_gaps_found);
            return Ok(merged_result);
        }

        info!("📊 LMDB-only gap detection completed: {} gaps found", lmdb_result.total_gaps_found);
        Ok(lmdb_result)
    }

    /// Merge gap detection results from multiple storage backends
    #[cfg(feature = "postgres")]
    fn merge_gap_results(
        &self,
        lmdb_result: &GapDetectionResult,
        postgres_result: &GapDetectionResult,
    ) -> GapDetectionResult {
        let mut result = GapDetectionResult::new();
        let mut merged_ranges: Vec<DateRange> = Vec::new();

        // Find gaps that exist in both results (intersection)
        for lmdb_range in &lmdb_result.missing_date_ranges {
            for postgres_range in &postgres_result.missing_date_ranges {
                // Check if ranges overlap
                if let Some(intersection) = self.intersect_date_ranges(lmdb_range, postgres_range) {
                    merged_ranges.push(intersection);
                }
            }
        }

        // Remove duplicate ranges and sort
        merged_ranges.sort_by(|a, b| a.start.cmp(&b.start));
        merged_ranges.dedup_by(|a, b| a.start == b.start && a.end == b.end);

        result.missing_date_ranges = merged_ranges;
        result.total_gaps_found = result.missing_date_ranges.len();

        result
    }

    /// Find intersection of two date ranges
    #[cfg(feature = "postgres")]
    fn intersect_date_ranges(&self, range1: &DateRange, range2: &DateRange) -> Option<DateRange> {
        let start = std::cmp::max(range1.start, range2.start);
        let end = std::cmp::min(range1.end, range2.end);

        if start <= end {
            Some(DateRange::new(start, end))
        } else {
            None
        }
    }
}

impl Default for GapDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_date_range_creation() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let range = DateRange::new(start, end);

        assert_eq!(range.start, start);
        assert_eq!(range.end, end);
        assert_eq!(range.days_count(), 5);
    }

    #[test]
    fn test_date_range_contains() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let range = DateRange::new(start, end);

        let test_date = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        assert!(range.contains(test_date));

        let outside_date = NaiveDate::from_ymd_opt(2024, 1, 6).unwrap();
        assert!(!range.contains(outside_date));
    }

    #[test]
    fn test_gap_detection_result_creation() {
        let mut result = GapDetectionResult::new();
        assert!(!result.has_gaps());
        assert_eq!(result.total_gaps_found, 0);
        assert_eq!(result.total_days_missing(), 0);

        let range = DateRange::new(
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap()
        );
        result.missing_date_ranges.push(range);
        result.total_gaps_found = 1;

        assert!(result.has_gaps());
        assert_eq!(result.total_gaps_found, 1);
        assert_eq!(result.total_days_missing(), 5);
    }

    #[test]
    fn test_invalid_date_range() {
        let gap_detector = GapDetector::new();
        let start = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

        let existing_dates = vec![];
        let result = gap_detector.find_missing_dates(start, end, &existing_dates);
        
        // Should handle gracefully
        assert_eq!(result.total_gaps_found, 0);
    }

    #[test]
    fn test_find_missing_dates() {
        let gap_detector = GapDetector::new();
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        
        // Missing dates: 2024-01-03, 2024-01-04, 2024-01-07, 2024-01-08, 2024-01-09
        let existing_dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 6).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
        ];

        let result = gap_detector.find_missing_dates(start, end, &existing_dates);

        assert_eq!(result.total_gaps_found, 2);
        assert_eq!(result.total_days_missing(), 5);
        
        // Gap 1: Jan 3-4
        assert_eq!(result.missing_date_ranges[0].start, NaiveDate::from_ymd_opt(2024, 1, 3).unwrap());
        assert_eq!(result.missing_date_ranges[0].end, NaiveDate::from_ymd_opt(2024, 1, 4).unwrap());
        
        // Gap 2: Jan 7-9
        assert_eq!(result.missing_date_ranges[1].start, NaiveDate::from_ymd_opt(2024, 1, 7).unwrap());
        assert_eq!(result.missing_date_ranges[1].end, NaiveDate::from_ymd_opt(2024, 1, 9).unwrap());
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn test_intersect_date_ranges() {
        let gap_detector = GapDetector::new();
        
        let range1 = DateRange::new(
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap()
        );
        let range2 = DateRange::new(
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        );

        let intersection = gap_detector.intersect_date_ranges(&range1, &range2).unwrap();
        assert_eq!(intersection.start, NaiveDate::from_ymd_opt(2024, 1, 5).unwrap());
        assert_eq!(intersection.end, NaiveDate::from_ymd_opt(2024, 1, 10).unwrap());

        // Non-overlapping ranges
        let range3 = DateRange::new(
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap()
        );
        let range4 = DateRange::new(
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        );

        let no_intersection = gap_detector.intersect_date_ranges(&range3, &range4);
        assert!(no_intersection.is_none());
    }
}