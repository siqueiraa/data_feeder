use std::collections::HashMap;
use chrono::{Utc, NaiveDate};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::structs::{FuturesOHLCVCandle, TimeRange, TimestampMS};
use super::errors::HistoricalDataError;

/// Volume profile validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfileValidationConfig {
    /// Minimum volume threshold for valid candles
    pub min_volume_threshold: f64,
    /// Maximum volume multiplier for outlier detection (vs average)
    pub max_volume_multiplier: f64,
    /// Maximum price gap percentage between consecutive candles
    pub max_price_gap_percentage: f64,
    /// Minimum number of candles required for profile validation
    pub min_candles_for_validation: u32,
    /// Enable/disable volume anomaly detection
    pub enable_volume_anomaly_detection: bool,
    /// Enable/disable price continuity validation
    pub enable_price_continuity_validation: bool,
    /// Trading session validation (for market hours checking)
    pub enable_trading_session_validation: bool,
}

impl Default for VolumeProfileValidationConfig {
    fn default() -> Self {
        Self {
            min_volume_threshold: 0.000001, // Very small but non-zero
            max_volume_multiplier: 50.0,    // 50x average volume
            max_price_gap_percentage: 10.0,  // 10% price gap
            min_candles_for_validation: 10,
            enable_volume_anomaly_detection: true,
            enable_price_continuity_validation: true,
            enable_trading_session_validation: false, // Crypto trades 24/7
        }
    }
}

/// Volume profile validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfileValidationResult {
    /// Symbol being validated
    pub symbol: String,
    /// Date being validated
    pub date: NaiveDate,
    /// Validation timestamp
    pub validated_at: TimestampMS,
    /// Overall validation status
    pub is_valid: bool,
    /// Number of candles validated
    pub candles_validated: u32,
    /// Volume validation results
    pub volume_validation: VolumeValidationResult,
    /// Price validation results
    pub price_validation: PriceValidationResult,
    /// Gap analysis results
    pub gap_analysis: GapAnalysisResult,
    /// Any validation errors found
    pub validation_errors: Vec<ValidationError>,
    /// Performance metrics
    pub validation_duration_ms: u64,
}

/// Volume-specific validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeValidationResult {
    /// Total volume for the period
    pub total_volume: f64,
    /// Average volume per candle
    pub average_volume: f64,
    /// Number of zero-volume candles
    pub zero_volume_candles: u32,
    /// Number of outlier volume candles
    pub outlier_volume_candles: u32,
    /// Maximum volume in a single candle
    pub max_volume: f64,
    /// Minimum non-zero volume
    pub min_non_zero_volume: f64,
    /// Volume distribution quality score (0.0-1.0)
    pub distribution_quality_score: f64,
}

/// Price-specific validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceValidationResult {
    /// Price range for the period
    pub price_range: (f64, f64), // (min, max)
    /// Number of price gaps detected
    pub price_gaps_detected: u32,
    /// Maximum price gap percentage
    pub max_price_gap_percentage: f64,
    /// Price continuity score (0.0-1.0)
    pub price_continuity_score: f64,
    /// OHLC consistency validation
    pub ohlc_consistency_issues: u32,
}

/// Gap analysis specific to volume profiles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GapAnalysisResult {
    /// Time gaps detected (from existing gap detection)
    pub time_gaps: Vec<TimeRange>,
    /// Volume gaps (periods with suspicious volume patterns)
    pub volume_gaps: Vec<VolumeGap>,
    /// Data quality gaps (periods with poor data quality)
    pub data_quality_gaps: Vec<DataQualityGap>,
    /// Overall gap impact score (0.0-1.0, lower is better)
    pub gap_impact_score: f64,
}

/// Volume gap definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeGap {
    /// Time range of the volume gap
    pub time_range: TimeRange,
    /// Expected volume vs actual volume
    pub volume_deviation: f64,
    /// Gap severity (low, medium, high)
    pub severity: GapSeverity,
    /// Description of the gap
    pub description: String,
}

/// Data quality gap definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataQualityGap {
    /// Time range of the quality issue
    pub time_range: TimeRange,
    /// Quality issue type
    pub issue_type: DataQualityIssueType,
    /// Issue severity
    pub severity: GapSeverity,
    /// Description of the issue
    pub description: String,
}

/// Gap severity levels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GapSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Data quality issue types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataQualityIssueType {
    ZeroVolume,
    OutlierVolume,
    PriceDiscontinuity,
    OHLCInconsistency,
    MissingData,
    InvalidTimestamp,
}

/// Validation error definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// Error code for categorization
    pub error_code: String,
    /// Human-readable error message
    pub message: String,
    /// Timestamp when error occurred
    pub timestamp: TimestampMS,
    /// Severity of the error
    pub severity: GapSeverity,
    /// Additional context data
    pub context: HashMap<String, String>,
}

/// Volume profile validator
pub struct VolumeProfileValidator {
    config: VolumeProfileValidationConfig,
}

impl VolumeProfileValidator {
    /// Create new volume profile validator
    pub fn new(config: VolumeProfileValidationConfig) -> Self {
        Self { config }
    }

    /// Validate volume profile data after gap detection
    pub fn validate_volume_profile_data(
        &self,
        symbol: &str,
        date: NaiveDate,
        candles: &[FuturesOHLCVCandle],
        existing_gaps: &[TimeRange],
    ) -> Result<VolumeProfileValidationResult, HistoricalDataError> {
        let start_time = std::time::Instant::now();
        
        info!("🔍 Starting volume profile validation for {} on {} with {} candles", 
              symbol, date, candles.len());

        // Initialize validation result
        let mut validation_result = VolumeProfileValidationResult {
            symbol: symbol.to_string(),
            date,
            validated_at: Utc::now().timestamp_millis(),
            is_valid: true,
            candles_validated: candles.len() as u32,
            volume_validation: VolumeValidationResult::default(),
            price_validation: PriceValidationResult::default(),
            gap_analysis: GapAnalysisResult::default(),
            validation_errors: Vec::new(),
            validation_duration_ms: 0,
        };

        // Early validation for minimum candle requirement
        if candles.len() < self.config.min_candles_for_validation as usize {
            let error = ValidationError {
                error_code: "INSUFFICIENT_CANDLES".to_string(),
                message: format!("Insufficient candles for validation: {} < {}", 
                               candles.len(), self.config.min_candles_for_validation),
                timestamp: Utc::now().timestamp_millis(),
                severity: GapSeverity::High,
                context: HashMap::new(),
            };
            validation_result.validation_errors.push(error);
            validation_result.is_valid = false;
            validation_result.validation_duration_ms = start_time.elapsed().as_millis() as u64;
            return Ok(validation_result);
        }

        // Perform volume validation
        if self.config.enable_volume_anomaly_detection {
            validation_result.volume_validation = self.validate_volume_data(candles, &mut validation_result.validation_errors)?;
        }

        // Perform price validation
        if self.config.enable_price_continuity_validation {
            validation_result.price_validation = self.validate_price_data(candles, &mut validation_result.validation_errors)?;
        }

        // Perform enhanced gap analysis
        validation_result.gap_analysis = self.analyze_gaps_for_volume_profile(
            candles, 
            existing_gaps, 
            &mut validation_result.validation_errors
        )?;

        // Determine overall validation status
        validation_result.is_valid = validation_result.validation_errors.iter()
            .all(|error| matches!(error.severity, GapSeverity::Low | GapSeverity::Medium));

        validation_result.validation_duration_ms = start_time.elapsed().as_millis() as u64;

        let status = if validation_result.is_valid { "✅ PASSED" } else { "❌ FAILED" };
        info!("🔍 Volume profile validation {}: {} errors, {} warnings ({}ms)", 
              status,
              validation_result.validation_errors.iter().filter(|e| matches!(e.severity, GapSeverity::High | GapSeverity::Critical)).count(),
              validation_result.validation_errors.iter().filter(|e| matches!(e.severity, GapSeverity::Low | GapSeverity::Medium)).count(),
              validation_result.validation_duration_ms);

        Ok(validation_result)
    }

    /// Validate volume data for anomalies and quality issues
    fn validate_volume_data(
        &self,
        candles: &[FuturesOHLCVCandle],
        errors: &mut Vec<ValidationError>,
    ) -> Result<VolumeValidationResult, HistoricalDataError> {
        debug!("📊 Validating volume data for {} candles", candles.len());

        let mut result = VolumeValidationResult::default();
        
        // Calculate basic volume statistics
        let volumes: Vec<f64> = candles.iter().map(|c| c.volume).collect();
        result.total_volume = volumes.iter().sum();
        result.average_volume = result.total_volume / volumes.len() as f64;
        result.max_volume = volumes.iter().fold(0.0, |a, &b| a.max(b));
        result.min_non_zero_volume = volumes.iter()
            .filter(|&&v| v > 0.0)
            .fold(f64::INFINITY, |a, &b| a.min(b));

        // Count zero volume candles
        result.zero_volume_candles = volumes.iter()
            .filter(|&&v| v < self.config.min_volume_threshold)
            .count() as u32;

        // Detect volume outliers
        let outlier_threshold = result.average_volume * self.config.max_volume_multiplier;
        result.outlier_volume_candles = volumes.iter()
            .filter(|&&v| v > outlier_threshold)
            .count() as u32;

        // Add validation errors for significant issues
        if result.zero_volume_candles > candles.len() as u32 / 10 { // More than 10% zero volume
            errors.push(ValidationError {
                error_code: "HIGH_ZERO_VOLUME_RATIO".to_string(),
                message: format!("High ratio of zero volume candles: {}/{}", 
                               result.zero_volume_candles, candles.len()),
                timestamp: Utc::now().timestamp_millis(),
                severity: GapSeverity::Medium,
                context: HashMap::new(),
            });
        }

        if result.outlier_volume_candles > 0 {
            errors.push(ValidationError {
                error_code: "VOLUME_OUTLIERS_DETECTED".to_string(),
                message: format!("Detected {} volume outliers above {}x average", 
                               result.outlier_volume_candles, self.config.max_volume_multiplier),
                timestamp: Utc::now().timestamp_millis(),
                severity: GapSeverity::Low,
                context: HashMap::new(),
            });
        }

        // Calculate distribution quality score
        result.distribution_quality_score = self.calculate_volume_distribution_quality(&volumes);

        debug!("📊 Volume validation complete: total={:.2}, avg={:.2}, zeros={}, outliers={}", 
               result.total_volume, result.average_volume, 
               result.zero_volume_candles, result.outlier_volume_candles);

        Ok(result)
    }

    /// Validate price data for continuity and consistency
    fn validate_price_data(
        &self,
        candles: &[FuturesOHLCVCandle],
        errors: &mut Vec<ValidationError>,
    ) -> Result<PriceValidationResult, HistoricalDataError> {
        debug!("💰 Validating price data for {} candles", candles.len());

        let mut result = PriceValidationResult::default();
        
        // Calculate price range
        let prices: Vec<f64> = candles.iter().flat_map(|c| vec![c.open, c.high, c.low, c.close]).collect();
        let min_price = prices.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_price = prices.iter().fold(0.0f64, |a, &b| a.max(b));
        result.price_range = (min_price, max_price);

        // Check for price gaps between consecutive candles
        let mut max_gap_percentage = 0.0f64;
        let mut price_gaps = 0;
        let mut ohlc_issues = 0;

        for i in 0..candles.len() {
            let candle = &candles[i];
            
            // Validate OHLC consistency within candle
            if !(candle.low <= candle.open && candle.open <= candle.high &&
                 candle.low <= candle.close && candle.close <= candle.high &&
                 candle.low <= candle.high) {
                ohlc_issues += 1;
                if ohlc_issues <= 5 { // Don't spam errors
                    errors.push(ValidationError {
                        error_code: "OHLC_INCONSISTENCY".to_string(),
                        message: format!("OHLC inconsistency at {}: O={}, H={}, L={}, C={}", 
                                       candle.open_time, candle.open, candle.high, candle.low, candle.close),
                        timestamp: Utc::now().timestamp_millis(),
                        severity: GapSeverity::High,
                        context: HashMap::new(),
                    });
                }
            }

            // Check price gaps between consecutive candles
            if i > 0 {
                let prev_candle = &candles[i - 1];
                let price_gap = (candle.open - prev_candle.close).abs();
                let gap_percentage = (price_gap / prev_candle.close) * 100.0;
                
                if gap_percentage > self.config.max_price_gap_percentage {
                    price_gaps += 1;
                    max_gap_percentage = max_gap_percentage.max(gap_percentage);
                    
                    if price_gaps <= 3 { // Don't spam errors
                        errors.push(ValidationError {
                            error_code: "LARGE_PRICE_GAP".to_string(),
                            message: format!("Large price gap: {:.2}% between {} and {}", 
                                           gap_percentage, prev_candle.close_time, candle.open_time),
                            timestamp: Utc::now().timestamp_millis(),
                            severity: if gap_percentage > 20.0 { GapSeverity::High } else { GapSeverity::Medium },
                            context: HashMap::new(),
                        });
                    }
                }
            }
        }

        result.price_gaps_detected = price_gaps;
        result.max_price_gap_percentage = max_gap_percentage;
        result.ohlc_consistency_issues = ohlc_issues;
        
        // Calculate price continuity score
        result.price_continuity_score = self.calculate_price_continuity_score(candles, price_gaps);

        debug!("💰 Price validation complete: range=({:.2}-{:.2}), gaps={}, OHLC_issues={}", 
               min_price, max_price, price_gaps, ohlc_issues);

        Ok(result)
    }

    /// Analyze gaps specifically for volume profile impact
    fn analyze_gaps_for_volume_profile(
        &self,
        candles: &[FuturesOHLCVCandle],
        existing_gaps: &[TimeRange],
        errors: &mut Vec<ValidationError>,
    ) -> Result<GapAnalysisResult, HistoricalDataError> {
        debug!("🕳️  Analyzing gaps for volume profile impact: {} existing gaps", existing_gaps.len());

        let mut result = GapAnalysisResult {
            time_gaps: existing_gaps.to_vec(),
            volume_gaps: Vec::new(),
            data_quality_gaps: Vec::new(),
            gap_impact_score: 0.0,
        };

        // Detect volume gaps (periods with suspicious volume patterns)
        result.volume_gaps = self.detect_volume_gaps(candles)?;
        
        // Detect data quality gaps
        result.data_quality_gaps = self.detect_data_quality_gaps(candles)?;

        // Calculate overall gap impact score
        result.gap_impact_score = self.calculate_gap_impact_score(&result, candles.len());

        // Add errors for critical gaps
        for gap in &result.volume_gaps {
            if matches!(gap.severity, GapSeverity::High | GapSeverity::Critical) {
                errors.push(ValidationError {
                    error_code: "CRITICAL_VOLUME_GAP".to_string(),
                    message: gap.description.clone(),
                    timestamp: Utc::now().timestamp_millis(),
                    severity: gap.severity.clone(),
                    context: HashMap::new(),
                });
            }
        }

        for gap in &result.data_quality_gaps {
            if matches!(gap.severity, GapSeverity::High | GapSeverity::Critical) {
                errors.push(ValidationError {
                    error_code: "CRITICAL_DATA_QUALITY_GAP".to_string(),
                    message: gap.description.clone(),
                    timestamp: Utc::now().timestamp_millis(),
                    severity: gap.severity.clone(),
                    context: HashMap::new(),
                });
            }
        }

        debug!("🕳️  Gap analysis complete: {} volume gaps, {} quality gaps, impact_score={:.3}", 
               result.volume_gaps.len(), result.data_quality_gaps.len(), result.gap_impact_score);

        Ok(result)
    }

    /// Calculate volume distribution quality score (0.0-1.0, higher is better)
    fn calculate_volume_distribution_quality(&self, volumes: &[f64]) -> f64 {
        if volumes.is_empty() {
            return 0.0;
        }

        let mean = volumes.iter().sum::<f64>() / volumes.len() as f64;
        let variance = volumes.iter()
            .map(|v| (v - mean).powi(2))
            .sum::<f64>() / volumes.len() as f64;
        let std_dev = variance.sqrt();
        
        // Coefficient of variation (lower is better for consistency)
        let cv = if mean > 0.0 { std_dev / mean } else { f64::INFINITY };
        
        // Convert to quality score (0.0-1.0, higher is better)
        // Use exponential decay to map CV to quality score
        ((-cv / 2.0).exp()).clamp(0.0, 1.0)
    }

    /// Calculate price continuity score (0.0-1.0, higher is better)
    fn calculate_price_continuity_score(&self, candles: &[FuturesOHLCVCandle], price_gaps: u32) -> f64 {
        if candles.is_empty() {
            return 0.0;
        }

        let gap_ratio = price_gaps as f64 / candles.len() as f64;
        // Convert gap ratio to continuity score
        (1.0 - gap_ratio).max(0.0)
    }

    /// Detect volume gaps in the data
    fn detect_volume_gaps(&self, candles: &[FuturesOHLCVCandle]) -> Result<Vec<VolumeGap>, HistoricalDataError> {
        let mut volume_gaps = Vec::new();
        
        if candles.len() < 3 {
            return Ok(volume_gaps);
        }

        let average_volume = candles.iter().map(|c| c.volume).sum::<f64>() / candles.len() as f64;
        
        // Sliding window to detect volume anomalies
        for window in candles.windows(3) {
            let window_avg = window.iter().map(|c| c.volume).sum::<f64>() / 3.0;
            
            // Check for significant volume drops
            if window_avg < average_volume * 0.1 && average_volume > 0.0 { // Less than 10% of normal
                let gap = VolumeGap {
                    time_range: TimeRange {
                        start: window[0].open_time,
                        end: window[2].close_time,
                    },
                    volume_deviation: (average_volume - window_avg) / average_volume,
                    severity: if window_avg < average_volume * 0.01 { 
                        GapSeverity::High 
                    } else { 
                        GapSeverity::Medium 
                    },
                    description: format!("Low volume period: {:.2} vs {:.2} average", window_avg, average_volume),
                };
                volume_gaps.push(gap);
            }
        }

        Ok(volume_gaps)
    }

    /// Detect data quality gaps
    fn detect_data_quality_gaps(&self, candles: &[FuturesOHLCVCandle]) -> Result<Vec<DataQualityGap>, HistoricalDataError> {
        let mut quality_gaps = Vec::new();

        for candle in candles.iter() {
            // Check for zero volume
            if candle.volume < self.config.min_volume_threshold {
                quality_gaps.push(DataQualityGap {
                    time_range: TimeRange {
                        start: candle.open_time,
                        end: candle.close_time,
                    },
                    issue_type: DataQualityIssueType::ZeroVolume,
                    severity: GapSeverity::Low,
                    description: format!("Zero volume candle at {}", candle.open_time),
                });
            }

            // Check for OHLC inconsistencies
            if !(candle.low <= candle.open && candle.open <= candle.high &&
                 candle.low <= candle.close && candle.close <= candle.high &&
                 candle.low <= candle.high) {
                quality_gaps.push(DataQualityGap {
                    time_range: TimeRange {
                        start: candle.open_time,
                        end: candle.close_time,
                    },
                    issue_type: DataQualityIssueType::OHLCInconsistency,
                    severity: GapSeverity::High,
                    description: format!("OHLC inconsistency: O={}, H={}, L={}, C={}", 
                                       candle.open, candle.high, candle.low, candle.close),
                });
            }

            // Check for invalid timestamps
            if candle.open_time >= candle.close_time {
                quality_gaps.push(DataQualityGap {
                    time_range: TimeRange {
                        start: candle.open_time,
                        end: candle.close_time,
                    },
                    issue_type: DataQualityIssueType::InvalidTimestamp,
                    severity: GapSeverity::Critical,
                    description: format!("Invalid timestamp: open_time={} >= close_time={}", 
                                       candle.open_time, candle.close_time),
                });
            }
        }

        Ok(quality_gaps)
    }

    /// Calculate overall gap impact score (0.0-1.0, lower is better)
    fn calculate_gap_impact_score(&self, gap_analysis: &GapAnalysisResult, total_candles: usize) -> f64 {
        if total_candles == 0 {
            return 1.0; // Maximum impact if no data
        }

        let mut impact_score = 0.0;
        
        // Weight different types of gaps
        let time_gap_weight = 0.4;
        let volume_gap_weight = 0.3;
        let quality_gap_weight = 0.3;

        // Calculate time gap impact
        let time_gap_impact = gap_analysis.time_gaps.len() as f64 / total_candles as f64;
        impact_score += time_gap_impact * time_gap_weight;

        // Calculate volume gap impact
        let volume_gap_impact = gap_analysis.volume_gaps.len() as f64 / total_candles as f64;
        impact_score += volume_gap_impact * volume_gap_weight;

        // Calculate quality gap impact with severity weighting
        let quality_gap_impact = gap_analysis.data_quality_gaps.iter()
            .map(|gap| match gap.severity {
                GapSeverity::Low => 0.25,
                GapSeverity::Medium => 0.5,
                GapSeverity::High => 0.75,
                GapSeverity::Critical => 1.0,
            })
            .sum::<f64>() / total_candles as f64;
        impact_score += quality_gap_impact * quality_gap_weight;

        impact_score.clamp(0.0, 1.0)
    }
}

// Default implementations for result structs
impl Default for VolumeValidationResult {
    fn default() -> Self {
        Self {
            total_volume: 0.0,
            average_volume: 0.0,
            zero_volume_candles: 0,
            outlier_volume_candles: 0,
            max_volume: 0.0,
            min_non_zero_volume: f64::INFINITY,
            distribution_quality_score: 0.0,
        }
    }
}

impl Default for PriceValidationResult {
    fn default() -> Self {
        Self {
            price_range: (0.0, 0.0),
            price_gaps_detected: 0,
            max_price_gap_percentage: 0.0,
            price_continuity_score: 1.0,
            ohlc_consistency_issues: 0,
        }
    }
}

impl Default for GapAnalysisResult {
    fn default() -> Self {
        Self {
            time_gaps: Vec::new(),
            volume_gaps: Vec::new(),
            data_quality_gaps: Vec::new(),
            gap_impact_score: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn create_test_candle(open_time: i64, close_time: i64, ohlc: (f64, f64, f64, f64), volume: f64) -> FuturesOHLCVCandle {
        FuturesOHLCVCandle::new_from_values(
            open_time, close_time, ohlc.0, ohlc.1, ohlc.2, ohlc.3, 
            volume, 100, volume * 0.6, true
        )
    }

    #[test]
    fn test_volume_profile_validator_creation() {
        let config = VolumeProfileValidationConfig::default();
        let validator = VolumeProfileValidator::new(config);
        assert_eq!(validator.config.min_volume_threshold, 0.000001);
    }

    #[test]
    fn test_validate_insufficient_candles() {
        let config = VolumeProfileValidationConfig {
            min_candles_for_validation: 10,
            ..Default::default()
        };
        let validator = VolumeProfileValidator::new(config);
        
        let candles = vec![
            create_test_candle(1000, 2000, (100.0, 101.0, 99.0, 100.5), 1000.0),
        ];
        
        let result = validator.validate_volume_profile_data(
            "BTCUSDT", 
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 
            &candles, 
            &[]
        ).unwrap();
        
        assert!(!result.is_valid);
        assert_eq!(result.validation_errors.len(), 1);
        assert_eq!(result.validation_errors[0].error_code, "INSUFFICIENT_CANDLES");
    }

    #[test]
    fn test_validate_good_data() {
        let config = VolumeProfileValidationConfig::default();
        let validator = VolumeProfileValidator::new(config);
        
        let mut candles = Vec::new();
        for i in 0..50 {
            candles.push(create_test_candle(
                i * 60000, 
                (i + 1) * 60000 - 1, 
                (100.0 + i as f64 * 0.1, 101.0 + i as f64 * 0.1, 99.0 + i as f64 * 0.1, 100.0 + i as f64 * 0.1), 
                1000.0 + i as f64 * 10.0
            ));
        }
        
        let result = validator.validate_volume_profile_data(
            "BTCUSDT", 
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 
            &candles, 
            &[]
        ).unwrap();
        
        assert!(result.is_valid);
        assert!(result.volume_validation.total_volume > 0.0);
        assert!(result.price_validation.price_continuity_score > 0.8);
    }

    #[test]
    fn test_ohlc_consistency_validation() {
        let config = VolumeProfileValidationConfig {
            min_candles_for_validation: 1,
            ..Default::default()
        };
        let validator = VolumeProfileValidator::new(config);
        
        let candles = vec![
            // Invalid OHLC: high < low
            create_test_candle(1000, 2000, (100.0, 98.0, 99.0, 100.0), 1000.0),
        ];
        
        let result = validator.validate_volume_profile_data(
            "BTCUSDT", 
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(), 
            &candles, 
            &[]
        ).unwrap();
        
        
        assert!(!result.is_valid);
        assert!(result.validation_errors.iter().any(|e| e.error_code == "OHLC_INCONSISTENCY"));
    }

    #[test]
    fn test_volume_distribution_quality() {
        let config = VolumeProfileValidationConfig::default();
        let validator = VolumeProfileValidator::new(config);
        
        // Test with consistent volumes
        let consistent_volumes = vec![1000.0; 10];
        let quality_score = validator.calculate_volume_distribution_quality(&consistent_volumes);
        assert!(quality_score > 0.9); // Should be high quality
        
        // Test with highly variable volumes
        let variable_volumes = vec![1.0, 1000.0, 1.0, 1000.0, 1.0];
        let quality_score = validator.calculate_volume_distribution_quality(&variable_volumes);
        assert!(quality_score < 0.8); // Should be lower quality than consistent volumes
    }
}