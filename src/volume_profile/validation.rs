use std::collections::HashMap;
use crate::volume_profile::structs::{PriceLevelData, VolumeProfileData};

/// Custom error types for volume conservation validation
#[derive(Debug, Clone, PartialEq)]
pub enum VolumeValidationError {
    /// Volume conservation violated - total doesn't match input
    VolumeConservationViolation {
        expected: f64,
        actual: f64,
        difference: f64,
    },
    /// Volume duplication detected - same volume counted multiple times
    VolumeDuplication {
        duplicated_volume: f64,
        count: usize,
    },
    /// Missing volume - input volume not fully distributed
    MissingVolume {
        expected: f64,
        distributed: f64,
        missing: f64,
    },
    /// Invalid volume data (negative, NaN, infinite)
    InvalidVolumeData(f64),
}

impl std::fmt::Display for VolumeValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolumeValidationError::VolumeConservationViolation { expected, actual, difference } => {
                write!(f, "Volume conservation violated: expected={}, actual={}, difference={}", 
                       expected, actual, difference)
            },
            VolumeValidationError::VolumeDuplication { duplicated_volume, count } => {
                write!(f, "Volume duplication detected: {} duplicated {} times", 
                       duplicated_volume, count)
            },
            VolumeValidationError::MissingVolume { expected, distributed, missing } => {
                write!(f, "Missing volume: expected={}, distributed={}, missing={}", 
                       expected, distributed, missing)
            },
            VolumeValidationError::InvalidVolumeData(volume) => {
                write!(f, "Invalid volume data: {}", volume)
            },
        }
    }
}

impl std::error::Error for VolumeValidationError {}

/// Validation result for volume conservation
#[derive(Debug, Clone)]
pub struct VolumeValidationResult {
    pub is_valid: bool,
    pub expected_volume: f64,
    pub actual_volume: f64,
    pub difference: f64,
    pub percentage_error: f64,
    pub details: Vec<String>,
}

impl VolumeValidationResult {
    /// Create a new valid result
    pub fn valid(expected: f64, actual: f64) -> Self {
        let difference = (expected - actual).abs();
        let percentage_error = if expected != 0.0 {
            (difference / expected.abs()) * 100.0
        } else {
            0.0
        };
        
        Self {
            is_valid: difference <= f64::EPSILON * 1000.0, // Allow for floating point precision
            expected_volume: expected,
            actual_volume: actual,
            difference,
            percentage_error,
            details: vec!["Volume conservation validated".to_string()],
        }
    }

    /// Create a new invalid result
    pub fn invalid(expected: f64, actual: f64, error: &str) -> Self {
        let difference = (expected - actual).abs();
        let percentage_error = if expected != 0.0 {
            (difference / expected.abs()) * 100.0
        } else {
            0.0
        };
        
        Self {
            is_valid: false,
            expected_volume: expected,
            actual_volume: actual,
            difference,
            percentage_error,
            details: vec![error.to_string()],
        }
    }

    /// Add validation detail
    pub fn add_detail(&mut self, detail: String) {
        self.details.push(detail);
    }
}

/// Distribution metrics for volume validation
#[derive(Debug, Clone)]
pub struct DistributionMetrics {
    pub total_levels: usize,
    pub non_zero_levels: usize,
    pub max_volume: f64,
    pub min_volume: f64,
    pub average_volume: f64,
    pub volume_concentration: f64, // Gini coefficient-like measure
}

/// Validator for ensuring volume conservation during price level aggregation
#[derive(Debug, Clone)]
pub struct VolumeConservationValidator;

impl VolumeConservationValidator {
    /// Validate volume conservation across all price levels
    pub fn validate_volume_conservation(
        expected_volume: f64,
        price_levels: &[PriceLevelData]
    ) -> Result<VolumeValidationResult, VolumeValidationError> {
        // Validate input volume
        if !expected_volume.is_finite() || expected_volume < 0.0 {
            return Err(VolumeValidationError::InvalidVolumeData(expected_volume));
        }

        // Calculate total distributed volume
        let mut total_distributed = 0.0;
        let mut level_volumes = Vec::new();
        
        for level in price_levels {
            // Validate each level's volume
            if !level.volume.is_finite() || level.volume < 0.0 {
                return Err(VolumeValidationError::InvalidVolumeData(level.volume));
            }
            
            total_distributed += level.volume;
            level_volumes.push(level.volume);
        }

        // Check for volume conservation
        let difference = (expected_volume - total_distributed).abs();
        let tolerance = f64::EPSILON * 1000.0; // Allow for floating point precision

        if difference > tolerance {
            let mut result = VolumeValidationResult::invalid(
                expected_volume, 
                total_distributed,
                &format!("Volume conservation violated: expected={}, actual={}, difference={}", 
                         expected_volume, total_distributed, difference)
            );
            
            // Add specific details about the violation
            if total_distributed < expected_volume {
                result.add_detail(format!("Missing volume: {} ({}%)", 
                                        expected_volume - total_distributed,
                                        ((expected_volume - total_distributed) / expected_volume) * 100.0));
            } else {
                result.add_detail(format!("Excess volume: {} ({}%)", 
                                        total_distributed - expected_volume,
                                        ((total_distributed - expected_volume) / expected_volume) * 100.0));
            }
            
            return Ok(result);
        }

        Ok(VolumeValidationResult::valid(expected_volume, total_distributed))
    }

    /// Calculate distribution metrics for price levels
    pub fn calculate_distribution_metrics(price_levels: &[PriceLevelData]) -> DistributionMetrics {
        let total_levels = price_levels.len();
        let non_zero_levels = price_levels.iter().filter(|l| l.volume > 0.0).count();
        
        let volumes: Vec<f64> = price_levels.iter().map(|l| l.volume).collect();
        let total_volume: f64 = volumes.iter().sum();
        
        let max_volume = volumes.iter().fold(0.0f64, |a, &b| a.max(b));
        let min_volume = volumes.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let average_volume = if total_levels > 0 { total_volume / total_levels as f64 } else { 0.0 };
        
        // Calculate volume concentration (Gini coefficient-like)
        let volume_concentration = if total_volume > 0.0 {
            let sorted_volumes = {
                let mut v = volumes.clone();
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                v
            };
            
            let mut _cum_sum = 0.0;
            let mut gini_numerator = 0.0;
            
            for (i, &volume) in sorted_volumes.iter().enumerate() {
                _cum_sum += volume;
                gini_numerator += (i as f64 + 1.0) * volume;
            }
            
            let gini = (2.0 * gini_numerator) / (total_volume * total_levels as f64) - 
                      (total_levels as f64 + 1.0) / total_levels as f64;
            gini.clamp(0.0, 1.0)
        } else {
            0.0
        };

        DistributionMetrics {
            total_levels,
            non_zero_levels,
            max_volume,
            min_volume,
            average_volume,
            volume_concentration,
        }
    }

    /// Detect volume duplication across price levels
    pub fn detect_volume_duplication(
        expected_volume: f64,
        price_levels: &[PriceLevelData]
    ) -> Result<(), VolumeValidationError> {
        let mut volume_counts = HashMap::new();
        
        for level in price_levels {
            // Use rounded volume as key to handle floating point precision
            let volume_key = (level.volume * 1000000.0).round() as i64; // 6 decimal precision
            *volume_counts.entry(volume_key).or_insert((0, level.volume)) = 
                (volume_counts.get(&volume_key).map(|(count, _)| count).unwrap_or(&0) + 1, level.volume);
        }

        // Check for suspicious duplication patterns
        for (_, (count, volume)) in volume_counts {
            if count > 1 && volume > 0.0 {
                // Check if this could indicate duplication
                let total_duplicated = volume * count as f64;
                if total_duplicated > expected_volume * 0.1 { // More than 10% of total
                    return Err(VolumeValidationError::VolumeDuplication {
                        duplicated_volume: volume,
                        count,
                    });
                }
            }
        }

        Ok(())
    }

    /// Validate a complete volume profile
    pub fn validate_volume_profile(profile: &VolumeProfileData) -> Result<VolumeValidationResult, VolumeValidationError> {
        let price_levels = &profile.price_levels;
        
        // First, validate volume conservation
        let mut result = Self::validate_volume_conservation(profile.total_volume, price_levels)?;
        
        // Then check for duplication
        if let Err(duplication_error) = Self::detect_volume_duplication(profile.total_volume, price_levels) {
            result.add_detail(format!("Volume duplication detected: {}", duplication_error));
            result.is_valid = false;
        }

        // Calculate and add distribution metrics
        let metrics = Self::calculate_distribution_metrics(price_levels);
        result.add_detail(format!("Distribution: {} levels, {} non-zero, concentration={:.3}", 
                                metrics.total_levels, metrics.non_zero_levels, metrics.volume_concentration));

        Ok(result)
    }

    /// Validate volume distribution accuracy
    pub fn validate_distribution_accuracy(
        original_trades: &[(f64, f64)], // (price, volume) pairs
        price_levels: &[PriceLevelData],
        _price_increment: f64,
    ) -> Result<DistributionMetrics, VolumeValidationError> {
        let original_total: f64 = original_trades.iter().map(|(_, v)| v).sum();
        let distributed_total: f64 = price_levels.iter().map(|l| l.volume).sum();

        // Validate totals match
        let difference = (original_total - distributed_total).abs();
        let tolerance = f64::EPSILON * 1000.0;

        if difference > tolerance {
            return Err(VolumeValidationError::VolumeConservationViolation {
                expected: original_total,
                actual: distributed_total,
                difference,
            });
        }

        Ok(Self::calculate_distribution_metrics(price_levels))
    }

    /// Generate a comprehensive validation report
    pub fn generate_validation_report(
        expected_volume: f64,
        price_levels: &[PriceLevelData]
    ) -> String {
        let mut report = String::new();
        
        report.push_str("Volume Conservation Validation Report\n");
        report.push_str("=====================================\n\n");
        
        match Self::validate_volume_conservation(expected_volume, price_levels) {
            Ok(result) => {
                report.push_str(&format!("Status: {}\n", if result.is_valid { "VALID" } else { "INVALID" }));
                report.push_str(&format!("Expected Volume: {:.6}\n", result.expected_volume));
                report.push_str(&format!("Actual Volume: {:.6}\n", result.actual_volume));
                report.push_str(&format!("Difference: {:.6}\n", result.difference));
                report.push_str(&format!("Percentage Error: {:.4}%\n", result.percentage_error));
                
                for detail in &result.details {
                    report.push_str(&format!("- {}\n", detail));
                }
            }
            Err(e) => {
                report.push_str(&format!("Error: {}\n", e));
            }
        }

        let metrics = Self::calculate_distribution_metrics(price_levels);
        report.push_str("\nDistribution Metrics:\n");
        report.push_str("---------------------\n");
        report.push_str(&format!("Total Price Levels: {}\n", metrics.total_levels));
        report.push_str(&format!("Non-zero Volume Levels: {}\n", metrics.non_zero_levels));
        report.push_str(&format!("Max Volume: {:.6}\n", metrics.max_volume));
        report.push_str(&format!("Min Volume: {:.6}\n", metrics.min_volume));
        report.push_str(&format!("Average Volume: {:.6}\n", metrics.average_volume));
        report.push_str(&format!("Volume Concentration: {:.4}\n", metrics.volume_concentration));

        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_conservation_valid() {
        let price_levels = vec![
            PriceLevelData { price: 100.0, volume: 100.0, percentage: 0.0, candle_count: 1 },
            PriceLevelData { price: 101.0, volume: 200.0, percentage: 0.0, candle_count: 2 },
            PriceLevelData { price: 102.0, volume: 300.0, percentage: 0.0, candle_count: 3 },
        ];
        
        let expected_volume = 600.0;
        let result = VolumeConservationValidator::validate_volume_conservation(expected_volume, &price_levels).unwrap();
        
        assert!(result.is_valid);
        assert_eq!(result.expected_volume, 600.0);
        assert_eq!(result.actual_volume, 600.0);
        assert!(result.difference < 1e-10);
    }

    #[test]
    fn test_volume_conservation_violation() {
        let price_levels = vec![
            PriceLevelData { price: 100.0, volume: 100.0, percentage: 0.0, candle_count: 1 },
            PriceLevelData { price: 101.0, volume: 200.0, percentage: 0.0, candle_count: 2 },
        ];
        
        let expected_volume = 600.0;
        let result = VolumeConservationValidator::validate_volume_conservation(expected_volume, &price_levels).unwrap();
        
        assert!(!result.is_valid);
        assert_eq!(result.expected_volume, 600.0);
        assert_eq!(result.actual_volume, 300.0);
        assert_eq!(result.difference, 300.0);
    }

    #[test]
    fn test_invalid_volume_data() {
        let price_levels = vec![
            PriceLevelData { price: 100.0, volume: -100.0, percentage: 0.0, candle_count: 1 },
        ];
        
        let expected_volume = 100.0;
        let result = VolumeConservationValidator::validate_volume_conservation(expected_volume, &price_levels);
        
        assert!(matches!(result, Err(VolumeValidationError::InvalidVolumeData(_))));
    }

    #[test]
    fn test_distribution_metrics() {
        let price_levels = vec![
            PriceLevelData { price: 100.0, volume: 100.0, percentage: 0.0, candle_count: 1 },
            PriceLevelData { price: 101.0, volume: 200.0, percentage: 0.0, candle_count: 2 },
            PriceLevelData { price: 102.0, volume: 0.0, percentage: 0.0, candle_count: 0 },
            PriceLevelData { price: 103.0, volume: 300.0, percentage: 0.0, candle_count: 3 },
        ];
        
        let metrics = VolumeConservationValidator::calculate_distribution_metrics(&price_levels);
        
        assert_eq!(metrics.total_levels, 4);
        assert_eq!(metrics.non_zero_levels, 3);
        assert_eq!(metrics.max_volume, 300.0);
        assert_eq!(metrics.min_volume, 0.0);
        assert_eq!(metrics.average_volume, 150.0);
        assert!(metrics.volume_concentration >= 0.0 && metrics.volume_concentration <= 1.0);
    }

    #[test]
    fn test_distribution_accuracy() {
        let original_trades = vec![
            (100.0, 100.0),
            (101.0, 200.0),
            (102.0, 300.0),
        ];
        
        let price_levels = vec![
            PriceLevelData { price: 100.0, volume: 100.0, percentage: 0.0, candle_count: 1 },
            PriceLevelData { price: 101.0, volume: 200.0, percentage: 0.0, candle_count: 2 },
            PriceLevelData { price: 102.0, volume: 300.0, percentage: 0.0, candle_count: 3 },
        ];
        
        let metrics = VolumeConservationValidator::validate_distribution_accuracy(
            &original_trades, 
            &price_levels, 
            1.0
        ).unwrap();
        
        assert_eq!(metrics.total_levels, 3);
        assert_eq!(metrics.max_volume, 300.0);
    }

    #[test]
    fn test_empty_price_levels() {
        let price_levels = vec![];
        let expected_volume = 0.0;
        
        let result = VolumeConservationValidator::validate_volume_conservation(expected_volume, &price_levels).unwrap();
        
        assert!(result.is_valid);
        assert_eq!(result.expected_volume, 0.0);
        assert_eq!(result.actual_volume, 0.0);
    }

    #[test]
    fn test_generate_validation_report() {
        let price_levels = vec![
            PriceLevelData { price: 100.0, volume: 100.0, percentage: 0.0, candle_count: 1 },
            PriceLevelData { price: 101.0, volume: 200.0, percentage: 0.0, candle_count: 2 },
        ];
        
        let expected_volume = 300.0;
        let report = VolumeConservationValidator::generate_validation_report(expected_volume, &price_levels);
        
        assert!(report.contains("VALID"));
        assert!(report.contains("300.0"));
        assert!(report.contains("Distribution Metrics"));
    }
}