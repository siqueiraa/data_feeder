use std::fmt;
use rust_decimal::{prelude::*, Decimal};
use rust_decimal_macros::dec;
use std::convert::TryFrom;

/// Custom error types for precision-related operations
#[derive(Debug, Clone, PartialEq)]
pub enum PrecisionError {
    /// Price is invalid (negative)
    InvalidPrice(Decimal),
    /// Price increment is invalid (negative, or zero)
    InvalidIncrement(Decimal),
    /// Precision loss exceeds tolerance threshold
    PrecisionLoss {
        original: Decimal,
        reconstructed: Decimal,
        loss: Decimal,
        tolerance: Decimal,
    },
    /// Price is outside supported range
    PriceOutOfRange {
        price: Decimal,
        min: Decimal,
        max: Decimal,
    },
}

impl fmt::Display for PrecisionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrecisionError::InvalidPrice(price) => write!(f, "Invalid price: {}", price),
            PrecisionError::InvalidIncrement(increment) => write!(f, "Invalid price increment: {}", increment),
            PrecisionError::PrecisionLoss { original, reconstructed, loss, tolerance } => {
                write!(f, "Precision loss exceeds tolerance: original={}, reconstructed={}, loss={}, tolerance={}", 
                       original, reconstructed, loss, tolerance)
            },
            PrecisionError::PriceOutOfRange { price, min, max } => {
                write!(f, "Price {} is outside supported range [{}, {}]", price, min, max)
            },
        }
    }
}

impl std::error::Error for PrecisionError {}

/// Validation result for precision operations
#[derive(Debug, Clone)]
pub struct PrecisionValidation {
    pub original_price: Decimal,
    pub reconstructed_price: Decimal,
    pub precision_loss: Decimal,
    pub precision_percentage: Decimal,
    pub is_accurate: bool,
    pub tolerance: Decimal,
}

/// Report for edge cases detected in price mapping
#[derive(Debug, Clone)]
pub struct EdgeCaseReport {
    pub price: Decimal,
    pub increment: Decimal,
    pub edge_type: EdgeCaseType,
    pub description: String,
}

/// Types of edge cases that can occur in price mapping
#[derive(Debug, Clone, PartialEq)]
pub enum EdgeCaseType {
    /// Price exactly on increment boundary
    Boundary,
    /// Very small price relative to increment
    VerySmall,
    /// Very large price relative to increment
    VeryLarge,
    /// Price increment ratio causes precision issues
    IncrementRatio,
    /// Floating point representation issue
    FloatingPoint,
}

/// Manager for handling price precision and key mapping with guaranteed accuracy
#[derive(Debug, Clone)]
pub struct PricePrecisionManager {
    /// Default precision tolerance (0.01%)
    pub default_tolerance: Decimal,
    /// Minimum supported price
    pub min_price: Decimal,
    /// Maximum supported price
    pub max_price: Decimal,
    /// Maximum supported increment
    pub max_increment: Decimal,
    /// Minimum supported increment
    pub min_increment: Decimal,
}

impl Default for PricePrecisionManager {
    fn default() -> Self {
        Self {
            default_tolerance: Decimal::try_from(0.0001).unwrap(), // 0.01%
            min_price: Decimal::try_from(0.00000001).unwrap(),     // 1e-8 (crypto decimals)
            max_price: Decimal::try_from(1_000_000.0).unwrap(),    // 1 million (large stocks/futures)
            max_increment: Decimal::try_from(10_000.0).unwrap(),   // 10k (very large increments)
            min_increment: Decimal::try_from(0.000_000_001).unwrap(), // 1e-9 (ultra-fine precision)
        }
    }
}

impl PricePrecisionManager {
    /// Create a new precision manager with custom parameters
    pub fn new(tolerance: Decimal, min_price: Decimal, max_price: Decimal, min_increment: Decimal, max_increment: Decimal) -> Self {
        Self {
            default_tolerance: tolerance,
            min_price,
            max_price,
            min_increment,
            max_increment,
        }
    }

    /// Convert price to normalized price key with guaranteed precision
    /// Uses banker-style rounding (round half to even) for boundary cases
    pub fn price_to_key(&self, price: Decimal, increment: Decimal) -> Result<i64, PrecisionError> {
        // Validate inputs
        self.validate_price(price)?;
        self.validate_increment(increment)?;
        
        // Handle edge case of zero increment
        if increment <= dec!(0.0) {
            return Err(PrecisionError::InvalidIncrement(increment));
        }

        // Use banker-style rounding for boundary cases
        let ratio = price / increment;
        let key = banker_round(ratio);
        
        Ok(key)
    }

    /// Convert price key back to price with accuracy validation
    pub fn key_to_price(&self, key: i64, increment: Decimal) -> Result<Decimal, PrecisionError> {
        self.validate_increment(increment)?;
        
        let price = Decimal::from(key) * increment;
        self.validate_price(price)?;
        
        Ok(price)
    }

    /// Validate price key conversion accuracy with round-trip precision check
    pub fn validate_precision(
        &self, 
        price: Decimal, 
        increment: Decimal
    ) -> Result<PrecisionValidation, PrecisionError> {
        self.validate_price(price)?;
        self.validate_increment(increment)?;
        
        let key = self.price_to_key(price, increment)?;
        let reconstructed = self.key_to_price(key, increment)?;
        
        let precision_loss = (reconstructed - price).abs();
        let precision_percentage = if price != Decimal::ZERO {
            (precision_loss / price.abs()) * dec!(100.0)
        } else {
            precision_loss * dec!(100.0)
        };
        
        let is_accurate = precision_percentage <= (self.default_tolerance * dec!(100.0));
        
        Ok(PrecisionValidation {
            original_price: price,
            reconstructed_price: reconstructed,
            precision_loss,
            precision_percentage,
            is_accurate,
            tolerance: self.default_tolerance,
        })
    }

    /// Validate price key conversion with custom tolerance
    pub fn validate_precision_with_tolerance(
        &self, 
        price: Decimal, 
        increment: Decimal, 
        tolerance: Decimal
    ) -> Result<PrecisionValidation, PrecisionError> {
        let _original_tolerance = self.default_tolerance;
        let mut manager = self.clone();
        manager.default_tolerance = tolerance;
        manager.validate_precision(price, increment)
    }

    /// Detect and handle edge cases in price mapping
    pub fn detect_edge_cases(&self, prices: &[Decimal], increment: Decimal) -> Vec<EdgeCaseReport> {
        let mut reports = Vec::new();
        
        for &price in prices {
            if self.validate_price(price).is_err() {
                continue;
            }
            
            let ratio = price / increment;
            let fractional = ratio.fract();
            
            // Check for boundary cases (within 0.1% of increment boundary)
            if (fractional - dec!(0.5)).abs() < dec!(0.001) {
                reports.push(EdgeCaseReport {
                    price,
                    increment,
                    edge_type: EdgeCaseType::Boundary,
                    description: format!("Price {} is exactly on increment boundary with increment {}", price, increment),
                });
            }
            
            // Check for very small prices
            if price < increment * dec!(0.01) {
                reports.push(EdgeCaseReport {
                    price,
                    increment,
                    edge_type: EdgeCaseType::VerySmall,
                    description: format!("Price {} is very small relative to increment {}", price, increment),
                });
            }
            
            // Check for very large prices
            if price > increment * dec!(1_000_000.0) {
                reports.push(EdgeCaseReport {
                    price,
                    increment,
                    edge_type: EdgeCaseType::VeryLarge,
                    description: format!("Price {} is very large relative to increment {}", price, increment),
                });
            }
            
            // Check for floating point representation issues
            let reconstructed = self.validate_precision(price, increment);
            if let Ok(validation) = reconstructed {
                if !validation.is_accurate {
                    reports.push(EdgeCaseReport {
                        price,
                        increment,
                        edge_type: EdgeCaseType::FloatingPoint,
                        description: format!("Floating point precision issue: original={}, reconstructed={}", 
                                           price, validation.reconstructed_price),
                    });
                }
            }
        }
        
        reports
    }

    /// Validate that all prices in a range can be mapped accurately
    pub fn validate_range_precision(&self, min_price: Decimal, max_price: Decimal, increment: Decimal) -> Result<Vec<PrecisionValidation>, PrecisionError> {
        self.validate_price(min_price)?;
        self.validate_price(max_price)?;
        self.validate_increment(increment)?;
        
        if min_price >= max_price {
            return Err(PrecisionError::InvalidPrice(min_price));
        }

        let mut validations = Vec::new();
        let mut current_price = min_price;
        
        while current_price <= max_price {
            let validation = self.validate_precision(current_price, increment)?;
            validations.push(validation);
            current_price += increment;
        }
        
        Ok(validations)
    }

    /// Check for duplicate mapping (different prices mapping to same key)
    pub fn check_duplicate_mapping(&self, prices: &[Decimal], increment: Decimal) -> Vec<(Decimal, Decimal, i64)> {
        let mut duplicates = Vec::new();
        let mut key_map: std::collections::HashMap<i64, Decimal> = std::collections::HashMap::new();
        
        for &price in prices {
            if let Ok(key) = self.price_to_key(price, increment) {
                if let Some(&existing_price) = key_map.get(&key) {
                    if (existing_price - price).abs() > Decimal::new(1, 10) {
                        duplicates.push((existing_price, price, key));
                    }
                } else {
                    key_map.insert(key, price);
                }
            }
        }
        
        duplicates
    }

    /// Validate price range coverage
    pub fn validate_range_coverage(&self, min_price: Decimal, max_price: Decimal, increment: Decimal) -> Result<bool, PrecisionError> {
        self.validate_price(min_price)?;
        self.validate_price(max_price)?;
        self.validate_increment(increment)?;
        
        let min_key = self.price_to_key(min_price, increment)?;
        let max_key = self.price_to_key(max_price, increment)?;
        
        // Check if we can cover the entire range without gaps
        let range = max_price - min_price;
        let expected_levels = ((range / increment).ceil()).to_i64().unwrap_or(0) + 1;
        let actual_levels = max_key - min_key + 1;
        
        Ok(expected_levels == actual_levels)
    }

    // Private helper methods
    
    fn validate_price(&self, price: Decimal) -> Result<(), PrecisionError> {
        if price < Decimal::ZERO {
            return Err(PrecisionError::InvalidPrice(price));
        }
        if price < self.min_price || price > self.max_price {
            return Err(PrecisionError::PriceOutOfRange {
                price,
                min: self.min_price,
                max: self.max_price,
            });
        }
        Ok(())
    }

    fn validate_increment(&self, increment: Decimal) -> Result<(), PrecisionError> {
        if increment <= Decimal::ZERO {
            return Err(PrecisionError::InvalidIncrement(increment));
        }
        if increment < self.min_increment || increment > self.max_increment {
            return Err(PrecisionError::InvalidIncrement(increment));
        }
        Ok(())
    }
}

/// Banker's rounding (round half to even) for consistent boundary handling
fn banker_round(value: Decimal) -> i64 {
    let rounded = value.round();
    rounded.to_i64().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_to_key_basic() {
        let manager = PricePrecisionManager::default();
        
        let key = manager.price_to_key(dec!(50000.25), dec!(0.01)).unwrap();
        assert_eq!(key, 5000025);
        
        let key = manager.price_to_key(dec!(100.0), dec!(0.5)).unwrap();
        assert_eq!(key, 200);
        
        let key = manager.price_to_key(dec!(0.0001), dec!(0.00000001)).unwrap();
        assert_eq!(key, 10000);
    }

    #[test]
    fn test_key_to_price_basic() {
        let manager = PricePrecisionManager::default();
        
        let price = manager.key_to_price(5000025, dec!(0.01)).unwrap();
        assert_eq!(price, dec!(50000.25));
        
        let price = manager.key_to_price(200, dec!(0.5)).unwrap();
        assert_eq!(price, dec!(100.0));
    }

    #[test]
    fn test_round_trip_precision() {
        let manager = PricePrecisionManager::default();
        
        let test_cases = vec![
            (dec!(50000.25), dec!(0.01)),
            (dec!(1.23456789), dec!(0.00000001)),
            (dec!(100000.0), dec!(1.0)),
            (dec!(0.0001), dec!(0.00000001)),
            (dec!(114367.6), dec!(0.01)), // Test case from real data
        ];
        
        for (price, increment) in test_cases {
            let validation = manager.validate_precision(price, increment).unwrap();
            assert!(validation.is_accurate, 
                   "Precision loss too high for price {}: {}%", 
                   price, validation.precision_percentage);
            assert!(validation.precision_percentage < dec!(0.01), 
                   "Precision should be within 0.01%, got {}%", 
                   validation.precision_percentage);
        }
    }

    #[test]
    fn test_boundary_handling() {
        let manager = PricePrecisionManager::default();
        
        // Test boundary cases with banker rounding
        let key = manager.price_to_key(dec!(100.005), dec!(0.01)).unwrap(); // Exactly 0.5
        assert_eq!(key, 10000); // Banker's rounding to even
        
        let key = manager.price_to_key(dec!(100.015), dec!(0.01)).unwrap(); // Exactly 0.5
        assert_eq!(key, 10002); // Banker's rounding to even
    }

    #[test]
    fn test_invalid_inputs() {
        let manager = PricePrecisionManager::default();
        
        // Invalid prices
        assert!(manager.price_to_key(Decimal::NEGATIVE_ONE, dec!(0.01)).is_err());
        assert!(manager.price_to_key(-dec!(100.0), dec!(0.01)).is_err());
        
        // Invalid increments
        assert!(manager.price_to_key(dec!(100.0), dec!(0.0)).is_err());
        assert!(manager.price_to_key(dec!(100.0), -dec!(0.01)).is_err());
    }

    #[test]
    fn test_edge_case_detection() {
        let manager = PricePrecisionManager::default();
        
        let prices = vec![dec!(100.005), dec!(0.0001), dec!(1000000.0), dec!(50.0)];
        let increment = dec!(0.01);
        
        let reports = manager.detect_edge_cases(&prices, increment);
        assert!(!reports.is_empty(), "Should detect edge cases");
        
        // Should detect boundary case
        let boundary_reports: Vec<_> = reports.iter()
            .filter(|r| r.edge_type == EdgeCaseType::Boundary)
            .collect();
        assert!(!boundary_reports.is_empty(), "Should detect boundary cases");
    }

    #[test]
    fn test_range_coverage_validation() {
        let manager = PricePrecisionManager::default();
        
        let is_covered = manager.validate_range_coverage(dec!(100.0), dec!(110.0), dec!(1.0)).unwrap();
        assert!(is_covered, "Range should be fully covered");
        
        let is_covered = manager.validate_range_coverage(dec!(100.0), dec!(105.5), dec!(1.0)).unwrap();
        assert!(is_covered, "Range should be fully covered with fractional end");
    }

    #[test]
    fn test_duplicate_mapping_detection() {
        let manager = PricePrecisionManager::default();
        
        // Use prices that clearly map to different keys
        let prices = vec![dec!(100.0), dec!(100.01), dec!(100.02), dec!(100.03)];
        let increment = dec!(0.01);
        
        let duplicates = manager.check_duplicate_mapping(&prices, increment);
        
        // These should all map to different keys
        assert!(duplicates.is_empty(), "No duplicates expected for clearly different prices");
        
        // Test actual duplicates - prices that round to the same increment
        let prices = vec![dec!(100.002), dec!(100.003), dec!(100.004)]; // All should map to same 0.01 bucket
        let duplicates = manager.check_duplicate_mapping(&prices, dec!(0.01));
        assert!(!duplicates.is_empty(), "Should detect duplicates for prices within same increment bucket");
    }

    #[test]
    fn test_banker_rounding() {
        assert_eq!(banker_round(2.5), 2);  // Round to even
        assert_eq!(banker_round(3.5), 4);  // Round to even
        assert_eq!(banker_round(1.4), 1);  // Round down
        assert_eq!(banker_round(1.6), 2);  // Round up
        assert_eq!(banker_round(-2.5), -2); // Negative round to even
        assert_eq!(banker_round(-3.5), -4); // Negative round to even
    }

    #[test]
    fn test_crypto_precision() {
        let manager = PricePrecisionManager::default();
        
        // Test Bitcoin-like precision
        let key = manager.price_to_key(dec!(50000.12345678), dec!(0.01)).unwrap();
        let _reconstructed = manager.key_to_price(key, dec!(0.01)).unwrap();
        
        let validation = manager.validate_precision(dec!(50000.12345678), dec!(0.01)).unwrap();
        assert!(validation.is_accurate);
        
        // Test Ethereum-like precision
        let _key = manager.price_to_key(dec!(3000.123456789012), dec!(0.0001)).unwrap();
        let validation = manager.validate_precision(dec!(3000.123456789012), dec!(0.0001)).unwrap();
        assert!(validation.is_accurate);
    }

    #[test]
    fn test_volume_conservation_validation() {
        let manager = PricePrecisionManager::default();
        
        // Test with realistic price ranges
        let validations = manager.validate_range_precision(dec!(114367.5), dec!(114367.7), dec!(0.01)).unwrap();
        
        for validation in validations {
            assert!(validation.is_accurate, 
                   "All prices should maintain precision: {}", validation.precision_percentage);
        }
    }
}