use std::fmt;

/// Custom error types for precision-related operations
#[derive(Debug, Clone, PartialEq)]
pub enum PrecisionError {
    /// Price is invalid (NaN, infinite, or negative)
    InvalidPrice(f64),
    /// Price increment is invalid (NaN, infinite, negative, or zero)
    InvalidIncrement(f64),
    /// Precision loss exceeds tolerance threshold
    PrecisionLoss {
        original: f64,
        reconstructed: f64,
        loss: f64,
        tolerance: f64,
    },
    /// Price is outside supported range
    PriceOutOfRange {
        price: f64,
        min: f64,
        max: f64,
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
    pub original_price: f64,
    pub reconstructed_price: f64,
    pub precision_loss: f64,
    pub precision_percentage: f64,
    pub is_accurate: bool,
    pub tolerance: f64,
}

/// Report for edge cases detected in price mapping
#[derive(Debug, Clone)]
pub struct EdgeCaseReport {
    pub price: f64,
    pub increment: f64,
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
    pub default_tolerance: f64,
    /// Minimum supported price
    pub min_price: f64,
    /// Maximum supported price
    pub max_price: f64,
    /// Maximum supported increment
    pub max_increment: f64,
    /// Minimum supported increment
    pub min_increment: f64,
}

impl Default for PricePrecisionManager {
    fn default() -> Self {
        Self {
            default_tolerance: 0.0001, // 0.01%
            min_price: 0.000_000_01,   // 1e-8 (crypto decimals)
            max_price: 1_000_000.0,    // 1 million (large stocks/futures)
            max_increment: 10_000.0,   // 10k (very large increments)
            min_increment: 0.000_000_001, // 1e-9 (ultra-fine precision)
        }
    }
}

impl PricePrecisionManager {
    /// Create a new precision manager with custom parameters
    pub fn new(tolerance: f64, min_price: f64, max_price: f64, min_increment: f64, max_increment: f64) -> Self {
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
    pub fn price_to_key(&self, price: f64, increment: f64) -> Result<i64, PrecisionError> {
        // Validate inputs
        self.validate_price(price)?;
        self.validate_increment(increment)?;
        
        // Handle edge case of zero increment
        if increment <= 0.0 {
            return Err(PrecisionError::InvalidIncrement(increment));
        }

        // Use banker-style rounding for boundary cases
        let ratio = price / increment;
        let key = banker_round(ratio);
        
        Ok(key)
    }

    /// Convert price key back to price with accuracy validation
    pub fn key_to_price(&self, key: i64, increment: f64) -> Result<f64, PrecisionError> {
        self.validate_increment(increment)?;
        
        let price = key as f64 * increment;
        self.validate_price(price)?;
        
        Ok(price)
    }

    /// Validate price key conversion accuracy with round-trip precision check
    pub fn validate_precision(&self, price: f64, increment: f64) -> Result<PrecisionValidation, PrecisionError> {
        self.validate_price(price)?;
        self.validate_increment(increment)?;
        
        let key = self.price_to_key(price, increment)?;
        let reconstructed = self.key_to_price(key, increment)?;
        
        let precision_loss = (reconstructed - price).abs();
        let precision_percentage = if price != 0.0 {
            (precision_loss / price.abs()) * 100.0
        } else {
            precision_loss * 100.0
        };
        
        let is_accurate = precision_percentage <= (self.default_tolerance * 100.0);
        
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
    pub fn validate_precision_with_tolerance(&self, price: f64, increment: f64, tolerance: f64) -> Result<PrecisionValidation, PrecisionError> {
        let _original_tolerance = self.default_tolerance;
        let mut manager = self.clone();
        manager.default_tolerance = tolerance;
        manager.validate_precision(price, increment)
    }

    /// Detect and handle edge cases in price mapping
    pub fn detect_edge_cases(&self, prices: &[f64], increment: f64) -> Vec<EdgeCaseReport> {
        let mut reports = Vec::new();
        
        for &price in prices {
            if self.validate_price(price).is_err() {
                continue;
            }
            
            let ratio = price / increment;
            let fractional = ratio.fract();
            
            // Check for boundary cases (within 0.1% of increment boundary)
            if (fractional - 0.5).abs() < 0.001 {
                reports.push(EdgeCaseReport {
                    price,
                    increment,
                    edge_type: EdgeCaseType::Boundary,
                    description: format!("Price {} is exactly on increment boundary with increment {}", price, increment),
                });
            }
            
            // Check for very small prices
            if price < increment * 0.01 {
                reports.push(EdgeCaseReport {
                    price,
                    increment,
                    edge_type: EdgeCaseType::VerySmall,
                    description: format!("Price {} is very small relative to increment {}", price, increment),
                });
            }
            
            // Check for very large prices
            if price > increment * 1_000_000.0 {
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
    pub fn validate_range_precision(&self, min_price: f64, max_price: f64, increment: f64) -> Result<Vec<PrecisionValidation>, PrecisionError> {
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
    pub fn check_duplicate_mapping(&self, prices: &[f64], increment: f64) -> Vec<(f64, f64, i64)> {
        let mut duplicates = Vec::new();
        let mut key_map: std::collections::HashMap<i64, f64> = std::collections::HashMap::new();
        
        for &price in prices {
            if let Ok(key) = self.price_to_key(price, increment) {
                if let Some(&existing_price) = key_map.get(&key) {
                    if (existing_price - price).abs() > f64::EPSILON {
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
    pub fn validate_range_coverage(&self, min_price: f64, max_price: f64, increment: f64) -> Result<bool, PrecisionError> {
        self.validate_price(min_price)?;
        self.validate_price(max_price)?;
        self.validate_increment(increment)?;
        
        let min_key = self.price_to_key(min_price, increment)?;
        let max_key = self.price_to_key(max_price, increment)?;
        
        // Check if we can cover the entire range without gaps
        let expected_levels = ((max_price - min_price) / increment).ceil() as i64 + 1;
        let actual_levels = max_key - min_key + 1;
        
        Ok(expected_levels == actual_levels)
    }

    // Private helper methods
    
    fn validate_price(&self, price: f64) -> Result<(), PrecisionError> {
        if !price.is_finite() {
            return Err(PrecisionError::InvalidPrice(price));
        }
        if price < 0.0 {
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

    fn validate_increment(&self, increment: f64) -> Result<(), PrecisionError> {
        if !increment.is_finite() || increment <= 0.0 {
            return Err(PrecisionError::InvalidIncrement(increment));
        }
        if increment < self.min_increment || increment > self.max_increment {
            return Err(PrecisionError::InvalidIncrement(increment));
        }
        Ok(())
    }
}

/// Banker's rounding (round half to even) for consistent boundary handling
fn banker_round(value: f64) -> i64 {
    let fractional = value.fract().abs();
    
    // If exactly 0.5, use even rounding
    if (fractional - 0.5).abs() < f64::EPSILON {
        let truncated = value.trunc() as i64;
        if truncated % 2 == 0 {
            // Already even, keep truncated value
            truncated
        } else {
            // Make it even by rounding away from zero
            if value >= 0.0 {
                truncated + 1
            } else {
                truncated - 1
            }
        }
    } else {
        // Not exactly 0.5, use standard rounding
        value.round() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_to_key_basic() {
        let manager = PricePrecisionManager::default();
        
        let key = manager.price_to_key(50000.25, 0.01).unwrap();
        assert_eq!(key, 5000025);
        
        let key = manager.price_to_key(100.0, 0.5).unwrap();
        assert_eq!(key, 200);
        
        let key = manager.price_to_key(0.0001, 0.00000001).unwrap();
        assert_eq!(key, 10000);
    }

    #[test]
    fn test_key_to_price_basic() {
        let manager = PricePrecisionManager::default();
        
        let price = manager.key_to_price(5000025, 0.01).unwrap();
        assert_eq!(price, 50000.25);
        
        let price = manager.key_to_price(200, 0.5).unwrap();
        assert_eq!(price, 100.0);
    }

    #[test]
    fn test_round_trip_precision() {
        let manager = PricePrecisionManager::default();
        
        let test_cases = vec![
            (50000.25, 0.01),
            (1.23456789, 0.00000001),
            (100000.0, 1.0),
            (0.0001, 0.00000001),
            (114367.6, 0.01), // Test case from real data
        ];
        
        for (price, increment) in test_cases {
            let validation = manager.validate_precision(price, increment).unwrap();
            assert!(validation.is_accurate, 
                   "Precision loss too high for price {}: {}%", 
                   price, validation.precision_percentage);
            assert!(validation.precision_percentage < 0.01, 
                   "Precision should be within 0.01%, got {}%", 
                   validation.precision_percentage);
        }
    }

    #[test]
    fn test_boundary_handling() {
        let manager = PricePrecisionManager::default();
        
        // Test boundary cases with banker rounding
        let key = manager.price_to_key(100.005, 0.01).unwrap(); // Exactly 0.5
        assert_eq!(key, 10000); // Banker's rounding to even
        
        let key = manager.price_to_key(100.015, 0.01).unwrap(); // Exactly 0.5
        assert_eq!(key, 10002); // Banker's rounding to even
    }

    #[test]
    fn test_invalid_inputs() {
        let manager = PricePrecisionManager::default();
        
        // Invalid prices
        assert!(manager.price_to_key(f64::NAN, 0.01).is_err());
        assert!(manager.price_to_key(f64::INFINITY, 0.01).is_err());
        assert!(manager.price_to_key(-100.0, 0.01).is_err());
        
        // Invalid increments
        assert!(manager.price_to_key(100.0, 0.0).is_err());
        assert!(manager.price_to_key(100.0, -0.01).is_err());
        assert!(manager.price_to_key(100.0, f64::NAN).is_err());
    }

    #[test]
    fn test_edge_case_detection() {
        let manager = PricePrecisionManager::default();
        
        let prices = vec![100.005, 0.0001, 1000000.0, 50.0];
        let increment = 0.01;
        
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
        
        let is_covered = manager.validate_range_coverage(100.0, 110.0, 1.0).unwrap();
        assert!(is_covered, "Range should be fully covered");
        
        let is_covered = manager.validate_range_coverage(100.0, 105.5, 1.0).unwrap();
        assert!(is_covered, "Range should be fully covered with fractional end");
    }

    #[test]
    fn test_duplicate_mapping_detection() {
        let manager = PricePrecisionManager::default();
        
        // Use prices that clearly map to different keys
        let prices = vec![100.0, 100.01, 100.02, 100.03];
        let increment = 0.01;
        
        let duplicates = manager.check_duplicate_mapping(&prices, increment);
        
        // These should all map to different keys
        assert!(duplicates.is_empty(), "No duplicates expected for clearly different prices");
        
        // Test actual duplicates - prices that round to the same increment
        let prices = vec![100.002, 100.003, 100.004]; // All should map to same 0.01 bucket
        let duplicates = manager.check_duplicate_mapping(&prices, 0.01);
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
        let key = manager.price_to_key(50000.12345678, 0.01).unwrap();
        let _reconstructed = manager.key_to_price(key, 0.01).unwrap();
        
        let validation = manager.validate_precision(50000.12345678, 0.01).unwrap();
        assert!(validation.is_accurate);
        
        // Test Ethereum-like precision
        let _key = manager.price_to_key(3000.123456789012, 0.0001).unwrap();
        let validation = manager.validate_precision(3000.123456789012, 0.0001).unwrap();
        assert!(validation.is_accurate);
    }

    #[test]
    fn test_volume_conservation_validation() {
        let manager = PricePrecisionManager::default();
        
        // Test with realistic price ranges
        let _prices = [114367.5, 114367.6, 114367.7];
        let increment = 0.01;
        
        let validations = manager.validate_range_precision(114367.5, 114367.7, increment).unwrap();
        
        for validation in validations {
            assert!(validation.is_accurate, 
                   "All prices should maintain precision: {}", validation.precision_percentage);
        }
    }
}