//! Optimized numeric types for volume profile calculations
//! 
//! This module provides high-performance alternatives to rust_decimal for
//! performance-critical paths in volume profile calculations.

use fixed::types::I64F64; // High precision for price calculations
use rust_decimal::Decimal;
use rust_decimal::prelude::{ToPrimitive, FromPrimitive};
use std::fmt;

/// Fixed-point price type for high-performance price calculations
/// Uses 64.64 fixed-point representation for balance of precision and speed
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OptimizedPrice(I64F64);

impl OptimizedPrice {
    /// Zero price constant
    pub const ZERO: Self = Self(I64F64::ZERO);
    
    /// Maximum price constant
    pub const MAX: Self = Self(I64F64::MAX);
    
    /// Minimum price constant  
    pub const MIN: Self = Self(I64F64::MIN);
    
    /// Create from f64 with bounds checking
    pub fn from_f64(value: f64) -> Self {
        Self(I64F64::from_num(value.clamp(-1e15, 1e15)))
    }
    
    /// Create from Decimal for compatibility
    pub fn from_decimal(decimal: Decimal) -> Self {
        Self(I64F64::from_num(decimal.to_f64().unwrap_or(0.0).clamp(-1e15, 1e15)))
    }
    
    /// Convert to f64 for external interfaces
    pub fn to_f64(self) -> f64 {
        self.0.to_num()
    }
    
    /// Convert to Decimal for compatibility
    pub fn to_decimal(self) -> Decimal {
        Decimal::from_f64(self.0.to_num()).unwrap_or(Decimal::ZERO)
    }
    
    /// Add two prices
    /// **Deprecated:** Use standard `+` operator instead
    #[deprecated(since = "1.4.0", note = "Use standard + operator instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: Self) -> Self {
        self + other
    }
    
    /// Subtract two prices
    /// **Deprecated:** Use standard `-` operator instead
    #[deprecated(since = "1.4.0", note = "Use standard - operator instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, other: Self) -> Self {
        self - other
    }
    
    /// Multiply price by a factor
    /// **Deprecated:** Use standard `*` operator instead
    #[deprecated(since = "1.4.0", note = "Use standard * operator instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, factor: f64) -> Self {
        self * factor
    }
    
    /// Divide price by a divisor
    /// **Deprecated:** Use standard `/` operator instead
    #[deprecated(since = "1.4.0", note = "Use standard / operator instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn div(self, divisor: f64) -> Self {
        self / divisor
    }
    
    /// Get absolute value
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }
    
    /// Get minimum of two prices
    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }
    
    /// Get maximum of two prices
    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }
}

impl Default for OptimizedPrice {
    fn default() -> Self {
        Self::ZERO
    }
}

impl fmt::Display for OptimizedPrice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<f64> for OptimizedPrice {
    fn from(value: f64) -> Self {
        Self::from_f64(value)
    }
}

impl From<OptimizedPrice> for f64 {
    fn from(price: OptimizedPrice) -> f64 {
        price.to_f64()
    }
}

impl From<Decimal> for OptimizedPrice {
    fn from(decimal: Decimal) -> Self {
        Self::from_decimal(decimal)
    }
}

impl From<OptimizedPrice> for Decimal {
    fn from(price: OptimizedPrice) -> Decimal {
        price.to_decimal()
    }
}

impl std::ops::Add for OptimizedPrice {
    type Output = Self;
    
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::Sub for OptimizedPrice {
    type Output = Self;
    
    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl std::ops::Mul<f64> for OptimizedPrice {
    type Output = Self;
    
    fn mul(self, rhs: f64) -> Self::Output {
        Self(self.0 * I64F64::from_num(rhs))
    }
}

impl std::ops::Div<f64> for OptimizedPrice {
    type Output = Self;
    
    fn div(self, rhs: f64) -> Self::Output {
        if rhs != 0.0 {
            Self(self.0 / I64F64::from_num(rhs))
        } else {
            Self::ZERO
        }
    }
}

/// Fast floating-point volume type for volume calculations
/// Uses f64 for maximum performance where high precision is not critical
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct OptimizedVolume(f64);

impl OptimizedVolume {
    /// Zero volume constant
    pub const ZERO: Self = Self(0.0);
    
    /// Create from f64
    pub fn from_f64(value: f64) -> Self {
        Self(value.max(0.0)) // Volume cannot be negative
    }
    
    /// Create from Decimal for compatibility
    pub fn from_decimal(decimal: Decimal) -> Self {
        Self(decimal.to_f64().unwrap_or(0.0).max(0.0))
    }
    
    /// Convert to f64
    pub fn to_f64(self) -> f64 {
        self.0
    }
    
    /// Convert to Decimal for compatibility
    pub fn to_decimal(self) -> Decimal {
        Decimal::from_f64(self.0).unwrap_or(Decimal::ZERO)
    }
    
    /// Add two volumes
    /// **Deprecated:** Use standard `+` operator instead
    #[deprecated(since = "1.4.0", note = "Use standard + operator instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: Self) -> Self {
        self + other
    }
    
    /// Subtract two volumes (clamped to zero)
    /// **Deprecated:** Use standard `-` operator instead
    #[deprecated(since = "1.4.0", note = "Use standard - operator instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, other: Self) -> Self {
        self - other
    }
    
    /// Multiply volume by a factor
    /// **Deprecated:** Use standard `*` operator instead
    #[deprecated(since = "1.4.0", note = "Use standard * operator instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, factor: f64) -> Self {
        self * factor
    }
    
    /// Divide volume by a divisor
    /// **Deprecated:** Use standard `/` operator instead
    #[deprecated(since = "1.4.0", note = "Use standard / operator instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn div(self, divisor: f64) -> Self {
        self / divisor
    }
    
    /// Check if volume is effectively zero
    pub fn is_zero(self) -> bool {
        self.0 < f64::EPSILON
    }
    
    /// Get maximum of two volumes
    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }
}

impl Default for OptimizedVolume {
    fn default() -> Self {
        Self::ZERO
    }
}

impl fmt::Display for OptimizedVolume {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<f64> for OptimizedVolume {
    fn from(value: f64) -> Self {
        Self::from_f64(value)
    }
}

impl From<OptimizedVolume> for f64 {
    fn from(volume: OptimizedVolume) -> f64 {
        volume.to_f64()
    }
}

impl From<Decimal> for OptimizedVolume {
    fn from(decimal: Decimal) -> Self {
        Self::from_decimal(decimal)
    }
}

impl From<OptimizedVolume> for Decimal {
    fn from(volume: OptimizedVolume) -> Decimal {
        volume.to_decimal()
    }
}

impl std::ops::Add for OptimizedVolume {
    type Output = Self;
    
    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::Sub for OptimizedVolume {
    type Output = Self;
    
    fn sub(self, rhs: Self) -> Self::Output {
        Self((self.0 - rhs.0).max(0.0))
    }
}

impl std::ops::Mul<f64> for OptimizedVolume {
    type Output = Self;
    
    fn mul(self, rhs: f64) -> Self::Output {
        Self((self.0 * rhs).max(0.0))
    }
}

impl std::ops::Div<f64> for OptimizedVolume {
    type Output = Self;
    
    fn div(self, rhs: f64) -> Self::Output {
        if rhs != 0.0 {
            Self((self.0 / rhs).max(0.0))
        } else {
            Self::ZERO
        }
    }
}

/// Utility functions for numeric type conversions
pub mod conversions {
    use super::*;
    
    /// Convert candle OHLCV data to optimized types for calculations
    pub fn convert_candle_data(
        open: f64, high: f64, low: f64, close: f64, volume: f64
    ) -> (OptimizedPrice, OptimizedPrice, OptimizedPrice, OptimizedPrice, OptimizedVolume) {
        (
            OptimizedPrice::from_f64(open),
            OptimizedPrice::from_f64(high),
            OptimizedPrice::from_f64(low),
            OptimizedPrice::from_f64(close),
            OptimizedVolume::from_f64(volume),
        )
    }
    
    /// Fast price increment calculation using optimized types
    pub fn calculate_price_increment_fast(price_range: f64, target_levels: u32) -> OptimizedPrice {
        if target_levels > 0 {
            OptimizedPrice::from_f64(price_range / target_levels as f64)
        } else {
            OptimizedPrice::from_f64(0.01) // Default increment
        }
    }
    
    /// Fast VWAP calculation using optimized types
    pub fn calculate_vwap_fast(price_volume_pairs: &[(OptimizedPrice, OptimizedVolume)]) -> OptimizedPrice {
        if price_volume_pairs.is_empty() {
            return OptimizedPrice::ZERO;
        }
        
        let mut total_value = 0.0;
        let mut total_volume = 0.0;
        
        for &(price, volume) in price_volume_pairs {
            let value = price.to_f64() * volume.to_f64();
            total_value += value;
            total_volume += volume.to_f64();
        }
        
        if total_volume > 0.0 {
            OptimizedPrice::from_f64(total_value / total_volume)
        } else {
            OptimizedPrice::ZERO
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    
    #[test]
    fn test_optimized_price_basic_operations() {
        let price1 = OptimizedPrice::from_f64(100.50);
        let price2 = OptimizedPrice::from_f64(200.25);
        
        // Addition
        let sum = price1 + price2;
        assert!((sum.to_f64() - 300.75).abs() < 1e-10);
        
        // Subtraction
        let diff = price2 - price1;
        assert!((diff.to_f64() - 99.75).abs() < 1e-10);
        
        // Multiplication
        let product = price1 * 2.0;
        assert!((product.to_f64() - 201.0).abs() < 1e-10);
        
        // Division
        let quotient = price2 / 2.0;
        assert!((quotient.to_f64() - 100.125).abs() < 1e-10);
    }
    
    #[test]
    fn test_optimized_price_decimal_conversion() {
        let decimal = dec!(123.456);
        let price = OptimizedPrice::from_decimal(decimal);
        let back_to_decimal = price.to_decimal();
        
        // Should maintain reasonable precision
        let diff = (decimal - back_to_decimal).abs();
        assert!(diff < dec!(1e-10));
    }
    
    #[test]
    fn test_optimized_volume_operations() {
        let vol1 = OptimizedVolume::from_f64(1000.0);
        let vol2 = OptimizedVolume::from_f64(2000.0);
        
        // Addition
        let sum = vol1 + vol2;
        assert_eq!(sum.to_f64(), 3000.0);
        
        // Subtraction with clamping
        let diff = vol1 - vol2;
        assert_eq!(diff.to_f64(), 0.0); // Should be clamped to zero
        
        // Division
        let quotient = vol2 / 2.0;
        assert_eq!(quotient.to_f64(), 1000.0);
    }
    
    #[test]
    fn test_conversion_functions() {
        let (open, high, low, close, volume) = conversions::convert_candle_data(
            100.0, 101.0, 99.0, 100.5, 1500.0
        );
        
        assert_eq!(open.to_f64(), 100.0);
        assert_eq!(high.to_f64(), 101.0);
        assert_eq!(low.to_f64(), 99.0);
        assert_eq!(close.to_f64(), 100.5);
        assert_eq!(volume.to_f64(), 1500.0);
    }
    
    #[test]
    fn test_vwap_calculation() {
        let data = vec![
            (OptimizedPrice::from_f64(100.0), OptimizedVolume::from_f64(1000.0)),
            (OptimizedPrice::from_f64(101.0), OptimizedVolume::from_f64(2000.0)),
            (OptimizedPrice::from_f64(102.0), OptimizedVolume::from_f64(1000.0)),
        ];
        
        let vwap = conversions::calculate_vwap_fast(&data);
        
        // VWAP = (100*1000 + 101*2000 + 102*1000) / (1000+2000+1000)
        // VWAP = (100000 + 202000 + 102000) / 4000 = 404000 / 4000 = 101.0
        assert!((vwap.to_f64() - 101.0).abs() < 1e-10);
    }
    
    #[test]
    fn test_bounds_checking() {
        // Test extremely large values
        let large_price = OptimizedPrice::from_f64(1e20);
        assert!(large_price.to_f64().is_finite());
        
        // Test negative volume clamping
        let negative_volume = OptimizedVolume::from_f64(-100.0);
        assert_eq!(negative_volume.to_f64(), 0.0);
    }
    
    #[test]
    fn test_performance_vs_decimal() {
        use std::time::Instant;
        
        // Test optimized types performance
        let start = Instant::now();
        let mut sum_optimized = OptimizedPrice::ZERO;
        for i in 0..100000 {
            let price = OptimizedPrice::from_f64(100.0 + i as f64 * 0.01);
            sum_optimized = sum_optimized + price;
        }
        let optimized_time = start.elapsed();
        
        // Test Decimal performance
        let start = Instant::now();
        let mut sum_decimal = Decimal::ZERO;
        for i in 0..100000 {
            let price = Decimal::from_f64(100.0 + i as f64 * 0.01).unwrap_or(Decimal::ZERO);
            sum_decimal += price;
        }
        let decimal_time = start.elapsed();
        
        println!("Optimized: {:?}, Decimal: {:?}", optimized_time, decimal_time);
        
        // Results should be similar
        let diff = (sum_optimized.to_decimal() - sum_decimal).abs();
        assert!(diff < dec!(1.0)); // Allow for small differences due to precision
        
        // Optimized types should be faster (this is not enforced in test, just for observation)
        println!("Performance improvement: {:.2}x", 
                decimal_time.as_nanos() as f64 / optimized_time.as_nanos() as f64);
    }
}