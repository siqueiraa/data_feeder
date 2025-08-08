//! Shared data structures optimized for performance
//! 
//! This module provides Arc-wrapped data structures to minimize cloning
//! in hot paths throughout the application.

use std::sync::Arc;
use rustc_hash::FxHashMap;
use std::sync::OnceLock;
use foldhash::HashMap as FoldHashMap;
use crate::historical::structs::FuturesOHLCVCandle;
use crate::technical_analysis::structs::IndicatorOutput;

/// Arc-wrapped candle for efficient sharing
pub type SharedCandle = Arc<FuturesOHLCVCandle>;

/// Arc-wrapped indicator output for efficient sharing
pub type SharedIndicatorOutput = Arc<IndicatorOutput>;

/// Arc-wrapped string for symbols to avoid cloning
pub type SharedSymbol = Arc<str>;

/// Arc-wrapped string for intervals to avoid cloning
pub type SharedInterval = Arc<str>;

/// Arc-wrapped vector of candles for efficient sharing
pub type SharedCandles = Arc<Vec<FuturesOHLCVCandle>>;

/// Arc-wrapped vector of shared candles for ultra-efficient sharing
pub type SharedCandleVec = Arc<Vec<SharedCandle>>;

/// Creates a shared symbol from a string
pub fn shared_symbol(symbol: &str) -> SharedSymbol {
    Arc::from(symbol)
}

/// Creates a shared interval from a string
pub fn shared_interval(interval: &str) -> SharedInterval {
    Arc::from(interval)
}

/// Creates a shared candle
pub fn shared_candle(candle: FuturesOHLCVCandle) -> SharedCandle {
    Arc::new(candle)
}

/// Creates shared candles from a vector
pub fn shared_candles(candles: Vec<FuturesOHLCVCandle>) -> SharedCandles {
    Arc::new(candles)
}

/// Creates shared indicator output
pub fn shared_indicator_output(output: IndicatorOutput) -> SharedIndicatorOutput {
    Arc::new(output)
}

/// Creates a vector of shared candles efficiently
pub fn shared_candle_vec(candles: Vec<SharedCandle>) -> SharedCandleVec {
    Arc::new(candles)
}

/// Converts shared candles back to owned candles (for compatibility)
pub fn unshare_candles(shared_candles: &[SharedCandle]) -> Vec<FuturesOHLCVCandle> {
    shared_candles.iter().map(|shared| (**shared).clone()).collect()
}

/// String interner for frequently used symbols and intervals (general purpose)
static STRING_INTERNER: OnceLock<std::sync::Mutex<FxHashMap<String, Arc<str>>>> = OnceLock::new();

/// Symbol-optimized interner using foldhash for high-frequency symbol lookups
/// Optimized for short string keys like "BTCUSDT", "ETHUSDT", etc.
static SYMBOL_INTERNER: OnceLock<std::sync::Mutex<FoldHashMap<String, Arc<str>>>> = OnceLock::new();

/// Get or create an interned string for a symbol (using FxHashMap for general use)
pub fn intern_symbol(symbol: &str) -> Arc<str> {
    let interner = STRING_INTERNER.get_or_init(|| std::sync::Mutex::new(FxHashMap::default()));
    let mut map = interner.lock().unwrap();
    
    if let Some(interned) = map.get(symbol) {
        Arc::clone(interned)
    } else {
        let interned: Arc<str> = Arc::from(symbol);
        map.insert(symbol.to_string(), Arc::clone(&interned));
        interned
    }
}

/// Get or create an interned string for a symbol using foldhash optimization
/// This is specifically optimized for short string keys like cryptocurrency symbols
/// Use this for high-frequency symbol operations in websocket and volume profile modules
pub fn intern_symbol_fast(symbol: &str) -> Arc<str> {
    let interner = SYMBOL_INTERNER.get_or_init(|| std::sync::Mutex::new(FoldHashMap::default()));
    let mut map = interner.lock().unwrap();
    
    if let Some(interned) = map.get(symbol) {
        Arc::clone(interned)
    } else {
        let interned: Arc<str> = Arc::from(symbol);
        map.insert(symbol.to_string(), Arc::clone(&interned));
        interned
    }
}

/// Type alias for foldhash HashMap optimized for symbol keys
pub type SymbolHashMap<V> = FoldHashMap<String, V>;

/// Create a new foldhash HashMap optimized for symbol lookups
pub fn new_symbol_hashmap<V>() -> SymbolHashMap<V> {
    FoldHashMap::default()
}

/// Get or create an interned string for an interval
pub fn intern_interval(interval: &str) -> Arc<str> {
    intern_symbol(interval) // Same mechanism for intervals
}

/// Create a database key using interned strings
pub fn create_db_key(symbol: &str, timeframe: u64) -> (Arc<str>, u64) {
    (intern_symbol(symbol), timeframe)
}

/// Convert timeframe to string efficiently
pub fn timeframe_to_string(timeframe: u64) -> &'static str {
    match timeframe {
        60 => "60",
        300 => "300", 
        900 => "900",
        1800 => "1800",
        3600 => "3600",
        14400 => "14400",
        86400 => "86400",
        _ => {
            // For non-standard timeframes, we can't avoid allocation
            // But we'll at least return a owned string consistently
            Box::leak(timeframe.to_string().into_boxed_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_symbol_creation() {
        let symbol = shared_symbol("BTCUSDT");
        assert_eq!(&*symbol, "BTCUSDT");
        
        // Test that cloning Arc doesn't clone the underlying data
        let symbol_clone = Arc::clone(&symbol);
        assert_eq!(symbol.as_ptr(), symbol_clone.as_ptr());
    }

    #[test]
    fn test_shared_interval_creation() {
        let interval = shared_interval("1m");
        assert_eq!(&*interval, "1m");
        
        let interval_clone = Arc::clone(&interval);
        assert_eq!(interval.as_ptr(), interval_clone.as_ptr());
    }

    #[test]
    fn test_memory_efficiency() {
        // Arc<str> should be more memory efficient than String for sharing
        let symbol_arc = shared_symbol("BTCUSDT");
        let symbol_string = "BTCUSDT".to_string();
        
        // Arc overhead vs String overhead comparison
        assert!(std::mem::size_of_val(&symbol_arc) <= std::mem::size_of::<usize>() * 2);
        assert!(std::mem::size_of_val(&symbol_string) >= std::mem::size_of::<usize>() * 3);
    }

    #[test]
    fn test_string_interning() {
        let symbol1 = intern_symbol("BTCUSDT");
        let symbol2 = intern_symbol("BTCUSDT");
        
        // Same symbol should return the same Arc (same pointer)
        assert_eq!(symbol1.as_ptr(), symbol2.as_ptr());
        assert_eq!(&*symbol1, "BTCUSDT");
    }

    #[test]
    fn test_different_symbols_interning() {
        let symbol1 = intern_symbol("BTCUSDT");
        let symbol2 = intern_symbol("ETHUSDT");
        
        // Different symbols should have different pointers
        assert_ne!(symbol1.as_ptr(), symbol2.as_ptr());
        assert_eq!(&*symbol1, "BTCUSDT");
        assert_eq!(&*symbol2, "ETHUSDT");
    }

    #[test]
    fn test_timeframe_to_string_optimization() {
        // Standard timeframes should return static strings
        assert_eq!(timeframe_to_string(60), "60");
        assert_eq!(timeframe_to_string(300), "300");
        assert_eq!(timeframe_to_string(3600), "3600");
        
        // Check that these are actually static (same pointer)
        let tf1 = timeframe_to_string(60);
        let tf2 = timeframe_to_string(60);
        assert_eq!(tf1.as_ptr(), tf2.as_ptr());
    }

    #[test]
    fn test_db_key_creation() {
        let key1 = create_db_key("BTCUSDT", 60);
        let key2 = create_db_key("BTCUSDT", 60);
        
        // Same symbol should be interned (same pointer)
        assert_eq!(key1.0.as_ptr(), key2.0.as_ptr());
        assert_eq!(key1.1, 60);
        assert_eq!(key2.1, 60);
    }
}