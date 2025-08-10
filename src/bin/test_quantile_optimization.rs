//! Test Quantile Performance Optimization
//! 
//! This program validates that the QuantileTracker cache optimization significantly
//! reduces the 39ms bottleneck observed in indicator processing.

use data_feeder::historical::structs::FuturesOHLCVCandle;
use data_feeder::technical_analysis::structs::QuantileTracker;
use std::time::Instant;

fn create_realistic_candle(timestamp: i64, volume: f64, trades: u64) -> FuturesOHLCVCandle {
    FuturesOHLCVCandle {
        open_time: timestamp,
        close_time: timestamp + 59999,
        open: 50000.0,
        high: 50100.0,
        low: 49900.0,
        close: 50050.0,
        volume,
        number_of_trades: trades,
        taker_buy_base_asset_volume: volume * 0.6,
        closed: true,
    }
}

fn main() {
    println!("🧪 Testing QuantileTracker Performance Optimization");
    
    // Create tracker with 30-day window (realistic production scenario)
    let mut tracker = QuantileTracker::new(30);
    
    // Phase 1: Add initial data (simulate historical data - reduced for memory efficiency)
    println!("\n📈 Phase 1: Loading initial data (5 days of 1-minute candles = ~7,200 records)");
    let start_time = chrono::Utc::now().timestamp_millis();
    
    for i in 0..7200 {
        let timestamp = start_time + (i * 60000); // Every minute
        let volume = 1000.0 + (i as f64 % 500.0); // Varying volume
        let trades = 50 + (i % 100); // Varying trade counts
        
        let candle = create_realistic_candle(timestamp, volume, trades as u64);
        tracker.update(&candle);
        
        // Debug cache invalidations during loading
        if i > 0 && (i % 1000 == 0) {
            let (records, _, cache_updates, _, cache_valid) = tracker.get_performance_metrics();
            let threshold = (records / 20).max(100);
            println!("📊 At {} records: cache_updates={}, threshold={}, cache_valid={}", 
                     records, cache_updates, threshold, cache_valid);
        }
    }
    
    println!("✅ Loaded {} records", tracker.len());
    
    // Phase 2: Test FIRST quantile calculation (should be slow - cache miss)
    println!("\n🔥 Phase 2: First quantile calculation (cache miss - expect slow)");
    let start = Instant::now();
    let first_result = tracker.get_quantiles();
    let first_duration = start.elapsed();
    
    println!("⏱️  First call: {:?} (cache miss)", first_duration);
    assert!(first_result.is_some());
    
    // Phase 3: Test SECOND quantile calculation (should be fast - cache hit)
    println!("\n⚡ Phase 3: Second quantile calculation (cache hit - expect fast)");
    let start = Instant::now();
    let second_result = tracker.get_quantiles();
    let second_duration = start.elapsed();
    
    println!("⏱️  Second call: {:?} (cache hit)", second_duration);
    assert!(second_result.is_some());
    
    // Phase 4: Add some new data (not enough to invalidate cache)
    println!("\n📊 Phase 4: Add 50 new records (not enough to trigger cache invalidation)");
    let (records_before, _, cache_updates_before, _, cache_valid_before) = tracker.get_performance_metrics();
    let threshold_before = (records_before / 20).max(100);
    println!("📊 Before adding records: {} records, {} cache updates, threshold: {}, cache valid: {}", 
             records_before, cache_updates_before, threshold_before, cache_valid_before);
    
    for i in 0..50 {
        let timestamp = start_time + (7200 + i) * 60000;
        let candle = create_realistic_candle(timestamp, 1200.0, 75);
        tracker.update(&candle);
    }
    
    let (records_after, _, cache_updates_after, _, cache_valid_after) = tracker.get_performance_metrics();
    println!("📊 After adding 50 records: {} records, {} cache updates, cache valid: {}", 
             records_after, cache_updates_after, cache_valid_after);
    
    // Test quantile calculation after small update (should still be fast)
    let start = Instant::now();
    let _third_result = tracker.get_quantiles();
    let third_duration = start.elapsed();
    
    println!("⏱️  Third call after 50 updates: {:?} (should still use cache)", third_duration);
    
    // Phase 5: Add many new records to trigger cache invalidation
    println!("\n🔄 Phase 5: Add 500 new records (enough to trigger cache invalidation)");
    for i in 0..500 {
        let timestamp = start_time + (7250 + i) * 60000;
        let candle = create_realistic_candle(timestamp, 800.0 + (i as f64), 40 + (i % 30) as u64);
        tracker.update(&candle);
    }
    
    // Test quantile calculation after cache invalidation (should be slow again)
    let start = Instant::now();
    let _fourth_result = tracker.get_quantiles();
    let fourth_duration = start.elapsed();
    
    println!("⏱️  Fourth call after cache invalidation: {:?} (cache rebuilt)", fourth_duration);
    
    // Phase 6: Test repeated calls (should be fast again)
    println!("\n🏃 Phase 6: Test 100 repeated calls (all should use cache)");
    let mut total_duration = std::time::Duration::new(0, 0);
    
    for _ in 0..100 {
        let start = Instant::now();
        let _result = tracker.get_quantiles();
        total_duration += start.elapsed();
    }
    
    let avg_cached_duration = total_duration / 100;
    println!("⏱️  Average cached call: {:?}", avg_cached_duration);
    
    // Performance analysis
    println!("\n📊 Performance Analysis:");
    println!("  1️⃣  First call (cache miss):     {:?}", first_duration);
    println!("  2️⃣  Second call (cache hit):     {:?}", second_duration);
    println!("  3️⃣  Third call (cached):         {:?}", third_duration);
    println!("  4️⃣  Fourth call (rebuilt):       {:?}", fourth_duration);
    println!("  5️⃣  Average cached call:        {:?}", avg_cached_duration);
    
    let cache_speedup = first_duration.as_micros() as f64 / second_duration.as_micros() as f64;
    println!("\n🚀 Cache Performance:");
    println!("  📈 Cache speedup: {:.2}x faster", cache_speedup);
    println!("  🎯 Cache hit duration: {:?}", second_duration);
    println!("  ⚠️  Cache miss duration: {:?}", first_duration);
    
    // Check if optimization meets target
    if second_duration.as_millis() < 1 {
        println!("\n✅ SUCCESS: Cache hits are sub-1ms ({}µs) - target achieved!", 
                second_duration.as_micros());
    } else if second_duration.as_millis() < 10 {
        println!("\n⚠️  PARTIAL SUCCESS: Cache hits are {}ms - good but not sub-1ms", 
                second_duration.as_millis());
    } else {
        println!("\n❌ OPTIMIZATION NEEDED: Cache hits are {}ms - still too slow", 
                second_duration.as_millis());
    }
    
    // Get performance metrics
    let (records, expiry_updates, cache_updates, expiry_freq, cache_valid) = tracker.get_performance_metrics();
    println!("\n📈 Tracker Metrics:");
    println!("  📊 Records: {}", records);
    println!("  🔄 Updates since expiry check: {}", expiry_updates);
    println!("  💰 Updates since cache invalidation: {}", cache_updates);
    println!("  ⚙️  Expiry check frequency: {}", expiry_freq);
    println!("  ✅ Cache valid: {}", cache_valid);
    
    println!("\n🏁 Quantile optimization test completed!");
}