//! Output generation baseline performance profiler for Story 4.4
//! Identifies the specific 30ms bottlenecks mentioned in the story

use std::time::{Duration, Instant};
use chrono::NaiveDate;
use rust_decimal_macros::dec;

use data_feeder::volume_profile::{
    calculator::DailyVolumeProfile,
    structs::{VolumeProfileConfig, PriceIncrementMode, VolumeDistributionMode, ValueAreaCalculationMode, VolumeProfileCalculationMode, UpdateFrequency}
};
use data_feeder::historical::structs::FuturesOHLCVCandle;

fn main() {
    println!("=== Story 4.4: Output Generation Baseline Performance Profile ===");
    println!("🎯 Target: Identify 30ms bottlenecks and establish <1ms optimization baseline");
    
    let start_total = Instant::now();
    
    // Create realistic volume profile with trading data that would exhibit the 30ms bottleneck
    let profile = create_complex_volume_profile();
    let creation_duration = start_total.elapsed();
    println!("📊 Complex volume profile creation: {:?}", creation_duration);
    
    // Test current JSON serialization approach (the bottleneck)
    let mut profile_mut = profile;
    let profile_data = profile_mut.get_profile_data();
    
    println!("\n=== Current Serialization Performance (Bottleneck Analysis) ===");
    
    // Single operation timing
    let start_single = Instant::now();
    let json_output = serde_json::to_string(&profile_data)
        .expect("Failed to serialize to JSON");
    let single_duration = start_single.elapsed();
    
    println!("🔍 Single JSON serialization: {:?}", single_duration);
    println!("📏 JSON output size: {} bytes ({} KB)", json_output.len(), json_output.len() / 1024);
    
    // Batch operations to simulate realistic workload (the 3.41s pipeline issue)
    let iterations = 100;
    let mut serialization_times = Vec::with_capacity(iterations);
    
    println!("\n=== Simulating Pipeline Load ({} operations) ===", iterations);
    let start_batch = Instant::now();
    
    for i in 0..iterations {
        let start_iter = Instant::now();
        let _serialized = serde_json::to_string(&profile_data)
            .expect("Failed to serialize");
        let iter_duration = start_iter.elapsed();
        serialization_times.push(iter_duration);
        
        if i % 25 == 0 {
            println!("📈 Operation {} - Serialization time: {:?}", i, iter_duration);
        }
    }
    
    let batch_duration = start_batch.elapsed();
    
    // Statistical analysis
    serialization_times.sort();
    let min_time = serialization_times[0];
    let max_time = serialization_times[iterations - 1];
    let median_time = serialization_times[iterations / 2];
    let avg_time: Duration = serialization_times.iter().sum::<Duration>() / iterations as u32;
    let p95_time = serialization_times[(iterations as f64 * 0.95) as usize];
    
    println!("\n=== Performance Analysis Results ===");
    println!("⏱️  Average serialization: {:?}", avg_time);
    println!("📊 Median serialization: {:?}", median_time);
    println!("⚡ Fastest serialization: {:?}", min_time);
    println!("🐌 Slowest serialization: {:?}", max_time);
    println!("📈 95th percentile: {:?}", p95_time);
    println!("📊 Total pipeline time: {:?}", batch_duration);
    
    // Calculate bottleneck metrics relative to Story 4.4 targets
    let target_1ms = Duration::from_millis(1);
    let current_30ms = Duration::from_millis(30); // Story bottleneck
    
    println!("\n=== Story 4.4 Bottleneck Assessment ===");
    
    if avg_time >= current_30ms {
        println!("🚨 CONFIRMED: 30ms+ bottleneck identified!");
        println!("💥 Bottleneck factor: {:.1}x slower than story target", 
                 avg_time.as_millis() as f64 / current_30ms.as_millis() as f64);
    } else {
        println!("⚠️  Bottleneck: {:.1}ms (less than expected 30ms)", avg_time.as_millis());
    }
    
    println!("❌ Performance gap vs 1ms target: {:.1}x slower", 
             avg_time.as_millis() as f64);
    
    // Pipeline impact calculation
    let pipeline_projected_time = avg_time * iterations as u32;
    let target_pipeline_time = target_1ms * iterations as u32; // <0.114s for 100 ops
    
    println!("\n=== Pipeline Impact Analysis ===");
    println!("📊 Current pipeline time (100 ops): {:?}", pipeline_projected_time);
    println!("🎯 Target pipeline time (100 ops): {:?}", target_pipeline_time);
    println!("📈 Required improvement: {:.1}x faster", 
             pipeline_projected_time.as_millis() as f64 / target_pipeline_time.as_millis() as f64);
    
    // Memory analysis
    let memory_per_op = json_output.len();
    let total_memory = memory_per_op * iterations;
    println!("💾 Memory usage per operation: {} KB", memory_per_op / 1024);
    println!("💾 Total memory pressure: {} MB", total_memory / 1024 / 1024);
    
    // Component breakdown analysis
    println!("\n=== Detailed Component Analysis ===");
    analyze_serialization_components(&profile_data);
    
    // Optimization recommendations
    println!("\n=== Story 4.4 Optimization Path ===");
    println!("🔧 PRIMARY: Implement sonic-rs zero-copy serialization");
    println!("🔧 SECONDARY: Add rkyv for technical analysis results");
    println!("🔧 TERTIARY: Implement pre-computed output caching");
    println!("🔧 PERFORMANCE: Enable async batching for aggregation");
    
    println!("\n=== Baseline Metrics (for future comparison) ===");
    println!("BASELINE_AVG_SERIALIZATION_MS={}", avg_time.as_millis());
    println!("BASELINE_P95_SERIALIZATION_MS={}", p95_time.as_millis());
    println!("BASELINE_OUTPUT_SIZE_BYTES={}", json_output.len());
    println!("BASELINE_MEMORY_PRESSURE_MB={}", total_memory / 1024 / 1024);
    println!("BASELINE_PIPELINE_TIME_MS={}", pipeline_projected_time.as_millis());
    
    if avg_time > target_1ms {
        println!("\n✅ BOTTLENECK CONFIRMED: Ready to implement Story 4.4 optimizations");
    } else {
        println!("\n⚠️  Consider testing with larger datasets to reproduce 30ms bottleneck");
    }
}

fn create_complex_volume_profile() -> DailyVolumeProfile {
    let config = VolumeProfileConfig {
        enabled: true,
        price_increment_mode: PriceIncrementMode::Adaptive,
        fixed_price_increment: dec!(0.01),
        min_price_increment: dec!(0.01), 
        max_price_increment: dec!(10.0),
        target_price_levels: 500, // Increased complexity
        update_frequency: UpdateFrequency::EveryCandle,
        batch_size: 100,
        volume_distribution_mode: VolumeDistributionMode::HighLowWeighted,
        value_area_calculation_mode: ValueAreaCalculationMode::Traditional,
        value_area_percentage: dec!(70),
        calculation_mode: VolumeProfileCalculationMode::Volume,
        asset_overrides: Default::default(),
        historical_days: 60,
    };
    
    let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let mut profile = DailyVolumeProfile::new("BTCUSDT".to_string(), date, &config);
    
    // Add complex trading data that would create the 30ms serialization bottleneck
    for i in 0..2000 { // More candles for complexity
        let base_price = 45000.0;
        let time_offset = i * 30 * 1000; // 30 second intervals
        let price_variation = ((i as f64 * 0.02).sin() * 200.0) + ((i as f64 * 0.005).cos() * 100.0);
        
        let candle = FuturesOHLCVCandle {
            open_time: 1736985600000 + time_offset,
            close_time: 1736985629999 + time_offset,
            open: base_price + price_variation,
            high: base_price + price_variation + (50.0 + (i % 20) as f64),
            low: base_price + price_variation - (50.0 + (i % 15) as f64), 
            close: base_price + price_variation + (25.0 - (i % 10) as f64),
            volume: 150.0 + (i as f64 * 0.8) + ((i as f64 * 0.1).sin() * 50.0),
            taker_buy_base_asset_volume: 75.0 + (i as f64 * 0.4),
            number_of_trades: 75 + (i % 150) as u64,
            closed: true,
        };
        
        profile.add_candle(&candle);
    }
    
    profile
}

fn analyze_serialization_components(profile_data: &data_feeder::volume_profile::structs::VolumeProfileData) {
    // Test individual component serialization times
    let start_levels = Instant::now();
    let levels_json = serde_json::to_string(&profile_data.price_levels).unwrap();
    let levels_time = start_levels.elapsed();
    
    let start_metadata = Instant::now();
    let metadata_json = format!("{{\"total_volume\": {}, \"vwap\": {}}}", profile_data.total_volume, profile_data.vwap);
    let metadata_time = start_metadata.elapsed();
    
    let start_value_area = Instant::now(); 
    let value_area_json = serde_json::to_string(&profile_data.value_area).unwrap();
    let value_area_time = start_value_area.elapsed();
    
    println!("📊 Price levels ({} levels): {:?} ({} bytes)", 
             profile_data.price_levels.len(), levels_time, levels_json.len());
    println!("📊 Debug metadata: {:?} ({} bytes)", metadata_time, metadata_json.len());
    println!("📊 Value area: {:?} ({} bytes)", value_area_time, value_area_json.len());
    
    // Identify the primary bottleneck component
    let total_component_time = levels_time + metadata_time + value_area_time;
    let levels_percent = (levels_time.as_nanos() as f64 / total_component_time.as_nanos() as f64) * 100.0;
    
    if levels_percent > 60.0 {
        println!("🚨 PRIMARY BOTTLENECK: Price levels serialization ({:.1}%)", levels_percent);
        println!("💡 OPTIMIZATION TARGET: Zero-copy serialization for price level arrays");
    }
}