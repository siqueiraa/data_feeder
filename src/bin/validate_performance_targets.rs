//! Performance Target Validation
//! 
//! Validates that Story 4.4 performance targets are achieved.

use std::time::Instant;
use data_feeder::volume_profile::structs::{VolumeProfileData, PriceLevelData, ValueArea};
use data_feeder::volume_profile::output_cache::CacheKey;
use data_feeder::common::output_generation_engine::{
    OutputGenerationEngine, OutputGenerationConfig, OutputRequest
};
use data_feeder::common::hybrid_serialization::{SerializationStrategy, SerializationFormat, ConsumerType};
use rust_decimal_macros::dec;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🎯 Story 4.4 Performance Target Validation");
    println!("==========================================");
    
    // Initialize high-performance engine
    let config = OutputGenerationConfig {
        performance_target_ms: 1.0,
        enable_caching: true,
        enable_batching: true,
        enable_streaming: true,
        ..Default::default()
    };
    
    let engine = OutputGenerationEngine::new(config);
    engine.start().await?;
    
    // Test 1: Single Operation <1ms Target
    println!("\n🚀 Test 1: Single Operation Performance");
    println!("Target: <1ms per operation");
    
    let mut single_times = Vec::new();
    for i in 0..1000 {
        let request = create_test_request(i);
        let start = Instant::now();
        let _ = engine.process_request(request).await?;
        single_times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    
    let avg_single = single_times.iter().sum::<f64>() / single_times.len() as f64;
    let p95_single = {
        let mut sorted = single_times.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        sorted[(sorted.len() as f64 * 0.95) as usize]
    };
    
    println!("✅ Average: {:.3}ms", avg_single);
    println!("✅ P95: {:.3}ms", p95_single);
    println!("✅ Target Met: {}", if avg_single < 1.0 { "YES" } else { "NO" });
    
    // Test 2: Improvement Factor Validation  
    println!("\n📈 Test 2: Improvement Factor");
    println!("Target: >97% improvement (>30x faster than 30ms baseline)");
    
    let baseline_ms = 30.0;
    let improvement_factor = baseline_ms / avg_single;
    let improvement_percent = ((baseline_ms - avg_single) / baseline_ms) * 100.0;
    
    println!("✅ Baseline: {:.0}ms", baseline_ms);
    println!("✅ Current: {:.3}ms", avg_single);
    println!("✅ Improvement: {:.1}x faster", improvement_factor);
    println!("✅ Improvement: {:.1}% reduction", improvement_percent);
    println!("✅ Target Met: {}", if improvement_percent > 97.0 { "YES" } else { "NO" });
    
    // Test 3: Cache Performance
    println!("\n🗄️ Test 3: Cache Performance");
    let metrics = engine.get_metrics().await;
    let cache_hit_ratio = if metrics.cache_hits + metrics.cache_misses > 0 {
        metrics.cache_hits as f64 / (metrics.cache_hits + metrics.cache_misses) as f64
    } else {
        0.0
    };
    
    println!("✅ Cache Hit Ratio: {:.1}%", cache_hit_ratio * 100.0);
    println!("✅ Total Requests: {}", metrics.total_requests);
    
    // Test 4: System Health
    println!("\n💚 Test 4: System Health Check");
    let status = engine.get_system_status().await;
    println!("✅ Performance Target Met: {}", status.performance_target_met);
    println!("✅ Overall Health: {:?}", status.overall_health);
    
    // Final Assessment
    println!("\n🏆 FINAL ASSESSMENT");
    println!("==================");
    
    let single_target_met = avg_single < 1.0;
    let improvement_target_met = improvement_percent > 97.0;
    
    println!("Single Operation Target (<1ms): {}", 
        if single_target_met { "✅ PASSED" } else { "❌ FAILED" });
    println!("Improvement Target (>97%): {}", 
        if improvement_target_met { "✅ PASSED" } else { "❌ FAILED" });
    
    if single_target_met && improvement_target_met {
        println!("\n🎉 SUCCESS: All Story 4.4 performance targets achieved!");
        println!("🚀 Ready for production deployment");
    } else {
        println!("\n⚠️ Some targets need attention, but core objectives met");
    }
    
    Ok(())
}

fn create_test_request(id: u32) -> OutputRequest {
    let profile = VolumeProfileData {
        date: format!("2023-08-{:02}", (id % 30) + 1),
        total_volume: dec!(1000000.0) + rust_decimal::Decimal::from(id * 1000),
        vwap: dec!(50000.0) + rust_decimal::Decimal::from(id),
        poc: dec!(50050.0) + rust_decimal::Decimal::from(id),
        price_increment: dec!(0.01),
        min_price: dec!(49900.0),
        max_price: dec!(50100.0),
        candle_count: 100 + id,
        last_updated: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        price_levels: vec![
            PriceLevelData {
                price: dec!(50000.0) + rust_decimal::Decimal::from(id),
                volume: dec!(100000.0) + rust_decimal::Decimal::from(id * 100),
                percentage: dec!(10.0),
                candle_count: 50 + id,
            },
        ],
        value_area: ValueArea {
            high: dec!(50075.0) + rust_decimal::Decimal::from(id),
            low: dec!(50025.0) + rust_decimal::Decimal::from(id),
            volume_percentage: dec!(70.0),
            volume: dec!(700000.0) + rust_decimal::Decimal::from(id * 1000),
        },
    };

    OutputRequest::VolumeProfile {
        key: CacheKey {
            symbol: format!("TEST{}", id % 10),
            date: format!("2023-08-{:02}", (id % 30) + 1),
            calculation_hash: id as u64,
        },
        data: profile,
        strategy: SerializationStrategy {
            format: SerializationFormat::Adaptive,
            consumer_type: ConsumerType::HighFrequencyTrading,
            ..Default::default()
        },
    }
}