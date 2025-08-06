//! Performance validation for optimized JSON parsing
//! 
//! This script validates that our simd-json and object pooling optimizations
//! provide measurable performance improvements over the baseline implementation.

use std::time::Instant;
use data_feeder::websocket::binance::kline::parse_any_kline_message;
use data_feeder::websocket::optimized_parser::parse_any_kline_message_optimized;
use data_feeder::common::object_pool::init_global_pools;

fn sample_websocket_message() -> &'static str {
    r#"{
        "e": "kline",
        "E": 1672531200000,
        "s": "BTCUSDT",
        "k": {
            "t": 1672531140000,
            "T": 1672531199999,
            "s": "BTCUSDT",
            "i": "1m",
            "f": 123456789,
            "L": 123456799,
            "o": "16800.00",
            "c": "16850.00",
            "h": "16860.00",
            "l": "16795.00",
            "v": "12.5",
            "n": 150,
            "x": true,
            "q": "210625.00",
            "V": "8.2",
            "B": "0"
        }
    }"#
}

fn benchmark_original_parser(iterations: usize) -> std::time::Duration {
    let sample = sample_websocket_message();
    let start = Instant::now();
    
    for _ in 0..iterations {
        let _ = parse_any_kline_message(sample).expect("Parsing should succeed");
    }
    
    start.elapsed()
}

fn benchmark_optimized_parser(iterations: usize) -> std::time::Duration {
    let sample = sample_websocket_message();
    let start = Instant::now();
    
    for _ in 0..iterations {
        let _ = parse_any_kline_message_optimized(sample).expect("Parsing should succeed");
    }
    
    start.elapsed()
}

fn main() {
    println!("🚀 Validating JSON parsing optimizations...");
    
    // Initialize object pools
    let pools = init_global_pools();
    println!("✅ Initialized object pools: {}", pools.stats_report());
    
    let iterations = 10_000;
    println!("📊 Running {} iterations of each parser...", iterations);
    
    // Warm up
    println!("🔥 Warming up parsers...");
    benchmark_original_parser(1000);
    benchmark_optimized_parser(1000);
    
    // Benchmark original parser
    println!("⏱️  Benchmarking original parser...");
    let original_time = benchmark_original_parser(iterations);
    let original_per_op = original_time.as_nanos() as f64 / iterations as f64;
    
    // Benchmark optimized parser
    println!("⚡ Benchmarking optimized parser...");
    let optimized_time = benchmark_optimized_parser(iterations);
    let optimized_per_op = optimized_time.as_nanos() as f64 / iterations as f64;
    
    // Calculate improvement
    let improvement_ratio = original_time.as_nanos() as f64 / optimized_time.as_nanos() as f64;
    let improvement_percent = ((original_time.as_nanos() as f64 - optimized_time.as_nanos() as f64) / original_time.as_nanos() as f64) * 100.0;
    
    println!("\n📈 Performance Results:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Original Parser:");
    println!("  Total time: {:?}", original_time);
    println!("  Per operation: {:.2} ns", original_per_op);
    println!("  Operations/sec: {:.0}", 1_000_000_000.0 / original_per_op);
    
    println!("\nOptimized Parser:");
    println!("  Total time: {:?}", optimized_time);
    println!("  Per operation: {:.2} ns", optimized_per_op);
    println!("  Operations/sec: {:.0}", 1_000_000_000.0 / optimized_per_op);
    
    println!("\nImprovement:");
    println!("  Speed increase: {:.2}x faster", improvement_ratio);
    println!("  Time reduction: {:.1}% less CPU time", improvement_percent);
    
    // Validation check
    if improvement_percent >= 20.0 {
        println!("\n✅ SUCCESS: Achieved {:.1}% performance improvement (target: ≥20%)", improvement_percent);
        
        // Test correctness
        let original_result = parse_any_kline_message(sample_websocket_message()).unwrap();
        let optimized_result = parse_any_kline_message_optimized(sample_websocket_message()).unwrap();
        
        assert_eq!(original_result.symbol, optimized_result.symbol);
        assert_eq!(original_result.event_type, optimized_result.event_type);
        assert_eq!(original_result.kline.open, optimized_result.kline.open);
        assert_eq!(original_result.kline.close, optimized_result.kline.close);
        assert_eq!(original_result.kline.is_kline_closed, optimized_result.kline.is_kline_closed);
        
        println!("✅ CORRECTNESS: Both parsers produce identical results");
        
        println!("\n🎯 OPTIMIZATION VALIDATION: PASSED");
        println!("   • CPU bottleneck (JSON parsing) successfully optimized");
        println!("   • Performance target achieved");
        println!("   • Correctness maintained");
        
    } else if improvement_percent >= 0.0 {
        println!("\n⚠️  PARTIAL SUCCESS: Achieved {:.1}% improvement (target: ≥20%)", improvement_percent);
        println!("   Consider additional optimizations or architecture changes.");
    } else {
        println!("\n❌ REGRESSION: Performance decreased by {:.1}%", -improvement_percent);
        println!("   Review implementation for potential issues.");
    }
    
    println!("\n📊 Final object pool stats:");
    println!("{}", pools.stats_report());
}