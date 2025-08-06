//! Simple CPU profiling tool to identify performance bottlenecks
//! 
//! This runs lightweight tests to measure CPU usage patterns and identify
//! the most resource-intensive operations in the application.

use std::time::{Duration, Instant};
use std::collections::HashMap;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    info!("🔍 Starting CPU bottleneck analysis");

    let mut results = HashMap::new();

    // Test 1: String operations (commonly expensive)
    info!("📊 Testing string operations...");
    let start = Instant::now();
    for _ in 0..100000 {
        let _s1 = "BTCUSDT".to_string();
        let _s2 = format!("{}_{}", "BTCUSDT", 60);
        let _s3 = _s1.clone();
    }
    let duration = start.elapsed();
    results.insert("string_operations", duration);
    info!("⏱️  String operations: {:?}", duration);

    // Test 2: Memory allocations
    info!("📊 Testing memory allocations...");
    let start = Instant::now();
    for _ in 0..50000 {
        let mut vec: Vec<f64> = Vec::with_capacity(100);
        for j in 0..100 {
            vec.push(j as f64);
        }
        let _sum: f64 = vec.iter().sum();
    }
    let duration = start.elapsed();
    results.insert("memory_allocations", duration);
    info!("⏱️  Memory allocations: {:?}", duration);

    // Test 3: Mathematical calculations
    info!("📊 Testing mathematical calculations...");
    let start = Instant::now();
    let mut result = 0.0;
    for i in 0..1000000 {
        result += (i as f64).sqrt() * (i as f64).ln();
    }
    let duration = start.elapsed();
    results.insert("math_calculations", duration);
    info!("⏱️  Math calculations: {:?} (result: {})", duration, result);

    // Test 4: JSON serialization/deserialization 
    info!("📊 Testing JSON operations...");
    let start = Instant::now();
    let test_data = serde_json::json!({
        "symbol": "BTCUSDT",
        "open": 50000.0,
        "high": 51000.0,
        "low": 49000.0,
        "close": 50500.0,
        "volume": 1000.0,
        "timestamp": 1672531200000i64
    });
    
    for _ in 0..10000 {
        let json_str = serde_json::to_string(&test_data).unwrap();
        let _parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    }
    let duration = start.elapsed();
    results.insert("json_operations", duration);
    info!("⏱️  JSON operations: {:?}", duration);

    // Test 5: HashMap operations
    info!("📊 Testing HashMap operations...");
    let start = Instant::now();
    let mut map: HashMap<String, f64> = HashMap::new();
    for i in 0..100000 {
        let key = format!("key_{}", i);
        map.insert(key.clone(), i as f64);
        let _value = map.get(&key);
    }
    let duration = start.elapsed();
    results.insert("hashmap_operations", duration);
    info!("⏱️  HashMap operations: {:?}", duration);

    // Test 6: Async operations overhead
    info!("📊 Testing async operations overhead...");
    let start = Instant::now();
    for _ in 0..10000 {
        async_task().await;
    }
    let duration = start.elapsed();
    results.insert("async_overhead", duration);
    info!("⏱️  Async overhead: {:?}", duration);

    // Analyze results
    info!("📈 PERFORMANCE ANALYSIS RESULTS");
    info!("════════════════════════════════");
    
    let mut sorted_results: Vec<_> = results.iter().collect();
    sorted_results.sort_by(|a, b| b.1.cmp(a.1));
    
    for (i, (operation, duration)) in sorted_results.iter().enumerate() {
        let ms = duration.as_millis();
        let status = match ms {
            0..=10 => "🟢 FAST",
            11..=50 => "🟡 MODERATE", 
            51..=100 => "🟠 SLOW",
            _ => "🔴 VERY SLOW"
        };
        
        info!("{}. {} - {}ms {}", i + 1, operation, ms, status);
    }

    // Generate recommendations
    info!("🔧 OPTIMIZATION RECOMMENDATIONS");
    info!("═══════════════════════════════");
    
    let slowest = sorted_results.first().unwrap();
    let fastest = sorted_results.last().unwrap();
    
    warn!("🎯 PRIMARY BOTTLENECK: {} ({:?})", slowest.0, slowest.1);
    info!("✅ MOST EFFICIENT: {} ({:?})", fastest.0, fastest.1);
    
    let slowest_ms = slowest.1.as_millis();
    if slowest_ms > 100 {
        warn!("⚠️  {} is consuming significant CPU time - high priority for optimization", slowest.0);
    }
    
    if slowest_ms > 50 {
        info!("💡 Consider optimizing {} operations", slowest.0);
    }

    // Calculate total CPU time
    let total_time: Duration = results.values().sum();
    info!("⏱️  Total CPU time used: {:?}", total_time);

    info!("✅ CPU bottleneck analysis complete");
    Ok(())
}

async fn async_task() {
    // Simulate lightweight async work
    tokio::task::yield_now().await;
}