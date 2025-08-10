//! Test Performance Monitoring System
//! 
//! This program tests the comprehensive performance monitoring and bottleneck detection system.

use data_feeder::performance::{init_performance_monitor, PerformanceConfig};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info,data_feeder=debug")
        .init();

    info!("🧪 Testing Performance Monitoring System");
    
    // Initialize performance monitoring with test configuration
    let config = PerformanceConfig {
        collection_interval: Duration::from_millis(200), // Collect every 200ms for testing
        single_core_threshold: 50.0, // Lower threshold for testing
        history_size: 30, // Keep 6 seconds of history
        detailed_threading: true,
    };

    info!("🚀 Initializing performance monitor with test configuration");
    let monitor = init_performance_monitor(Some(config)).await?;

    info!("📊 Performance monitor started - collecting metrics...");
    
    // Wait for some initial data collection
    sleep(Duration::from_secs(2)).await;

    // Get initial snapshot
    if let Some(snapshot) = monitor.get_current_snapshot() {
        info!("📸 Current performance snapshot:");
        info!("   Overall CPU: {:.1}%", snapshot.overall_cpu_percent);
        info!("   Process CPU: {:.1}%", snapshot.process_cpu_percent);
        info!("   Memory: {:.1}MB", snapshot.memory_usage_mb);
        info!("   Threads: {}", snapshot.thread_count);
        info!("   Single-core bottleneck: {}", snapshot.single_core_bottleneck);
        info!("   Per-core usage: {:?}", 
              snapshot.per_core_usage.iter().map(|u| format!("{:.1}%", u)).collect::<Vec<_>>());
    }

    // Test CPU-intensive workload to trigger bottleneck detection
    info!("🔥 Starting CPU-intensive workload to test bottleneck detection...");
    
    // Create some CPU load
    let handles: Vec<_> = (0..2).map(|i| {
        tokio::spawn(async move {
            info!("🏃 Starting CPU load generator {}", i);
            let start = std::time::Instant::now();
            let mut counter = 0u64;
            
            while start.elapsed() < Duration::from_secs(5) {
                // Some CPU work
                for _ in 0..10_000 {
                    counter = counter.wrapping_add(1);
                    counter = counter.wrapping_mul(17);
                    counter = counter.wrapping_add(counter % 1000);
                }
                
                // Yield to allow other tasks
                if counter % 100_000 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            
            info!("🏁 CPU load generator {} finished (counter: {})", i, counter);
        })
    }).collect();

    // Monitor performance during load
    for i in 0..10 {
        sleep(Duration::from_millis(500)).await;
        
        if let Some(snapshot) = monitor.get_current_snapshot() {
            info!("📊 Sample {} - CPU: {:.1}%, Memory: {:.1}MB, Bottleneck: {}",
                  i + 1,
                  snapshot.process_cpu_percent,
                  snapshot.memory_usage_mb,
                  snapshot.single_core_bottleneck);
            
            if snapshot.single_core_bottleneck {
                warn!("🎯 Single-core bottleneck detected on core {} ({:.1}%)",
                      snapshot.bottleneck_core_id.unwrap_or(0),
                      snapshot.per_core_usage[snapshot.bottleneck_core_id.unwrap_or(0)]);
            }
        }
    }

    // Wait for workload to complete
    for handle in handles {
        let _ = handle.await;
    }

    // Generate performance report
    info!("📋 Generating comprehensive performance report...");
    let report = monitor.generate_performance_report();
    
    info!("📈 Performance Report Summary:");
    info!("   Performance Score: {:.1}/100", report.performance_score);
    info!("   Average System CPU: {:.1}%", report.avg_system_cpu_percent);
    info!("   Average Process CPU: {:.1}%", report.avg_process_cpu_percent);
    info!("   Single-core bottlenecks detected: {}", report.single_core_bottleneck_count);
    info!("   Critical bottlenecks: {}", report.critical_bottleneck_count);
    info!("   Total samples: {}", report.total_samples);
    
    if !report.per_core_averages.is_empty() {
        info!("   Per-core averages: {:?}", 
              report.per_core_averages.iter().map(|u| format!("{:.1}%", u)).collect::<Vec<_>>());
    }

    info!("🎯 Recommendations:");
    for (i, rec) in report.recommendations.iter().enumerate() {
        info!("   {}. {}", i + 1, rec);
    }

    if !report.recent_bottlenecks.is_empty() {
        info!("⚠️ Recent bottlenecks detected:");
        for (i, bottleneck) in report.recent_bottlenecks.iter().enumerate() {
            info!("   {}. {:?} (severity: {:?}) - {}", 
                  i + 1, bottleneck.bottleneck_type, bottleneck.severity, bottleneck.recommendation);
        }
    }

    // Test history retrieval
    let history = monitor.get_cpu_history(Some(10));
    info!("📚 Recent history ({} samples):", history.len());
    for (i, snapshot) in history.iter().enumerate() {
        info!("   Sample {}: CPU {:.1}%, bottleneck: {}", 
              i + 1, snapshot.process_cpu_percent, snapshot.single_core_bottleneck);
    }

    info!("✅ Performance monitoring test completed successfully!");
    info!("🛑 Stopping performance monitor...");
    monitor.stop_monitoring();
    
    // Give it a moment to stop cleanly
    sleep(Duration::from_millis(500)).await;
    
    info!("🏁 Test finished");
    Ok(())
}