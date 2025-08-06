//! CPU Profiling Workload Runner
//! 
//! This binary runs representative workloads to identify CPU bottlenecks
//! and gather baseline performance metrics.

use data_feeder::profiling::{init_profiler, get_profiler, PerformanceReport};
use data_feeder::metrics::init_metrics;
use data_feeder::historical::structs::FuturesOHLCVCandle;
use data_feeder::technical_analysis::{
    actors::indicator::SymbolIndicatorState,
    structs::TechnicalAnalysisConfig,
    fast_serialization::SerializationBuffer,
};
use data_feeder::lmdb::LmdbActor;
use data_feeder::profile;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::time::sleep;
use tracing::{info, warn};
// Removed redundant serde_json import - using qualified calls where needed

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info,data_feeder=debug")
        .init();

    info!("🔍 Starting CPU profiling workload analysis");

    // Initialize profiler and metrics
    let _profiler = init_profiler(true);
    let _metrics = init_metrics()?;

    // Run comprehensive profiling workloads
    let mut results = HashMap::new();

    info!("📊 Running workload 1: String operations and memory allocations");
    results.insert("string_operations", profile_string_operations().await);

    info!("📊 Running workload 2: Data processing and serialization");
    results.insert("data_processing", profile_data_processing().await);

    info!("📊 Running workload 3: Technical analysis calculations");
    results.insert("technical_analysis", profile_technical_analysis().await);

    info!("📊 Running workload 4: Database operations");
    results.insert("database_operations", profile_database_operations().await);

    info!("📊 Running workload 5: WebSocket simulation");
    results.insert("websocket_simulation", profile_websocket_simulation().await);

    // Generate comprehensive performance report
    if let Some(profiler) = get_profiler() {
        let report = profiler.generate_report();
        
        // Save detailed report to file
        let report_json = serde_json::to_string_pretty(&report)?;
        std::fs::write("performance_baseline_report.json", report_json)?;
        
        // Print summary
        print_performance_summary(&report);
        
        // Print component-specific results
        for (workload, duration) in results {
            info!("⏱️  {} completed in: {:?}", workload, duration);
        }
    }

    info!("✅ CPU profiling analysis complete - results saved to performance_baseline_report.json");
    Ok(())
}

async fn profile_string_operations() -> Duration {
    let start = Instant::now();
    
    profile!("string_operations", "string_interning", {
        use data_feeder::common::shared_data::intern_symbol;
        
        // Simulate heavy string operations
        for _ in 0..10000 {
            let _symbol = intern_symbol("BTCUSDT");
            let _symbol2 = intern_symbol("ETHUSDT");
            let _symbol3 = intern_symbol("ADAUSDT");
        }
    });

    profile!("string_operations", "string_creation", {
        // Compare with regular string creation
        for _ in 0..10000 {
            let _s1 = "BTCUSDT".to_string();
            let _s2 = "ETHUSDT".to_string();
            let _s3 = "ADAUSDT".to_string();
        }
    });

    profile!("string_operations", "timeframe_conversion", {
        use data_feeder::common::shared_data::timeframe_to_string;
        
        for _ in 0..5000 {
            for tf in [60, 300, 900, 3600, 14400, 86400] {
                let _s = timeframe_to_string(tf);
            }
        }
    });

    start.elapsed()
}

async fn profile_data_processing() -> Duration {
    let start = Instant::now();
    
    // Create realistic test candles
    let candles: Vec<FuturesOHLCVCandle> = (0..1000).map(|i| {
        FuturesOHLCVCandle {
            open_time: i * 60_000,
            close_time: (i + 1) * 60_000 - 1,
            open: 50000.0 + (i as f64 * 0.1),
            high: 50100.0 + (i as f64 * 0.1),
            low: 49900.0 + (i as f64 * 0.1),
            close: 50000.0 + (i as f64 * 0.1),
            volume: 1000.0 + (i as f64 * 0.5),
            number_of_trades: (100 + i) as u64,
            taker_buy_base_asset_volume: 500.0 + (i as f64 * 0.25),
            closed: true,
        }
    }).collect();

    profile!("data_processing", "candle_cloning", {
        for candle in &candles {
            let _clone = candle.clone();
        }
    });

    profile!("data_processing", "arc_sharing", {
        use data_feeder::common::shared_data::shared_candle;
        use std::sync::Arc;
        
        let shared_candles: Vec<_> = candles.iter()
            .map(|c| shared_candle(c.clone()))
            .collect();
        
        // Simulate sharing
        for shared in &shared_candles {
            let _clone = Arc::clone(shared);
        }
    });

    profile!("data_processing", "serialization", {
        let _buffer = SerializationBuffer::new(); // Reserved for future serialization benchmarks
        for candle in &candles {
            let _serialized = serde_json::to_string(candle).unwrap();
        }
    });

    start.elapsed()
}

async fn profile_technical_analysis() -> Duration {
    let start = Instant::now();
    
    let config = TechnicalAnalysisConfig {
        symbols: vec!["BTCUSDT".to_string()],
        min_history_days: 30,
        ema_periods: vec![21, 89],
        timeframes: vec![60, 300, 900, 3600, 14400],
        volume_lookback_days: 30,
    };

    profile!("technical_analysis", "indicator_state_creation", {
        let _state = SymbolIndicatorState::new(&config);
    });

    profile!("technical_analysis", "candle_processing", {
        let mut state = SymbolIndicatorState::new(&config);
        
        // Process many candles to simulate real workload
        for i in 0..1000 {
            let candle = FuturesOHLCVCandle {
                open_time: i * 60_000,
                close_time: (i + 1) * 60_000 - 1,
                open: 50000.0 + (i as f64 * 0.1),
                high: 50100.0 + (i as f64 * 0.1),
                low: 49900.0 + (i as f64 * 0.1),
                close: 50000.0 + (i as f64 * 0.1),
                volume: 1000.0 + (i as f64 * 0.5),
                number_of_trades: (100 + i) as u64,
                taker_buy_base_asset_volume: 500.0 + (i as f64 * 0.25),
                closed: true,
            };
            
            state.process_candle(60, &candle);
        }
    });

    profile!("technical_analysis", "output_generation", {
        let mut state = SymbolIndicatorState::new(&config);
        
        // Initialize with some data
        for i in 0..100 {
            let candle = FuturesOHLCVCandle {
                open_time: i * 60_000,
                close_time: (i + 1) * 60_000 - 1,
                open: 50000.0,
                high: 50100.0,
                low: 49900.0,
                close: 50000.0,
                volume: 1000.0,
                number_of_trades: 100,
                taker_buy_base_asset_volume: 500.0,
                closed: true,
            };
            state.process_candle(60, &candle);
        }
        
        // Generate output many times
        for _ in 0..1000 {
            let _output = state.generate_output("BTCUSDT");
        }
    });

    profile!("technical_analysis", "simd_operations", {
        use data_feeder::technical_analysis::simd_math::vectorized_ema_batch_update;
        
        let price = 50000.0;
        let alphas = vec![0.1, 0.11, 0.12, 0.13];
        let betas = vec![0.9, 0.89, 0.88, 0.87];
        let mut emas = vec![49000.0, 49100.0, 49200.0, 49300.0];
        
        for _ in 0..10000 {
            vectorized_ema_batch_update(price, &alphas, &betas, &mut emas);
        }
    });

    start.elapsed()
}

async fn profile_database_operations() -> Duration {
    let start = Instant::now();
    
    profile!("database_operations", "lmdb_actor_creation", {
        // Test LMDB actor creation overhead
        let storage_path = std::path::PathBuf::from("lmdb_data");
        let _actor = LmdbActor::new(&storage_path, 75);
    });

    profile!("database_operations", "key_generation", {
        use data_feeder::common::shared_data::intern_symbol;
        
        // Simulate database key creation patterns
        for _ in 0..10000 {
            let _key1 = (intern_symbol("BTCUSDT"), 60u64);
            let _key2 = (intern_symbol("ETHUSDT"), 300u64);
            let _key3 = (intern_symbol("ADAUSDT"), 900u64);
        }
    });

    start.elapsed()
}

async fn profile_websocket_simulation() -> Duration {
    let start = Instant::now();
    
    profile!("websocket_simulation", "message_parsing", {
        // Simulate parsing WebSocket messages
        let sample_kline_data = r#"{
            "e": "kline",
            "E": 1672531200000,
            "s": "BTCUSDT",
            "k": {
                "t": 1672531200000,
                "T": 1672531259999,
                "s": "BTCUSDT",
                "i": "1m",
                "f": 100,
                "L": 200,
                "o": "50000.00",
                "c": "50010.00",
                "h": "50020.00",
                "l": "49990.00",
                "v": "1000.00",
                "n": 100,
                "x": true,
                "q": "50005000.00",
                "V": "500.00",
                "Q": "25002500.00"
            }
        }"#;
        
        for _ in 0..1000 {
            let _parsed: serde_json::Value = serde_json::from_str(sample_kline_data).unwrap();
        }
    });

    profile!("websocket_simulation", "message_processing", {
        // Simulate processing parsed messages into candles
        for i in 0..1000 {
            let _candle = FuturesOHLCVCandle {
                open_time: i * 60_000,
                close_time: (i + 1) * 60_000 - 1,
                open: 50000.0,
                high: 50010.0,
                low: 49990.0,
                close: 50005.0,
                volume: 1000.0,
                number_of_trades: 100,
                taker_buy_base_asset_volume: 500.0,
                closed: true,
            };
        }
    });

    // Simulate async operations
    profile!("websocket_simulation", "async_operations", {
        // This will be measured including the sleep time
        sleep(Duration::from_millis(10)).await;
    });

    start.elapsed()
}

fn print_performance_summary(report: &PerformanceReport) {
    info!("📊 PERFORMANCE BASELINE REPORT");
    info!("════════════════════════════════");
    info!("📈 Total Components: {}", report.total_components);
    info!("🔢 Total Operations: {}", report.total_operations);
    info!("⚡ Average CPU Usage: {:.2}%", report.avg_cpu_percent);
    
    info!("🔥 TOP CPU CONSUMERS:");
    for (i, stat) in report.top_cpu_consumers.iter().enumerate() {
        info!("  {}. {}::{} - {:.2}% CPU, {} calls, {:.2}ms avg", 
              i + 1,
              stat.component,
              stat.operation,
              stat.avg_cpu_percent,
              stat.total_calls,
              stat.avg_duration_nanos as f64 / 1_000_000.0
        );
    }
    
    info!("🐌 SLOWEST OPERATIONS:");
    for (i, stat) in report.slowest_operations.iter().enumerate() {
        info!("  {}. {}::{} - {:.2}ms avg, {} calls, {:.2}% CPU", 
              i + 1,
              stat.component,
              stat.operation,
              stat.avg_duration_nanos as f64 / 1_000_000.0,
              stat.total_calls,
              stat.avg_cpu_percent
        );
    }

    // Calculate some performance insights
    let total_time_ms: f64 = report.component_stats.values()
        .map(|s| s.total_duration_nanos as f64 / 1_000_000.0)
        .sum();
    
    info!("⏱️  Total processing time: {:.2}ms", total_time_ms);
    
    // Identify potential optimization targets
    let high_cpu_ops: Vec<_> = report.component_stats.values()
        .filter(|s| s.avg_cpu_percent > 5.0)
        .collect();
    
    let slow_ops: Vec<_> = report.component_stats.values()
        .filter(|s| s.avg_duration_nanos > 1_000_000) // > 1ms
        .collect();
    
    if !high_cpu_ops.is_empty() {
        warn!("⚠️  {} operations consuming >5% CPU - optimization candidates", high_cpu_ops.len());
    }
    
    if !slow_ops.is_empty() {
        warn!("⚠️  {} operations taking >1ms - potential bottlenecks", slow_ops.len());
    }
}