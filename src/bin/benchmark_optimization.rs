//! Performance benchmark runner for optimization validation
//! 
//! Executes comprehensive benchmarks to validate performance improvements
//! from the multi-layered optimization implementation.

use std::env;

// Re-export the benchmark module from tests
mod benchmark_suite {
    include!("../../tests/performance/optimization_benchmark.rs");
}

use benchmark_suite::{PerformanceBenchmark, BenchmarkConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for performance monitoring
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("🎯 Data Feeder Optimization Performance Benchmark");
    println!("================================================");
    
    // Parse command line arguments for benchmark configuration
    let args: Vec<String> = env::args().collect();
    let config = parse_benchmark_config(&args);
    
    println!("📊 Benchmark Configuration:");
    println!("   Operations per test: {}", config.operations_per_test);
    println!("   Concurrent workers: {}", config.concurrent_workers);
    println!("   Test iterations: {}", config.test_iterations);
    println!("   Warmup iterations: {}", config.warmup_iterations);
    
    // Create and run benchmark suite
    let mut benchmark = PerformanceBenchmark::new(config);
    
    println!("\n🚀 Starting benchmark execution...");
    let start_time = std::time::Instant::now();
    
    match benchmark.run_all_benchmarks().await {
        Ok(()) => {
            let total_duration = start_time.elapsed();
            println!("\n✅ Benchmark suite completed successfully!");
            println!("📈 Total benchmark duration: {:?}", total_duration);
            
            // Validate performance targets
            validate_performance_targets(&benchmark);
        }
        Err(e) => {
            eprintln!("❌ Benchmark execution failed: {}", e);
            std::process::exit(1);
        }
    }
    
    Ok(())
}

fn parse_benchmark_config(args: &[String]) -> BenchmarkConfig {
    let mut config = BenchmarkConfig::default();
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--operations" => {
                if i + 1 < args.len() {
                    config.operations_per_test = args[i + 1].parse().unwrap_or(config.operations_per_test);
                    i += 1;
                }
            }
            "--workers" => {
                if i + 1 < args.len() {
                    config.concurrent_workers = args[i + 1].parse().unwrap_or(config.concurrent_workers);
                    i += 1;
                }
            }
            "--iterations" => {
                if i + 1 < args.len() {
                    config.test_iterations = args[i + 1].parse().unwrap_or(config.test_iterations);
                    i += 1;
                }
            }
            "--warmup" => {
                if i + 1 < args.len() {
                    config.warmup_iterations = args[i + 1].parse().unwrap_or(config.warmup_iterations);
                    i += 1;
                }
            }
            "--quick" => {
                // Quick test configuration
                config.operations_per_test = 2000;
                config.concurrent_workers = 2;
                config.test_iterations = 2;
                config.warmup_iterations = 1;
            }
            "--intensive" => {
                // Intensive test configuration
                config.operations_per_test = 50000;
                config.concurrent_workers = 8;
                config.test_iterations = 10;
                config.warmup_iterations = 3;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    
    config
}

fn validate_performance_targets(benchmark: &PerformanceBenchmark) {
    println!("\n🎯 PERFORMANCE TARGET VALIDATION");
    println!("================================");
    
    let mut targets_met = 0;
    let mut total_targets = 0;
    
    // Find baseline and optimized results
    let baseline_storage = benchmark.results.iter()
        .find(|r| r.test_name.contains("Baseline Storage"));
    let optimized_storage = benchmark.results.iter()
        .find(|r| r.test_name.contains("Optimized Storage"));
    
    let baseline_concurrency = benchmark.results.iter()
        .find(|r| r.test_name.contains("Baseline Concurrency"));
    let optimized_concurrency = benchmark.results.iter()
        .find(|r| r.test_name.contains("Optimized Concurrency"));
    
    let integrated = benchmark.results.iter()
        .find(|r| r.test_name.contains("Integrated"));
    
    // Validate storage optimization target (15-25%)
    if let (Some(baseline), Some(optimized)) = (baseline_storage, optimized_storage) {
        total_targets += 1;
        let improvement = optimized.performance_improvement(baseline);
        if improvement >= 15.0 {
            targets_met += 1;
            println!("✅ Storage Optimization: {:.1}% (Target: 15-25%)", improvement);
        } else {
            println!("❌ Storage Optimization: {:.1}% (Target: 15-25%) - BELOW TARGET", improvement);
        }
    }
    
    // Validate concurrency optimization target (15-25%)
    if let (Some(baseline), Some(optimized)) = (baseline_concurrency, optimized_concurrency) {
        total_targets += 1;
        let improvement = optimized.performance_improvement(baseline);
        if improvement >= 15.0 {
            targets_met += 1;
            println!("✅ Concurrency Optimization: {:.1}% (Target: 15-25%)", improvement);
        } else {
            println!("❌ Concurrency Optimization: {:.1}% (Target: 15-25%) - BELOW TARGET", improvement);
        }
    }
    
    // Validate integrated performance target (20%+)
    if let Some(integrated_result) = integrated {
        total_targets += 1;
        
        let baseline_avg_ops = match (baseline_storage, baseline_concurrency) {
            (Some(s), Some(c)) => (s.operations_per_second + c.operations_per_second) / 2.0,
            _ => 0.0,
        };
        
        if baseline_avg_ops > 0.0 {
            let integrated_improvement = ((integrated_result.operations_per_second - baseline_avg_ops) / baseline_avg_ops) * 100.0;
            if integrated_improvement >= 20.0 {
                targets_met += 1;
                println!("✅ Integrated Performance: {:.1}% (Target: 20%+)", integrated_improvement);
            } else {
                println!("❌ Integrated Performance: {:.1}% (Target: 20%+) - BELOW TARGET", integrated_improvement);
            }
        }
    }
    
    // Resource efficiency validation
    if let Some(integrated_result) = integrated {
        total_targets += 1;
        let efficiency_percent = integrated_result.resource_efficiency * 100.0;
        if efficiency_percent >= 80.0 {
            targets_met += 1;
            println!("✅ Resource Efficiency: {:.1}% (Target: 80%+)", efficiency_percent);
        } else {
            println!("❌ Resource Efficiency: {:.1}% (Target: 80%+) - BELOW TARGET", efficiency_percent);
        }
    }
    
    // Overall validation summary
    println!("\n📊 VALIDATION SUMMARY");
    println!("====================");
    println!("Targets Met: {}/{}", targets_met, total_targets);
    
    if targets_met == total_targets {
        println!("🎉 ALL PERFORMANCE TARGETS MET! Optimization implementation is successful.");
    } else if targets_met >= total_targets / 2 {
        println!("⚠️ PARTIAL SUCCESS: Some targets met, review areas needing improvement.");
    } else {
        println!("❌ PERFORMANCE TARGETS NOT MET: Significant optimization work needed.");
    }
    
    // Recommendations
    println!("\n💡 RECOMMENDATIONS");
    println!("==================");
    
    if targets_met < total_targets {
        println!("• Consider tuning optimization parameters");
        println!("• Review concurrent worker pool sizing");
        println!("• Analyze resource contention patterns");
        println!("• Validate hardware configuration");
    }
    
    if let Some(integrated_result) = integrated {
        if integrated_result.resource_efficiency < 0.8 {
            println!("• Investigate resource bottlenecks");
            println!("• Consider load balancing strategy adjustments");
        }
    }
    
    println!("• Run benchmark multiple times for statistical accuracy");
    println!("• Monitor production performance metrics");
}

fn print_usage() {
    println!("Data Feeder Optimization Benchmark");
    println!("Usage: benchmark_optimization [options]");
    println!();
    println!("Options:");
    println!("  --operations <n>    Number of operations per test (default: 10000)");
    println!("  --workers <n>       Number of concurrent workers (default: 4)");
    println!("  --iterations <n>    Number of test iterations (default: 5)");
    println!("  --warmup <n>        Number of warmup iterations (default: 2)");
    println!("  --quick             Quick test configuration");
    println!("  --intensive         Intensive test configuration");
    println!("  --help, -h          Show this help message");
    println!();
    println!("Examples:");
    println!("  benchmark_optimization --quick");
    println!("  benchmark_optimization --operations 20000 --workers 8");
    println!("  benchmark_optimization --intensive");
}