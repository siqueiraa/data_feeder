//! Output Generation Performance Benchmark
//! 
//! This benchmark validates the <1ms output generation target and measures
//! comprehensive performance improvements for professional trading performance.

use std::time::Instant;
use std::sync::Arc;
use data_feeder::volume_profile::structs::{VolumeProfileData, PriceLevelData, ValueArea};
use data_feeder::volume_profile::output_cache::CacheKey;
use data_feeder::technical_analysis::async_batching::{IndicatorType, IndicatorParameters, PriceType};
use data_feeder::common::output_generation_engine::{
    OutputGenerationEngine, OutputGenerationConfig, OutputRequest
};
use data_feeder::common::hybrid_serialization::{SerializationStrategy, SerializationFormat, ConsumerType};
use rust_decimal_macros::dec;

/// Benchmark configuration
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub single_operation_iterations: usize,
    pub batch_operation_sizes: Vec<usize>,
    pub pipeline_operation_count: usize,
    pub warmup_iterations: usize,
    pub target_single_ms: f64,
    pub target_pipeline_ms: f64,
    pub enable_detailed_metrics: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            single_operation_iterations: 10000,   // 10k single operations
            batch_operation_sizes: vec![10, 50, 100, 500, 1000], // Various batch sizes
            pipeline_operation_count: 3000,       // ~3k ops (similar to story baseline)
            warmup_iterations: 1000,              // 1k warmup
            target_single_ms: 1.0,                // <1ms single operation target
            target_pipeline_ms: 114.0,            // <114ms pipeline target (3.41s → 114ms)
            enable_detailed_metrics: true,
        }
    }
}

/// Benchmark results
#[derive(Debug, Clone)]
pub struct BenchmarkResults {
    pub single_operation_metrics: OperationMetrics,
    pub batch_operation_metrics: Vec<(usize, OperationMetrics)>,
    pub pipeline_metrics: PipelineMetrics,
    pub cache_performance: CachePerformanceMetrics,
    pub overall_assessment: PerformanceAssessment,
}

/// Individual operation performance metrics
#[derive(Debug, Clone)]
pub struct OperationMetrics {
    pub min_time_ms: f64,
    pub max_time_ms: f64,
    pub average_time_ms: f64,
    pub median_time_ms: f64,
    pub p95_time_ms: f64,
    pub p99_time_ms: f64,
    pub std_dev_ms: f64,
    pub throughput_ops_per_second: f64,
    pub success_rate: f64,
}

/// Pipeline performance metrics
#[derive(Debug, Clone)]
pub struct PipelineMetrics {
    pub total_time_ms: f64,
    pub operations_count: usize,
    pub average_time_per_op_ms: f64,
    pub throughput_ops_per_second: f64,
    pub memory_usage_mb: f64,
    pub cache_hit_ratio: f64,
}

/// Cache performance metrics
#[derive(Debug, Clone)]
pub struct CachePerformanceMetrics {
    pub hit_ratio: f64,
    pub average_hit_time_ms: f64,
    pub average_miss_time_ms: f64,
    pub cache_size_entries: usize,
    pub memory_usage_mb: f64,
}

/// Overall performance assessment
#[derive(Debug, Clone)]
pub struct PerformanceAssessment {
    pub single_operation_target_met: bool,
    pub pipeline_target_met: bool,
    pub improvement_factor: f64,        // How much faster than baseline
    pub performance_grade: PerformanceGrade,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PerformanceGrade {
    Excellent,  // Exceeds targets significantly
    Good,       // Meets targets
    Fair,       // Close to targets
    Poor,       // Below targets
}

/// Output generation benchmark suite
pub struct OutputGenerationBenchmark {
    config: BenchmarkConfig,
    engine: Arc<OutputGenerationEngine>,
}

impl OutputGenerationBenchmark {
    /// Create new benchmark suite
    pub fn new(config: BenchmarkConfig) -> Self {
        let engine_config = OutputGenerationConfig {
            performance_target_ms: config.target_single_ms,
            enable_caching: true,
            enable_batching: true,
            enable_streaming: true,
            ..Default::default()
        };

        Self {
            config,
            engine: Arc::new(OutputGenerationEngine::new(engine_config)),
        }
    }

    /// Run complete benchmark suite
    pub async fn run_benchmark(&self) -> BenchmarkResults {
        println!("🚀 Starting Output Generation Performance Benchmark");
        println!("📋 Configuration:");
        println!("   - Single operations: {}", self.config.single_operation_iterations);
        println!("   - Pipeline operations: {}", self.config.pipeline_operation_count);
        println!("   - Target single op: <{}ms", self.config.target_single_ms);
        println!("   - Target pipeline: <{}ms", self.config.target_pipeline_ms);
        
        // Start the engine
        self.engine.start().await.expect("Failed to start engine");

        // Warmup phase
        println!("\n🔥 Warming up system ({} iterations)...", self.config.warmup_iterations);
        self.warmup().await;

        // Single operation benchmark
        println!("\n⚡ Running single operation benchmark...");
        let single_metrics = self.benchmark_single_operations().await;
        
        // Batch operation benchmarks
        println!("\n📦 Running batch operation benchmarks...");
        let mut batch_metrics = Vec::new();
        for &batch_size in &self.config.batch_operation_sizes {
            println!("   Testing batch size: {}", batch_size);
            let metrics = self.benchmark_batch_operations(batch_size).await;
            batch_metrics.push((batch_size, metrics));
        }

        // Pipeline benchmark
        println!("\n🔄 Running pipeline benchmark...");
        let pipeline_metrics = self.benchmark_pipeline().await;

        // Cache performance
        println!("\n🗄️ Analyzing cache performance...");
        let cache_metrics = self.analyze_cache_performance().await;

        // Generate assessment
        let assessment = self.assess_performance(
            &single_metrics,
            &batch_metrics,
            &pipeline_metrics,
            &cache_metrics,
        );

        BenchmarkResults {
            single_operation_metrics: single_metrics,
            batch_operation_metrics: batch_metrics,
            pipeline_metrics,
            cache_performance: cache_metrics,
            overall_assessment: assessment,
        }
    }

    /// Warm up the system
    async fn warmup(&self) {
        for _ in 0..self.config.warmup_iterations {
            let request = self.create_volume_profile_request(0);
            let _ = self.engine.process_request(request).await;
        }
    }

    /// Benchmark single operations
    async fn benchmark_single_operations(&self) -> OperationMetrics {
        let mut durations = Vec::with_capacity(self.config.single_operation_iterations);
        let mut success_count = 0;

        for i in 0..self.config.single_operation_iterations {
            let request = self.create_volume_profile_request(i as u32);
            
            let start = Instant::now();
            let result = self.engine.process_request(request).await;
            let duration = start.elapsed();

            if result.is_ok() {
                success_count += 1;
            }
            
            durations.push(duration.as_secs_f64() * 1000.0); // Convert to ms
        }

        self.calculate_operation_metrics(durations, success_count, self.config.single_operation_iterations)
    }

    /// Benchmark batch operations
    async fn benchmark_batch_operations(&self, batch_size: usize) -> OperationMetrics {
        let iterations = (self.config.single_operation_iterations / batch_size).max(100); // At least 100 iterations
        let mut durations = Vec::with_capacity(iterations);
        let mut success_count = 0;

        for i in 0..iterations {
            let requests = (0..batch_size)
                .map(|j| self.create_volume_profile_request((i * batch_size + j) as u32))
                .collect();

            let batch_request = OutputRequest::Batch {
                requests,
                strategy: SerializationStrategy::default(),
            };
            
            let start = Instant::now();
            let result = self.engine.process_request(batch_request).await;
            let duration = start.elapsed();

            if result.is_ok() {
                success_count += 1;
            }
            
            durations.push(duration.as_secs_f64() * 1000.0); // Convert to ms
        }

        self.calculate_operation_metrics(durations, success_count, iterations)
    }

    /// Benchmark complete pipeline
    async fn benchmark_pipeline(&self) -> PipelineMetrics {
        let start = Instant::now();
        let mut _success_count = 0;

        for i in 0..self.config.pipeline_operation_count {
            let request = if i % 3 == 0 {
                self.create_volume_profile_request(i as u32)
            } else {
                self.create_technical_analysis_request(i as u32)
            };

            let result = self.engine.process_request(request).await;
            if result.is_ok() {
                _success_count += 1;
            }
        }

        let total_duration = start.elapsed();
        let engine_metrics = self.engine.get_metrics().await;

        PipelineMetrics {
            total_time_ms: total_duration.as_secs_f64() * 1000.0,
            operations_count: self.config.pipeline_operation_count,
            average_time_per_op_ms: (total_duration.as_secs_f64() * 1000.0) / self.config.pipeline_operation_count as f64,
            throughput_ops_per_second: self.config.pipeline_operation_count as f64 / total_duration.as_secs_f64(),
            memory_usage_mb: engine_metrics.memory_usage_mb,
            cache_hit_ratio: if engine_metrics.cache_hits + engine_metrics.cache_misses > 0 {
                engine_metrics.cache_hits as f64 / (engine_metrics.cache_hits + engine_metrics.cache_misses) as f64
            } else {
                0.0
            },
        }
    }

    /// Analyze cache performance
    async fn analyze_cache_performance(&self) -> CachePerformanceMetrics {
        let engine_metrics = self.engine.get_metrics().await;

        // Run cache hit/miss test
        let mut hit_times = Vec::new();
        let mut miss_times = Vec::new();

        // Generate cache misses
        for i in 0..100 {
            let request = self.create_volume_profile_request(i + 10000); // New keys
            let start = Instant::now();
            let _ = self.engine.process_request(request).await;
            miss_times.push(start.elapsed().as_secs_f64() * 1000.0);
        }

        // Generate cache hits
        for i in 0..100 {
            let request = self.create_volume_profile_request(i + 10000); // Same keys as above
            let start = Instant::now();
            let _ = self.engine.process_request(request).await;
            hit_times.push(start.elapsed().as_secs_f64() * 1000.0);
        }

        CachePerformanceMetrics {
            hit_ratio: if engine_metrics.cache_hits + engine_metrics.cache_misses > 0 {
                engine_metrics.cache_hits as f64 / (engine_metrics.cache_hits + engine_metrics.cache_misses) as f64
            } else {
                0.0
            },
            average_hit_time_ms: hit_times.iter().sum::<f64>() / hit_times.len() as f64,
            average_miss_time_ms: miss_times.iter().sum::<f64>() / miss_times.len() as f64,
            cache_size_entries: 0, // Would need to expose from cache
            memory_usage_mb: engine_metrics.memory_usage_mb,
        }
    }

    /// Calculate operation metrics from duration data
    fn calculate_operation_metrics(&self, mut durations: Vec<f64>, success_count: usize, total_operations: usize) -> OperationMetrics {
        durations.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let min_time = durations[0];
        let max_time = durations[durations.len() - 1];
        let average_time = durations.iter().sum::<f64>() / durations.len() as f64;
        let median_time = durations[durations.len() / 2];
        let p95_time = durations[(durations.len() as f64 * 0.95) as usize];
        let p99_time = durations[(durations.len() as f64 * 0.99) as usize];

        // Calculate standard deviation
        let variance = durations.iter()
            .map(|x| (x - average_time).powi(2))
            .sum::<f64>() / durations.len() as f64;
        let std_dev = variance.sqrt();

        let throughput = 1000.0 / average_time; // ops per second
        let success_rate = success_count as f64 / total_operations as f64;

        OperationMetrics {
            min_time_ms: min_time,
            max_time_ms: max_time,
            average_time_ms: average_time,
            median_time_ms: median_time,
            p95_time_ms: p95_time,
            p99_time_ms: p99_time,
            std_dev_ms: std_dev,
            throughput_ops_per_second: throughput,
            success_rate,
        }
    }

    /// Assess overall performance
    fn assess_performance(
        &self,
        single_metrics: &OperationMetrics,
        batch_metrics: &[(usize, OperationMetrics)],
        pipeline_metrics: &PipelineMetrics,
        cache_metrics: &CachePerformanceMetrics,
    ) -> PerformanceAssessment {
        let single_target_met = single_metrics.average_time_ms < self.config.target_single_ms;
        let pipeline_target_met = pipeline_metrics.total_time_ms < self.config.target_pipeline_ms;

        // Calculate improvement factor (baseline was 30ms per op, target is <1ms)
        let baseline_time_ms = 30.0; // From story context
        let improvement_factor = baseline_time_ms / single_metrics.average_time_ms;

        let mut recommendations = Vec::new();

        // Performance grade
        let grade = if single_target_met && pipeline_target_met {
            if single_metrics.average_time_ms < self.config.target_single_ms * 0.5 {
                PerformanceGrade::Excellent
            } else {
                PerformanceGrade::Good
            }
        } else if single_metrics.average_time_ms < self.config.target_single_ms * 1.5 {
            recommendations.push("Consider optimizing cache hit ratio".to_string());
            PerformanceGrade::Fair
        } else {
            recommendations.push("Significant optimization needed for single operations".to_string());
            recommendations.push("Consider enabling all performance features".to_string());
            PerformanceGrade::Poor
        };

        // Cache recommendations
        if cache_metrics.hit_ratio < 0.8 {
            recommendations.push("Improve cache hit ratio - consider cache warming strategies".to_string());
        }

        // Batch performance recommendations
        let best_batch = batch_metrics.iter()
            .min_by(|a, b| a.1.average_time_ms.partial_cmp(&b.1.average_time_ms).unwrap());
        
        if let Some((best_size, best_metrics)) = best_batch {
            if best_metrics.average_time_ms < single_metrics.average_time_ms * 0.8 {
                recommendations.push(format!("Consider using batch size {} for optimal performance", best_size));
            }
        }

        PerformanceAssessment {
            single_operation_target_met: single_target_met,
            pipeline_target_met,
            improvement_factor,
            performance_grade: grade,
            recommendations,
        }
    }

    /// Create volume profile request for testing
    fn create_volume_profile_request(&self, id: u32) -> OutputRequest {
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

    /// Create technical analysis request for testing
    fn create_technical_analysis_request(&self, id: u32) -> OutputRequest {
        OutputRequest::TechnicalAnalysis {
            id: format!("ta-{}", id),
            symbol: format!("SYM{}", id % 5),
            indicator_type: match id % 3 {
                0 => IndicatorType::RelativeStrengthIndex,
                1 => IndicatorType::VolumeWeightedAveragePrice,
                _ => IndicatorType::MovingAverageConvergenceDivergence,
            },
            parameters: IndicatorParameters {
                period: Some(14 + (id % 10) as usize),
                price_type: PriceType::Close,
                ..Default::default()
            },
            strategy: SerializationStrategy {
                format: SerializationFormat::Adaptive,
                consumer_type: ConsumerType::HighFrequencyTrading,
                ..Default::default()
            },
        }
    }

    /// Print benchmark results
    pub fn print_results(&self, results: &BenchmarkResults) {
        println!("\n{}", "=".repeat(80));
        println!("🏁 OUTPUT GENERATION BENCHMARK RESULTS");
        println!("{}", "=".repeat(80));

        // Single operation results
        println!("\n⚡ SINGLE OPERATION PERFORMANCE:");
        let single = &results.single_operation_metrics;
        println!("   Average time: {:.3}ms (target: <{}ms) {}", 
                single.average_time_ms, 
                self.config.target_single_ms,
                if single.average_time_ms < self.config.target_single_ms { "✅" } else { "❌" });
        println!("   Median time:  {:.3}ms", single.median_time_ms);
        println!("   P95 time:     {:.3}ms", single.p95_time_ms);
        println!("   P99 time:     {:.3}ms", single.p99_time_ms);
        println!("   Min/Max:      {:.3}ms / {:.3}ms", single.min_time_ms, single.max_time_ms);
        println!("   Std Dev:      {:.3}ms", single.std_dev_ms);
        println!("   Throughput:   {:.0} ops/sec", single.throughput_ops_per_second);
        println!("   Success rate: {:.1}%", single.success_rate * 100.0);

        // Pipeline results
        println!("\n🔄 PIPELINE PERFORMANCE:");
        let pipeline = &results.pipeline_metrics;
        println!("   Total time:   {:.1}ms (target: <{}ms) {}", 
                pipeline.total_time_ms, 
                self.config.target_pipeline_ms,
                if pipeline.total_time_ms < self.config.target_pipeline_ms { "✅" } else { "❌" });
        println!("   Operations:   {}", pipeline.operations_count);
        println!("   Avg per op:   {:.3}ms", pipeline.average_time_per_op_ms);
        println!("   Throughput:   {:.0} ops/sec", pipeline.throughput_ops_per_second);
        println!("   Cache hit:    {:.1}%", pipeline.cache_hit_ratio * 100.0);

        // Cache performance
        println!("\n🗄️  CACHE PERFORMANCE:");
        let cache = &results.cache_performance;
        println!("   Hit ratio:    {:.1}%", cache.hit_ratio * 100.0);
        println!("   Hit time:     {:.3}ms", cache.average_hit_time_ms);
        println!("   Miss time:    {:.3}ms", cache.average_miss_time_ms);
        println!("   Memory usage: {:.1}MB", cache.memory_usage_mb);

        // Batch performance
        println!("\n📦 BATCH PERFORMANCE:");
        for (size, metrics) in &results.batch_operation_metrics {
            println!("   Batch {}: {:.3}ms avg, {:.0} ops/sec", 
                    size, metrics.average_time_ms, metrics.throughput_ops_per_second);
        }

        // Overall assessment
        println!("\n🏆 OVERALL ASSESSMENT:");
        let assessment = &results.overall_assessment;
        println!("   Single target: {}", if assessment.single_operation_target_met { "✅ MET" } else { "❌ NOT MET" });
        println!("   Pipeline target: {}", if assessment.pipeline_target_met { "✅ MET" } else { "❌ NOT MET" });
        println!("   Improvement: {:.1}x faster than baseline", assessment.improvement_factor);
        println!("   Performance grade: {:?}", assessment.performance_grade);

        if !assessment.recommendations.is_empty() {
            println!("\n💡 RECOMMENDATIONS:");
            for rec in &assessment.recommendations {
                println!("   • {}", rec);
            }
        }

        println!("\n{}", "=".repeat(80));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Output Generation Performance Benchmark");
    println!("Validating <1ms target and >97% improvement over 30ms baseline\n");

    let config = BenchmarkConfig::default();
    let benchmark = OutputGenerationBenchmark::new(config.clone());

    let results = benchmark.run_benchmark().await;
    benchmark.print_results(&results);

    // Generate performance report
    println!("\n📊 Performance Report Summary:");
    if results.overall_assessment.performance_grade == PerformanceGrade::Excellent ||
       results.overall_assessment.performance_grade == PerformanceGrade::Good {
        println!("🎉 SUCCESS: Performance targets achieved!");
        println!("✅ Single operation: {:.3}ms (target: <{}ms)", 
                results.single_operation_metrics.average_time_ms, config.target_single_ms);
        println!("✅ Pipeline: {:.1}ms (target: <{}ms)", 
                results.pipeline_metrics.total_time_ms, config.target_pipeline_ms);
        println!("🚀 Improvement factor: {:.1}x", results.overall_assessment.improvement_factor);
    } else {
        println!("⚠️  WARNING: Performance targets not fully met");
        println!("Single operation: {:.3}ms (target: <{}ms)", 
                results.single_operation_metrics.average_time_ms, config.target_single_ms);
        println!("Pipeline: {:.1}ms (target: <{}ms)", 
                results.pipeline_metrics.total_time_ms, config.target_pipeline_ms);
    }

    Ok(())
}