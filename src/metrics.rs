/// Prometheus metrics module for performance monitoring
/// 
/// This module provides comprehensive monitoring and observability for the data feeder
/// application, tracking performance metrics, errors, and system health.
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, debug};
use crate::tokio_metrics::TokioMetricsCollector;

/// Global metrics registry for the application
pub struct MetricsRegistry {
    registry: Registry,
    tokio_metrics_collector: Option<TokioMetricsCollector>,
    
    // Technical Analysis Performance Metrics
    pub indicator_processing_duration: HistogramVec,
    pub indicator_processing_total: IntCounterVec,
    pub indicator_errors_total: IntCounterVec,
    
    // Data Processing Performance  
    pub candle_processing_duration: HistogramVec,
    pub candles_processed_total: IntCounterVec,
    pub kafka_publish_duration: HistogramVec,
    pub kafka_publish_total: IntCounterVec,
    pub kafka_publish_errors_total: IntCounterVec,
    
    // System Health Metrics
    pub memory_usage_bytes: IntGaugeVec,
    pub active_connections: IntGaugeVec,
    pub last_update_timestamp: IntGaugeVec,
    
    // Custom Performance Metrics
    pub ema_calculations_total: IntCounterVec,
    pub simd_operations_total: IntCounterVec,
    pub memory_pool_allocations_total: IntCounterVec,
    pub t_digest_updates_total: IntCounterVec,
    pub t_digest_size_gauge: IntGaugeVec,
    pub t_digest_memory_bytes: IntGaugeVec,
    
    // CPU Optimization Metrics (Story 6.2 - AC: 1)
    pub simd_instructions_used: IntCounterVec,
    pub cpu_hotpath_executions: IntCounterVec,
    pub thread_pool_utilization: IntGaugeVec,
    pub algorithm_optimization_hits: IntCounterVec,
    
    // Memory Optimization Metrics (Story 6.2 - AC: 2)
    pub memory_pool_efficiency: IntGaugeVec,
    pub smart_pool_adaptations: IntCounterVec,
    pub garbage_collection_events: IntCounterVec,
    pub zero_copy_operations: IntCounterVec,
    
    // Processing Pipeline Optimization Metrics (Story 6.2 - AC: 3)
    pub batch_processing_efficiency: IntGaugeVec,
    pub cache_hit_rate: IntGaugeVec,
    pub pipeline_throughput: IntCounterVec,
    pub intelligent_cache_operations: IntCounterVec,
    
    // Network Optimization Metrics (Story 6.2 - AC: 4)
    pub connection_pool_hit_rate: IntGaugeVec,
    pub connection_pool_utilization: IntGaugeVec,
    pub payload_compression_ratio: IntGaugeVec,
    pub protocol_batch_efficiency: IntGaugeVec,
    
    // Storage Optimization Metrics (Story 6.2 - AC: 5)
    pub lmdb_operation_duration: HistogramVec,
    pub storage_cache_hit_rate: IntGaugeVec,
    pub query_optimization_effectiveness: IntGaugeVec,
    pub storage_throughput: IntCounterVec,

    // Concurrency Optimization Metrics (Story 6.2 - AC: 6)
    pub lock_free_operations_total: IntCounterVec,
    pub actor_message_processing_duration: HistogramVec,
    pub work_stealing_success_rate: IntGaugeVec,
    pub concurrent_worker_utilization: IntGaugeVec,
    
    // Error Tracking
    pub error_count_by_type: IntCounterVec,
    pub panic_count_total: IntCounterVec,
    
    // CPU Performance Metrics (NEW)
    pub cpu_usage_percent: IntGaugeVec,
    pub component_cpu_time_nanos: IntCounterVec,
    pub operation_duration_histogram: HistogramVec,
    
    // Resource-Aware Metrics (AC: 1, 4, 5)
    pub system_resource_utilization: IntGaugeVec,
    pub adaptive_config_effectiveness: IntGaugeVec,
    pub resource_pattern_detection: IntCounterVec,
    pub configuration_adaptation_events: IntCounterVec,
    pub resource_threshold_breaches: IntCounterVec,
}

impl MetricsRegistry {
    /// Create a new metrics registry with all performance counters
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        
        // Initialize tokio metrics collector
        let tokio_metrics_collector = match crate::tokio_metrics::initialize_tokio_metrics(
            &registry,
            Some(Duration::from_secs(30))
        ) {
            Ok(collector) => {
                info!("✅ Tokio metrics collector initialized successfully");
                Some(collector)
            },
            Err(e) => {
                tracing::warn!("⚠️ Failed to initialize tokio metrics: {}", e);
                None
            }
        };
        
        // Technical Analysis Performance Metrics
        let indicator_processing_duration = HistogramVec::new(
            HistogramOpts::new(
                "indicator_processing_duration_seconds",
                "Time spent processing technical indicators"
            ).buckets(vec![0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05]),
            &["symbol", "operation_type"]
        )?;
        
        let indicator_processing_total = IntCounterVec::new(
            Opts::new("indicator_processing_total", "Total number of indicator calculations"),
            &["symbol", "indicator_type"]
        )?;
        
        let indicator_errors_total = IntCounterVec::new(
            Opts::new("indicator_errors_total", "Total number of indicator processing errors"),
            &["symbol", "error_type"]
        )?;
        
        // Data Processing Performance
        let candle_processing_duration = HistogramVec::new(
            HistogramOpts::new(
                "candle_processing_duration_seconds", 
                "Time spent processing individual candles"
            ).buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005]),
            &["timeframe", "operation"]
        )?;
        
        let candles_processed_total = IntCounterVec::new(
            Opts::new("candles_processed_total", "Total number of candles processed"),
            &["symbol", "timeframe"]
        )?;
        
        let kafka_publish_duration = HistogramVec::new(
            HistogramOpts::new(
                "kafka_publish_duration_seconds",
                "Time spent publishing to Kafka"
            ).buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
            &["topic", "partition"]
        )?;
        
        let kafka_publish_total = IntCounterVec::new(
            Opts::new("kafka_publish_total", "Total number of Kafka messages published"),
            &["topic", "status"]
        )?;
        
        let kafka_publish_errors_total = IntCounterVec::new(
            Opts::new("kafka_publish_errors_total", "Total number of Kafka publish errors"),
            &["topic", "error_type"]
        )?;
        
        // System Health Metrics
        let memory_usage_bytes = IntGaugeVec::new(
            Opts::new("memory_usage_bytes", "Current memory usage in bytes"),
            &["component", "pool_type"]
        )?;
        
        let active_connections = IntGaugeVec::new(
            Opts::new("active_connections", "Number of active connections"),
            &["connection_type", "endpoint"]
        )?;
        
        let last_update_timestamp = IntGaugeVec::new(
            Opts::new("last_update_timestamp", "Timestamp of last successful update"),
            &["component", "symbol"]
        )?;
        
        // Custom Performance Metrics
        let ema_calculations_total = IntCounterVec::new(
            Opts::new("ema_calculations_total", "Total number of EMA calculations performed"),
            &["symbol", "timeframe", "period"]
        )?;
        
        let simd_operations_total = IntCounterVec::new(
            Opts::new("simd_operations_total", "Total number of SIMD-optimized operations"),
            &["operation_type", "vector_size"]
        )?;
        
        let memory_pool_allocations_total = IntCounterVec::new(
            Opts::new("memory_pool_allocations_total", "Total memory pool allocations avoided"),
            &["pool_type", "component"]
        )?;
        
        let t_digest_updates_total = IntCounterVec::new(
            Opts::new("t_digest_updates_total", "Total T-Digest updates performed"),
            &["symbol", "quantile_type"]
        )?;
        
        let t_digest_size_gauge = IntGaugeVec::new(
            Opts::new("t_digest_size_centroids", "Number of centroids in T-Digest"),
            &["symbol", "digest_type"]
        )?;
        
        let t_digest_memory_bytes = IntGaugeVec::new(
            Opts::new("t_digest_memory_bytes", "Estimated T-Digest memory usage in bytes"),
            &["symbol", "digest_type"]
        )?;
        
        // CPU Optimization Metrics (Story 6.2 - AC: 1)
        let simd_instructions_used = IntCounterVec::new(
            Opts::new("simd_instructions_used_total", "Total number of SIMD instructions executed"),
            &["instruction_type", "data_type", "vector_width"]
        )?;
        
        let cpu_hotpath_executions = IntCounterVec::new(
            Opts::new("cpu_hotpath_executions_total", "Total executions of CPU-optimized hot paths"),
            &["hotpath_type", "optimization_level"]
        )?;
        
        let thread_pool_utilization = IntGaugeVec::new(
            Opts::new("thread_pool_utilization_percent", "Thread pool utilization percentage"),
            &["pool_type", "thread_category"]
        )?;
        
        let algorithm_optimization_hits = IntCounterVec::new(
            Opts::new("algorithm_optimization_hits_total", "Total algorithm optimization cache hits"),
            &["algorithm_type", "optimization_category"]
        )?;
        
        // Memory Optimization Metrics (Story 6.2 - AC: 2)
        let memory_pool_efficiency = IntGaugeVec::new(
            Opts::new("memory_pool_efficiency_percent", "Memory pool efficiency percentage (hit rate)"),
            &["pool_type", "component"]
        )?;
        
        let smart_pool_adaptations = IntCounterVec::new(
            Opts::new("smart_pool_adaptations_total", "Total smart pool size adaptations"),
            &["pool_type", "adaptation_direction"]
        )?;
        
        let garbage_collection_events = IntCounterVec::new(
            Opts::new("garbage_collection_events_total", "Total garbage collection events triggered"),
            &["pool_type", "trigger_reason"]
        )?;
        
        let zero_copy_operations = IntCounterVec::new(
            Opts::new("zero_copy_operations_total", "Total zero-copy operations performed"),
            &["operation_type", "data_size_category"]
        )?;
        
        // Processing Pipeline Optimization Metrics (Story 6.2 - AC: 3)
        let batch_processing_efficiency = IntGaugeVec::new(
            Opts::new("batch_processing_efficiency_percent", "Batch processing efficiency percentage"),
            &["processor_type", "batch_size_category"]
        )?;
        
        let cache_hit_rate = IntGaugeVec::new(
            Opts::new("cache_hit_rate_percent", "Cache hit rate percentage"),
            &["cache_type", "data_category"]
        )?;
        
        let pipeline_throughput = IntCounterVec::new(
            Opts::new("pipeline_throughput_items_total", "Total items processed through optimized pipelines"),
            &["pipeline_type", "processing_stage"]
        )?;
        
        let intelligent_cache_operations = IntCounterVec::new(
            Opts::new("intelligent_cache_operations_total", "Total intelligent cache operations"),
            &["operation_type", "cache_strategy"]
        )?;
        
        // Network Optimization Metrics (Story 6.2 - AC: 4)
        let connection_pool_hit_rate = IntGaugeVec::new(
            Opts::new("connection_pool_hit_rate_percent", "Connection pool hit rate percentage"),
            &["pool_type", "endpoint"]
        )?;
        
        let connection_pool_utilization = IntGaugeVec::new(
            Opts::new("connection_pool_utilization_percent", "Connection pool utilization percentage"),
            &["pool_type", "endpoint"]
        )?;
        
        let payload_compression_ratio = IntGaugeVec::new(
            Opts::new("payload_compression_ratio_percent", "Payload compression ratio percentage"),
            &["compression_type", "data_category"]
        )?;
        
        let protocol_batch_efficiency = IntGaugeVec::new(
            Opts::new("protocol_batch_efficiency_percent", "Protocol batching efficiency percentage"),
            &["protocol_type", "message_category"]
        )?;
        
        // Storage Optimization Metrics (Story 6.2 - AC: 5)
        let lmdb_operation_duration = HistogramVec::new(
            HistogramOpts::new(
                "lmdb_operation_duration_seconds",
                "Duration of LMDB storage operations"
            ).buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
            &["operation_type", "database", "optimization_level"]
        )?;
        
        let storage_cache_hit_rate = IntGaugeVec::new(
            Opts::new("storage_cache_hit_rate_percent", "Storage cache hit rate percentage"),
            &["cache_level", "data_category"]
        )?;
        
        let query_optimization_effectiveness = IntGaugeVec::new(
            Opts::new("query_optimization_effectiveness_percent", "Query optimization effectiveness percentage"),
            &["query_type", "optimization_strategy"]
        )?;
        
        let storage_throughput = IntCounterVec::new(
            Opts::new("storage_throughput_operations_total", "Total storage operations processed"),
            &["operation_type", "database", "optimization_enabled"]
        )?;

        // Concurrency Optimization Metrics (Story 6.2 - AC: 6)
        let lock_free_operations_total = IntCounterVec::new(
            Opts::new("lock_free_operations_total", "Total lock-free operations performed"),
            &["operation_type", "data_structure", "success"]
        )?;

        let actor_message_processing_duration = HistogramVec::new(
            HistogramOpts::new(
                "actor_message_processing_duration_seconds",
                "Time spent processing actor messages in batches"
            ).buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05]),
            &["actor_type", "batch_size_range"]
        )?;

        let work_stealing_success_rate = IntGaugeVec::new(
            Opts::new("work_stealing_success_rate_percent", "Success rate of work stealing attempts"),
            &["worker_pool", "steal_strategy"]
        )?;

        let concurrent_worker_utilization = IntGaugeVec::new(
            Opts::new("concurrent_worker_utilization_percent", "Utilization rate of concurrent workers"),
            &["pool_id", "worker_type", "scaling_enabled"]
        )?;
        
        // Error Tracking
        let error_count_by_type = IntCounterVec::new(
            Opts::new("error_count_by_type", "Total errors by type"),
            &["error_type", "component", "severity"]
        )?;
        
        let panic_count_total = IntCounterVec::new(
            Opts::new("panic_count_total", "Total number of panics (should be zero)"),
            &["component", "location"]
        )?;
        
        // CPU Performance Metrics (NEW)
        let cpu_usage_percent = IntGaugeVec::new(
            Opts::new("cpu_usage_percent", "Current CPU usage percentage by component"),
            &["component", "thread"]
        )?;
        
        let component_cpu_time_nanos = IntCounterVec::new(
            Opts::new("component_cpu_time_nanos", "Total CPU time consumed by component in nanoseconds"),
            &["component", "operation"]
        )?;
        
        let operation_duration_histogram = HistogramVec::new(
            HistogramOpts::new(
                "operation_duration_seconds",
                "Duration of operations across all components"
            ).buckets(vec![0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0, 10.0]),
            &["component", "operation"]
        )?;
        
        // Resource-Aware Metrics
        let system_resource_utilization = IntGaugeVec::new(
            Opts::new("system_resource_utilization_percent", "System resource utilization percentage"),
            &["resource_type", "environment"]
        )?;
        
        let adaptive_config_effectiveness = IntGaugeVec::new(
            Opts::new("adaptive_config_effectiveness_score", "Effectiveness score of adaptive configuration (0-100)"),
            &["config_type", "resource_profile"]
        )?;
        
        let resource_pattern_detection = IntCounterVec::new(
            Opts::new("resource_pattern_detection_total", "Total resource usage patterns detected"),
            &["pattern_type", "severity", "environment"]
        )?;
        
        let configuration_adaptation_events = IntCounterVec::new(
            Opts::new("configuration_adaptation_events_total", "Total configuration adaptation events"),
            &["adaptation_type", "trigger_cause", "success"]
        )?;
        
        let resource_threshold_breaches = IntCounterVec::new(
            Opts::new("resource_threshold_breaches_total", "Total resource threshold breach events"),
            &["threshold_type", "breach_level", "recovery_action"]
        )?;
        
        // Register all metrics
        registry.register(Box::new(indicator_processing_duration.clone()))?;
        registry.register(Box::new(indicator_processing_total.clone()))?;
        registry.register(Box::new(indicator_errors_total.clone()))?;
        registry.register(Box::new(candle_processing_duration.clone()))?;
        registry.register(Box::new(candles_processed_total.clone()))?;
        registry.register(Box::new(kafka_publish_duration.clone()))?;
        registry.register(Box::new(kafka_publish_total.clone()))?;
        registry.register(Box::new(kafka_publish_errors_total.clone()))?;
        registry.register(Box::new(memory_usage_bytes.clone()))?;
        registry.register(Box::new(active_connections.clone()))?;
        registry.register(Box::new(last_update_timestamp.clone()))?;
        registry.register(Box::new(ema_calculations_total.clone()))?;
        registry.register(Box::new(simd_operations_total.clone()))?;
        registry.register(Box::new(memory_pool_allocations_total.clone()))?;
        registry.register(Box::new(t_digest_updates_total.clone()))?;
        registry.register(Box::new(t_digest_size_gauge.clone()))?;
        registry.register(Box::new(t_digest_memory_bytes.clone()))?;
        
        // Register CPU optimization metrics (Story 6.2)
        registry.register(Box::new(simd_instructions_used.clone()))?;
        registry.register(Box::new(cpu_hotpath_executions.clone()))?;
        registry.register(Box::new(thread_pool_utilization.clone()))?;
        registry.register(Box::new(algorithm_optimization_hits.clone()))?;
        
        // Register memory optimization metrics (Story 6.2) 
        registry.register(Box::new(memory_pool_efficiency.clone()))?;
        registry.register(Box::new(smart_pool_adaptations.clone()))?;
        registry.register(Box::new(garbage_collection_events.clone()))?;
        registry.register(Box::new(zero_copy_operations.clone()))?;
        
        // Register processing pipeline optimization metrics (Story 6.2)
        registry.register(Box::new(batch_processing_efficiency.clone()))?;
        registry.register(Box::new(cache_hit_rate.clone()))?;
        registry.register(Box::new(pipeline_throughput.clone()))?;
        registry.register(Box::new(intelligent_cache_operations.clone()))?;
        
        // Register network optimization metrics (Story 6.2)
        registry.register(Box::new(connection_pool_hit_rate.clone()))?;
        registry.register(Box::new(connection_pool_utilization.clone()))?;
        registry.register(Box::new(payload_compression_ratio.clone()))?;
        registry.register(Box::new(protocol_batch_efficiency.clone()))?;
        
        // Register storage optimization metrics (Story 6.2)
        registry.register(Box::new(lmdb_operation_duration.clone()))?;
        registry.register(Box::new(storage_cache_hit_rate.clone()))?;
        registry.register(Box::new(query_optimization_effectiveness.clone()))?;
        registry.register(Box::new(storage_throughput.clone()))?;

        // Register concurrency optimization metrics (Story 6.2)
        registry.register(Box::new(lock_free_operations_total.clone()))?;
        registry.register(Box::new(actor_message_processing_duration.clone()))?;
        registry.register(Box::new(work_stealing_success_rate.clone()))?;
        registry.register(Box::new(concurrent_worker_utilization.clone()))?;
        
        registry.register(Box::new(error_count_by_type.clone()))?;
        registry.register(Box::new(panic_count_total.clone()))?;
        
        // Register CPU performance metrics
        registry.register(Box::new(cpu_usage_percent.clone()))?;
        registry.register(Box::new(component_cpu_time_nanos.clone()))?;
        registry.register(Box::new(operation_duration_histogram.clone()))?;
        
        // Register resource-aware metrics
        registry.register(Box::new(system_resource_utilization.clone()))?;
        registry.register(Box::new(adaptive_config_effectiveness.clone()))?;
        registry.register(Box::new(resource_pattern_detection.clone()))?;
        registry.register(Box::new(configuration_adaptation_events.clone()))?;
        registry.register(Box::new(resource_threshold_breaches.clone()))?;
        
        let _total_metrics = if tokio_metrics_collector.is_some() { 
            info!("Prometheus metrics registry initialized with {} business metrics + tokio task metrics", 35);
            35 + 9 // 35 business metrics + 9 tokio task metrics
        } else {
            info!("Prometheus metrics registry initialized with {} business metrics (tokio metrics disabled)", 35);
            35
        };
        
        // Add some initial test data to verify metrics are working
        let metrics_registry = MetricsRegistry {
            registry,
            tokio_metrics_collector,
            indicator_processing_duration,
            indicator_processing_total,
            indicator_errors_total,
            candle_processing_duration,
            candles_processed_total,
            kafka_publish_duration,
            kafka_publish_total,
            kafka_publish_errors_total,
            memory_usage_bytes,
            active_connections,
            last_update_timestamp,
            ema_calculations_total,
            simd_operations_total,
            memory_pool_allocations_total,
            t_digest_updates_total,
            t_digest_size_gauge,
            t_digest_memory_bytes,
            simd_instructions_used,
            cpu_hotpath_executions,
            thread_pool_utilization,
            algorithm_optimization_hits,
            memory_pool_efficiency,
            smart_pool_adaptations,
            garbage_collection_events,
            zero_copy_operations,
            batch_processing_efficiency,
            cache_hit_rate,
            pipeline_throughput,
            intelligent_cache_operations,
            connection_pool_hit_rate,
            connection_pool_utilization,
            payload_compression_ratio,
            protocol_batch_efficiency,
            lmdb_operation_duration,
            storage_cache_hit_rate,
            query_optimization_effectiveness,
            storage_throughput,
            lock_free_operations_total,
            actor_message_processing_duration,
            work_stealing_success_rate,
            concurrent_worker_utilization,
            error_count_by_type,
            panic_count_total,
            cpu_usage_percent,
            component_cpu_time_nanos,
            operation_duration_histogram,
            system_resource_utilization,
            adaptive_config_effectiveness,
            resource_pattern_detection,
            configuration_adaptation_events,
            resource_threshold_breaches,
        };
        
        // Add some test data so metrics show up immediately
        metrics_registry.indicator_processing_total
            .with_label_values(&["TEST", "startup"])
            .inc();
        metrics_registry.ema_calculations_total
            .with_label_values(&["BTCUSDT", "60", "21"])
            .inc();
        metrics_registry.memory_pool_allocations_total
            .with_label_values(&["test_pool", "startup"])
            .inc();
            
        // Initialize new resource-aware metrics with test data
        metrics_registry.system_resource_utilization
            .with_label_values(&["memory", "startup"])
            .set(50);
        metrics_registry.adaptive_config_effectiveness
            .with_label_values(&["thread_pool", "standard"])
            .set(85);
        
        Ok(metrics_registry)
    }
    
    /// Export metrics in Prometheus text format
    pub fn export_metrics(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        
        encoder.encode(&metric_families, &mut buffer)?;
        
        String::from_utf8(buffer)
            .map_err(|e| prometheus::Error::Msg(format!("UTF-8 encoding error: {}", e)))
    }
    
    /// Record indicator processing time and increment counters
    pub fn record_indicator_processing(&self, symbol: &str, operation_type: &str, duration_seconds: f64) {
        self.indicator_processing_duration
            .with_label_values(&[symbol, operation_type])
            .observe(duration_seconds);
            
        self.indicator_processing_total
            .with_label_values(&[symbol, operation_type])
            .inc();
    }
    
    /// Record memory pool usage to track optimization effectiveness
    pub fn record_memory_pool_usage(&self, pool_type: &str, component: &str) {
        self.memory_pool_allocations_total
            .with_label_values(&[pool_type, component])
            .inc();
    }
    
    /// Record SIMD operation for performance tracking
    pub fn record_simd_operation(&self, operation_type: &str, vector_size: usize) {
        self.simd_operations_total
            .with_label_values(&[operation_type, &vector_size.to_string()])
            .inc();
    }
    
    /// Record T-Digest update for quantile tracking
    pub fn record_t_digest_update(&self, symbol: &str, quantile_type: &str) {
        self.t_digest_updates_total
            .with_label_values(&[symbol, quantile_type])
            .inc();
    }
    
    /// Record T-Digest size metrics for memory monitoring
    pub fn record_t_digest_size(&self, symbol: &str, digest_type: &str, centroids: usize, estimated_bytes: usize) {
        self.t_digest_size_gauge
            .with_label_values(&[symbol, digest_type])
            .set(centroids as i64);
        self.t_digest_memory_bytes
            .with_label_values(&[symbol, digest_type])
            .set(estimated_bytes as i64);
    }
    
    /// Update system health metrics
    pub fn update_system_health(&self, component: &str, memory_bytes: i64, connections: i64) {
        self.memory_usage_bytes
            .with_label_values(&[component, "total"])
            .set(memory_bytes);
            
        self.active_connections
            .with_label_values(&["websocket", component])
            .set(connections);
            
        self.last_update_timestamp
            .with_label_values(&[component, "system"])
            .set(chrono::Utc::now().timestamp());
    }
    
    /// Get access to the tokio metrics collector for task instrumentation
    pub fn tokio_metrics_collector(&self) -> Option<&TokioMetricsCollector> {
        self.tokio_metrics_collector.as_ref()
    }
    
    /// Record CPU usage for a component
    pub fn record_cpu_usage(&self, component: &str, thread: &str, cpu_percent: i64) {
        self.cpu_usage_percent
            .with_label_values(&[component, thread])
            .set(cpu_percent);
    }
    
    /// Record CPU time consumed by a component operation
    pub fn record_cpu_time(&self, component: &str, operation: &str, cpu_time_nanos: u64) {
        self.component_cpu_time_nanos
            .with_label_values(&[component, operation])
            .inc_by(cpu_time_nanos);
    }
    
    /// Record operation duration for histogram analysis
    pub fn record_operation_duration(&self, component: &str, operation: &str, duration_seconds: f64) {
        self.operation_duration_histogram
            .with_label_values(&[component, operation])
            .observe(duration_seconds);
    }
    
    /// Record system resource utilization for monitoring
    pub fn record_system_resource_utilization(&self, resource_type: &str, environment: &str, utilization_percent: i64) {
        self.system_resource_utilization
            .with_label_values(&[resource_type, environment])
            .set(utilization_percent);
    }
    
    /// Record adaptive configuration effectiveness score
    pub fn record_adaptive_config_effectiveness(&self, config_type: &str, resource_profile: &str, effectiveness_score: i64) {
        self.adaptive_config_effectiveness
            .with_label_values(&[config_type, resource_profile])
            .set(effectiveness_score);
    }
    
    /// Record resource pattern detection events
    pub fn record_resource_pattern_detection(&self, pattern_type: &str, severity: &str, environment: &str) {
        self.resource_pattern_detection
            .with_label_values(&[pattern_type, severity, environment])
            .inc();
    }
    
    /// Record configuration adaptation events
    pub fn record_configuration_adaptation(&self, adaptation_type: &str, trigger_cause: &str, success: bool) {
        let success_str = if success { "success" } else { "failure" };
        self.configuration_adaptation_events
            .with_label_values(&[adaptation_type, trigger_cause, success_str])
            .inc();
    }
    
    /// Record resource threshold breach events
    pub fn record_resource_threshold_breach(&self, threshold_type: &str, breach_level: &str, recovery_action: &str) {
        self.resource_threshold_breaches
            .with_label_values(&[threshold_type, breach_level, recovery_action])
            .inc();
    }
    
    /// Record connection pool metrics
    pub fn record_connection_pool_metrics(&self, pool_type: &str, endpoint: &str, hit_rate: f64, utilization: f64) {
        self.connection_pool_hit_rate
            .with_label_values(&[pool_type, endpoint])
            .set((hit_rate * 100.0) as i64);
        self.connection_pool_utilization
            .with_label_values(&[pool_type, endpoint])
            .set((utilization * 100.0) as i64);
    }
    
    /// Record payload compression metrics
    pub fn record_payload_compression(&self, compression_type: &str, data_category: &str, compression_ratio: f64) {
        self.payload_compression_ratio
            .with_label_values(&[compression_type, data_category])
            .set((compression_ratio * 100.0) as i64);
    }
    
    /// Record protocol batching efficiency
    pub fn record_protocol_batch_efficiency(&self, protocol_type: &str, message_category: &str, efficiency: f64) {
        self.protocol_batch_efficiency
            .with_label_values(&[protocol_type, message_category])
            .set((efficiency * 100.0) as i64);
    }
    
    /// Record LMDB operation duration
    pub fn record_lmdb_operation_duration(&self, operation_type: &str, database: &str, optimization_level: &str, duration_seconds: f64) {
        self.lmdb_operation_duration
            .with_label_values(&[operation_type, database, optimization_level])
            .observe(duration_seconds);
    }
    
    /// Record storage cache hit rate
    pub fn record_storage_cache_hit_rate(&self, cache_level: &str, data_category: &str, hit_rate: f64) {
        self.storage_cache_hit_rate
            .with_label_values(&[cache_level, data_category])
            .set((hit_rate * 100.0) as i64);
    }
    
    /// Record query optimization effectiveness
    pub fn record_query_optimization_effectiveness(&self, query_type: &str, optimization_strategy: &str, effectiveness: f64) {
        self.query_optimization_effectiveness
            .with_label_values(&[query_type, optimization_strategy])
            .set((effectiveness * 100.0) as i64);
    }
    
    /// Record storage throughput
    pub fn record_storage_throughput(&self, operation_type: &str, database: &str, optimization_enabled: bool) {
        let optimization_str = if optimization_enabled { "enabled" } else { "disabled" };
        self.storage_throughput
            .with_label_values(&[operation_type, database, optimization_str])
            .inc();
    }

    /// Record comprehensive storage optimization metrics
    pub fn record_storage_optimization_metrics(&self, database: &str, optimization_type: &str, cache_hit_rate: f64, query_effectiveness: f64, operation_duration: std::time::Duration) {
        // Record cache hit rate
        self.record_storage_cache_hit_rate("lmdb", database, cache_hit_rate);
        
        // Record query optimization effectiveness
        self.record_query_optimization_effectiveness("range_query", optimization_type, query_effectiveness);
        
        // Record operation duration with correct label order: operation_type, database, optimization_level
        self.lmdb_operation_duration
            .with_label_values(&[optimization_type, database, "optimized"])
            .observe(operation_duration.as_secs_f64());
        
        // Record throughput
        self.record_storage_throughput("optimized_query", database, true);
    }

    /// Record LMDB configuration optimization metrics
    pub fn record_lmdb_config_optimization(&self, workload_pattern: &str, map_size: usize, max_readers: u64, max_dbs: u64) {
        // Record LMDB configuration optimization
        self.storage_throughput
            .with_label_values(&["lmdb_config", workload_pattern, "enabled"])
            .inc();
        
        // Record map size as a gauge (convert to MB for readability)
        self.memory_usage_bytes
            .with_label_values(&["lmdb_config", "map_size"])
            .set((map_size / (1024 * 1024)) as i64);
        
        // Record reader and database limits
        self.active_connections
            .with_label_values(&["lmdb_readers", "max_configured"])
            .set(max_readers as i64);
        
        self.active_connections
            .with_label_values(&["lmdb_databases", "max_configured"])
            .set(max_dbs as i64);
        
        debug!("📊 LMDB config optimization recorded: workload={}, map_size={}MB, readers={}, dbs={}", 
               workload_pattern, map_size / (1024*1024), max_readers, max_dbs);
    }

    /// Record comprehensive concurrency optimization metrics
    pub fn record_concurrency_optimization_metrics(&self, operation_type: &str, pool_type: &str, concurrent_tasks: u64, processing_time: f64, efficiency: f64) {
        // Record processing time
        self.lmdb_operation_duration
            .with_label_values(&[operation_type, pool_type, "concurrent"])
            .observe(processing_time);
        
        // Record concurrent task count as a gauge
        self.active_connections
            .with_label_values(&["concurrent_tasks", operation_type])
            .set(concurrent_tasks as i64);
        
        // Record efficiency as performance metric using operation duration
        self.lmdb_operation_duration
            .with_label_values(&["concurrency_efficiency", pool_type, "efficiency"])
            .observe(efficiency * 1000.0); // Convert to "ms" scale for consistency
        
        // Record successful concurrent operation
        self.record_storage_throughput(operation_type, pool_type, true);
        
        debug!("📊 Recorded concurrency metrics: {} tasks, {:.3}s, {:.2}% efficiency", 
               concurrent_tasks, processing_time, efficiency * 100.0);
    }

    // Concurrency Optimization Metrics (Story 6.2 - AC: 6)
    
    /// Record lock-free operation
    pub fn record_lock_free_operation(&self, operation_type: &str, data_structure: &str, success: bool) {
        let success_str = if success { "success" } else { "failure" };
        self.lock_free_operations_total
            .with_label_values(&[operation_type, data_structure, success_str])
            .inc();
    }

    /// Record actor message processing duration
    pub fn record_actor_message_processing(&self, actor_type: &str, batch_size: usize, duration: std::time::Duration) {
        let batch_size_range = match batch_size {
            1..=5 => "small",
            6..=20 => "medium",
            21..=50 => "large",
            _ => "extra_large",
        };
        
        self.actor_message_processing_duration
            .with_label_values(&[actor_type, batch_size_range])
            .observe(duration.as_secs_f64());
    }

    /// Record work stealing success rate
    pub fn record_work_stealing_metrics(&self, attempted: u64, successful: u64, queue_size: usize) {
        let success_rate = if attempted == 0 { 0.0 } else { successful as f64 / attempted as f64 };
        
        self.work_stealing_success_rate
            .with_label_values(&["work_stealer", "standard"])
            .set((success_rate * 100.0) as i64);
            
        // Record queue utilization
        let queue_category = match queue_size {
            0..=10 => "low",
            11..=50 => "medium", 
            51..=100 => "high",
            _ => "very_high",
        };
        
        self.concurrent_worker_utilization
            .with_label_values(&["queue_utilization", queue_category, "false"])
            .set(queue_size as i64);
    }

    /// Record task coordination metrics
    pub fn record_task_coordination_metrics(&self, task_type: &str, completed_tasks: u64, failed_tasks: u64, active_tasks: usize) {
        // Record task success rate using appropriate labels
        let total_tasks = completed_tasks + failed_tasks;
        let success_rate = if total_tasks == 0 { 1.0 } else { completed_tasks as f64 / total_tasks as f64 };
        
        // Use work_stealing_success_rate with correct labels (worker_pool, steal_strategy)
        self.work_stealing_success_rate
            .with_label_values(&[task_type, "coordination"])
            .set((success_rate * 100.0) as i64);
            
        // Record active task count using correct labels (pool_id, worker_type, scaling_enabled)
        self.concurrent_worker_utilization
            .with_label_values(&["coordination_pool", task_type, "false"])
            .set(active_tasks as i64);
            
        // Record task completion metrics
        self.lock_free_operations_total
            .with_label_values(&["task_completed", task_type, "success"])
            .inc_by(completed_tasks);
            
        if failed_tasks > 0 {
            self.lock_free_operations_total
                .with_label_values(&["task_failed", task_type, "failure"])
                .inc_by(failed_tasks);
        }
    }

    /// Record load balancer metrics  
    pub fn record_load_balancer_metrics(&self, strategy: &str, selected_pool: usize, pool_loads: &[u64]) {
        // Record selected pool
        self.concurrent_worker_utilization
            .with_label_values(&["selected_pool", strategy, "false"])
            .set(selected_pool as i64);
            
        // Record load distribution variance
        if !pool_loads.is_empty() {
            let avg_load = pool_loads.iter().sum::<u64>() as f64 / pool_loads.len() as f64;
            let variance = pool_loads.iter()
                .map(|&load| {
                    let diff = load as f64 - avg_load;
                    diff * diff
                })
                .sum::<f64>() / pool_loads.len() as f64;
                
            self.work_stealing_success_rate
                .with_label_values(&[&format!("load_variance_{}", strategy), "variance"])
                .set((variance * 100.0) as i64);
        }
    }

    /// Record optimized actor performance metrics
    pub fn record_optimized_actor_metrics(&self, actor_type: &str, messages_per_sec: f64, avg_batch_size: u64, mailbox_overflows: u64) {
        // Record throughput (increment by messages processed)
        self.pipeline_throughput
            .with_label_values(&[actor_type, "messages_per_second"])
            .inc_by(messages_per_sec as u64);
            
        // Record batch efficiency
        self.batch_processing_efficiency
            .with_label_values(&[actor_type, "average_batch_size"])
            .set(avg_batch_size as i64);
            
        // Record mailbox overflow rate
        if mailbox_overflows > 0 {
            self.lock_free_operations_total
                .with_label_values(&["mailbox_overflow", actor_type, "failure"])
                .inc_by(mailbox_overflows);
        }
    }

    /// Record lock-free data structure performance
    pub fn record_lock_free_buffer_metrics(&self, buffer_type: &str, capacity: usize, current_size: usize, operation_success_rate: f64) {
        // Record buffer utilization
        let utilization = current_size as f64 / capacity as f64;
        self.concurrent_worker_utilization
            .with_label_values(&["buffer_utilization", buffer_type, "false"])
            .set((utilization * 100.0) as i64);
            
        // Record operation success rate
        self.work_stealing_success_rate
            .with_label_values(&[&format!("buffer_{}", buffer_type), "buffer_operation"])
            .set((operation_success_rate * 100.0) as i64);
            
        // Record buffer efficiency category
        let efficiency_category = match (utilization * 100.0) as u8 {
            0..=25 => "underutilized",
            26..=75 => "optimal",
            76..=90 => "high_usage",
            _ => "overloaded",
        };
        
        self.lock_free_operations_total
            .with_label_values(&["buffer_efficiency", buffer_type, efficiency_category])
            .inc();
    }

    /// Record adaptive worker pool scaling metrics
    pub fn record_worker_pool_scaling(&self, pool_id: usize, old_size: usize, new_size: usize, trigger_reason: &str) {
        let scale_direction = if new_size > old_size { "scale_up" } else { "scale_down" };
        let scale_amount = (new_size as i64 - old_size as i64).unsigned_abs();
        
        // Record scaling event
        self.lock_free_operations_total
            .with_label_values(&["worker_pool_scaling", scale_direction, trigger_reason])
            .inc_by(scale_amount);
            
        // Record current pool size
        self.concurrent_worker_utilization
            .with_label_values(&["worker_pool_size", &pool_id.to_string(), "true"])
            .set(new_size as i64);
    }

    /// Record concurrency performance summary
    pub fn record_concurrency_performance_summary(&self, total_operations: u64, concurrent_efficiency: f64, resource_utilization: f64) {
        // Record total concurrent operations
        self.lock_free_operations_total
            .with_label_values(&["total_concurrent_ops", "all", "completed"])
            .inc_by(total_operations);
            
        // Record overall concurrency efficiency
        self.work_stealing_success_rate
            .with_label_values(&["overall_concurrency", "efficiency"])
            .set((concurrent_efficiency * 100.0) as i64);
            
        // Record resource utilization
        self.concurrent_worker_utilization
            .with_label_values(&["resource_utilization", "overall", "false"])
            .set((resource_utilization * 100.0) as i64);
    }
    pub fn record_work_stealing_success_rate(&self, worker_pool: &str, steal_strategy: &str, success_rate: f64) {
        self.work_stealing_success_rate
            .with_label_values(&[worker_pool, steal_strategy])
            .set((success_rate * 100.0) as i64);
    }

    /// Record concurrent worker utilization
    pub fn record_concurrent_worker_utilization(&self, pool_id: &str, worker_type: &str, scaling_enabled: bool, utilization: f64) {
        let scaling_str = if scaling_enabled { "enabled" } else { "disabled" };
        self.concurrent_worker_utilization
            .with_label_values(&[pool_id, worker_type, scaling_str])
            .set((utilization * 100.0) as i64);
    }

    /// Record processing optimization metrics for batch processing and caching
    pub fn record_processing_optimization_metrics(&self, component: &str, optimization_type: &str, efficiency: f64, hit_rate: f64) {
        // Record batch processing efficiency
        self.batch_processing_efficiency
            .with_label_values(&[component, optimization_type])
            .set((efficiency * 100.0) as i64);
        
        // Record cache hit rate
        self.cache_hit_rate
            .with_label_values(&[component, optimization_type])
            .set((hit_rate * 100.0) as i64);
    }

    /// Record network optimization metrics for connection pooling, compression, and protocol efficiency
    pub fn record_network_optimization_metrics(&self, pool_type: &str, compression_enabled: bool, pool_hit_rate: f64, pool_utilization: f64, compression_ratio: f64, latency_reduction: f64) {
        let compression_str = if compression_enabled { "enabled" } else { "disabled" };
        
        // Record connection pool hit rate
        self.connection_pool_hit_rate
            .with_label_values(&[pool_type, compression_str])
            .set((pool_hit_rate * 100.0) as i64);
        
        // Record connection pool utilization
        self.connection_pool_utilization
            .with_label_values(&[pool_type, compression_str])
            .set((pool_utilization * 100.0) as i64);
        
        // Record payload compression ratio
        self.payload_compression_ratio
            .with_label_values(&[pool_type, compression_str])
            .set((compression_ratio * 100.0) as i64);
        
        // Record protocol batch efficiency (latency reduction from batching)
        self.protocol_batch_efficiency
            .with_label_values(&[pool_type, compression_str])
            .set((latency_reduction * 100.0) as i64);
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new().expect("Failed to create metrics registry")
    }
}

/// Global metrics instance
static METRICS: std::sync::OnceLock<Arc<MetricsRegistry>> = std::sync::OnceLock::new();

/// Initialize global metrics registry
pub fn init_metrics() -> Result<Arc<MetricsRegistry>, prometheus::Error> {
    let metrics = Arc::new(MetricsRegistry::new()?);
    METRICS.set(metrics.clone()).map_err(|_| {
        prometheus::Error::Msg("Metrics already initialized".to_string())
    })?;
    
    info!("Global metrics registry initialized");
    Ok(metrics)
}

/// Get global metrics instance
pub fn get_metrics() -> Option<Arc<MetricsRegistry>> {
    METRICS.get().cloned()
}

/// Convenience macros for metrics recording
#[macro_export]
macro_rules! record_indicator_timing {
    ($symbol:expr, $operation:expr, $duration:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_indicator_processing($symbol, $operation, $duration);
        }
    };
}

#[macro_export]
macro_rules! record_memory_pool_hit {
    ($pool_type:expr, $component:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_memory_pool_usage($pool_type, $component);
        }
    };
}

#[macro_export]
macro_rules! record_simd_op {
    ($operation:expr, $vector_size:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_simd_operation($operation, $vector_size);
        }
    };
}

#[macro_export]
macro_rules! record_cpu_usage {
    ($component:expr, $thread:expr, $cpu_percent:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_cpu_usage($component, $thread, $cpu_percent);
        }
    };
}

#[macro_export]
macro_rules! record_operation_duration {
    ($component:expr, $operation:expr, $duration_seconds:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_operation_duration($component, $operation, $duration_seconds);
        }
    };
}

#[macro_export]
macro_rules! record_resource_utilization {
    ($resource_type:expr, $environment:expr, $utilization_percent:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_system_resource_utilization($resource_type, $environment, $utilization_percent);
        }
    };
}

#[macro_export]
macro_rules! record_config_effectiveness {
    ($config_type:expr, $resource_profile:expr, $effectiveness_score:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_adaptive_config_effectiveness($config_type, $resource_profile, $effectiveness_score);
        }
    };
}

#[macro_export]
macro_rules! record_resource_pattern {
    ($pattern_type:expr, $severity:expr, $environment:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_resource_pattern_detection($pattern_type, $severity, $environment);
        }
    };
}

#[macro_export]
macro_rules! record_config_adaptation {
    ($adaptation_type:expr, $trigger_cause:expr, $success:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_configuration_adaptation($adaptation_type, $trigger_cause, $success);
        }
    };
}

#[macro_export]
macro_rules! record_threshold_breach {
    ($threshold_type:expr, $breach_level:expr, $recovery_action:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_resource_threshold_breach($threshold_type, $breach_level, $recovery_action);
        }
    };
}

#[macro_export]
macro_rules! record_connection_pool_metrics {
    ($pool_type:expr, $endpoint:expr, $hit_rate:expr, $utilization:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_connection_pool_metrics($pool_type, $endpoint, $hit_rate, $utilization);
        }
    };
}

#[macro_export]
macro_rules! record_network_optimization {
    ($pool_type:expr, $compression_enabled:expr, $hit_rate:expr, $utilization:expr, $compression_ratio:expr, $latency_reduction:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_network_optimization_metrics($pool_type, $compression_enabled, $hit_rate, $utilization, $compression_ratio, $latency_reduction);
        }
    };
}

#[macro_export]
macro_rules! record_payload_compression {
    ($compression_type:expr, $data_category:expr, $compression_ratio:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_payload_compression($compression_type, $data_category, $compression_ratio);
        }
    };
}

#[macro_export]
macro_rules! record_protocol_batch_efficiency {
    ($protocol_type:expr, $message_category:expr, $efficiency:expr) => {
        if let Some(metrics) = $crate::metrics::get_metrics() {
            metrics.record_protocol_batch_efficiency($protocol_type, $message_category, $efficiency);
        }
    };
}