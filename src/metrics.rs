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
use tracing::info;
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
            info!("Prometheus metrics registry initialized with {} business metrics + tokio task metrics", 27);
            27 + 9 // 27 business metrics + 9 tokio task metrics
        } else {
            info!("Prometheus metrics registry initialized with {} business metrics (tokio metrics disabled)", 27);
            27
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