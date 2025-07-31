//! Tokio runtime metrics integration with Prometheus
//! 
//! This module provides integration between tokio-metrics and Prometheus,
//! exposing Tokio runtime metrics for monitoring and observability.

use prometheus::{Gauge, Counter, Registry};
use std::time::Duration;
use tokio::time::interval;
use tokio_metrics::TaskMonitor;
use tracing::{info, warn};

/// Tokio metrics collector that integrates with Prometheus
pub struct TokioMetricsCollector {
    task_monitor: TaskMonitor,
    prometheus_metrics: PrometheusMetrics,
    collection_interval: Duration,
}

/// Prometheus metrics for Tokio task data
struct PrometheusMetrics {
    // Task metrics
    instrumented_count: Gauge,
    dropped_count: Counter,
    first_poll_count: Counter,
    total_first_poll_delay_seconds: Counter,
    total_idled_count: Counter,
    total_scheduled_count: Counter,
    total_scheduled_duration_seconds: Counter,
    total_slow_poll_count: Counter,
    total_slow_poll_duration_seconds: Counter,
}

impl TokioMetricsCollector {
    /// Create a new tokio metrics collector
    pub fn new(registry: &Registry, collection_interval: Duration) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let task_monitor = TaskMonitor::new();
        let prometheus_metrics = PrometheusMetrics::new(registry)?;
        
        Ok(Self {
            task_monitor,
            prometheus_metrics,
            collection_interval,
        })
    }
    
    /// Start the metrics collection loop
    pub fn start_collection(&self) {
        let task_monitor = self.task_monitor.clone();
        let prometheus_metrics = self.prometheus_metrics.clone();
        let collection_interval = self.collection_interval;
        
        tokio::spawn(async move {
            let mut interval = interval(collection_interval);
            info!("🔍 Starting tokio task metrics collection every {:?}", collection_interval);
            
            loop {
                interval.tick().await;
                
                // Collect task metrics
                if let Err(e) = Self::collect_task_metrics(&task_monitor, &prometheus_metrics).await {
                    warn!("Failed to collect task metrics: {}", e);
                }
            }
        });
    }
    
    /// Collect task metrics from tokio monitor
    async fn collect_task_metrics(
        monitor: &TaskMonitor,
        metrics: &PrometheusMetrics,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let task_metrics = monitor.intervals();
        
        for interval in task_metrics {
            // Task count metrics
            metrics.instrumented_count.set(interval.instrumented_count as f64);
            metrics.dropped_count.inc_by(interval.dropped_count as f64);
            metrics.first_poll_count.inc_by(interval.first_poll_count as f64);
            
            // Task timing metrics
            let first_poll_delay_seconds = interval.total_first_poll_delay.as_secs_f64();
            let scheduled_duration_seconds = interval.total_scheduled_duration.as_secs_f64();
            let slow_poll_duration_seconds = interval.total_slow_poll_duration.as_secs_f64();
            
            metrics.total_first_poll_delay_seconds.inc_by(first_poll_delay_seconds);
            metrics.total_scheduled_duration_seconds.inc_by(scheduled_duration_seconds);
            metrics.total_slow_poll_duration_seconds.inc_by(slow_poll_duration_seconds);
            
            // Task activity metrics
            metrics.total_idled_count.inc_by(interval.total_idled_count as f64);
            metrics.total_scheduled_count.inc_by(interval.total_scheduled_count as f64);
            metrics.total_slow_poll_count.inc_by(interval.total_slow_poll_count as f64);
        }
        
        Ok(())
    }
    
    /// Get the task monitor for instrumenting new tasks
    pub fn task_monitor(&self) -> &TaskMonitor {
        &self.task_monitor
    }
}

impl PrometheusMetrics {
    fn new(registry: &Registry) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let metrics = Self {
            // Task metrics
            instrumented_count: Gauge::new("tokio_task_instrumented_count", "Number of tasks instrumented for metrics")?,
            dropped_count: Counter::new("tokio_task_dropped_count", "Number of tasks dropped")?,
            first_poll_count: Counter::new("tokio_task_first_poll_count", "Number of first polls")?,
            total_first_poll_delay_seconds: Counter::new("tokio_task_total_first_poll_delay_seconds", "Total delay before first poll in seconds")?,
            total_idled_count: Counter::new("tokio_task_total_idled_count", "Total times tasks went idle")?,
            total_scheduled_count: Counter::new("tokio_task_total_scheduled_count", "Total times tasks were scheduled")?,
            total_scheduled_duration_seconds: Counter::new("tokio_task_total_scheduled_duration_seconds", "Total time tasks spent scheduled in seconds")?,
            total_slow_poll_count: Counter::new("tokio_task_total_slow_poll_count", "Total number of slow polls")?,
            total_slow_poll_duration_seconds: Counter::new("tokio_task_total_slow_poll_duration_seconds", "Total duration of slow polls in seconds")?,
        };
        
        // Register task metrics with Prometheus registry
        registry.register(Box::new(metrics.instrumented_count.clone()))?;
        registry.register(Box::new(metrics.dropped_count.clone()))?;
        registry.register(Box::new(metrics.first_poll_count.clone()))?;
        registry.register(Box::new(metrics.total_first_poll_delay_seconds.clone()))?;
        registry.register(Box::new(metrics.total_idled_count.clone()))?;
        registry.register(Box::new(metrics.total_scheduled_count.clone()))?;
        registry.register(Box::new(metrics.total_scheduled_duration_seconds.clone()))?;
        registry.register(Box::new(metrics.total_slow_poll_count.clone()))?;
        registry.register(Box::new(metrics.total_slow_poll_duration_seconds.clone()))?;
        
        Ok(metrics)
    }
}

impl Clone for PrometheusMetrics {
    fn clone(&self) -> Self {
        Self {
            // Task metrics
            instrumented_count: self.instrumented_count.clone(),
            dropped_count: self.dropped_count.clone(),
            first_poll_count: self.first_poll_count.clone(),
            total_first_poll_delay_seconds: self.total_first_poll_delay_seconds.clone(),
            total_idled_count: self.total_idled_count.clone(),
            total_scheduled_count: self.total_scheduled_count.clone(),
            total_scheduled_duration_seconds: self.total_scheduled_duration_seconds.clone(),
            total_slow_poll_count: self.total_slow_poll_count.clone(),
            total_slow_poll_duration_seconds: self.total_slow_poll_duration_seconds.clone(),
        }
    }
}

/// Initialize tokio metrics collection with Prometheus integration
pub fn initialize_tokio_metrics(
    registry: &Registry,
    collection_interval: Option<Duration>,
) -> Result<TokioMetricsCollector, Box<dyn std::error::Error + Send + Sync>> {
    let interval = collection_interval.unwrap_or_else(|| Duration::from_secs(30));
    
    info!("🔍 Initializing tokio metrics collection with {:?} interval", interval);
    
    let collector = TokioMetricsCollector::new(registry, interval)?;
    collector.start_collection();
    
    info!("✅ Tokio metrics collector initialized and started");
    
    Ok(collector)
}