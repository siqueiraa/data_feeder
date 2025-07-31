use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};
use serde::{Serialize, Deserialize};

/// Queue performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMetricsData {
    pub queue_name: String,
    pub tasks_submitted: u64,
    pub tasks_queued: u64,
    pub tasks_started: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub tasks_timeout: u64,
    pub total_wait_time_ms: u64,
    pub total_execution_time_ms: u64,
    pub average_wait_time_ms: f64,
    pub average_execution_time_ms: f64,
    pub current_throughput_per_sec: f64,
    pub peak_throughput_per_sec: f64,
    #[serde(skip)]
    pub last_activity: Option<Instant>,
}

impl QueueMetricsData {
    fn new(queue_name: String) -> Self {
        Self {
            queue_name,
            tasks_submitted: 0,
            tasks_queued: 0,
            tasks_started: 0,
            tasks_completed: 0,
            tasks_failed: 0,
            tasks_timeout: 0,
            total_wait_time_ms: 0,
            total_execution_time_ms: 0,
            average_wait_time_ms: 0.0,
            average_execution_time_ms: 0.0,
            current_throughput_per_sec: 0.0,
            peak_throughput_per_sec: 0.0,
            last_activity: None,
        }
    }

    fn update_averages(&mut self) {
        if self.tasks_completed > 0 {
            self.average_wait_time_ms = self.total_wait_time_ms as f64 / self.tasks_completed as f64;
            self.average_execution_time_ms = self.total_execution_time_ms as f64 / self.tasks_completed as f64;
        }
    }
}

/// Thread-safe queue metrics collector
#[derive(Debug, Clone)]
pub struct QueueMetrics {
    data: Arc<RwLock<QueueMetricsData>>,
    // Recent activity tracking for throughput calculation
    recent_completions: Arc<RwLock<Vec<Instant>>>,
    throughput_window: Duration,
    start_time: Instant,
}

impl QueueMetrics {
    /// Create new metrics collector
    pub fn new(queue_name: String) -> Self {
        Self {
            data: Arc::new(RwLock::new(QueueMetricsData::new(queue_name))),
            recent_completions: Arc::new(RwLock::new(Vec::new())),
            throughput_window: Duration::from_secs(60), // 1-minute window
            start_time: Instant::now(),
        }
    }

    /// Record task submission
    pub fn record_task_submitted(&self) {
        tokio::spawn({
            let data = self.data.clone();
            async move {
                let mut metrics = data.write().await;
                metrics.tasks_submitted += 1;
                metrics.last_activity = Some(Instant::now());
                debug!("📊 Task submitted (total: {})", metrics.tasks_submitted);
            }
        });
    }

    /// Record task queued
    pub fn record_task_queued(&self) {
        tokio::spawn({
            let data = self.data.clone();
            async move {
                let mut metrics = data.write().await;
                metrics.tasks_queued += 1;
                debug!("📊 Task queued (total: {})", metrics.tasks_queued);
            }
        });
    }

    /// Record task started
    pub fn record_task_started(&self) {
        tokio::spawn({
            let data = self.data.clone();
            async move {
                let mut metrics = data.write().await;
                metrics.tasks_started += 1;
                debug!("📊 Task started (total: {})", metrics.tasks_started);
            }
        });
    }

    /// Record task completion with timing
    pub fn record_task_completed(&self, execution_time: Duration) {
        let completion_time = Instant::now();
        
        tokio::spawn({
            let data = self.data.clone();
            let recent_completions = self.recent_completions.clone();
            let throughput_window = self.throughput_window;
            
            async move {
                // Update main metrics
                {
                    let mut metrics = data.write().await;
                    metrics.tasks_completed += 1;
                    metrics.total_execution_time_ms += execution_time.as_millis() as u64;
                    metrics.last_activity = Some(completion_time);
                    metrics.update_averages();
                    
                    debug!("📊 Task completed (total: {}, avg exec time: {:.2}ms)", 
                           metrics.tasks_completed, metrics.average_execution_time_ms);
                }
                
                // Update throughput tracking
                {
                    let mut completions = recent_completions.write().await;
                    completions.push(completion_time);
                    
                    // Clean old entries
                    let cutoff = completion_time - throughput_window;
                    completions.retain(|&time| time >= cutoff);
                    
                    // Calculate current throughput
                    let current_throughput = completions.len() as f64 / throughput_window.as_secs_f64();
                    
                    // Update metrics with new throughput
                    let mut metrics = data.write().await;
                    metrics.current_throughput_per_sec = current_throughput;
                    if current_throughput > metrics.peak_throughput_per_sec {
                        metrics.peak_throughput_per_sec = current_throughput;
                    }
                }
            }
        });
    }

    /// Record task failure
    pub fn record_task_error(&self) {
        tokio::spawn({
            let data = self.data.clone();
            async move {
                let mut metrics = data.write().await;
                metrics.tasks_failed += 1;
                metrics.last_activity = Some(Instant::now());
                debug!("📊 Task failed (total: {})", metrics.tasks_failed);
            }
        });
    }

    /// Record task timeout
    pub fn record_task_timeout(&self) {
        tokio::spawn({
            let data = self.data.clone();
            async move {
                let mut metrics = data.write().await;
                metrics.tasks_timeout += 1;
                metrics.last_activity = Some(Instant::now());
                debug!("📊 Task timeout (total: {})", metrics.tasks_timeout);
            }
        });
    }

    /// Record wait time for a task
    pub fn record_wait_time(&self, wait_time: Duration) {
        tokio::spawn({
            let data = self.data.clone();
            async move {
                let mut metrics = data.write().await;
                metrics.total_wait_time_ms += wait_time.as_millis() as u64;
                metrics.update_averages();
                debug!("📊 Wait time recorded: {:?} (avg: {:.2}ms)", 
                       wait_time, metrics.average_wait_time_ms);
            }
        });
    }

    /// Get current metrics snapshot
    pub async fn get_metrics(&self) -> QueueMetricsData {
        self.data.read().await.clone()
    }

    /// Get queue health status
    pub async fn get_health(&self) -> QueueHealth {
        let data = self.data.read().await;
        let uptime = self.start_time.elapsed();
        
        // Calculate success rate
        let total_processed = data.tasks_completed + data.tasks_failed + data.tasks_timeout;
        let success_rate = if total_processed > 0 {
            data.tasks_completed as f64 / total_processed as f64
        } else {
            1.0
        };

        // Determine health status
        let status = if success_rate >= 0.95 && data.current_throughput_per_sec > 0.0 {
            HealthStatus::Healthy
        } else if success_rate >= 0.80 {
            HealthStatus::Warning
        } else {
            HealthStatus::Critical
        };

        QueueHealth {
            status,
            uptime,
            success_rate,
            current_load: data.current_throughput_per_sec / data.peak_throughput_per_sec.max(1.0),
            queue_name: data.queue_name.clone(),
            last_activity: data.last_activity,
        }
    }

    /// Reset all metrics
    pub async fn reset(&self) {
        let queue_name = {
            let data = self.data.read().await;
            data.queue_name.clone()
        };
        
        *self.data.write().await = QueueMetricsData::new(queue_name);
        self.recent_completions.write().await.clear();
        
        info!("📊 Queue metrics reset");
    }

    /// Log current metrics summary
    pub async fn log_summary(&self) {
        let data = self.data.read().await;
        let health = self.get_health().await;
        
        info!("📊 Queue '{}' Summary:", data.queue_name);
        info!("  Status: {:?}", health.status);
        info!("  Uptime: {:?}", health.uptime);
        info!("  Success Rate: {:.1}%", health.success_rate * 100.0);
        info!("  Tasks: {} submitted, {} completed, {} failed", 
              data.tasks_submitted, data.tasks_completed, data.tasks_failed);
        info!("  Throughput: {:.2}/sec (peak: {:.2}/sec)", 
              data.current_throughput_per_sec, data.peak_throughput_per_sec);
        info!("  Avg Times: {:.2}ms wait, {:.2}ms execution", 
              data.average_wait_time_ms, data.average_execution_time_ms);
    }
}

/// Queue health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueHealth {
    pub status: HealthStatus,
    #[serde(skip)]
    pub uptime: Duration,
    pub success_rate: f64,
    pub current_load: f64,
    pub queue_name: String,
    #[serde(skip)]
    pub last_activity: Option<Instant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
}

/// Metrics aggregator for multiple queues
#[derive(Debug)]
pub struct MetricsAggregator {
    queues: Arc<RwLock<Vec<QueueMetrics>>>,
}

impl Default for MetricsAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsAggregator {
    pub fn new() -> Self {
        Self {
            queues: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a queue for monitoring
    pub async fn register_queue(&self, metrics: QueueMetrics) {
        let mut queues = self.queues.write().await;
        queues.push(metrics);
        info!("📊 Registered queue for metrics aggregation");
    }

    /// Get aggregated health status
    pub async fn get_overall_health(&self) -> OverallHealth {
        let queues = self.queues.read().await;
        let mut total_healthy = 0;
        let mut total_warning = 0;
        let mut total_critical = 0;
        let mut queue_healths = Vec::new();

        for queue_metrics in queues.iter() {
            let health = queue_metrics.get_health().await;
            match health.status {
                HealthStatus::Healthy => total_healthy += 1,
                HealthStatus::Warning => total_warning += 1,
                HealthStatus::Critical => total_critical += 1,
            }
            queue_healths.push(health);
        }

        let overall_status = if total_critical > 0 {
            HealthStatus::Critical
        } else if total_warning > 0 {
            HealthStatus::Warning
        } else {
            HealthStatus::Healthy
        };

        OverallHealth {
            status: overall_status,
            total_queues: queues.len(),
            healthy_queues: total_healthy,
            warning_queues: total_warning,
            critical_queues: total_critical,
            queue_healths,
        }
    }

    /// Log summary of all queues
    pub async fn log_all_summaries(&self) {
        let queues = self.queues.read().await;
        info!("📊 === Queue Metrics Summary ({} queues) ===", queues.len());
        
        for queue_metrics in queues.iter() {
            queue_metrics.log_summary().await;
        }
        
        let overall = self.get_overall_health().await;
        info!("📊 Overall Status: {:?} ({} healthy, {} warning, {} critical)", 
              overall.status, overall.healthy_queues, overall.warning_queues, overall.critical_queues);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverallHealth {
    pub status: HealthStatus,
    pub total_queues: usize,
    pub healthy_queues: usize,
    pub warning_queues: usize,
    pub critical_queues: usize,
    pub queue_healths: Vec<QueueHealth>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_basic_metrics() {
        let metrics = QueueMetrics::new("test_queue".to_string());
        
        metrics.record_task_submitted();
        metrics.record_task_queued();
        metrics.record_task_started();
        metrics.record_task_completed(Duration::from_millis(100));
        
        // Give async operations time to complete
        sleep(Duration::from_millis(10)).await;
        
        let data = metrics.get_metrics().await;
        assert_eq!(data.tasks_submitted, 1);
        assert_eq!(data.tasks_completed, 1);
        assert!(data.average_execution_time_ms > 0.0);
    }

    #[tokio::test]
    async fn test_health_calculation() {
        let metrics = QueueMetrics::new("test_queue".to_string());
        
        // Simulate successful tasks
        for _ in 0..10 {
            metrics.record_task_completed(Duration::from_millis(50));
        }
        
        sleep(Duration::from_millis(50)).await;
        
        let health = metrics.get_health().await;
        assert_eq!(health.success_rate, 1.0);
        assert!(matches!(health.status, HealthStatus::Warning | HealthStatus::Healthy));
    }

    #[tokio::test]
    async fn test_throughput_calculation() {
        let metrics = QueueMetrics::new("test_queue".to_string());
        
        // Simulate rapid completions
        for _ in 0..5 {
            metrics.record_task_completed(Duration::from_millis(10));
            sleep(Duration::from_millis(100)).await;
        }
        
        let data = metrics.get_metrics().await;
        assert!(data.current_throughput_per_sec > 0.0);
    }
}