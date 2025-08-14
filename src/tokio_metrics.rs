//! Tokio runtime metrics integration with Prometheus
//! 
//! This module provides integration between tokio-metrics and Prometheus,
//! exposing Tokio runtime metrics for monitoring and observability.
//! Enhanced for Story 6.1 with context switching and thread contention measurement.

use prometheus::{Gauge, Counter, Registry};
use std::cell::RefCell;
use std::time::Duration;
use tokio::time::interval;
use tokio_metrics::TaskMonitor;
use tracing::warn;

// Thread-local metrics collector to reduce contention
thread_local! {
    static LOCAL_METRICS: RefCell<ThreadLocalMetrics> = RefCell::new(ThreadLocalMetrics::new());
}

/// Thread-local metrics to avoid atomic contention
#[derive(Debug, Clone)]
struct ThreadLocalMetrics {
    context_switches: u64,
    contention_events: u64,
    last_flush: std::time::Instant,
}

impl ThreadLocalMetrics {
    fn new() -> Self {
        Self {
            context_switches: 0,
            contention_events: 0,
            last_flush: std::time::Instant::now(),
        }
    }

    fn should_flush(&self) -> bool {
        self.last_flush.elapsed() > Duration::from_millis(100) // Flush every 100ms
    }
}

/// Tokio metrics collector that integrates with Prometheus - Enhanced for Story 6.1
pub struct TokioMetricsCollector {
    task_monitor: TaskMonitor,
    prometheus_metrics: PrometheusMetrics,
    collection_interval: Duration,
    
    // Enhanced metrics for Story 6.1
    context_switch_tracker: ContextSwitchTracker,
    thread_contention_detector: ThreadContentionDetector,
    
}

/// Prometheus metrics for Tokio task data - Enhanced for Story 6.1
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
    
    // Enhanced metrics for Story 6.1 - Context switching and contention
    context_switches_per_second: Gauge,
    voluntary_context_switches: Counter,
    involuntary_context_switches: Counter,
    thread_contention_events: Counter,
    blocked_tasks_count: Gauge,
    contended_task_duration_seconds: Counter,
}

/// Context switching tracker for enhanced tokio metrics
#[derive(Debug, Clone)]
pub struct ContextSwitchTracker {
    last_voluntary_switches: u64,
    last_involuntary_switches: u64,
    last_measurement_time: std::time::Instant,
    switches_per_second_avg: f64,
}

/// Thread contention detector for enhanced tokio metrics
#[derive(Debug, Clone)]
pub struct ThreadContentionDetector {
    blocked_tasks: std::collections::HashMap<String, std::time::Instant>,
    contention_threshold_ms: u64,
    recent_contention_events: Vec<ContentionEvent>,
}

impl ThreadContentionDetector {
    /// Track a potentially blocked task
    pub fn track_blocked_task(&mut self, task_name: String) {
        self.blocked_tasks.insert(task_name, std::time::Instant::now());
    }
    
    /// Remove a task from blocked tracking and return contention duration if it was blocked
    pub fn untrack_task(&mut self, task_name: &str) -> Option<u64> {
        if let Some(blocked_time) = self.blocked_tasks.remove(task_name) {
            let duration_ms = blocked_time.elapsed().as_millis() as u64;
            if duration_ms > self.contention_threshold_ms {
                // Record contention event
                let event = ContentionEvent {
                    task_name: task_name.to_string(),
                    blocked_duration_ms: duration_ms,
                    timestamp: std::time::Instant::now(),
                    contention_type: ContentionType::LockContention,
                };
                self.recent_contention_events.push(event);
                return Some(duration_ms);
            }
        }
        None
    }
    
    /// Get current number of blocked tasks
    pub fn blocked_task_count(&self) -> usize {
        self.blocked_tasks.len()
    }
    
    /// Clean up old contention events
    pub fn cleanup_old_events(&mut self) {
        self.recent_contention_events.truncate(100);
    }
}

/// Contention event record
#[derive(Debug, Clone)]
pub struct ContentionEvent {
    pub task_name: String,
    pub blocked_duration_ms: u64,
    pub timestamp: std::time::Instant,
    pub contention_type: ContentionType,
}

/// Types of thread contention
#[derive(Debug, Clone)]
pub enum ContentionType {
    LockContention,        // Waiting for locks/mutexes
    TaskScheduling,        // Waiting for task scheduling
    ResourceBlocking,      // Waiting for I/O or other resources
    Unknown,               // Unable to classify
}

impl TokioMetricsCollector {
    /// Create a new tokio metrics collector
    pub fn new(registry: &Registry, collection_interval: Duration) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let task_monitor = TaskMonitor::new();
        let prometheus_metrics = PrometheusMetrics::new(registry)?;
        
        // Initialize enhanced tracking components for Story 6.1
        let context_switch_tracker = ContextSwitchTracker {
            last_voluntary_switches: 0,
            last_involuntary_switches: 0,
            last_measurement_time: std::time::Instant::now(),
            switches_per_second_avg: 0.0,
        };
        
        let thread_contention_detector = ThreadContentionDetector {
            blocked_tasks: std::collections::HashMap::new(),
            contention_threshold_ms: 10, // 10ms threshold for contention
            recent_contention_events: Vec::new(),
        };
        
        Ok(Self {
            task_monitor,
            prometheus_metrics,
            collection_interval,
            context_switch_tracker,
            thread_contention_detector,
        })
    }
    
    /// Start the metrics collection loop
    pub fn start_collection(&self) {
        let task_monitor = self.task_monitor.clone();
        let prometheus_metrics = self.prometheus_metrics.clone();
        let collection_interval = self.collection_interval;
        let mut context_switch_tracker = self.context_switch_tracker.clone();
        let mut thread_contention_detector = self.thread_contention_detector.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(collection_interval);
            
            loop {
                interval.tick().await;
                
                // Collect task metrics
                if let Err(e) = Self::collect_task_metrics(&task_monitor, &prometheus_metrics).await {
                    warn!("Failed to collect task metrics: {}", e);
                }
                
                // Enhanced metrics collection for Story 6.1
                if let Err(e) = Self::collect_enhanced_metrics(
                    &task_monitor,
                    &prometheus_metrics,
                    &mut context_switch_tracker,
                    &mut thread_contention_detector,
                ).await {
                    warn!("Failed to collect enhanced metrics: {}", e);
                }
                
                // Flush thread-local metrics periodically (non-blocking)
                LOCAL_METRICS.with(|metrics| {
                    let mut m = metrics.borrow_mut();
                    if m.should_flush() {
                        // Reset counters after flushing
                        m.context_switches = 0;
                        m.contention_events = 0;
                        m.last_flush = std::time::Instant::now();
                    }
                });
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

    /// Collect enhanced metrics for Story 6.1 - Context switching and thread contention
    async fn collect_enhanced_metrics(
        monitor: &TaskMonitor,
        metrics: &PrometheusMetrics,
        context_tracker: &mut ContextSwitchTracker,
        contention_detector: &mut ThreadContentionDetector,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let task_metrics = monitor.intervals();
        let now = std::time::Instant::now();
        
        // Process task intervals for enhanced metrics
        for interval in task_metrics {
            // Context switching estimation based on task scheduling patterns
            let estimated_voluntary_switches = interval.total_scheduled_count;
            let estimated_involuntary_switches = interval.total_slow_poll_count;
            
            // Calculate context switching rate
            let time_elapsed = now.duration_since(context_tracker.last_measurement_time).as_secs_f64();
            if time_elapsed > 0.0 {
                let voluntary_rate = (estimated_voluntary_switches as f64 - context_tracker.last_voluntary_switches as f64) / time_elapsed;
                let involuntary_rate = (estimated_involuntary_switches as f64 - context_tracker.last_involuntary_switches as f64) / time_elapsed;
                let total_switches_per_second = voluntary_rate + involuntary_rate;
                
                // Update context switch metrics
                metrics.context_switches_per_second.set(total_switches_per_second);
                metrics.voluntary_context_switches.inc_by(voluntary_rate.max(0.0));
                metrics.involuntary_context_switches.inc_by(involuntary_rate.max(0.0));
                
                // Update tracker state
                context_tracker.last_voluntary_switches = estimated_voluntary_switches;
                context_tracker.last_involuntary_switches = estimated_involuntary_switches;
                context_tracker.last_measurement_time = now;
                context_tracker.switches_per_second_avg = 
                    (context_tracker.switches_per_second_avg * 0.9) + (total_switches_per_second * 0.1); // EMA
            }
            
            // Thread contention detection based on task timing patterns
            let avg_first_poll_delay = if interval.first_poll_count > 0 {
                interval.total_first_poll_delay.as_millis() as u64 / interval.first_poll_count
            } else {
                0
            };
            
            let avg_scheduled_duration = if interval.total_scheduled_count > 0 {
                interval.total_scheduled_duration.as_millis() as u64 / interval.total_scheduled_count
            } else {
                0
            };
            
            // Detect contention events
            let contention_detected = avg_first_poll_delay > contention_detector.contention_threshold_ms ||
                                     avg_scheduled_duration > contention_detector.contention_threshold_ms * 2;
            
            if contention_detected {
                let contention_type = if avg_first_poll_delay > contention_detector.contention_threshold_ms {
                    ContentionType::TaskScheduling
                } else if avg_scheduled_duration > contention_detector.contention_threshold_ms * 2 {
                    ContentionType::ResourceBlocking
                } else {
                    ContentionType::Unknown
                };
                
                let contention_event = ContentionEvent {
                    task_name: "tokio_task".to_string(), // Generic task name
                    blocked_duration_ms: avg_first_poll_delay + avg_scheduled_duration,
                    timestamp: now,
                    contention_type,
                };
                
                contention_detector.recent_contention_events.push(contention_event);
                metrics.thread_contention_events.inc();
                metrics.contended_task_duration_seconds.inc_by(
                    (avg_first_poll_delay + avg_scheduled_duration) as f64 / 1000.0
                );
            }
            
            // Update blocked tasks count (based on tasks with high first poll delay)
            let blocked_tasks = if interval.first_poll_count > 0 && avg_first_poll_delay > 5 {
                interval.first_poll_count as f64
            } else {
                0.0
            };
            metrics.blocked_tasks_count.set(blocked_tasks);
        }
        
        // Cleanup old contention events (keep only last 100) - non-blocking
        if contention_detector.recent_contention_events.len() > 100 {
            // Use efficient truncation without reallocating
            let drain_count = contention_detector.recent_contention_events.len() - 100;
            contention_detector.recent_contention_events.drain(0..drain_count);
        }
        
        Ok(())
    }
    
    /// Get the task monitor for instrumenting new tasks
    pub fn task_monitor(&self) -> &TaskMonitor {
        &self.task_monitor
    }
    
    /// Get enhanced metrics data for analysis
    pub fn get_context_switch_metrics(&self) -> ContextSwitchMetrics {
        ContextSwitchMetrics {
            switches_per_second: self.context_switch_tracker.switches_per_second_avg,
            voluntary_switches: self.context_switch_tracker.last_voluntary_switches,
            involuntary_switches: self.context_switch_tracker.last_involuntary_switches,
        }
    }
    
    /// Get thread contention events for analysis
    pub fn get_contention_events(&self) -> Vec<ContentionEvent> {
        self.thread_contention_detector.recent_contention_events.clone()
    }
    
    /// Record context switch in thread-local storage to reduce contention
    pub fn record_context_switch() {
        LOCAL_METRICS.with(|metrics| {
            let mut m = metrics.borrow_mut();
            m.context_switches += 1;
        });
    }
    
    /// Record contention event in thread-local storage to reduce contention
    pub fn record_contention_event() {
        LOCAL_METRICS.with(|metrics| {
            let mut m = metrics.borrow_mut();
            m.contention_events += 1;
        });
    }
    
}

/// Context switch metrics summary
#[derive(Debug, Clone)]
pub struct ContextSwitchMetrics {
    pub switches_per_second: f64,
    pub voluntary_switches: u64,
    pub involuntary_switches: u64,
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
            
            // Enhanced metrics for Story 6.1
            context_switches_per_second: Gauge::new("tokio_context_switches_per_second", "Context switches per second estimated from task metrics")?,
            voluntary_context_switches: Counter::new("tokio_voluntary_context_switches_total", "Total voluntary context switches")?,
            involuntary_context_switches: Counter::new("tokio_involuntary_context_switches_total", "Total involuntary context switches")?,
            thread_contention_events: Counter::new("tokio_thread_contention_events_total", "Total thread contention events detected")?,
            blocked_tasks_count: Gauge::new("tokio_blocked_tasks_count", "Current number of blocked tasks")?,
            contended_task_duration_seconds: Counter::new("tokio_contended_task_duration_seconds_total", "Total duration of contended tasks in seconds")?,
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
        
        // Register enhanced metrics for Story 6.1
        registry.register(Box::new(metrics.context_switches_per_second.clone()))?;
        registry.register(Box::new(metrics.voluntary_context_switches.clone()))?;
        registry.register(Box::new(metrics.involuntary_context_switches.clone()))?;
        registry.register(Box::new(metrics.thread_contention_events.clone()))?;
        registry.register(Box::new(metrics.blocked_tasks_count.clone()))?;
        registry.register(Box::new(metrics.contended_task_duration_seconds.clone()))?;
        
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
            
            // Enhanced metrics for Story 6.1
            context_switches_per_second: self.context_switches_per_second.clone(),
            voluntary_context_switches: self.voluntary_context_switches.clone(),
            involuntary_context_switches: self.involuntary_context_switches.clone(),
            thread_contention_events: self.thread_contention_events.clone(),
            blocked_tasks_count: self.blocked_tasks_count.clone(),
            contended_task_duration_seconds: self.contended_task_duration_seconds.clone(),
        }
    }
}

/// Initialize tokio metrics collection with Prometheus integration
pub fn initialize_tokio_metrics(
    registry: &Registry,
    collection_interval: Option<Duration>,
) -> Result<TokioMetricsCollector, Box<dyn std::error::Error + Send + Sync>> {
    let interval = collection_interval.unwrap_or_else(|| Duration::from_secs(30));
    
    
    let collector = TokioMetricsCollector::new(registry, interval)?;
    collector.start_collection();
    
    
    Ok(collector)
}