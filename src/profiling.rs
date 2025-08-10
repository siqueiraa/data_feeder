//! CPU Profiling and Performance Analysis Module
//! 
//! This module provides comprehensive CPU profiling tools for identifying
//! performance bottlenecks and optimizing resource consumption.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use tracing::{info, debug};

/// Performance profile entry containing timing and CPU usage data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileEntry {
    pub component: String,
    pub operation: String,
    pub duration_nanos: u64,
    pub cpu_usage_percent: f64,
    pub memory_usage_bytes: u64,
    pub timestamp: u64,
    pub thread_id: String,
    pub call_count: u64,
}

/// Aggregated performance statistics for a component/operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    pub component: String,
    pub operation: String,
    pub total_calls: u64,
    pub total_duration_nanos: u64,
    pub avg_duration_nanos: u64,
    pub min_duration_nanos: u64,
    pub max_duration_nanos: u64,
    pub avg_cpu_percent: f64,
    pub max_cpu_percent: f64,
    pub total_memory_bytes: u64,
    pub avg_memory_bytes: u64,
}

/// CPU profiler for monitoring performance across application components
#[derive(Debug)]
pub struct CpuProfiler {
    profiles: Arc<Mutex<HashMap<String, Vec<ProfileEntry>>>>,
    stats: Arc<Mutex<HashMap<String, PerformanceStats>>>,
    enabled: bool,
}

impl CpuProfiler {
    /// Create a new CPU profiler instance
    pub fn new(enabled: bool) -> Self {
        Self {
            profiles: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(HashMap::new())),
            enabled,
        }
    }

    /// Start profiling a component operation
    pub fn start_profiling(&self, component: &str, operation: &str) -> ProfileGuard {
        if !self.enabled {
            return ProfileGuard::disabled();
        }

        ProfileGuard::new(
            component.to_string(),
            operation.to_string(),
            self.profiles.clone(),
            self.stats.clone(),
        )
    }

    /// Record a profile entry manually
    pub fn record_entry(&self, entry: ProfileEntry) {
        if !self.enabled {
            return;
        }

        let key = format!("{}::{}", entry.component, entry.operation);
        
        // Store individual entry
        {
            let mut profiles = self.profiles.lock().unwrap();
            profiles.entry(key.clone()).or_default().push(entry.clone());
        }

        // Update aggregated stats
        {
            let mut stats = self.stats.lock().unwrap();
            let stat = stats.entry(key).or_insert_with(|| PerformanceStats {
                component: entry.component.clone(),
                operation: entry.operation.clone(),
                total_calls: 0,
                total_duration_nanos: 0,
                avg_duration_nanos: 0,
                min_duration_nanos: u64::MAX,
                max_duration_nanos: 0,
                avg_cpu_percent: 0.0,
                max_cpu_percent: 0.0,
                total_memory_bytes: 0,
                avg_memory_bytes: 0,
            });

            stat.total_calls += 1;
            stat.total_duration_nanos += entry.duration_nanos;
            stat.avg_duration_nanos = stat.total_duration_nanos / stat.total_calls;
            stat.min_duration_nanos = stat.min_duration_nanos.min(entry.duration_nanos);
            stat.max_duration_nanos = stat.max_duration_nanos.max(entry.duration_nanos);
            
            // Update CPU stats
            stat.avg_cpu_percent = (stat.avg_cpu_percent * (stat.total_calls - 1) as f64 + entry.cpu_usage_percent) / stat.total_calls as f64;
            stat.max_cpu_percent = stat.max_cpu_percent.max(entry.cpu_usage_percent);
            
            // Update memory stats
            stat.total_memory_bytes += entry.memory_usage_bytes;
            stat.avg_memory_bytes = stat.total_memory_bytes / stat.total_calls;
        }
    }

    /// Get performance statistics for all components
    pub fn get_stats(&self) -> HashMap<String, PerformanceStats> {
        self.stats.lock().unwrap().clone()
    }

    /// Get top CPU consuming operations
    pub fn get_top_cpu_consumers(&self, limit: usize) -> Vec<PerformanceStats> {
        let stats = self.stats.lock().unwrap();
        let mut sorted_stats: Vec<_> = stats.values().cloned().collect();
        
        // Sort by average CPU usage descending
        sorted_stats.sort_by(|a, b| b.avg_cpu_percent.partial_cmp(&a.avg_cpu_percent).unwrap_or(std::cmp::Ordering::Equal));
        sorted_stats.truncate(limit);
        sorted_stats
    }

    /// Get slowest operations by duration
    pub fn get_slowest_operations(&self, limit: usize) -> Vec<PerformanceStats> {
        let stats = self.stats.lock().unwrap();
        let mut sorted_stats: Vec<_> = stats.values().cloned().collect();
        
        // Sort by average duration descending
        sorted_stats.sort_by(|a, b| b.avg_duration_nanos.cmp(&a.avg_duration_nanos));
        sorted_stats.truncate(limit);
        sorted_stats
    }

    /// Generate a performance report
    pub fn generate_report(&self) -> PerformanceReport {
        let stats = self.get_stats();
        let top_cpu = self.get_top_cpu_consumers(10);
        let slowest = self.get_slowest_operations(10);
        
        let total_operations: u64 = stats.values().map(|s| s.total_calls).sum();
        let avg_cpu_across_all: f64 = if !stats.is_empty() {
            stats.values().map(|s| s.avg_cpu_percent).sum::<f64>() / stats.len() as f64
        } else {
            0.0
        };

        PerformanceReport {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            total_components: stats.len(),
            total_operations,
            avg_cpu_percent: avg_cpu_across_all,
            top_cpu_consumers: top_cpu,
            slowest_operations: slowest,
            component_stats: stats,
        }
    }

    /// Clear all collected data
    pub fn clear(&self) {
        self.profiles.lock().unwrap().clear();
        self.stats.lock().unwrap().clear();
    }

    /// Export profiles to JSON
    pub fn export_to_json(&self) -> Result<String, serde_json::Error> {
        let report = self.generate_report();
        serde_json::to_string_pretty(&report)
    }
}

// Type aliases for complex types
type ProfileMap = Arc<Mutex<HashMap<String, Vec<ProfileEntry>>>>;
type StatsMap = Arc<Mutex<HashMap<String, PerformanceStats>>>;

/// RAII guard for automatic profiling of code blocks
pub struct ProfileGuard {
    component: Option<String>,
    operation: Option<String>,
    start_time: Option<Instant>,
    profiles: Option<ProfileMap>,
    stats: Option<StatsMap>,
}

impl ProfileGuard {
    fn new(
        component: String,
        operation: String,
        profiles: ProfileMap,
        stats: StatsMap,
    ) -> Self {
        Self {
            component: Some(component),
            operation: Some(operation),
            start_time: Some(Instant::now()),
            profiles: Some(profiles),
            stats: Some(stats),
        }
    }

    pub fn disabled() -> Self {
        Self {
            component: None,
            operation: None,
            start_time: None,
            profiles: None,
            stats: None,
        }
    }

    /// Record custom metrics during the profiling session
    pub fn record_metric(&self, metric_name: &str, value: f64) {
        if let (Some(component), Some(operation)) = (&self.component, &self.operation) {
            debug!("Custom metric for {}::{} - {}: {}", component, operation, metric_name, value);
        }
    }
}

impl Drop for ProfileGuard {
    fn drop(&mut self) {
        if let (Some(component), Some(operation), Some(start_time), Some(profiles), Some(stats)) = (
            &self.component,
            &self.operation,
            &self.start_time,
            &self.profiles,
            &self.stats,
        ) {
            let duration = start_time.elapsed();
            let cpu_usage = get_current_cpu_usage(); // Simplified - would need proper CPU monitoring
            let memory_usage = get_current_memory_usage(); // Simplified - would need proper memory monitoring
            
            let entry = ProfileEntry {
                component: component.clone(),
                operation: operation.clone(),
                duration_nanos: duration.as_nanos() as u64,
                cpu_usage_percent: cpu_usage,
                memory_usage_bytes: memory_usage,
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                thread_id: format!("{:?}", std::thread::current().id()),
                call_count: 1,
            };

            let key = format!("{}::{}", component, operation);
            
            // Store individual entry
            {
                let mut profiles_guard = profiles.lock().unwrap();
                profiles_guard.entry(key.clone()).or_default().push(entry.clone());
            }

            // Update aggregated stats
            {
                let mut stats_guard = stats.lock().unwrap();
                let stat = stats_guard.entry(key).or_insert_with(|| PerformanceStats {
                    component: component.clone(),
                    operation: operation.clone(),
                    total_calls: 0,
                    total_duration_nanos: 0,
                    avg_duration_nanos: 0,
                    min_duration_nanos: u64::MAX,
                    max_duration_nanos: 0,
                    avg_cpu_percent: 0.0,
                    max_cpu_percent: 0.0,
                    total_memory_bytes: 0,
                    avg_memory_bytes: 0,
                });

                stat.total_calls += 1;
                stat.total_duration_nanos += entry.duration_nanos;
                stat.avg_duration_nanos = stat.total_duration_nanos / stat.total_calls;
                stat.min_duration_nanos = stat.min_duration_nanos.min(entry.duration_nanos);
                stat.max_duration_nanos = stat.max_duration_nanos.max(entry.duration_nanos);
                
                // Update CPU stats
                stat.avg_cpu_percent = (stat.avg_cpu_percent * (stat.total_calls - 1) as f64 + entry.cpu_usage_percent) / stat.total_calls as f64;
                stat.max_cpu_percent = stat.max_cpu_percent.max(entry.cpu_usage_percent);
                
                // Update memory stats
                stat.total_memory_bytes += entry.memory_usage_bytes;
                stat.avg_memory_bytes = stat.total_memory_bytes / stat.total_calls;
            }
        }
    }
}

/// Complete performance report
#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub timestamp: u64,
    pub total_components: usize,
    pub total_operations: u64,
    pub avg_cpu_percent: f64,
    pub top_cpu_consumers: Vec<PerformanceStats>,
    pub slowest_operations: Vec<PerformanceStats>,
    pub component_stats: HashMap<String, PerformanceStats>,
}

/// Global profiler instance
static PROFILER: std::sync::OnceLock<Arc<CpuProfiler>> = std::sync::OnceLock::new();

/// Initialize the global profiler
pub fn init_profiler(enabled: bool) -> Arc<CpuProfiler> {
    let profiler = Arc::new(CpuProfiler::new(enabled));
    PROFILER.set(profiler.clone()).expect("Profiler already initialized");
    info!("CPU profiler initialized (enabled: {})", enabled);
    profiler
}

/// Get the global profiler instance
pub fn get_profiler() -> Option<Arc<CpuProfiler>> {
    PROFILER.get().cloned()
}

/// Macro for easy profiling of code blocks
#[macro_export]
macro_rules! profile {
    ($component:expr, $operation:expr, $code:block) => {{
        let _guard = if let Some(profiler) = $crate::profiling::get_profiler() {
            profiler.start_profiling($component, $operation)
        } else {
            $crate::profiling::ProfileGuard::disabled()
        };
        $code
    }};
}

/// Real CPU usage tracking using the performance monitoring system
fn get_current_cpu_usage() -> f64 {
    // Use the new performance monitoring system if available
    if let Some(monitor) = crate::performance::get_performance_monitor() {
        if let Some(snapshot) = monitor.get_current_snapshot() {
            return snapshot.process_cpu_percent as f64;
        }
    }
    
    // Fallback to direct sysinfo if performance monitor not available
    use sysinfo::{System, Pid};
    let mut system = System::new_all();
    system.refresh_processes();
    
    let pid = Pid::from(std::process::id() as usize);
    if let Some(process) = system.process(pid) {
        process.cpu_usage() as f64
    } else {
        0.0
    }
}

/// Real memory usage tracking using the performance monitoring system
fn get_current_memory_usage() -> u64 {
    // Use the new performance monitoring system if available
    if let Some(monitor) = crate::performance::get_performance_monitor() {
        if let Some(snapshot) = monitor.get_current_snapshot() {
            return (snapshot.memory_usage_mb * 1_048_576.0) as u64; // Convert MB to bytes
        }
    }
    
    // Fallback to direct sysinfo if performance monitor not available
    use sysinfo::{System, Pid};
    let mut system = System::new_all();
    system.refresh_processes();
    
    let pid = Pid::from(std::process::id() as usize);
    if let Some(process) = system.process(pid) {
        process.memory() * 1024 // Convert KB to bytes
    } else {
        0
    }
}

/// Enhanced profiling with system resource monitoring
pub struct SystemProfiler {
    cpu_profiler: Arc<CpuProfiler>,
}

impl SystemProfiler {
    pub fn new(enabled: bool) -> Self {
        Self {
            cpu_profiler: Arc::new(CpuProfiler::new(enabled)),
        }
    }

    pub fn profile_with_system_metrics(&self, component: &str, operation: &str) -> SystemProfileGuard {
        SystemProfileGuard::new(component, operation, &self.cpu_profiler)
    }
}

/// Enhanced profile guard with system metrics
pub struct SystemProfileGuard {
    _guard: ProfileGuard,
}

impl SystemProfileGuard {
    fn new(component: &str, operation: &str, profiler: &Arc<CpuProfiler>) -> Self {
        Self {
            _guard: profiler.start_profiling(component, operation),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_profiler_basic_functionality() {
        let profiler = CpuProfiler::new(true);
        
        // Test profiling a simple operation
        {
            let _guard = profiler.start_profiling("test_component", "test_operation");
            thread::sleep(Duration::from_millis(10));
        }
        
        let stats = profiler.get_stats();
        assert_eq!(stats.len(), 1);
        
        let key = "test_component::test_operation";
        assert!(stats.contains_key(key));
        
        let stat = &stats[key];
        assert_eq!(stat.total_calls, 1);
        assert!(stat.avg_duration_nanos > 0);
    }

    #[test]
    fn test_profiler_disabled() {
        let profiler = CpuProfiler::new(false);
        
        {
            let _guard = profiler.start_profiling("test_component", "test_operation");
            thread::sleep(Duration::from_millis(10));
        }
        
        let stats = profiler.get_stats();
        assert_eq!(stats.len(), 0);
    }

    #[test]
    fn test_performance_report_generation() {
        let profiler = CpuProfiler::new(true);
        
        // Generate some test data
        for i in 0..5 {
            let _guard = profiler.start_profiling("component1", &format!("operation_{}", i));
            thread::sleep(Duration::from_millis(1));
        }
        
        let report = profiler.generate_report();
        assert_eq!(report.total_components, 5);
        assert_eq!(report.total_operations, 5);
        assert!(!report.component_stats.is_empty());
    }
}