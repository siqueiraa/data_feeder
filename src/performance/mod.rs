//! Comprehensive Performance Monitoring and Bottleneck Detection System
//! 
//! This module implements real-time CPU monitoring, bottleneck detection, and performance
//! analysis specifically designed to identify single-core usage patterns and performance issues.

use sysinfo::{System, Pid};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::interval;
use tracing::{debug, warn};
use serde::{Serialize, Deserialize};

/// Performance monitoring configuration
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// How often to collect CPU metrics (default: 500ms)
    pub collection_interval: Duration,
    /// Threshold for single-core bottleneck detection (default: 80%)
    pub single_core_threshold: f32,
    /// Number of samples to keep in history (default: 120 = 1 minute at 500ms intervals)
    pub history_size: usize,
    /// Enable detailed per-thread monitoring
    pub detailed_threading: bool,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_millis(500),
            single_core_threshold: 80.0,
            history_size: 120,
            detailed_threading: true,
        }
    }
}

/// Real-time CPU metrics for a single measurement point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuSnapshot {
    pub timestamp: u64,
    pub overall_cpu_percent: f32,
    pub per_core_usage: Vec<f32>,
    pub process_cpu_percent: f32,
    pub memory_usage_mb: f64,
    pub thread_count: usize,
    pub single_core_bottleneck: bool,
    pub bottleneck_core_id: Option<usize>,
    pub load_imbalance_ratio: f32,
}

/// Bottleneck detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BottleneckAnalysis {
    pub detected: bool,
    pub bottleneck_type: BottleneckType,
    pub severity: BottleneckSeverity,
    pub affected_cores: Vec<usize>,
    pub imbalance_ratio: f32,
    pub recommendation: String,
    pub detected_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BottleneckType {
    SingleCoreHigh,      // One core >80% while others <50%
    LoadImbalance,       // Significant variation between cores
    AllCoresHigh,        // All cores >90%
    MemoryPressure,      // High memory usage with CPU spikes
    ThreadContention,    // High thread count with low CPU efficiency
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BottleneckSeverity {
    Low,       // Minor performance impact
    Medium,    // Noticeable performance degradation
    High,      // Significant performance issues
    Critical,  // System performance severely impacted
}

/// Component-specific performance tracking
#[derive(Debug, Default)]
pub struct ComponentPerformance {
    pub cpu_time_nanos: AtomicU64,
    pub operation_count: AtomicU64,
    pub last_activity: Arc<Mutex<Option<Instant>>>,
    pub peak_cpu_percent: Arc<Mutex<f32>>,
}

/// Main performance monitor
pub struct PerformanceMonitor {
    system: Arc<Mutex<System>>,
    config: PerformanceConfig,
    cpu_history: Arc<Mutex<Vec<CpuSnapshot>>>,
    bottleneck_history: Arc<Mutex<Vec<BottleneckAnalysis>>>,
    component_performance: Arc<Mutex<HashMap<String, ComponentPerformance>>>,
    process_pid: Pid,
    running: Arc<Mutex<bool>>,
}

impl PerformanceMonitor {
    /// Create a new performance monitor
    pub fn new(config: Option<PerformanceConfig>) -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        
        let process_pid = Pid::from(std::process::id() as usize);
        
        Self {
            system: Arc::new(Mutex::new(system)),
            config: config.unwrap_or_default(),
            cpu_history: Arc::new(Mutex::new(Vec::with_capacity(120))),
            bottleneck_history: Arc::new(Mutex::new(Vec::new())),
            component_performance: Arc::new(Mutex::new(HashMap::new())),
            process_pid,
            running: Arc::new(Mutex::new(false)),
        }
    }

    /// Start the performance monitoring background task
    pub async fn start_monitoring(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        {
            let mut running = self.running.lock().unwrap();
            if *running {
                warn!("Performance monitor already running");
                return Ok(());
            }
            *running = true;
        }


        let system = self.system.clone();
        let cpu_history = self.cpu_history.clone();
        let bottleneck_history = self.bottleneck_history.clone();
        let config = self.config.clone();
        let process_pid = self.process_pid;
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut interval = interval(config.collection_interval);

            loop {
                interval.tick().await;
                
                // Check if we should stop
                {
                    let should_run = *running.lock().unwrap();
                    if !should_run {
                        break;
                    }
                }

                // Collect CPU metrics
                let snapshot = {
                    let mut sys = system.lock().unwrap();
                    sys.refresh_cpu();
                    sys.refresh_processes();
                    
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    let per_core_usage: Vec<f32> = sys.cpus()
                        .iter()
                        .map(|cpu| cpu.cpu_usage())
                        .collect();

                    let overall_cpu = per_core_usage.iter().sum::<f32>() / per_core_usage.len() as f32;
                    
                    let (process_cpu, memory_mb) = if let Some(process) = sys.process(process_pid) {
                        (
                            process.cpu_usage(),
                            process.memory() as f64 / 1_048_576.0, // Convert to MB
                        )
                    } else {
                        (0.0, 0.0)
                    };

                    // Get approximate thread count using system-level info
                    let thread_count = sys.cpus().len(); // Use CPU count as baseline - we'll improve this later

                    // Detect single-core bottleneck
                    let (single_core_bottleneck, bottleneck_core_id, load_imbalance_ratio) = 
                        Self::detect_single_core_bottleneck(&per_core_usage, config.single_core_threshold);

                    CpuSnapshot {
                        timestamp,
                        overall_cpu_percent: overall_cpu,
                        per_core_usage,
                        process_cpu_percent: process_cpu,
                        memory_usage_mb: memory_mb,
                        thread_count,
                        single_core_bottleneck,
                        bottleneck_core_id,
                        load_imbalance_ratio,
                    }
                };

                // Analyze bottlenecks
                let bottleneck_analysis = Self::analyze_bottlenecks(&snapshot, &config);

                // Store metrics
                {
                    let mut history = cpu_history.lock().unwrap();
                    history.push(snapshot.clone());
                    
                    // Keep only recent history
                    if history.len() > config.history_size {
                        history.remove(0);
                    }
                }

                // Store bottleneck analysis
                if let Some(analysis) = bottleneck_analysis {
                    let mut bottlenecks = bottleneck_history.lock().unwrap();
                    bottlenecks.push(analysis.clone());
                    
                }

                // Update Prometheus metrics if available
                if let Some(metrics) = crate::metrics::get_metrics() {
                    metrics.record_cpu_usage("data_feeder", "main", snapshot.process_cpu_percent as i64);
                    
                    for (core_id, usage) in snapshot.per_core_usage.iter().enumerate() {
                        metrics.record_cpu_usage("system", &format!("core_{}", core_id), *usage as i64);
                    }
                    
                    metrics.record_system_resource_utilization("cpu", "production", snapshot.overall_cpu_percent as i64);
                    metrics.record_system_resource_utilization("memory", "production", (snapshot.memory_usage_mb / 10.0) as i64);
                }

            }
        });

        Ok(())
    }

    /// Stop the performance monitoring
    pub fn stop_monitoring(&self) {
        let mut running = self.running.lock().unwrap();
        *running = false;
    }

    /// Get the number of CPU cores
    pub fn get_cpu_count(&self) -> usize {
        let system = self.system.lock().unwrap();
        system.cpus().len()
    }

    /// Get current CPU snapshot
    pub fn get_current_snapshot(&self) -> Option<CpuSnapshot> {
        let history = self.cpu_history.lock().unwrap();
        history.last().cloned()
    }

    /// Get recent CPU history
    pub fn get_cpu_history(&self, last_n: Option<usize>) -> Vec<CpuSnapshot> {
        let history = self.cpu_history.lock().unwrap();
        match last_n {
            Some(n) => {
                let start = history.len().saturating_sub(n);
                history[start..].to_vec()
            },
            None => history.clone(),
        }
    }

    /// Get recent bottleneck analyses
    pub fn get_bottleneck_history(&self, last_n: Option<usize>) -> Vec<BottleneckAnalysis> {
        let bottlenecks = self.bottleneck_history.lock().unwrap();
        match last_n {
            Some(n) => {
                let start = bottlenecks.len().saturating_sub(n);
                bottlenecks[start..].to_vec()
            },
            None => bottlenecks.clone(),
        }
    }

    /// Generate comprehensive performance report
    pub fn generate_performance_report(&self) -> PerformanceReport {
        let cpu_history = self.get_cpu_history(Some(60)); // Last 60 samples (~30 seconds)
        let bottlenecks = self.get_bottleneck_history(Some(20));
        
        let avg_cpu = if !cpu_history.is_empty() {
            cpu_history.iter().map(|s| s.overall_cpu_percent).sum::<f32>() / cpu_history.len() as f32
        } else {
            0.0
        };

        let avg_process_cpu = if !cpu_history.is_empty() {
            cpu_history.iter().map(|s| s.process_cpu_percent).sum::<f32>() / cpu_history.len() as f32
        } else {
            0.0
        };

        let single_core_bottlenecks = cpu_history.iter()
            .filter(|s| s.single_core_bottleneck)
            .count();

        let critical_bottlenecks = bottlenecks.iter()
            .filter(|b| matches!(b.severity, BottleneckSeverity::Critical | BottleneckSeverity::High))
            .count();

        let per_core_averages = if !cpu_history.is_empty() {
            let core_count = cpu_history[0].per_core_usage.len();
            let mut averages = vec![0.0; core_count];
            
            for snapshot in &cpu_history {
                for (i, usage) in snapshot.per_core_usage.iter().enumerate() {
                    averages[i] += usage;
                }
            }
            
            for avg in &mut averages {
                *avg /= cpu_history.len() as f32;
            }
            
            averages
        } else {
            vec![]
        };

        PerformanceReport {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            avg_system_cpu_percent: avg_cpu,
            avg_process_cpu_percent: avg_process_cpu,
            per_core_averages,
            single_core_bottleneck_count: single_core_bottlenecks,
            critical_bottleneck_count: critical_bottlenecks,
            total_samples: cpu_history.len(),
            recent_bottlenecks: bottlenecks.clone(),
            performance_score: Self::calculate_performance_score(&cpu_history, &bottlenecks),
            recommendations: Self::generate_recommendations(&cpu_history, &bottlenecks),
        }
    }

    /// Register a component for performance tracking
    pub fn register_component(&self, name: &str) {
        let mut components = self.component_performance.lock().unwrap();
        components.insert(name.to_string(), ComponentPerformance::default());
        debug!("📊 Registered component for performance tracking: {}", name);
    }

    /// Record CPU time for a component
    pub fn record_component_cpu_time(&self, component: &str, cpu_time_nanos: u64) {
        let components = self.component_performance.lock().unwrap();
        if let Some(perf) = components.get(component) {
            perf.cpu_time_nanos.fetch_add(cpu_time_nanos, Ordering::Relaxed);
            perf.operation_count.fetch_add(1, Ordering::Relaxed);
            
            let mut last_activity = perf.last_activity.lock().unwrap();
            *last_activity = Some(Instant::now());
        }
    }

    /// Detect single-core bottleneck
    fn detect_single_core_bottleneck(per_core_usage: &[f32], threshold: f32) -> (bool, Option<usize>, f32) {
        if per_core_usage.is_empty() {
            return (false, None, 0.0);
        }

        let max_usage = per_core_usage.iter().fold(0.0f32, |a, &b| a.max(b));
        let min_usage = per_core_usage.iter().fold(100.0f32, |a, &b| a.min(b));
        let avg_usage = per_core_usage.iter().sum::<f32>() / per_core_usage.len() as f32;
        
        let imbalance_ratio = if min_usage > 0.0 {
            max_usage / min_usage
        } else {
            max_usage
        };

        // Single-core bottleneck: one core >threshold while average <50% of that core
        let bottleneck_detected = max_usage > threshold && avg_usage < (max_usage * 0.6);
        
        let bottleneck_core = if bottleneck_detected {
            per_core_usage.iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                .map(|(i, _)| i)
        } else {
            None
        };

        (bottleneck_detected, bottleneck_core, imbalance_ratio)
    }

    /// Analyze bottlenecks in CPU snapshot
    fn analyze_bottlenecks(snapshot: &CpuSnapshot, _config: &PerformanceConfig) -> Option<BottleneckAnalysis> {
        let timestamp = snapshot.timestamp;
        
        // Single-core high usage
        if snapshot.single_core_bottleneck {
            let severity = if snapshot.per_core_usage[snapshot.bottleneck_core_id.unwrap()] > 95.0 {
                BottleneckSeverity::Critical
            } else if snapshot.per_core_usage[snapshot.bottleneck_core_id.unwrap()] > 90.0 {
                BottleneckSeverity::High
            } else {
                BottleneckSeverity::Medium
            };

            return Some(BottleneckAnalysis {
                detected: true,
                bottleneck_type: BottleneckType::SingleCoreHigh,
                severity,
                affected_cores: vec![snapshot.bottleneck_core_id.unwrap()],
                imbalance_ratio: snapshot.load_imbalance_ratio,
                recommendation: format!(
                    "Single-core bottleneck on core {}. Consider implementing parallel processing or async tasks to distribute load across cores.",
                    snapshot.bottleneck_core_id.unwrap()
                ),
                detected_at: timestamp,
            });
        }

        // Load imbalance
        if snapshot.load_imbalance_ratio > 3.0 {
            return Some(BottleneckAnalysis {
                detected: true,
                bottleneck_type: BottleneckType::LoadImbalance,
                severity: BottleneckSeverity::Medium,
                affected_cores: vec![], // All cores affected by imbalance
                imbalance_ratio: snapshot.load_imbalance_ratio,
                recommendation: "Significant load imbalance detected. Review workload distribution and consider load balancing strategies.".to_string(),
                detected_at: timestamp,
            });
        }

        // All cores high
        let high_core_count = snapshot.per_core_usage.iter().filter(|&&usage| usage > 90.0).count();
        if high_core_count >= snapshot.per_core_usage.len() * 8 / 10 { // 80% of cores
            return Some(BottleneckAnalysis {
                detected: true,
                bottleneck_type: BottleneckType::AllCoresHigh,
                severity: BottleneckSeverity::High,
                affected_cores: (0..snapshot.per_core_usage.len()).collect(),
                imbalance_ratio: snapshot.load_imbalance_ratio,
                recommendation: "System-wide high CPU usage. Consider scaling horizontally or optimizing algorithms.".to_string(),
                detected_at: timestamp,
            });
        }

        None
    }

    /// Calculate overall performance score (0-100)
    fn calculate_performance_score(history: &[CpuSnapshot], bottlenecks: &[BottleneckAnalysis]) -> f32 {
        if history.is_empty() {
            return 100.0; // No data = assume good performance
        }

        let mut score = 100.0;

        // Penalize high CPU usage
        let avg_cpu = history.iter().map(|s| s.overall_cpu_percent).sum::<f32>() / history.len() as f32;
        if avg_cpu > 80.0 {
            score -= (avg_cpu - 80.0) * 2.0;
        }

        // Penalize single-core bottlenecks
        let bottleneck_ratio = history.iter().filter(|s| s.single_core_bottleneck).count() as f32 / history.len() as f32;
        score -= bottleneck_ratio * 30.0;

        // Penalize critical bottlenecks
        let critical_bottlenecks = bottlenecks.iter()
            .filter(|b| matches!(b.severity, BottleneckSeverity::Critical))
            .count() as f32;
        score -= critical_bottlenecks * 20.0;

        score.clamp(0.0, 100.0)
    }

    /// Generate performance recommendations
    fn generate_recommendations(history: &[CpuSnapshot], bottlenecks: &[BottleneckAnalysis]) -> Vec<String> {
        let mut recommendations = Vec::new();

        if history.is_empty() {
            return recommendations;
        }

        // Check for single-core bottlenecks
        let single_core_ratio = history.iter()
            .filter(|s| s.single_core_bottleneck)
            .count() as f32 / history.len() as f32;
        
        if single_core_ratio > 0.3 {
            recommendations.push(
                "🎯 Frequent single-core bottlenecks detected. Implement multi-threading in quantile calculations or heavy computation tasks.".to_string()
            );
        }

        // Check for high memory usage
        let avg_memory = history.iter().map(|s| s.memory_usage_mb).sum::<f64>() / history.len() as f64;
        if avg_memory > 500.0 {
            recommendations.push(
                format!("💾 High memory usage ({:.1}MB). Consider memory optimization and object pooling.", avg_memory)
            );
        }

        // Check for thread scaling issues
        let avg_threads = history.iter().map(|s| s.thread_count).sum::<usize>() / history.len();
        let cpu_cores = history[0].per_core_usage.len();
        if avg_threads > cpu_cores * 4 {
            recommendations.push(
                format!("🧵 High thread count ({} threads for {} cores). Consider thread pool optimization.", avg_threads, cpu_cores)
            );
        }

        // Add bottleneck-specific recommendations
        for bottleneck in bottlenecks.iter().take(3) { // Top 3 recent bottlenecks
            if !recommendations.iter().any(|r| r.contains(&bottleneck.recommendation)) {
                recommendations.push(format!("⚠️ {}", bottleneck.recommendation));
            }
        }

        if recommendations.is_empty() {
            recommendations.push("✅ No significant performance issues detected.".to_string());
        }

        recommendations
    }
}

/// Comprehensive performance report
#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub timestamp: u64,
    pub avg_system_cpu_percent: f32,
    pub avg_process_cpu_percent: f32,
    pub per_core_averages: Vec<f32>,
    pub single_core_bottleneck_count: usize,
    pub critical_bottleneck_count: usize,
    pub total_samples: usize,
    pub recent_bottlenecks: Vec<BottleneckAnalysis>,
    pub performance_score: f32,
    pub recommendations: Vec<String>,
}

/// Global performance monitor instance
static PERFORMANCE_MONITOR: std::sync::OnceLock<Arc<PerformanceMonitor>> = std::sync::OnceLock::new();

/// Initialize global performance monitor
pub async fn init_performance_monitor(config: Option<PerformanceConfig>) -> Result<Arc<PerformanceMonitor>, Box<dyn std::error::Error + Send + Sync>> {
    let monitor = Arc::new(PerformanceMonitor::new(config));
    
    PERFORMANCE_MONITOR.set(monitor.clone()).map_err(|_| {
        "Performance monitor already initialized"
    })?;

    // Start monitoring
    monitor.start_monitoring().await?;

    Ok(monitor)
}

/// Get global performance monitor instance
pub fn get_performance_monitor() -> Option<Arc<PerformanceMonitor>> {
    PERFORMANCE_MONITOR.get().cloned()
}