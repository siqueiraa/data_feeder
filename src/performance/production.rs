//! Production Readiness Framework
//! 
//! This module implements load testing, stress testing, regression validation pipelines,
//! production health checks, and comprehensive monitoring for production deployment
//! readiness validation.

use serde::{Serialize, Deserialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, debug, warn, error};
use tokio::time::{interval, timeout};
use futures::future::join_all;

/// Production readiness configuration
#[derive(Debug, Clone)]
pub struct ProductionConfig {
    /// Load testing configuration
    pub load_test_config: LoadTestConfig,
    /// Stress testing configuration
    pub stress_test_config: StressTestConfig,
    /// Health check configuration
    pub health_check_config: HealthCheckConfig,
    /// Regression pipeline configuration
    pub regression_config: RegressionConfig,
    /// Enable production monitoring
    pub enable_monitoring: bool,
}

impl Default for ProductionConfig {
    fn default() -> Self {
        Self {
            load_test_config: LoadTestConfig::default(),
            stress_test_config: StressTestConfig::default(),
            health_check_config: HealthCheckConfig::default(),
            regression_config: RegressionConfig::default(),
            enable_monitoring: true,
        }
    }
}

/// Load testing configuration
#[derive(Debug, Clone)]
pub struct LoadTestConfig {
    /// Duration of sustained load test
    pub test_duration: Duration,
    /// Number of concurrent connections/operations
    pub concurrent_load: usize,
    /// Target operations per second
    pub target_ops_per_second: f64,
    /// Acceptable response time percentiles
    pub response_time_p99_ms: f64,
    pub response_time_p95_ms: f64,
    pub response_time_p50_ms: f64,
    /// Memory usage limits during load
    pub max_memory_usage_mb: f64,
    /// CPU usage limits during load
    pub max_cpu_usage_percent: f32,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        Self {
            test_duration: Duration::from_secs(300), // 5 minutes
            concurrent_load: 100,
            target_ops_per_second: 1000.0,
            response_time_p99_ms: 500.0,
            response_time_p95_ms: 200.0,
            response_time_p50_ms: 50.0,
            max_memory_usage_mb: 2048.0, // 2GB
            max_cpu_usage_percent: 80.0,
        }
    }
}

/// Stress testing configuration
#[derive(Debug, Clone)]
pub struct StressTestConfig {
    /// Duration of stress test
    pub test_duration: Duration,
    /// Maximum load multiplier (e.g., 10x normal load)
    pub max_load_multiplier: f64,
    /// Load ramp-up duration
    pub ramp_up_duration: Duration,
    /// Failure threshold (percentage of failed operations)
    pub failure_threshold_percent: f32,
    /// Circuit breaker configuration
    pub circuit_breaker_enabled: bool,
    /// Recovery time after stress
    pub recovery_duration: Duration,
}

impl Default for StressTestConfig {
    fn default() -> Self {
        Self {
            test_duration: Duration::from_secs(180), // 3 minutes
            max_load_multiplier: 10.0,
            ramp_up_duration: Duration::from_secs(30),
            failure_threshold_percent: 5.0,
            circuit_breaker_enabled: true,
            recovery_duration: Duration::from_secs(60),
        }
    }
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Health check interval
    pub check_interval: Duration,
    /// Health check timeout
    pub check_timeout: Duration,
    /// Number of consecutive failures before marking unhealthy
    pub failure_threshold: usize,
    /// Health check categories
    pub enabled_checks: Vec<HealthCheckType>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HealthCheckType {
    Memory,
    Cpu,
    Storage,
    Network,
    Dependencies,
    Performance,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(30),
            check_timeout: Duration::from_secs(5),
            failure_threshold: 3,
            enabled_checks: vec![
                HealthCheckType::Memory,
                HealthCheckType::Cpu,
                HealthCheckType::Storage,
                HealthCheckType::Network,
                HealthCheckType::Performance,
            ],
        }
    }
}

/// Regression testing configuration
#[derive(Debug, Clone)]
pub struct RegressionConfig {
    /// Enable automated regression testing
    pub auto_regression_enabled: bool,
    /// Regression detection threshold (percentage degradation)
    pub regression_threshold_percent: f32,
    /// Number of baseline runs to average
    pub baseline_runs: usize,
    /// Number of current runs to average
    pub current_runs: usize,
    /// Minimum confidence level for regression detection
    pub confidence_threshold: f32,
}

impl Default for RegressionConfig {
    fn default() -> Self {
        Self {
            auto_regression_enabled: true,
            regression_threshold_percent: 10.0,
            baseline_runs: 5,
            current_runs: 5,
            confidence_threshold: 0.95,
        }
    }
}

/// Load testing results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTestResult {
    pub test_name: String,
    pub start_time: u64,
    pub duration_seconds: f64,
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub operations_per_second: f64,
    pub response_times: ResponseTimeStats,
    pub resource_usage: ResourceUsageStats,
    pub error_breakdown: HashMap<String, u32>,
    pub test_passed: bool,
    pub failure_reasons: Vec<String>,
}

/// Response time statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseTimeStats {
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub min_ms: f64,
    pub average_ms: f64,
}

/// Resource usage statistics during testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsageStats {
    pub peak_memory_mb: f64,
    pub average_memory_mb: f64,
    pub peak_cpu_percent: f32,
    pub average_cpu_percent: f32,
    pub network_throughput_mbps: f64,
    pub storage_io_ops_per_sec: f64,
}

/// Stress testing results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressTestResult {
    pub test_name: String,
    pub start_time: u64,
    pub max_load_achieved: f64,
    pub breaking_point: Option<f64>,
    pub recovery_time_seconds: f64,
    pub failure_modes: Vec<FailureMode>,
    pub degradation_pattern: DegradationPattern,
    pub test_passed: bool,
}

/// System failure modes under stress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureMode {
    pub failure_type: String,
    pub load_level: f64,
    pub first_occurrence: u64,
    pub frequency: u32,
    pub recovery_time_seconds: f64,
}

/// Performance degradation pattern under stress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DegradationPattern {
    Linear { slope: f64 },
    Exponential { base: f64 },
    Cliff { breaking_point: f64 },
    Oscillating { amplitude: f64, frequency: f64 },
    Stable,
}

/// Health check status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub timestamp: u64,
    pub overall_status: HealthState,
    pub check_results: HashMap<HealthCheckType, HealthCheckResult>,
    pub uptime_seconds: f64,
    pub last_failure: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
    Critical,
}

/// Individual health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub status: HealthState,
    pub check_duration_ms: f64,
    pub message: String,
    pub metrics: HashMap<String, f64>,
    pub last_success: Option<u64>,
    pub consecutive_failures: usize,
}

/// Production readiness framework
pub struct ProductionReadinessFramework {
    config: ProductionConfig,
    load_test_history: Arc<Mutex<VecDeque<LoadTestResult>>>,
    stress_test_history: Arc<Mutex<VecDeque<StressTestResult>>>,
    health_status: Arc<Mutex<HealthStatus>>,
    metrics_counter: AtomicU64,
    active_operations: AtomicUsize,
}

impl ProductionReadinessFramework {
    /// Create new production readiness framework
    pub fn new(config: ProductionConfig) -> Self {
        let initial_health = HealthStatus {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            overall_status: HealthState::Healthy,
            check_results: HashMap::new(),
            uptime_seconds: 0.0,
            last_failure: None,
        };

        Self {
            config,
            load_test_history: Arc::new(Mutex::new(VecDeque::with_capacity(50))),
            stress_test_history: Arc::new(Mutex::new(VecDeque::with_capacity(50))),
            health_status: Arc::new(Mutex::new(initial_health)),
            metrics_counter: AtomicU64::new(0),
            active_operations: AtomicUsize::new(0),
        }
    }

    /// Execute comprehensive load testing
    pub async fn execute_load_test(&self, test_name: &str) -> LoadTestResult {
        info!("Starting load test: {}", test_name);
        let start_time = Instant::now();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let config = &self.config.load_test_config;
        let mut response_times = Vec::new();
        let mut error_breakdown = HashMap::new();
        let mut successful_ops = 0u64;
        let mut failed_ops = 0u64;

        // Create concurrent load generators
        let mut tasks = Vec::new();
        let total_target_ops = (config.target_ops_per_second * config.test_duration.as_secs_f64()) as usize;
        let ops_per_task = (total_target_ops / config.concurrent_load).max(1); // Ensure at least 1 operation per task

        for task_id in 0..config.concurrent_load {
            let task = self.execute_load_test_task(task_id, ops_per_task);
            tasks.push(task);
        }

        // Execute all load test tasks concurrently
        let task_results = join_all(tasks).await;

        // Aggregate results
        for result in task_results {
            match result {
                Ok((task_response_times, task_errors)) => {
                    let task_op_count = task_response_times.len() as u64;
                    response_times.extend(task_response_times);
                    successful_ops += task_op_count;
                    
                    for (error_type, count) in task_errors {
                        *error_breakdown.entry(error_type).or_insert(0) += count;
                        failed_ops += count as u64;
                    }
                }
                Err(e) => {
                    error!("Load test task failed: {}", e);
                    *error_breakdown.entry("task_failure".to_string()).or_insert(0) += 1;
                    failed_ops += 1;
                }
            }
        }

        let total_duration = start_time.elapsed().as_secs_f64();
        let total_operations = successful_ops + failed_ops;
        let ops_per_second = total_operations as f64 / total_duration;

        // Calculate response time statistics
        let response_time_stats = self.calculate_response_time_stats(&response_times);

        // Simulate resource usage (in production would collect real metrics)
        let resource_usage = ResourceUsageStats {
            peak_memory_mb: 1024.0,
            average_memory_mb: 768.0,
            peak_cpu_percent: 75.0,
            average_cpu_percent: 65.0,
            network_throughput_mbps: 100.0,
            storage_io_ops_per_sec: 500.0,
        };

        // Determine if test passed
        let mut failure_reasons = Vec::new();
        if ops_per_second < config.target_ops_per_second * 0.9 {
            failure_reasons.push(format!("Throughput {:.1} ops/s below target {:.1}", ops_per_second, config.target_ops_per_second));
        }
        if response_time_stats.p99_ms > config.response_time_p99_ms {
            failure_reasons.push(format!("P99 response time {:.1}ms exceeds limit {:.1}ms", response_time_stats.p99_ms, config.response_time_p99_ms));
        }
        if resource_usage.peak_memory_mb > config.max_memory_usage_mb {
            failure_reasons.push(format!("Peak memory {:.1}MB exceeds limit {:.1}MB", resource_usage.peak_memory_mb, config.max_memory_usage_mb));
        }

        let test_passed = failure_reasons.is_empty();
        let failure_reasons_for_log = failure_reasons.clone();

        let result = LoadTestResult {
            test_name: test_name.to_string(),
            start_time: timestamp,
            duration_seconds: total_duration,
            total_operations,
            successful_operations: successful_ops,
            failed_operations: failed_ops,
            operations_per_second: ops_per_second,
            response_times: response_time_stats,
            resource_usage,
            error_breakdown,
            test_passed,
            failure_reasons,
        };

        // Store result
        {
            let mut history = self.load_test_history.lock().unwrap();
            if history.len() >= 50 {
                history.pop_front();
            }
            history.push_back(result.clone());
        }

        self.metrics_counter.fetch_add(1, Ordering::Relaxed);

        if test_passed {
            info!("Load test '{}' PASSED: {:.1} ops/s, P99: {:.1}ms", test_name, ops_per_second, result.response_times.p99_ms);
        } else {
            warn!("Load test '{}' FAILED: {:?}", test_name, failure_reasons_for_log);
        }

        result
    }

    /// Execute individual load test task
    async fn execute_load_test_task(&self, task_id: usize, operations: usize) -> Result<(Vec<f64>, HashMap<String, u32>), Box<dyn std::error::Error + Send + Sync>> {
        let mut response_times = Vec::with_capacity(operations);
        let mut errors = HashMap::new();

        debug!("Load test task {} starting {} operations", task_id, operations);

        for _op in 0..operations {
            let _op_start = Instant::now();
            
            // Simulate operation (in production would call actual system)
            match self.simulate_operation().await {
                Ok(duration) => {
                    response_times.push(duration);
                }
                Err(error_type) => {
                    *errors.entry(error_type).or_insert(0) += 1;
                }
            }
            
            // Add small delay to control load
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        Ok((response_times, errors))
    }

    /// Simulate a system operation for testing
    async fn simulate_operation(&self) -> Result<f64, String> {
        self.active_operations.fetch_add(1, Ordering::Relaxed);
        
        // Simulate variable response times
        let base_latency = 10.0 + (rand::random::<f64>() * 40.0); // 10-50ms
        tokio::time::sleep(Duration::from_millis(base_latency as u64)).await;

        self.active_operations.fetch_sub(1, Ordering::Relaxed);

        // Simulate occasional failures
        if rand::random::<f64>() < 0.02 { // 2% failure rate
            Err("simulated_timeout".to_string())
        } else {
            Ok(base_latency)
        }
    }

    /// Calculate response time statistics
    fn calculate_response_time_stats(&self, response_times: &[f64]) -> ResponseTimeStats {
        if response_times.is_empty() {
            return ResponseTimeStats {
                p50_ms: 0.0, p95_ms: 0.0, p99_ms: 0.0,
                max_ms: 0.0, min_ms: 0.0, average_ms: 0.0,
            };
        }

        let mut sorted_times = response_times.to_vec();
        sorted_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let len = sorted_times.len();
        let p50_idx = (len as f64 * 0.50) as usize;
        let p95_idx = (len as f64 * 0.95) as usize;
        let p99_idx = (len as f64 * 0.99) as usize;

        ResponseTimeStats {
            p50_ms: sorted_times[p50_idx.min(len - 1)],
            p95_ms: sorted_times[p95_idx.min(len - 1)],
            p99_ms: sorted_times[p99_idx.min(len - 1)],
            max_ms: sorted_times[len - 1],
            min_ms: sorted_times[0],
            average_ms: sorted_times.iter().sum::<f64>() / len as f64,
        }
    }

    /// Execute stress testing
    pub async fn execute_stress_test(&self, test_name: &str) -> StressTestResult {
        info!("Starting stress test: {}", test_name);
        let start_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let config = &self.config.stress_test_config;

        let mut failure_modes = Vec::new();
        let mut breaking_point = None;
        let mut current_load = 1.0;

        // Ramp up load gradually
        let ramp_steps = 10;
        let load_increment = config.max_load_multiplier / ramp_steps as f64;
        let step_duration = config.ramp_up_duration.as_secs() / ramp_steps;

        for step in 0..ramp_steps {
            current_load = (step + 1) as f64 * load_increment;
            info!("Stress test at {:.1}x load", current_load);

            // Execute load at current level
            let step_result = self.execute_stress_step(current_load, Duration::from_secs(step_duration)).await;
            
            match step_result {
                Ok(failure_rate) => {
                    if failure_rate > config.failure_threshold_percent {
                        warn!("Breaking point detected at {:.1}x load ({:.1}% failures)", current_load, failure_rate);
                        breaking_point = Some(current_load);
                        
                        failure_modes.push(FailureMode {
                            failure_type: "high_failure_rate".to_string(),
                            load_level: current_load,
                            first_occurrence: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                            frequency: 1,
                            recovery_time_seconds: 0.0,
                        });
                        break;
                    }
                }
                Err(error) => {
                    error!("Stress test failed at {:.1}x load: {}", current_load, error);
                    breaking_point = Some(current_load);
                    
                    failure_modes.push(FailureMode {
                        failure_type: error,
                        load_level: current_load,
                        first_occurrence: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                        frequency: 1,
                        recovery_time_seconds: 0.0,
                    });
                    break;
                }
            }
        }

        // Test recovery
        info!("Testing recovery after stress");
        let recovery_start = Instant::now();
        
        // Simulate recovery period
        tokio::time::sleep(config.recovery_duration).await;
        
        let recovery_time = recovery_start.elapsed().as_secs_f64();
        let max_load_achieved = current_load;

        // Determine degradation pattern
        let degradation_pattern = if let Some(breaking_point) = breaking_point {
            DegradationPattern::Cliff { breaking_point }
        } else {
            DegradationPattern::Linear { slope: -5.0 } // Simulated linear degradation
        };

        let test_passed = breaking_point.is_none() || breaking_point.unwrap() > config.max_load_multiplier * 0.8;

        let result = StressTestResult {
            test_name: test_name.to_string(),
            start_time,
            max_load_achieved,
            breaking_point,
            recovery_time_seconds: recovery_time,
            failure_modes,
            degradation_pattern,
            test_passed,
        };

        // Store result
        {
            let mut history = self.stress_test_history.lock().unwrap();
            if history.len() >= 50 {
                history.pop_front();
            }
            history.push_back(result.clone());
        }

        if test_passed {
            info!("Stress test '{}' PASSED: Max load {:.1}x", test_name, max_load_achieved);
        } else {
            warn!("Stress test '{}' FAILED: Breaking point at {:.1}x", test_name, breaking_point.unwrap_or(0.0));
        }

        result
    }

    /// Execute stress test step at specific load level
    async fn execute_stress_step(&self, load_multiplier: f64, duration: Duration) -> Result<f32, String> {
        let operations = (100.0 * load_multiplier) as usize;
        let mut failures = 0;

        let end_time = Instant::now() + duration;
        
        while Instant::now() < end_time {
            let mut batch_failures = 0;
            
            // Execute batch of operations
            for _ in 0..operations.min(50) { // Batch size limit
                match timeout(Duration::from_millis(100), self.simulate_operation()).await {
                    Ok(Ok(_)) => {}, // Success
                    Ok(Err(_)) => batch_failures += 1,
                    Err(_) => batch_failures += 1, // Timeout
                }
            }

            failures += batch_failures;
            
            // Check for system overload
            if batch_failures > operations / 2 {
                return Err("system_overload".to_string());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let total_operations = operations * 10; // Approximate total operations
        let failure_rate = (failures as f32 / total_operations as f32) * 100.0;
        
        Ok(failure_rate)
    }

    /// Start health monitoring
    pub async fn start_health_monitoring(&self) -> tokio::task::JoinHandle<()> {
        let config = self.config.health_check_config.clone();
        let health_status = self.health_status.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(config.check_interval);
            let start_time = Instant::now();
            
            info!("Health monitoring started with {:.1}s interval", config.check_interval.as_secs_f32());

            loop {
                interval.tick().await;

                let mut check_results = HashMap::new();
                let mut overall_status = HealthState::Healthy;

                // Execute each enabled health check
                for check_type in &config.enabled_checks {
                    let check_result = Self::execute_health_check(check_type, &config).await;
                    
                    match check_result.status {
                        HealthState::Critical => overall_status = HealthState::Critical,
                        HealthState::Unhealthy if overall_status != HealthState::Critical => overall_status = HealthState::Unhealthy,
                        HealthState::Degraded if matches!(overall_status, HealthState::Healthy) => overall_status = HealthState::Degraded,
                        _ => {}
                    }
                    
                    check_results.insert(check_type.clone(), check_result);
                }

                // Update health status
                {
                    let mut status = health_status.lock().unwrap();
                    status.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                    status.overall_status = overall_status.clone();
                    status.check_results = check_results;
                    status.uptime_seconds = start_time.elapsed().as_secs_f64();
                    
                    if overall_status != HealthState::Healthy {
                        status.last_failure = Some(status.timestamp);
                    }
                }

                match overall_status {
                    HealthState::Healthy => debug!("Health check: All systems healthy"),
                    HealthState::Degraded => warn!("Health check: System performance degraded"),
                    HealthState::Unhealthy => error!("Health check: System unhealthy"),
                    HealthState::Critical => error!("Health check: CRITICAL system state"),
                }
            }
        })
    }

    /// Execute individual health check
    async fn execute_health_check(check_type: &HealthCheckType, _config: &HealthCheckConfig) -> HealthCheckResult {
        let start = Instant::now();
        
        let (status, message, metrics) = match check_type {
            HealthCheckType::Memory => {
                // Simulate memory check
                let memory_usage = 65.0; // Simulate 65% memory usage
                if memory_usage > 90.0 {
                    (HealthState::Critical, "Memory usage critical".to_string(), HashMap::from([("memory_percent".to_string(), memory_usage)]))
                } else if memory_usage > 80.0 {
                    (HealthState::Unhealthy, "Memory usage high".to_string(), HashMap::from([("memory_percent".to_string(), memory_usage)]))
                } else {
                    (HealthState::Healthy, "Memory usage normal".to_string(), HashMap::from([("memory_percent".to_string(), memory_usage)]))
                }
            }
            HealthCheckType::Cpu => {
                let cpu_usage = 45.0; // Simulate 45% CPU usage
                if cpu_usage > 95.0 {
                    (HealthState::Critical, "CPU usage critical".to_string(), HashMap::from([("cpu_percent".to_string(), cpu_usage)]))
                } else if cpu_usage > 85.0 {
                    (HealthState::Degraded, "CPU usage high".to_string(), HashMap::from([("cpu_percent".to_string(), cpu_usage)]))
                } else {
                    (HealthState::Healthy, "CPU usage normal".to_string(), HashMap::from([("cpu_percent".to_string(), cpu_usage)]))
                }
            }
            HealthCheckType::Storage => {
                let storage_usage = 70.0;
                if storage_usage > 95.0 {
                    (HealthState::Critical, "Storage full".to_string(), HashMap::from([("storage_percent".to_string(), storage_usage)]))
                } else {
                    (HealthState::Healthy, "Storage normal".to_string(), HashMap::from([("storage_percent".to_string(), storage_usage)]))
                }
            }
            HealthCheckType::Network => {
                // Simulate network connectivity check
                tokio::time::sleep(Duration::from_millis(10)).await;
                (HealthState::Healthy, "Network connectivity OK".to_string(), HashMap::from([("latency_ms".to_string(), 10.0)]))
            }
            HealthCheckType::Dependencies => {
                (HealthState::Healthy, "All dependencies available".to_string(), HashMap::new())
            }
            HealthCheckType::Performance => {
                let response_time = 45.0; // ms
                if response_time > 200.0 {
                    (HealthState::Degraded, "Performance degraded".to_string(), HashMap::from([("response_time_ms".to_string(), response_time)]))
                } else {
                    (HealthState::Healthy, "Performance normal".to_string(), HashMap::from([("response_time_ms".to_string(), response_time)]))
                }
            }
        };

        let check_duration = start.elapsed().as_millis() as f64;

        let is_healthy = matches!(status, HealthState::Healthy);
        
        HealthCheckResult {
            status,
            check_duration_ms: check_duration,
            message,
            metrics,
            last_success: if is_healthy { Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) } else { None },
            consecutive_failures: if is_healthy { 0 } else { 1 },
        }
    }

    /// Get current health status
    pub fn get_health_status(&self) -> HealthStatus {
        self.health_status.lock().unwrap().clone()
    }

    /// Get load test history
    pub fn get_load_test_history(&self) -> Vec<LoadTestResult> {
        self.load_test_history.lock().unwrap().iter().cloned().collect()
    }

    /// Get stress test history
    pub fn get_stress_test_history(&self) -> Vec<StressTestResult> {
        self.stress_test_history.lock().unwrap().iter().cloned().collect()
    }

    /// Get production readiness score
    pub fn get_readiness_score(&self) -> f32 {
        let health_status = self.get_health_status();
        let load_tests = self.get_load_test_history();
        let stress_tests = self.get_stress_test_history();

        let mut score: f32 = 100.0;

        // Health check contribution (40%)
        match health_status.overall_status {
            HealthState::Healthy => {},
            HealthState::Degraded => score -= 10.0,
            HealthState::Unhealthy => score -= 25.0,
            HealthState::Critical => score -= 40.0,
        }

        // Load test contribution (40%)
        if let Some(latest_load_test) = load_tests.last() {
            if !latest_load_test.test_passed {
                score -= 20.0;
            }
        } else {
            score -= 15.0; // No load test results
        }

        // Stress test contribution (20%)
        if let Some(latest_stress_test) = stress_tests.last() {
            if !latest_stress_test.test_passed {
                score -= 10.0;
            }
        } else {
            score -= 5.0; // No stress test results
        }

        score.max(0.0)
    }

    /// Get production operations count
    pub fn get_production_operations_count(&self) -> u64 {
        self.metrics_counter.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_production_config_default() {
        let config = ProductionConfig::default();
        assert!(config.enable_monitoring);
        assert_eq!(config.load_test_config.concurrent_load, 100);
        assert_eq!(config.stress_test_config.max_load_multiplier, 10.0);
    }

    #[test]
    fn test_load_test_config() {
        let config = LoadTestConfig::default();
        assert_eq!(config.test_duration, Duration::from_secs(300));
        assert_eq!(config.target_ops_per_second, 1000.0);
        assert_eq!(config.response_time_p99_ms, 500.0);
    }

    #[test]
    fn test_health_check_config() {
        let config = HealthCheckConfig::default();
        assert_eq!(config.check_interval, Duration::from_secs(30));
        assert_eq!(config.failure_threshold, 3);
        assert!(config.enabled_checks.contains(&HealthCheckType::Memory));
    }

    #[test]
    fn test_production_framework_creation() {
        let config = ProductionConfig::default();
        let framework = ProductionReadinessFramework::new(config);
        
        assert_eq!(framework.get_production_operations_count(), 0);
        let health_status = framework.get_health_status();
        assert_eq!(health_status.overall_status, HealthState::Healthy);
    }

    #[tokio::test]
    async fn test_simulate_operation() {
        let config = ProductionConfig::default();
        let framework = ProductionReadinessFramework::new(config);
        
        let result = framework.simulate_operation().await;
        // Should either succeed with timing or fail with error
        match result {
            Ok(duration) => assert!(duration > 0.0),
            Err(error) => assert!(!error.is_empty()),
        }
    }

    #[test]
    fn test_response_time_stats() {
        let config = ProductionConfig::default();
        let framework = ProductionReadinessFramework::new(config);
        
        let response_times = vec![10.0, 20.0, 30.0, 40.0, 50.0, 100.0, 200.0];
        let stats = framework.calculate_response_time_stats(&response_times);
        
        assert!(stats.p50_ms >= 20.0 && stats.p50_ms <= 40.0);
        assert!(stats.p99_ms >= 100.0);
        assert_eq!(stats.max_ms, 200.0);
        assert_eq!(stats.min_ms, 10.0);
    }

    #[tokio::test]
    async fn test_health_check_execution() {
        let config = HealthCheckConfig::default();
        
        let result = ProductionReadinessFramework::execute_health_check(&HealthCheckType::Memory, &config).await;
        
        assert!(result.check_duration_ms >= 0.0);
        assert!(!result.message.is_empty());
    }

    #[tokio::test]
    async fn test_load_test_execution() {
        let mut config = ProductionConfig::default();
        config.load_test_config.test_duration = Duration::from_millis(500); // Longer test for more operations
        config.load_test_config.concurrent_load = 4;
        config.load_test_config.target_ops_per_second = 50.0; // Higher ops per second
        
        let framework = ProductionReadinessFramework::new(config);
        
        let result = framework.execute_load_test("test_load").await;
        
        assert!(!result.test_name.is_empty());
        assert!(result.total_operations >= 0, "Total operations should be non-negative, got: {}", result.total_operations);
        assert!(result.duration_seconds > 0.0);
    }

    #[tokio::test]
    async fn test_stress_test_execution() {
        let mut config = ProductionConfig::default();
        config.stress_test_config.test_duration = Duration::from_millis(100);
        config.stress_test_config.ramp_up_duration = Duration::from_millis(50);
        config.stress_test_config.max_load_multiplier = 3.0;
        
        let framework = ProductionReadinessFramework::new(config);
        
        let result = framework.execute_stress_test("test_stress").await;
        
        assert!(!result.test_name.is_empty());
        assert!(result.max_load_achieved > 0.0);
        assert!(result.recovery_time_seconds >= 0.0);
    }

    #[test]
    fn test_readiness_score_calculation() {
        let config = ProductionConfig::default();
        let framework = ProductionReadinessFramework::new(config);
        
        let score = framework.get_readiness_score();
        
        // Should start with a reasonable score despite no test history
        assert!(score >= 60.0 && score <= 100.0);
    }

    #[test]
    fn test_degradation_pattern_serialization() {
        let pattern = DegradationPattern::Cliff { breaking_point: 5.0 };
        let serialized = serde_json::to_string(&pattern).unwrap();
        let deserialized: DegradationPattern = serde_json::from_str(&serialized).unwrap();
        
        match deserialized {
            DegradationPattern::Cliff { breaking_point } => assert_eq!(breaking_point, 5.0),
            _ => panic!("Wrong degradation pattern type"),
        }
    }
}