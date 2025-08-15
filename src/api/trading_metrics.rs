//! Trading Performance Metrics Collection and Monitoring
//! 
//! This module implements high-precision latency monitoring for trading operations,
//! specifically designed to track and validate ≤1ms latency targets for critical
//! trading paths including order placement, cancellation, and market data processing.

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{info, warn, debug};

/// High-precision trading performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingPerformanceMetrics {
    pub timestamp: u64,
    pub order_placement_latency_us: u64,
    pub order_cancellation_latency_us: u64,
    pub order_modification_latency_us: u64,
    pub position_query_latency_us: u64,
    pub balance_query_latency_us: u64,
    pub websocket_message_processing_us: u64,
    pub json_parsing_latency_us: u64,
    pub authentication_generation_us: u64,
    pub zero_copy_serialization_us: u64,
    pub network_round_trip_us: u64,
    pub end_to_end_order_latency_us: u64,
    
    // Latency distribution analysis
    pub latency_percentiles: LatencyPercentiles,
    
    // Performance targets and compliance
    pub latency_target_compliance: LatencyTargetCompliance,
    
    // Resource utilization during trading operations
    pub cpu_usage_during_trading: f32,
    pub memory_allocation_bytes: u64,
    pub gc_pressure_indicator: f32,
}

/// Latency percentile distribution for detailed analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50_microseconds: u64,
    pub p90_microseconds: u64,
    pub p95_microseconds: u64,
    pub p99_microseconds: u64,
    pub p99_9_microseconds: u64,
    pub max_microseconds: u64,
    pub min_microseconds: u64,
}

/// Latency target compliance monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyTargetCompliance {
    /// Percentage of operations meeting ≤1ms target
    pub operations_under_1ms_percent: f32,
    /// Percentage of operations meeting ≤500μs target (aggressive)
    pub operations_under_500us_percent: f32,
    /// Count of operations exceeding 1ms target (SLA violations)
    pub sla_violations_count: u64,
    /// Worst latency spike recorded
    pub worst_latency_spike_us: u64,
    /// Timestamp of worst latency spike
    pub worst_latency_spike_timestamp: u64,
    /// Overall SLA compliance score (0-100)
    pub overall_sla_score: f32,
}

/// Trading operation type for metrics categorization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradingOperation {
    OrderPlacement,
    OrderCancellation,
    OrderModification,
    PositionQuery,
    BalanceQuery,
    MarketDataProcessing,
    Authentication,
    JsonParsing,
    ZeroCopySerialization,
    NetworkRoundTrip,
    EndToEndWorkflow,
}

impl TradingOperation {
    /// Get the maximum acceptable latency for this operation in microseconds
    pub fn max_acceptable_latency_us(&self) -> u64 {
        match self {
            // Critical trading operations - must be under 1ms (1000μs)
            Self::OrderPlacement => 1000,
            Self::OrderCancellation => 1000,
            Self::OrderModification => 1000,
            Self::EndToEndWorkflow => 1000,
            
            // Data operations - can be slightly higher but still sub-millisecond
            Self::PositionQuery => 1500,
            Self::BalanceQuery => 1500,
            Self::MarketDataProcessing => 500, // High frequency data
            
            // Internal operations - should be very fast
            Self::Authentication => 100,
            Self::JsonParsing => 50,
            Self::ZeroCopySerialization => 25,
            
            // Network operations - external dependency
            Self::NetworkRoundTrip => 2000, // 2ms for network tolerance
        }
    }

    /// Get the aggressive performance target (50% of max acceptable)
    pub fn aggressive_target_us(&self) -> u64 {
        self.max_acceptable_latency_us() / 2
    }
}

/// Real-time latency measurement and tracking
#[derive(Debug)]
pub struct TradingLatencyTracker {
    /// Operation latency histograms
    operation_histograms: Arc<RwLock<HashMap<TradingOperation, Vec<u64>>>>,
    /// Atomic counters for fast increments
    operation_counters: HashMap<TradingOperation, AtomicU64>,
    /// SLA violation counters
    sla_violation_counters: HashMap<TradingOperation, AtomicU64>,
    /// Worst case latency tracking
    worst_case_latencies: Arc<RwLock<HashMap<TradingOperation, (u64, u64)>>>, // (latency_us, timestamp)
    /// Real-time compliance score
    compliance_score: AtomicU64, // Fixed-point representation (score * 100)
    /// Configuration
    config: TradingMetricsConfig,
}

/// Configuration for trading metrics collection
#[derive(Debug, Clone)]
pub struct TradingMetricsConfig {
    /// Maximum number of latency samples to keep per operation
    pub max_samples_per_operation: usize,
    /// Enable detailed percentile calculation
    pub enable_detailed_percentiles: bool,
    /// Alert threshold for SLA violations (percentage)
    pub sla_violation_alert_threshold: f32,
    /// Metrics aggregation interval
    pub aggregation_interval: Duration,
    /// Enable real-time latency alerting
    pub enable_real_time_alerting: bool,
}

impl Default for TradingMetricsConfig {
    fn default() -> Self {
        Self {
            max_samples_per_operation: 10000,
            enable_detailed_percentiles: true,
            sla_violation_alert_threshold: 1.0, // Alert if >1% violations
            aggregation_interval: Duration::from_secs(30),
            enable_real_time_alerting: true,
        }
    }
}

impl TradingLatencyTracker {
    pub fn new(config: TradingMetricsConfig) -> Self {
        let mut operation_counters = HashMap::new();
        let mut sla_violation_counters = HashMap::new();
        
        // Initialize counters for all operation types
        let operations = [
            TradingOperation::OrderPlacement,
            TradingOperation::OrderCancellation,
            TradingOperation::OrderModification,
            TradingOperation::PositionQuery,
            TradingOperation::BalanceQuery,
            TradingOperation::MarketDataProcessing,
            TradingOperation::Authentication,
            TradingOperation::JsonParsing,
            TradingOperation::ZeroCopySerialization,
            TradingOperation::NetworkRoundTrip,
            TradingOperation::EndToEndWorkflow,
        ];

        for op in &operations {
            operation_counters.insert(*op, AtomicU64::new(0));
            sla_violation_counters.insert(*op, AtomicU64::new(0));
        }

        Self {
            operation_histograms: Arc::new(RwLock::new(HashMap::new())),
            operation_counters,
            sla_violation_counters,
            worst_case_latencies: Arc::new(RwLock::new(HashMap::new())),
            compliance_score: AtomicU64::new(10000), // Start at 100.00%
            config,
        }
    }

    /// Record a latency measurement for a specific trading operation
    pub async fn record_latency(&self, operation: TradingOperation, latency: Duration) {
        let latency_us = latency.as_micros() as u64;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        
        // Increment operation counter
        self.operation_counters.get(&operation).unwrap().fetch_add(1, Ordering::Relaxed);
        
        // Check SLA compliance
        let max_acceptable = operation.max_acceptable_latency_us();
        if latency_us > max_acceptable {
            self.sla_violation_counters.get(&operation).unwrap().fetch_add(1, Ordering::Relaxed);
            
            if self.config.enable_real_time_alerting {
                warn!("🚨 SLA Violation: {:?} took {}μs (limit: {}μs)", 
                    operation, latency_us, max_acceptable);
            }
        }
        
        // Update worst case latency tracking
        {
            let mut worst_cases = self.worst_case_latencies.write().await;
            let current_worst = worst_cases.entry(operation).or_insert((0, 0));
            if latency_us > current_worst.0 {
                *current_worst = (latency_us, timestamp);
                debug!("New worst case latency for {:?}: {}μs", operation, latency_us);
            }
        }
        
        // Store latency sample for histogram analysis
        {
            let mut histograms = self.operation_histograms.write().await;
            let histogram = histograms.entry(operation).or_insert_with(Vec::new);
            
            // Keep histogram size bounded
            if histogram.len() >= self.config.max_samples_per_operation {
                histogram.remove(0); // Remove oldest sample
            }
            histogram.push(latency_us);
        }
        
        // Update real-time compliance score
        self.update_compliance_score().await;
    }

    /// Measure and record latency for a closure
    pub async fn measure_operation<F, R>(&self, operation: TradingOperation, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        let duration = start.elapsed();
        self.record_latency(operation, duration).await;
        result
    }

    /// Measure and record latency for an async closure
    pub async fn measure_async_operation<F, Fut, R>(&self, operation: TradingOperation, f: F) -> R
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        let start = Instant::now();
        let result = f().await;
        let duration = start.elapsed();
        self.record_latency(operation, duration).await;
        result
    }

    /// Calculate latency percentiles for an operation
    #[allow(dead_code)]
    async fn calculate_percentiles(&self, operation: TradingOperation) -> Option<LatencyPercentiles> {
        let histograms = self.operation_histograms.read().await;
        let histogram = histograms.get(&operation)?;
        
        if histogram.is_empty() {
            return None;
        }

        let mut sorted = histogram.clone();
        sorted.sort_unstable();
        
        let len = sorted.len();
        
        Some(LatencyPercentiles {
            p50_microseconds: sorted[len * 50 / 100],
            p90_microseconds: sorted[len * 90 / 100],
            p95_microseconds: sorted[len * 95 / 100],
            p99_microseconds: sorted[len * 99 / 100],
            p99_9_microseconds: sorted[len * 999 / 1000],
            max_microseconds: *sorted.last().unwrap(),
            min_microseconds: *sorted.first().unwrap(),
        })
    }

    /// Update the real-time compliance score
    async fn update_compliance_score(&self) {
        let mut total_operations = 0u64;
        let mut total_violations = 0u64;
        
        for counter in self.operation_counters.values() {
            total_operations += counter.load(Ordering::Relaxed);
        }
        
        for violation_counter in self.sla_violation_counters.values() {
            total_violations += violation_counter.load(Ordering::Relaxed);
        }
        
        if total_operations > 0 {
            let compliance_rate = ((total_operations - total_violations) as f64 / total_operations as f64) * 100.0;
            self.compliance_score.store((compliance_rate * 100.0) as u64, Ordering::Relaxed);
            
            // Alert if compliance drops below threshold
            if self.config.enable_real_time_alerting && 
               compliance_rate < (100.0 - self.config.sla_violation_alert_threshold as f64) {
                warn!("🚨 SLA Compliance Alert: {}% ({}/{} violations)", 
                    compliance_rate, total_violations, total_operations);
            }
        }
    }

    /// Generate comprehensive trading performance metrics
    pub async fn generate_metrics(&self) -> TradingPerformanceMetrics {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        
        // Calculate aggregate percentiles across all critical operations
        let critical_ops = [
            TradingOperation::OrderPlacement,
            TradingOperation::OrderCancellation,
            TradingOperation::OrderModification,
            TradingOperation::EndToEndWorkflow,
        ];
        
        let mut all_latencies = Vec::new();
        for op in &critical_ops {
            if let Some(histogram) = self.operation_histograms.read().await.get(op) {
                all_latencies.extend(histogram.iter().copied());
            }
        }
        
        all_latencies.sort_unstable();
        
        let latency_percentiles = if !all_latencies.is_empty() {
            let len = all_latencies.len();
            LatencyPercentiles {
                p50_microseconds: all_latencies[len * 50 / 100],
                p90_microseconds: all_latencies[len * 90 / 100],
                p95_microseconds: all_latencies[len * 95 / 100],
                p99_microseconds: all_latencies[len * 99 / 100],
                p99_9_microseconds: all_latencies[len * 999 / 1000],
                max_microseconds: *all_latencies.last().unwrap(),
                min_microseconds: *all_latencies.first().unwrap(),
            }
        } else {
            LatencyPercentiles {
                p50_microseconds: 0,
                p90_microseconds: 0,
                p95_microseconds: 0,
                p99_microseconds: 0,
                p99_9_microseconds: 0,
                max_microseconds: 0,
                min_microseconds: 0,
            }
        };
        
        // Calculate SLA compliance
        let total_operations: u64 = self.operation_counters.values()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum();
        
        let total_violations: u64 = self.sla_violation_counters.values()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum();
        
        let operations_under_1ms = total_operations.saturating_sub(total_violations);
        let operations_under_500us = all_latencies.iter()
            .filter(|&&latency| latency <= 500)
            .count() as u64;
        
        let worst_cases = self.worst_case_latencies.read().await;
        let (worst_latency, worst_timestamp) = worst_cases.values()
            .max_by_key(|(latency, _)| *latency)
            .copied()
            .unwrap_or((0, 0));
        
        let operations_under_1ms_percent = if total_operations > 0 {
            (operations_under_1ms as f32 / total_operations as f32) * 100.0
        } else { 100.0 };
        
        let operations_under_500us_percent = if !all_latencies.is_empty() {
            (operations_under_500us as f32 / all_latencies.len() as f32) * 100.0
        } else { 100.0 };
        
        let latency_target_compliance = LatencyTargetCompliance {
            operations_under_1ms_percent,
            operations_under_500us_percent,
            sla_violations_count: total_violations,
            worst_latency_spike_us: worst_latency,
            worst_latency_spike_timestamp: worst_timestamp,
            overall_sla_score: operations_under_1ms_percent,
        };
        
        // Get individual operation latencies
        let histograms = self.operation_histograms.read().await;
        let get_latest_latency = |op: TradingOperation| -> u64 {
            histograms.get(&op)
                .and_then(|histogram| histogram.last())
                .copied()
                .unwrap_or(0)
        };
        
        TradingPerformanceMetrics {
            timestamp,
            order_placement_latency_us: get_latest_latency(TradingOperation::OrderPlacement),
            order_cancellation_latency_us: get_latest_latency(TradingOperation::OrderCancellation),
            order_modification_latency_us: get_latest_latency(TradingOperation::OrderModification),
            position_query_latency_us: get_latest_latency(TradingOperation::PositionQuery),
            balance_query_latency_us: get_latest_latency(TradingOperation::BalanceQuery),
            websocket_message_processing_us: get_latest_latency(TradingOperation::MarketDataProcessing),
            json_parsing_latency_us: get_latest_latency(TradingOperation::JsonParsing),
            authentication_generation_us: get_latest_latency(TradingOperation::Authentication),
            zero_copy_serialization_us: get_latest_latency(TradingOperation::ZeroCopySerialization),
            network_round_trip_us: get_latest_latency(TradingOperation::NetworkRoundTrip),
            end_to_end_order_latency_us: get_latest_latency(TradingOperation::EndToEndWorkflow),
            latency_percentiles,
            latency_target_compliance,
            cpu_usage_during_trading: 0.0, // TODO: Integrate with system metrics
            memory_allocation_bytes: 0,     // TODO: Integrate with memory profiler
            gc_pressure_indicator: 0.0,     // TODO: Not applicable for Rust, but useful for monitoring
        }
    }

    /// Get real-time SLA compliance score (0-100)
    pub fn get_real_time_sla_score(&self) -> f32 {
        self.compliance_score.load(Ordering::Relaxed) as f32 / 100.0
    }

    /// Reset all metrics (useful for testing)
    pub async fn reset_metrics(&self) {
        for counter in self.operation_counters.values() {
            counter.store(0, Ordering::Relaxed);
        }
        for counter in self.sla_violation_counters.values() {
            counter.store(0, Ordering::Relaxed);
        }
        self.operation_histograms.write().await.clear();
        self.worst_case_latencies.write().await.clear();
        self.compliance_score.store(10000, Ordering::Relaxed);
    }
}

/// Global instance for trading metrics (singleton pattern)
static TRADING_METRICS: std::sync::OnceLock<TradingLatencyTracker> = std::sync::OnceLock::new();

/// Initialize global trading metrics tracker
pub fn initialize_trading_metrics(config: TradingMetricsConfig) {
    TRADING_METRICS.set(TradingLatencyTracker::new(config))
        .expect("Trading metrics already initialized");
    info!("✅ Initialized trading performance metrics tracker");
}

/// Get reference to global trading metrics tracker
pub fn get_trading_metrics() -> Option<&'static TradingLatencyTracker> {
    TRADING_METRICS.get()
}

/// Convenience macro for measuring trading operation latency
#[macro_export]
macro_rules! measure_trading_latency {
    ($operation:expr, $code:block) => {{
        if let Some(metrics) = $crate::api::trading_metrics::get_trading_metrics() {
            metrics.measure_operation($operation, || $code).await
        } else {
            $code
        }
    }};
}

/// Convenience macro for measuring async trading operation latency
#[macro_export]
macro_rules! measure_trading_latency_async {
    ($operation:expr, $code:block) => {{
        if let Some(metrics) = $crate::api::trading_metrics::get_trading_metrics() {
            metrics.measure_async_operation($operation, || async $code).await
        } else {
            $code
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_latency_tracking() {
        let config = TradingMetricsConfig::default();
        let tracker = TradingLatencyTracker::new(config);
        
        // Record some test latencies
        tracker.record_latency(TradingOperation::OrderPlacement, Duration::from_micros(500)).await;
        tracker.record_latency(TradingOperation::OrderPlacement, Duration::from_micros(800)).await;
        tracker.record_latency(TradingOperation::OrderPlacement, Duration::from_micros(1200)).await; // SLA violation
        
        let metrics = tracker.generate_metrics().await;
        assert_eq!(metrics.latency_target_compliance.sla_violations_count, 1);
        assert!(metrics.latency_target_compliance.operations_under_1ms_percent < 100.0);
        
        let sla_score = tracker.get_real_time_sla_score();
        assert!(sla_score > 0.0 && sla_score <= 100.0);
    }

    #[tokio::test]
    async fn test_measure_operation() {
        let config = TradingMetricsConfig::default();
        let tracker = TradingLatencyTracker::new(config);
        
        // Measure a simple operation
        let result = tracker.measure_operation(TradingOperation::JsonParsing, || {
            std::thread::sleep(Duration::from_micros(100));
            42
        }).await;
        
        assert_eq!(result, 42);
        
        let metrics = tracker.generate_metrics().await;
        assert!(metrics.json_parsing_latency_us > 0);
    }

    #[tokio::test]
    async fn test_percentile_calculation() {
        let config = TradingMetricsConfig::default();
        let tracker = TradingLatencyTracker::new(config);
        
        // Add a range of latencies
        for i in 1..=100 {
            tracker.record_latency(TradingOperation::OrderPlacement, Duration::from_micros(i * 10)).await;
        }
        
        let percentiles = tracker.calculate_percentiles(TradingOperation::OrderPlacement).await.unwrap();
        assert!(percentiles.p50_microseconds > 0);
        assert!(percentiles.p95_microseconds > percentiles.p50_microseconds);
        assert!(percentiles.p99_microseconds > percentiles.p95_microseconds);
        assert!(percentiles.max_microseconds >= percentiles.p99_microseconds);
        assert!(percentiles.min_microseconds <= percentiles.p50_microseconds);
    }
}