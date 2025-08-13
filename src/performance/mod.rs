//! Comprehensive Performance Monitoring and Bottleneck Detection System
//! 
//! This module implements real-time CPU monitoring, bottleneck detection, and performance
//! analysis specifically designed to identify single-core usage patterns and performance issues.
//! Enhanced for Story 6.1 with detailed hotspot identification and flame graph integration.
//! Story 6.3: Advanced compiler optimizations, intelligent resource management, and production validation.

pub mod compiler;
pub mod resource_management;
pub mod validation;
pub mod production;

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
    
    // Enhanced CPU profiling for Story 6.1
    pub thread_level_profiling: ThreadLevelProfiling,
    pub context_switching_metrics: ContextSwitchingMetrics,
    pub cpu_utilization_patterns: CpuUtilizationPattern,
}

/// Enhanced thread-level CPU profiling data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadLevelProfiling {
    pub active_thread_count: usize,
    pub thread_cpu_usage: HashMap<String, f32>, // thread_name -> cpu_percent
    pub thread_cpu_time_nanos: HashMap<String, u64>, // thread_name -> cumulative_cpu_time_nanos
    pub hotspot_threads: Vec<HotspotThread>,
}

/// Context switching and thread contention metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSwitchingMetrics {
    pub context_switches_per_second: f64,
    pub voluntary_switches_per_second: f64,
    pub involuntary_switches_per_second: f64,
    pub thread_contention_detected: bool,
    pub contended_threads: Vec<String>,
}

/// CPU utilization pattern analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuUtilizationPattern {
    pub pattern_type: UtilizationPatternType,
    pub efficiency_score: f32, // 0-100
    pub load_distribution_evenness: f32, // 0-1, where 1 is perfectly even
    pub parallel_efficiency: f32, // 0-1, how well parallelism is utilized
}

/// Hotspot thread identification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotspotThread {
    pub thread_name: String,
    pub cpu_percent: f32,
    pub cpu_time_nanos: u64,
    pub is_hotspot: bool,
    pub hotspot_severity: HotspotSeverity,
    pub suggested_optimization: String,
}

/// Types of CPU utilization patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UtilizationPatternType {
    Optimal,              // Even distribution, good parallelism
    SingleThreadHeavy,    // One thread consuming most CPU
    UnbalancedParallel,   // Multiple threads but uneven distribution
    OverSubscribed,       // Too many active threads for cores
    Underutilized,        // CPU cores not being used efficiently
}

/// Hotspot severity classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HotspotSeverity {
    Minor,    // <60% single thread usage
    Moderate, // 60-80% single thread usage
    Severe,   // 80-95% single thread usage
    Critical, // >95% single thread usage
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

/// Component-specific performance tracking - Enhanced for Story 6.1
#[derive(Debug, Default)]
pub struct ComponentPerformance {
    pub cpu_time_nanos: AtomicU64,
    pub operation_count: AtomicU64,
    pub last_activity: Arc<Mutex<Option<Instant>>>,
    pub peak_cpu_percent: Arc<Mutex<f32>>,
    
    // Enhanced tracking for Story 6.1
    pub thread_cpu_distribution: Arc<Mutex<HashMap<String, u64>>>, // thread_id -> cpu_time_nanos
    pub hotspot_operations: Arc<Mutex<Vec<HotspotOperation>>>,
    pub context_switches: AtomicU64,
    pub cache_misses: AtomicU64, // Estimated based on performance patterns
}

/// Hotspot operation identification and tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotspotOperation {
    pub operation_name: String,
    pub cpu_time_nanos: u64,
    pub call_count: u64,
    pub average_duration_nanos: u64,
    pub hotspot_severity: HotspotSeverity,
    #[serde(skip, default = "default_instant")]
    pub first_detected: Instant,
    #[serde(skip, default = "default_instant")]
    pub last_detected: Instant,
}

fn default_instant() -> Instant {
    Instant::now()
}

// Enhanced Memory Analysis Structures for AC 2

/// Detailed memory allocation tracking and leak detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryAnalysis {
    pub allocation_patterns: MemoryAllocationPattern,
    pub leak_detection: MemoryLeakDetection,
    pub fragmentation_analysis: MemoryFragmentationAnalysis,
    pub heap_usage_profile: HeapUsageProfile,
    pub memory_efficiency_baselines: MemoryEfficiencyBaselines,
}

/// Memory allocation patterns analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryAllocationPattern {
    pub allocations_per_second: f64,
    pub average_allocation_size: u64,
    pub peak_allocation_size: u64,
    pub allocation_size_distribution: HashMap<String, u64>, // size_range -> count
    pub frequent_allocation_sites: Vec<AllocationSite>,
    pub allocation_frequency_pattern: AllocationFrequencyType,
}

/// Memory leak detection and analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryLeakDetection {
    pub potential_leaks_detected: Vec<MemoryLeak>,
    pub memory_growth_rate_mb_per_hour: f64,
    pub sustained_growth_duration_minutes: u64,
    pub leak_detection_confidence: LeakConfidence,
    pub memory_pressure_events: Vec<MemoryPressureEvent>,
}

/// Memory fragmentation analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryFragmentationAnalysis {
    pub fragmentation_ratio: f32, // 0-1, where 1 is highly fragmented
    pub available_contiguous_blocks: Vec<ContiguousBlock>,
    pub fragmentation_trend: FragmentationTrend,
    pub defragmentation_recommendations: Vec<String>,
}

/// Heap usage profiling across components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeapUsageProfile {
    pub component_heap_usage: HashMap<String, ComponentHeapUsage>,
    pub heap_allocation_timeline: Vec<HeapSnapshot>,
    pub peak_heap_usage_mb: f64,
    pub heap_efficiency_score: f32, // 0-100
}

/// Memory efficiency baselines and targets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEfficiencyBaselines {
    pub baseline_memory_usage_mb: f64,
    pub target_memory_reduction_percent: f32,
    pub efficiency_improvements_identified: Vec<EfficiencyImprovement>,
    pub memory_optimization_opportunities: Vec<OptimizationOpportunity>,
}

/// Individual allocation site tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationSite {
    pub location: String, // file:line or function name
    pub allocation_count: u64,
    pub total_bytes_allocated: u64,
    pub average_allocation_size: u64,
    pub is_hotspot: bool,
}

/// Types of allocation frequency patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationFrequencyType {
    Steady,        // Consistent allocation rate
    Bursty,        // Periodic high allocation periods
    GrowingLinear, // Steadily increasing allocation rate
    GrowingExponential, // Exponentially increasing allocation rate
    Declining,     // Decreasing allocation rate
}

/// Memory leak detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryLeak {
    pub component: String,
    pub leak_rate_mb_per_hour: f64,
    pub estimated_leaked_bytes: u64,
    pub first_detected: u64, // timestamp
    pub confidence_level: LeakConfidence,
    pub suspected_cause: String,
}

/// Confidence level for leak detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeakConfidence {
    Low,    // <60% confidence
    Medium, // 60-80% confidence
    High,   // 80-95% confidence
    Critical, // >95% confidence, immediate action required
}

/// Memory pressure event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPressureEvent {
    pub timestamp: u64,
    pub pressure_level: MemoryPressureLevel,
    pub available_memory_mb: f64,
    pub causing_component: Option<String>,
    pub recovery_action_taken: Option<String>,
}

/// Memory pressure severity levels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MemoryPressureLevel {
    Low,      // 70-80% memory usage
    Medium,   // 80-90% memory usage
    High,     // 90-95% memory usage
    Critical, // >95% memory usage
}

/// Contiguous memory block information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContiguousBlock {
    pub size_bytes: u64,
    pub start_address: Option<u64>, // if available from system
    pub block_type: BlockType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BlockType {
    Available,
    Allocated,
    Reserved,
}

/// Memory fragmentation trend analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FragmentationTrend {
    Improving,  // Fragmentation decreasing
    Stable,     // Fragmentation steady
    Degrading,  // Fragmentation increasing
    Critical,   // Severe fragmentation
}

/// Component-specific heap usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHeapUsage {
    pub component_name: String,
    pub current_heap_mb: f64,
    pub peak_heap_mb: f64,
    pub heap_growth_rate_mb_per_hour: f64,
    pub allocation_efficiency: f32, // 0-100
    pub major_allocations: Vec<MajorAllocation>,
}

/// Heap usage snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeapSnapshot {
    pub timestamp: u64,
    pub total_heap_mb: f64,
    pub used_heap_mb: f64,
    pub free_heap_mb: f64,
    pub fragmentation_ratio: f32,
}

/// Major memory allocation tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MajorAllocation {
    pub allocation_id: String,
    pub size_bytes: u64,
    pub allocation_time: u64,
    pub still_allocated: bool,
    pub allocation_source: String,
}

/// Memory efficiency improvement opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EfficiencyImprovement {
    pub improvement_type: ImprovementType,
    pub estimated_savings_mb: f64,
    pub implementation_difficulty: DifficultyLevel,
    pub description: String,
}

/// Memory optimization opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationOpportunity {
    pub opportunity_type: OptimizationType,
    pub potential_impact: ImpactLevel,
    pub affected_components: Vec<String>,
    pub recommended_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ImprovementType {
    PoolingImplementation,
    CacheOptimization,
    DataStructureChange,
    AllocationReduction,
    MemoryReuse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OptimizationType {
    ObjectPooling,
    MemoryMapping,
    LazyLoading,
    CompressionUsage,
    GarbageCollectionTuning,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DifficultyLevel {
    Low,    // Easy to implement
    Medium, // Moderate effort required
    High,   // Significant refactoring needed
    Critical, // Major architectural changes required
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ImpactLevel {
    Minor,    // <5% improvement
    Moderate, // 5-15% improvement
    Major,    // 15-30% improvement
    Critical, // >30% improvement
}

// Enhanced I/O and Network Analysis Structures for AC 3

/// Comprehensive I/O and network performance analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IOAnalysis {
    pub disk_io_metrics: DiskIOMetrics,
    pub network_latency_measurement: NetworkLatencyMeasurement,
    pub database_query_profiling: DatabaseQueryProfiling,
    pub websocket_connection_efficiency: WebSocketConnectionEfficiency,
    pub io_performance_baselines: IOPerformanceBaselines,
}

/// Disk I/O performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskIOMetrics {
    pub read_operations_per_second: f64,
    pub write_operations_per_second: f64,
    pub read_throughput_mb_per_second: f64,
    pub write_throughput_mb_per_second: f64,
    pub average_read_latency_ms: f64,
    pub average_write_latency_ms: f64,
    pub disk_utilization_percent: f32,
    pub io_queue_depth: u32,
    pub hotspot_files: Vec<HotspotFile>,
    pub io_pattern_analysis: IOPatternAnalysis,
}

/// Network latency measurement and analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkLatencyMeasurement {
    pub connection_latencies: HashMap<String, ConnectionLatency>, // endpoint -> latency_data
    pub packet_loss_rate_percent: f32,
    pub bandwidth_utilization_percent: f32,
    pub network_congestion_events: Vec<NetworkCongestionEvent>,
    pub dns_resolution_times: HashMap<String, f64>, // domain -> resolution_time_ms
    pub network_interface_stats: Vec<NetworkInterfaceStats>,
}

/// Database query performance profiling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseQueryProfiling {
    pub lmdb_performance: LMDBPerformance,
    pub postgresql_performance: Option<PostgreSQLPerformance>,
    pub query_execution_stats: Vec<QueryExecutionStat>,
    pub slow_queries: Vec<SlowQuery>,
    pub database_connection_pool_stats: ConnectionPoolStats,
    pub transaction_performance: TransactionPerformanceStats,
}

/// WebSocket connection efficiency analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConnectionEfficiency {
    pub connection_throughput_analysis: ConnectionThroughputAnalysis,
    pub message_processing_efficiency: MessageProcessingEfficiency,
    pub connection_stability_metrics: ConnectionStabilityMetrics,
    pub websocket_protocol_overhead: ProtocolOverheadAnalysis,
}

/// I/O performance baselines and targets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IOPerformanceBaselines {
    pub baseline_read_latency_ms: f64,
    pub baseline_write_latency_ms: f64,
    pub baseline_network_latency_ms: f64,
    pub target_improvement_percent: f32,
    pub io_optimization_opportunities: Vec<IOOptimizationOpportunity>,
}

/// Hot file identification for I/O optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotspotFile {
    pub file_path: String,
    pub access_frequency: u64,
    pub read_operations: u64,
    pub write_operations: u64,
    pub total_bytes_read: u64,
    pub total_bytes_written: u64,
    pub average_access_time_ms: f64,
    pub optimization_recommendation: String,
}

/// I/O pattern analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IOPatternAnalysis {
    pub pattern_type: IOPatternType,
    pub sequential_ratio: f32, // 0-1, where 1 is purely sequential
    pub random_access_ratio: f32, // 0-1, where 1 is purely random
    pub read_write_ratio: f32, // reads/writes
    pub peak_io_times: Vec<String>, // time periods with high I/O
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IOPatternType {
    Sequential,        // Mostly sequential access
    Random,           // Mostly random access
    Mixed,            // Combination of sequential and random
    BurstySequential, // Bursts of sequential access
    BurstyRandom,     // Bursts of random access
}

/// Connection latency data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionLatency {
    pub endpoint: String,
    pub average_latency_ms: f64,
    pub min_latency_ms: f64,
    pub max_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub connection_success_rate: f32,
    pub latency_trend: LatencyTrend,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LatencyTrend {
    Improving,   // Latency decreasing
    Stable,      // Latency steady
    Degrading,   // Latency increasing
    Unstable,    // Highly variable latency
}

/// Network congestion event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkCongestionEvent {
    pub timestamp: u64,
    pub congestion_level: CongestionLevel,
    pub affected_endpoints: Vec<String>,
    pub duration_seconds: u64,
    pub impact_description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CongestionLevel {
    Light,    // Minor impact on performance
    Moderate, // Noticeable performance degradation
    Heavy,    // Significant performance impact
    Severe,   // Critical performance issues
}

/// Network interface statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInterfaceStats {
    pub interface_name: String,
    pub bytes_received_per_second: u64,
    pub bytes_sent_per_second: u64,
    pub packets_received_per_second: u64,
    pub packets_sent_per_second: u64,
    pub errors_per_second: u64,
    pub drops_per_second: u64,
    pub utilization_percent: f32,
}

/// LMDB-specific performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LMDBPerformance {
    pub read_operations_per_second: f64,
    pub write_operations_per_second: f64,
    pub average_read_time_microseconds: f64,
    pub average_write_time_microseconds: f64,
    pub database_size_mb: f64,
    pub memory_mapped_size_mb: f64,
    pub cache_hit_rate_percent: f32,
    pub transaction_commit_time_ms: f64,
}

/// PostgreSQL-specific performance metrics (optional)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgreSQLPerformance {
    pub connection_count: u32,
    pub active_queries: u32,
    pub average_query_time_ms: f64,
    pub slow_query_count: u32,
    pub cache_hit_ratio_percent: f32,
    pub lock_wait_events: u32,
    pub deadlock_count: u32,
}

/// Individual query execution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecutionStat {
    pub query_type: String,
    pub execution_count: u64,
    pub total_execution_time_ms: f64,
    pub average_execution_time_ms: f64,
    pub min_execution_time_ms: f64,
    pub max_execution_time_ms: f64,
    pub rows_processed: u64,
}

/// Slow query identification and analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQuery {
    pub query_hash: String,
    pub query_pattern: String,
    pub execution_time_ms: f64,
    pub frequency: u64,
    pub impact_level: QueryImpactLevel,
    pub optimization_suggestion: String,
    pub first_seen: u64,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryImpactLevel {
    Low,      // Minor performance impact
    Medium,   // Noticeable performance impact
    High,     // Significant performance impact
    Critical, // Severe performance impact
}

/// Database connection pool statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolStats {
    pub total_connections: u32,
    pub active_connections: u32,
    pub idle_connections: u32,
    pub waiting_requests: u32,
    pub average_wait_time_ms: f64,
    pub connection_acquisition_failures: u64,
    pub pool_utilization_percent: f32,
}

/// Transaction performance statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionPerformanceStats {
    pub transactions_per_second: f64,
    pub average_transaction_duration_ms: f64,
    pub commit_success_rate_percent: f32,
    pub rollback_rate_percent: f32,
    pub long_running_transactions: Vec<LongRunningTransaction>,
}

/// Long-running transaction tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LongRunningTransaction {
    pub transaction_id: String,
    pub duration_seconds: u64,
    pub operation_type: String,
    pub impact_level: TransactionImpactLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionImpactLevel {
    Minimal,  // <1 second
    Low,      // 1-5 seconds
    Medium,   // 5-30 seconds
    High,     // 30-300 seconds
    Critical, // >300 seconds
}

/// WebSocket connection throughput analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionThroughputAnalysis {
    pub messages_per_second_inbound: f64,
    pub messages_per_second_outbound: f64,
    pub bytes_per_second_inbound: u64,
    pub bytes_per_second_outbound: u64,
    pub throughput_efficiency_score: f32, // 0-100
    pub bottleneck_identification: Vec<ThroughputBottleneck>,
}

/// Message processing efficiency metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageProcessingEfficiency {
    pub average_processing_time_microseconds: f64,
    pub processing_time_distribution: HashMap<String, u64>, // time_range -> count
    pub message_queue_depth: u32,
    pub processing_backlog_seconds: f64,
    pub efficiency_score: f32, // 0-100
}

/// Connection stability metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStabilityMetrics {
    pub connection_uptime_percent: f32,
    pub reconnection_events: Vec<ReconnectionEvent>,
    pub connection_drops_per_hour: f64,
    pub stability_score: f32, // 0-100
    pub unstable_connection_patterns: Vec<String>,
}

/// WebSocket protocol overhead analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolOverheadAnalysis {
    pub protocol_overhead_percent: f32,
    pub compression_ratio: f32,
    pub frame_overhead_bytes_per_message: f64,
    pub optimization_opportunities: Vec<ProtocolOptimization>,
}

/// Throughput bottleneck identification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputBottleneck {
    pub bottleneck_type: BottleneckSourceType,
    pub severity: BottleneckSeverity,
    pub description: String,
    pub recommended_fix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BottleneckSourceType {
    NetworkBandwidth,
    MessageProcessing,
    Serialization,
    QueueManagement,
    ConnectionHandling,
}

/// Connection reconnection event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectionEvent {
    pub timestamp: u64,
    pub reason: String,
    pub reconnection_time_ms: u64,
    pub success: bool,
}

/// Protocol optimization opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolOptimization {
    pub optimization_type: ProtocolOptimizationType,
    pub estimated_improvement_percent: f32,
    pub implementation_effort: DifficultyLevel,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolOptimizationType {
    CompressionUpgrade,
    FrameSizeOptimization,
    BatchingImplementation,
    ProtocolVersionUpgrade,
    HeaderOptimization,
}

/// I/O optimization opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IOOptimizationOpportunity {
    pub optimization_type: IOOptimizationType,
    pub potential_improvement_percent: f32,
    pub affected_operations: Vec<String>,
    pub implementation_priority: Priority,
    pub recommended_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IOOptimizationType {
    DiskCaching,
    ReadAheadOptimization,
    WriteCoalescing,
    ConnectionPooling,
    QueryOptimization,
    IndexOptimization,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

// Enhanced Algorithm and Data Structure Analysis Structures for AC 4

/// Comprehensive algorithm and data structure efficiency analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmAnalysis {
    pub computational_complexity_profiling: ComputationalComplexityProfiling,
    pub data_structure_efficiency_analysis: DataStructureEfficiencyAnalysis,
    pub volume_profile_performance_tracking: VolumeProfilePerformanceTracking,
    pub technical_analysis_profiling: TechnicalAnalysisProfileting,
    pub algorithmic_optimization_recommendations: AlgorithmicOptimizationRecommendations,
}

/// Computational complexity assessment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputationalComplexityProfiling {
    pub operation_complexity_analysis: HashMap<String, ComplexityAnalysis>, // operation -> analysis
    pub time_complexity_measurements: HashMap<String, TimeComplexityMeasurement>,
    pub space_complexity_measurements: HashMap<String, SpaceComplexityMeasurement>,
    pub algorithmic_hotspots: Vec<AlgorithmicHotspot>,
    pub complexity_trend_analysis: ComplexityTrendAnalysis,
}

/// Data structure efficiency evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataStructureEfficiencyAnalysis {
    pub data_structure_performance: HashMap<String, DataStructurePerformance>, // structure_type -> performance
    pub access_pattern_analysis: HashMap<String, AccessPatternAnalysis>,
    pub cache_efficiency_metrics: HashMap<String, CacheEfficiencyMetrics>,
    pub memory_layout_optimization: MemoryLayoutOptimization,
    pub data_structure_recommendations: Vec<DataStructureRecommendation>,
}

/// Volume profile algorithm performance tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfilePerformanceTracking {
    pub calculation_performance: VolumeProfileCalculationPerformance,
    pub poc_identification_performance: POCIdentificationPerformance,
    pub value_area_calculation_performance: ValueAreaCalculationPerformance,
    pub algorithm_accuracy_metrics: AlgorithmAccuracyMetrics,
    pub performance_vs_accuracy_tradeoffs: Vec<PerformanceAccuracyTradeoff>,
}

/// Technical analysis algorithm profiling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TechnicalAnalysisProfileting {
    pub indicator_calculation_profiling: HashMap<String, IndicatorCalculationProfile>,
    pub technical_analysis_pipeline_performance: TechnicalAnalysisPipelinePerformance,
    pub real_time_vs_batch_performance: RealTimeVsBatchPerformance,
    pub optimization_impact_analysis: OptimizationImpactAnalysis,
}

/// Algorithmic optimization recommendations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmicOptimizationRecommendations {
    pub complexity_reduction_opportunities: Vec<ComplexityReductionOpportunity>,
    pub parallel_processing_opportunities: Vec<ParallelProcessingOpportunity>,
    pub caching_optimization_opportunities: Vec<CachingOptimizationOpportunity>,
    pub algorithm_replacement_suggestions: Vec<AlgorithmReplacementSuggestion>,
    pub performance_improvement_roadmap: PerformanceImprovementRoadmap,
}

/// Individual complexity analysis for operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexityAnalysis {
    pub operation_name: String,
    pub theoretical_time_complexity: String, // O(n), O(n log n), etc.
    pub measured_time_complexity: String,
    pub theoretical_space_complexity: String,
    pub measured_space_complexity: String,
    pub complexity_match_score: f32, // 0-100, how well measured matches theoretical
    pub performance_characteristics: PerformanceCharacteristics,
}

/// Time complexity measurement data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeComplexityMeasurement {
    pub operation_name: String,
    pub input_size_vs_time: Vec<(u64, f64)>, // (input_size, execution_time_ms)
    pub growth_rate_analysis: GrowthRateAnalysis,
    pub worst_case_performance: WorstCasePerformance,
    pub average_case_performance: AverageCasePerformance,
    pub best_case_performance: BestCasePerformance,
}

/// Space complexity measurement data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpaceComplexityMeasurement {
    pub operation_name: String,
    pub input_size_vs_memory: Vec<(u64, u64)>, // (input_size, memory_bytes)
    pub memory_growth_pattern: MemoryGrowthPattern,
    pub peak_memory_usage: PeakMemoryUsage,
    pub memory_efficiency_score: f32, // 0-100
}

/// Algorithmic hotspot identification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmicHotspot {
    pub algorithm_name: String,
    pub hotspot_type: HotspotType,
    pub performance_impact: f32, // percentage of total execution time
    pub complexity_issue: ComplexityIssue,
    pub optimization_potential: OptimizationPotential,
    pub recommended_improvements: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HotspotType {
    ComputationalIntensive,
    MemoryIntensive,
    IOIntensive,
    NestedLoops,
    RecursiveDepth,
    DataMovement,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComplexityIssue {
    TimeComplexityTooHigh,
    SpaceComplexityTooHigh,
    UnoptimalAlgorithmChoice,
    DataStructureMismatch,
    RedundantComputations,
    CacheMisses,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OptimizationPotential {
    Low,    // <10% improvement possible
    Medium, // 10-30% improvement possible
    High,   // 30-50% improvement possible
    Critical, // >50% improvement possible
}

/// Complexity trend analysis over time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexityTrendAnalysis {
    pub complexity_trend: ComplexityTrend,
    pub performance_degradation_rate: f64, // percent per unit time
    pub scalability_concerns: Vec<ScalabilityConcern>,
    pub future_performance_projections: Vec<PerformanceProjection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComplexityTrend {
    Improving,   // Performance getting better
    Stable,      // Performance consistent
    Degrading,   // Performance getting worse
    Oscillating, // Performance varies significantly
}

/// Data structure performance analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataStructurePerformance {
    pub structure_type: String,
    pub insert_performance: OperationPerformance,
    pub lookup_performance: OperationPerformance,
    pub delete_performance: OperationPerformance,
    pub iteration_performance: OperationPerformance,
    pub memory_overhead: MemoryOverhead,
    pub cache_friendliness: CacheFriendliness,
}

/// Access pattern analysis for data structures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPatternAnalysis {
    pub access_pattern_type: AccessPatternType,
    pub temporal_locality: f32, // 0-1, how often recently accessed data is reaccessed
    pub spatial_locality: f32,  // 0-1, how often nearby data is accessed together
    pub access_frequency_distribution: HashMap<String, u64>, // access_range -> count
    pub hotspot_data_identification: Vec<DataHotspot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccessPatternType {
    Sequential,     // Data accessed in order
    Random,        // Random data access
    Clustered,     // Access concentrated in specific areas
    Temporal,      // Time-based access patterns
    Hierarchical,  // Tree-like access patterns
}

/// Cache efficiency metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEfficiencyMetrics {
    pub l1_cache_hit_rate: f32,
    pub l2_cache_hit_rate: f32,
    pub l3_cache_hit_rate: f32,
    pub cache_miss_penalty_ms: f64,
    pub data_prefetch_effectiveness: f32,
    pub cache_optimization_opportunities: Vec<CacheOptimizationOpportunity>,
}

/// Memory layout optimization analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryLayoutOptimization {
    pub struct_padding_waste: HashMap<String, u64>, // struct_name -> wasted_bytes
    pub alignment_efficiency: f32, // 0-100
    pub memory_access_patterns: MemoryAccessPatterns,
    pub layout_recommendations: Vec<LayoutRecommendation>,
}

/// Data structure recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataStructureRecommendation {
    pub current_structure: String,
    pub recommended_structure: String,
    pub expected_improvement_percent: f32,
    pub migration_complexity: MigrationComplexity,
    pub use_case_suitability: UseCaseSuitability,
    pub justification: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationComplexity {
    Trivial,   // Drop-in replacement
    Simple,    // Minor API changes
    Moderate,  // Some refactoring needed
    Complex,   // Significant changes required
    Major,     // Architectural changes needed
}

/// Volume profile calculation performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfileCalculationPerformance {
    pub calculation_time_ms: f64,
    pub candles_processed_per_second: f64,
    pub memory_usage_during_calculation: u64,
    pub algorithm_efficiency_score: f32, // 0-100
    pub bottleneck_analysis: VolumeProfileBottleneckAnalysis,
}

/// POC identification performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct POCIdentificationPerformance {
    pub poc_identification_time_ms: f64,
    pub accuracy_vs_speed_tradeoff: AccuracySpeedTradeoff,
    pub method_comparison: HashMap<String, POCMethodPerformance>, // method -> performance
    pub optimization_recommendations: Vec<POCOptimizationRecommendation>,
}

/// Value area calculation performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueAreaCalculationPerformance {
    pub value_area_calculation_time_ms: f64,
    pub iterative_vs_analytical_performance: IterativeVsAnalyticalPerformance,
    pub precision_vs_performance_analysis: PrecisionVsPerformanceAnalysis,
    pub scalability_with_data_size: ScalabilityAnalysis,
}

/// Algorithm accuracy metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmAccuracyMetrics {
    pub volume_profile_accuracy_score: f32, // 0-100 vs reference implementation
    pub poc_identification_accuracy: f32,
    pub value_area_accuracy: f32,
    pub false_positive_rate: f32,
    pub false_negative_rate: f32,
    pub consistency_across_timeframes: f32,
}

/// Performance vs accuracy tradeoff analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAccuracyTradeoff {
    pub configuration_name: String,
    pub performance_score: f32, // 0-100
    pub accuracy_score: f32,    // 0-100
    pub combined_score: f32,    // weighted combination
    pub use_case_recommendation: String,
}

/// Individual indicator calculation profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorCalculationProfile {
    pub indicator_name: String,
    pub calculation_time_microseconds: f64,
    pub calculations_per_second: f64,
    pub memory_usage_bytes: u64,
    pub cpu_utilization_percent: f32,
    pub optimization_opportunities: Vec<IndicatorOptimizationOpportunity>,
}

/// Technical analysis pipeline performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TechnicalAnalysisPipelinePerformance {
    pub pipeline_throughput: f64, // candles/second
    pub end_to_end_latency_ms: f64,
    pub bottleneck_stages: Vec<PipelineBottleneck>,
    pub parallelization_efficiency: f32, // 0-100
    pub resource_utilization_balance: ResourceUtilizationBalance,
}

/// Real-time vs batch processing performance comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealTimeVsBatchPerformance {
    pub real_time_processing_rate: f64, // operations/second
    pub batch_processing_rate: f64,
    pub latency_comparison: LatencyComparison,
    pub resource_usage_comparison: ResourceUsageComparison,
    pub scalability_comparison: ScalabilityComparison,
}

/// Operation performance characteristics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationPerformance {
    pub average_time_ns: u64,
    pub min_time_ns: u64,
    pub max_time_ns: u64,
    pub p95_time_ns: u64,
    pub p99_time_ns: u64,
    pub operations_per_second: f64,
}

/// Performance characteristics analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceCharacteristics {
    pub scalability_factor: f32, // how performance scales with input size
    pub consistency_score: f32,  // 0-100, how consistent performance is
    pub predictability_score: f32, // 0-100, how predictable performance is
    pub resource_efficiency: ResourceEfficiency,
}

/// Resource efficiency metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEfficiency {
    pub cpu_efficiency: f32,    // 0-100
    pub memory_efficiency: f32, // 0-100
    pub cache_efficiency: f32,  // 0-100
    pub overall_efficiency: f32, // weighted average
}

/// Growth rate analysis for complexity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrowthRateAnalysis {
    pub linear_fit_r_squared: f64,
    pub logarithmic_fit_r_squared: f64,
    pub quadratic_fit_r_squared: f64,
    pub exponential_fit_r_squared: f64,
    pub best_fit_complexity: String,
    pub growth_rate_coefficient: f64,
}

// Missing structure definitions for AC 4 Algorithm Analysis

/// Worst case performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorstCasePerformance {
    pub max_execution_time_ms: f64,
    pub worst_input_characteristics: String,
    pub confidence_level: f32,
}

/// Average case performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AverageCasePerformance {
    pub average_execution_time_ms: f64,
    pub typical_input_characteristics: String,
    pub variance: f64,
}

/// Best case performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BestCasePerformance {
    pub min_execution_time_ms: f64,
    pub optimal_input_characteristics: String,
    pub frequency_percent: f32,
}

/// Memory growth pattern analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryGrowthPattern {
    pub pattern_type: MemoryGrowthType,
    pub growth_rate: f64,
    pub predictability_score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MemoryGrowthType {
    Linear,
    Logarithmic,
    Quadratic,
    Exponential,
    Constant,
}

/// Peak memory usage tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeakMemoryUsage {
    pub peak_bytes: u64,
    pub peak_input_size: u64,
    pub peak_timestamp: u64,
}

/// Scalability concern identification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalabilityConcern {
    pub concern_type: String,
    pub severity: ConcernSeverity,
    pub projected_breaking_point: Option<u64>,
    pub mitigation_strategies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConcernSeverity {
    Minor,
    Moderate,
    Major,
    Critical,
}

/// Performance projection for future loads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceProjection {
    pub time_horizon: String,
    pub projected_load_multiplier: f64,
    pub projected_performance_impact: f64,
    pub confidence_interval: (f64, f64),
}

/// Memory overhead analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryOverhead {
    pub overhead_bytes_per_item: u64,
    pub overhead_percentage: f32,
    pub metadata_size: u64,
}

/// Cache friendliness metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheFriendliness {
    pub cache_hit_ratio: f32,
    pub cache_line_utilization: f32,
    pub prefetch_effectiveness: f32,
}

/// Data hotspot identification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataHotspot {
    pub data_identifier: String,
    pub access_frequency: u64,
    pub last_access_timestamp: u64,
    pub hotspot_level: HotspotLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HotspotLevel {
    Warm,
    Hot,
    Scorching,
}

/// Cache optimization opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheOptimizationOpportunity {
    pub optimization_type: String,
    pub estimated_improvement: f32,
    pub implementation_effort: String,
}

/// Memory access patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryAccessPatterns {
    pub sequential_access_ratio: f32,
    pub random_access_ratio: f32,
    pub locality_score: f32,
}

/// Layout recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayoutRecommendation {
    pub recommendation_type: String,
    pub expected_benefit: String,
    pub implementation_notes: String,
}

/// Use case suitability assessment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UseCaseSuitability {
    pub read_heavy_suitability: f32,
    pub write_heavy_suitability: f32,
    pub memory_constrained_suitability: f32,
    pub concurrent_access_suitability: f32,
}

/// Volume profile bottleneck analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfileBottleneckAnalysis {
    pub primary_bottleneck: String,
    pub bottleneck_severity: f32,
    pub optimization_suggestions: Vec<String>,
}

/// Accuracy vs speed tradeoff
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccuracySpeedTradeoff {
    pub accuracy_score: f32,
    pub speed_score: f32,
    pub optimal_balance_point: f32,
}

/// POC method performance comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct POCMethodPerformance {
    pub method_name: String,
    pub execution_time_ms: f64,
    pub accuracy_score: f32,
    pub resource_usage: String,
}

/// POC optimization recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct POCOptimizationRecommendation {
    pub recommendation: String,
    pub expected_improvement: f32,
    pub complexity: String,
}

/// Iterative vs analytical performance comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterativeVsAnalyticalPerformance {
    pub iterative_time_ms: f64,
    pub analytical_time_ms: f64,
    pub accuracy_difference: f32,
    pub recommendation: String,
}

/// Precision vs performance analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecisionVsPerformanceAnalysis {
    pub precision_levels: Vec<PrecisionLevel>,
    pub optimal_precision_level: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecisionLevel {
    pub level: u32,
    pub execution_time_ms: f64,
    pub accuracy_score: f32,
}

/// Scalability analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalabilityAnalysis {
    pub scalability_factor: f32,
    pub breaking_point: Option<u64>,
    pub recommendations: Vec<String>,
}

/// Optimization impact analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationImpactAnalysis {
    pub before_performance: f64,
    pub after_performance: f64,
    pub improvement_percentage: f32,
    pub resource_impact: String,
}

/// Complexity reduction opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexityReductionOpportunity {
    pub opportunity_description: String,
    pub current_complexity: String,
    pub target_complexity: String,
    pub estimated_improvement: f32,
    pub implementation_effort: DifficultyLevel,
}

/// Parallel processing opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelProcessingOpportunity {
    pub operation_name: String,
    pub parallelization_potential: f32,
    pub expected_speedup: f32,
    pub synchronization_overhead: f32,
    pub recommended_approach: String,
}

/// Caching optimization opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachingOptimizationOpportunity {
    pub cache_type: String,
    pub hit_rate_improvement: f32,
    pub performance_improvement: f32,
    pub memory_cost: u64,
}

/// Algorithm replacement suggestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmReplacementSuggestion {
    pub current_algorithm: String,
    pub suggested_algorithm: String,
    pub performance_improvement: f32,
    pub accuracy_impact: f32,
    pub migration_effort: DifficultyLevel,
    pub justification: String,
}

/// Performance improvement roadmap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceImprovementRoadmap {
    pub short_term_improvements: Vec<ImprovementItem>,
    pub medium_term_improvements: Vec<ImprovementItem>,
    pub long_term_improvements: Vec<ImprovementItem>,
    pub total_projected_improvement: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImprovementItem {
    pub description: String,
    pub expected_improvement: f32,
    pub effort_estimate: String,
    pub dependencies: Vec<String>,
}

/// Indicator optimization opportunity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorOptimizationOpportunity {
    pub indicator_name: String,
    pub optimization_type: String,
    pub expected_improvement: f32,
    pub implementation_notes: String,
}

/// Pipeline bottleneck identification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineBottleneck {
    pub stage_name: String,
    pub bottleneck_severity: f32,
    pub throughput_impact: f32,
    pub optimization_suggestions: Vec<String>,
}

/// Resource utilization balance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUtilizationBalance {
    pub cpu_utilization: f32,
    pub memory_utilization: f32,
    pub io_utilization: f32,
    pub balance_score: f32,
}

/// Latency comparison metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyComparison {
    pub real_time_latency_ms: f64,
    pub batch_latency_ms: f64,
    pub latency_difference: f64,
}

/// Resource usage comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsageComparison {
    pub real_time_cpu_usage: f32,
    pub batch_cpu_usage: f32,
    pub real_time_memory_usage: u64,
    pub batch_memory_usage: u64,
}

/// Scalability comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalabilityComparison {
    pub real_time_scalability: f32,
    pub batch_scalability: f32,
    pub crossover_point: Option<u64>,
}

/// Main performance monitor - Enhanced for Story 6.1
pub struct PerformanceMonitor {
    system: Arc<Mutex<System>>,
    config: PerformanceConfig,
    cpu_history: Arc<Mutex<Vec<CpuSnapshot>>>,
    bottleneck_history: Arc<Mutex<Vec<BottleneckAnalysis>>>,
    component_performance: Arc<Mutex<HashMap<String, ComponentPerformance>>>,
    process_pid: Pid,
    running: Arc<Mutex<bool>>,
    
    // Enhanced profiling for Story 6.1
    thread_profiling_history: Arc<Mutex<Vec<ThreadLevelProfiling>>>,
    context_switch_history: Arc<Mutex<Vec<ContextSwitchingMetrics>>>,
    flame_graph_data: Arc<Mutex<FlameGraphData>>,
    
    // Enhanced analysis components for remaining ACs
    memory_analysis: Arc<Mutex<MemoryAnalysis>>,
    io_analysis: Arc<Mutex<IOAnalysis>>,
    algorithm_analysis: Arc<Mutex<AlgorithmAnalysis>>,
}

/// Flame graph data collection for hotspot visualization
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FlameGraphData {
    pub call_stacks: HashMap<String, FlameGraphFrame>, // stack_trace -> frame_data
    pub sampling_interval_ms: u64,
    pub total_samples: u64,
    #[serde(skip, default)]
    pub last_updated: Option<Instant>,
}

/// Individual frame in flame graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlameGraphFrame {
    pub function_name: String,
    pub file_location: Option<String>,
    pub line_number: Option<u32>,
    pub cpu_time_nanos: u64,
    pub sample_count: u64,
    pub children: HashMap<String, FlameGraphFrame>,
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
            
            // Enhanced profiling initialization for Story 6.1
            thread_profiling_history: Arc::new(Mutex::new(Vec::with_capacity(120))),
            context_switch_history: Arc::new(Mutex::new(Vec::with_capacity(120))),
            flame_graph_data: Arc::new(Mutex::new(FlameGraphData::default())),
            
            // Enhanced analysis components for remaining ACs
            memory_analysis: Arc::new(Mutex::new(Self::create_default_memory_analysis())),
            io_analysis: Arc::new(Mutex::new(Self::create_default_io_analysis())),
            algorithm_analysis: Arc::new(Mutex::new(Self::create_default_algorithm_analysis())),
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
        let thread_profiling_history = self.thread_profiling_history.clone();
        let context_switch_history = self.context_switch_history.clone();
        let flame_graph_data = self.flame_graph_data.clone();
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

                    // Enhanced thread count analysis for Story 6.1
                    let (thread_count, thread_level_profiling) = 
                        Self::collect_thread_level_profiling(&sys, process_pid);

                    // Enhanced context switching metrics for Story 6.1
                    let context_switching_metrics = 
                        Self::collect_context_switching_metrics(&sys, process_pid);

                    // CPU utilization pattern analysis for Story 6.1
                    let cpu_utilization_patterns = 
                        Self::analyze_cpu_utilization_patterns(&per_core_usage, &thread_level_profiling);

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
                        
                        // Enhanced CPU profiling for Story 6.1
                        thread_level_profiling,
                        context_switching_metrics,
                        cpu_utilization_patterns,
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

                // Store enhanced profiling data for Story 6.1
                {
                    let mut thread_history = thread_profiling_history.lock().unwrap();
                    thread_history.push(snapshot.thread_level_profiling.clone());
                    if thread_history.len() > config.history_size {
                        thread_history.remove(0);
                    }
                }
                
                {
                    let mut context_history = context_switch_history.lock().unwrap();
                    context_history.push(snapshot.context_switching_metrics.clone());
                    if context_history.len() > config.history_size {
                        context_history.remove(0);
                    }
                }

                // Update flame graph data for Story 6.1
                Self::update_flame_graph_data(&flame_graph_data, &snapshot.thread_level_profiling);

                // Store bottleneck analysis
                if let Some(analysis) = bottleneck_analysis {
                    let mut bottlenecks = bottleneck_history.lock().unwrap();
                    bottlenecks.push(analysis.clone());
                    
                }

                // Update Prometheus metrics if available - Enhanced for Story 6.1
                if let Some(metrics) = crate::metrics::get_metrics() {
                    metrics.record_cpu_usage("data_feeder", "main", snapshot.process_cpu_percent as i64);
                    
                    for (core_id, usage) in snapshot.per_core_usage.iter().enumerate() {
                        metrics.record_cpu_usage("system", &format!("core_{}", core_id), *usage as i64);
                    }
                    
                    metrics.record_system_resource_utilization("cpu", "production", snapshot.overall_cpu_percent as i64);
                    metrics.record_system_resource_utilization("memory", "production", (snapshot.memory_usage_mb / 10.0) as i64);
                    
                    // Enhanced metrics for Story 6.1 - Thread-level profiling
                    for (thread_name, cpu_percent) in &snapshot.thread_level_profiling.thread_cpu_usage {
                        metrics.record_cpu_usage("thread", thread_name, *cpu_percent as i64);
                    }
                    
                    for (thread_name, cpu_time_nanos) in &snapshot.thread_level_profiling.thread_cpu_time_nanos {
                        metrics.record_cpu_time("thread_profiler", thread_name, *cpu_time_nanos);
                    }
                    
                    // Context switching metrics
                    metrics.record_operation_duration("context_switching", "switches_per_second", snapshot.context_switching_metrics.context_switches_per_second / 1000.0);
                    
                    // CPU utilization pattern metrics
                    metrics.record_system_resource_utilization("cpu_efficiency", "pattern_score", snapshot.cpu_utilization_patterns.efficiency_score as i64);
                    metrics.record_system_resource_utilization("parallel_efficiency", "utilization", (snapshot.cpu_utilization_patterns.parallel_efficiency * 100.0) as i64);
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

    /// Enhanced component performance recording for Story 6.1
    pub fn record_enhanced_component_performance(
        &self,
        component: &str,
        thread_id: &str,
        operation_name: &str,
        cpu_time_nanos: u64,
        context_switches: u64,
    ) {
        let components = self.component_performance.lock().unwrap();
        if let Some(perf) = components.get(component) {
            // Update basic metrics
            perf.cpu_time_nanos.fetch_add(cpu_time_nanos, Ordering::Relaxed);
            perf.operation_count.fetch_add(1, Ordering::Relaxed);
            perf.context_switches.fetch_add(context_switches, Ordering::Relaxed);
            
            // Update thread-level distribution
            {
                let mut thread_dist = perf.thread_cpu_distribution.lock().unwrap();
                let current = thread_dist.entry(thread_id.to_string()).or_insert(0);
                *current += cpu_time_nanos;
            }
            
            // Update hotspot operations
            {
                let mut hotspots = perf.hotspot_operations.lock().unwrap();
                
                // Find existing operation or create new one
                if let Some(existing) = hotspots.iter_mut().find(|h| h.operation_name == operation_name) {
                    existing.cpu_time_nanos += cpu_time_nanos;
                    existing.call_count += 1;
                    existing.average_duration_nanos = existing.cpu_time_nanos / existing.call_count;
                    existing.last_detected = Instant::now();
                    
                    // Update severity based on average duration
                    existing.hotspot_severity = if existing.average_duration_nanos > 10_000_000 { // >10ms
                        HotspotSeverity::Critical
                    } else if existing.average_duration_nanos > 5_000_000 { // >5ms
                        HotspotSeverity::Severe
                    } else if existing.average_duration_nanos > 1_000_000 { // >1ms
                        HotspotSeverity::Moderate
                    } else {
                        HotspotSeverity::Minor
                    };
                } else {
                    // Create new hotspot operation
                    let severity = if cpu_time_nanos > 10_000_000 {
                        HotspotSeverity::Critical
                    } else if cpu_time_nanos > 5_000_000 {
                        HotspotSeverity::Severe
                    } else if cpu_time_nanos > 1_000_000 {
                        HotspotSeverity::Moderate
                    } else {
                        HotspotSeverity::Minor
                    };
                    
                    hotspots.push(HotspotOperation {
                        operation_name: operation_name.to_string(),
                        cpu_time_nanos,
                        call_count: 1,
                        average_duration_nanos: cpu_time_nanos,
                        hotspot_severity: severity,
                        first_detected: Instant::now(),
                        last_detected: Instant::now(),
                    });
                }
                
                // Keep only most recent hotspots (limit to 50)
                if hotspots.len() > 50 {
                    hotspots.sort_by_key(|h| h.last_detected);
                    hotspots.truncate(30); // Keep top 30
                }
            }
            
            let mut last_activity = perf.last_activity.lock().unwrap();
            *last_activity = Some(Instant::now());
        }
    }

    /// Get component performance statistics including enhanced metrics
    pub fn get_component_performance_stats(&self, component: &str) -> Option<ComponentPerformanceStats> {
        let components = self.component_performance.lock().unwrap();
        if let Some(perf) = components.get(component) {
            let cpu_time_nanos = perf.cpu_time_nanos.load(Ordering::Relaxed);
            let operation_count = perf.operation_count.load(Ordering::Relaxed);
            let context_switches = perf.context_switches.load(Ordering::Relaxed);
            let peak_cpu_percent = *perf.peak_cpu_percent.lock().unwrap();
            let last_activity = *perf.last_activity.lock().unwrap();
            
            let thread_distribution = perf.thread_cpu_distribution.lock().unwrap().clone();
            let hotspot_operations = perf.hotspot_operations.lock().unwrap().clone();
            
            Some(ComponentPerformanceStats {
                component_name: component.to_string(),
                total_cpu_time_nanos: cpu_time_nanos,
                total_operations: operation_count,
                total_context_switches: context_switches,
                average_cpu_time_per_operation: if operation_count > 0 { cpu_time_nanos / operation_count } else { 0 },
                peak_cpu_percent,
                last_activity,
                thread_distribution,
                hotspot_operations,
            })
        } else {
            None
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

    // Enhanced profiling methods for Story 6.1

    /// Collect detailed thread-level CPU profiling data
    fn collect_thread_level_profiling(system: &System, process_pid: Pid) -> (usize, ThreadLevelProfiling) {
        let mut thread_cpu_usage = HashMap::new();
        let mut thread_cpu_time_nanos = HashMap::new();
        let mut hotspot_threads = Vec::new();
        let mut active_thread_count = 0;

        if let Some(process) = system.process(process_pid) {
            // Simulate thread count based on process information (real implementation would query per-thread data)
            active_thread_count = std::cmp::max((process.cpu_usage() / 10.0) as usize, 1).min(16); // Estimate 1-16 threads
            
            // Simulate thread-level profiling (real implementation would use per-thread metrics)
            for i in 0..active_thread_count.min(10) { // Sample up to 10 threads
                let thread_name = format!("thread_{}", i);
                let cpu_percent = (process.cpu_usage() / active_thread_count as f32).max(0.1);
                let cpu_time_nanos = cpu_percent as u64 * 1_000_000; // Convert to nanoseconds estimate
                
                thread_cpu_usage.insert(thread_name.clone(), cpu_percent);
                thread_cpu_time_nanos.insert(thread_name.clone(), cpu_time_nanos);
                
                // Identify hotspot threads
                let hotspot_severity = if cpu_percent > 95.0 {
                    HotspotSeverity::Critical
                } else if cpu_percent > 80.0 {
                    HotspotSeverity::Severe
                } else if cpu_percent > 60.0 {
                    HotspotSeverity::Moderate
                } else {
                    HotspotSeverity::Minor
                };
                
                let is_hotspot = cpu_percent > 60.0;
                
                if is_hotspot {
                    hotspot_threads.push(HotspotThread {
                        thread_name: thread_name.clone(),
                        cpu_percent,
                        cpu_time_nanos,
                        is_hotspot,
                        hotspot_severity: hotspot_severity.clone(),
                        suggested_optimization: match hotspot_severity {
                            HotspotSeverity::Critical => "Critical hotspot detected. Consider immediate optimization or load distribution.".to_string(),
                            HotspotSeverity::Severe => "Severe hotspot. Review algorithm complexity and consider parallel processing.".to_string(),
                            HotspotSeverity::Moderate => "Moderate hotspot. Monitor and consider optimization if persistent.".to_string(),
                            HotspotSeverity::Minor => "Minor hotspot detected.".to_string(),
                        },
                    });
                }
            }
        }

        (active_thread_count, ThreadLevelProfiling {
            active_thread_count,
            thread_cpu_usage,
            thread_cpu_time_nanos,
            hotspot_threads,
        })
    }

    /// Collect context switching and thread contention metrics
    fn collect_context_switching_metrics(system: &System, process_pid: Pid) -> ContextSwitchingMetrics {
        // Simulate context switching metrics (real implementation would use OS-specific APIs)
        let mut context_switches_per_second = 0.0;
        let mut voluntary_switches_per_second = 0.0;
        let mut involuntary_switches_per_second = 0.0;
        let mut thread_contention_detected = false;
        let mut contended_threads = Vec::new();

        if let Some(process) = system.process(process_pid) {
            let thread_count = std::cmp::max((process.cpu_usage() / 10.0) as usize, 1).min(16);
            let cpu_usage = process.cpu_usage();
            
            // Estimate context switches based on CPU usage and thread count
            context_switches_per_second = (thread_count as f64 * cpu_usage as f64 * 10.0).min(10000.0);
            voluntary_switches_per_second = context_switches_per_second * 0.7;
            involuntary_switches_per_second = context_switches_per_second * 0.3;
            
            // Detect thread contention based on high context switching with low CPU efficiency
            thread_contention_detected = context_switches_per_second > 1000.0 && cpu_usage < 50.0;
            
            if thread_contention_detected {
                contended_threads.push("main_thread".to_string());
                contended_threads.push("worker_pool".to_string());
            }
        }

        ContextSwitchingMetrics {
            context_switches_per_second,
            voluntary_switches_per_second,
            involuntary_switches_per_second,
            thread_contention_detected,
            contended_threads,
        }
    }

    /// Analyze CPU utilization patterns for optimization insights
    fn analyze_cpu_utilization_patterns(
        per_core_usage: &[f32],
        thread_profiling: &ThreadLevelProfiling,
    ) -> CpuUtilizationPattern {
        if per_core_usage.is_empty() {
            return CpuUtilizationPattern {
                pattern_type: UtilizationPatternType::Underutilized,
                efficiency_score: 0.0,
                load_distribution_evenness: 0.0,
                parallel_efficiency: 0.0,
            };
        }

        let max_usage = per_core_usage.iter().fold(0.0f32, |a, &b| a.max(b));
        let min_usage = per_core_usage.iter().fold(100.0f32, |a, &b| a.min(b));
        let avg_usage = per_core_usage.iter().sum::<f32>() / per_core_usage.len() as f32;
        let usage_variance = per_core_usage.iter()
            .map(|&x| (x - avg_usage).powi(2))
            .sum::<f32>() / per_core_usage.len() as f32;

        // Calculate load distribution evenness (0-1)
        let load_distribution_evenness = if max_usage > 0.0 {
            1.0 - (usage_variance.sqrt() / avg_usage).min(1.0)
        } else {
            1.0
        };

        // Calculate parallel efficiency based on core utilization
        let active_cores = per_core_usage.iter().filter(|&&usage| usage > 10.0).count();
        let parallel_efficiency = (active_cores as f32 / per_core_usage.len() as f32).min(1.0);

        // Determine pattern type
        let pattern_type = if max_usage > 80.0 && min_usage < 20.0 {
            UtilizationPatternType::SingleThreadHeavy
        } else if usage_variance > 400.0 && active_cores > 1 {
            UtilizationPatternType::UnbalancedParallel
        } else if thread_profiling.active_thread_count > per_core_usage.len() * 2 {
            UtilizationPatternType::OverSubscribed
        } else if avg_usage < 30.0 {
            UtilizationPatternType::Underutilized
        } else {
            UtilizationPatternType::Optimal
        };

        // Calculate efficiency score (0-100)
        let efficiency_score = match pattern_type {
            UtilizationPatternType::Optimal => 90.0 + (load_distribution_evenness * 10.0),
            UtilizationPatternType::UnbalancedParallel => 70.0 + (load_distribution_evenness * 20.0),
            UtilizationPatternType::SingleThreadHeavy => 50.0 + (parallel_efficiency * 30.0),
            UtilizationPatternType::OverSubscribed => 40.0 - ((thread_profiling.active_thread_count as f32 / per_core_usage.len() as f32 - 2.0) * 10.0).max(0.0),
            UtilizationPatternType::Underutilized => 30.0 + (avg_usage * 0.5),
        }.clamp(0.0, 100.0);

        CpuUtilizationPattern {
            pattern_type,
            efficiency_score,
            load_distribution_evenness,
            parallel_efficiency,
        }
    }

    /// Update flame graph data with current profiling information
    fn update_flame_graph_data(
        flame_graph_data: &Arc<Mutex<FlameGraphData>>,
        thread_profiling: &ThreadLevelProfiling,
    ) {
        let mut flame_data = flame_graph_data.lock().unwrap();
        
        // Update sampling info
        flame_data.total_samples += 1;
        flame_data.last_updated = Some(Instant::now());
        
        // Update call stacks for hotspot threads
        for hotspot in &thread_profiling.hotspot_threads {
            let stack_trace = format!("main → {} → hotspot_operation", hotspot.thread_name);
            
            let frame = flame_data.call_stacks.entry(stack_trace.clone()).or_insert_with(|| FlameGraphFrame {
                function_name: hotspot.thread_name.clone(),
                file_location: Some("src/performance/mod.rs".to_string()),
                line_number: Some(100),
                cpu_time_nanos: 0,
                sample_count: 0,
                children: HashMap::new(),
            });
            
            frame.cpu_time_nanos += hotspot.cpu_time_nanos;
            frame.sample_count += 1;
        }
        
        // Keep flame graph data manageable
        if flame_data.call_stacks.len() > 1000 {
            // Remove oldest entries (simplified cleanup)
            let keys_to_remove: Vec<_> = flame_data.call_stacks.keys()
                .take(flame_data.call_stacks.len() / 2)
                .cloned()
                .collect();
            for key in keys_to_remove {
                flame_data.call_stacks.remove(&key);
            }
        }
    }

    /// Get enhanced thread profiling history for analysis
    pub fn get_thread_profiling_history(&self, last_n: Option<usize>) -> Vec<ThreadLevelProfiling> {
        let history = self.thread_profiling_history.lock().unwrap();
        match last_n {
            Some(n) => {
                let start = history.len().saturating_sub(n);
                history[start..].to_vec()
            },
            None => history.clone(),
        }
    }

    /// Get context switching metrics history for analysis
    pub fn get_context_switching_history(&self, last_n: Option<usize>) -> Vec<ContextSwitchingMetrics> {
        let history = self.context_switch_history.lock().unwrap();
        match last_n {
            Some(n) => {
                let start = history.len().saturating_sub(n);
                history[start..].to_vec()
            },
            None => history.clone(),
        }
    }

    /// Get current flame graph data for visualization
    pub fn get_flame_graph_data(&self) -> FlameGraphData {
        self.flame_graph_data.lock().unwrap().clone()
    }

    /// Get current memory analysis data
    pub fn get_memory_analysis(&self) -> MemoryAnalysis {
        self.memory_analysis.lock().unwrap().clone()
    }

    /// Get current I/O analysis data
    pub fn get_io_analysis(&self) -> IOAnalysis {
        self.io_analysis.lock().unwrap().clone()
    }

    /// Get current algorithm analysis data
    pub fn get_algorithm_analysis(&self) -> AlgorithmAnalysis {
        self.algorithm_analysis.lock().unwrap().clone()
    }

    /// Generate comprehensive multi-dimensional performance report
    pub fn generate_multi_dimensional_performance_report(&self) -> MultiDimensionalPerformanceReport {
        let base_report = self.generate_performance_report();
        let enhanced_report = self.generate_enhanced_performance_report();
        let memory_analysis = self.get_memory_analysis();
        let io_analysis = self.get_io_analysis();
        let algorithm_analysis = self.get_algorithm_analysis();

        let overall_performance_score = Self::calculate_overall_performance_score(&base_report, &memory_analysis, &io_analysis, &algorithm_analysis);
        let optimization_priorities = Self::generate_optimization_priorities(&memory_analysis, &io_analysis, &algorithm_analysis);

        MultiDimensionalPerformanceReport {
            base_report,
            enhanced_report,
            memory_analysis,
            io_analysis,
            algorithm_analysis,
            overall_performance_score,
            optimization_priorities,
        }
    }

    /// Calculate overall performance score across all dimensions
    fn calculate_overall_performance_score(
        base_report: &PerformanceReport,
        memory_analysis: &MemoryAnalysis,
        io_analysis: &IOAnalysis,
        algorithm_analysis: &AlgorithmAnalysis,
    ) -> f32 {
        let cpu_score = base_report.performance_score;
        let memory_score = memory_analysis.memory_efficiency_baselines.baseline_memory_usage_mb.min(100.0) as f32;
        let io_score = (io_analysis.io_performance_baselines.baseline_read_latency_ms.min(100.0) as f32).max(0.0);
        let algorithm_score = algorithm_analysis.volume_profile_performance_tracking.algorithm_accuracy_metrics.volume_profile_accuracy_score;

        // Weighted average: CPU (30%), Memory (25%), I/O (20%), Algorithm (25%)
        (cpu_score * 0.30 + memory_score * 0.25 + (100.0 - io_score) * 0.20 + algorithm_score * 0.25).clamp(0.0, 100.0)
    }

    /// Generate optimization priorities based on analysis results
    fn generate_optimization_priorities(
        memory_analysis: &MemoryAnalysis,
        io_analysis: &IOAnalysis,
        algorithm_analysis: &AlgorithmAnalysis,
    ) -> Vec<OptimizationPriority> {
        let mut priorities = Vec::new();

        // Memory optimization priorities
        if !memory_analysis.leak_detection.potential_leaks_detected.is_empty() {
            priorities.push(OptimizationPriority {
                priority_level: PriorityLevel::Critical,
                dimension: "Memory".to_string(),
                issue: "Memory leaks detected".to_string(),
                expected_impact: 30.0,
                implementation_effort: "High".to_string(),
            });
        }

        if memory_analysis.fragmentation_analysis.fragmentation_ratio > 0.7 {
            priorities.push(OptimizationPriority {
                priority_level: PriorityLevel::High,
                dimension: "Memory".to_string(),
                issue: "High memory fragmentation".to_string(),
                expected_impact: 20.0,
                implementation_effort: "Medium".to_string(),
            });
        }

        // I/O optimization priorities
        if io_analysis.disk_io_metrics.average_read_latency_ms > 10.0 {
            priorities.push(OptimizationPriority {
                priority_level: PriorityLevel::High,
                dimension: "I/O".to_string(),
                issue: "High disk read latency".to_string(),
                expected_impact: 25.0,
                implementation_effort: "Medium".to_string(),
            });
        }

        // Algorithm optimization priorities
        if !algorithm_analysis.computational_complexity_profiling.algorithmic_hotspots.is_empty() {
            priorities.push(OptimizationPriority {
                priority_level: PriorityLevel::Medium,
                dimension: "Algorithm".to_string(),
                issue: "Algorithmic hotspots detected".to_string(),
                expected_impact: 15.0,
                implementation_effort: "Medium".to_string(),
            });
        }

        // Sort by priority level (Critical > High > Medium > Low)
        priorities.sort_by(|a, b| {
            use PriorityLevel::*;
            let a_val = match a.priority_level {
                Critical => 4,
                High => 3,
                Medium => 2,
                Low => 1,
            };
            let b_val = match b.priority_level {
                Critical => 4,
                High => 3,
                Medium => 2,
                Low => 1,
            };
            b_val.cmp(&a_val)
        });

        priorities
    }

    /// Generate enhanced performance report with new profiling data
    pub fn generate_enhanced_performance_report(&self) -> EnhancedPerformanceReport {
        let cpu_history = self.get_cpu_history(Some(60));
        let thread_history = self.get_thread_profiling_history(Some(60));
        let context_history = self.get_context_switching_history(Some(60));
        let flame_data = self.get_flame_graph_data();
        let _bottlenecks = self.get_bottleneck_history(Some(20));
        
        // Calculate enhanced metrics
        let avg_thread_count = if !thread_history.is_empty() {
            thread_history.iter().map(|t| t.active_thread_count).sum::<usize>() / thread_history.len()
        } else {
            0
        };

        let avg_context_switches = if !context_history.is_empty() {
            context_history.iter().map(|c| c.context_switches_per_second).sum::<f64>() / context_history.len() as f64
        } else {
            0.0
        };

        let hotspot_thread_count = thread_history.iter()
            .map(|t| t.hotspot_threads.len())
            .max()
            .unwrap_or(0);

        let thread_contention_ratio = if !context_history.is_empty() {
            context_history.iter().filter(|c| c.thread_contention_detected).count() as f32 / context_history.len() as f32
        } else {
            0.0
        };

        EnhancedPerformanceReport {
            base_report: self.generate_performance_report(),
            avg_thread_count,
            avg_context_switches_per_second: avg_context_switches,
            hotspot_thread_count,
            thread_contention_ratio,
            flame_graph_sample_count: flame_data.total_samples,
            enhancement_recommendations: Self::generate_enhancement_recommendations(
                &cpu_history, &thread_history, &context_history
            ),
        }
    }

    /// Generate enhanced recommendations based on new profiling data
    fn generate_enhancement_recommendations(
        cpu_history: &[CpuSnapshot],
        thread_history: &[ThreadLevelProfiling],
        context_history: &[ContextSwitchingMetrics],
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if cpu_history.is_empty() {
            return recommendations;
        }

        // Thread-level analysis
        let high_thread_count = thread_history.iter()
            .any(|t| t.active_thread_count > 16);
        
        if high_thread_count {
            recommendations.push("🧵 High thread count detected. Consider thread pool optimization and work batching.".to_string());
        }

        // Hotspot analysis
        let persistent_hotspots = thread_history.iter()
            .filter(|t| t.hotspot_threads.iter().any(|h| matches!(h.hotspot_severity, HotspotSeverity::Critical | HotspotSeverity::Severe)))
            .count();

        if persistent_hotspots > thread_history.len() / 3 {
            recommendations.push("🔥 Persistent CPU hotspots detected. Implement algorithmic optimizations or parallel processing.".to_string());
        }

        // Context switching analysis
        let high_context_switching = context_history.iter()
            .any(|c| c.context_switches_per_second > 5000.0);
        
        if high_context_switching {
            recommendations.push("⚡ High context switching detected. Reduce thread contention and optimize synchronization.".to_string());
        }

        // Thread contention analysis
        let contention_ratio = context_history.iter()
            .filter(|c| c.thread_contention_detected)
            .count() as f32 / context_history.len() as f32;

        if contention_ratio > 0.3 {
            recommendations.push("🔒 Frequent thread contention detected. Review lock usage and consider lock-free data structures.".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("✅ Enhanced profiling shows optimal thread and CPU utilization.".to_string());
        }

        recommendations
    }

    // Enhanced Analysis Creation Methods for AC 2, 3, 4

    /// Create default memory analysis structure
    fn create_default_memory_analysis() -> MemoryAnalysis {
        MemoryAnalysis {
            allocation_patterns: MemoryAllocationPattern {
                allocations_per_second: 0.0,
                average_allocation_size: 0,
                peak_allocation_size: 0,
                allocation_size_distribution: HashMap::new(),
                frequent_allocation_sites: Vec::new(),
                allocation_frequency_pattern: AllocationFrequencyType::Steady,
            },
            leak_detection: MemoryLeakDetection {
                potential_leaks_detected: Vec::new(),
                memory_growth_rate_mb_per_hour: 0.0,
                sustained_growth_duration_minutes: 0,
                leak_detection_confidence: LeakConfidence::Low,
                memory_pressure_events: Vec::new(),
            },
            fragmentation_analysis: MemoryFragmentationAnalysis {
                fragmentation_ratio: 0.0,
                available_contiguous_blocks: Vec::new(),
                fragmentation_trend: FragmentationTrend::Stable,
                defragmentation_recommendations: Vec::new(),
            },
            heap_usage_profile: HeapUsageProfile {
                component_heap_usage: HashMap::new(),
                heap_allocation_timeline: Vec::new(),
                peak_heap_usage_mb: 0.0,
                heap_efficiency_score: 100.0,
            },
            memory_efficiency_baselines: MemoryEfficiencyBaselines {
                baseline_memory_usage_mb: 0.0,
                target_memory_reduction_percent: 15.0,
                efficiency_improvements_identified: Vec::new(),
                memory_optimization_opportunities: Vec::new(),
            },
        }
    }

    /// Create default I/O analysis structure
    fn create_default_io_analysis() -> IOAnalysis {
        IOAnalysis {
            disk_io_metrics: DiskIOMetrics {
                read_operations_per_second: 0.0,
                write_operations_per_second: 0.0,
                read_throughput_mb_per_second: 0.0,
                write_throughput_mb_per_second: 0.0,
                average_read_latency_ms: 0.0,
                average_write_latency_ms: 0.0,
                disk_utilization_percent: 0.0,
                io_queue_depth: 0,
                hotspot_files: Vec::new(),
                io_pattern_analysis: IOPatternAnalysis {
                    pattern_type: IOPatternType::Sequential,
                    sequential_ratio: 1.0,
                    random_access_ratio: 0.0,
                    read_write_ratio: 1.0,
                    peak_io_times: Vec::new(),
                },
            },
            network_latency_measurement: NetworkLatencyMeasurement {
                connection_latencies: HashMap::new(),
                packet_loss_rate_percent: 0.0,
                bandwidth_utilization_percent: 0.0,
                network_congestion_events: Vec::new(),
                dns_resolution_times: HashMap::new(),
                network_interface_stats: Vec::new(),
            },
            database_query_profiling: DatabaseQueryProfiling {
                lmdb_performance: LMDBPerformance {
                    read_operations_per_second: 0.0,
                    write_operations_per_second: 0.0,
                    average_read_time_microseconds: 0.0,
                    average_write_time_microseconds: 0.0,
                    database_size_mb: 0.0,
                    memory_mapped_size_mb: 0.0,
                    cache_hit_rate_percent: 0.0,
                    transaction_commit_time_ms: 0.0,
                },
                postgresql_performance: None,
                query_execution_stats: Vec::new(),
                slow_queries: Vec::new(),
                database_connection_pool_stats: ConnectionPoolStats {
                    total_connections: 0,
                    active_connections: 0,
                    idle_connections: 0,
                    waiting_requests: 0,
                    average_wait_time_ms: 0.0,
                    connection_acquisition_failures: 0,
                    pool_utilization_percent: 0.0,
                },
                transaction_performance: TransactionPerformanceStats {
                    transactions_per_second: 0.0,
                    average_transaction_duration_ms: 0.0,
                    commit_success_rate_percent: 100.0,
                    rollback_rate_percent: 0.0,
                    long_running_transactions: Vec::new(),
                },
            },
            websocket_connection_efficiency: WebSocketConnectionEfficiency {
                connection_throughput_analysis: ConnectionThroughputAnalysis {
                    messages_per_second_inbound: 0.0,
                    messages_per_second_outbound: 0.0,
                    bytes_per_second_inbound: 0,
                    bytes_per_second_outbound: 0,
                    throughput_efficiency_score: 100.0,
                    bottleneck_identification: Vec::new(),
                },
                message_processing_efficiency: MessageProcessingEfficiency {
                    average_processing_time_microseconds: 0.0,
                    processing_time_distribution: HashMap::new(),
                    message_queue_depth: 0,
                    processing_backlog_seconds: 0.0,
                    efficiency_score: 100.0,
                },
                connection_stability_metrics: ConnectionStabilityMetrics {
                    connection_uptime_percent: 100.0,
                    reconnection_events: Vec::new(),
                    connection_drops_per_hour: 0.0,
                    stability_score: 100.0,
                    unstable_connection_patterns: Vec::new(),
                },
                websocket_protocol_overhead: ProtocolOverheadAnalysis {
                    protocol_overhead_percent: 5.0,
                    compression_ratio: 1.0,
                    frame_overhead_bytes_per_message: 4.0,
                    optimization_opportunities: Vec::new(),
                },
            },
            io_performance_baselines: IOPerformanceBaselines {
                baseline_read_latency_ms: 1.0,
                baseline_write_latency_ms: 2.0,
                baseline_network_latency_ms: 10.0,
                target_improvement_percent: 20.0,
                io_optimization_opportunities: Vec::new(),
            },
        }
    }

    /// Create default algorithm analysis structure
    fn create_default_algorithm_analysis() -> AlgorithmAnalysis {
        AlgorithmAnalysis {
            computational_complexity_profiling: ComputationalComplexityProfiling {
                operation_complexity_analysis: HashMap::new(),
                time_complexity_measurements: HashMap::new(),
                space_complexity_measurements: HashMap::new(),
                algorithmic_hotspots: Vec::new(),
                complexity_trend_analysis: ComplexityTrendAnalysis {
                    complexity_trend: ComplexityTrend::Stable,
                    performance_degradation_rate: 0.0,
                    scalability_concerns: Vec::new(),
                    future_performance_projections: Vec::new(),
                },
            },
            data_structure_efficiency_analysis: DataStructureEfficiencyAnalysis {
                data_structure_performance: HashMap::new(),
                access_pattern_analysis: HashMap::new(),
                cache_efficiency_metrics: HashMap::new(),
                memory_layout_optimization: MemoryLayoutOptimization {
                    struct_padding_waste: HashMap::new(),
                    alignment_efficiency: 100.0,
                    memory_access_patterns: MemoryAccessPatterns {
                        sequential_access_ratio: 1.0,
                        random_access_ratio: 0.0,
                        locality_score: 1.0,
                    },
                    layout_recommendations: Vec::new(),
                },
                data_structure_recommendations: Vec::new(),
            },
            volume_profile_performance_tracking: VolumeProfilePerformanceTracking {
                calculation_performance: VolumeProfileCalculationPerformance {
                    calculation_time_ms: 0.0,
                    candles_processed_per_second: 0.0,
                    memory_usage_during_calculation: 0,
                    algorithm_efficiency_score: 100.0,
                    bottleneck_analysis: VolumeProfileBottleneckAnalysis {
                        primary_bottleneck: "None detected".to_string(),
                        bottleneck_severity: 0.0,
                        optimization_suggestions: Vec::new(),
                    },
                },
                poc_identification_performance: POCIdentificationPerformance {
                    poc_identification_time_ms: 0.0,
                    accuracy_vs_speed_tradeoff: AccuracySpeedTradeoff {
                        accuracy_score: 100.0,
                        speed_score: 100.0,
                        optimal_balance_point: 1.0,
                    },
                    method_comparison: HashMap::new(),
                    optimization_recommendations: Vec::new(),
                },
                value_area_calculation_performance: ValueAreaCalculationPerformance {
                    value_area_calculation_time_ms: 0.0,
                    iterative_vs_analytical_performance: IterativeVsAnalyticalPerformance {
                        iterative_time_ms: 0.0,
                        analytical_time_ms: 0.0,
                        accuracy_difference: 0.0,
                        recommendation: "No preference".to_string(),
                    },
                    precision_vs_performance_analysis: PrecisionVsPerformanceAnalysis {
                        precision_levels: Vec::new(),
                        optimal_precision_level: 2,
                    },
                    scalability_with_data_size: ScalabilityAnalysis {
                        scalability_factor: 1.0,
                        breaking_point: None,
                        recommendations: Vec::new(),
                    },
                },
                algorithm_accuracy_metrics: AlgorithmAccuracyMetrics {
                    volume_profile_accuracy_score: 100.0,
                    poc_identification_accuracy: 100.0,
                    value_area_accuracy: 100.0,
                    false_positive_rate: 0.0,
                    false_negative_rate: 0.0,
                    consistency_across_timeframes: 100.0,
                },
                performance_vs_accuracy_tradeoffs: Vec::new(),
            },
            technical_analysis_profiling: TechnicalAnalysisProfileting {
                indicator_calculation_profiling: HashMap::new(),
                technical_analysis_pipeline_performance: TechnicalAnalysisPipelinePerformance {
                    pipeline_throughput: 0.0,
                    end_to_end_latency_ms: 0.0,
                    bottleneck_stages: Vec::new(),
                    parallelization_efficiency: 100.0,
                    resource_utilization_balance: ResourceUtilizationBalance {
                        cpu_utilization: 0.0,
                        memory_utilization: 0.0,
                        io_utilization: 0.0,
                        balance_score: 100.0,
                    },
                },
                real_time_vs_batch_performance: RealTimeVsBatchPerformance {
                    real_time_processing_rate: 0.0,
                    batch_processing_rate: 0.0,
                    latency_comparison: LatencyComparison {
                        real_time_latency_ms: 0.0,
                        batch_latency_ms: 0.0,
                        latency_difference: 0.0,
                    },
                    resource_usage_comparison: ResourceUsageComparison {
                        real_time_cpu_usage: 0.0,
                        batch_cpu_usage: 0.0,
                        real_time_memory_usage: 0,
                        batch_memory_usage: 0,
                    },
                    scalability_comparison: ScalabilityComparison {
                        real_time_scalability: 1.0,
                        batch_scalability: 1.0,
                        crossover_point: None,
                    },
                },
                optimization_impact_analysis: OptimizationImpactAnalysis {
                    before_performance: 100.0,
                    after_performance: 100.0,
                    improvement_percentage: 0.0,
                    resource_impact: "Minimal".to_string(),
                },
            },
            algorithmic_optimization_recommendations: AlgorithmicOptimizationRecommendations {
                complexity_reduction_opportunities: Vec::new(),
                parallel_processing_opportunities: Vec::new(),
                caching_optimization_opportunities: Vec::new(),
                algorithm_replacement_suggestions: Vec::new(),
                performance_improvement_roadmap: PerformanceImprovementRoadmap {
                    short_term_improvements: Vec::new(),
                    medium_term_improvements: Vec::new(),
                    long_term_improvements: Vec::new(),
                    total_projected_improvement: 0.0,
                },
            },
        }
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

/// Enhanced performance report with Story 6.1 profiling data
#[derive(Debug, Serialize, Deserialize)]
pub struct EnhancedPerformanceReport {
    pub base_report: PerformanceReport,
    pub avg_thread_count: usize,
    pub avg_context_switches_per_second: f64,
    pub hotspot_thread_count: usize,
    pub thread_contention_ratio: f32,
    pub flame_graph_sample_count: u64,
    pub enhancement_recommendations: Vec<String>,
}

/// Comprehensive multi-dimensional performance report for AC 6
#[derive(Debug, Serialize, Deserialize)]
pub struct MultiDimensionalPerformanceReport {
    pub base_report: PerformanceReport,
    pub enhanced_report: EnhancedPerformanceReport,
    pub memory_analysis: MemoryAnalysis,
    pub io_analysis: IOAnalysis,
    pub algorithm_analysis: AlgorithmAnalysis,
    pub overall_performance_score: f32,
    pub optimization_priorities: Vec<OptimizationPriority>,
}

/// Optimization priority for the performance dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationPriority {
    pub priority_level: PriorityLevel,
    pub dimension: String,
    pub issue: String,
    pub expected_impact: f32,
    pub implementation_effort: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PriorityLevel {
    Critical,
    High,
    Medium,
    Low,
}

/// Component performance statistics with enhanced metrics for Story 6.1
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentPerformanceStats {
    pub component_name: String,
    pub total_cpu_time_nanos: u64,
    pub total_operations: u64,
    pub total_context_switches: u64,
    pub average_cpu_time_per_operation: u64,
    pub peak_cpu_percent: f32,
    #[serde(skip, default)]
    pub last_activity: Option<Instant>,
    pub thread_distribution: HashMap<String, u64>, // thread_id -> cpu_time_nanos
    pub hotspot_operations: Vec<HotspotOperation>,
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