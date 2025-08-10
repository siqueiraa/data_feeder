//! CPU Topology and NUMA-Aware Threading Optimization
//! 
//! This module provides CPU topology detection and NUMA-aware thread configuration
//! to fix load imbalance and single-core bottlenecks detected in performance logs.


/// CPU topology information for performance optimization
#[derive(Debug, Clone)]
pub struct CpuTopology {
    pub total_cores: usize,
    pub numa_nodes: Vec<NumaNode>,
    pub performance_cores: Vec<usize>,
    pub efficiency_cores: Vec<usize>,
    pub recommended_thread_count: usize,
}

#[derive(Debug, Clone)]
pub struct NumaNode {
    pub id: usize,
    pub cores: Vec<usize>,
    pub memory_gb: f64,
}

impl CpuTopology {
    /// Detect CPU topology and provide optimization recommendations
    pub fn detect() -> Self {
        let total_cores = num_cpus::get();

        // For Apple Silicon (M1/M2/M3), detect P-cores vs E-cores
        let (performance_cores, efficiency_cores) = Self::detect_apple_silicon_cores(total_cores);
        
        // Create NUMA nodes (Apple Silicon is typically single NUMA node)
        let numa_nodes = Self::detect_numa_topology(total_cores, &performance_cores, &efficiency_cores);
        
        // Calculate optimal thread count based on workload type
        let recommended_thread_count = Self::calculate_optimal_threads(
            total_cores, 
            performance_cores.len(), 
            efficiency_cores.len()
        );

        Self {
            total_cores,
            numa_nodes,
            performance_cores,
            efficiency_cores,
            recommended_thread_count,
        }
    }

    /// Detect Apple Silicon P-cores vs E-cores
    fn detect_apple_silicon_cores(total_cores: usize) -> (Vec<usize>, Vec<usize>) {
        // Apple Silicon core layout detection
        let (p_cores, e_cores) = match total_cores {
            8 => (vec![0, 1, 2, 3], vec![4, 5, 6, 7]), // M1
            10 => (vec![0, 1, 2, 3, 4, 5, 6, 7], vec![8, 9]), // M1 Pro
            14 => (vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], vec![10, 11, 12, 13]), // M1 Max/M2 Pro
            16 => (vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], vec![12, 13, 14, 15]), // M2 Max
            24 => (vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], vec![16, 17, 18, 19, 20, 21, 22, 23]), // M3 Max
            _ => {
                // Generic detection for unknown configurations
                let p_count = (total_cores * 2 / 3).max(4); // Assume 2/3 are P-cores
                let p_cores: Vec<usize> = (0..p_count).collect();
                let e_cores: Vec<usize> = (p_count..total_cores).collect();
                (p_cores, e_cores)
            }
        };

        (p_cores, e_cores)
    }

    /// Detect NUMA topology
    fn detect_numa_topology(_total_cores: usize, p_cores: &[usize], e_cores: &[usize]) -> Vec<NumaNode> {
        // Apple Silicon is typically single NUMA node, but create logical separation
        vec![
            NumaNode {
                id: 0,
                cores: [p_cores, e_cores].concat(),
                memory_gb: 16.0, // Default assumption
            }
        ]
    }

    /// Calculate optimal thread count for mixed workloads
    fn calculate_optimal_threads(total_cores: usize, p_cores: usize, e_cores: usize) -> usize {
        // For CPU-intensive workloads like quantile calculations:
        // - Use all P-cores + some E-cores
        // - Leave 1-2 cores for system overhead
        let optimal = if p_cores >= 6 {
            // High-performance system: use most P-cores + half E-cores
            p_cores + (e_cores / 2)
        } else {
            // Lower-performance system: use all available cores - 1
            total_cores - 1
        };

        optimal.max(4).min(total_cores)
    }

    /// Get recommended thread pool configuration for different workload types
    pub fn get_thread_pool_config(&self) -> ThreadPoolConfig {
        ThreadPoolConfig {
            // CPU-intensive work (quantile calculations, sorting)
            cpu_intensive_threads: self.performance_cores.len(),
            
            // I/O work (async tasks, websocket, database)
            io_threads: self.recommended_thread_count,
            
            // Background work (metrics, cleanup)
            background_threads: self.efficiency_cores.len().max(2),

            // Core affinity hints
            prefer_p_cores: self.performance_cores.clone(),
            prefer_e_cores: self.efficiency_cores.clone(),
        }
    }

    /// Configure rayon thread pool for optimal CPU utilization
    pub fn configure_rayon_thread_pool(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cpu_threads = self.performance_cores.len();
        
        
        rayon::ThreadPoolBuilder::new()
            .num_threads(cpu_threads)
            .thread_name(|i| format!("rayon-cpu-{}", i))
            .build_global()?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ThreadPoolConfig {
    pub cpu_intensive_threads: usize,
    pub io_threads: usize,
    pub background_threads: usize,
    pub prefer_p_cores: Vec<usize>,
    pub prefer_e_cores: Vec<usize>,
}

/// Initialize optimal CPU configuration to fix single-core bottlenecks
pub async fn init_cpu_optimization() -> Result<CpuTopology, Box<dyn std::error::Error + Send + Sync>> {
    
    let topology = CpuTopology::detect();
    
    // Configure rayon for parallel quantile calculations
    topology.configure_rayon_thread_pool()?;
    
    let _config = topology.get_thread_pool_config();
    
    Ok(topology)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_detection() {
        let topology = CpuTopology::detect();
        assert!(topology.total_cores > 0);
        assert!(topology.recommended_thread_count > 0);
        assert!(topology.recommended_thread_count <= topology.total_cores);
    }

    #[test]
    fn test_thread_pool_config() {
        let topology = CpuTopology::detect();
        let config = topology.get_thread_pool_config();
        
        assert!(config.cpu_intensive_threads > 0);
        assert!(config.io_threads > 0);
        assert!(config.background_threads > 0);
    }
}