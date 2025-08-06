use crate::system_resources::SystemResources;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// Adaptive configuration parameters that adjust based on system resources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    /// Thread pool configurations
    pub thread_pools: ThreadPoolConfig,
    /// Buffer size configurations  
    pub buffers: BufferConfig,
    /// Processing batch size configurations
    pub processing: ProcessingConfig,
    /// Memory management configurations
    pub memory: MemoryConfig,
    /// Manual overrides for all adaptive parameters
    pub overrides: ConfigOverrides,
}

/// Thread pool size configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadPoolConfig {
    /// Main worker thread pool size
    pub worker_threads: usize,
    /// I/O intensive operations thread pool size
    pub io_threads: usize,
    /// CPU intensive operations thread pool size
    pub cpu_threads: usize,
    /// Network operations thread pool size
    pub network_threads: usize,
}

/// Buffer size configurations (in bytes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferConfig {
    /// WebSocket message buffer size
    pub websocket_buffer_size: usize,
    /// Database batch buffer size  
    pub database_buffer_size: usize,
    /// File I/O buffer size
    pub file_io_buffer_size: usize,
    /// Network buffer size
    pub network_buffer_size: usize,
}

/// Processing batch size configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Database batch size (number of records)
    pub database_batch_size: usize,
    /// API request batch size
    pub api_batch_size: usize,
    /// File processing chunk size
    pub file_chunk_size: usize,
    /// Memory processing chunk size
    pub memory_chunk_size: usize,
}

/// Memory management configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Maximum memory usage percentage (0-100)
    pub max_memory_usage_percent: f64,
    /// Memory cleanup threshold percentage (0-100)
    pub cleanup_threshold_percent: f64,
    /// Cache size limits in MB
    pub cache_size_mb: usize,
    /// Enable/disable memory-intensive features
    pub enable_memory_intensive_features: bool,
}

/// Manual overrides for adaptive configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigOverrides {
    /// Manual thread pool size overrides
    pub thread_pools: Option<ThreadPoolConfig>,
    /// Manual buffer size overrides
    pub buffers: Option<BufferConfig>,
    /// Manual processing configuration overrides
    pub processing: Option<ProcessingConfig>,
    /// Manual memory configuration overrides
    pub memory: Option<MemoryConfig>,
    /// Disable adaptive configuration entirely
    pub disable_adaptive: bool,
}

/// TOML-compatible adaptive configuration for config.toml
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AdaptiveConfigToml {
    /// Enable adaptive configuration system
    pub enabled: Option<bool>,
    /// Manual overrides section
    pub overrides: Option<ConfigOverridesToml>,
}

/// TOML-compatible overrides
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ConfigOverridesToml {
    /// Thread pool overrides
    pub worker_threads: Option<usize>,
    pub io_threads: Option<usize>,
    pub cpu_threads: Option<usize>,
    pub network_threads: Option<usize>,
    /// Buffer overrides (in MB for user convenience)
    pub websocket_buffer_mb: Option<usize>,
    pub database_buffer_mb: Option<usize>,
    pub file_io_buffer_mb: Option<usize>,
    pub network_buffer_mb: Option<usize>,
    /// Processing overrides
    pub database_batch_size: Option<usize>,
    pub api_batch_size: Option<usize>,
    pub file_chunk_size: Option<usize>,
    pub memory_chunk_size: Option<usize>,
    /// Memory overrides
    pub max_memory_usage_percent: Option<f64>,
    pub cleanup_threshold_percent: Option<f64>,
    pub cache_size_mb: Option<usize>,
    pub enable_memory_intensive_features: Option<bool>,
    /// Disable adaptive system entirely
    pub disable_adaptive: Option<bool>,
}

impl AdaptiveConfig {
    /// Create adaptive configuration based on detected system resources
    pub fn from_system_resources(
        resources: &SystemResources,
        toml_overrides: Option<AdaptiveConfigToml>,
    ) -> Self {
        info!("🧠 Generating adaptive configuration based on system resources...");
        
        let config_start = std::time::Instant::now();
        
        // Check if adaptive configuration is disabled
        let overrides_toml = toml_overrides.as_ref().and_then(|c| c.overrides.as_ref());
        if toml_overrides.as_ref().map(|c| c.enabled.unwrap_or(true)) == Some(false) ||
           overrides_toml.map(|o| o.disable_adaptive.unwrap_or(false)) == Some(true) {
            info!("📋 Adaptive configuration disabled, using static defaults");
            return Self::default_static_config();
        }
        
        // Generate base configuration from system resources
        let thread_pools = Self::calculate_thread_pools(resources);
        let buffers = Self::calculate_buffers(resources);
        let processing = Self::calculate_processing(resources);
        let memory = Self::calculate_memory(resources);
        
        // Apply manual overrides if provided
        let overrides = Self::create_overrides(overrides_toml);
        let final_config = Self::apply_overrides(
            AdaptiveConfig {
                thread_pools,
                buffers,
                processing,
                memory,
                overrides: overrides.clone(),
            },
            &overrides,
        );
        
        let config_elapsed = config_start.elapsed();
        info!("✅ Adaptive configuration generated in {:?}", config_elapsed);
        
        // Log the final configuration
        Self::log_configuration(&final_config, resources);
        
        final_config
    }
    
    /// Calculate optimal thread pool configurations
    fn calculate_thread_pools(resources: &SystemResources) -> ThreadPoolConfig {
        let cpu_cores = resources.cpu_cores;
        
        // Base calculations
        let worker_threads = resources.get_recommended_thread_pool_size();
        
        // I/O threads: Can be higher than CPU cores since they're mostly blocking
        let io_threads = (cpu_cores * 2).clamp(2, 16);
        
        // CPU threads: Conservative, actual CPU-bound work
        let cpu_threads = (cpu_cores.saturating_sub(1)).clamp(1, 8);
        
        // Network threads: Moderate, for concurrent connections
        let network_threads = (cpu_cores / 2).clamp(1, 4);
        
        debug!("🧵 Thread pool calculation: worker={}, io={}, cpu={}, network={}", 
               worker_threads, io_threads, cpu_threads, network_threads);
        
        ThreadPoolConfig {
            worker_threads,
            io_threads,
            cpu_threads,
            network_threads,
        }
    }
    
    /// Calculate optimal buffer sizes
    fn calculate_buffers(resources: &SystemResources) -> BufferConfig {
        let base_buffer_mb = resources.get_recommended_buffer_size_mb();
        
        // Convert to bytes and distribute across different buffer types
        let base_buffer_bytes = base_buffer_mb * 1024 * 1024;
        
        // WebSocket buffer: Small, frequent messages
        let websocket_buffer_size = (base_buffer_bytes / 8).clamp(1024 * 1024, 64 * 1024 * 1024); // 1MB - 64MB
        
        // Database buffer: Larger for batch operations
        let database_buffer_size = (base_buffer_bytes / 4).clamp(4 * 1024 * 1024, 256 * 1024 * 1024); // 4MB - 256MB
        
        // File I/O buffer: Medium size for file operations
        let file_io_buffer_size = (base_buffer_bytes / 6).clamp(2 * 1024 * 1024, 128 * 1024 * 1024); // 2MB - 128MB
        
        // Network buffer: Optimized for network operations
        let network_buffer_size = (base_buffer_bytes / 10).clamp(512 * 1024, 32 * 1024 * 1024); // 512KB - 32MB
        
        debug!("📦 Buffer calculation: websocket={}MB, database={}MB, file_io={}MB, network={}MB",
               websocket_buffer_size / (1024 * 1024),
               database_buffer_size / (1024 * 1024),
               file_io_buffer_size / (1024 * 1024),
               network_buffer_size / (1024 * 1024));
        
        BufferConfig {
            websocket_buffer_size,
            database_buffer_size,
            file_io_buffer_size,
            network_buffer_size,
        }
    }
    
    /// Calculate optimal processing configurations
    fn calculate_processing(resources: &SystemResources) -> ProcessingConfig {
        let memory_gb = resources.available_memory_bytes / (1024 * 1024 * 1024);
        let cpu_cores = resources.cpu_cores;
        
        // Scale batch sizes based on available resources
        let memory_factor = (memory_gb as f64 / 4.0).clamp(0.5, 4.0); // Scale factor based on 4GB baseline
        let cpu_factor = (cpu_cores as f64 / 4.0).clamp(0.5, 4.0); // Scale factor based on 4 core baseline
        
        // Database batch size: Balance memory usage and efficiency
        let database_batch_size = ((1000.0 * memory_factor) as usize).clamp(100, 10000);
        
        // API batch size: Conservative to avoid rate limiting
        let api_batch_size = ((50.0 * cpu_factor) as usize).clamp(10, 200);
        
        // File chunk size: Balance I/O efficiency and memory
        let file_chunk_size = ((8192.0 * memory_factor) as usize).clamp(1024, 65536);
        
        // Memory chunk size: For in-memory processing
        let memory_chunk_size = ((4096.0 * cpu_factor) as usize).clamp(512, 16384);
        
        debug!("⚙️ Processing calculation: db_batch={}, api_batch={}, file_chunk={}, memory_chunk={}",
               database_batch_size, api_batch_size, file_chunk_size, memory_chunk_size);
        
        ProcessingConfig {
            database_batch_size,
            api_batch_size,
            file_chunk_size,
            memory_chunk_size,
        }
    }
    
    /// Calculate optimal memory configurations
    fn calculate_memory(resources: &SystemResources) -> MemoryConfig {
        let memory_utilization = resources.get_memory_utilization();
        let available_gb = resources.available_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        
        // Conservative memory usage limits
        let max_memory_usage_percent = if resources.is_containerized {
            70.0 // More conservative in containers
        } else {
            80.0 // Slightly higher on bare metal
        };
        
        // Cleanup threshold: trigger cleanup before hitting limits
        let cleanup_threshold_percent = max_memory_usage_percent - 10.0;
        
        // Cache size: Use 10% of available memory, with bounds
        let cache_size_mb = ((available_gb * 0.1 * 1024.0) as usize).clamp(64, 2048); // 64MB - 2GB
        
        // Enable memory-intensive features only if sufficient resources
        let enable_memory_intensive_features = resources.has_sufficient_resources() && memory_utilization < 60.0;
        
        debug!("🧠 Memory calculation: max_usage={:.1}%, cleanup_threshold={:.1}%, cache={}MB, intensive={}",
               max_memory_usage_percent, cleanup_threshold_percent, cache_size_mb, enable_memory_intensive_features);
        
        MemoryConfig {
            max_memory_usage_percent,
            cleanup_threshold_percent,
            cache_size_mb,
            enable_memory_intensive_features,
        }
    }
    
    /// Create overrides from TOML configuration
    fn create_overrides(toml_overrides: Option<&ConfigOverridesToml>) -> ConfigOverrides {
        let Some(overrides) = toml_overrides else {
            return ConfigOverrides::default();
        };
        
        let thread_pools = if overrides.worker_threads.is_some() ||
                             overrides.io_threads.is_some() ||
                             overrides.cpu_threads.is_some() ||
                             overrides.network_threads.is_some() {
            Some(ThreadPoolConfig {
                worker_threads: overrides.worker_threads.unwrap_or(4),
                io_threads: overrides.io_threads.unwrap_or(8),
                cpu_threads: overrides.cpu_threads.unwrap_or(2),
                network_threads: overrides.network_threads.unwrap_or(2),
            })
        } else {
            None
        };
        
        let buffers = if overrides.websocket_buffer_mb.is_some() ||
                        overrides.database_buffer_mb.is_some() ||
                        overrides.file_io_buffer_mb.is_some() ||
                        overrides.network_buffer_mb.is_some() {
            Some(BufferConfig {
                websocket_buffer_size: overrides.websocket_buffer_mb.unwrap_or(8) * 1024 * 1024,
                database_buffer_size: overrides.database_buffer_mb.unwrap_or(32) * 1024 * 1024,
                file_io_buffer_size: overrides.file_io_buffer_mb.unwrap_or(16) * 1024 * 1024,
                network_buffer_size: overrides.network_buffer_mb.unwrap_or(4) * 1024 * 1024,
            })
        } else {
            None
        };
        
        let processing = if overrides.database_batch_size.is_some() ||
                           overrides.api_batch_size.is_some() ||
                           overrides.file_chunk_size.is_some() ||
                           overrides.memory_chunk_size.is_some() {
            Some(ProcessingConfig {
                database_batch_size: overrides.database_batch_size.unwrap_or(1000),
                api_batch_size: overrides.api_batch_size.unwrap_or(50),
                file_chunk_size: overrides.file_chunk_size.unwrap_or(8192),
                memory_chunk_size: overrides.memory_chunk_size.unwrap_or(4096),
            })
        } else {
            None
        };
        
        let memory = if overrides.max_memory_usage_percent.is_some() ||
                       overrides.cleanup_threshold_percent.is_some() ||
                       overrides.cache_size_mb.is_some() ||
                       overrides.enable_memory_intensive_features.is_some() {
            Some(MemoryConfig {
                max_memory_usage_percent: overrides.max_memory_usage_percent.unwrap_or(80.0),
                cleanup_threshold_percent: overrides.cleanup_threshold_percent.unwrap_or(70.0),
                cache_size_mb: overrides.cache_size_mb.unwrap_or(256),
                enable_memory_intensive_features: overrides.enable_memory_intensive_features.unwrap_or(true),
            })
        } else {
            None
        };
        
        ConfigOverrides {
            thread_pools,
            buffers,
            processing,
            memory,
            disable_adaptive: overrides.disable_adaptive.unwrap_or(false),
        }
    }
    
    /// Apply manual overrides to adaptive configuration
    fn apply_overrides(mut config: AdaptiveConfig, overrides: &ConfigOverrides) -> AdaptiveConfig {
        if overrides.disable_adaptive {
            info!("⚠️ Adaptive configuration disabled by override, using static defaults");
            return Self::default_static_config();
        }
        
        if let Some(ref thread_override) = overrides.thread_pools {
            info!("🔧 Applying thread pool overrides");
            config.thread_pools = thread_override.clone();
        }
        
        if let Some(ref buffer_override) = overrides.buffers {
            info!("🔧 Applying buffer size overrides");
            config.buffers = buffer_override.clone();
        }
        
        if let Some(ref processing_override) = overrides.processing {
            info!("🔧 Applying processing configuration overrides");
            config.processing = processing_override.clone();
        }
        
        if let Some(ref memory_override) = overrides.memory {
            info!("🔧 Applying memory configuration overrides");
            config.memory = memory_override.clone();
        }
        
        config
    }
    
    /// Default static configuration when adaptive is disabled
    pub fn default_static_config() -> Self {
        AdaptiveConfig {
            thread_pools: ThreadPoolConfig {
                worker_threads: 4,
                io_threads: 8,
                cpu_threads: 2,
                network_threads: 2,
            },
            buffers: BufferConfig {
                websocket_buffer_size: 8 * 1024 * 1024,    // 8MB
                database_buffer_size: 32 * 1024 * 1024,    // 32MB
                file_io_buffer_size: 16 * 1024 * 1024,     // 16MB
                network_buffer_size: 4 * 1024 * 1024,      // 4MB
            },
            processing: ProcessingConfig {
                database_batch_size: 1000,
                api_batch_size: 50,
                file_chunk_size: 8192,
                memory_chunk_size: 4096,
            },
            memory: MemoryConfig {
                max_memory_usage_percent: 80.0,
                cleanup_threshold_percent: 70.0,
                cache_size_mb: 256,
                enable_memory_intensive_features: true,
            },
            overrides: ConfigOverrides::default(),
        }
    }
    
    /// Log the final adaptive configuration
    fn log_configuration(config: &AdaptiveConfig, resources: &SystemResources) {
        info!("🎯 Adaptive Configuration Summary:");
        info!("   🧵 Thread Pools:");
        info!("     - Worker threads: {}", config.thread_pools.worker_threads);
        info!("     - I/O threads: {}", config.thread_pools.io_threads);
        info!("     - CPU threads: {}", config.thread_pools.cpu_threads);
        info!("     - Network threads: {}", config.thread_pools.network_threads);
        
        info!("   📦 Buffer Sizes:");
        info!("     - WebSocket: {} MB", config.buffers.websocket_buffer_size / (1024 * 1024));
        info!("     - Database: {} MB", config.buffers.database_buffer_size / (1024 * 1024));
        info!("     - File I/O: {} MB", config.buffers.file_io_buffer_size / (1024 * 1024));
        info!("     - Network: {} MB", config.buffers.network_buffer_size / (1024 * 1024));
        
        info!("   ⚙️ Processing:");
        info!("     - DB batch size: {}", config.processing.database_batch_size);
        info!("     - API batch size: {}", config.processing.api_batch_size);
        info!("     - File chunk size: {}", config.processing.file_chunk_size);
        info!("     - Memory chunk size: {}", config.processing.memory_chunk_size);
        
        info!("   🧠 Memory Management:");
        info!("     - Max usage: {:.1}%", config.memory.max_memory_usage_percent);
        info!("     - Cleanup threshold: {:.1}%", config.memory.cleanup_threshold_percent);
        info!("     - Cache size: {} MB", config.memory.cache_size_mb);
        info!("     - Memory intensive features: {}", config.memory.enable_memory_intensive_features);
        
        // Performance metrics
        let total_threads = config.thread_pools.worker_threads + 
                          config.thread_pools.io_threads + 
                          config.thread_pools.cpu_threads + 
                          config.thread_pools.network_threads;
        let total_buffer_mb = (config.buffers.websocket_buffer_size + 
                              config.buffers.database_buffer_size + 
                              config.buffers.file_io_buffer_size + 
                              config.buffers.network_buffer_size) / (1024 * 1024);
        
        info!("   📊 Resource Utilization Estimates:");
        info!("     - Total threads: {} ({}% of CPU cores)", total_threads, 
              (total_threads as f64 / resources.cpu_cores as f64 * 100.0) as u32);
        info!("     - Total buffers: {} MB ({:.1}% of available memory)", total_buffer_mb,
              (total_buffer_mb as f64 * 1024.0 * 1024.0 / resources.available_memory_bytes as f64 * 100.0));
        info!("     - Estimated memory footprint: ~{} MB", total_buffer_mb + config.memory.cache_size_mb);
    }
    
    /// Get configuration value with optional manual override
    pub fn get_database_batch_size(&self) -> usize {
        self.processing.database_batch_size
    }
    
    /// Get worker thread pool size
    pub fn get_worker_thread_count(&self) -> usize {
        self.thread_pools.worker_threads
    }
    
    /// Get WebSocket buffer size
    pub fn get_websocket_buffer_size(&self) -> usize {
        self.buffers.websocket_buffer_size
    }
    
    /// Check if memory intensive features should be enabled
    pub fn should_enable_memory_intensive_features(&self) -> bool {
        self.memory.enable_memory_intensive_features
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_config_generation() {
        let resources = SystemResources {
            cpu_cores: 8,
            total_memory_bytes: 16 * 1024 * 1024 * 1024, // 16GB
            available_memory_bytes: 12 * 1024 * 1024 * 1024, // 12GB
            page_size_bytes: 4096,
            is_containerized: false,
            cpu_architecture: "x86_64".to_string(),
            os_info: "linux unix".to_string(),
        };

        let config = AdaptiveConfig::from_system_resources(&resources, None);
        
        // Verify reasonable thread pool sizes
        assert!(config.thread_pools.worker_threads > 0);
        assert!(config.thread_pools.worker_threads <= 16);
        
        // Verify buffer sizes are reasonable
        assert!(config.buffers.websocket_buffer_size >= 1024 * 1024); // At least 1MB
        assert!(config.buffers.database_buffer_size >= 4 * 1024 * 1024); // At least 4MB
        
        // Verify processing configurations
        assert!(config.processing.database_batch_size >= 100);
        assert!(config.processing.api_batch_size >= 10);
    }

    #[test]
    fn test_configuration_overrides() {
        let resources = SystemResources {
            cpu_cores: 4,
            total_memory_bytes: 8 * 1024 * 1024 * 1024, // 8GB
            available_memory_bytes: 6 * 1024 * 1024 * 1024, // 6GB
            page_size_bytes: 4096,
            is_containerized: false,
            cpu_architecture: "x86_64".to_string(),
            os_info: "linux unix".to_string(),
        };

        let toml_overrides = Some(AdaptiveConfigToml {
            enabled: Some(true),
            overrides: Some(ConfigOverridesToml {
                worker_threads: Some(16),
                database_buffer_mb: Some(64),
                database_batch_size: Some(2000),
                ..Default::default()
            }),
        });

        let config = AdaptiveConfig::from_system_resources(&resources, toml_overrides);
        
        // Verify overrides are applied
        assert_eq!(config.thread_pools.worker_threads, 16);
        assert_eq!(config.buffers.database_buffer_size, 64 * 1024 * 1024);
        assert_eq!(config.processing.database_batch_size, 2000);
    }

    #[test]
    fn test_disabled_adaptive_configuration() {
        let resources = SystemResources {
            cpu_cores: 8,
            total_memory_bytes: 16 * 1024 * 1024 * 1024, // 16GB
            available_memory_bytes: 12 * 1024 * 1024 * 1024, // 12GB
            page_size_bytes: 4096,
            is_containerized: false,
            cpu_architecture: "x86_64".to_string(),
            os_info: "linux unix".to_string(),
        };

        let toml_overrides = Some(AdaptiveConfigToml {
            enabled: Some(false),
            overrides: None,
        });

        let config = AdaptiveConfig::from_system_resources(&resources, toml_overrides);
        let default_config = AdaptiveConfig::default_static_config();
        
        // Should match default static configuration
        assert_eq!(config.thread_pools.worker_threads, default_config.thread_pools.worker_threads);
        assert_eq!(config.buffers.websocket_buffer_size, default_config.buffers.websocket_buffer_size);
    }
}