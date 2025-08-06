use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use tracing::{debug, info, warn};

/// System resource information detected at startup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemResources {
    /// Number of logical CPU cores available
    pub cpu_cores: usize,
    /// Total system memory in bytes
    pub total_memory_bytes: u64,
    /// Available system memory in bytes (may be less than total)
    pub available_memory_bytes: u64,
    /// Memory page size in bytes
    pub page_size_bytes: usize,
    /// Whether running in containerized environment
    pub is_containerized: bool,
    /// CPU architecture string
    pub cpu_architecture: String,
    /// Operating system information
    pub os_info: String,
}

/// Global singleton for system resources (detected once at startup)
static SYSTEM_RESOURCES: OnceLock<SystemResources> = OnceLock::new();

impl SystemResources {
    /// Detect system resources at startup and cache globally
    pub fn detect_and_cache() -> Result<&'static SystemResources, Box<dyn std::error::Error + Send + Sync>> {
        // Check if already initialized
        if let Some(resources) = SYSTEM_RESOURCES.get() {
            return Ok(resources);
        }
        
        // Detect resources
        info!("🔍 Detecting system resources...");
        let resources = Self::detect_system_resources()?;
        
        // Try to set in OnceLock, but handle race condition
        match SYSTEM_RESOURCES.set(resources) {
            Ok(()) => SYSTEM_RESOURCES.get().ok_or_else(|| "Failed to retrieve cached resources".into()),
            Err(_value) => {
                // Another thread already set it, return the existing value
                SYSTEM_RESOURCES.get().ok_or_else(|| "Failed to retrieve cached resources".into())
            }
        }
    }

    /// Get cached system resources (must call detect_and_cache first)
    pub fn get_cached() -> Option<&'static SystemResources> {
        SYSTEM_RESOURCES.get()
    }

    /// Internal method to detect all system resources
    fn detect_system_resources() -> Result<SystemResources, Box<dyn std::error::Error + Send + Sync>> {
        let detection_start = std::time::Instant::now();
        
        // CPU core detection
        let cpu_cores = Self::detect_cpu_cores()?;
        debug!("💻 Detected CPU cores: {}", cpu_cores);

        // Memory detection
        let (total_memory_bytes, available_memory_bytes) = Self::detect_memory()?;
        debug!("🧠 Detected memory: {} GB total, {} GB available", 
               total_memory_bytes / (1024 * 1024 * 1024),
               available_memory_bytes / (1024 * 1024 * 1024));

        // System page size
        let page_size_bytes = Self::detect_page_size()?;
        debug!("📄 System page size: {} bytes", page_size_bytes);

        // Container detection
        let is_containerized = Self::detect_containerization();
        debug!("🐳 Containerized environment: {}", is_containerized);

        // CPU architecture
        let cpu_architecture = Self::detect_cpu_architecture();
        debug!("🏗️  CPU architecture: {}", cpu_architecture);

        // OS information  
        let os_info = Self::detect_os_info();
        debug!("🖥️  Operating system: {}", os_info);

        let detection_elapsed = detection_start.elapsed();
        info!("✅ System resource detection completed in {:?}", detection_elapsed);
        
        let resources = SystemResources {
            cpu_cores,
            total_memory_bytes,
            available_memory_bytes,
            page_size_bytes,
            is_containerized,
            cpu_architecture,
            os_info,
        };

        // Log comprehensive system information
        info!("📊 System Resources Summary:");
        info!("   💻 CPU Cores: {}", resources.cpu_cores);
        info!("   🧠 Total Memory: {:.2} GB", resources.total_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
        info!("   💾 Available Memory: {:.2} GB", resources.available_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
        info!("   📄 Page Size: {} KB", resources.page_size_bytes / 1024);
        info!("   🐳 Containerized: {}", resources.is_containerized);
        info!("   🏗️  Architecture: {}", resources.cpu_architecture);
        info!("   🖥️  OS: {}", resources.os_info);

        Ok(resources)
    }

    /// Detect number of CPU cores available to the process
    fn detect_cpu_cores() -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        // Use available parallelism (respects CPU limits in containers)
        match std::thread::available_parallelism() {
            Ok(parallelism) => {
                let cores = parallelism.get();
                debug!("🔍 Available parallelism: {}", cores);
                Ok(cores)
            }
            Err(e) => {
                warn!("⚠️ Failed to detect available parallelism: {}", e);
                // Fallback to number of CPUs
                let cores = num_cpus::get();
                warn!("🔄 Using fallback CPU detection: {} cores", cores);
                Ok(cores)
            }
        }
    }

    /// Detect total and available system memory
    fn detect_memory() -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
        #[cfg(target_os = "linux")]
        {
            Self::detect_memory_linux()
        }
        #[cfg(target_os = "macos")]
        {
            Self::detect_memory_macos()
        }
        #[cfg(target_os = "windows")]
        {
            Self::detect_memory_windows()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            Err("Unsupported operating system for memory detection".into())
        }
    }

    #[cfg(target_os = "linux")]
    fn detect_memory_linux() -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
        let meminfo = std::fs::read_to_string("/proc/meminfo")?;
        let mut total_kb = None;
        let mut available_kb = None;

        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                if let Some(value) = line.split_whitespace().nth(1) {
                    total_kb = value.parse::<u64>().ok();
                }
            } else if line.starts_with("MemAvailable:") {
                if let Some(value) = line.split_whitespace().nth(1) {
                    available_kb = value.parse::<u64>().ok();
                }
            }
        }

        match (total_kb, available_kb) {
            (Some(total), Some(available)) => {
                Ok((total * 1024, available * 1024)) // Convert KB to bytes
            }
            _ => Err("Failed to parse memory information from /proc/meminfo".into())
        }
    }

    #[cfg(target_os = "macos")]
    fn detect_memory_macos() -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
        use std::process::Command;

        // Get total memory using sysctl
        let total_output = Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()?;
        
        let total_bytes = String::from_utf8(total_output.stdout)?
            .trim()
            .parse::<u64>()?;

        // Get available memory using vm_stat
        let vm_output = Command::new("vm_stat")
            .output()?;
        
        let vm_output_str = String::from_utf8(vm_output.stdout)?;
        let mut free_pages = 0u64;
        let mut inactive_pages = 0u64;
        
        for line in vm_output_str.lines() {
            if line.starts_with("Pages free:") {
                if let Some(value) = line.split_whitespace().nth(2) {
                    free_pages = value.trim_end_matches('.').parse().unwrap_or(0);
                }
            } else if line.starts_with("Pages inactive:") {
                if let Some(value) = line.split_whitespace().nth(2) {
                    inactive_pages = value.trim_end_matches('.').parse().unwrap_or(0);
                }
            }
        }

        // macOS page size is typically 4096 bytes
        let page_size = 4096u64;
        let available_bytes = (free_pages + inactive_pages) * page_size;

        Ok((total_bytes, available_bytes))
    }

    #[cfg(target_os = "windows")]
    fn detect_memory_windows() -> Result<(u64, u64), Box<dyn std::error::Error + Send + Sync>> {
        // Windows implementation would use GetPhysicallyInstalledSystemMemory and GlobalMemoryStatusEx
        // For now, return a reasonable default
        warn!("Windows memory detection not fully implemented, using defaults");
        Ok((8 * 1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024)) // 8GB total, 4GB available
    }

    /// Detect system page size
    fn detect_page_size() -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        #[cfg(unix)]
        {
            unsafe {
                let page_size = libc::sysconf(libc::_SC_PAGESIZE);
                if page_size > 0 {
                    Ok(page_size as usize)
                } else {
                    Err("Failed to get system page size".into())
                }
            }
        }
        #[cfg(not(unix))]
        {
            // Default page size for non-Unix systems
            Ok(4096)
        }
    }

    /// Detect if running in a containerized environment
    fn detect_containerization() -> bool {
        // Check for common container indicators
        std::path::Path::exists(std::path::Path::new("/.dockerenv")) ||
        std::env::var("container").is_ok() ||
        std::fs::read_to_string("/proc/1/cgroup")
            .map(|cgroup| cgroup.contains("docker") || cgroup.contains("containerd") || cgroup.contains("kubepods"))
            .unwrap_or(false)
    }

    /// Detect CPU architecture
    fn detect_cpu_architecture() -> String {
        std::env::consts::ARCH.to_string()
    }

    /// Detect operating system information
    fn detect_os_info() -> String {
        format!("{} {}", std::env::consts::OS, std::env::consts::FAMILY)
    }

    /// Get memory utilization percentage
    pub fn get_memory_utilization(&self) -> f64 {
        let used_memory = self.total_memory_bytes - self.available_memory_bytes;
        (used_memory as f64 / self.total_memory_bytes as f64) * 100.0
    }

    /// Check if system has sufficient resources for high-performance operation
    pub fn has_sufficient_resources(&self) -> bool {
        // Heuristics for sufficient resources:
        // - At least 2 CPU cores
        // - At least 2GB available memory
        // - Memory utilization less than 80%
        self.cpu_cores >= 2 && 
        self.available_memory_bytes >= 2 * 1024 * 1024 * 1024 && // 2GB
        self.get_memory_utilization() < 80.0
    }

    /// Get recommended thread pool size based on CPU cores
    pub fn get_recommended_thread_pool_size(&self) -> usize {
        // Conservative approach: use CPU cores - 1 to leave one core for system
        // Minimum of 1, maximum of 16 to prevent resource exhaustion
        (self.cpu_cores.saturating_sub(1)).clamp(1, 16)
    }

    /// Get recommended buffer size based on available memory
    pub fn get_recommended_buffer_size_mb(&self) -> usize {
        // Use 1% of available memory, with reasonable bounds
        let recommended = (self.available_memory_bytes / 100) / (1024 * 1024); // 1% in MB
        recommended.clamp(10, 512) as usize // Minimum 10MB, maximum 512MB
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_resource_detection() {
        let resources = SystemResources::detect_system_resources().expect("Failed to detect system resources");
        
        // Basic sanity checks
        assert!(resources.cpu_cores > 0, "Should detect at least 1 CPU core");
        assert!(resources.total_memory_bytes > 0, "Should detect some memory");
        assert!(resources.available_memory_bytes <= resources.total_memory_bytes, "Available memory should not exceed total");
        assert!(resources.page_size_bytes > 0, "Page size should be positive");
        
        // Check that string fields are not empty
        assert!(!resources.cpu_architecture.is_empty(), "CPU architecture should not be empty");
        assert!(!resources.os_info.is_empty(), "OS info should not be empty");
    }

    #[test]
    fn test_memory_utilization_calculation() {
        let resources = SystemResources {
            cpu_cores: 4,
            total_memory_bytes: 8 * 1024 * 1024 * 1024, // 8GB
            available_memory_bytes: 4 * 1024 * 1024 * 1024, // 4GB
            page_size_bytes: 4096,
            is_containerized: false,
            cpu_architecture: "x86_64".to_string(),
            os_info: "linux unix".to_string(),
        };

        let utilization = resources.get_memory_utilization();
        assert!((utilization - 50.0).abs() < 0.1, "Expected ~50% memory utilization, got {}", utilization);
    }

    #[test]
    fn test_thread_pool_recommendations() {
        let resources = SystemResources {
            cpu_cores: 8,
            total_memory_bytes: 16 * 1024 * 1024 * 1024, // 16GB
            available_memory_bytes: 12 * 1024 * 1024 * 1024, // 12GB
            page_size_bytes: 4096,
            is_containerized: false,
            cpu_architecture: "x86_64".to_string(),
            os_info: "linux unix".to_string(),
        };

        let thread_pool_size = resources.get_recommended_thread_pool_size();
        assert_eq!(thread_pool_size, 7, "Expected 7 threads for 8 cores (cores - 1)");

        let buffer_size = resources.get_recommended_buffer_size_mb();
        assert!((10..=512).contains(&buffer_size), "Buffer size should be within bounds, got {}", buffer_size);
    }

    #[test]
    fn test_sufficient_resources_check() {
        // Sufficient resources
        let good_resources = SystemResources {
            cpu_cores: 4,
            total_memory_bytes: 8 * 1024 * 1024 * 1024, // 8GB
            available_memory_bytes: 6 * 1024 * 1024 * 1024, // 6GB (75% utilization)
            page_size_bytes: 4096,
            is_containerized: false,
            cpu_architecture: "x86_64".to_string(),
            os_info: "linux unix".to_string(),
        };
        assert!(good_resources.has_sufficient_resources());

        // Insufficient resources (low memory)
        let low_memory_resources = SystemResources {
            cpu_cores: 4,
            total_memory_bytes: 2 * 1024 * 1024 * 1024, // 2GB
            available_memory_bytes: 1024 * 1024 * 1024, // 1GB
            page_size_bytes: 4096,
            is_containerized: false,
            cpu_architecture: "x86_64".to_string(),
            os_info: "linux unix".to_string(),
        };
        assert!(!low_memory_resources.has_sufficient_resources());
    }
}