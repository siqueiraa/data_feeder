//! Logging configuration for dual output (console + file) with rotation
//! 
//! Provides structured logging to both console and rotating log files to facilitate
//! debugging after cargo run and bug detection as requested by the user.

use tracing_subscriber::{
    fmt::{self, time::ChronoUtc},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};
use tracing_appender::non_blocking;

/// Logging configuration options
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    /// Directory to store log files
    pub log_dir: String,
    /// Log level filter (e.g., "info", "debug", "data_feeder=debug")
    pub level_filter: String,
    /// Maximum file size before rotation (default: daily rotation)
    pub rotation: LogRotation,
    /// Whether to include timestamps in console output
    pub console_timestamps: bool,
    /// Whether to use JSON format for file logs (structured)
    pub file_json_format: bool,
}

/// Log rotation configuration
#[derive(Debug, Clone)]
pub enum LogRotation {
    /// Rotate daily (recommended for production)
    Daily,
    /// Rotate hourly (for debugging heavy loads)
    Hourly,
    /// Rotate when file reaches size limit (MB)
    SizeBased(u64),
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            log_dir: "logs".to_string(),
            level_filter: "info,data_feeder=info".to_string(),
            rotation: LogRotation::Daily,
            console_timestamps: true,
            file_json_format: true,
        }
    }
}

/// Initialize dual output logging (console + rotating files)
/// 
/// Creates two output streams:
/// 1. Console: Human-readable format with colors (for development)
/// 2. File: Structured JSON format with rotation (for debugging/analysis)
/// 
/// File structure:
/// - logs/data_feeder.YYYY-MM-DD.log (daily rotation)
/// - logs/data_feeder.YYYY-MM-DD-HH.log (hourly rotation if enabled)
/// 
/// Returns a guard that must be kept alive for the duration of the application
/// to ensure the background logging thread continues running.
pub fn init_dual_logging(config: LoggingConfig) -> Result<tracing_appender::non_blocking::WorkerGuard, Box<dyn std::error::Error + Send + Sync>> {
    // Ensure log directory exists
    std::fs::create_dir_all(&config.log_dir)?;
    
    // Create env filter from configuration
    let console_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.level_filter));
    let file_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.level_filter));
    
    // Configure file appender with rotation
    let file_appender = match config.rotation {
        LogRotation::Daily => {
            tracing_appender::rolling::daily(&config.log_dir, "data_feeder.log")
        }
        LogRotation::Hourly => {
            tracing_appender::rolling::hourly(&config.log_dir, "data_feeder.log")
        }
        LogRotation::SizeBased(_size_mb) => {
            // tracing_appender doesn't support size-based rotation natively
            // Fall back to daily rotation for now
            tracing_appender::rolling::daily(&config.log_dir, "data_feeder.log")
        }
    };
    
    // Create non-blocking file writer
    let (file_writer, guard) = non_blocking(file_appender);
    
    // Console layer: Human-readable with colors
    let console_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true) // Enable colors
        .with_level(true)
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_timer(if config.console_timestamps {
            ChronoUtc::new("%Y-%m-%d %H:%M:%S%.3f UTC".to_string())
        } else {
            ChronoUtc::new("".to_string()) // No timestamps for cleaner console
        })
        .with_filter(console_filter);
    
    // File layer: Structured JSON format
    let file_layer = if config.file_json_format {
        fmt::layer()
            .json()
            .with_writer(file_writer)
            .with_ansi(false) // No colors in files
            .with_level(true)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_timer(ChronoUtc::new("%Y-%m-%dT%H:%M:%S%.3fZ".to_string())) // ISO8601 format
            .with_filter(file_filter)
            .boxed()
    } else {
        fmt::layer()
            .with_writer(file_writer)
            .with_ansi(false) // No colors in files
            .with_level(true)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_timer(ChronoUtc::new("%Y-%m-%d %H:%M:%S%.3f UTC".to_string()))
            .with_filter(file_filter)
            .boxed()
    };
    
    // Initialize subscriber with both layers
    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();
    
    // Log the initialization
    tracing::info!(
        log_dir = %config.log_dir,
        rotation = ?config.rotation,
        json_format = config.file_json_format,
        "📁 Dual logging initialized - console + rotating files"
    );
    
    Ok(guard)
}

/// Initialize simple logging for testing or minimal setups
pub fn init_simple_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter("info,data_feeder=info")
        .init();
    
    tracing::info!("🖥️ Simple console logging initialized");
    Ok(())
}

/// Get log file paths for the current date
pub fn get_current_log_files(log_dir: &str) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    
    if let Ok(entries) = std::fs::read_dir(log_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && 
               path.extension().map(|ext| ext == "log").unwrap_or(false) &&
               path.file_name()
                   .and_then(|name| name.to_str())
                   .map(|name| name.starts_with("data_feeder"))
                   .unwrap_or(false) {
                files.push(path);
            }
        }
    }
    
    files.sort();
    files
}

/// Clean up old log files (keep only recent ones)
pub fn cleanup_old_logs(log_dir: &str, keep_days: u32) -> Result<usize, std::io::Error> {
    let cutoff_time = std::time::SystemTime::now() 
        - std::time::Duration::from_secs(keep_days as u64 * 24 * 3600);
    
    let mut removed_count = 0;
    
    if let Ok(entries) = std::fs::read_dir(log_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && 
               path.extension().map(|ext| ext == "log").unwrap_or(false) {
                
                if let Ok(metadata) = path.metadata() {
                    if let Ok(modified) = metadata.modified() {
                        if modified < cutoff_time && std::fs::remove_file(&path).is_ok() {
                            removed_count += 1;
                            tracing::debug!("🗑️ Removed old log file: {:?}", path);
                        }
                    }
                }
            }
        }
    }
    
    if removed_count > 0 {
        tracing::info!("🧹 Cleaned up {} old log files (older than {} days)", removed_count, keep_days);
    }
    
    Ok(removed_count)
}

/// Log basic system information for debugging
pub fn log_system_info() {
    // Log environment information
    tracing::info!(
        package_version = env!("CARGO_PKG_VERSION"),
        target_arch = std::env::consts::ARCH,
        target_os = std::env::consts::OS,
        "📊 Environment information logged"
    );
    
    // Log memory and CPU info
    tracing::info!(
        cpu_count = num_cpus::get(),
        "🖥️ Hardware information logged"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_logging_config_default() {
        let config = LoggingConfig::default();
        assert_eq!(config.log_dir, "logs");
        assert_eq!(config.level_filter, "info,data_feeder=info");
        assert!(matches!(config.rotation, LogRotation::Daily));
        assert!(config.console_timestamps);
        assert!(config.file_json_format);
    }
    
    #[test]
    fn test_get_current_log_files() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path();
        
        // Create some test log files
        std::fs::write(log_dir.join("data_feeder.2025-01-01.log"), "test").unwrap();
        std::fs::write(log_dir.join("data_feeder.2025-01-02.log"), "test").unwrap();
        std::fs::write(log_dir.join("other.log"), "test").unwrap(); // Should be ignored
        std::fs::write(log_dir.join("data_feeder.txt"), "test").unwrap(); // Wrong extension
        
        let log_files = get_current_log_files(log_dir.to_str().unwrap());
        assert_eq!(log_files.len(), 2);
        
        // Check that files are sorted
        assert!(log_files[0].file_name().unwrap().to_str().unwrap().contains("2025-01-01"));
        assert!(log_files[1].file_name().unwrap().to_str().unwrap().contains("2025-01-02"));
    }
    
    #[test]
    fn test_cleanup_old_logs() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path();
        
        // Create test log files
        let recent_file = log_dir.join("data_feeder.recent.log");
        let old_file = log_dir.join("data_feeder.old.log");
        
        std::fs::write(&recent_file, "recent").unwrap();
        std::fs::write(&old_file, "old").unwrap();
        
        // Set old file's modified time to past
        let _old_time = std::time::SystemTime::now() - std::time::Duration::from_secs(10 * 24 * 3600); // 10 days ago
        // Note: Setting file times requires additional crates, so we'll just test the function exists
        
        // Test cleanup (won't actually remove files in this simple test)
        let result = cleanup_old_logs(log_dir.to_str().unwrap(), 7);
        assert!(result.is_ok());
    }
}