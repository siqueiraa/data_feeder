/// Shared LMDB configuration to ensure consistent environment options across all actors
use heed::{Env, EnvOpenOptions};
use std::path::Path;
use tracing::{info, debug};
use crate::common::constants::{LMDB_MAP_SIZE, LMDB_MAX_DBS, LMDB_MAX_READERS};
use crate::historical::errors::HistoricalDataError;
use crate::common::error_utils::ErrorContext;
use crate::storage_optimization::{LmdbOptimizer, WorkloadPattern};

/// Open an LMDB environment with consistent configuration across all actors
/// This ensures that all parts of the application use identical environment options
/// to prevent "environment already opened with different options" errors
pub fn open_lmdb_environment(path: &Path) -> Result<Env, HistoricalDataError> {
    unsafe {
        EnvOpenOptions::new()
            .map_size(LMDB_MAP_SIZE)
            .max_dbs(LMDB_MAX_DBS)
            .max_readers(LMDB_MAX_READERS)
            .open(path)
            .with_db_context(&format!("Failed to open LMDB environment at: {}", path.display()))
    }
}

/// Open an optimized LMDB environment based on workload pattern
pub fn open_optimized_lmdb_environment(path: &Path, workload: WorkloadPattern) -> Result<Env, HistoricalDataError> {
    let mut optimizer = LmdbOptimizer::new(true); // Enable auto-tuning
    let optimized_config = optimizer.optimize_config(&workload);
    
    // Apply optimization based on workload pattern with enhanced tuning
    let map_size = optimized_config.map_size.max(LMDB_MAP_SIZE); // Use at least default map size
    let max_readers = optimized_config.max_readers.min(LMDB_MAX_READERS); // Cap at default max
    let max_dbs = optimized_config.max_dbs.min(LMDB_MAX_DBS); // Cap at default max
    
    info!("🚀 Opening optimized LMDB environment: workload={:?}, map_size={}MB, max_readers={}, max_dbs={}", 
          workload, map_size / (1024*1024), max_readers, max_dbs);
    
    // Record optimization configuration metrics
    if let Some(metrics) = crate::metrics::get_metrics() {
        metrics.record_lmdb_config_optimization(
            &format!("{:?}", workload),
            map_size,
            max_readers as u64,
            max_dbs as u64,
        );
    }
    
    unsafe {
        // For now, we use the same options for all workload patterns
        // Future enhancement: Add workload-specific LMDB flags when they become available in heed
        debug!("🔧 LMDB workload optimization: {:?} (advanced flags not yet implemented)", workload);
        
        EnvOpenOptions::new()
            .map_size(map_size)
            .max_dbs(max_dbs)
            .max_readers(max_readers)
            .open(path)
            .with_db_context(&format!("Failed to open optimized LMDB environment at: {} (workload: {:?})", path.display(), workload))
    }
}

/// Get the shared LMDB configuration values for validation or logging
pub fn get_lmdb_config() -> (usize, u32, u32) {
    (LMDB_MAP_SIZE, LMDB_MAX_DBS, LMDB_MAX_READERS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_consistent_lmdb_config() {
        let (map_size, max_dbs, max_readers) = get_lmdb_config();
        assert_eq!(map_size, 1024 * 1024 * 1024); // 1GB
        assert_eq!(max_dbs, 15);
        assert_eq!(max_readers, 256);
    }

    #[test]
    fn test_open_lmdb_environment() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");
        std::fs::create_dir_all(&db_path).expect("Failed to create db dir");

        let result = open_lmdb_environment(&db_path);
        assert!(result.is_ok(), "Failed to open LMDB environment: {:?}", result.err());
        
        // Test opening the same environment again should work with identical config
        let result2 = open_lmdb_environment(&db_path);
        assert!(result2.is_ok(), "Failed to open LMDB environment second time: {:?}", result2.err());
    }
}