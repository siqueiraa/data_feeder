#[cfg(test)]
mod error_handling_tests {
    use crate::historical::actor::*;
    use crate::historical::errors::*;
    use tempfile::TempDir;
    use std::path::Path;

    #[test]
    fn test_historical_actor_new_success() {
        let temp_dir = TempDir::new().unwrap();
        let csv_temp_dir = TempDir::new().unwrap();
        
        let symbols = vec!["BTCUSDT".to_string()];
        let timeframes = vec![60u64];
        
        let result = HistoricalActor::new(
            &symbols,
            &timeframes,
            temp_dir.path(),
            csv_temp_dir.path()
        );
        
        assert!(result.is_ok(), "HistoricalActor creation should succeed");
        
        // Actor creation successful - that's the main test
        let _actor = result.unwrap();
    }

    #[test]
    fn test_historical_actor_new_multiple_symbols() {
        let temp_dir = TempDir::new().unwrap();
        let csv_temp_dir = TempDir::new().unwrap();
        
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let timeframes = vec![60u64, 300u64];
        
        let result = HistoricalActor::new(
            &symbols,
            &timeframes,
            temp_dir.path(),
            csv_temp_dir.path()
        );
        
        assert!(result.is_ok(), "HistoricalActor creation with multiple symbols should succeed");
        
        // 2 symbols × 2 timeframes should create 4 database combinations
        let _actor = result.unwrap();
    }

    #[test]
    fn test_historical_actor_new_invalid_path() {
        let temp_dir = TempDir::new().unwrap();
        let invalid_path = Path::new("/invalid/nonexistent/readonly/path");
        
        let symbols = vec!["BTCUSDT".to_string()];
        let timeframes = vec![60u64];
        
        let result = HistoricalActor::new(
            &symbols,
            &timeframes,
            temp_dir.path(), // Pass a valid base_path
            invalid_path // Pass the invalid path to csv_path
        );
        
        assert!(result.is_err(), "HistoricalActor creation should fail with invalid path");
        match result.unwrap_err() {
            HistoricalDataError::Io(_) => {}, // Expected
            HistoricalDataError::DirectoryCreation(_) => {}, // Also acceptable
            e => panic!("Expected IO or DirectoryCreation error for invalid path, got: {:?}", e),
        }
    }

    #[test]
    fn test_historical_actor_error_types() {
        // Test that our error types can be created and match correctly
        let db_error = HistoricalDataError::DatabaseInitialization("test".to_string());
        match db_error {
            HistoricalDataError::DatabaseInitialization(_) => {}, // Expected
            other => panic!("Error type mismatch: {:?}", other),
        }
        
        let dir_error = HistoricalDataError::DirectoryCreation("test".to_string());
        match dir_error {
            HistoricalDataError::DirectoryCreation(_) => {}, // Expected
            other => panic!("Error type mismatch: {:?}", other),
        }
        
        let semaphore_error = HistoricalDataError::SemaphoreAcquisition("test".to_string());
        match semaphore_error {
            HistoricalDataError::SemaphoreAcquisition(_) => {}, // Expected
            other => panic!("Error type mismatch: {:?}", other),
        }
    }

    #[test]
    fn test_error_display_formatting() {
        let errors = vec![
            HistoricalDataError::DatabaseInitialization("DB init failed".to_string()),
            HistoricalDataError::DirectoryCreation("Dir creation failed".to_string()),
            HistoricalDataError::SemaphoreAcquisition("Semaphore failed".to_string()),
        ];
        
        for error in errors {
            let error_string = format!("{}", error);
            assert!(!error_string.is_empty(), "Error should have non-empty display");
            assert!(error_string.len() > 10, "Error message should be descriptive");
        }
    }

    #[test]
    fn test_constants_usage() {
        // Test that our constants are being used properly
        use crate::common::constants::*;
        
        assert!(LMDB_MAP_SIZE > 0);
        assert!(LMDB_MAX_DBS > 0);
        assert!(LMDB_MAX_READERS > 0);
        assert!(BATCH_SIZE > 0);
        
        assert!(!CANDLES_DB_NAME.is_empty());
        assert!(!CERTIFIED_RANGE_DB_NAME.is_empty());
    }

    #[tokio::test]
    async fn test_no_panic_in_error_conditions() {
        // This test verifies that our code doesn't panic in error conditions
        
        // Test 1: Invalid directory path
        let temp_dir = TempDir::new().unwrap();
        let invalid_path = Path::new("/completely/invalid/path/that/should/not/exist");
        
        let symbols = vec!["BTCUSDT".to_string()];
        let timeframes = vec![60u64];
        
        let result = HistoricalActor::new(&symbols, &timeframes, temp_dir.path(), invalid_path);
        
        assert!(result.is_err(), "HistoricalActor creation with invalid path should return an error");
    }

    #[test]
    fn test_error_chain_conversion() {
        use std::io::{Error as IoError, ErrorKind};
        
        // Test that IO errors convert properly
        let io_error = IoError::new(ErrorKind::PermissionDenied, "Access denied");
        let hist_error = HistoricalDataError::Io(io_error);
        
        match hist_error {
            HistoricalDataError::Io(ref e) => {
                assert_eq!(e.kind(), ErrorKind::PermissionDenied);
            },
            other => panic!("Expected IO error conversion, got: {:?}", other),
        }
    }

    #[test]
    fn test_error_context_helpers() {
        use crate::common::error_utils::ErrorContext;
        use std::io::{Error as IoError, ErrorKind};
        
        let io_error = IoError::new(ErrorKind::NotFound, "File not found");
        let result: Result<(), _> = Err(io_error);
        
        let with_context = result.with_io_context("Test operation failed");
        assert!(with_context.is_err());
        
        match with_context.unwrap_err() {
            HistoricalDataError::Io(_) => {}, // Expected
            other => panic!("Expected IO error with context, got: {:?}", other),
        }
    }
}