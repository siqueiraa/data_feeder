use tempfile::TempDir;
use kameo;

use crate::historical::structs::FuturesOHLCVCandle;
use super::actor::LmdbActor;
use super::messages::{LmdbActorMessage, LmdbActorResponse};
use super::gap_detector::GapDetector;

    fn create_test_candle(close_time: i64, open: f64, high: f64, low: f64, close: f64, volume: f64) -> FuturesOHLCVCandle {
        FuturesOHLCVCandle {
            open_time: close_time - 60000,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            number_of_trades: 100,
            taker_buy_base_asset_volume: volume * 0.6, // Example value
            closed: true,
        }
    }

#[tokio::test]
async fn test_lmdb_actor_basic_operations() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let actor = LmdbActor::new(temp_dir.path(), 75)?;
    let actor_ref = kameo::spawn(actor);

    // Initialize database
    let init_msg = LmdbActorMessage::InitializeDatabase {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
    };
    
    match actor_ref.ask(init_msg).await? {
        LmdbActorResponse::Success => {},
        response => panic!("Expected Success, got {:?}", response),
    }

    // Store test candles
    let candles = vec![
        create_test_candle(1000, 50000.0, 50001.0, 49999.0, 50000.0, 100.0),
        create_test_candle(2000, 50100.0, 50101.0, 50099.0, 50100.0, 100.0),
        create_test_candle(3000, 50200.0, 50201.0, 50199.0, 50200.0, 100.0),
    ];

    let store_msg = LmdbActorMessage::StoreCandles {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
        candles: candles.clone(),
    };

    match actor_ref.ask(store_msg).await? {
        LmdbActorResponse::Success => {},
        response => panic!("Expected Success, got {:?}", response),
    }

    // Get data range
    let range_msg = LmdbActorMessage::GetDataRange {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
    };

    match actor_ref.ask(range_msg).await? {
        LmdbActorResponse::DataRange { earliest, latest, count } => {
            assert_eq!(earliest, Some(1000));
            assert_eq!(latest, Some(3000));
            assert_eq!(count, 3);
        },
        response => panic!("Expected DataRange, got {:?}", response),
    }

    Ok(())
}

#[tokio::test]
async fn test_gap_detection_comprehensive() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let actor = LmdbActor::new(temp_dir.path(), 30)?; // 30 seconds threshold to detect minute-level gaps
    let actor_ref = kameo::spawn(actor);

    // Initialize database
    let init_msg = LmdbActorMessage::InitializeDatabase {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
    };
    actor_ref.ask(init_msg).await?;

    // Create candles with gaps
    let candles = vec![
        create_test_candle(120000, 50000.0, 50001.0, 49999.0, 50000.0, 100.0),  // 2 minutes
        create_test_candle(180000, 50100.0, 50101.0, 50099.0, 50100.0, 100.0),  // 3 minutes (gap from 1-2 minutes)
        create_test_candle(240000, 50200.0, 50201.0, 50199.0, 50200.0, 100.0),  // 4 minutes (no gap)
        // Gap from 5-7 minutes (internal gap)
        create_test_candle(480000, 50300.0, 50301.0, 50299.0, 50300.0, 100.0),  // 8 minutes
        create_test_candle(540000, 50400.0, 50401.0, 50399.0, 50400.0, 100.0),  // 9 minutes
    ];

    let store_msg = LmdbActorMessage::StoreCandles {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
        candles,
    };
    actor_ref.ask(store_msg).await?;

    // Validate data range that should show gaps
    let validate_msg = LmdbActorMessage::ValidateDataRange {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
        start: 60000,   // 1 minute
        end: 600000,    // 10 minutes
    };

    match actor_ref.ask(validate_msg).await? {
        LmdbActorResponse::ValidationResult { gaps } => {
            // With open_time based detection:
            // 1. Internal gap: 240000 to 420000 (4-7 minutes missing periods)
            // 2. Suffix gap: 540000 to 600000 (9-10 minutes missing period)
            // Note: No prefix gap since first candle starts at requested start time (60000)
            
            assert!(!gaps.is_empty(), "Should detect gaps");
            
            println!("Detected gaps: {:?}", gaps);
            
            // Check for internal gap (periods 4-7 minutes missing: 240000-420000)
            let has_internal_gap = gaps.iter().any(|(start, end)| *start == 240000 && *end == 420000);
            assert!(has_internal_gap, "Should detect internal gap from 4-7 minutes");
            
            // Check for suffix gap (period 9-10 minutes missing: 540000-600000)
            let has_suffix_gap = gaps.iter().any(|(start, end)| *start == 540000 && *end == 600000);
            assert!(has_suffix_gap, "Should detect suffix gap from 9-10 minutes");
        },
        response => panic!("Expected ValidationResult, got {:?}", response),
    }

    Ok(())
}

#[test]
fn test_gap_detector_internal_gaps() {
    let gap_detector = GapDetector::new(1); // 1 second threshold to detect all gaps
    
    // Create candles with internal gaps
    let candles = vec![
        create_test_candle(60000, 50000.0, 50001.0, 49999.0, 50000.0, 100.0),   // 1 minute
        create_test_candle(120000, 50100.0, 50101.0, 50099.0, 50100.0, 100.0),  // 2 minutes (no gap)
        create_test_candle(180000, 50200.0, 50201.0, 50199.0, 50200.0, 100.0),  // 3 minutes (no gap)
        // Gap here - missing 4 and 5 minutes
        create_test_candle(360000, 50300.0, 50301.0, 50299.0, 50300.0, 100.0),  // 6 minutes
        create_test_candle(420000, 50400.0, 50401.0, 50399.0, 50400.0, 100.0),  // 7 minutes (no gap)
    ];

    let gaps = gap_detector.detect_gaps(&candles, 60, 60000, 420000);
    
    // Should detect internal gap between 3 and 6 minutes (periods 4-5 missing: 180000-300000)
    let internal_gap = gaps.iter().find(|(start, end)| *start == 180000 && *end == 300000);
    assert!(internal_gap.is_some(), "Should detect internal gap from 3-5 minutes");
}

#[test]
fn test_gap_detector_prefix_suffix() {
    let gap_detector = GapDetector::new(30); // 30 seconds threshold to detect 1-minute gaps
    
    let candles = vec![
        create_test_candle(180000, 50000.0, 50001.0, 49999.0, 50000.0, 100.0),  // 3 minutes
        create_test_candle(240000, 50100.0, 50101.0, 50099.0, 50100.0, 100.0),  // 4 minutes
        create_test_candle(300000, 50200.0, 50201.0, 50199.0, 50200.0, 100.0),  // 5 minutes
    ];

    // Request range from 1-7 minutes
    let gaps = gap_detector.detect_gaps(&candles, 60, 60000, 420000);
    
    
    // With open_time logic:
    // - First candle open_time: 180000 - 60000 = 120000 (2 minutes)
    // - Second candle open_time: 240000 - 60000 = 180000 (3 minutes)  
    // - Third candle open_time: 300000 - 60000 = 240000 (4 minutes)
    // So we have candles at open_times: 120000, 180000, 240000
    // Prefix gap: 60000 to 120000 (1-2 minutes missing)
    // Suffix gap: 240000 to 420000 (4-7 minutes missing) - but since last candle is at 240000, gap starts at 300000
    let has_prefix = gaps.iter().any(|(start, end)| *start == 60000 && *end == 120000);
    let has_suffix = gaps.iter().any(|(start, end)| *start == 300000 && *end == 420000);
    
    assert!(has_prefix, "Should detect prefix gap from 1-2 minutes");
    assert!(has_suffix, "Should detect suffix gap from 5-7 minutes");
}

#[test]
fn test_gap_detector_no_gaps() {
    let gap_detector = GapDetector::new(75);
    
    // Perfect sequence with no gaps
    let candles = vec![
        create_test_candle(60000, 50000.0, 50001.0, 49999.0, 50000.0, 100.0),   // 1 minute
        create_test_candle(120000, 50100.0, 50101.0, 50099.0, 50100.0, 100.0),  // 2 minutes
        create_test_candle(180000, 50200.0, 50201.0, 50199.0, 50200.0, 100.0),  // 3 minutes
        create_test_candle(240000, 50300.0, 50301.0, 50299.0, 50300.0, 100.0),  // 4 minutes
    ];

    let gaps = gap_detector.detect_gaps(&candles, 60, 60000, 240000);
    
    assert!(gaps.is_empty(), "Should detect no gaps in perfect sequence");
}

#[test]
fn test_gap_detector_micro_gap_filtering() {
    let gap_detector = GapDetector::new(120); // 2-minute threshold
    
    let candles = vec![
        create_test_candle(60000, 50000.0, 50001.0, 49999.0, 50000.0, 100.0),   // 1 minute
        create_test_candle(180000, 50100.0, 50101.0, 50099.0, 50100.0, 100.0),  // 3 minutes (1-minute gap)
        create_test_candle(420000, 50200.0, 50201.0, 50199.0, 50200.0, 100.0),  // 7 minutes (4-minute gap)
    ];

    let gaps = gap_detector.detect_gaps(&candles, 60, 60000, 420000);
    
    // Should filter out the 1-minute gap (60s < 120s threshold)
    // Should keep the 4-minute gap (240s > 120s threshold)
    assert_eq!(gaps.len(), 1, "Should filter micro-gaps below threshold");
    
    let large_gap = &gaps[0];
    assert_eq!(large_gap.0, 180000); // After 2-minute candle period ends
    assert_eq!(large_gap.1, 360000); // Before 6-minute candle period starts
}

#[tokio::test]
async fn test_storage_stats() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let actor = LmdbActor::new(temp_dir.path(), 75)?;
    let actor_ref = kameo::spawn(actor);

    // Initialize database for multiple symbols/timeframes
    for symbol in &["BTCUSDT", "ETHUSDT"] {
        for timeframe in &[60, 300] {
            let init_msg = LmdbActorMessage::InitializeDatabase {
                symbol: symbol.to_string(),
                timeframe: *timeframe,
            };
            actor_ref.ask(init_msg).await?;
        }
    }

    // Store some test data
    let candles = vec![create_test_candle(60000, 50000.0, 50001.0, 49999.0, 50000.0, 100.0)];
    let store_msg = LmdbActorMessage::StoreCandles {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
        candles,
    };
    actor_ref.ask(store_msg).await?;

    // Get storage stats
    let stats_msg = LmdbActorMessage::GetStorageStats;
    match actor_ref.ask(stats_msg).await? {
        LmdbActorResponse::StorageStats { symbols, total_candles, symbol_stats, .. } => {
            assert_eq!(symbols.len(), 2, "Should track 2 symbols");
            assert_eq!(total_candles, 1, "Should count 1 stored candle");
            assert_eq!(symbol_stats.len(), 4, "Should have 4 symbol-timeframe combinations");
        },
        response => panic!("Expected StorageStats, got {:?}", response),
    }

    Ok(())
}

#[tokio::test]
async fn test_get_candles_with_limit() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let actor = LmdbActor::new(temp_dir.path(), 75)?;
    let actor_ref = kameo::spawn(actor);

    // Initialize database
    let init_msg = LmdbActorMessage::InitializeDatabase {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
    };
    actor_ref.ask(init_msg).await?;

    // Store multiple candles
    let candles: Vec<_> = (1..=10)
        .map(|i| create_test_candle(i * 60000, 50000.0 + i as f64 * 100.0, 50001.0 + i as f64 * 100.0, 49999.0 + i as f64 * 100.0, 50000.0 + i as f64 * 100.0, 100.0))
        .collect();

    let store_msg = LmdbActorMessage::StoreCandles {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
        candles,
    };
    actor_ref.ask(store_msg).await?;

    // Get candles with limit
    let get_msg = LmdbActorMessage::GetCandles {
        symbol: "BTCUSDT".to_string(),
        timeframe: 60,
        start: 60000,
        end: 600000,
        limit: Some(5),
    };

    match actor_ref.ask(get_msg).await? {
        LmdbActorResponse::Candles(retrieved_candles) => {
            assert_eq!(retrieved_candles.len(), 5, "Should respect limit parameter");
        },
        response => panic!("Expected Candles, got {:?}", response),
    }

    Ok(())
}

#[tokio::test]
async fn test_concurrent_database_initialization() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let actor = LmdbActor::new(temp_dir.path(), 75)?;
    let actor_ref = kameo::spawn(actor);
    
    // Spawn multiple concurrent operations that require the same database initialization
    let symbol = "BTCUSDT".to_string();
    let timeframe = 60u64;
    
    // Create multiple concurrent tasks that all require database initialization
    let mut handles = Vec::new();
    
    for i in 0..10 {
        let actor_ref_clone = actor_ref.clone();
        let symbol_clone = symbol.clone();
        
        let handle = tokio::spawn(async move {
            // Each task tries to store candles, which should trigger initialization
            let candle = create_test_candle(
                (i + 1) * 60000, 
                50000.0 + i as f64 * 100.0, 
                50001.0 + i as f64 * 100.0, 
                49999.0 + i as f64 * 100.0, 
                50000.0 + i as f64 * 100.0, 
                100.0
            );
            
            let store_msg = LmdbActorMessage::StoreCandles {
                symbol: symbol_clone,
                timeframe,
                candles: vec![candle],
            };
            
            actor_ref_clone.ask(store_msg).await
        });
        
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    let mut success_count = 0;
    for handle in handles {
        match handle.await? {
            Ok(LmdbActorResponse::Success) => success_count += 1,
            Ok(response) => panic!("Expected Success, got {:?}", response),
            Err(e) => panic!("Task failed: {}", e),
        }
    }
    
    assert_eq!(success_count, 10, "All concurrent operations should succeed");
    
    // Verify that all data was stored correctly
    let range_msg = LmdbActorMessage::GetDataRange {
        symbol: symbol,
        timeframe,
    };
    
    match actor_ref.ask(range_msg).await? {
        LmdbActorResponse::DataRange { count, .. } => {
            assert_eq!(count, 10, "All 10 candles should be stored");
        },
        response => panic!("Expected DataRange, got {:?}", response),
    }
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_mixed_operations() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let actor = LmdbActor::new(temp_dir.path(), 75)?;
    let actor_ref = kameo::spawn(actor);
    
    let symbol = "ETHUSDT".to_string();
    let timeframe = 300u64;
    
    // Create multiple concurrent operations of different types
    let mut handles = Vec::new();
    
    // Store operations
    for i in 0..5 {
        let actor_ref_clone = actor_ref.clone();
        let symbol_clone = symbol.clone();
        
        let handle = tokio::spawn(async move {
            let candle = create_test_candle(
                (i + 1) * 300000, 
                3000.0 + i as f64 * 10.0, 
                3001.0 + i as f64 * 10.0, 
                2999.0 + i as f64 * 10.0, 
                3000.0 + i as f64 * 10.0, 
                50.0
            );
            
            let store_msg = LmdbActorMessage::StoreCandles {
                symbol: symbol_clone,
                timeframe,
                candles: vec![candle],
            };
            
            actor_ref_clone.ask(store_msg).await
        });
        
        handles.push(handle);
    }
    
    // Get operations (should auto-initialize if needed)
    for _i in 0..3 {
        let actor_ref_clone = actor_ref.clone();
        let symbol_clone = symbol.clone();
        
        let handle = tokio::spawn(async move {
            let get_msg = LmdbActorMessage::GetDataRange {
                symbol: symbol_clone,
                timeframe,
            };
            
            actor_ref_clone.ask(get_msg).await
        });
        
        handles.push(handle);
    }
    
    // Validation operations
    for _i in 0..2 {
        let actor_ref_clone = actor_ref.clone();
        let symbol_clone = symbol.clone();
        
        let handle = tokio::spawn(async move {
            let validate_msg = LmdbActorMessage::ValidateDataRange {
                symbol: symbol_clone,
                timeframe,
                start: 300000,
                end: 1800000,
            };
            
            actor_ref_clone.ask(validate_msg).await
        });
        
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    let mut results = Vec::new();
    for handle in handles {
        let result = handle.await?;
        results.push(result);
    }
    
    // All operations should succeed without errors
    for result in results {
        match result {
            Ok(LmdbActorResponse::Success) |
            Ok(LmdbActorResponse::DataRange { .. }) |
            Ok(LmdbActorResponse::ValidationResult { .. }) => {
                // Expected responses
            },
            Ok(response) => panic!("Unexpected response: {:?}", response),
            Err(e) => panic!("Operation failed: {}", e),
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_multiple_symbols_concurrent_initialization() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let actor = LmdbActor::new(temp_dir.path(), 75)?;
    let actor_ref = kameo::spawn(actor);
    
    let symbols = vec!["BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT"];
    let timeframes = vec![60, 300, 3600];
    
    // Create concurrent operations for different symbol-timeframe combinations
    let mut handles = Vec::new();
    
    for symbol in &symbols {
        for &timeframe in &timeframes {
            let actor_ref_clone = actor_ref.clone();
            let symbol_clone = symbol.to_string();
            
            let handle = tokio::spawn(async move {
                // Each combination gets its own candle
                let candle = create_test_candle(
                    timeframe as i64 * 1000, 
                    1000.0, 1001.0, 999.0, 1000.0, 10.0
                );
                
                let store_msg = LmdbActorMessage::StoreCandles {
                    symbol: symbol_clone.clone(),
                    timeframe,
                    candles: vec![candle],
                };
                
                // Also test concurrent get operation
                let get_msg = LmdbActorMessage::GetDataRange {
                    symbol: symbol_clone,
                    timeframe,
                };
                
                // Store first, then get
                let store_result = actor_ref_clone.ask(store_msg).await;
                let get_result = actor_ref_clone.ask(get_msg).await;
                
                (store_result, get_result)
            });
            
            handles.push(handle);
        }
    }
    
    // Wait for all operations and verify success
    let mut successful_operations = 0;
    for handle in handles {
        let (store_result, get_result) = handle.await?;
        
        match store_result {
            Ok(LmdbActorResponse::Success) => successful_operations += 1,
            Ok(response) => panic!("Expected Success for store, got {:?}", response),
            Err(e) => panic!("Store operation failed: {}", e),
        }
        
        match get_result {
            Ok(LmdbActorResponse::DataRange { count, .. }) => {
                assert_eq!(count, 1, "Should have stored exactly 1 candle");
                successful_operations += 1;
            },
            Ok(response) => panic!("Expected DataRange for get, got {:?}", response),
            Err(e) => panic!("Get operation failed: {}", e),
        }
    }
    
    let expected_operations = symbols.len() * timeframes.len() * 2; // store + get for each combination
    assert_eq!(successful_operations, expected_operations, "All operations should succeed");
    
    Ok(())
}

#[tokio::test]
async fn test_race_condition_stress_test() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let actor = LmdbActor::new(temp_dir.path(), 75)?;
    let actor_ref = kameo::spawn(actor);
    
    // Stress test with many concurrent operations on the same symbol/timeframe
    let symbol = "STRESSTEST".to_string();
    let timeframe = 60u64;
    let num_concurrent_ops = 50;
    
    let mut handles = Vec::new();
    
    // Create many concurrent operations that all target the same database
    for i in 0..num_concurrent_ops {
        let actor_ref_clone = actor_ref.clone();
        let symbol_clone = symbol.clone();
        
        let handle = tokio::spawn(async move {
            let operation_type = i % 4; // Vary operation types
            
            match operation_type {
                0 => {
                    // Store operation
                    let candle = create_test_candle(
                        (i + 1) * 60000, 
                        50000.0 + i as f64, 
                        50001.0 + i as f64, 
                        49999.0 + i as f64, 
                        50000.0 + i as f64, 
                        1.0
                    );
                    
                    let msg = LmdbActorMessage::StoreCandles {
                        symbol: symbol_clone,
                        timeframe,
                        candles: vec![candle],
                    };
                    
                    actor_ref_clone.ask(msg).await
                },
                1 => {
                    // Get data range operation
                    let msg = LmdbActorMessage::GetDataRange {
                        symbol: symbol_clone,
                        timeframe,
                    };
                    
                    actor_ref_clone.ask(msg).await
                },
                2 => {
                    // Get candles operation
                    let msg = LmdbActorMessage::GetCandles {
                        symbol: symbol_clone,
                        timeframe,
                        start: 60000,
                        end: 3000000,
                        limit: Some(10),
                    };
                    
                    actor_ref_clone.ask(msg).await
                },
                3 => {
                    // Validation operation
                    let msg = LmdbActorMessage::ValidateDataRange {
                        symbol: symbol_clone,
                        timeframe,
                        start: 60000,
                        end: 3000000,
                    };
                    
                    actor_ref_clone.ask(msg).await
                },
                _ => unreachable!(),
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all operations and count successes
    let mut successful_operations = 0;
    let mut failed_operations = 0;
    
    for handle in handles {
        match handle.await? {
            Ok(LmdbActorResponse::Success) |
            Ok(LmdbActorResponse::DataRange { .. }) |
            Ok(LmdbActorResponse::Candles(_)) |
            Ok(LmdbActorResponse::ValidationResult { .. }) |
            Ok(LmdbActorResponse::StorageStats { .. }) |
            Ok(LmdbActorResponse::VolumeProfileValidation(_)) |
            Ok(LmdbActorResponse::VolumeProfileValidationHistory(_)) => {
                successful_operations += 1;
            },
            Ok(LmdbActorResponse::ErrorResponse(err)) => {
                // Should not have initialization errors anymore
                if err.contains("Database initialization error") {
                    panic!("Got initialization error that should have been fixed: {}", err);
                }
                failed_operations += 1;
            },
            Err(e) => {
                panic!("Operation failed unexpectedly: {}", e);
            }
        }
    }
    
    // All operations should succeed (no race condition errors)
    assert_eq!(successful_operations, num_concurrent_ops, "All operations should succeed without race conditions");
    assert_eq!(failed_operations, 0, "No operations should fail due to race conditions");
    
    Ok(())
}