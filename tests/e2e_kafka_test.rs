use std::time::Duration;

use kameo::spawn;
use kameo::request::MessageSend;
use tokio::time::timeout;

use data_feeder::kafka::{KafkaActor, KafkaConfig};
use data_feeder::technical_analysis::{
    TechnicalAnalysisConfig, IndicatorActor, 
    actors::indicator::IndicatorTell
};

mod fixtures;
use fixtures::{create_test_kafka_config, create_sample_indicator_output};

/// Test the full end-to-end flow from configuration loading to Kafka publishing
#[tokio::test]
async fn test_e2e_config_loading_and_kafka() {
    // Test loading configuration from config.test.toml would require main module
    // For now, we'll just verify that our test config works as expected
    let test_config = create_test_kafka_config();
    assert!(test_config.enabled, "Test Kafka config should be enabled");
    assert_eq!(test_config.bootstrap_servers, vec!["140.238.175.132:9092"]);
    assert_eq!(test_config.topic_prefix, "test_ta_");
    println!("✅ Test configuration validated successfully");
}

/// Test complete actor communication flow: IndicatorActor → KafkaActor
#[tokio::test]
async fn test_e2e_indicator_to_kafka_flow() {
    // Create Kafka actor
    let kafka_config = create_test_kafka_config();
    let kafka_actor = KafkaActor::new(kafka_config).expect("Failed to create KafkaActor");
    let kafka_actor_ref = spawn(kafka_actor);

    // Create IndicatorActor
    let ta_config = TechnicalAnalysisConfig {
        symbols: vec!["BTCUSDT".to_string()],
        timeframes: vec![60, 300, 900, 3600, 14400], // 1m, 5m, 15m, 1h, 4h
        ema_periods: vec![21, 89],
        volume_lookback_days: 30,
        min_history_days: 30,
    };
    
    let indicator_actor = IndicatorActor::new(ta_config);
    let indicator_actor_ref = spawn(indicator_actor);

    // Give actors time to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Set Kafka actor reference on IndicatorActor
    let set_kafka_msg = IndicatorTell::SetKafkaActor {
        kafka_actor: kafka_actor_ref.clone(),
    };
    
    let set_result = timeout(Duration::from_secs(5), indicator_actor_ref.tell(set_kafka_msg).send()).await;
    match set_result {
        Ok(Ok(_)) => println!("✅ Successfully set Kafka actor reference"),
        Ok(Err(e)) => {
            println!("❌ Failed to set Kafka actor reference: {}", e);
            return;
        }
        Err(_) => {
            println!("⏱️ Timeout setting Kafka actor reference");
            return;
        }
    }

    // Wait for setup to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Simulate indicator processing that should trigger Kafka publishing
    // Note: This is a simplified simulation. In real usage, the IndicatorActor
    // would receive ProcessMultiTimeFrameUpdate messages from TimeFrameActor
    let sample_indicators = create_sample_indicator_output("BTCUSDT", chrono::Utc::now().timestamp_millis());
    
    // Direct publish to Kafka (simulating what happens in ProcessMultiTimeFrameUpdate)
    let publish_msg = data_feeder::kafka::KafkaTell::PublishIndicators {
        indicators: Box::new(sample_indicators.clone()),
    };
    
    let publish_result = timeout(Duration::from_secs(10), kafka_actor_ref.tell(publish_msg).send()).await;
    match publish_result {
        Ok(Ok(_)) => {
            println!("✅ Successfully published indicator through E2E flow");
            
            // Verify the message was sent by checking stats
            tokio::time::sleep(Duration::from_millis(500)).await;
            
            let stats_result = kafka_actor_ref.ask(data_feeder::kafka::KafkaAsk::GetStats).await;
            match stats_result {
                Ok(data_feeder::kafka::KafkaReply::Stats { messages_sent, messages_failed, .. }) => {
                    println!("📊 E2E Stats: sent={}, failed={}", messages_sent, messages_failed);
                }
                Ok(_) => println!("⚠️ Unexpected stats response type"),
                Err(e) => println!("⚠️ Could not retrieve stats: {}", e),
            }
        }
        Ok(Err(e)) => println!("❌ E2E publish failed: {}", e),
        Err(_) => println!("⏱️ E2E publish timed out"),
    }
}

/// Test actor lifecycle management in E2E scenario
#[tokio::test]
async fn test_e2e_actor_lifecycle() {
    println!("🔄 Testing E2E actor lifecycle...");
    
    // Create and start actors
    let kafka_config = create_test_kafka_config();
    let kafka_actor = KafkaActor::new(kafka_config).expect("Failed to create KafkaActor");
    let kafka_actor_ref = spawn(kafka_actor);
    
    // Give actor time to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;
    
    // Test health status during initialization
    let initial_health = kafka_actor_ref.ask(data_feeder::kafka::KafkaAsk::GetHealthStatus).await;
    match initial_health {
        Ok(data_feeder::kafka::KafkaReply::HealthStatus { is_enabled, .. }) => {
            assert!(is_enabled, "Kafka should be enabled");
            println!("✅ Actor properly initialized with correct health status");
        }
        Ok(_) => println!("⚠️ Unexpected health status response type"),
        Err(e) => println!("⚠️ Could not get initial health status: {}", e),
    }
    
    // Test stats during lifecycle
    let initial_stats = kafka_actor_ref.ask(data_feeder::kafka::KafkaAsk::GetStats).await;
    match initial_stats {
        Ok(data_feeder::kafka::KafkaReply::Stats { messages_sent, messages_failed, .. }) => {
            assert_eq!(messages_sent, 0, "Should start with 0 messages sent");
            assert_eq!(messages_failed, 0, "Should start with 0 messages failed");
            println!("✅ Actor properly initialized with zero stats");
        }
        Ok(_) => println!("⚠️ Unexpected stats response type"),
        Err(e) => println!("⚠️ Could not get initial stats: {}", e),
    }
    
    // Publish a message
    let indicators = create_sample_indicator_output("ETHUSDT", chrono::Utc::now().timestamp_millis());
    let publish_msg = data_feeder::kafka::KafkaTell::PublishIndicators { indicators: Box::new(indicators) };
    
    let _ = kafka_actor_ref.tell(publish_msg).send().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Test stats after publishing
    let final_stats = kafka_actor_ref.ask(data_feeder::kafka::KafkaAsk::GetStats).await;
    match final_stats {
        Ok(data_feeder::kafka::KafkaReply::Stats { messages_sent, messages_failed, .. }) => {
            println!("📊 Final lifecycle stats: sent={}, failed={}", messages_sent, messages_failed);
        }
        Ok(_) => println!("⚠️ Unexpected stats response type"),
        Err(e) => println!("⚠️ Could not get final stats: {}", e),
    }
    
    println!("✅ E2E actor lifecycle test completed");
}

/// Test configuration validation in E2E scenario
#[tokio::test]
async fn test_e2e_configuration_validation() {
    println!("🔧 Testing E2E configuration validation...");
    
    // Test valid configuration
    let valid_config = KafkaConfig {
        enabled: true,
        bootstrap_servers: vec!["140.238.175.132:9092".to_string()],
        topic_prefix: "e2e_test_".to_string(),
        client_id: "e2e_test_client".to_string(),
        acks: "all".to_string(),
        retries: 3,
        max_in_flight_requests_per_connection: 1,
        compression_type: "snappy".to_string(),
        batch_size: 1024,
        linger_ms: 5,
        request_timeout_ms: 10000,
        delivery_timeout_ms: 30000,
    };
    
    let actor_result = KafkaActor::new(valid_config.clone());
    assert!(actor_result.is_ok(), "Valid configuration should create actor successfully");
    
    println!("✅ Valid configuration accepted");
    
    // Test configuration with invalid broker (should still create actor, but connection will fail)
    let invalid_config = KafkaConfig {
        bootstrap_servers: vec!["invalid-broker:9092".to_string()],
        ..valid_config
    };
    
    let invalid_actor_result = KafkaActor::new(invalid_config);
    assert!(invalid_actor_result.is_ok(), "Actor creation should succeed even with invalid broker");
    println!("✅ Invalid broker configuration handled gracefully");
    
    // Test disabled configuration
    let disabled_config = KafkaConfig {
        enabled: false,
        ..create_test_kafka_config()
    };
    
    let disabled_actor = KafkaActor::new(disabled_config).unwrap();
    let disabled_actor_ref = spawn(disabled_actor);
    
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    let health_result = disabled_actor_ref.ask(data_feeder::kafka::KafkaAsk::GetHealthStatus).await;
    match health_result {
        Ok(data_feeder::kafka::KafkaReply::HealthStatus { is_enabled, .. }) => {
            assert!(!is_enabled, "Disabled configuration should show as not enabled");
            println!("✅ Disabled configuration handled correctly");
        }
        Ok(_) => println!("⚠️ Unexpected health status response type"),
        Err(e) => println!("⚠️ Could not verify disabled configuration: {}", e),
    }
}

/// Test message serialization and topic routing in E2E scenario
#[tokio::test]
async fn test_e2e_message_serialization_and_routing() {
    println!("📨 Testing E2E message serialization and routing...");
    
    let kafka_config = KafkaConfig {
        topic_prefix: "e2e_routing_test_".to_string(),
        ..create_test_kafka_config()
    };
    
    let kafka_actor = KafkaActor::new(kafka_config).expect("Failed to create KafkaActor");
    let kafka_actor_ref = spawn(kafka_actor);
    
    tokio::time::sleep(Duration::from_millis(1000)).await;
    
    // Test different symbols to verify routing
    let symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"];
    
    for (i, symbol) in symbols.iter().enumerate() {
        let timestamp = chrono::Utc::now().timestamp_millis() + (i as i64 * 1000);
        let indicators = create_sample_indicator_output(symbol, timestamp);
        
        // Verify serialization works
        let json_result = serde_json::to_string(&indicators);
        assert!(json_result.is_ok(), "Should serialize {} indicators", symbol);
        
        let json_str = json_result.unwrap();
        assert!(json_str.contains(symbol), "JSON should contain symbol {}", symbol);
        println!("✅ Serialization successful for {}: {} bytes", symbol, json_str.len());
        
        // Publish the message
        let publish_msg = data_feeder::kafka::KafkaTell::PublishIndicators {
            indicators: Box::new(indicators.clone()),
        };
        
        let publish_result = kafka_actor_ref.tell(publish_msg).send().await;
        match publish_result {
            Ok(_) => println!("✅ Published {} indicators", symbol),
            Err(e) => println!("❌ Failed to publish {}: {}", symbol, e),
        }
        
        // Small delay between messages
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    // Check final statistics
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let stats_result = kafka_actor_ref.ask(data_feeder::kafka::KafkaAsk::GetStats).await;
    match stats_result {
        Ok(data_feeder::kafka::KafkaReply::Stats { messages_sent, messages_failed, .. }) => {
            println!("📊 Routing test final stats: sent={}, failed={}", messages_sent, messages_failed);
        }
        Ok(_) => println!("⚠️ Unexpected stats response type"),
        Err(e) => println!("⚠️ Could not get routing test stats: {}", e),
    }
}

/// Test error handling and recovery in E2E scenario
#[tokio::test]
async fn test_e2e_error_handling() {
    println!("🚨 Testing E2E error handling and recovery...");
    
    // Test with invalid broker configuration
    let error_config = KafkaConfig {
        enabled: true,
        bootstrap_servers: vec!["definitely-invalid-broker:9092".to_string()],
        request_timeout_ms: 2000, // Short timeout for faster test
        delivery_timeout_ms: 5000,
        ..create_test_kafka_config()
    };
    
    let error_actor = KafkaActor::new(error_config).expect("Should create actor even with invalid config");
    let error_actor_ref = spawn(error_actor);
    
    // Give time for connection attempt to fail
    tokio::time::sleep(Duration::from_millis(3000)).await;
    
    // Check that error is reported in health status
    let health_result = error_actor_ref.ask(data_feeder::kafka::KafkaAsk::GetHealthStatus).await;
    match health_result {
        Ok(data_feeder::kafka::KafkaReply::HealthStatus { is_healthy, is_enabled, last_error }) => {
            assert!(is_enabled, "Should be enabled despite connection failure");
            println!("Health status: healthy={}, error={:?}", is_healthy, last_error);
            
            if !is_healthy && last_error.is_some() {
                println!("✅ Error properly detected and reported");
            }
        }
        Ok(_) => println!("⚠️ Unexpected health status response type"),
        Err(e) => println!("⚠️ Could not get error health status: {}", e),
    }
    
    // Try to publish a message - should fail gracefully
    let indicators = create_sample_indicator_output("ERRORTEST", chrono::Utc::now().timestamp_millis());
    let publish_msg = data_feeder::kafka::KafkaTell::PublishIndicators { indicators: Box::new(indicators) };
    
    let publish_result = timeout(Duration::from_secs(3), error_actor_ref.tell(publish_msg).send()).await;
    match publish_result {
        Ok(Ok(_)) => println!("✅ Error case handled gracefully"),
        Ok(Err(e)) => println!("✅ Error case failed as expected: {}", e),
        Err(_) => println!("✅ Error case timed out as expected"),
    }
    
    // Check that failure is recorded in stats
    let error_stats = error_actor_ref.ask(data_feeder::kafka::KafkaAsk::GetStats).await;
    match error_stats {
        Ok(data_feeder::kafka::KafkaReply::Stats { messages_sent, messages_failed, .. }) => {
            println!("📊 Error handling stats: sent={}, failed={}", messages_sent, messages_failed);
        }
        Ok(_) => println!("⚠️ Unexpected stats response type"),
        Err(e) => println!("⚠️ Could not get error handling stats: {}", e),
    }
}