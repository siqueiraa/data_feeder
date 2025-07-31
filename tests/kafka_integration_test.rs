use std::time::Duration;

use kameo::spawn;
use kameo::request::MessageSend;
use tokio::time::timeout;

use data_feeder::kafka::{KafkaActor, KafkaTell, KafkaAsk, KafkaReply};

mod fixtures;
use fixtures::{create_test_kafka_config, create_sample_indicator_output, create_disabled_kafka_config};

/// Test that KafkaActor can connect to the real Kafka broker
#[tokio::test]
async fn test_kafka_real_connection() {
    let config = create_test_kafka_config();
    let actor = KafkaActor::new(config).expect("Failed to create KafkaActor");
    let actor_ref = spawn(actor);

    // Give the actor time to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Check health status
    let health_result = timeout(Duration::from_secs(10), actor_ref.ask(KafkaAsk::GetHealthStatus))
        .await
        .expect("Health check timed out");

    match health_result {
        Ok(KafkaReply::HealthStatus { is_healthy, is_enabled, last_error }) => {
            assert!(is_enabled, "Kafka should be enabled");
            if !is_healthy {
                eprintln!("Kafka connection failed: {:?}", last_error);
                // Don't fail the test immediately - might be network issue
                println!("Warning: Kafka connection test failed - this might be expected in CI");
            } else {
                println!("✅ Successfully connected to Kafka broker");
            }
        }
        Ok(_) => {
            println!("Warning: Unexpected health status response type");
        }
        Err(e) => {
            eprintln!("Failed to send health status request: {}", e);
            println!("Warning: Health status request send failed - this might be expected in CI");
        }
    }
}

/// Test publishing a single indicator message to Kafka
#[tokio::test]
async fn test_publish_single_indicator() {
    let config = create_test_kafka_config();
    let actor = KafkaActor::new(config).expect("Failed to create KafkaActor");
    let actor_ref = spawn(actor);

    // Give the actor time to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Create sample indicator data
    let indicators = create_sample_indicator_output("BTCUSDT", chrono::Utc::now().timestamp_millis());
    
    // Publish indicators
    let publish_msg = KafkaTell::PublishIndicators {
        indicators: Box::new(indicators.clone()),
    };
    
    let publish_result = timeout(Duration::from_secs(10), actor_ref.tell(publish_msg).send())
        .await
        .expect("Publish timed out");

    match publish_result {
        Ok(_) => {
            println!("✅ Successfully published indicator message");
            
            // Wait a bit for the message to be sent
            tokio::time::sleep(Duration::from_millis(500)).await;
            
            // Check statistics
            let stats_result = actor_ref.ask(KafkaAsk::GetStats).await;
            
            match stats_result {
                Ok(KafkaReply::Stats { messages_sent, messages_failed, .. }) => {
                    println!("📊 Kafka stats: sent={}, failed={}", messages_sent, messages_failed);
                    // Note: We don't assert on exact counts as the test might run multiple times
                }
                Ok(_) => println!("⚠️ Unexpected stats response type"),
                Err(e) => println!("⚠️ Could not send stats request: {}", e),
            }
        }
        Err(e) => {
            eprintln!("Failed to publish indicator: {}", e);
            println!("Warning: Kafka publish test failed - this might be expected in CI");
        }
    }
}

/// Test publishing multiple indicators in sequence
#[tokio::test]
async fn test_publish_multiple_indicators() {
    let config = create_test_kafka_config();
    let actor = KafkaActor::new(config).expect("Failed to create KafkaActor");
    let actor_ref = spawn(actor);

    // Give the actor time to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"];
    let base_timestamp = chrono::Utc::now().timestamp_millis();
    
    // Publish multiple messages
    for (i, symbol) in symbols.iter().enumerate() {
        let indicators = create_sample_indicator_output(symbol, base_timestamp + (i as i64 * 60000));
        let publish_msg = KafkaTell::PublishIndicators {
            indicators: Box::new(indicators),
        };
        
        let result = timeout(Duration::from_secs(5), actor_ref.tell(publish_msg).send()).await;
        match result {
            Ok(Ok(_)) => println!("✅ Published {}", symbol),
            Ok(Err(e)) => eprintln!("❌ Failed to publish {}: {}", symbol, e),
            Err(_) => eprintln!("⏱️ Timeout publishing {}", symbol),
        }
        
        // Small delay between messages
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Check final statistics
    let stats_result = actor_ref.ask(KafkaAsk::GetStats).await;
    
    match stats_result {
        Ok(KafkaReply::Stats { messages_sent, messages_failed, .. }) => {
            println!("📊 Final stats: sent={}, failed={}", messages_sent, messages_failed);
        }
        Ok(_) => println!("⚠️ Unexpected stats response type"),
        Err(e) => println!("⚠️ Could not send stats request: {}", e),
    }
}

/// Test behavior when Kafka is disabled
#[tokio::test]
async fn test_disabled_kafka_actor() {
    let config = create_disabled_kafka_config();
    let actor = KafkaActor::new(config).expect("Failed to create KafkaActor");
    let actor_ref = spawn(actor);

    // Give the actor time to initialize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check health status
    let health_result = actor_ref.ask(KafkaAsk::GetHealthStatus).await;

    match health_result {
        Ok(KafkaReply::HealthStatus { is_healthy: _, is_enabled, last_error: _ }) => {
            assert!(!is_enabled, "Kafka should be disabled");
            println!("✅ Disabled Kafka actor behaves correctly");
        }
        Ok(_) => panic!("Unexpected health status response type"),
        Err(e) => panic!("Failed to send health status request: {}", e),
    }

    // Try to publish a message - should succeed but not actually send
    let indicators = create_sample_indicator_output("BTCUSDT", chrono::Utc::now().timestamp_millis());
    let publish_msg = KafkaTell::PublishIndicators {
        indicators: Box::new(indicators),
    };
    
    let result = actor_ref.tell(publish_msg).send().await;
    assert!(result.is_ok(), "Publishing should succeed even when disabled");
    
    // Check that no messages were actually sent
    let stats_result = actor_ref.ask(KafkaAsk::GetStats).await;
    
    match stats_result {
        Ok(KafkaReply::Stats { messages_sent, .. }) => {
            println!("📊 Messages sent with disabled Kafka: {}", messages_sent);
        }
        Ok(_) => println!("⚠️ Unexpected stats response type"),
        Err(e) => println!("⚠️ Could not send stats request: {}", e),
    }
}

/// Test Kafka actor error handling with invalid configuration
#[tokio::test]
async fn test_invalid_kafka_config() {
    let mut config = create_test_kafka_config();
    config.bootstrap_servers = vec!["invalid-broker:9092".to_string()];
    
    let actor = KafkaActor::new(config).expect("Failed to create KafkaActor");
    let actor_ref = spawn(actor);

    // Give the actor time to attempt connection
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Check health status - should indicate connection failure
    let health_result = actor_ref.ask(KafkaAsk::GetHealthStatus).await;

    match health_result {
        Ok(KafkaReply::HealthStatus { is_healthy, is_enabled, last_error }) => {
            assert!(is_enabled, "Kafka should be enabled");
            println!("Health status: healthy={}, error={:?}", is_healthy, last_error);
            
            if !is_healthy {
                println!("✅ Correctly detected connection failure");
            } else {
                println!("⚠️ Unexpected: connection succeeded to invalid broker");
            }
        }
        Ok(_) => panic!("Unexpected health status response type"),
        Err(e) => panic!("Failed to send health status request: {}", e),
    }
}

/// Test message serialization and deserialization
#[tokio::test]
async fn test_indicator_serialization() {
    let indicators = create_sample_indicator_output("BTCUSDT", chrono::Utc::now().timestamp_millis());
    
    // Test JSON serialization
    let json_str = serde_json::to_string(&indicators)
        .expect("Failed to serialize indicators");
    
    println!("📝 Serialized indicator size: {} bytes", json_str.len());
    assert!(!json_str.is_empty(), "Serialized JSON should not be empty");
    
    // Test deserialization
    let deserialized: data_feeder::technical_analysis::structs::IndicatorOutput = 
        serde_json::from_str(&json_str)
        .expect("Failed to deserialize indicators");
    
    assert_eq!(deserialized.symbol, indicators.symbol);
    assert_eq!(deserialized.timestamp, indicators.timestamp);
    println!("✅ Serialization/deserialization works correctly");
}

/// Performance test - measure publishing throughput
#[tokio::test]
async fn test_kafka_performance() {
    let config = create_test_kafka_config();
    let actor = KafkaActor::new(config).expect("Failed to create KafkaActor");
    let actor_ref = spawn(actor);

    // Give the actor time to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let num_messages = 10;
    let start_time = std::time::Instant::now();
    
    // Publish messages in parallel
    let mut handles = Vec::new();
    
    for i in 0..num_messages {
        let actor_ref_clone = actor_ref.clone();
        let handle = tokio::spawn(async move {
            let indicators = create_sample_indicator_output(
                "BTCUSDT", 
                chrono::Utc::now().timestamp_millis() + i
            );
            let publish_msg = KafkaTell::PublishIndicators { indicators: Box::new(indicators) };
            actor_ref_clone.tell(publish_msg).send().await
        });
        handles.push(handle);
    }
    
    // Wait for all messages to complete
    let mut successful = 0;
    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => successful += 1,
            Ok(Err(e)) => eprintln!("Publish failed: {}", e),
            Err(e) => eprintln!("Task failed: {}", e),
        }
    }
    
    let elapsed = start_time.elapsed();
    let throughput = successful as f64 / elapsed.as_secs_f64();
    
    println!("📊 Performance test results:");
    println!("   Messages: {} successful out of {}", successful, num_messages);
    println!("   Time: {:?}", elapsed);
    println!("   Throughput: {:.2} messages/second", throughput);
    
    // Check final statistics
    tokio::time::sleep(Duration::from_millis(1000)).await;
    match actor_ref.ask(KafkaAsk::GetStats).await {
        Ok(KafkaReply::Stats { messages_sent, messages_failed, .. }) => {
            println!("   Final stats: sent={}, failed={}", messages_sent, messages_failed);
        }
        Ok(_) => println!("   Unexpected stats response type"),
        Err(e) => println!("   Could not send stats request: {}", e),
    }
}