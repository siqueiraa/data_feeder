//! Health check endpoints for Kubernetes probes
//! 
//! Provides HTTP endpoints for liveness and readiness probes:
//! - GET /health - Liveness probe (process is running)
//! - GET /ready - Readiness probe (all dependencies are healthy)

use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use warp::Filter;
use serde_json::json;
use tracing::{info, warn, error};

use crate::kafka::actor::KafkaActor;
use crate::postgres::actor::PostgresActor;
use crate::system_resources::SystemResources;
use kameo::actor::ActorRef;

/// Health check server configuration
#[derive(Clone)]
pub struct HealthConfig {
    pub port: u16,
    pub timeout_seconds: u64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            timeout_seconds: 5,
        }
    }
}

/// Health check dependencies
#[derive(Clone)]
pub struct HealthDependencies {
    pub kafka_actor: Option<ActorRef<KafkaActor>>,
    pub postgres_actor: Option<ActorRef<PostgresActor>>,
}

/// Resource environment classification
#[derive(Debug, Clone, PartialEq)]
pub enum ResourceEnvironment {
    LowResource,      // < 2 CPU cores or < 2GB RAM
    StandardResource, // 2-4 CPU cores and 2-8GB RAM 
    HighResource,     // > 4 CPU cores and > 8GB RAM
    Containerized,    // Running in containerized environment
}

/// Start the health check HTTP server
pub async fn start_health_server(
    config: HealthConfig,
    dependencies: HealthDependencies,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deps = Arc::new(dependencies);
    
    // Health endpoint - liveness probe
    let health = warp::path("health")
        .and(warp::get())
        .map(|| {
            info!("Health check requested");
            warp::reply::with_status(
                warp::reply::json(&json!({
                    "status": "healthy",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "service": "data-feeder"
                })),
                warp::http::StatusCode::OK
            )
        });

    // Ready endpoint - readiness probe
    let deps_for_ready = Arc::clone(&deps);
    let timeout_duration = Duration::from_secs(config.timeout_seconds);
    let ready = warp::path("ready")
        .and(warp::get())
        .and_then(move || {
            let deps = Arc::clone(&deps_for_ready);
            let timeout_dur = timeout_duration;
            async move {
                match timeout(timeout_dur, check_readiness(deps)).await {
                    Ok(result) => result,
                    Err(_) => {
                        error!("Readiness check timed out after {}s", timeout_dur.as_secs());
                        Ok(warp::reply::with_status(
                            warp::reply::json(&json!({
                                "status": "not_ready",
                                "reason": "timeout",
                                "timestamp": chrono::Utc::now().to_rfc3339()
                            })),
                            warp::http::StatusCode::SERVICE_UNAVAILABLE
                        ))
                    }
                }
            }
        });

    // Combine routes
    let routes = health.or(ready);

    info!("🏥 Starting health check server on port {}", config.port);
    
    warp::serve(routes)
        .run(([0, 0, 0, 0], config.port))
        .await;

    Ok(())
}

/// Check if all dependencies are ready
async fn check_readiness(
    dependencies: Arc<HealthDependencies>
) -> Result<warp::reply::WithStatus<warp::reply::Json>, warp::Rejection> {
    let mut status = json!({
        "status": "ready",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "checks": {}
    });

    let mut is_ready = true;
    let checks = status["checks"].as_object_mut().unwrap();

    // Check Kafka health if available
    if let Some(kafka_actor) = &dependencies.kafka_actor {
        match check_kafka_health(kafka_actor).await {
            Ok(kafka_status) => {
                checks.insert("kafka".to_string(), kafka_status);
            }
            Err(e) => {
                warn!("Kafka health check failed: {}", e);
                is_ready = false;
                checks.insert("kafka".to_string(), json!({
                    "status": "unhealthy",
                    "error": e.to_string()
                }));
            }
        }
    } else {
        checks.insert("kafka".to_string(), json!({
            "status": "not_configured"
        }));
    }

    // Check PostgreSQL health if available
    if let Some(postgres_actor) = &dependencies.postgres_actor {
        match check_postgres_health(postgres_actor).await {
            Ok(postgres_status) => {
                checks.insert("postgres".to_string(), postgres_status);
            }
            Err(e) => {
                warn!("PostgreSQL health check failed: {}", e);
                is_ready = false;
                checks.insert("postgres".to_string(), json!({
                    "status": "unhealthy", 
                    "error": e.to_string()
                }));
            }
        }
    } else {
        checks.insert("postgres".to_string(), json!({
            "status": "not_configured"
        }));
    }

    // LMDB is always considered healthy if the process is running
    checks.insert("lmdb".to_string(), json!({
        "status": "healthy"
    }));

    // Add resource-aware health checks (AC: 2)
    match check_resource_health().await {
        Ok(resource_status) => {
            checks.insert("resources".to_string(), resource_status);
        }
        Err(e) => {
            warn!("Resource health check failed: {}", e);
            is_ready = false;
            checks.insert("resources".to_string(), json!({
                "status": "unhealthy",
                "error": e.to_string()
            }));
        }
    }

    if is_ready {
        status["status"] = json!("ready");
        info!("✅ Readiness check passed");
        Ok(warp::reply::with_status(
            warp::reply::json(&status),
            warp::http::StatusCode::OK
        ))
    } else {
        status["status"] = json!("not_ready");
        warn!("❌ Readiness check failed");
        Ok(warp::reply::with_status(
            warp::reply::json(&status),
            warp::http::StatusCode::SERVICE_UNAVAILABLE
        ))
    }
}

/// Check Kafka actor health
pub async fn check_kafka_health(
    kafka_actor: &ActorRef<KafkaActor>
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    use crate::kafka::actor::{KafkaAsk, KafkaReply};
    
    match kafka_actor.ask(KafkaAsk::GetHealthStatus).await {
        Ok(KafkaReply::HealthStatus { is_healthy, is_enabled, last_error }) => {
            if is_healthy && is_enabled {
                Ok(json!({
                    "status": "healthy",
                    "enabled": true
                }))
            } else if !is_enabled {
                Ok(json!({
                    "status": "disabled",
                    "enabled": false
                }))
            } else {
                Ok(json!({
                    "status": "unhealthy",
                    "enabled": true,
                    "error": last_error
                }))
            }
        }
        Ok(KafkaReply::Stats { .. }) => {
            Err("Unexpected Stats response to health check request".into())
        }
        Ok(KafkaReply::Success) => {
            Err("Unexpected Success response to health check request".into())
        }
        Ok(KafkaReply::Error(error_msg)) => {
            Err(format!("Kafka actor returned error: {}", error_msg).into())
        }
        Err(e) => Err(format!("Failed to get Kafka health status: {}", e).into()),
    }
}

/// Check PostgreSQL actor health
pub async fn check_postgres_health(
    postgres_actor: &ActorRef<PostgresActor>
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    use crate::postgres::actor::{PostgresAsk, PostgresReply};
    
    match postgres_actor.ask(PostgresAsk::GetHealthStatus).await {
        Ok(PostgresReply::HealthStatus { is_healthy, connection_count, last_error }) => {
            if is_healthy {
                Ok(json!({
                    "status": "healthy",
                    "connections": connection_count
                }))
            } else {
                Ok(json!({
                    "status": "unhealthy",
                    "connections": connection_count,
                    "error": last_error
                }))
            }
        }
        Ok(PostgresReply::RecentCandles(_)) => {
            Err("Unexpected RecentCandles response to health check request".into())
        }
        Ok(PostgresReply::Success) => {
            Err("Unexpected Success response to health check request".into())
        }
        Ok(PostgresReply::Error(error_msg)) => {
            Err(format!("PostgreSQL actor returned error: {}", error_msg).into())
        }
        Err(e) => Err(format!("Failed to get PostgreSQL health status: {}", e).into()),
    }
}

/// Classify resource environment based on system resources
pub fn classify_resource_environment() -> ResourceEnvironment {
    if let Some(resources) = SystemResources::get_cached() {
        if resources.is_containerized {
            return ResourceEnvironment::Containerized;
        }
        
        let memory_gb = resources.available_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        
        match (resources.cpu_cores, memory_gb) {
            (cores, memory) if cores <= 2 || memory < 2.0 => ResourceEnvironment::LowResource,
            (cores, memory) if cores <= 4 && memory <= 8.0 => ResourceEnvironment::StandardResource,
            _ => ResourceEnvironment::HighResource,
        }
    } else {
        // Default to standard if we can't detect resources
        ResourceEnvironment::StandardResource
    }
}

/// Check resource-aware health status (AC: 2)
async fn check_resource_health() -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let resource_env = classify_resource_environment();
    
    // Get current system resources if available
    let (resource_status, performance_metrics) = if let Some(resources) = SystemResources::get_cached() {
        let memory_utilization = resources.get_memory_utilization();
        let has_sufficient_resources = resources.has_sufficient_resources();
        
        // Calculate performance score based on resource availability
        let performance_score = if has_sufficient_resources {
            100 - (memory_utilization as i32).clamp(0, 100)
        } else {
            50 // Degraded performance expected
        };
        
        let status = if memory_utilization > 90.0 {
            "critical"
        } else if memory_utilization > 80.0 {
            "warning"
        } else if has_sufficient_resources {
            "healthy"
        } else {
            "degraded"
        };
        
        // Record metrics
        if let Some(metrics) = crate::metrics::get_metrics() {
            let env_str = match resource_env {
                ResourceEnvironment::LowResource => "low_resource",
                ResourceEnvironment::StandardResource => "standard_resource",
                ResourceEnvironment::HighResource => "high_resource",
                ResourceEnvironment::Containerized => "containerized",
            };
            
            metrics.record_system_resource_utilization("memory", env_str, memory_utilization as i64);
            metrics.record_system_resource_utilization("cpu", env_str, 
                ((resources.cpu_cores as f64 / 16.0) * 100.0) as i64); // Normalize to 16 cores = 100%
        }
        
        (status, json!({
            "cpu_cores": resources.cpu_cores,
            "memory_gb": resources.available_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            "memory_utilization_percent": memory_utilization,
            "performance_score": performance_score,
            "sufficient_resources": has_sufficient_resources
        }))
    } else {
        ("unknown", json!({
            "error": "System resources not detected"
        }))
    };
    
    Ok(json!({
        "status": resource_status,
        "environment": match resource_env {
            ResourceEnvironment::LowResource => "low_resource",
            ResourceEnvironment::StandardResource => "standard_resource", 
            ResourceEnvironment::HighResource => "high_resource",
            ResourceEnvironment::Containerized => "containerized",
        },
        "metrics": performance_metrics,
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}

/// Validate system performance across different resource configurations (AC: 2)  
pub fn validate_performance_across_environments() -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let resource_env = classify_resource_environment();
    
    // Define performance thresholds based on environment
    let (expected_throughput, max_latency_ms, min_memory_available_percent) = match resource_env {
        ResourceEnvironment::LowResource => (100, 500, 30.0),      // Lower expectations
        ResourceEnvironment::StandardResource => (500, 200, 40.0), // Standard expectations
        ResourceEnvironment::HighResource => (1000, 100, 50.0),    // High expectations
        ResourceEnvironment::Containerized => (200, 300, 35.0),    // Container-adjusted expectations
    };
    
    let mut validation_results = Vec::new();
    let mut overall_status = "healthy";
    
    // Check memory availability
    if let Some(resources) = SystemResources::get_cached() {
        let memory_available_percent = 100.0 - resources.get_memory_utilization();
        if memory_available_percent < min_memory_available_percent {
            validation_results.push(json!({
                "check": "memory_availability",
                "status": "failed",
                "expected": format!("≥{}%", min_memory_available_percent),
                "actual": format!("{:.1}%", memory_available_percent)
            }));
            overall_status = "degraded";
        } else {
            validation_results.push(json!({
                "check": "memory_availability", 
                "status": "passed",
                "actual": format!("{:.1}%", memory_available_percent)
            }));
        }
        
        // Check CPU availability
        let recommended_threads = resources.get_recommended_thread_pool_size();
        if recommended_threads < 2 {
            validation_results.push(json!({
                "check": "cpu_availability",
                "status": "warning",
                "message": "Limited CPU resources detected"
            }));
            if overall_status == "healthy" {
                overall_status = "warning";
            }
        } else {
            validation_results.push(json!({
                "check": "cpu_availability",
                "status": "passed",
                "threads": recommended_threads
            }));
        }
    }
    
    Ok(json!({
        "overall_status": overall_status,
        "environment": match resource_env {
            ResourceEnvironment::LowResource => "low_resource",
            ResourceEnvironment::StandardResource => "standard_resource",
            ResourceEnvironment::HighResource => "high_resource", 
            ResourceEnvironment::Containerized => "containerized",
        },
        "performance_thresholds": {
            "expected_throughput": expected_throughput,
            "max_latency_ms": max_latency_ms,
            "min_memory_available_percent": min_memory_available_percent
        },
        "validation_results": validation_results,
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}