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
            port: 9876,
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