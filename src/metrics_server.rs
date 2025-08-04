/// HTTP server for serving Prometheus metrics
/// 
/// This module provides a simple HTTP server that exposes Prometheus metrics
/// on /metrics endpoint for scraping by monitoring systems.
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use http_body_util::Full;
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::metrics::get_metrics;
use crate::health::{HealthDependencies, check_kafka_health, check_postgres_health};
use serde_json::json;
use std::sync::Arc;
use chrono;

/// Handle HTTP requests for metrics endpoint
async fn handle_request(
    req: Request<Incoming>,
    health_deps: Arc<HealthDependencies>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            match get_metrics() {
                Some(metrics) => {
                    match metrics.export_metrics() {
                        Ok(metrics_text) => {
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                                .body(Full::new(Bytes::from(metrics_text)))
                                .map_err(|_| ())
                                .or_else(|_| Ok(Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Full::new(Bytes::from("Failed to build response")))
                                    .unwrap()))
                        }
                        Err(e) => {
                            error!("Failed to export metrics: {}", e);
                            Ok(Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Full::new(Bytes::from(format!("Failed to export metrics: {}", e))))
                                .unwrap())
                        }
                    }
                }
                None => {
                    warn!("Metrics registry not initialized");
                    Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Full::new(Bytes::from("Metrics registry not initialized")))
                        .unwrap())
                }
            }
        }
        (&Method::GET, "/health") => {
            // Simple liveness probe - just check if service is running
            let health_response = json!({
                "status": "healthy",
                "service": "data_feeder",
                "timestamp": chrono::Utc::now().to_rfc3339()
            });
            
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(health_response.to_string())))
                .unwrap())
        }
        (&Method::GET, "/ready") => {
            // Comprehensive readiness probe with dependency checks
            match check_readiness(health_deps).await {
                Ok(response) => {
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(response)))
                        .unwrap())
                }
                Err(response) => {
                    Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(response)))
                        .unwrap())
                }
            }
        }
        (&Method::GET, "/startup") => {
            // Startup probe - responds immediately to indicate service is starting
            let startup_response = json!({
                "status": "starting",
                "service": "data_feeder",
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "message": "Service is starting up"
            });
            
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(startup_response.to_string())))
                .unwrap())
        }
        _ => {
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("Not Found")))
                .unwrap())
        }
    }
}

/// Check if all dependencies are ready
async fn check_readiness(dependencies: Arc<HealthDependencies>) -> Result<String, String> {
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

    // Update overall status
    if !is_ready {
        status["status"] = json!("not_ready");
    }

    let response_json = status.to_string();
    if is_ready {
        Ok(response_json)
    } else {
        Err(response_json)
    }
}

/// Start the metrics HTTP server
pub async fn start_metrics_server(
    port: u16, 
    health_dependencies: HealthDependencies,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    let health_deps = Arc::new(health_dependencies);
    
    info!("🚀 Metrics server starting on http://{}", addr);
    info!("📊 Prometheus metrics available at http://{}/metrics", addr);
    info!("💚 Health check available at http://{}/health", addr);
    info!("🔍 Readiness check available at http://{}/ready", addr);
    info!("🎯 Startup check available at http://{}/startup", addr);
    
    // Test the server immediately to confirm it's listening
    let test_addr = format!("http://localhost:{}/startup", port);
    
    // Give the server a moment to start and test connectivity
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        match reqwest::get(&test_addr).await {
            Ok(response) => {
                info!("✅ Server test successful: {} {:?}", response.status(), response.version());
            }
            Err(e) => {
                error!("❌ Server test failed: {}", e);
            }
        }
    });
    
    info!("✅ Server bound and listening on {}", addr);
    
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let health_deps_clone = Arc::clone(&health_deps);
        
        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(move |req| {
                    handle_request(req, Arc::clone(&health_deps_clone))
                }))
                .await
            {
                error!("Error serving connection: {:?}", err);
            }
        });
    }
}