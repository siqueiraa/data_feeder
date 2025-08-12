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
#[cfg(feature = "kafka")]
use crate::health::check_kafka_health;
#[cfg(feature = "postgres")]
use crate::health::check_postgres_health;
use crate::health::{HealthDependencies, validate_performance_across_environments};
use crate::deployment_validation::{validate_deployment, quick_deployment_check};
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
        (&Method::GET, "/validate") => {
            // Resource-aware validation endpoint (AC: 2, 3)
            match validate_performance_across_environments() {
                Ok(validation_result) => {
                    let status_code = match validation_result["overall_status"].as_str() {
                        Some("healthy") => StatusCode::OK,
                        Some("warning") => StatusCode::OK, 
                        Some("degraded") => StatusCode::PARTIAL_CONTENT,
                        _ => StatusCode::SERVICE_UNAVAILABLE,
                    };
                    
                    Ok(Response::builder()
                        .status(status_code)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(validation_result.to_string())))
                        .unwrap())
                }
                Err(e) => {
                    error!("Validation check failed: {}", e);
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(json!({
                            "status": "error",
                            "error": e.to_string(),
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string())))
                        .unwrap())
                }
            }
        }
        (&Method::GET, "/dashboard/multidimensional") => {
            // Multi-dimensional performance dashboard endpoint for AC 6
            use crate::performance::get_performance_monitor;
            
            match get_performance_monitor() {
                Some(monitor) => {
                    let multi_dimensional_report = monitor.generate_multi_dimensional_performance_report();
                    
                    let dashboard_response = json!({
                        "status": "success",
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "overall_performance_score": multi_dimensional_report.overall_performance_score,
                        "dimensions": {
                            "cpu": {
                                "score": multi_dimensional_report.base_report.performance_score,
                                "avg_usage": multi_dimensional_report.base_report.avg_system_cpu_percent,
                                "bottlenecks": multi_dimensional_report.base_report.critical_bottleneck_count
                            },
                            "memory": {
                                "baseline_usage_mb": multi_dimensional_report.memory_analysis.memory_efficiency_baselines.baseline_memory_usage_mb,
                                "leak_count": multi_dimensional_report.memory_analysis.leak_detection.potential_leaks_detected.len(),
                                "fragmentation_ratio": multi_dimensional_report.memory_analysis.fragmentation_analysis.fragmentation_ratio,
                                "heap_efficiency": multi_dimensional_report.memory_analysis.heap_usage_profile.heap_efficiency_score
                            },
                            "io": {
                                "read_latency_ms": multi_dimensional_report.io_analysis.disk_io_metrics.average_read_latency_ms,
                                "write_latency_ms": multi_dimensional_report.io_analysis.disk_io_metrics.average_write_latency_ms,
                                "network_latency_ms": multi_dimensional_report.io_analysis.io_performance_baselines.baseline_network_latency_ms,
                                "throughput_score": multi_dimensional_report.io_analysis.websocket_connection_efficiency.connection_throughput_analysis.throughput_efficiency_score
                            },
                            "algorithm": {
                                "volume_profile_accuracy": multi_dimensional_report.algorithm_analysis.volume_profile_performance_tracking.algorithm_accuracy_metrics.volume_profile_accuracy_score,
                                "hotspot_count": multi_dimensional_report.algorithm_analysis.computational_complexity_profiling.algorithmic_hotspots.len(),
                                "efficiency_score": multi_dimensional_report.algorithm_analysis.volume_profile_performance_tracking.calculation_performance.algorithm_efficiency_score
                            }
                        },
                        "optimization_priorities": multi_dimensional_report.optimization_priorities,
                        "recommendations": multi_dimensional_report.base_report.recommendations,
                        "flame_graph_samples": multi_dimensional_report.enhanced_report.flame_graph_sample_count,
                        "thread_analysis": {
                            "avg_thread_count": multi_dimensional_report.enhanced_report.avg_thread_count,
                            "hotspot_threads": multi_dimensional_report.enhanced_report.hotspot_thread_count,
                            "contention_ratio": multi_dimensional_report.enhanced_report.thread_contention_ratio
                        }
                    });
                    
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(dashboard_response.to_string())))
                        .unwrap())
                }
                None => {
                    warn!("Performance monitor not initialized");
                    Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(json!({
                            "status": "error",
                            "message": "Performance monitor not initialized",
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string())))
                        .unwrap())
                }
            }
        }
        (&Method::GET, "/deploy/validate") => {
            // Full deployment validation endpoint (AC: 3, 6)
            match validate_deployment().await {
                Ok(validation_result) => {
                    let status_code = match validation_result.overall_status {
                        crate::deployment_validation::ValidationStatus::Passed => StatusCode::OK,
                        crate::deployment_validation::ValidationStatus::Warning => StatusCode::OK,
                        crate::deployment_validation::ValidationStatus::Failed => StatusCode::BAD_REQUEST,
                        crate::deployment_validation::ValidationStatus::Error => StatusCode::INTERNAL_SERVER_ERROR,
                    };
                    
                    Ok(Response::builder()
                        .status(status_code)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(serde_json::to_string(&validation_result).unwrap_or_else(|_| "{}".to_string()))))
                        .unwrap())
                }
                Err(e) => {
                    error!("Deployment validation failed: {}", e);
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(json!({
                            "status": "error",
                            "error": e.to_string(),
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string())))
                        .unwrap())
                }
            }
        }
        (&Method::GET, "/deploy/check") => {
            // Quick deployment check for CI/CD (AC: 3, 6)
            match quick_deployment_check().await {
                Ok(is_deployable) => {
                    let status_code = if is_deployable { StatusCode::OK } else { StatusCode::BAD_REQUEST };
                    
                    Ok(Response::builder()
                        .status(status_code)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(json!({
                            "deployable": is_deployable,
                            "message": if is_deployable { "System ready for deployment" } else { "Deployment not recommended" },
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string())))
                        .unwrap())
                }
                Err(e) => {
                    error!("Quick deployment check failed: {}", e);
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(json!({
                            "deployable": false,
                            "error": e.to_string(),
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string())))
                        .unwrap())
                }
            }
        }
        (&Method::GET, "/dashboard") => {
            // Enhanced CPU Performance Dashboard - Story 6.1
            match generate_cpu_dashboard().await {
                Ok(dashboard_html) => {
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/html")
                        .body(Full::new(Bytes::from(dashboard_html)))
                        .unwrap())
                }
                Err(e) => {
                    error!("Failed to generate CPU dashboard: {}", e);
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(json!({
                            "status": "error",
                            "error": e.to_string(),
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string())))
                        .unwrap())
                }
            }
        }
        (&Method::GET, "/api/performance/report") => {
            // Enhanced Performance Report API - Story 6.1
            match generate_performance_report_api().await {
                Ok(report_json) => {
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .header("Cache-Control", "no-cache")
                        .body(Full::new(Bytes::from(report_json)))
                        .unwrap())
                }
                Err(e) => {
                    error!("Failed to generate performance report: {}", e);
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(json!({
                            "status": "error",
                            "error": e.to_string(),
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string())))
                        .unwrap())
                }
            }
        }
        (&Method::GET, "/api/performance/flame-graph") => {
            // Flame Graph Data API - Story 6.1
            match generate_flame_graph_data().await {
                Ok(flame_graph_json) => {
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .header("Cache-Control", "no-cache")
                        .body(Full::new(Bytes::from(flame_graph_json)))
                        .unwrap())
                }
                Err(e) => {
                    error!("Failed to generate flame graph data: {}", e);
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(json!({
                            "status": "error",
                            "error": e.to_string(),
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string())))
                        .unwrap())
                }
            }
        }
        (&Method::GET, "/api/performance/threads") => {
            // Thread-level profiling data API - Story 6.1  
            match generate_thread_profiling_data().await {
                Ok(thread_data_json) => {
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .header("Cache-Control", "no-cache")
                        .body(Full::new(Bytes::from(thread_data_json)))
                        .unwrap())
                }
                Err(e) => {
                    error!("Failed to generate thread profiling data: {}", e);
                    Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(json!({
                            "status": "error",
                            "error": e.to_string(),
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        }).to_string())))
                        .unwrap())
                }
            }
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
    #[cfg(feature = "kafka")]
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
    #[cfg(not(feature = "kafka"))]
    {
        checks.insert("kafka".to_string(), json!({
            "status": "feature_disabled"
        }));
    }

    // Check PostgreSQL health if available
    #[cfg(feature = "postgres")]
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
    #[cfg(not(feature = "postgres"))]
    {
        checks.insert("postgres".to_string(), json!({
            "status": "feature_disabled"
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

// Enhanced Dashboard Generation Functions for Story 6.1

/// Generate comprehensive CPU performance dashboard
async fn generate_cpu_dashboard() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let performance_monitor = crate::performance::get_performance_monitor()
        .ok_or("Performance monitor not initialized")?;
    
    let enhanced_report = performance_monitor.generate_enhanced_performance_report();
    let flame_graph_data = performance_monitor.get_flame_graph_data();
    let thread_history = performance_monitor.get_thread_profiling_history(Some(20));
    
    let dashboard_html = format!(r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Feeder - CPU Performance Dashboard</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .dashboard {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        .stat-card {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .stat-value {{
            font-size: 2.5em;
            font-weight: bold;
            color: #333;
        }}
        .stat-label {{
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-top: 5px;
        }}
        .performance-score {{
            background: linear-gradient(45deg, #43a047, #66bb6a);
            color: white;
        }}
        .recommendations {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-top: 20px;
        }}
        .recommendation {{
            padding: 10px;
            margin: 10px 0;
            border-left: 4px solid #2196F3;
            background: #f8f9fa;
        }}
        .chart-container {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin: 20px 0;
        }}
        .refresh-btn {{
            background: #2196F3;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
        }}
        .timestamp {{
            color: #666;
            font-size: 0.9em;
        }}
        .hotspot {{
            background: #ffebee;
            border-left: 4px solid #f44336;
            padding: 10px;
            margin: 5px 0;
        }}
        .hotspot.critical {{ border-color: #d32f2f; }}
        .hotspot.severe {{ border-color: #f57c00; }}
        .hotspot.moderate {{ border-color: #fbc02d; }}
    </style>
</head>
<body>
    <div class="dashboard">
        <div class="header">
            <h1>🚀 Data Feeder CPU Performance Dashboard</h1>
            <p>Real-time CPU monitoring with enhanced profiling - Story 6.1</p>
            <p class="timestamp">Last updated: {timestamp}</p>
            <button class="refresh-btn" onclick="location.reload()">🔄 Refresh</button>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card performance-score">
                <div class="stat-value">{performance_score:.1}</div>
                <div class="stat-label">Performance Score</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{avg_cpu:.1}%</div>
                <div class="stat-label">Average CPU Usage</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{thread_count}</div>
                <div class="stat-label">Active Threads</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{context_switches:.0}/s</div>
                <div class="stat-label">Context Switches</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{hotspot_count}</div>
                <div class="stat-label">CPU Hotspots</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{contention_ratio:.1}%</div>
                <div class="stat-label">Thread Contention</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h3>🔥 CPU Hotspots</h3>
            {hotspots_html}
        </div>
        
        <div class="chart-container">
            <h3>📊 Flame Graph Data</h3>
            <p>Total samples: {flame_samples}</p>
            <p>Call stack depth: {flame_depth}</p>
            <div style="font-family: monospace; font-size: 12px; background: #f8f9fa; padding: 10px; border-radius: 5px;">
                {flame_graph_preview}
            </div>
        </div>
        
        <div class="recommendations">
            <h3>💡 Performance Recommendations</h3>
            {recommendations_html}
        </div>
        
        <div class="chart-container">
            <h3>📈 Performance APIs</h3>
            <ul>
                <li><a href="/api/performance/report">Enhanced Performance Report</a> - JSON format</li>
                <li><a href="/api/performance/flame-graph">Flame Graph Data</a> - JSON format</li>
                <li><a href="/api/performance/threads">Thread Profiling Data</a> - JSON format</li>
                <li><a href="/metrics">Prometheus Metrics</a> - Prometheus format</li>
            </ul>
        </div>
    </div>
    
    <script>
        // Auto-refresh every 30 seconds
        setTimeout(() => location.reload(), 30000);
    </script>
</body>
</html>"#,
        timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
        performance_score = enhanced_report.base_report.performance_score,
        avg_cpu = enhanced_report.base_report.avg_system_cpu_percent,
        thread_count = enhanced_report.avg_thread_count,
        context_switches = enhanced_report.avg_context_switches_per_second,
        hotspot_count = enhanced_report.hotspot_thread_count,
        contention_ratio = enhanced_report.thread_contention_ratio * 100.0,
        flame_samples = flame_graph_data.total_samples,
        flame_depth = flame_graph_data.call_stacks.len(),
        hotspots_html = generate_hotspots_html(&thread_history),
        flame_graph_preview = generate_flame_graph_preview(&flame_graph_data),
        recommendations_html = generate_recommendations_html(&enhanced_report.enhancement_recommendations),
    );
    
    Ok(dashboard_html)
}

/// Generate enhanced performance report as JSON API
async fn generate_performance_report_api() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let performance_monitor = crate::performance::get_performance_monitor()
        .ok_or("Performance monitor not initialized")?;
    
    let enhanced_report = performance_monitor.generate_enhanced_performance_report();
    let report_with_metadata = json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "version": "1.0",
        "story": "6.1",
        "report": enhanced_report
    });
    
    Ok(report_with_metadata.to_string())
}

/// Generate flame graph data as JSON API
async fn generate_flame_graph_data() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let performance_monitor = crate::performance::get_performance_monitor()
        .ok_or("Performance monitor not initialized")?;
    
    let flame_graph_data = performance_monitor.get_flame_graph_data();
    let flame_graph_with_metadata = json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "version": "1.0",
        "story": "6.1",
        "flame_graph": flame_graph_data
    });
    
    Ok(flame_graph_with_metadata.to_string())
}

/// Generate thread profiling data as JSON API
async fn generate_thread_profiling_data() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let performance_monitor = crate::performance::get_performance_monitor()
        .ok_or("Performance monitor not initialized")?;
    
    let thread_profiling = performance_monitor.get_thread_profiling_history(Some(50));
    let context_switching = performance_monitor.get_context_switching_history(Some(50));
    
    let thread_data = json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "version": "1.0", 
        "story": "6.1",
        "thread_profiling": thread_profiling,
        "context_switching": context_switching
    });
    
    Ok(thread_data.to_string())
}

/// Generate HTML for CPU hotspots display
fn generate_hotspots_html(thread_history: &[crate::performance::ThreadLevelProfiling]) -> String {
    if thread_history.is_empty() {
        return "<p>No hotspot data available</p>".to_string();
    }
    
    let latest = &thread_history[thread_history.len() - 1];
    if latest.hotspot_threads.is_empty() {
        return "<p>✅ No CPU hotspots detected</p>".to_string();
    }
    
    let mut html = String::new();
    for hotspot in &latest.hotspot_threads {
        let severity_class = match hotspot.hotspot_severity {
            crate::performance::HotspotSeverity::Critical => "critical",
            crate::performance::HotspotSeverity::Severe => "severe", 
            crate::performance::HotspotSeverity::Moderate => "moderate",
            crate::performance::HotspotSeverity::Minor => "",
        };
        
        html.push_str(&format!(
            r#"<div class="hotspot {}">
                <strong>{}</strong> - {:.1}% CPU
                <br><small>{}</small>
            </div>"#,
            severity_class,
            hotspot.thread_name,
            hotspot.cpu_percent,
            hotspot.suggested_optimization
        ));
    }
    
    html
}

/// Generate flame graph preview text
fn generate_flame_graph_preview(flame_graph_data: &crate::performance::FlameGraphData) -> String {
    if flame_graph_data.call_stacks.is_empty() {
        return "No flame graph data available".to_string();
    }
    
    let mut preview = String::new();
    for (stack_trace, frame) in flame_graph_data.call_stacks.iter().take(5) {
        preview.push_str(&format!(
            "{} [{} samples, {:.2}ms]\n",
            stack_trace,
            frame.sample_count,
            frame.cpu_time_nanos as f64 / 1_000_000.0
        ));
    }
    
    if flame_graph_data.call_stacks.len() > 5 {
        preview.push_str(&format!("... and {} more call stacks", flame_graph_data.call_stacks.len() - 5));
    }
    
    preview
}

/// Generate HTML for performance recommendations
fn generate_recommendations_html(recommendations: &[String]) -> String {
    if recommendations.is_empty() {
        return "<p>✅ No performance recommendations at this time</p>".to_string();
    }
    
    recommendations.iter()
        .map(|rec| format!("<div class=\"recommendation\">{}</div>", rec))
        .collect::<Vec<_>>()
        .join("\n")
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
    info!("✅ Resource validation available at http://{}/validate", addr);
    info!("🚀 Deployment validation available at http://{}/deploy/validate", addr);
    info!("⚡ Quick deployment check available at http://{}/deploy/check", addr);
    
    // Enhanced CPU Performance Dashboard - Story 6.1
    info!("🔥 CPU Performance Dashboard available at http://{}/dashboard", addr);
    info!("📈 Enhanced Performance Report API at http://{}/api/performance/report", addr);
    info!("🔥 Flame Graph Data API at http://{}/api/performance/flame-graph", addr);
    info!("🧵 Thread Profiling API at http://{}/api/performance/threads", addr);
    
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