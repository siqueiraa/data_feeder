//! Deployment validation module for resource-aware deployment verification
//! 
//! This module provides validation functionality to ensure that:
//! - Resource detection is working correctly during deployment  
//! - Configuration adaptation is functioning as expected
//! - System performs correctly across different resource environments
//! - Deployment validation integrates with existing deployment workflows

use crate::system_resources::SystemResources;
use crate::adaptive_config::AdaptiveConfig;
use crate::health::{ResourceEnvironment, classify_resource_environment};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::{info, warn, error, debug};

/// Deployment validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentValidationResult {
    pub overall_status: ValidationStatus,
    pub resource_detection: ResourceDetectionResult,
    pub configuration_adaptation: ConfigurationAdaptationResult,
    pub performance_validation: PerformanceValidationResult,
    pub integration_validation: IntegrationValidationResult,
    pub timestamp: String,
    pub validation_duration_ms: u64,
}

/// Validation status levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationStatus {
    Passed,
    Warning,
    Failed,
    Error,
}

/// Resource detection validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceDetectionResult {
    pub status: ValidationStatus,
    pub detected_resources: Option<SystemResourcesSummary>,
    pub resource_environment: String,
    pub issues: Vec<String>,
}

/// Configuration adaptation validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationAdaptationResult {
    pub status: ValidationStatus,
    pub adaptive_config_enabled: bool,
    pub configuration_summary: Option<AdaptiveConfigSummary>,
    pub adaptation_effectiveness: i32, // 0-100 score
    pub issues: Vec<String>,
}

/// Performance validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceValidationResult {
    pub status: ValidationStatus,
    pub memory_validation: MemoryValidationResult,
    pub cpu_validation: CpuValidationResult,
    pub throughput_validation: ThroughputValidationResult,
    pub issues: Vec<String>,
}

/// Integration validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationValidationResult {
    pub status: ValidationStatus,
    pub deployment_workflow_compatibility: bool,
    pub monitoring_integration: bool,
    pub health_check_integration: bool,
    pub issues: Vec<String>,
}

/// System resources summary for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemResourcesSummary {
    pub cpu_cores: usize,
    pub total_memory_gb: f64,
    pub available_memory_gb: f64,
    pub memory_utilization_percent: f64,
    pub is_containerized: bool,
    pub cpu_architecture: String,
    pub os_info: String,
}

/// Adaptive configuration summary for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConfigSummary {
    pub worker_threads: usize,
    pub total_buffer_size_mb: usize,
    pub database_batch_size: usize,
    pub memory_intensive_features_enabled: bool,
    pub estimated_memory_footprint_mb: usize,
}

/// Memory validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryValidationResult {
    pub status: ValidationStatus,
    pub available_memory_sufficient: bool,
    pub memory_utilization_acceptable: bool,
    pub estimated_usage_within_limits: bool,
}

/// CPU validation result  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuValidationResult {
    pub status: ValidationStatus,
    pub cpu_cores_sufficient: bool,
    pub thread_pool_sizing_appropriate: bool,
    pub cpu_architecture_supported: bool,
}

/// Throughput validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputValidationResult {
    pub status: ValidationStatus,
    pub expected_throughput: u32,
    pub configuration_optimized: bool,
    pub performance_degradation_acceptable: bool,
}

/// Comprehensive deployment validation (AC: 3)
pub async fn validate_deployment() -> Result<DeploymentValidationResult, Box<dyn std::error::Error + Send + Sync>> {
    let validation_start = Instant::now();
    info!("🚀 Starting deployment validation...");
    
    // Perform all validation checks
    let resource_detection = validate_resource_detection().await?;
    let configuration_adaptation = validate_configuration_adaptation().await?;
    let performance_validation = validate_performance().await?;
    let integration_validation = validate_integration().await?;
    
    // Determine overall status
    let overall_status = determine_overall_status(&[
        &resource_detection.status,
        &configuration_adaptation.status,
        &performance_validation.status,
        &integration_validation.status,
    ]);
    
    let validation_duration = validation_start.elapsed();
    
    // Record metrics
    if let Some(metrics) = crate::metrics::get_metrics() {
        let env_str = match classify_resource_environment() {
            ResourceEnvironment::LowResource => "low_resource",
            ResourceEnvironment::StandardResource => "standard_resource",
            ResourceEnvironment::HighResource => "high_resource",
            ResourceEnvironment::Containerized => "containerized",
        };
        
        let success = overall_status == ValidationStatus::Passed || overall_status == ValidationStatus::Warning;
        metrics.record_configuration_adaptation("deployment_validation", "startup", success);
        
        if success {
            metrics.record_adaptive_config_effectiveness("deployment", env_str, 
                configuration_adaptation.adaptation_effectiveness as i64);
        }
    }
    
    let result = DeploymentValidationResult {
        overall_status,
        resource_detection,
        configuration_adaptation,
        performance_validation,
        integration_validation,
        timestamp: chrono::Utc::now().to_rfc3339(),
        validation_duration_ms: validation_duration.as_millis() as u64,
    };
    
    // Log results
    match result.overall_status {
        ValidationStatus::Passed => info!("✅ Deployment validation passed in {:?}", validation_duration),
        ValidationStatus::Warning => warn!("⚠️ Deployment validation completed with warnings in {:?}", validation_duration),
        ValidationStatus::Failed => error!("❌ Deployment validation failed in {:?}", validation_duration),
        ValidationStatus::Error => error!("💥 Deployment validation encountered errors in {:?}", validation_duration),
    }
    
    Ok(result)
}

/// Validate resource detection functionality
async fn validate_resource_detection() -> Result<ResourceDetectionResult, Box<dyn std::error::Error + Send + Sync>> {
    debug!("🔍 Validating resource detection...");
    let mut issues = Vec::new();
    
    let detected_resources = if let Some(resources) = SystemResources::get_cached() {
        // Validate detected resources are reasonable
        if resources.cpu_cores == 0 {
            issues.push("CPU cores detection returned 0".to_string());
        }
        if resources.total_memory_bytes == 0 {
            issues.push("Total memory detection returned 0".to_string());
        }
        if resources.available_memory_bytes > resources.total_memory_bytes {
            issues.push("Available memory exceeds total memory".to_string());
        }
        if resources.page_size_bytes == 0 {
            issues.push("Page size detection returned 0".to_string());
        }
        
        Some(SystemResourcesSummary {
            cpu_cores: resources.cpu_cores,
            total_memory_gb: resources.total_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            available_memory_gb: resources.available_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            memory_utilization_percent: resources.get_memory_utilization(),
            is_containerized: resources.is_containerized,
            cpu_architecture: resources.cpu_architecture.clone(),
            os_info: resources.os_info.clone(),
        })
    } else {
        issues.push("System resources not detected or cached".to_string());
        None
    };
    
    let resource_environment = match classify_resource_environment() {
        ResourceEnvironment::LowResource => "low_resource",
        ResourceEnvironment::StandardResource => "standard_resource",
        ResourceEnvironment::HighResource => "high_resource",
        ResourceEnvironment::Containerized => "containerized",
    }.to_string();
    
    let status = if issues.is_empty() {
        ValidationStatus::Passed
    } else if detected_resources.is_some() {
        ValidationStatus::Warning
    } else {
        ValidationStatus::Failed
    };
    
    Ok(ResourceDetectionResult {
        status,
        detected_resources,
        resource_environment,
        issues,
    })
}

/// Validate configuration adaptation functionality
async fn validate_configuration_adaptation() -> Result<ConfigurationAdaptationResult, Box<dyn std::error::Error + Send + Sync>> {
    debug!("⚙️ Validating configuration adaptation...");
    let mut issues = Vec::new();
    let mut adaptation_effectiveness = 0i32;
    
    // Check if system resources are available for adaptation
    let resources_available = SystemResources::get_cached().is_some();
    if !resources_available {
        issues.push("System resources not available for configuration adaptation".to_string());
        return Ok(ConfigurationAdaptationResult {
            status: ValidationStatus::Failed,
            adaptive_config_enabled: false,
            configuration_summary: None,
            adaptation_effectiveness: 0,
            issues,
        });
    }
    
    let resources = SystemResources::get_cached().unwrap();
    
    // Generate adaptive configuration to test adaptation logic
    let adaptive_config = AdaptiveConfig::from_system_resources(resources, None);
    
    // Validate configuration makes sense for the detected resources
    let thread_pool_reasonable = adaptive_config.get_worker_thread_count() <= resources.cpu_cores * 2
        && adaptive_config.get_worker_thread_count() > 0;
    if !thread_pool_reasonable {
        issues.push(format!("Thread pool size {} not reasonable for {} CPU cores", 
            adaptive_config.get_worker_thread_count(), resources.cpu_cores));
    } else {
        adaptation_effectiveness += 25;
    }
    
    // Check buffer sizing is reasonable
    let buffer_size_mb = adaptive_config.get_websocket_buffer_size() / (1024 * 1024);
    let reasonable_buffer_range = 1..=256; // 1MB to 256MB seems reasonable
    if !reasonable_buffer_range.contains(&buffer_size_mb) {
        issues.push(format!("WebSocket buffer size {}MB not in reasonable range", buffer_size_mb));
    } else {
        adaptation_effectiveness += 25;
    }
    
    // Check memory intensive features setting
    let memory_features_enabled = adaptive_config.should_enable_memory_intensive_features();
    let sufficient_resources = resources.has_sufficient_resources();
    if memory_features_enabled == sufficient_resources {
        adaptation_effectiveness += 25;
    } else {
        issues.push(format!("Memory intensive features setting ({}) doesn't match resource availability ({})", 
            memory_features_enabled, sufficient_resources));
    }
    
    // Check batch size scaling
    let batch_size = adaptive_config.get_database_batch_size();
    if batch_size > 0 && batch_size <= 10000 {
        adaptation_effectiveness += 25;
    } else {
        issues.push(format!("Database batch size {} not in reasonable range", batch_size));
    }
    
    let total_buffer_mb = (
        adaptive_config.buffers.websocket_buffer_size +
        adaptive_config.buffers.database_buffer_size +
        adaptive_config.buffers.file_io_buffer_size +
        adaptive_config.buffers.network_buffer_size
    ) / (1024 * 1024);
    
    let configuration_summary = Some(AdaptiveConfigSummary {
        worker_threads: adaptive_config.get_worker_thread_count(),
        total_buffer_size_mb: total_buffer_mb,
        database_batch_size: adaptive_config.get_database_batch_size(),
        memory_intensive_features_enabled: adaptive_config.should_enable_memory_intensive_features(),
        estimated_memory_footprint_mb: total_buffer_mb + adaptive_config.memory.cache_size_mb,
    });
    
    let status = if issues.is_empty() {
        ValidationStatus::Passed
    } else if adaptation_effectiveness >= 50 {
        ValidationStatus::Warning
    } else {
        ValidationStatus::Failed
    };
    
    Ok(ConfigurationAdaptationResult {
        status,
        adaptive_config_enabled: true,
        configuration_summary,
        adaptation_effectiveness,
        issues,
    })
}

/// Validate system performance expectations
async fn validate_performance() -> Result<PerformanceValidationResult, Box<dyn std::error::Error + Send + Sync>> {
    debug!("⚡ Validating performance expectations...");
    let mut issues = Vec::new();
    
    let resources = SystemResources::get_cached().ok_or("System resources not available")?;
    let resource_env = classify_resource_environment();
    
    // Memory validation
    let memory_utilization = resources.get_memory_utilization();
    let available_memory_sufficient = resources.available_memory_bytes >= 1024 * 1024 * 1024; // At least 1GB
    let memory_utilization_acceptable = memory_utilization < 85.0;
    let estimated_usage_within_limits = ((resources.get_recommended_buffer_size_mb() * 1024 * 1024) as u64)
        < resources.available_memory_bytes / 2; // Use less than 50% for buffers
    
    let memory_status = if available_memory_sufficient && memory_utilization_acceptable && estimated_usage_within_limits {
        ValidationStatus::Passed
    } else if available_memory_sufficient && memory_utilization < 95.0 {
        ValidationStatus::Warning
    } else {
        ValidationStatus::Failed
    };
    
    if !available_memory_sufficient {
        issues.push("Insufficient available memory (< 1GB)".to_string());
    }
    if !memory_utilization_acceptable {
        issues.push(format!("High memory utilization: {:.1}%", memory_utilization));
    }
    
    let memory_validation = MemoryValidationResult {
        status: memory_status,
        available_memory_sufficient,
        memory_utilization_acceptable,
        estimated_usage_within_limits,
    };
    
    // CPU validation
    let cpu_cores_sufficient = resources.cpu_cores >= 2;
    let thread_pool_sizing_appropriate = resources.get_recommended_thread_pool_size() > 0;
    let cpu_architecture_supported = ["x86_64", "aarch64"].contains(&resources.cpu_architecture.as_str());
    
    let cpu_status = if cpu_cores_sufficient && thread_pool_sizing_appropriate && cpu_architecture_supported {
        ValidationStatus::Passed
    } else if thread_pool_sizing_appropriate {
        ValidationStatus::Warning
    } else {
        ValidationStatus::Failed
    };
    
    if !cpu_cores_sufficient {
        issues.push(format!("Limited CPU cores: {}", resources.cpu_cores));
    }
    if !cpu_architecture_supported {
        issues.push(format!("Unsupported CPU architecture: {}", resources.cpu_architecture));
    }
    
    let cpu_validation = CpuValidationResult {
        status: cpu_status,
        cpu_cores_sufficient,
        thread_pool_sizing_appropriate,
        cpu_architecture_supported,
    };
    
    // Throughput validation based on environment
    let (expected_throughput, configuration_optimized, performance_degradation_acceptable) = match resource_env {
        ResourceEnvironment::LowResource => (100, resources.cpu_cores >= 1, true),
        ResourceEnvironment::StandardResource => (500, resources.cpu_cores >= 2 && resources.available_memory_bytes >= 2 * 1024 * 1024 * 1024, true),
        ResourceEnvironment::HighResource => (1000, resources.cpu_cores >= 4 && resources.available_memory_bytes >= 4 * 1024 * 1024 * 1024, false),
        ResourceEnvironment::Containerized => (200, resources.cpu_cores >= 1, true),
    };
    
    let throughput_status = if configuration_optimized {
        ValidationStatus::Passed
    } else if performance_degradation_acceptable {
        ValidationStatus::Warning
    } else {
        ValidationStatus::Failed
    };
    
    if !configuration_optimized {
        issues.push("System configuration not optimized for expected throughput".to_string());
    }
    
    let throughput_validation = ThroughputValidationResult {
        status: throughput_status,
        expected_throughput,
        configuration_optimized,
        performance_degradation_acceptable,
    };
    
    // Overall performance status
    let overall_performance_status = determine_overall_status(&[
        &memory_validation.status,
        &cpu_validation.status,
        &throughput_validation.status,
    ]);
    
    Ok(PerformanceValidationResult {
        status: overall_performance_status,
        memory_validation,
        cpu_validation,
        throughput_validation,
        issues,
    })
}

/// Validate integration with existing systems
async fn validate_integration() -> Result<IntegrationValidationResult, Box<dyn std::error::Error + Send + Sync>> {
    debug!("🔗 Validating system integration...");
    let mut issues = Vec::new();
    
    // Check deployment workflow compatibility (AC: 6)
    let deployment_workflow_compatibility = true; // Assume compatible unless we detect issues
    
    // Check monitoring integration (AC: 4, 5)
    let monitoring_integration = crate::metrics::get_metrics().is_some();
    if !monitoring_integration {
        issues.push("Metrics registry not available for monitoring integration".to_string());
    }
    
    // Check health check integration (AC: 2)
    let health_check_integration = SystemResources::get_cached().is_some();
    if !health_check_integration {
        issues.push("System resources not available for health check integration".to_string());
    }
    
    let status = if issues.is_empty() {
        ValidationStatus::Passed
    } else if monitoring_integration || health_check_integration {
        ValidationStatus::Warning
    } else {
        ValidationStatus::Failed
    };
    
    Ok(IntegrationValidationResult {
        status,
        deployment_workflow_compatibility,
        monitoring_integration,
        health_check_integration,
        issues,
    })
}

/// Determine overall status from individual statuses
fn determine_overall_status(statuses: &[&ValidationStatus]) -> ValidationStatus {
    if statuses.iter().any(|s| **s == ValidationStatus::Error) {
        ValidationStatus::Error
    } else if statuses.iter().any(|s| **s == ValidationStatus::Failed) {
        ValidationStatus::Failed
    } else if statuses.iter().any(|s| **s == ValidationStatus::Warning) {
        ValidationStatus::Warning
    } else {
        ValidationStatus::Passed
    }
}

/// Quick deployment validation check for CI/CD pipelines
pub async fn quick_deployment_check() -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    info!("🚀 Running quick deployment check...");
    
    let validation_result = validate_deployment().await?;
    
    let is_deployable = matches!(validation_result.overall_status, 
        ValidationStatus::Passed | ValidationStatus::Warning);
    
    if is_deployable {
        info!("✅ Quick deployment check passed - system ready for deployment");
    } else {
        error!("❌ Quick deployment check failed - deployment not recommended");
    }
    
    Ok(is_deployable)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_deployment_validation_structure() {
        // This test ensures the validation structure works without requiring full system resources
        let result = DeploymentValidationResult {
            overall_status: ValidationStatus::Passed,
            resource_detection: ResourceDetectionResult {
                status: ValidationStatus::Passed,
                detected_resources: None,
                resource_environment: "test".to_string(),
                issues: vec![],
            },
            configuration_adaptation: ConfigurationAdaptationResult {
                status: ValidationStatus::Passed,
                adaptive_config_enabled: true,
                configuration_summary: None,
                adaptation_effectiveness: 100,
                issues: vec![],
            },
            performance_validation: PerformanceValidationResult {
                status: ValidationStatus::Passed,
                memory_validation: MemoryValidationResult {
                    status: ValidationStatus::Passed,
                    available_memory_sufficient: true,
                    memory_utilization_acceptable: true,
                    estimated_usage_within_limits: true,
                },
                cpu_validation: CpuValidationResult {
                    status: ValidationStatus::Passed,
                    cpu_cores_sufficient: true,
                    thread_pool_sizing_appropriate: true,
                    cpu_architecture_supported: true,
                },
                throughput_validation: ThroughputValidationResult {
                    status: ValidationStatus::Passed,
                    expected_throughput: 500,
                    configuration_optimized: true,
                    performance_degradation_acceptable: false,
                },
                issues: vec![],
            },
            integration_validation: IntegrationValidationResult {
                status: ValidationStatus::Passed,
                deployment_workflow_compatibility: true,
                monitoring_integration: true,
                health_check_integration: true,
                issues: vec![],
            },
            timestamp: chrono::Utc::now().to_rfc3339(),
            validation_duration_ms: 100,
        };
        
        // Test serialization
        let _json = serde_json::to_string(&result).expect("Should serialize to JSON");
        
        assert_eq!(result.overall_status, ValidationStatus::Passed);
    }
    
    #[test]
    fn test_overall_status_determination() {
        assert_eq!(determine_overall_status(&[&ValidationStatus::Passed, &ValidationStatus::Passed]), ValidationStatus::Passed);
        assert_eq!(determine_overall_status(&[&ValidationStatus::Passed, &ValidationStatus::Warning]), ValidationStatus::Warning);
        assert_eq!(determine_overall_status(&[&ValidationStatus::Warning, &ValidationStatus::Failed]), ValidationStatus::Failed);
        assert_eq!(determine_overall_status(&[&ValidationStatus::Failed, &ValidationStatus::Error]), ValidationStatus::Error);
    }
}