use std::collections::HashMap;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, debug};

use crate::api::binance::BinanceKlinesClient;
use crate::api::gate_io::GateIoKlinesClient;
use crate::api::exchange::{Exchange, Ticker24hr, ExchangeInfo};
use crate::api::types::{ApiConfig, ApiError, ApiRequest, ApiResponse};
use crate::historical::structs::FuturesOHLCVCandle;

/// Supported exchange types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExchangeType {
    Binance,
    GateIo,
}

impl std::fmt::Display for ExchangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExchangeType::Binance => write!(f, "binance"),
            ExchangeType::GateIo => write!(f, "gate_io"),
        }
    }
}

/// Exchange health status
#[derive(Debug, Clone)]
pub struct ExchangeHealth {
    pub exchange_type: ExchangeType,
    pub is_healthy: bool,
    pub last_success_time: Option<i64>,
    pub consecutive_failures: u32,
    pub total_requests: u64,
    pub successful_requests: u64,
}

impl ExchangeHealth {
    pub fn new(exchange_type: ExchangeType) -> Self {
        Self {
            exchange_type,
            is_healthy: true,
            last_success_time: None,
            consecutive_failures: 0,
            total_requests: 0,
            successful_requests: 0,
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.successful_requests as f64 / self.total_requests as f64
        }
    }

    pub fn record_success(&mut self) {
        self.is_healthy = true;
        self.last_success_time = Some(chrono::Utc::now().timestamp_millis());
        self.consecutive_failures = 0;
        self.total_requests += 1;
        self.successful_requests += 1;
    }

    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.total_requests += 1;
        
        // Mark as unhealthy if we have too many consecutive failures
        if self.consecutive_failures >= 5 {
            self.is_healthy = false;
        }
    }
}

/// Unified exchange manager for handling multiple cryptocurrency exchanges
pub struct ExchangeManager {
    exchanges: HashMap<ExchangeType, Box<dyn Exchange + Send + Sync>>,
    health_stats: Arc<RwLock<HashMap<ExchangeType, ExchangeHealth>>>,
    primary_exchange: ExchangeType,
    fallback_exchanges: Vec<ExchangeType>,
}

impl ExchangeManager {
    /// Create a new exchange manager with default configuration
    pub async fn new() -> Result<Self, ApiError> {
        let mut exchanges: HashMap<ExchangeType, Box<dyn Exchange + Send + Sync>> = HashMap::new();
        let mut health_stats = HashMap::new();

        // Initialize Binance exchange
        let binance_config = ApiConfig::binance_futures();
        let binance_client = BinanceKlinesClient::from_config(binance_config)
            .map_err(|e| ApiError::Network(format!("Failed to create Binance client: {}", e)))?;
        exchanges.insert(ExchangeType::Binance, Box::new(binance_client));
        health_stats.insert(ExchangeType::Binance, ExchangeHealth::new(ExchangeType::Binance));

        // Initialize Gate.io exchange
        let gate_io_config = ApiConfig::gate_io_futures();
        let gate_io_client = GateIoKlinesClient::from_config(gate_io_config)
            .map_err(|e| ApiError::Network(format!("Failed to create Gate.io client: {}", e)))?;
        exchanges.insert(ExchangeType::GateIo, Box::new(gate_io_client));
        health_stats.insert(ExchangeType::GateIo, ExchangeHealth::new(ExchangeType::GateIo));

        info!("✅ Initialized ExchangeManager with {} exchanges", exchanges.len());

        Ok(Self {
            exchanges,
            health_stats: Arc::new(RwLock::new(health_stats)),
            primary_exchange: ExchangeType::Binance, // Default to Binance as primary
            fallback_exchanges: vec![ExchangeType::GateIo],
        })
    }

    /// Create a new exchange manager with custom configuration
    pub async fn with_config(
        binance_config: Option<ApiConfig>,
        gate_io_config: Option<ApiConfig>,
        primary_exchange: ExchangeType,
    ) -> Result<Self, ApiError> {
        let mut exchanges: HashMap<ExchangeType, Box<dyn Exchange + Send + Sync>> = HashMap::new();
        let mut health_stats = HashMap::new();
        let mut fallback_exchanges = Vec::new();

        // Initialize Binance if config provided
        if let Some(config) = binance_config {
            let binance_client = BinanceKlinesClient::from_config(config)
                .map_err(|e| ApiError::Network(format!("Failed to create Binance client: {}", e)))?;
            exchanges.insert(ExchangeType::Binance, Box::new(binance_client));
            health_stats.insert(ExchangeType::Binance, ExchangeHealth::new(ExchangeType::Binance));
            
            if primary_exchange != ExchangeType::Binance {
                fallback_exchanges.push(ExchangeType::Binance);
            }
        }

        // Initialize Gate.io if config provided
        if let Some(config) = gate_io_config {
            let gate_io_client = GateIoKlinesClient::from_config(config)
                .map_err(|e| ApiError::Network(format!("Failed to create Gate.io client: {}", e)))?;
            exchanges.insert(ExchangeType::GateIo, Box::new(gate_io_client));
            health_stats.insert(ExchangeType::GateIo, ExchangeHealth::new(ExchangeType::GateIo));
            
            if primary_exchange != ExchangeType::GateIo {
                fallback_exchanges.push(ExchangeType::GateIo);
            }
        }

        if exchanges.is_empty() {
            return Err(ApiError::Network("At least one exchange configuration must be provided".to_string()));
        }

        if !exchanges.contains_key(&primary_exchange) {
            return Err(ApiError::Network(format!("Primary exchange {:?} not configured", primary_exchange)));
        }

        info!("✅ Initialized ExchangeManager with {} exchanges, primary: {:?}", 
            exchanges.len(), primary_exchange);

        Ok(Self {
            exchanges,
            health_stats: Arc::new(RwLock::new(health_stats)),
            primary_exchange,
            fallback_exchanges,
        })
    }

    /// Get klines data from the best available exchange  
    pub async fn get_klines(&mut self, request: &ApiRequest) -> Result<ApiResponse<Vec<FuturesOHLCVCandle>>, ApiError> {
        let exchange_order = self.get_exchange_order().await;
        
        for exchange_type in &exchange_order {
            if let Some(exchange) = self.exchanges.get_mut(exchange_type) {
                debug!("Attempting klines request on {} for symbol {}", exchange_type, request.symbol);
                
                let start_time = std::time::Instant::now();
                match exchange.get_klines(request).await {
                    Ok(response) => {
                        let duration = start_time.elapsed();
                        self.record_success_by_ref(exchange_type, duration).await;
                        
                        info!("✅ Got {} klines from {} for {} {} in {:?}", 
                            response.data.len(), exchange_type, request.symbol, request.interval, duration);
                        
                        return Ok(response);
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        self.record_failure_by_ref(exchange_type).await;
                        
                        warn!("❌ Klines request failed on {} after {:?}: {}", exchange_type, duration, e);
                        
                        // Continue to next exchange if this was a recoverable error
                        if e.is_recoverable() {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(ApiError::Network("All exchanges failed for klines request".to_string()))
    }

    /// Get 24hr ticker from the best available exchange
    pub async fn get_ticker_24hr(&mut self, symbol: &str) -> Result<ApiResponse<Ticker24hr>, ApiError> {
        let exchange_order = self.get_exchange_order().await;
        
        for exchange_type in &exchange_order {
            if let Some(exchange) = self.exchanges.get_mut(exchange_type) {
                debug!("Attempting ticker request on {} for symbol {}", exchange_type, symbol);
                
                let start_time = std::time::Instant::now();
                match exchange.get_ticker_24hr(symbol).await {
                    Ok(response) => {
                        let duration = start_time.elapsed();
                        self.record_success_by_ref(exchange_type, duration).await;
                        
                        info!("✅ Got ticker from {} for {} in {:?}", 
                            exchange_type, symbol, duration);
                        
                        return Ok(response);
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        self.record_failure_by_ref(exchange_type).await;
                        
                        warn!("❌ Ticker request failed on {} after {:?}: {}", exchange_type, duration, e);
                        
                        if e.is_recoverable() {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(ApiError::Network("All exchanges failed for ticker request".to_string()))
    }

    /// Get exchange info from the best available exchange
    pub async fn get_exchange_info(&mut self) -> Result<ApiResponse<ExchangeInfo>, ApiError> {
        let exchange_order = self.get_exchange_order().await;
        
        for exchange_type in &exchange_order {
            if let Some(exchange) = self.exchanges.get_mut(exchange_type) {
                debug!("Attempting exchange info request on {}", exchange_type);
                
                let start_time = std::time::Instant::now();
                match exchange.get_exchange_info().await {
                    Ok(response) => {
                        let duration = start_time.elapsed();
                        self.record_success_by_ref(exchange_type, duration).await;
                        
                        info!("✅ Got exchange info from {} with {} symbols in {:?}", 
                            exchange_type, response.data.symbols.len(), duration);
                        
                        return Ok(response);
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        self.record_failure_by_ref(exchange_type).await;
                        
                        warn!("❌ Exchange info request failed on {} after {:?}: {}", exchange_type, duration, e);
                        
                        if e.is_recoverable() {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(ApiError::Network("All exchanges failed for exchange info request".to_string()))
    }

    /// Set rate limit for a specific exchange
    pub async fn set_exchange_rate_limit(&mut self, exchange_type: ExchangeType, min_interval: Duration) -> Result<(), ApiError> {
        if let Some(exchange) = self.exchanges.get_mut(&exchange_type) {
            exchange.set_rate_limit(min_interval).await;
            info!("Updated rate limit for {} to {:?}", exchange_type, min_interval);
            Ok(())
        } else {
            Err(ApiError::Network(format!("Exchange {:?} not configured", exchange_type)))
        }
    }

    /// Set rate limit for all exchanges
    pub async fn set_global_rate_limit(&mut self, min_interval: Duration) -> Result<(), ApiError> {
        for (exchange_type, exchange) in self.exchanges.iter_mut() {
            exchange.set_rate_limit(min_interval).await;
            info!("Updated rate limit for {} to {:?}", exchange_type, min_interval);
        }
        Ok(())
    }

    /// Get health statistics for all exchanges
    pub async fn get_health_stats(&self) -> HashMap<ExchangeType, ExchangeHealth> {
        self.health_stats.read().await.clone()
    }

    /// Get the current primary exchange
    pub fn get_primary_exchange(&self) -> ExchangeType {
        self.primary_exchange.clone()
    }

    /// Set a new primary exchange
    pub async fn set_primary_exchange(&mut self, exchange_type: ExchangeType) -> Result<(), ApiError> {
        if !self.exchanges.contains_key(&exchange_type) {
            return Err(ApiError::Network(format!("Exchange {:?} not configured", exchange_type)));
        }

        // Update fallback list
        self.fallback_exchanges.clear();
        for existing_type in self.exchanges.keys() {
            if *existing_type != exchange_type {
                self.fallback_exchanges.push(existing_type.clone());
            }
        }

        self.primary_exchange = exchange_type.clone();
        info!("✅ Set primary exchange to {:?}", exchange_type);
        Ok(())
    }

    /// Get the exchange order based on health and priority
    async fn get_exchange_order(&self) -> Vec<ExchangeType> {
        let health_stats = self.health_stats.read().await;
        let mut exchange_order = vec![self.primary_exchange.clone()];
        
        // Check if primary exchange is healthy
        if let Some(health) = health_stats.get(&self.primary_exchange) {
            if !health.is_healthy {
                // If primary is unhealthy, sort fallbacks by success rate
                let mut healthy_fallbacks: Vec<_> = self.fallback_exchanges.iter()
                    .filter_map(|ex| {
                        health_stats.get(ex).map(|h| (ex.clone(), h.success_rate()))
                    })
                    .collect();
                
                healthy_fallbacks.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                
                exchange_order = healthy_fallbacks.into_iter()
                    .map(|(ex, _)| ex)
                    .collect();
                
                // Add primary back to end as last resort
                exchange_order.push(self.primary_exchange.clone());
            } else {
                // Primary is healthy, add fallbacks in order
                exchange_order.extend(self.fallback_exchanges.iter().cloned());
            }
        }

        exchange_order
    }

    /// Record a successful operation by reference (zero-copy)
    async fn record_success_by_ref(&self, exchange_type: &ExchangeType, _duration: std::time::Duration) {
        let mut health_stats = self.health_stats.write().await;
        if let Some(health) = health_stats.get_mut(exchange_type) {
            health.record_success();
        }
    }

    /// Record a failed operation by reference (zero-copy)
    async fn record_failure_by_ref(&self, exchange_type: &ExchangeType) {
        let mut health_stats = self.health_stats.write().await;
        if let Some(health) = health_stats.get_mut(exchange_type) {
            health.record_failure();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_exchange_manager_creation() {
        let result = ExchangeManager::new().await;
        assert!(result.is_ok());
        
        let manager = result.unwrap();
        assert_eq!(manager.get_primary_exchange(), ExchangeType::Binance);
        assert_eq!(manager.exchanges.len(), 2);
        assert!(manager.exchanges.contains_key(&ExchangeType::Binance));
        assert!(manager.exchanges.contains_key(&ExchangeType::GateIo));
    }

    #[tokio::test]
    async fn test_custom_configuration() {
        let binance_config = Some(ApiConfig::binance_futures());
        let gate_io_config = Some(ApiConfig::gate_io_futures());
        
        let result = ExchangeManager::with_config(
            binance_config,
            gate_io_config,
            ExchangeType::GateIo,
        ).await;
        
        assert!(result.is_ok());
        let manager = result.unwrap();
        assert_eq!(manager.get_primary_exchange(), ExchangeType::GateIo);
    }

    #[tokio::test]
    async fn test_set_primary_exchange() {
        let mut manager = ExchangeManager::new().await.unwrap();
        
        let result = manager.set_primary_exchange(ExchangeType::GateIo).await;
        assert!(result.is_ok());
        assert_eq!(manager.get_primary_exchange(), ExchangeType::GateIo);
    }

    #[tokio::test]
    async fn test_health_stats() {
        let manager = ExchangeManager::new().await.unwrap();
        let health_stats = manager.get_health_stats().await;
        
        assert_eq!(health_stats.len(), 2);
        assert!(health_stats.contains_key(&ExchangeType::Binance));
        assert!(health_stats.contains_key(&ExchangeType::GateIo));
        
        // All should start healthy
        for (_, health) in health_stats {
            assert!(health.is_healthy);
            assert_eq!(health.total_requests, 0);
            assert_eq!(health.successful_requests, 0);
        }
    }

    #[tokio::test]
    async fn test_rate_limit_setting() {
        let mut manager = ExchangeManager::new().await.unwrap();
        
        let new_interval = Duration::from_millis(200);
        let result = manager.set_exchange_rate_limit(ExchangeType::Binance, new_interval).await;
        assert!(result.is_ok());
        
        let result = manager.set_global_rate_limit(Duration::from_millis(300)).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_exchange_type_display() {
        assert_eq!(ExchangeType::Binance.to_string(), "binance");
        assert_eq!(ExchangeType::GateIo.to_string(), "gate_io");
    }

    #[test]
    fn test_exchange_health() {
        let mut health = ExchangeHealth::new(ExchangeType::Binance);
        
        assert!(health.is_healthy);
        assert_eq!(health.success_rate(), 0.0);
        
        health.record_success();
        assert_eq!(health.success_rate(), 1.0);
        assert_eq!(health.total_requests, 1);
        assert_eq!(health.successful_requests, 1);
        
        health.record_failure();
        assert_eq!(health.success_rate(), 0.5);
        assert_eq!(health.total_requests, 2);
        assert_eq!(health.successful_requests, 1);
        assert_eq!(health.consecutive_failures, 1);
        assert!(health.is_healthy); // Still healthy with only 1 failure
        
        // Make it unhealthy with 5 consecutive failures
        for _ in 0..4 {
            health.record_failure();
        }
        assert!(!health.is_healthy);
        assert_eq!(health.consecutive_failures, 5);
    }
}