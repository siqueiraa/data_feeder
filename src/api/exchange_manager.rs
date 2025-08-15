use std::collections::HashMap;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, debug};

use crate::api::binance::BinanceKlinesClient;
use crate::api::gate_io::{GateIoKlinesClient, GateIoTradingClient};
use crate::api::exchange::{Exchange, TradingExchange, Ticker24hr, ExchangeInfo};
use crate::api::types::{
    ApiConfig, ApiError, ApiRequest, ApiResponse,
    OrderRequest, OrderResponse, ModifyOrderRequest, Order, Position, Balance,
    CrossExchangePrices, ArbitrageOpportunity
};
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
    trading_exchanges: HashMap<ExchangeType, Box<dyn TradingExchange + Send + Sync>>,
    health_stats: Arc<RwLock<HashMap<ExchangeType, ExchangeHealth>>>,
    primary_exchange: ExchangeType,
    fallback_exchanges: Vec<ExchangeType>,
}

impl ExchangeManager {
    /// Create a new exchange manager with default configuration
    pub async fn new() -> Result<Self, ApiError> {
        let mut exchanges: HashMap<ExchangeType, Box<dyn Exchange + Send + Sync>> = HashMap::new();
        let trading_exchanges: HashMap<ExchangeType, Box<dyn TradingExchange + Send + Sync>> = HashMap::new();
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
            trading_exchanges,
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
        let trading_exchanges: HashMap<ExchangeType, Box<dyn TradingExchange + Send + Sync>> = HashMap::new();
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
            trading_exchanges,
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

    /// Add a trading exchange with authentication credentials
    pub async fn add_trading_exchange(&mut self, exchange_type: ExchangeType, api_key: String, api_secret: String) -> Result<(), ApiError> {
        match exchange_type {
            ExchangeType::GateIo => {
                let config = ApiConfig::gate_io_futures();
                let trading_client = GateIoTradingClient::from_config(
                    config,
                    Some(api_key),
                    Some(api_secret)
                ).map_err(|e| ApiError::Network(format!("Failed to create Gate.io trading client: {}", e)))?;
                
                self.trading_exchanges.insert(exchange_type, Box::new(trading_client));
                info!("✅ Added Gate.io trading client to ExchangeManager");
                Ok(())
            }
            ExchangeType::Binance => {
                // TODO: Implement BinanceTradingClient when needed
                Err(ApiError::Network("Binance trading client not yet implemented".to_string()))
            }
        }
    }

    /// Place an order on the best available trading exchange
    pub async fn place_order(&mut self, order_request: &OrderRequest) -> Result<OrderResponse, ApiError> {
        let exchange_order = self.get_exchange_order().await;
        
        for exchange_type in &exchange_order {
            if let Some(trading_exchange) = self.trading_exchanges.get_mut(exchange_type) {
                debug!("Attempting order placement on {} for symbol {}", exchange_type, order_request.symbol);
                
                let start_time = std::time::Instant::now();
                match trading_exchange.place_order(order_request).await {
                    Ok(response) => {
                        let duration = start_time.elapsed();
                        self.record_success_by_ref(exchange_type, duration).await;
                        
                        info!("✅ Successfully placed order on {} for {} {} in {:?}", 
                            exchange_type, order_request.symbol, order_request.quantity, duration);
                        
                        return Ok(response);
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        self.record_failure_by_ref(exchange_type).await;
                        
                        warn!("❌ Order placement failed on {} after {:?}: {}", exchange_type, duration, e);
                        
                        if e.is_recoverable() {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(ApiError::Network("No trading exchanges available for order placement".to_string()))
    }

    /// Cancel an order on the best available trading exchange
    pub async fn cancel_order(&mut self, symbol: &str, order_id: &str) -> Result<OrderResponse, ApiError> {
        let exchange_order = self.get_exchange_order().await;
        
        for exchange_type in &exchange_order {
            if let Some(trading_exchange) = self.trading_exchanges.get_mut(exchange_type) {
                debug!("Attempting order cancellation on {} for order {}", exchange_type, order_id);
                
                let start_time = std::time::Instant::now();
                match trading_exchange.cancel_order(symbol, order_id).await {
                    Ok(response) => {
                        let duration = start_time.elapsed();
                        self.record_success_by_ref(exchange_type, duration).await;
                        
                        info!("✅ Successfully cancelled order {} on {} in {:?}", 
                            order_id, exchange_type, duration);
                        
                        return Ok(response);
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        self.record_failure_by_ref(exchange_type).await;
                        
                        warn!("❌ Order cancellation failed on {} after {:?}: {}", exchange_type, duration, e);
                        
                        if e.is_recoverable() {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(ApiError::Network("No trading exchanges available for order cancellation".to_string()))
    }

    /// Modify an order on the best available trading exchange
    pub async fn modify_order(&mut self, modify_request: &ModifyOrderRequest) -> Result<OrderResponse, ApiError> {
        let exchange_order = self.get_exchange_order().await;
        
        for exchange_type in &exchange_order {
            if let Some(trading_exchange) = self.trading_exchanges.get_mut(exchange_type) {
                debug!("Attempting order modification on {} for order {}", exchange_type, modify_request.order_id);
                
                let start_time = std::time::Instant::now();
                match trading_exchange.modify_order(modify_request).await {
                    Ok(response) => {
                        let duration = start_time.elapsed();
                        self.record_success_by_ref(exchange_type, duration).await;
                        
                        info!("✅ Successfully modified order {} on {} in {:?}", 
                            modify_request.order_id, exchange_type, duration);
                        
                        return Ok(response);
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        self.record_failure_by_ref(exchange_type).await;
                        
                        warn!("❌ Order modification failed on {} after {:?}: {}", exchange_type, duration, e);
                        
                        if e.is_recoverable() {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(ApiError::Network("No trading exchanges available for order modification".to_string()))
    }

    /// Get open orders from the best available trading exchange
    pub async fn get_open_orders(&mut self, symbol: Option<&str>) -> Result<Vec<Order>, ApiError> {
        let exchange_order = self.get_exchange_order().await;
        
        for exchange_type in &exchange_order {
            if let Some(trading_exchange) = self.trading_exchanges.get_mut(exchange_type) {
                debug!("Attempting to get open orders on {}", exchange_type);
                
                let start_time = std::time::Instant::now();
                match trading_exchange.get_open_orders(symbol).await {
                    Ok(orders) => {
                        let duration = start_time.elapsed();
                        self.record_success_by_ref(exchange_type, duration).await;
                        
                        info!("✅ Retrieved {} open orders from {} in {:?}", 
                            orders.len(), exchange_type, duration);
                        
                        return Ok(orders);
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        self.record_failure_by_ref(exchange_type).await;
                        
                        warn!("❌ Get open orders failed on {} after {:?}: {}", exchange_type, duration, e);
                        
                        if e.is_recoverable() {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(ApiError::Network("No trading exchanges available for getting open orders".to_string()))
    }

    /// Get positions from the best available trading exchange
    pub async fn get_positions(&mut self, symbol: Option<&str>) -> Result<Vec<Position>, ApiError> {
        let exchange_order = self.get_exchange_order().await;
        
        for exchange_type in &exchange_order {
            if let Some(trading_exchange) = self.trading_exchanges.get_mut(exchange_type) {
                debug!("Attempting to get positions on {}", exchange_type);
                
                let start_time = std::time::Instant::now();
                match trading_exchange.get_positions(symbol).await {
                    Ok(positions) => {
                        let duration = start_time.elapsed();
                        self.record_success_by_ref(exchange_type, duration).await;
                        
                        info!("✅ Retrieved {} positions from {} in {:?}", 
                            positions.len(), exchange_type, duration);
                        
                        return Ok(positions);
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        self.record_failure_by_ref(exchange_type).await;
                        
                        warn!("❌ Get positions failed on {} after {:?}: {}", exchange_type, duration, e);
                        
                        if e.is_recoverable() {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(ApiError::Network("No trading exchanges available for getting positions".to_string()))
    }

    /// Get account balance from the best available trading exchange
    pub async fn get_balance(&mut self) -> Result<Vec<Balance>, ApiError> {
        let exchange_order = self.get_exchange_order().await;
        
        for exchange_type in &exchange_order {
            if let Some(trading_exchange) = self.trading_exchanges.get_mut(exchange_type) {
                debug!("Attempting to get balance on {}", exchange_type);
                
                let start_time = std::time::Instant::now();
                match trading_exchange.get_balance().await {
                    Ok(balances) => {
                        let duration = start_time.elapsed();
                        self.record_success_by_ref(exchange_type, duration).await;
                        
                        info!("✅ Retrieved {} account balances from {} in {:?}", 
                            balances.len(), exchange_type, duration);
                        
                        return Ok(balances);
                    }
                    Err(e) => {
                        let duration = start_time.elapsed();
                        self.record_failure_by_ref(exchange_type).await;
                        
                        warn!("❌ Get balance failed on {} after {:?}: {}", exchange_type, duration, e);
                        
                        if e.is_recoverable() {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Err(ApiError::Network("No trading exchanges available for getting balance".to_string()))
    }

    /// Get cross-exchange price comparison for a specific symbol
    pub async fn get_cross_exchange_prices(&mut self, symbol: &str) -> Result<CrossExchangePrices, ApiError> {
        let mut cross_prices = CrossExchangePrices::new(symbol.to_string());
        
        // Fetch 24hr ticker from Binance if available
        if self.exchanges.contains_key(&ExchangeType::Binance) {
            if let Ok(binance_ticker) = self.get_ticker_24hr(symbol).await {
                cross_prices = cross_prices.with_binance_price(binance_ticker.data.last_price);
            }
        }
        
        // Fetch 24hr ticker from Gate.io if available
        if self.exchanges.contains_key(&ExchangeType::GateIo) {
            if let Some(gate_io_exchange) = self.exchanges.get_mut(&ExchangeType::GateIo) {
                if let Ok(gate_io_ticker) = gate_io_exchange.get_ticker_24hr(symbol).await {
                    cross_prices = cross_prices.with_gate_io_price(gate_io_ticker.data.last_price);
                }
            }
        }
        
        if !cross_prices.is_complete() {
            return Err(ApiError::Network("Unable to fetch prices from both exchanges".to_string()));
        }
        
        info!("✅ Retrieved cross-exchange prices for {}: Binance={:?}, Gate.io={:?}, Spread={:?}%", 
            symbol, cross_prices.binance_price, cross_prices.gate_io_price, cross_prices.spread_percentage);
        
        Ok(cross_prices)
    }

    /// Detect arbitrage opportunities across exchanges
    pub async fn detect_arbitrage_opportunities(&mut self, symbols: &[String], min_profit_percentage: f64) -> Result<Vec<ArbitrageOpportunity>, ApiError> {
        let mut opportunities = Vec::new();
        
        for symbol in symbols {
            if let Ok(cross_prices) = self.get_cross_exchange_prices(symbol).await {
                if let (Some(binance_price), Some(gate_io_price)) = (cross_prices.binance_price, cross_prices.gate_io_price) {
                    
                    // Check if Gate.io is cheaper (buy from Gate.io, sell on Binance)
                    if binance_price > gate_io_price {
                        let opportunity = ArbitrageOpportunity::new(
                            symbol.clone(),
                            "gate_io".to_string(),
                            "binance".to_string(),
                            gate_io_price,
                            binance_price,
                            0.001, // Min quantity (should be configurable)
                            1.0,   // Max quantity (should be configurable)
                        );
                        
                        if opportunity.is_profitable(min_profit_percentage) {
                            opportunities.push(opportunity);
                        }
                    }
                    
                    // Check if Binance is cheaper (buy from Binance, sell on Gate.io)
                    if gate_io_price > binance_price {
                        let opportunity = ArbitrageOpportunity::new(
                            symbol.clone(),
                            "binance".to_string(),
                            "gate_io".to_string(),
                            binance_price,
                            gate_io_price,
                            0.001, // Min quantity (should be configurable)
                            1.0,   // Max quantity (should be configurable)
                        );
                        
                        if opportunity.is_profitable(min_profit_percentage) {
                            opportunities.push(opportunity);
                        }
                    }
                }
            }
        }
        
        info!("✅ Found {} arbitrage opportunities with min profit {}%", opportunities.len(), min_profit_percentage);
        
        Ok(opportunities)
    }

    /// Get unified market data across all exchanges
    pub async fn get_unified_market_data(&mut self, symbol: &str) -> Result<Vec<ApiResponse<Ticker24hr>>, ApiError> {
        let mut market_data = Vec::new();
        
        for exchange_type in [ExchangeType::Binance, ExchangeType::GateIo] {
            if let Some(exchange) = self.exchanges.get_mut(&exchange_type) {
                match exchange.get_ticker_24hr(symbol).await {
                    Ok(ticker) => market_data.push(ticker),
                    Err(e) => debug!("Failed to get ticker from {:?}: {}", exchange_type, e),
                }
            }
        }
        
        if market_data.is_empty() {
            return Err(ApiError::Network("No market data available from any exchange".to_string()));
        }
        
        info!("✅ Retrieved unified market data for {} from {} exchanges", symbol, market_data.len());
        Ok(market_data)
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
        assert_eq!(manager.trading_exchanges.len(), 0); // No trading exchanges by default
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

    #[tokio::test]
    async fn test_gate_io_trading_integration() {
        let mut manager = ExchangeManager::new().await.unwrap();
        
        // Test that we can add Gate.io trading client
        let result = manager.add_trading_exchange(
            ExchangeType::GateIo,
            "test_key".to_string(),
            "test_secret".to_string(),
        ).await;
        
        // Should succeed even with test credentials (client creation should work)
        assert!(result.is_ok());
        assert_eq!(manager.trading_exchanges.len(), 1);
        assert!(manager.trading_exchanges.contains_key(&ExchangeType::GateIo));
    }

    #[tokio::test]
    async fn test_exchange_routing_with_health_monitoring() {
        let manager = ExchangeManager::new().await.unwrap();
        
        // Initially both exchanges should be healthy
        let health_stats = manager.get_health_stats().await;
        assert!(health_stats.get(&ExchangeType::Binance).unwrap().is_healthy);
        assert!(health_stats.get(&ExchangeType::GateIo).unwrap().is_healthy);
        
        // Test exchange order - should start with primary (Binance)
        let exchange_order = manager.get_exchange_order().await;
        assert_eq!(exchange_order[0], ExchangeType::Binance);
        assert_eq!(exchange_order[1], ExchangeType::GateIo);
    }

    #[tokio::test]
    async fn test_fallback_mechanism() {
        let manager = ExchangeManager::new().await.unwrap();
        
        // Mark primary exchange as unhealthy by simulating failures
        {
            let mut health_stats = manager.health_stats.write().await;
            if let Some(binance_health) = health_stats.get_mut(&ExchangeType::Binance) {
                binance_health.is_healthy = false;
                binance_health.consecutive_failures = 5;
            }
        }
        
        // Exchange order should now prioritize healthy exchanges
        let exchange_order = manager.get_exchange_order().await;
        
        // Gate.io should be first now (as it's healthy), Binance should be last
        assert_eq!(exchange_order[0], ExchangeType::GateIo);
        assert_eq!(exchange_order.last().unwrap(), &ExchangeType::Binance);
    }

    #[tokio::test]
    async fn test_cross_exchange_functionality() {
        let gate_io_config = Some(ApiConfig::gate_io_futures());
        let binance_config = Some(ApiConfig::binance_futures());
        
        // Test that we can create manager with both exchanges
        let result = ExchangeManager::with_config(
            binance_config,
            gate_io_config,
            ExchangeType::GateIo, // Set Gate.io as primary
        ).await;
        
        assert!(result.is_ok());
        let manager = result.unwrap();
        assert_eq!(manager.get_primary_exchange(), ExchangeType::GateIo);
        assert_eq!(manager.exchanges.len(), 2);
        
        // Both exchanges should be configured
        assert!(manager.exchanges.contains_key(&ExchangeType::Binance));
        assert!(manager.exchanges.contains_key(&ExchangeType::GateIo));
    }

    #[tokio::test]
    async fn test_exchange_manager_error_handling() {
        // Test error handling for invalid configurations
        
        // No exchanges configured - should fail
        let result = ExchangeManager::with_config(None, None, ExchangeType::Binance).await;
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("At least one exchange configuration must be provided"));
        }
        
        // Primary exchange not configured - should fail
        let gate_io_config = Some(ApiConfig::gate_io_futures());
        let result = ExchangeManager::with_config(None, gate_io_config, ExchangeType::Binance).await;
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Primary exchange Binance not configured"));
        }
    }

    #[tokio::test]
    async fn test_health_monitoring_updates() {
        let manager = ExchangeManager::new().await.unwrap();
        
        // Test recording success
        manager.record_success_by_ref(&ExchangeType::Binance, Duration::from_millis(100)).await;
        let health_stats = manager.get_health_stats().await;
        let binance_health = health_stats.get(&ExchangeType::Binance).unwrap();
        assert_eq!(binance_health.total_requests, 1);
        assert_eq!(binance_health.successful_requests, 1);
        assert_eq!(binance_health.consecutive_failures, 0);
        assert!(binance_health.is_healthy);
        
        // Test recording failure
        manager.record_failure_by_ref(&ExchangeType::Binance).await;
        let health_stats = manager.get_health_stats().await;
        let binance_health = health_stats.get(&ExchangeType::Binance).unwrap();
        assert_eq!(binance_health.total_requests, 2);
        assert_eq!(binance_health.successful_requests, 1);
        assert_eq!(binance_health.consecutive_failures, 1);
        assert_eq!(binance_health.success_rate(), 0.5);
    }

    #[tokio::test]
    async fn test_rate_limiting_configuration() {
        let mut manager = ExchangeManager::new().await.unwrap();
        
        // Test setting rate limit for specific exchange
        let new_interval = Duration::from_millis(200);
        let result = manager.set_exchange_rate_limit(ExchangeType::GateIo, new_interval).await;
        assert!(result.is_ok());
        
        // Test setting rate limit for non-existent exchange
        let mut manager_with_one_exchange = ExchangeManager::with_config(
            Some(ApiConfig::binance_futures()),
            None,
            ExchangeType::Binance,
        ).await.unwrap();
        
        let result = manager_with_one_exchange.set_exchange_rate_limit(ExchangeType::GateIo, new_interval).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Exchange GateIo not configured"));
    }

    #[tokio::test] 
    async fn test_trading_exchange_integration() {
        let mut manager = ExchangeManager::new().await.unwrap();
        
        // Initially no trading exchanges
        assert_eq!(manager.trading_exchanges.len(), 0);
        
        // Add Gate.io trading exchange  
        let result = manager.add_trading_exchange(
            ExchangeType::GateIo,
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
        ).await;
        assert!(result.is_ok());
        assert_eq!(manager.trading_exchanges.len(), 1);
        
        // Test that Binance trading is not yet implemented
        let result = manager.add_trading_exchange(
            ExchangeType::Binance,
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Binance trading client not yet implemented"));
    }

    #[tokio::test]
    async fn test_cross_exchange_analytics_types() {
        // Test CrossExchangePrices
        let mut cross_prices = CrossExchangePrices::new("BTCUSDT".to_string());
        assert!(!cross_prices.is_complete());
        
        cross_prices = cross_prices.with_binance_price(50000.0);
        assert!(!cross_prices.is_complete());
        
        cross_prices = cross_prices.with_gate_io_price(49800.0);
        assert!(cross_prices.is_complete());
        assert_eq!(cross_prices.spread, Some(200.0));
        assert_eq!(cross_prices.spread_percentage, Some((200.0 / 49800.0) * 100.0));
    }

    #[tokio::test]
    async fn test_arbitrage_opportunity() {
        let opportunity = ArbitrageOpportunity::new(
            "BTCUSDT".to_string(),
            "gate_io".to_string(),
            "binance".to_string(),
            49800.0,
            50000.0,
            0.001,
            1.0,
        );
        
        assert_eq!(opportunity.price_difference, 200.0);
        assert_eq!(opportunity.percentage_profit, (200.0 / 49800.0) * 100.0);
        assert!(opportunity.is_profitable(0.1)); // 0.1% minimum profit
        assert!(!opportunity.is_profitable(1.0)); // 1% minimum profit (too high)
    }

    #[tokio::test]
    async fn test_cross_exchange_functionality_comprehensive() {
        let mut manager = ExchangeManager::new().await.unwrap();
        
        // Test that cross-exchange methods exist and have correct signatures
        let symbols = vec!["BTCUSDT".to_string()];
        
        // These will fail in unit tests without real API endpoints, but test method availability
        let _cross_prices_result = manager.get_cross_exchange_prices("BTCUSDT").await;
        let _arbitrage_result = manager.detect_arbitrage_opportunities(&symbols, 0.1).await;
        let _market_data_result = manager.get_unified_market_data("BTCUSDT").await;
        
        // The important thing is that these methods compile and are callable
        // Integration tests will validate the actual functionality
    }

    #[tokio::test]
    async fn test_exchange_manager_comprehensive_integration() {
        // Test the complete integration workflow
        let mut manager = ExchangeManager::new().await.unwrap();
        
        // Verify both data and trading capabilities are available
        assert_eq!(manager.exchanges.len(), 2);
        assert_eq!(manager.trading_exchanges.len(), 0);
        
        // Add Gate.io trading capabilities
        let result = manager.add_trading_exchange(
            ExchangeType::GateIo,
            "test_key".to_string(),
            "test_secret".to_string(),
        ).await;
        assert!(result.is_ok());
        assert_eq!(manager.trading_exchanges.len(), 1);
        
        // Verify health monitoring is working
        let health_stats = manager.get_health_stats().await;
        assert_eq!(health_stats.len(), 2);
        for (exchange_type, health) in health_stats {
            match exchange_type {
                ExchangeType::Binance | ExchangeType::GateIo => {
                    assert!(health.is_healthy);
                    assert_eq!(health.total_requests, 0);
                }
            }
        }
        
        // Test that we have both market data and trading capabilities
        assert!(manager.exchanges.contains_key(&ExchangeType::Binance));
        assert!(manager.exchanges.contains_key(&ExchangeType::GateIo));
        assert!(manager.trading_exchanges.contains_key(&ExchangeType::GateIo));
    }
}