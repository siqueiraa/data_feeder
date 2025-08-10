//! Async Batching System for Technical Analysis Results
//! 
//! This module implements intelligent batching of technical analysis calculations
//! to reduce per-operation overhead and achieve <1ms output generation.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::time::sleep;
use crate::technical_analysis::rkyv_serialization::{TechnicalAnalysisSerializer, TechnicalAnalysisError};

/// Batch request for technical analysis computation
#[derive(Debug)]
pub struct BatchRequest {
    pub id: String,
    pub symbol: String,
    pub indicator_type: IndicatorType,
    pub parameters: IndicatorParameters,
    pub response_sender: oneshot::Sender<Result<String, TechnicalAnalysisError>>,
    pub created_at: Instant,
}

/// Technical analysis indicator types
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum IndicatorType {
    VolumeWeightedAveragePrice,
    RelativeStrengthIndex,
    MovingAverageConvergenceDivergence,
    BollingerBands,
    StochasticOscillator,
    VolumeProfile,
}

/// Parameters for technical analysis indicators
#[derive(Debug, Clone)]
pub struct IndicatorParameters {
    pub period: Option<usize>,
    pub fast_period: Option<usize>,
    pub slow_period: Option<usize>,
    pub signal_period: Option<usize>,
    pub standard_deviations: Option<f64>,
    pub price_type: PriceType,
}

/// Price type for indicator calculations
#[derive(Debug, Clone)]
pub enum PriceType {
    Close,
    High,
    Low,
    Open,
    TypicalPrice, // (H+L+C)/3
    WeightedClose, // (H+L+C+C)/4
}

impl Default for IndicatorParameters {
    fn default() -> Self {
        Self {
            period: Some(14),
            fast_period: None,
            slow_period: None,
            signal_period: None,
            standard_deviations: Some(2.0),
            price_type: PriceType::Close,
        }
    }
}

/// Batch processing configuration
#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_batch_size: usize,
    pub batch_timeout_ms: u64,
    pub max_queue_size: usize,
    pub worker_threads: usize,
    pub aggregation_enabled: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,      // Process up to 100 requests per batch
            batch_timeout_ms: 10,     // Maximum 10ms batching delay
            max_queue_size: 10000,    // Support 10k queued requests
            worker_threads: 4,        // 4 worker threads for parallel processing
            aggregation_enabled: true, // Enable intelligent aggregation
        }
    }
}

/// Batch processing metrics
#[derive(Debug, Clone)]
pub struct BatchMetrics {
    pub requests_processed: u64,
    pub batches_processed: u64,
    pub average_batch_size: f64,
    pub average_processing_time_ms: f64,
    pub queue_length: usize,
    pub aggregation_ratio: f64, // Percentage of requests aggregated vs processed individually
}

/// Async batching system for technical analysis
pub struct AsyncBatchingSystem {
    config: BatchConfig,
    request_queue: Arc<Mutex<VecDeque<BatchRequest>>>,
    serializer: Arc<Mutex<TechnicalAnalysisSerializer>>,
    metrics: Arc<RwLock<BatchMetrics>>,
    aggregated_cache: Arc<RwLock<HashMap<BatchKey, (String, Instant)>>>,
    shutdown_signal: Arc<Mutex<bool>>,
}

/// Key for aggregating similar requests
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct BatchKey {
    symbol: String,
    indicator_type: IndicatorType,
    parameters_hash: u64,
}

impl AsyncBatchingSystem {
    /// Create new async batching system
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            request_queue: Arc::new(Mutex::new(VecDeque::new())),
            serializer: Arc::new(Mutex::new(TechnicalAnalysisSerializer::new())),
            metrics: Arc::new(RwLock::new(BatchMetrics {
                requests_processed: 0,
                batches_processed: 0,
                average_batch_size: 0.0,
                average_processing_time_ms: 0.0,
                queue_length: 0,
                aggregation_ratio: 0.0,
            })),
            aggregated_cache: Arc::new(RwLock::new(HashMap::new())),
            shutdown_signal: Arc::new(Mutex::new(false)),
        }
    }

    /// Start the batching system with background workers
    pub async fn start(&self) {
        for _ in 0..self.config.worker_threads {
            self.spawn_worker().await;
        }

        // Spawn cache cleanup worker
        self.spawn_cache_cleanup().await;
    }

    /// Submit request for batched processing
    pub async fn submit_request(
        &self,
        id: String,
        symbol: String,
        indicator_type: IndicatorType,
        parameters: IndicatorParameters,
    ) -> Result<String, TechnicalAnalysisError> {
        // Check if we have a cached aggregated result
        if self.config.aggregation_enabled {
            let batch_key = Self::create_batch_key(&symbol, &indicator_type, &parameters);
            if let Some(cached) = self.get_aggregated_result(&batch_key).await {
                self.update_metrics_aggregation().await;
                return Ok(cached);
            }
        }

        // Create oneshot channel for response
        let (tx, rx) = oneshot::channel();

        let request = BatchRequest {
            id,
            symbol,
            indicator_type,
            parameters,
            response_sender: tx,
            created_at: Instant::now(),
        };

        // Add to queue
        {
            let mut queue = self.request_queue.lock().await;
            if queue.len() >= self.config.max_queue_size {
                return Err(TechnicalAnalysisError::QueueFull);
            }
            queue.push_back(request);
        }

        // Wait for result
        rx.await
            .map_err(|_| TechnicalAnalysisError::ProcessingError("Request cancelled".to_string()))?
    }

    /// Spawn worker thread for batch processing
    async fn spawn_worker(&self) {
        let request_queue = self.request_queue.clone();
        let serializer = self.serializer.clone();
        let metrics = self.metrics.clone();
        let aggregated_cache = self.aggregated_cache.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            loop {
                // Check shutdown signal
                {
                    let shutdown = shutdown_signal.lock().await;
                    if *shutdown {
                        break;
                    }
                }

                // Collect batch of requests
                let batch = Self::collect_batch(&request_queue, &config).await;
                if batch.is_empty() {
                    sleep(Duration::from_millis(1)).await;
                    continue;
                }

                // Process batch
                let start_time = Instant::now();
                let batch_size = batch.len();

                Self::process_batch(
                    batch,
                    &serializer,
                    &aggregated_cache,
                    &config,
                ).await;

                // Update metrics
                let processing_time_ms = start_time.elapsed().as_millis() as f64;
                Self::update_batch_metrics(&metrics, batch_size, processing_time_ms).await;
            }
        });
    }

    /// Collect batch of requests from queue
    async fn collect_batch(
        request_queue: &Arc<Mutex<VecDeque<BatchRequest>>>,
        config: &BatchConfig,
    ) -> Vec<BatchRequest> {
        let mut batch = Vec::new();
        let batch_start = Instant::now();

        loop {
            let mut queue = request_queue.lock().await;
            
            // Collect requests up to batch size or timeout
            while batch.len() < config.max_batch_size {
                if let Some(request) = queue.pop_front() {
                    batch.push(request);
                } else {
                    break;
                }
            }

            drop(queue);

            // Stop if we have enough requests or timeout reached
            if batch.len() >= config.max_batch_size ||
               batch_start.elapsed() >= Duration::from_millis(config.batch_timeout_ms) {
                break;
            }

            // Short sleep to allow more requests to accumulate
            if !batch.is_empty() {
                sleep(Duration::from_millis(1)).await;
            } else {
                break;
            }
        }

        batch
    }

    /// Process a batch of requests
    async fn process_batch(
        batch: Vec<BatchRequest>,
        serializer: &Arc<Mutex<TechnicalAnalysisSerializer>>,
        aggregated_cache: &Arc<RwLock<HashMap<BatchKey, (String, Instant)>>>,
        config: &BatchConfig,
    ) {
        // Group requests by similarity for aggregation
        let mut groups = HashMap::new();
        for request in batch {
            if config.aggregation_enabled {
                let batch_key = Self::create_batch_key_from_request(&request);
                groups.entry(batch_key).or_insert_with(Vec::new).push(request);
            } else {
                // Process individually
                Self::process_single_request(request, serializer).await;
            }
        }

        // Process aggregated groups
        for (batch_key, requests) in groups {
            if requests.len() == 1 {
                // Single request, process normally
                Self::process_single_request(requests.into_iter().next().unwrap(), serializer).await;
            } else {
                // Multiple similar requests, compute once and share result
                Self::process_aggregated_requests(requests, batch_key, serializer, aggregated_cache).await;
            }
        }
    }

    /// Process single request
    async fn process_single_request(
        request: BatchRequest,
        serializer: &Arc<Mutex<TechnicalAnalysisSerializer>>,
    ) {
        let result = {
            let mut ser = serializer.lock().await;
            // Simulate technical analysis computation and serialization
            ser.serialize_indicator_result(
                &request.symbol,
                &request.indicator_type,
                &request.parameters,
            ).await
        };

        let _ = request.response_sender.send(result);
    }

    /// Process aggregated requests (compute once, share result)
    async fn process_aggregated_requests(
        requests: Vec<BatchRequest>,
        batch_key: BatchKey,
        serializer: &Arc<Mutex<TechnicalAnalysisSerializer>>,
        aggregated_cache: &Arc<RwLock<HashMap<BatchKey, (String, Instant)>>>,
    ) {
        // Use the first request as representative
        let representative = &requests[0];
        
        let result = {
            let mut ser = serializer.lock().await;
            ser.serialize_indicator_result(
                &representative.symbol,
                &representative.indicator_type,
                &representative.parameters,
            ).await
        };

        // Cache result for future aggregation
        if let Ok(ref data) = result {
            let mut cache = aggregated_cache.write().await;
            cache.insert(batch_key, (data.clone(), Instant::now()));
        }

        // Send result to all requesters
        for request in requests {
            let response = match &result {
                Ok(data) => Ok(data.clone()),
                Err(err) => Err(err.clone()),
            };
            let _ = request.response_sender.send(response);
        }
    }

    /// Get cached aggregated result
    async fn get_aggregated_result(&self, batch_key: &BatchKey) -> Option<String> {
        let cache = self.aggregated_cache.read().await;
        if let Some((data, created_at)) = cache.get(batch_key) {
            // Check if cache entry is still fresh (5 seconds)
            if created_at.elapsed() < Duration::from_secs(5) {
                return Some(data.clone());
            }
        }
        None
    }

    /// Create batch key from request
    fn create_batch_key_from_request(request: &BatchRequest) -> BatchKey {
        Self::create_batch_key(&request.symbol, &request.indicator_type, &request.parameters)
    }

    /// Create batch key for aggregation
    fn create_batch_key(
        symbol: &str,
        indicator_type: &IndicatorType,
        parameters: &IndicatorParameters,
    ) -> BatchKey {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        parameters.period.hash(&mut hasher);
        parameters.fast_period.hash(&mut hasher);
        parameters.slow_period.hash(&mut hasher);
        parameters.signal_period.hash(&mut hasher);
        parameters.standard_deviations.map(|f| f.to_bits()).hash(&mut hasher);

        BatchKey {
            symbol: symbol.to_string(),
            indicator_type: indicator_type.clone(),
            parameters_hash: hasher.finish(),
        }
    }

    /// Spawn cache cleanup worker
    async fn spawn_cache_cleanup(&self) {
        let aggregated_cache = self.aggregated_cache.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        tokio::spawn(async move {
            loop {
                // Check shutdown signal
                {
                    let shutdown = shutdown_signal.lock().await;
                    if *shutdown {
                        break;
                    }
                }

                // Clean expired entries (older than 30 seconds)
                {
                    let mut cache = aggregated_cache.write().await;
                    cache.retain(|_, (_, created_at)| created_at.elapsed() < Duration::from_secs(30));
                }

                // Run cleanup every 10 seconds
                sleep(Duration::from_secs(10)).await;
            }
        });
    }

    /// Update metrics for aggregation
    async fn update_metrics_aggregation(&self) {
        let mut metrics = self.metrics.write().await;
        metrics.requests_processed += 1;
        // Update aggregation ratio calculation as needed
    }

    /// Update batch processing metrics
    async fn update_batch_metrics(
        metrics: &Arc<RwLock<BatchMetrics>>,
        batch_size: usize,
        processing_time_ms: f64,
    ) {
        let mut m = metrics.write().await;
        m.requests_processed += batch_size as u64;
        m.batches_processed += 1;
        
        // Update running averages
        let total_batches = m.batches_processed as f64;
        m.average_batch_size = (m.average_batch_size * (total_batches - 1.0) + batch_size as f64) / total_batches;
        m.average_processing_time_ms = (m.average_processing_time_ms * (total_batches - 1.0) + processing_time_ms) / total_batches;
    }

    /// Get processing metrics
    pub async fn get_metrics(&self) -> BatchMetrics {
        let mut metrics = self.metrics.read().await.clone();
        metrics.queue_length = self.request_queue.lock().await.len();
        metrics
    }

    /// Shutdown the batching system
    pub async fn shutdown(&self) {
        let mut shutdown = self.shutdown_signal.lock().await;
        *shutdown = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_batch_processing() {
        let config = BatchConfig {
            max_batch_size: 5,
            batch_timeout_ms: 100,
            ..Default::default()
        };
        let system = Arc::new(AsyncBatchingSystem::new(config));
        system.start().await;

        // Submit multiple requests
        let mut handles = Vec::new();
        for i in 0..3 {
            let system_clone = Arc::clone(&system);
            let handle = tokio::spawn(async move {
                system_clone.submit_request(
                    format!("req-{}", i),
                    "BTCUSDT".to_string(),
                    IndicatorType::RelativeStrengthIndex,
                    IndicatorParameters::default(),
                ).await
            });
            handles.push(handle);
        }

        // Wait for all requests to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "Request should succeed");
        }

        let metrics = system.get_metrics().await;
        assert_eq!(metrics.requests_processed, 3);

        system.shutdown().await;
    }

    #[tokio::test]
    async fn test_request_aggregation() {
        let config = BatchConfig {
            aggregation_enabled: true,
            ..Default::default()
        };
        let system = Arc::new(AsyncBatchingSystem::new(config));
        system.start().await;

        // Submit identical requests (should be aggregated)
        let mut handles = Vec::new();
        for i in 0..5 {
            let system_clone = Arc::clone(&system);
            let handle = tokio::spawn(async move {
                system_clone.submit_request(
                    format!("req-{}", i),
                    "BTCUSDT".to_string(),
                    IndicatorType::RelativeStrengthIndex,
                    IndicatorParameters::default(),
                ).await
            });
            handles.push(handle);
        }

        // All requests should get the same result
        let mut results = Vec::new();
        for handle in handles {
            let result = handle.await.unwrap().unwrap();
            results.push(result);
        }

        // Verify all results are identical (indicating successful aggregation)
        for i in 1..results.len() {
            assert_eq!(results[0], results[i], "Aggregated results should be identical");
        }

        system.shutdown().await;
    }
}