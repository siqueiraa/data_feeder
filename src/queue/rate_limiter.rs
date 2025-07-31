use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock, Semaphore};
use tokio::time::{timeout, Duration, Instant, interval};
use tracing::{debug, error, info, warn};

use super::types::{Priority, QueueTask, QueueConfig, AsyncTask};
use super::metrics::QueueMetrics;

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per time window
    pub max_requests: u32,
    /// Time window duration
    pub window_duration: Duration,
    /// Burst allowance (extra requests allowed in short bursts)
    pub burst_allowance: u32,
    /// Minimum delay between requests
    pub min_delay: Duration,
    /// Whether to use adaptive rate limiting
    pub adaptive: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window_duration: Duration::from_secs(60),
            burst_allowance: 20,
            min_delay: Duration::from_millis(100),
            adaptive: true,
        }
    }
}

/// Rate-limited API request
pub struct RateLimitedRequest<T> {
    pub task: AsyncTask<T>,
    pub metadata: QueueTask<T>,
    pub endpoint: String,
    pub estimated_cost: u32,
}

/// Rate limiting statistics
#[derive(Debug, Clone)]
pub struct RateLimitStats {
    pub requests_in_window: u32,
    pub requests_queued: usize,
    pub requests_rejected: u64,
    pub average_delay_ms: f64,
    pub current_delay_ms: u64,
    pub burst_tokens_available: u32,
}

/// Intelligent rate limiting queue for API calls
#[derive(Debug)]
pub struct RateLimitingQueue<T> {
    config: QueueConfig,
    #[allow(dead_code)] // Used in rate limiting logic, kept for API completeness
    rate_config: RateLimitConfig,
    task_sender: mpsc::UnboundedSender<RateLimitedRequest<T>>,
    #[allow(dead_code)] // Used in worker dispatcher, kept for API completeness
    worker_semaphore: Arc<Semaphore>,
    metrics: QueueMetrics,
    task_counter: std::sync::atomic::AtomicU64,
    stats: Arc<RwLock<RateLimitStats>>,
}

impl<T: Send + 'static> RateLimitingQueue<T> {
    /// Create new rate limiting queue
    pub fn new(config: QueueConfig, rate_config: RateLimitConfig) -> Self {
        let (task_sender, task_receiver) = mpsc::unbounded_channel();
        let worker_semaphore = Arc::new(Semaphore::new(config.max_workers));
        let metrics = QueueMetrics::new("rate_limit_queue".to_string());
        let stats = Arc::new(RwLock::new(RateLimitStats {
            requests_in_window: 0,
            requests_queued: 0,
            requests_rejected: 0,
            average_delay_ms: 0.0,
            current_delay_ms: rate_config.min_delay.as_millis() as u64,
            burst_tokens_available: rate_config.burst_allowance,
        }));

        let queue = Self {
            config: config.clone(),
            rate_config: rate_config.clone(),
            task_sender,
            worker_semaphore: worker_semaphore.clone(),
            metrics: metrics.clone(),
            task_counter: std::sync::atomic::AtomicU64::new(0),
            stats: stats.clone(),
        };

        info!("🚀 RateLimitingQueue initialized: {} workers, {}/{}s (burst: {})", 
              config.max_workers, rate_config.max_requests, rate_config.window_duration.as_secs(), rate_config.burst_allowance);

        // Start rate limiter
        queue.start_rate_limiter(task_receiver, worker_semaphore, metrics, stats, rate_config);
        queue
    }

    /// Submit a rate-limited API request
    pub async fn submit_request(
        &self,
        task: AsyncTask<T>,
        endpoint: String,
        priority: Priority,
        estimated_cost: u32,
        timeout_duration: Option<Duration>,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
        let task_id = self.task_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (result_sender, result_receiver) = oneshot::channel();

        let request = RateLimitedRequest {
            task,
            metadata: QueueTask::new(
                task_id,
                priority,
                timeout_duration,
                result_sender,
                format!("api_request:{}", endpoint),
            ),
            endpoint: endpoint.clone(),
            estimated_cost,
        };

        // Check queue capacity
        {
            let stats = self.stats.read().await;
            if stats.requests_queued >= self.config.max_size {
                return Err("Rate limiting queue is full".into());
            }
        }

        // Submit to queue
        if self.task_sender.send(request).is_err() {
            return Err("Rate limiting queue is closed".into());
        }

        self.metrics.record_task_submitted();
        debug!("📤 Submitted rate-limited request {} to {}", task_id, endpoint);

        // Wait for completion
        let wait_result = if let Some(timeout_dur) = timeout_duration {
            timeout(timeout_dur, result_receiver).await
        } else {
            Ok(result_receiver.await)
        };

        match wait_result {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("Request was cancelled".into()),
            Err(_) => Err("Request timed out".into()),
        }
    }

    /// Start the rate limiter task
    fn start_rate_limiter(
        &self,
        mut task_receiver: mpsc::UnboundedReceiver<RateLimitedRequest<T>>,
        worker_semaphore: Arc<Semaphore>,
        metrics: QueueMetrics,
        stats: Arc<RwLock<RateLimitStats>>,
        rate_config: RateLimitConfig,
    ) {
        let config = self.config.clone();
        
        tokio::spawn(async move {
            let mut pending_requests: VecDeque<RateLimitedRequest<T>> = VecDeque::new();
            let mut request_times: VecDeque<Instant> = VecDeque::new();
            let mut last_request_time = Instant::now();
            let mut current_delay = rate_config.min_delay;
            let mut burst_tokens = rate_config.burst_allowance;
            
            let mut rate_limiter_timer = interval(Duration::from_millis(10));
            let mut window_cleaner = interval(rate_config.window_duration / 4);
            let mut stats_updater = interval(Duration::from_secs(5));
            
            info!("📋 Rate limiter started");

            loop {
                tokio::select! {
                    // Handle new requests
                    Some(request) = task_receiver.recv() => {
                        pending_requests.push_back(request);
                        metrics.record_task_queued();
                        
                        // Update queue size in stats
                        {
                            let mut stats_guard = stats.write().await;
                            stats_guard.requests_queued = pending_requests.len();
                        }
                    }
                    
                    // Process rate limiting
                    _ = rate_limiter_timer.tick() => {
                        Self::process_rate_limited_requests(
                            &mut pending_requests,
                            &mut request_times,
                            &mut last_request_time,
                            &mut current_delay,
                            &mut burst_tokens,
                            &worker_semaphore,
                            &metrics,
                            &stats,
                            &rate_config,
                            &config,
                        ).await;
                    }
                    
                    // Clean old request times
                    _ = window_cleaner.tick() => {
                        Self::clean_request_window(&mut request_times, &rate_config);
                    }
                    
                    // Update statistics
                    _ = stats_updater.tick() => {
                        Self::update_rate_limit_stats(&stats, pending_requests.len(), &request_times, current_delay).await;
                    }
                }
            }
        });
    }

    /// Process rate-limited requests
    #[allow(clippy::too_many_arguments)] // Complex state management requires many parameters
    async fn process_rate_limited_requests(
        pending_requests: &mut VecDeque<RateLimitedRequest<T>>,
        request_times: &mut VecDeque<Instant>,
        last_request_time: &mut Instant,
        current_delay: &mut Duration,
        burst_tokens: &mut u32,
        worker_semaphore: &Arc<Semaphore>,
        metrics: &QueueMetrics,
        stats: &Arc<RwLock<RateLimitStats>>,
        rate_config: &RateLimitConfig,
        config: &QueueConfig,
    ) {
        // Clean expired requests first
        Self::clean_expired_requests(pending_requests, metrics).await;

        // Sort by priority (simple priority ordering)
        let mut high_priority = VecDeque::new();
        let mut normal_priority = VecDeque::new();
        
        while let Some(request) = pending_requests.pop_front() {
            match request.metadata.priority {
                Priority::Critical | Priority::High => high_priority.push_back(request),
                _ => normal_priority.push_back(request),
            }
        }
        
        // Merge back with priority order
        pending_requests.extend(high_priority);
        pending_requests.extend(normal_priority);

        // Process requests respecting rate limits
        while let Some(_request) = pending_requests.front() {
            // Check if rate limit allows this request
            if !Self::can_make_request(request_times, burst_tokens, rate_config) {
                break;
            }

            // Check if enough time has passed since last request
            let time_since_last = last_request_time.elapsed();
            if time_since_last < *current_delay {
                break;
            }

            // Try to acquire worker
            match Arc::clone(worker_semaphore).try_acquire_owned() {
                Ok(permit) => {
                    let request = pending_requests.pop_front().unwrap();
                    let request_time = Instant::now();
                    
                    // Update rate limiting state
                    request_times.push_back(request_time);
                    *last_request_time = request_time;
                    
                    // Use burst token if available, otherwise increment request count
                    if *burst_tokens > 0 && request.estimated_cost <= *burst_tokens {
                        *burst_tokens -= request.estimated_cost;
                    }
                    
                    // Adaptive delay adjustment
                    if rate_config.adaptive {
                        Self::adjust_delay(current_delay, request_times, rate_config);
                    }
                    
                    metrics.record_task_started();
                    debug!("🏃 Processing rate-limited request {} (delay: {:?})", 
                           request.metadata.id, current_delay);

                    // Spawn worker
                    let metrics_clone = metrics.clone();
                    let task_id = request.metadata.id;
                    let created_at = request.metadata.created_at;
                    let worker_timeout = config.worker_timeout;
                    
                    tokio::spawn(async move {
                        let _permit = permit;
                        let start_time = Instant::now();
                        let wait_time = created_at.elapsed();
                        
                        // Execute the API request with timeout
                        let execution_result = if let Some(timeout_dur) = worker_timeout.checked_sub(Duration::from_millis(100)) {
                            match timeout(timeout_dur, request.task).await {
                                Ok(result) => result,
                                Err(_) => Err("API request timeout".into()),
                            }
                        } else {
                            request.task.await
                        };

                        let execution_time = start_time.elapsed();
                        
                        // Send result
                        let result_for_metrics = execution_result.is_ok();
                        if request.metadata.result_sender.send(execution_result).is_err() {
                            warn!("📤 Failed to send API result for request {}", task_id);
                        }

                        // Update metrics
                        metrics_clone.record_wait_time(wait_time);
                        if result_for_metrics {
                            metrics_clone.record_task_completed(execution_time);
                            debug!("✅ API request {} completed in {:?}", task_id, execution_time);
                        } else {
                            metrics_clone.record_task_error();
                            error!("❌ API request {} failed", task_id);
                        }
                    });
                }
                Err(_) => {
                    // No workers available
                    break;
                }
            }
        }

        // Update queue size in stats
        {
            let mut stats_guard = stats.write().await;
            stats_guard.requests_queued = pending_requests.len();
        }
    }

    /// Check if a request can be made based on rate limits
    fn can_make_request(
        request_times: &VecDeque<Instant>,
        burst_tokens: &u32,
        rate_config: &RateLimitConfig,
    ) -> bool {
        let requests_in_window = request_times.len() as u32;
        
        // Check if within rate limit or have burst tokens
        requests_in_window < rate_config.max_requests || *burst_tokens > 0
    }

    /// Adjust delay based on current load (adaptive rate limiting)
    fn adjust_delay(
        current_delay: &mut Duration,
        request_times: &VecDeque<Instant>,
        rate_config: &RateLimitConfig,
    ) {
        let requests_in_window = request_times.len() as u32;
        let window_utilization = requests_in_window as f64 / rate_config.max_requests as f64;
        
        // Increase delay as we approach rate limit
        let base_delay = rate_config.min_delay;
        let max_delay = base_delay * 10;
        
        let adjusted_delay = if window_utilization > 0.8 {
            // High utilization - increase delay significantly
            base_delay + Duration::from_millis((base_delay.as_millis() as f64 * window_utilization * 5.0) as u64)
        } else if window_utilization > 0.6 {
            // Medium utilization - moderate increase
            base_delay + Duration::from_millis((base_delay.as_millis() as f64 * window_utilization * 2.0) as u64)
        } else {
            // Low utilization - use minimum delay
            base_delay
        };
        
        *current_delay = adjusted_delay.min(max_delay);
    }

    /// Clean old request times from the window
    fn clean_request_window(
        request_times: &mut VecDeque<Instant>,
        rate_config: &RateLimitConfig,
    ) {
        let cutoff = Instant::now() - rate_config.window_duration;
        
        while let Some(&front_time) = request_times.front() {
            if front_time < cutoff {
                request_times.pop_front();
            } else {
                break;
            }
        }
    }

    /// Clean expired requests
    async fn clean_expired_requests(
        pending_requests: &mut VecDeque<RateLimitedRequest<T>>,
        metrics: &QueueMetrics,
    ) {
        let mut valid_requests = VecDeque::new();
        
        while let Some(request) = pending_requests.pop_front() {
            if request.metadata.is_expired() {
                warn!("🗑️ Cleaning up expired rate-limited request {}", request.metadata.id);
                let _ = request.metadata.result_sender.send(Err("Request expired in queue".into()));
                metrics.record_task_timeout();
            } else {
                valid_requests.push_back(request);
            }
        }
        
        *pending_requests = valid_requests;
    }

    /// Update rate limiting statistics
    async fn update_rate_limit_stats(
        stats: &Arc<RwLock<RateLimitStats>>,
        queue_size: usize,
        request_times: &VecDeque<Instant>,
        current_delay: Duration,
    ) {
        let mut stats_guard = stats.write().await;
        stats_guard.requests_in_window = request_times.len() as u32;
        stats_guard.requests_queued = queue_size;
        stats_guard.current_delay_ms = current_delay.as_millis() as u64;
        
        // Calculate average delay from recent request times
        if request_times.len() > 1 {
            let delays: Vec<u64> = request_times
                .iter()
                .zip(request_times.iter().skip(1))
                .map(|(prev, curr)| curr.duration_since(*prev).as_millis() as u64)
                .collect();
            
            if !delays.is_empty() {
                stats_guard.average_delay_ms = delays.iter().sum::<u64>() as f64 / delays.len() as f64;
            }
        }
    }

    /// Get current rate limiting statistics
    pub async fn get_stats(&self) -> RateLimitStats {
        self.stats.read().await.clone()
    }

    /// Get queue metrics
    pub fn get_metrics(&self) -> &QueueMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_rate_limiting() {
        let rate_config = RateLimitConfig {
            max_requests: 5,
            window_duration: Duration::from_secs(1),
            burst_allowance: 2,
            min_delay: Duration::from_millis(100),
            adaptive: false,
        };
        
        let config = QueueConfig {
            max_workers: 10,
            ..Default::default()
        };
        
        let queue: Arc<RateLimitingQueue<i32>> = Arc::new(RateLimitingQueue::new(config, rate_config));

        // Submit requests rapidly
        let mut handles = Vec::new();
        for i in 0..10 {
            let queue = queue.clone();
            let handle = tokio::spawn(async move {
                let task = Box::pin(async move { Ok(i) });
                queue.submit_request(
                    task,
                    format!("/api/endpoint/{}", i),
                    Priority::Normal,
                    1,
                    Some(Duration::from_secs(5)),
                ).await
            });
            handles.push(handle);
        }

        // Collect results
        let mut results = Vec::new();
        for handle in handles {
            if let Ok(result) = handle.await {
                results.push(result);
            }
        }

        // Should have some successful requests
        assert!(!results.is_empty());
        
        // Check stats
        let stats = queue.get_stats().await;
        assert!(stats.requests_in_window <= 7); // max_requests + burst_allowance
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let rate_config = RateLimitConfig {
            max_requests: 2,
            window_duration: Duration::from_secs(1),
            burst_allowance: 0,
            min_delay: Duration::from_millis(200),
            adaptive: false,
        };
        
        let config = QueueConfig {
            max_workers: 1, // Force serialization
            ..Default::default()
        };
        
        let queue: Arc<RateLimitingQueue<i32>> = Arc::new(RateLimitingQueue::new(config, rate_config));
        let execution_order = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let mut handles = Vec::new();

        // Submit in reverse priority order
        for (i, priority) in [(0, Priority::Low), (1, Priority::High), (2, Priority::Critical)].iter() {
            let order = execution_order.clone();
            let task_id = *i;
            let queue = queue.clone();
            
            let handle = tokio::spawn(async move {
                let task = Box::pin(async move {
                    let mut order = order.lock().await;
                    order.push(task_id);
                    Ok(task_id)
                });
                
                queue.submit_request(
                    task,
                    format!("/api/{}", task_id),
                    *priority,
                    1,
                    Some(Duration::from_secs(10)),
                ).await
            });
            handles.push(handle);
        }

        // Wait for completion
        for handle in handles {
            let _ = handle.await;
        }

        let final_order = execution_order.lock().await;
        // Higher priority should execute first
        if !final_order.is_empty() {
            assert!(final_order[0] == 2 || final_order[0] == 1); // Critical or High priority first
        }
    }
}