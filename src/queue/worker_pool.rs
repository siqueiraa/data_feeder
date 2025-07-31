use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use super::types::{QueueConfig, TaskResult};
use super::metrics::QueueMetrics;

/// Generic worker pool for parallel task execution
pub struct WorkerPool<T> {
    config: QueueConfig,
    task_sender: mpsc::UnboundedSender<WorkerTask<T>>,
    worker_semaphore: Arc<Semaphore>,
    metrics: QueueMetrics,
    task_counter: std::sync::atomic::AtomicU64,
}

/// Task wrapper for worker execution
struct WorkerTask<T> {
    id: u64,
    task: Box<dyn FnOnce() -> TaskResult<T> + Send + 'static>,
    result_sender: tokio::sync::oneshot::Sender<TaskResult<T>>,
    created_at: Instant,
    task_name: String,
}

impl<T: Send + 'static> WorkerPool<T> {
    /// Create new worker pool
    pub fn new(config: QueueConfig, pool_name: String) -> Self {
        let (task_sender, task_receiver) = mpsc::unbounded_channel();
        let worker_semaphore = Arc::new(Semaphore::new(config.max_workers));
        let metrics = QueueMetrics::new(format!("worker_pool_{}", pool_name));

        let pool = Self {
            config: config.clone(),
            task_sender,
            worker_semaphore: worker_semaphore.clone(),
            metrics: metrics.clone(),
            task_counter: std::sync::atomic::AtomicU64::new(0),
        };

        // Start worker dispatcher
        pool.start_dispatcher(task_receiver, worker_semaphore, metrics);

        info!("🚀 WorkerPool '{}' initialized with {} workers", pool_name, config.max_workers);
        pool
    }

    /// Submit a task to the worker pool
    pub async fn submit<F>(
        &self,
        task: F,
        task_name: String,
        timeout: Option<Duration>,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce() -> TaskResult<T> + Send + 'static,
    {
        let task_id = self.task_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (result_sender, result_receiver) = tokio::sync::oneshot::channel();

        let worker_task = WorkerTask {
            id: task_id,
            task: Box::new(task),
            result_sender,
            created_at: Instant::now(),
            task_name: task_name.clone(),
        };

        // Submit to pool
        if self.task_sender.send(worker_task).is_err() {
            return Err("Worker pool is closed".into());
        }

        self.metrics.record_task_submitted();
        debug!("📤 Submitted task {} ({})", task_id, task_name);

        // Wait for completion
        let wait_result = if let Some(timeout_dur) = timeout {
            tokio::time::timeout(timeout_dur, result_receiver).await
        } else {
            Ok(result_receiver.await)
        };

        match wait_result {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("Task was cancelled".into()),
            Err(_) => Err("Task timed out".into()),
        }
    }

    /// Submit a blocking task that will run on a dedicated thread
    pub async fn submit_blocking<F>(
        &self,
        task: F,
        task_name: String,
        timeout: Option<Duration>,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce() -> TaskResult<T> + Send + 'static,
    {
        self.submit(task, format!("blocking_{}", task_name), timeout).await
    }

    /// Start the task dispatcher
    fn start_dispatcher(
        &self,
        mut task_receiver: mpsc::UnboundedReceiver<WorkerTask<T>>,
        worker_semaphore: Arc<Semaphore>,
        metrics: QueueMetrics,
    ) {
        let config = self.config.clone();
        
        tokio::spawn(async move {
            let mut pending_tasks = std::collections::VecDeque::new();
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(30));
            
            info!("📋 Worker pool dispatcher started");

            loop {
                tokio::select! {
                    // Handle new tasks
                    Some(task) = task_receiver.recv() => {
                        pending_tasks.push_back(task);
                        metrics.record_task_queued();
                        debug!("📥 Queued worker task (queue size: {})", pending_tasks.len());
                    }
                    
                    // Try to dispatch tasks
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        Self::dispatch_tasks(&mut pending_tasks, &worker_semaphore, &metrics, &config).await;
                    }
                    
                    // Periodic cleanup
                    _ = cleanup_interval.tick() => {
                        Self::cleanup_abandoned_tasks(&mut pending_tasks, &metrics).await;
                    }
                }
            }
        });
    }

    /// Dispatch tasks to available workers
    async fn dispatch_tasks(
        pending_tasks: &mut std::collections::VecDeque<WorkerTask<T>>,
        worker_semaphore: &Arc<Semaphore>,
        metrics: &QueueMetrics,
        config: &QueueConfig,
    ) {
        while let Some(task) = pending_tasks.pop_front() {
            // Check if task should be abandoned (too old)
            if task.created_at.elapsed() > config.queue_timeout {
                warn!("⏰ Worker task {} expired after {:?}", task.id, task.created_at.elapsed());
                let _ = task.result_sender.send(Err("Task expired in queue".into()));
                metrics.record_task_timeout();
                continue;
            }

            // Try to acquire worker
            match Arc::clone(worker_semaphore).try_acquire_owned() {
                Ok(permit) => {
                    let task_id = task.id;
                    let task_name = task.task_name.clone();
                    let created_at = task.created_at;
                    
                    metrics.record_task_started();
                    debug!("🏃 Starting worker task {} ({})", task_id, task_name);

                    // Spawn worker
                    let metrics_clone = metrics.clone();
                    let worker_timeout = config.worker_timeout;
                    
                    tokio::spawn(async move {
                        let _permit = permit;
                        let start_time = Instant::now();
                        let wait_time = created_at.elapsed();
                        
                        // Execute task with timeout protection
                        let execution_result = if let Some(timeout_dur) = worker_timeout.checked_sub(Duration::from_millis(100)) {
                            // Use spawn_blocking for potentially CPU-intensive tasks
                            match tokio::time::timeout(timeout_dur, tokio::task::spawn_blocking(move || {
                                (task.task)()
                            })).await {
                                Ok(Ok(result)) => result,
                                Ok(Err(e)) => Err(format!("Task panicked: {:?}", e).into()),
                                Err(_) => Err("Worker timeout".into()),
                            }
                        } else {
                            // Direct execution for quick tasks
                            (task.task)()
                        };

                        let execution_time = start_time.elapsed();
                        
                        // Send result
                        let result_for_metrics = execution_result.is_ok();
                        if task.result_sender.send(execution_result).is_err() {
                            warn!("📤 Failed to send worker result for task {}", task_id);
                        }

                        // Update metrics
                        metrics_clone.record_wait_time(wait_time);
                        if result_for_metrics {
                            metrics_clone.record_task_completed(execution_time);
                            debug!("✅ Worker task {} completed in {:?}", task_id, execution_time);
                        } else {
                            metrics_clone.record_task_error();
                            error!("❌ Worker task {} failed", task_id);
                        }
                    });
                }
                Err(_) => {
                    // No workers available, put task back
                    pending_tasks.push_front(task);
                    break;
                }
            }
        }
    }

    /// Clean up abandoned tasks
    async fn cleanup_abandoned_tasks(
        pending_tasks: &mut std::collections::VecDeque<WorkerTask<T>>,
        metrics: &QueueMetrics,
    ) {
        let mut valid_tasks = std::collections::VecDeque::new();
        let max_age = Duration::from_secs(300); // 5 minutes
        
        while let Some(task) = pending_tasks.pop_front() {
            if task.created_at.elapsed() > max_age {
                warn!("🗑️ Cleaning up abandoned worker task {}", task.id);
                let _ = task.result_sender.send(Err("Task abandoned".into()));
                metrics.record_task_timeout();
            } else {
                valid_tasks.push_back(task);
            }
        }
        
        *pending_tasks = valid_tasks;
    }

    /// Get queue metrics
    pub fn get_metrics(&self) -> &QueueMetrics {
        &self.metrics
    }

    /// Get current queue size
    pub async fn queue_size(&self) -> usize {
        // This is an approximation since we don't have direct access to pending_tasks
        // In a real implementation, you might want to expose this through the dispatcher
        0
    }

    /// Get available worker count
    pub fn available_workers(&self) -> usize {
        self.worker_semaphore.available_permits()
    }

    /// Get total worker count
    pub fn total_workers(&self) -> usize {
        self.config.max_workers
    }
}

/// Specialized worker pools for common use cases
///
/// CPU-intensive task pool
pub type CpuPool<T> = WorkerPool<T>;

/// I/O task pool
pub type IoPool<T> = WorkerPool<T>;

/// Mixed workload pool
pub type GeneralPool<T> = WorkerPool<T>;

/// Pool manager for coordinating multiple worker pools
pub struct PoolManager {
    cpu_pool: Arc<CpuPool<()>>,
    io_pool: Arc<IoPool<Vec<u8>>>,
    general_pool: Arc<GeneralPool<String>>,
    metrics_aggregator: super::metrics::MetricsAggregator,
}

impl PoolManager {
    /// Create new pool manager with optimized pool configurations
    pub async fn new() -> Self {
        let cpu_cores = num_cpus::get();
        
        // CPU pool - limited to CPU cores for CPU-bound tasks
        let cpu_config = QueueConfig {
            max_workers: cpu_cores,
            worker_timeout: Duration::from_secs(30),
            queue_timeout: Duration::from_secs(60),
            ..Default::default()
        };
        let cpu_pool = Arc::new(CpuPool::new(cpu_config, "cpu".to_string()));

        // I/O pool - can have more workers since they'll be waiting on I/O
        let io_config = QueueConfig {
            max_workers: cpu_cores * 4,
            worker_timeout: Duration::from_secs(60),
            queue_timeout: Duration::from_secs(120),
            ..Default::default()
        };
        let io_pool = Arc::new(IoPool::new(io_config, "io".to_string()));

        // General pool - balanced configuration
        let general_config = QueueConfig {
            max_workers: cpu_cores * 2,
            worker_timeout: Duration::from_secs(45),
            queue_timeout: Duration::from_secs(90),
            ..Default::default()
        };
        let general_pool = Arc::new(GeneralPool::new(general_config, "general".to_string()));

        // Set up metrics aggregation
        let metrics_aggregator = super::metrics::MetricsAggregator::new();
        metrics_aggregator.register_queue(cpu_pool.get_metrics().clone()).await;
        metrics_aggregator.register_queue(io_pool.get_metrics().clone()).await;
        metrics_aggregator.register_queue(general_pool.get_metrics().clone()).await;

        info!("🚀 PoolManager initialized with {} CPU, {} I/O, {} general workers", 
              cpu_cores, cpu_cores * 4, cpu_cores * 2);

        Self {
            cpu_pool,
            io_pool,
            general_pool,
            metrics_aggregator,
        }
    }

    /// Get CPU pool reference
    pub fn cpu_pool(&self) -> &Arc<CpuPool<()>> {
        &self.cpu_pool
    }

    /// Get I/O pool reference
    pub fn io_pool(&self) -> &Arc<IoPool<Vec<u8>>> {
        &self.io_pool
    }

    /// Get general pool reference
    pub fn general_pool(&self) -> &Arc<GeneralPool<String>> {
        &self.general_pool
    }

    /// Get overall health status
    pub async fn get_health(&self) -> super::metrics::OverallHealth {
        self.metrics_aggregator.get_overall_health().await
    }

    /// Log summary of all pools
    pub async fn log_summary(&self) {
        self.metrics_aggregator.log_all_summaries().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_worker_pool_basic() {
        let config = QueueConfig {
            max_workers: 2,
            ..Default::default()
        };
        let pool: WorkerPool<i32> = WorkerPool::new(config, "test".to_string());

        let result = pool.submit(
            || Ok(42),
            "test_task".to_string(),
            Some(Duration::from_secs(1)),
        ).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_worker_pool_concurrency() {
        let config = QueueConfig {
            max_workers: 4,
            ..Default::default()
        };
        let pool: WorkerPool<usize> = WorkerPool::new(config, "concurrent".to_string());
        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for i in 0..10 {
            let counter = counter.clone();
            let handle = tokio::spawn({
                let pool = &pool;
                async move {
                    pool.submit(
                        move || {
                            counter.fetch_add(1, Ordering::SeqCst);
                            Ok(i)
                        },
                        format!("task_{}", i),
                        Some(Duration::from_secs(1)),
                    ).await
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn test_worker_pool_timeout() {
        let config = QueueConfig {
            max_workers: 1,
            worker_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let pool: WorkerPool<()> = WorkerPool::new(config, "timeout".to_string());

        let result = pool.submit(
            || {
                std::thread::sleep(std::time::Duration::from_millis(200));
                Ok(())
            },
            "slow_task".to_string(),
            Some(Duration::from_millis(50)),
        ).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pool_manager() {
        let manager = PoolManager::new().await;

        // Test CPU pool
        let cpu_result = manager.cpu_pool().submit(
            || Ok(()),
            "cpu_test".to_string(),
            Some(Duration::from_secs(1)),
        ).await;
        assert!(cpu_result.is_ok());

        // Test I/O pool
        let io_result = manager.io_pool().submit(
            || Ok(vec![1, 2, 3]),
            "io_test".to_string(),
            Some(Duration::from_secs(1)),
        ).await;
        assert!(io_result.is_ok());

        // Test general pool
        let general_result = manager.general_pool().submit(
            || Ok("test".to_string()),
            "general_test".to_string(),
            Some(Duration::from_secs(1)),
        ).await;
        assert!(general_result.is_ok());

        // Check health
        let health = manager.get_health().await;
        assert!(health.total_queues >= 3);
    }
}