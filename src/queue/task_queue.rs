use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock, Semaphore};
use tokio::time::{timeout, Duration, Instant};
use tracing::{debug, error, info, warn};

use super::types::{Priority, QueueTask, QueueConfig, QueueStats, CpuTask, BlockingTask};
use super::metrics::QueueMetrics;

/// Priority-based task queue with non-blocking execution
pub struct TaskQueue {
    config: QueueConfig,
    task_sender: mpsc::UnboundedSender<CpuTask>,
    #[allow(dead_code)] // Used in worker manager, kept for API completeness
    worker_semaphore: Arc<Semaphore>,
    task_counter: AtomicU64,
    metrics: QueueMetrics,
    stats: Arc<RwLock<QueueStats>>,
}

impl TaskQueue {
    /// Create a new task queue
    pub fn new(config: QueueConfig) -> Self {
        let (task_sender, task_receiver) = mpsc::unbounded_channel();
        let worker_semaphore = Arc::new(Semaphore::new(config.max_workers));
        let metrics = QueueMetrics::new("task_queue".to_string());
        let stats = Arc::new(RwLock::new(QueueStats::default()));

        let queue = Self {
            config: config.clone(),
            task_sender,
            worker_semaphore: worker_semaphore.clone(),
            task_counter: AtomicU64::new(0),
            metrics: metrics.clone(),
            stats: stats.clone(),
        };

        // Start the worker manager
        queue.start_worker_manager(task_receiver, worker_semaphore, metrics, stats);

        info!("🚀 TaskQueue initialized with {} workers", config.max_workers);
        queue
    }

    /// Submit a CPU-intensive task for execution
    pub async fn submit_blocking<T: Send + 'static>(
        &self,
        task: BlockingTask<T>,
        priority: Priority,
        timeout_duration: Option<Duration>,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
        let task_id = self.task_counter.fetch_add(1, Ordering::SeqCst);
        let (result_sender, result_receiver) = oneshot::channel();

        // Convert to unit task wrapper
        let (unit_sender, unit_receiver) = oneshot::channel();
        let unit_task = Box::new(move || {
            let result = task();
            let _ = unit_sender.send(result);
            Ok(())
        });

        let cpu_task = CpuTask {
            task: unit_task,
            metadata: QueueTask::new(
                task_id,
                priority,
                timeout_duration,
                result_sender,
                "blocking_task".to_string(),
            ),
        };

        // Submit to queue
        if self.task_sender.send(cpu_task).is_err() {
            return Err("Task queue is closed".into());
        }

        self.metrics.record_task_submitted();
        debug!("📤 Submitted task {} with priority {}", task_id, priority);

        // Wait for completion
        let wait_result = if let Some(timeout_dur) = timeout_duration {
            timeout(timeout_dur, result_receiver).await
        } else {
            Ok(result_receiver.await)
        };

        match wait_result {
            Ok(Ok(Ok(()))) => {
                // Get the actual result
                match unit_receiver.await {
                    Ok(result) => result,
                    Err(_) => Err("Task execution failed".into()),
                }
            }
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err("Task was cancelled".into()),
            Err(_) => Err("Task timed out".into()),
        }
    }

    /// Get current queue statistics
    pub async fn get_stats(&self) -> QueueStats {
        self.stats.read().await.clone()
    }

    /// Get queue metrics
    pub fn get_metrics(&self) -> &QueueMetrics {
        &self.metrics
    }

    /// Start the worker manager task
    fn start_worker_manager(
        &self,
        mut task_receiver: mpsc::UnboundedReceiver<CpuTask>,
        worker_semaphore: Arc<Semaphore>,
        metrics: QueueMetrics,
        stats: Arc<RwLock<QueueStats>>,
    ) {
        let config = self.config.clone();
        
        tokio::spawn(async move {
            // Priority queue for pending tasks
            let mut pending_tasks: BinaryHeap<TaskWrapper> = BinaryHeap::new();
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(10));
            
            info!("📋 Task queue worker manager started");

            loop {
                tokio::select! {
                    // Handle new tasks
                    Some(task) = task_receiver.recv() => {
                        let task_id = task.metadata.id;
                        let task_priority = task.metadata.priority;
                        let task_wrapper = TaskWrapper::new(task);
                        pending_tasks.push(task_wrapper);
                        metrics.record_task_queued();
                        
                        debug!("📥 Queued task {} (priority: {}, queue size: {})", 
                               task_id,
                               task_priority,
                               pending_tasks.len());
                    }
                    
                    // Try to dispatch high-priority tasks
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        Self::dispatch_tasks(&mut pending_tasks, &worker_semaphore, &metrics, &config).await;
                    }
                    
                    // Periodic cleanup and stats update
                    _ = cleanup_interval.tick() => {
                        Self::cleanup_expired_tasks(&mut pending_tasks, &metrics).await;
                        Self::update_stats(&stats, pending_tasks.len(), &metrics).await;
                    }
                }
            }
        });
    }

    /// Dispatch tasks to available workers
    async fn dispatch_tasks(
        pending_tasks: &mut BinaryHeap<TaskWrapper>,
        worker_semaphore: &Arc<Semaphore>,
        metrics: &QueueMetrics,
        config: &QueueConfig,
    ) {
        while let Some(task_wrapper) = pending_tasks.peek() {
            // Check if task has expired
            if task_wrapper.task.metadata.is_expired() {
                let expired_task = pending_tasks.pop().unwrap();
                warn!("⏰ Task {} expired after {:?}", 
                      expired_task.task.metadata.id, 
                      expired_task.task.metadata.age());
                
                let _ = expired_task.task.metadata.result_sender.send(
                    Err("Task expired".into())
                );
                metrics.record_task_timeout();
                continue;
            }

            // Try to acquire worker permit (non-blocking)
            match Arc::clone(worker_semaphore).try_acquire_owned() {
                Ok(permit) => {
                    let task = pending_tasks.pop().unwrap().task;
                    let task_id = task.metadata.id;
                    let task_priority = task.metadata.priority;
                    let created_at = task.metadata.created_at;
                    
                    metrics.record_task_started();
                    
                    debug!("🏃 Starting task {} (priority: {}, wait time: {:?})", 
                           task_id, task_priority, created_at.elapsed());

                    // Spawn worker task
                    let metrics_clone = metrics.clone();
                    let worker_timeout = config.worker_timeout;
                    let task_function = task.task;
                    let result_sender = task.metadata.result_sender;
                    
                    tokio::spawn(async move {
                        let _permit = permit; // Keep permit alive for duration of task
                        let start_time = Instant::now();
                        
                        // Execute the blocking task
                        let execution_result = if let Some(timeout_dur) = worker_timeout.checked_sub(Duration::from_millis(100)) {
                            match timeout(timeout_dur, tokio::task::spawn_blocking(move || {
                                task_function()
                            })).await {
                                Ok(Ok(result)) => result,
                                Ok(Err(e)) => Err(format!("Task panicked: {:?}", e).into()),
                                Err(_) => Err("Worker timeout".into()),
                            }
                        } else {
                            match tokio::task::spawn_blocking(move || {
                                task_function()
                            }).await {
                                Ok(result) => result,
                                Err(e) => Err(format!("Task panicked: {:?}", e).into()),
                            }
                        };

                        let execution_time = start_time.elapsed();
                        
                        // Update metrics and send result
                        let result_for_metrics = execution_result.is_ok();

                        // Send result back
                        if result_sender.send(execution_result).is_err() {
                            warn!("📤 Failed to send result for task {}", task_id);
                        }

                        // Update metrics
                        if result_for_metrics {
                            metrics_clone.record_task_completed(execution_time);
                            debug!("✅ Task {} completed in {:?}", task_id, execution_time);
                        } else {
                            metrics_clone.record_task_error();
                            error!("❌ Task {} failed", task_id);
                        }
                    });
                }
                Err(_) => {
                    // No workers available, break and try later
                    break;
                }
            }
        }
    }

    /// Clean up expired tasks
    async fn cleanup_expired_tasks(
        pending_tasks: &mut BinaryHeap<TaskWrapper>,
        metrics: &QueueMetrics,
    ) {
        let mut expired_tasks = Vec::new();
        
        // Extract expired tasks
        while let Some(task_wrapper) = pending_tasks.peek() {
            if task_wrapper.task.metadata.is_expired() {
                expired_tasks.push(pending_tasks.pop().unwrap());
            } else {
                break;
            }
        }

        // Notify expired tasks
        for expired_task in expired_tasks {
            warn!("🗑️ Cleaning up expired task {}", expired_task.task.metadata.id);
            let _ = expired_task.task.metadata.result_sender.send(
                Err("Task expired in queue".into())
            );
            metrics.record_task_timeout();
        }
    }

    /// Update queue statistics
    async fn update_stats(
        stats: &Arc<RwLock<QueueStats>>,
        queue_size: usize,
        metrics: &QueueMetrics,
    ) {
        let mut stats_guard = stats.write().await;
        let queue_metrics = metrics.get_metrics().await;
        
        stats_guard.queue_size = queue_size;
        stats_guard.total_processed = queue_metrics.tasks_completed;
        stats_guard.total_errors = queue_metrics.tasks_failed;
        stats_guard.average_wait_time_ms = queue_metrics.average_wait_time_ms;
        stats_guard.average_process_time_ms = queue_metrics.average_execution_time_ms;
        stats_guard.current_throughput_per_sec = queue_metrics.current_throughput_per_sec;
    }
}

/// Wrapper for priority queue ordering
#[derive(Debug)]
struct TaskWrapper {
    task: CpuTask,
    priority_key: (Priority, Reverse<u64>),
}

impl TaskWrapper {
    fn new(task: CpuTask) -> Self {
        let priority_key = (task.metadata.priority, Reverse(task.metadata.id));
        Self { task, priority_key }
    }
}

impl PartialEq for TaskWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.priority_key == other.priority_key
    }
}

impl Eq for TaskWrapper {}

impl PartialOrd for TaskWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskWrapper {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority_key.cmp(&other.priority_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_basic_task_execution() {
        let config = QueueConfig {
            max_workers: 2,
            ..Default::default()
        };
        let queue = TaskQueue::new(config);

        let result = queue.submit_blocking(
            Box::new(|| Ok(42)),
            Priority::Normal,
            Some(Duration::from_secs(1)),
        ).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let config = QueueConfig {
            max_workers: 1, // Force serialization
            ..Default::default()
        };
        let queue = TaskQueue::new(config);
        let execution_order = Arc::new(std::sync::Mutex::new(Vec::new()));

        let mut handles = Vec::new();

        // Submit tasks in reverse priority order
        for (i, priority) in [(0, Priority::Low), (1, Priority::High), (2, Priority::Critical)].iter() {
            let order = execution_order.clone();
            let task_id = *i;
            
            let handle = tokio::spawn({
                let queue = &queue;
                async move {
                    queue.submit_blocking(
                        Box::new(move || {
                            let mut order = order.lock().unwrap();
                            order.push(task_id);
                            Ok(())
                        }),
                        *priority,
                        Some(Duration::from_secs(5)),
                    ).await
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            let _ = handle.await;
        }

        let final_order = execution_order.lock().unwrap();
        // Higher priority tasks should execute first: Critical(2), High(1), Low(0)
        assert_eq!(*final_order, vec![2, 1, 0]);
    }

    #[tokio::test]
    async fn test_timeout_handling() {
        let config = QueueConfig {
            max_workers: 1,
            worker_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let queue = TaskQueue::new(config);

        let result = queue.submit_blocking(
            Box::new(|| {
                std::thread::sleep(std::time::Duration::from_millis(200));
                Ok(())
            }),
            Priority::Normal,
            Some(Duration::from_millis(50)),
        ).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_execution() {
        let config = QueueConfig {
            max_workers: 4,
            ..Default::default()
        };
        let queue = TaskQueue::new(config);
        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let counter = counter.clone();
            let handle = tokio::spawn({
                let queue = &queue;
                async move {
                    queue.submit_blocking(
                        Box::new(move || {
                            counter.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }),
                        Priority::Normal,
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
}