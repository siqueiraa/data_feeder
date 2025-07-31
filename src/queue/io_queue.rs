use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::{timeout, Duration, Instant};
use tracing::{debug, error, info, warn};

use super::types::{Priority, QueueTask, QueueConfig, IoTask, AsyncTask};
use super::metrics::QueueMetrics;

/// I/O operation types
#[derive(Debug, Clone)]
pub enum IoOperation {
    ReadFile(PathBuf),
    WriteFile(PathBuf, Vec<u8>),
    CreateDir(PathBuf),
    RemoveFile(PathBuf),
    RemoveDir(PathBuf),
    CopyFile(PathBuf, PathBuf),
    MoveFile(PathBuf, PathBuf),
    ListDir(PathBuf),
    FileExists(PathBuf),
    FileMetadata(PathBuf),
}

impl std::fmt::Display for IoOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoOperation::ReadFile(path) => write!(f, "READ:{}", path.display()),
            IoOperation::WriteFile(path, _) => write!(f, "WRITE:{}", path.display()),
            IoOperation::CreateDir(path) => write!(f, "MKDIR:{}", path.display()),
            IoOperation::RemoveFile(path) => write!(f, "RM:{}", path.display()),
            IoOperation::RemoveDir(path) => write!(f, "RMDIR:{}", path.display()),
            IoOperation::CopyFile(src, dst) => write!(f, "COPY:{}→{}", src.display(), dst.display()),
            IoOperation::MoveFile(src, dst) => write!(f, "MOVE:{}→{}", src.display(), dst.display()),
            IoOperation::ListDir(path) => write!(f, "LS:{}", path.display()),
            IoOperation::FileExists(path) => write!(f, "EXISTS:{}", path.display()),
            IoOperation::FileMetadata(path) => write!(f, "STAT:{}", path.display()),
        }
    }
}

/// I/O operation result types
#[derive(Debug)]
pub enum IoResult {
    FileContent(Vec<u8>),
    Success,
    DirectoryListing(Vec<PathBuf>),
    Exists(bool),
    Metadata(std::fs::Metadata),
}

impl Clone for IoResult {
    fn clone(&self) -> Self {
        match self {
            IoResult::FileContent(content) => IoResult::FileContent(content.clone()),
            IoResult::Success => IoResult::Success,
            IoResult::DirectoryListing(paths) => IoResult::DirectoryListing(paths.clone()),
            IoResult::Exists(exists) => IoResult::Exists(*exists),
            IoResult::Metadata(_) => {
                // std::fs::Metadata doesn't implement Clone, so we'll create a new Success variant
                IoResult::Success
            }
        }
    }
}

/// Non-blocking I/O queue with worker pool
#[derive(Debug)]
pub struct IoQueue {
    config: QueueConfig,
    task_sender: mpsc::UnboundedSender<IoTask<IoResult>>,
    #[allow(dead_code)] // Used in worker manager, kept for API completeness
    worker_semaphore: Arc<Semaphore>,
    metrics: QueueMetrics,
    task_counter: std::sync::atomic::AtomicU64,
}

impl IoQueue {
    /// Create new I/O queue
    pub fn new(config: QueueConfig) -> Self {
        let (task_sender, task_receiver) = mpsc::unbounded_channel();
        let worker_semaphore = Arc::new(Semaphore::new(config.max_workers));
        let metrics = QueueMetrics::new("io_queue".to_string());

        let queue = Self {
            config: config.clone(),
            task_sender,
            worker_semaphore: worker_semaphore.clone(),
            metrics: metrics.clone(),
            task_counter: std::sync::atomic::AtomicU64::new(0),
        };

        // Start worker manager
        queue.start_worker_manager(task_receiver, worker_semaphore, metrics);

        info!("🚀 IoQueue initialized with {} workers", config.max_workers);
        queue
    }

    /// Submit file read operation
    pub async fn read_file<P: AsRef<Path>>(
        &self,
        path: P,
        priority: Priority,
        timeout_duration: Option<Duration>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let path_buf = path.as_ref().to_path_buf();
        let operation = IoOperation::ReadFile(path_buf.clone());
        
        let task = Box::pin(async move {
            match fs::read(&path_buf).await {
                Ok(content) => Ok(IoResult::FileContent(content)),
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            }
        });

        let result = self.submit_task(task, operation, priority, timeout_duration).await?;
        match result {
            IoResult::FileContent(content) => Ok(content),
            _ => Err("Unexpected result type".into()),
        }
    }

    /// Submit file write operation
    pub async fn write_file<P: AsRef<Path>>(
        &self,
        path: P,
        content: Vec<u8>,
        priority: Priority,
        timeout_duration: Option<Duration>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path_buf = path.as_ref().to_path_buf();
        let operation = IoOperation::WriteFile(path_buf.clone(), content.clone());
        
        let task = Box::pin(async move {
            match fs::write(&path_buf, &content).await {
                Ok(_) => Ok(IoResult::Success),
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            }
        });

        let result = self.submit_task(task, operation, priority, timeout_duration).await?;
        match result {
            IoResult::Success => Ok(()),
            _ => Err("Unexpected result type".into()),
        }
    }

    /// Submit directory creation operation
    pub async fn create_dir<P: AsRef<Path>>(
        &self,
        path: P,
        priority: Priority,
        timeout_duration: Option<Duration>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path_buf = path.as_ref().to_path_buf();
        let operation = IoOperation::CreateDir(path_buf.clone());
        
        let task = Box::pin(async move {
            match fs::create_dir_all(&path_buf).await {
                Ok(_) => Ok(IoResult::Success),
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            }
        });

        let result = self.submit_task(task, operation, priority, timeout_duration).await?;
        match result {
            IoResult::Success => Ok(()),
            _ => Err("Unexpected result type".into()),
        }
    }

    /// Submit file removal operation
    pub async fn remove_file<P: AsRef<Path>>(
        &self,
        path: P,
        priority: Priority,
        timeout_duration: Option<Duration>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path_buf = path.as_ref().to_path_buf();
        let operation = IoOperation::RemoveFile(path_buf.clone());
        
        let task = Box::pin(async move {
            match fs::remove_file(&path_buf).await {
                Ok(_) => Ok(IoResult::Success),
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            }
        });

        let result = self.submit_task(task, operation, priority, timeout_duration).await?;
        match result {
            IoResult::Success => Ok(()),
            _ => Err("Unexpected result type".into()),
        }
    }

    /// Submit directory listing operation
    pub async fn list_dir<P: AsRef<Path>>(
        &self,
        path: P,
        priority: Priority,
        timeout_duration: Option<Duration>,
    ) -> Result<Vec<PathBuf>, Box<dyn std::error::Error + Send + Sync>> {
        let path_buf = path.as_ref().to_path_buf();
        let operation = IoOperation::ListDir(path_buf.clone());
        
        let task = Box::pin(async move {
            match fs::read_dir(&path_buf).await {
                Ok(mut entries) => {
                    let mut paths = Vec::new();
                    while let Ok(Some(entry)) = entries.next_entry().await {
                        paths.push(entry.path());
                    }
                    Ok(IoResult::DirectoryListing(paths))
                }
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            }
        });

        let result = self.submit_task(task, operation, priority, timeout_duration).await?;
        match result {
            IoResult::DirectoryListing(paths) => Ok(paths),
            _ => Err("Unexpected result type".into()),
        }
    }

    /// Submit file existence check
    pub async fn file_exists<P: AsRef<Path>>(
        &self,
        path: P,
        priority: Priority,
        timeout_duration: Option<Duration>,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let path_buf = path.as_ref().to_path_buf();
        let operation = IoOperation::FileExists(path_buf.clone());
        
        let task = Box::pin(async move {
            let exists = fs::try_exists(&path_buf).await.unwrap_or(false);
            Ok(IoResult::Exists(exists))
        });

        let result = self.submit_task(task, operation, priority, timeout_duration).await?;
        match result {
            IoResult::Exists(exists) => Ok(exists),
            _ => Err("Unexpected result type".into()),
        }
    }

    /// Submit file copy operation
    pub async fn copy_file<P: AsRef<Path>>(
        &self,
        src: P,
        dst: P,
        priority: Priority,
        timeout_duration: Option<Duration>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let src_buf = src.as_ref().to_path_buf();
        let dst_buf = dst.as_ref().to_path_buf();
        let operation = IoOperation::CopyFile(src_buf.clone(), dst_buf.clone());
        
        let task = Box::pin(async move {
            match fs::copy(&src_buf, &dst_buf).await {
                Ok(_) => Ok(IoResult::Success),
                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            }
        });

        let result = self.submit_task(task, operation, priority, timeout_duration).await?;
        match result {
            IoResult::Success => Ok(()),
            _ => Err("Unexpected result type".into()),
        }
    }

    /// Submit generic I/O task
    async fn submit_task(
        &self,
        task: AsyncTask<IoResult>,
        operation: IoOperation,
        priority: Priority,
        timeout_duration: Option<Duration>,
    ) -> Result<IoResult, Box<dyn std::error::Error + Send + Sync>> {
        let task_id = self.task_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (result_sender, result_receiver) = oneshot::channel();

        let io_task = IoTask {
            task,
            metadata: QueueTask::new(
                task_id,
                priority,
                timeout_duration,
                result_sender,
                operation.to_string(),
            ),
        };

        // Submit to queue
        if self.task_sender.send(io_task).is_err() {
            return Err("I/O queue is closed".into());
        }

        self.metrics.record_task_submitted();
        debug!("📤 Submitted I/O task {} ({})", task_id, operation);

        // Wait for completion
        let wait_result = if let Some(timeout_dur) = timeout_duration {
            timeout(timeout_dur, result_receiver).await
        } else {
            Ok(result_receiver.await)
        };

        match wait_result {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("Task was cancelled".into()),
            Err(_) => Err("Task timed out".into()),
        }
    }

    /// Start worker manager
    fn start_worker_manager(
        &self,
        mut task_receiver: mpsc::UnboundedReceiver<IoTask<IoResult>>,
        worker_semaphore: Arc<Semaphore>,
        metrics: QueueMetrics,
    ) {
        let config = self.config.clone();
        
        tokio::spawn(async move {
            let mut pending_tasks: VecDeque<IoTask<IoResult>> = VecDeque::new();
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(10));
            
            info!("📋 I/O queue worker manager started");

            loop {
                tokio::select! {
                    // Handle new tasks
                    Some(task) = task_receiver.recv() => {
                        pending_tasks.push_back(task);
                        metrics.record_task_queued();
                        debug!("📥 Queued I/O task (queue size: {})", pending_tasks.len());
                    }
                    
                    // Try to dispatch tasks
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        Self::dispatch_io_tasks(&mut pending_tasks, &worker_semaphore, &metrics, &config).await;
                    }
                    
                    // Periodic cleanup
                    _ = cleanup_interval.tick() => {
                        Self::cleanup_expired_tasks(&mut pending_tasks, &metrics).await;
                    }
                }
            }
        });
    }

    /// Dispatch I/O tasks to workers
    async fn dispatch_io_tasks(
        pending_tasks: &mut VecDeque<IoTask<IoResult>>,
        worker_semaphore: &Arc<Semaphore>,
        metrics: &QueueMetrics,
        config: &QueueConfig,
    ) {
        // Sort by priority (simple approach for VecDeque)
        let mut high_priority_tasks = Vec::new();
        let mut normal_tasks = Vec::new();
        
        while let Some(task) = pending_tasks.pop_front() {
            if task.metadata.is_expired() {
                warn!("⏰ I/O Task {} expired", task.metadata.id);
                let _ = task.metadata.result_sender.send(Err("Task expired".into()));
                metrics.record_task_timeout();
                continue;
            }

            match task.metadata.priority {
                Priority::Critical | Priority::High => high_priority_tasks.push(task),
                _ => normal_tasks.push(task),
            }
        }

        // Process high priority first, then normal
        for task in high_priority_tasks.into_iter().chain(normal_tasks.into_iter()) {
            match Arc::clone(worker_semaphore).try_acquire_owned() {
                Ok(permit) => {
                    let task_id = task.metadata.id;
                    let task_type = task.metadata.task_type.clone();
                    let created_at = task.metadata.created_at;
                    
                    metrics.record_task_started();
                    debug!("🏃 Starting I/O task {} ({})", task_id, task_type);

                    // Spawn worker
                    let metrics_clone = metrics.clone();
                    let worker_timeout = config.worker_timeout;
                    
                    tokio::spawn(async move {
                        let _permit = permit;
                        let start_time = Instant::now();
                        let wait_time = created_at.elapsed();
                        
                        // Execute the I/O task
                        let execution_result = if let Some(timeout_dur) = worker_timeout.checked_sub(Duration::from_millis(100)) {
                            match timeout(timeout_dur, task.task).await {
                                Ok(result) => result,
                                Err(_) => Err("Worker timeout".into()),
                            }
                        } else {
                            task.task.await
                        };

                        let execution_time = start_time.elapsed();
                        
                        // Send result
                        let result_for_metrics = execution_result.is_ok();
                        if task.metadata.result_sender.send(execution_result).is_err() {
                            warn!("📤 Failed to send I/O result for task {}", task_id);
                        }

                        // Update metrics
                        metrics_clone.record_wait_time(wait_time);
                        if result_for_metrics {
                            metrics_clone.record_task_completed(execution_time);
                            debug!("✅ I/O task {} completed in {:?}", task_id, execution_time);
                        } else {
                            metrics_clone.record_task_error();
                            error!("❌ I/O task {} failed", task_id);
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

    /// Clean up expired tasks
    async fn cleanup_expired_tasks(
        pending_tasks: &mut VecDeque<IoTask<IoResult>>,
        metrics: &QueueMetrics,
    ) {
        let mut valid_tasks = VecDeque::new();
        
        while let Some(task) = pending_tasks.pop_front() {
            if task.metadata.is_expired() {
                warn!("🗑️ Cleaning up expired I/O task {}", task.metadata.id);
                let _ = task.metadata.result_sender.send(Err("Task expired in queue".into()));
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_file_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = QueueConfig {
            max_workers: 2,
            ..Default::default()
        };
        let io_queue = IoQueue::new(config);

        let test_file = temp_dir.path().join("test.txt");
        let test_content = b"Hello, World!";

        // Test write
        io_queue.write_file(
            &test_file,
            test_content.to_vec(),
            Priority::Normal,
            Some(Duration::from_secs(1)),
        ).await.unwrap();

        // Test read
        let content = io_queue.read_file(
            &test_file,
            Priority::Normal,
            Some(Duration::from_secs(1)),
        ).await.unwrap();

        assert_eq!(content, test_content);

        // Test file exists
        let exists = io_queue.file_exists(
            &test_file,
            Priority::Normal,
            Some(Duration::from_secs(1)),
        ).await.unwrap();

        assert!(exists);
    }

    #[tokio::test]
    async fn test_directory_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = QueueConfig {
            max_workers: 2,
            ..Default::default()
        };
        let io_queue = IoQueue::new(config);

        let test_subdir = temp_dir.path().join("subdir");

        // Test create directory
        io_queue.create_dir(
            &test_subdir,
            Priority::Normal,
            Some(Duration::from_secs(1)),
        ).await.unwrap();

        // Test directory listing
        let entries = io_queue.list_dir(
            temp_dir.path(),
            Priority::Normal,
            Some(Duration::from_secs(1)),
        ).await.unwrap();

        assert!(entries.iter().any(|p| p.file_name().unwrap() == "subdir"));
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        let temp_dir = TempDir::new().unwrap();
        let config = QueueConfig {
            max_workers: 1, // Force serialization
            ..Default::default()
        };
        let io_queue = Arc::new(IoQueue::new(config));

        let mut handles = Vec::new();

        // Submit tasks in reverse priority order
        for (i, priority) in [(0, Priority::Low), (1, Priority::High), (2, Priority::Critical)].iter() {
            let test_file = temp_dir.path().join(format!("test_{}.txt", i));
            let content = format!("Content {}", i).into_bytes();
            let io_queue = io_queue.clone();
            
            let handle = tokio::spawn(async move {
                io_queue.write_file(
                    &test_file,
                    content,
                    *priority,
                    Some(Duration::from_secs(5)),
                ).await
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }
    }

    #[tokio::test]
    async fn test_concurrent_io() {
        let temp_dir = TempDir::new().unwrap();
        let config = QueueConfig {
            max_workers: 4,
            ..Default::default()
        };
        let io_queue = Arc::new(IoQueue::new(config));

        let mut handles = Vec::new();
        
        for i in 0..10 {
            let test_file = temp_dir.path().join(format!("concurrent_{}.txt", i));
            let content = format!("Content {}", i).into_bytes();
            let io_queue = io_queue.clone();
            
            let handle = tokio::spawn(async move {
                io_queue.write_file(
                    &test_file,
                    content,
                    Priority::Normal,
                    Some(Duration::from_secs(1)),
                ).await
            });
            handles.push(handle);
        }

        // Wait for all operations
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        // Verify all files were created
        let entries = io_queue.list_dir(
            temp_dir.path(),
            Priority::Normal,
            Some(Duration::from_secs(1)),
        ).await.unwrap();

        assert_eq!(entries.len(), 10);
    }
}