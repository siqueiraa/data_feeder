use std::fmt;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot;
use serde::{Serialize, Deserialize};

/// Task priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum Priority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Priority::Low => write!(f, "LOW"),
            Priority::Normal => write!(f, "NORMAL"),
            Priority::High => write!(f, "HIGH"),
            Priority::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Generic task result type
pub type TaskResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Future type for async tasks
pub type AsyncTask<T> = Pin<Box<dyn Future<Output = TaskResult<T>> + Send + 'static>>;

/// Blocking task closure type
pub type BlockingTask<T> = Box<dyn FnOnce() -> TaskResult<T> + Send + 'static>;

/// Generic queue task wrapper
#[derive(Debug)]
pub struct QueueTask<T> {
    pub id: u64,
    pub priority: Priority,
    pub created_at: std::time::Instant,
    pub timeout: Option<std::time::Duration>,
    pub result_sender: oneshot::Sender<TaskResult<T>>,
    pub task_type: String,
}

impl<T> QueueTask<T> {
    pub fn new(
        id: u64,
        priority: Priority,
        timeout: Option<std::time::Duration>,
        result_sender: oneshot::Sender<TaskResult<T>>,
        task_type: String,
    ) -> Self {
        Self {
            id,
            priority,
            created_at: std::time::Instant::now(),
            timeout,
            result_sender,
            task_type,
        }
    }

    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    pub fn is_expired(&self) -> bool {
        if let Some(timeout) = self.timeout {
            self.age() > timeout
        } else {
            false
        }
    }
}

/// CPU-intensive task wrapper
pub struct CpuTask {
    pub task: BlockingTask<()>,
    pub metadata: QueueTask<()>,
}

impl fmt::Debug for CpuTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CpuTask")
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// I/O operation task wrapper  
pub struct IoTask<T> {
    pub task: AsyncTask<T>,
    pub metadata: QueueTask<T>,
}

impl<T: std::fmt::Debug> fmt::Debug for IoTask<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoTask")
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Database operation types
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DbOperation {
    Store,
    Retrieve,
    Update,
    Delete,
    Batch,
}

impl fmt::Display for DbOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbOperation::Store => write!(f, "STORE"),
            DbOperation::Retrieve => write!(f, "RETRIEVE"),
            DbOperation::Update => write!(f, "UPDATE"),
            DbOperation::Delete => write!(f, "DELETE"),
            DbOperation::Batch => write!(f, "BATCH"),
        }
    }
}

/// Database task wrapper
pub struct DbTask<T> {
    pub operation: DbOperation,
    pub task: AsyncTask<T>,
    pub metadata: QueueTask<T>,
}

impl<T: std::fmt::Debug> fmt::Debug for DbTask<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbTask")
            .field("operation", &self.operation)
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Queue configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub max_size: usize,
    pub max_workers: usize,
    pub worker_timeout: std::time::Duration,
    pub queue_timeout: std::time::Duration,
    pub batch_size: usize,
    pub batch_timeout: std::time::Duration,
    pub enable_metrics: bool,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_size: 10000,
            max_workers: num_cpus::get(),
            worker_timeout: std::time::Duration::from_secs(30),
            queue_timeout: std::time::Duration::from_secs(60),
            batch_size: 100,
            batch_timeout: std::time::Duration::from_millis(100),
            enable_metrics: true,
        }
    }
}

/// Queue statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub queue_size: usize,
    pub active_workers: usize,
    pub total_processed: u64,
    pub total_errors: u64,
    pub average_wait_time_ms: f64,
    pub average_process_time_ms: f64,
    pub current_throughput_per_sec: f64,
}

impl Default for QueueStats {
    fn default() -> Self {
        Self {
            queue_size: 0,
            active_workers: 0,
            total_processed: 0,
            total_errors: 0,
            average_wait_time_ms: 0.0,
            average_process_time_ms: 0.0,
            current_throughput_per_sec: 0.0,
        }
    }
}