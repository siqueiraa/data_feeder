//! Concurrency Optimization Module
//! 
//! This module provides advanced concurrency optimizations including lock-free algorithms,
//! actor system optimization, and intelligent work distribution.
//! Enhanced for Story 6.2 to achieve 15-25% concurrency performance improvements.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Lock-free circular buffer for high-performance concurrent access
pub struct LockFreeRingBuffer<T> {
    buffer: Vec<AtomicOption<T>>,
    capacity: usize,
    write_pos: AtomicUsize,
    read_pos: AtomicUsize,
    size: AtomicUsize,
}

/// True atomic option wrapper for lock-free data structures
/// Uses atomic pointers for zero-lock operations
struct AtomicOption<T> {
    inner: AtomicPtr<T>,
}

impl<T> AtomicOption<T> {
    fn new() -> Self {
        Self {
            inner: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    fn take(&self) -> Option<T> {
        let ptr = self.inner.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if ptr.is_null() {
            None
        } else {
            unsafe { Some(*Box::from_raw(ptr)) }
        }
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.inner.load(Ordering::Acquire).is_null()
    }
}

impl<T> Drop for AtomicOption<T> {
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::Acquire);
        if !ptr.is_null() {
            unsafe { drop(Box::from_raw(ptr)); }
        }
    }
}

impl<T: Send + Sync + 'static> LockFreeRingBuffer<T> {
    /// Create a new lock-free ring buffer
    pub fn new(capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(AtomicOption::new());
        }

        Self {
            buffer,
            capacity,
            write_pos: AtomicUsize::new(0),
            read_pos: AtomicUsize::new(0),
            size: AtomicUsize::new(0),
        }
    }

    /// Try to push an item (non-blocking)
    pub fn try_push(&self, item: T) -> Result<(), T> {
        // Optimistic fast path: check size with relaxed ordering first
        let current_size = self.size.load(Ordering::Relaxed);
        if current_size >= self.capacity {
            return Err(item);
        }

        let write_pos = self.write_pos.fetch_add(1, Ordering::Relaxed) % self.capacity;
        
        // Try to store the item, if slot is occupied, we'll get false and need to return the item
        let new_ptr = Box::into_raw(Box::new(item));
        let old_ptr = self.buffer[write_pos].inner.compare_exchange(
            std::ptr::null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        
        match old_ptr {
            Ok(_) => {
                // Successfully stored, increment size
                self.size.fetch_add(1, Ordering::Release);
                Ok(())
            }
            Err(_) => {
                // Slot was not empty, buffer is full - clean up and return error
                let item = unsafe { *Box::from_raw(new_ptr) };
                Err(item)
            }
        }
    }

    /// Try to pop an item (non-blocking)
    pub fn try_pop(&self) -> Option<T> {
        // Optimistic fast path: check size with relaxed ordering first
        let current_size = self.size.load(Ordering::Relaxed);
        if current_size == 0 {
            return None;
        }

        let read_pos = self.read_pos.fetch_add(1, Ordering::Relaxed) % self.capacity;
        
        if let Some(item) = self.buffer[read_pos].take() {
            // Only use acquire-release ordering when actually modifying size
            self.size.fetch_sub(1, Ordering::Release);
            Some(item)
        } else {
            None
        }
    }

    /// Get current buffer size
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Acquire)
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get buffer capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Type alias for the complex queue structure
type QueueContainer<T> = Arc<RwLock<Vec<Arc<RwLock<VecDeque<T>>>>>>;

/// Work-stealing queue for efficient task distribution
pub struct WorkStealingQueue<T> {
    local_queue: Arc<RwLock<VecDeque<T>>>,
    steal_queues: QueueContainer<T>,
    stats: WorkStealingStats,
}

#[derive(Debug)]
pub struct WorkStealingStats {
    pub local_pushes: AtomicU64,
    pub local_pops: AtomicU64,
    pub steals_attempted: AtomicU64,
    pub steals_successful: AtomicU64,
    pub work_items_processed: AtomicU64,
}

impl WorkStealingStats {
    fn new() -> Self {
        Self {
            local_pushes: AtomicU64::new(0),
            local_pops: AtomicU64::new(0),
            steals_attempted: AtomicU64::new(0),
            steals_successful: AtomicU64::new(0),
            work_items_processed: AtomicU64::new(0),
        }
    }

    /// Calculate steal success rate
    pub fn steal_success_rate(&self) -> f64 {
        let attempted = self.steals_attempted.load(Ordering::Relaxed);
        let successful = self.steals_successful.load(Ordering::Relaxed);
        
        if attempted == 0 {
            0.0
        } else {
            successful as f64 / attempted as f64
        }
    }
}

impl<T: Send + Sync + 'static> Default for WorkStealingQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync + 'static> WorkStealingQueue<T> {
    /// Create a new work-stealing queue
    pub fn new() -> Self {
        Self {
            local_queue: Arc::new(RwLock::new(VecDeque::new())),
            steal_queues: Arc::new(RwLock::new(Vec::new())),
            stats: WorkStealingStats::new(),
        }
    }

    /// Register another queue for stealing
    pub async fn register_steal_target(&self, target: Arc<RwLock<VecDeque<T>>>) {
        let mut queues = self.steal_queues.write().await;
        queues.push(target);
        debug!("Registered new steal target, total queues: {}", queues.len());
    }

    /// Push work to local queue
    pub async fn push(&self, item: T) {
        let mut queue = self.local_queue.write().await;
        queue.push_back(item);
        self.stats.local_pushes.fetch_add(1, Ordering::Relaxed);
        debug!("Pushed work item to local queue, size: {}", queue.len());
    }

    /// Try to get work (local first, then steal)
    pub async fn try_pop(&self) -> Option<T> {
        // Try local queue first
        {
            let mut local = self.local_queue.write().await;
            if let Some(item) = local.pop_front() {
                self.stats.local_pops.fetch_add(1, Ordering::Relaxed);
                self.stats.work_items_processed.fetch_add(1, Ordering::Relaxed);
                return Some(item);
            }
        }

        // Try stealing from other queues
        self.try_steal().await
    }

    /// Attempt to steal work from other queues
    async fn try_steal(&self) -> Option<T> {
        let steal_queues = self.steal_queues.read().await;
        
        for target_queue in steal_queues.iter() {
            self.stats.steals_attempted.fetch_add(1, Ordering::Relaxed);
            
            let mut target = target_queue.write().await;
            if let Some(item) = target.pop_back() { // Steal from back to avoid contention
                self.stats.steals_successful.fetch_add(1, Ordering::Relaxed);
                self.stats.work_items_processed.fetch_add(1, Ordering::Relaxed);
                debug!("Successfully stole work item");
                return Some(item);
            }
        }

        None
    }

    /// Get work-stealing statistics
    pub fn get_stats(&self) -> &WorkStealingStats {
        &self.stats
    }
}

/// Optimized actor system with intelligent message batching
pub struct OptimizedActor<M, S> {
    state: Arc<RwLock<S>>,
    mailbox: Arc<LockFreeRingBuffer<M>>,
    batch_processor: BatchProcessor<M>,
    metrics: ActorMetrics,
    shutdown: Arc<AtomicBool>,
}

/// Batch processor for efficient message handling
pub struct BatchProcessor<M> {
    batch_size: usize,
    batch_timeout: Duration,
    pending_batch: Arc<RwLock<Vec<M>>>,
    last_batch_time: Arc<RwLock<Instant>>,
}

#[derive(Debug)]
pub struct ActorMetrics {
    pub messages_processed: AtomicU64,
    pub batches_processed: AtomicU64,
    pub avg_batch_size: AtomicU64,
    pub processing_time_ns: AtomicU64,
    pub mailbox_overflows: AtomicU64,
}

impl ActorMetrics {
    fn new() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            batches_processed: AtomicU64::new(0),
            avg_batch_size: AtomicU64::new(0),
            processing_time_ns: AtomicU64::new(0),
            mailbox_overflows: AtomicU64::new(0),
        }
    }

    /// Calculate average processing time per message
    pub fn avg_processing_time_ns(&self) -> f64 {
        let total_time = self.processing_time_ns.load(Ordering::Relaxed);
        let total_messages = self.messages_processed.load(Ordering::Relaxed);
        
        if total_messages == 0 {
            0.0
        } else {
            total_time as f64 / total_messages as f64
        }
    }

    /// Calculate messages per second throughput
    pub fn messages_per_second(&self, elapsed: Duration) -> f64 {
        let messages = self.messages_processed.load(Ordering::Relaxed);
        let seconds = elapsed.as_secs_f64();
        
        if seconds > 0.0 {
            messages as f64 / seconds
        } else {
            0.0
        }
    }
}

impl<M> BatchProcessor<M>
where
    M: Send + Sync + 'static,
{
    /// Create a new batch processor
    pub fn new(batch_size: usize, batch_timeout: Duration) -> Self {
        Self {
            batch_size,
            batch_timeout,
            pending_batch: Arc::new(RwLock::new(Vec::with_capacity(batch_size))),
            last_batch_time: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Add message to batch
    pub async fn add_to_batch(&self, message: M) -> Option<Vec<M>> {
        let mut batch = self.pending_batch.write().await;
        batch.push(message);
        
        let should_flush = batch.len() >= self.batch_size ||
                          self.last_batch_time.read().await.elapsed() >= self.batch_timeout;
        
        if should_flush {
            let ready_batch = std::mem::take(&mut *batch);
            *self.last_batch_time.write().await = Instant::now();
            Some(ready_batch)
        } else {
            None
        }
    }

    /// Force flush current batch
    pub async fn flush_batch(&self) -> Vec<M> {
        let mut batch = self.pending_batch.write().await;
        let ready_batch = std::mem::take(&mut *batch);
        *self.last_batch_time.write().await = Instant::now();
        ready_batch
    }
}

impl<M, S> OptimizedActor<M, S>
where
    M: Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    /// Create a new optimized actor
    pub fn new(
        initial_state: S,
        mailbox_capacity: usize,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(initial_state)),
            mailbox: Arc::new(LockFreeRingBuffer::new(mailbox_capacity)),
            batch_processor: BatchProcessor::new(batch_size, batch_timeout),
            metrics: ActorMetrics::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Send message to actor (non-blocking)
    pub fn try_send(&self, message: M) -> Result<(), M> {
        match self.mailbox.try_push(message) {
            Ok(()) => Ok(()),
            Err(message) => {
                self.metrics.mailbox_overflows.fetch_add(1, Ordering::Relaxed);
                warn!("Actor mailbox overflow, dropping message");
                Err(message)
            }
        }
    }

    /// Start actor processing loop
    pub async fn run<F, Fut>(&self, mut handler: F)
    where
        F: FnMut(&[M], Arc<RwLock<S>>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        info!("Starting optimized actor processing loop");
        
        while !self.shutdown.load(Ordering::Relaxed) {
            // Process messages in batches
            let mut batch = Vec::with_capacity(self.batch_processor.batch_size);
            
            // Collect messages for batch
            while batch.len() < self.batch_processor.batch_size {
                if let Some(message) = self.mailbox.try_pop() {
                    batch.push(message);
                } else {
                    break;
                }
            }

            // Process batch if we have messages
            if !batch.is_empty() {
                let processing_start = Instant::now();
                handler(&batch, self.state.clone()).await;
                let processing_time = processing_start.elapsed();
                
                // Update metrics
                let batch_size = batch.len();
                self.metrics.messages_processed.fetch_add(batch_size as u64, Ordering::Relaxed);
                self.metrics.batches_processed.fetch_add(1, Ordering::Relaxed);
                self.metrics.processing_time_ns.fetch_add(
                    processing_time.as_nanos() as u64,
                    Ordering::Relaxed,
                );
                
                // Update average batch size
                let current_avg = self.metrics.avg_batch_size.load(Ordering::Relaxed);
                let batches = self.metrics.batches_processed.load(Ordering::Relaxed);
                let new_avg = ((current_avg * (batches - 1)) + batch_size as u64) / batches;
                self.metrics.avg_batch_size.store(new_avg, Ordering::Relaxed);
                
                debug!(
                    "Processed batch of {} messages in {:?}",
                    batch_size, processing_time
                );
            } else {
                // No messages, yield to other tasks without sleeping to reduce context switches
                tokio::task::yield_now().await;
            }
        }
        
        info!("Actor processing loop shutdown");
    }

    /// Shutdown the actor
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        info!("Actor shutdown requested");
    }

    /// Get actor metrics
    pub fn get_metrics(&self) -> &ActorMetrics {
        &self.metrics
    }

    /// Get current mailbox size
    pub fn mailbox_size(&self) -> usize {
        self.mailbox.len()
    }
}

/// Advanced work distribution system
pub struct WorkDistributor<T> {
    worker_pools: Vec<WorkerPool<T>>,
    load_balancer: LoadBalancer,
    global_stats: GlobalWorkStats,
}

/// Worker pool with adaptive scaling
pub struct WorkerPool<T> {
    _workers: Vec<Worker<T>>,
    work_queue: Arc<WorkStealingQueue<T>>,
    pool_id: usize,
    active_workers: AtomicUsize,
    max_workers: usize,
    min_workers: usize,
    scale_metrics: ScaleMetrics,
}

/// Individual worker thread
pub struct Worker<T> {
    _id: usize,
    _work_queue: Arc<WorkStealingQueue<T>>,
    is_active: Arc<AtomicBool>,
    processed_count: AtomicU64,
}

/// Load balancer for work distribution
pub struct LoadBalancer {
    strategy: LoadBalancingStrategy,
    pool_loads: Vec<AtomicU64>,
}

#[derive(Debug, Clone)]
pub enum LoadBalancingStrategy {
    RoundRobin,
    LeastLoaded,
    WeightedRandom { weights: Vec<f64> },
    AdaptiveHashing,
}

/// Scaling metrics for adaptive worker pools
#[derive(Debug)]
pub struct ScaleMetrics {
    pub queue_length_history: RwLock<VecDeque<usize>>,
    pub throughput_history: RwLock<VecDeque<f64>>,
    pub last_scale_event: RwLock<Instant>,
    pub scale_cooldown: Duration,
}

/// Global work distribution statistics
#[derive(Debug)]
pub struct GlobalWorkStats {
    pub total_work_items: AtomicU64,
    pub completed_work_items: AtomicU64,
    pub failed_work_items: AtomicU64,
    pub avg_completion_time_ns: AtomicU64,
    pub active_workers: AtomicUsize,
    pub total_workers: AtomicUsize,
}

impl LoadBalancer {
    /// Create a new load balancer
    pub fn new(strategy: LoadBalancingStrategy, num_pools: usize) -> Self {
        let mut pool_loads = Vec::with_capacity(num_pools);
        for _ in 0..num_pools {
            pool_loads.push(AtomicU64::new(0));
        }

        Self {
            strategy,
            pool_loads,
        }
    }

    /// Select optimal pool for work distribution
    pub fn select_pool(&self, work_hint: Option<u64>) -> usize {
        match &self.strategy {
            LoadBalancingStrategy::RoundRobin => {
                // Simple round-robin based on total work distributed
                let total_work: u64 = self.pool_loads.iter()
                    .map(|load| load.load(Ordering::Relaxed))
                    .sum();
                (total_work as usize) % self.pool_loads.len()
            }
            LoadBalancingStrategy::LeastLoaded => {
                // Select pool with minimum current load
                let mut min_load = u64::MAX;
                let mut min_index = 0;
                
                for (i, load) in self.pool_loads.iter().enumerate() {
                    let current_load = load.load(Ordering::Relaxed);
                    if current_load < min_load {
                        min_load = current_load;
                        min_index = i;
                    }
                }
                min_index
            }
            LoadBalancingStrategy::WeightedRandom { weights } => {
                // Weighted random selection
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                
                let mut hasher = DefaultHasher::new();
                work_hint.unwrap_or(0).hash(&mut hasher);
                let hash = hasher.finish();
                
                let total_weight: f64 = weights.iter().sum();
                let mut threshold = (hash as f64 / u64::MAX as f64) * total_weight;
                
                for (i, &weight) in weights.iter().enumerate() {
                    threshold -= weight;
                    if threshold <= 0.0 {
                        return i;
                    }
                }
                weights.len() - 1
            }
            LoadBalancingStrategy::AdaptiveHashing => {
                // Consistent hashing with load awareness
                if let Some(hint) = work_hint {
                    let base_pool = (hint as usize) % self.pool_loads.len();
                    let base_load = self.pool_loads[base_pool].load(Ordering::Relaxed);
                    
                    // Check if base pool is overloaded
                    let avg_load: u64 = self.pool_loads.iter()
                        .map(|load| load.load(Ordering::Relaxed))
                        .sum::<u64>() / self.pool_loads.len() as u64;
                    
                    if base_load > avg_load * 2 {
                        // Find alternative pool
                        ((hint + 1) as usize) % self.pool_loads.len()
                    } else {
                        base_pool
                    }
                } else {
                    0
                }
            }
        }
    }

    /// Update pool load statistics
    pub fn update_pool_load(&self, pool_id: usize, delta: i64) {
        if pool_id < self.pool_loads.len() {
            if delta >= 0 {
                self.pool_loads[pool_id].fetch_add(delta as u64, Ordering::Relaxed);
            } else {
                self.pool_loads[pool_id].fetch_sub((-delta) as u64, Ordering::Relaxed);
            }
        }
    }
    
    /// Get the number of pools in the load balancer
    pub fn pool_count(&self) -> usize {
        self.pool_loads.len()
    }
    
    /// Get current load distribution across all pools
    pub fn get_pool_loads(&self) -> Vec<u64> {
        self.pool_loads.iter()
            .map(|load| load.load(Ordering::Relaxed))
            .collect()
    }
}

impl<T> WorkDistributor<T>
where
    T: Send + Sync + 'static,
{
    /// Create a new work distributor
    pub fn new(
        num_pools: usize,
        workers_per_pool: usize,
        strategy: LoadBalancingStrategy,
    ) -> Self {
        let mut worker_pools = Vec::with_capacity(num_pools);
        
        for pool_id in 0..num_pools {
            let pool = WorkerPool::new(pool_id, workers_per_pool, workers_per_pool / 2, workers_per_pool * 2);
            worker_pools.push(pool);
        }

        let load_balancer = LoadBalancer::new(strategy, num_pools);

        Self {
            worker_pools,
            load_balancer,
            global_stats: GlobalWorkStats::new(),
        }
    }

    /// Distribute work item to optimal pool
    pub async fn distribute_work(&self, work_item: T, hint: Option<u64>) -> Result<(), T> {
        let pool_id = self.load_balancer.select_pool(hint);
        
        if let Some(pool) = self.worker_pools.get(pool_id) {
            pool.add_work(work_item).await?;
            self.load_balancer.update_pool_load(pool_id, 1);
            self.global_stats.total_work_items.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            Err(work_item)
        }
    }

    /// Get global work distribution statistics
    pub fn get_global_stats(&self) -> &GlobalWorkStats {
        &self.global_stats
    }

    /// Get pool statistics
    pub fn get_pool_stats(&self) -> Vec<(usize, &WorkStealingStats)> {
        self.worker_pools
            .iter()
            .map(|pool| (pool.pool_id, pool.work_queue.get_stats()))
            .collect()
    }
}

impl<T> WorkerPool<T>
where
    T: Send + Sync + 'static,
{
    /// Create a new worker pool
    pub fn new(pool_id: usize, initial_workers: usize, min_workers: usize, max_workers: usize) -> Self {
        let work_queue = Arc::new(WorkStealingQueue::new());
        let mut workers = Vec::with_capacity(max_workers);
        
        // Create initial workers
        for worker_id in 0..initial_workers {
            let worker = Worker::new(worker_id, work_queue.clone());
            workers.push(worker);
        }

        let scale_metrics = ScaleMetrics {
            queue_length_history: RwLock::new(VecDeque::with_capacity(100)),
            throughput_history: RwLock::new(VecDeque::with_capacity(100)),
            last_scale_event: RwLock::new(Instant::now()),
            scale_cooldown: Duration::from_secs(30),
        };

        Self {
            _workers: workers,
            work_queue,
            pool_id,
            active_workers: AtomicUsize::new(initial_workers),
            max_workers,
            min_workers,
            scale_metrics,
        }
    }

    /// Add work to the pool
    pub async fn add_work(&self, work_item: T) -> Result<(), T> {
        self.work_queue.push(work_item).await;
        
        // Update scaling metrics
        let queue_len = self.work_queue.local_queue.read().await.len();
        {
            let mut history = self.scale_metrics.queue_length_history.write().await;
            history.push_back(queue_len);
            if history.len() > 100 {
                history.pop_front();
            }
        }

        // Check if scaling is needed
        self.maybe_scale().await;
        
        Ok(())
    }

    /// Adaptive scaling based on queue length and throughput
    async fn maybe_scale(&self) {
        let last_scale = *self.scale_metrics.last_scale_event.read().await;
        if last_scale.elapsed() < self.scale_metrics.scale_cooldown {
            return; // Still in cooldown period
        }

        let queue_history = self.scale_metrics.queue_length_history.read().await;
        if queue_history.len() < 10 {
            return; // Not enough data
        }

        let avg_queue_len: f64 = queue_history.iter().map(|&len| len as f64).sum::<f64>() / queue_history.len() as f64;
        let current_workers = self.active_workers.load(Ordering::Relaxed);

        // Scale up if queue is consistently long
        if avg_queue_len > 50.0 && current_workers < self.max_workers {
            self.scale_up().await;
        }
        // Scale down if queue is consistently short
        else if avg_queue_len < 5.0 && current_workers > self.min_workers {
            self.scale_down().await;
        }
    }

    /// Scale up worker pool
    async fn scale_up(&self) {
        let current_workers = self.active_workers.load(Ordering::Relaxed);
        if current_workers < self.max_workers {
            // In a real implementation, this would spawn new worker tasks
            self.active_workers.fetch_add(1, Ordering::Relaxed);
            *self.scale_metrics.last_scale_event.write().await = Instant::now();
            info!("Scaled up pool {} to {} workers", self.pool_id, current_workers + 1);
        }
    }

    /// Scale down worker pool
    async fn scale_down(&self) {
        let current_workers = self.active_workers.load(Ordering::Relaxed);
        if current_workers > self.min_workers {
            // In a real implementation, this would gracefully shutdown workers
            self.active_workers.fetch_sub(1, Ordering::Relaxed);
            *self.scale_metrics.last_scale_event.write().await = Instant::now();
            info!("Scaled down pool {} to {} workers", self.pool_id, current_workers - 1);
        }
    }

    /// Get current active worker count
    pub fn active_worker_count(&self) -> usize {
        self.active_workers.load(Ordering::Relaxed)
    }
}

impl<T> Worker<T> {
    /// Create a new worker
    pub fn new(id: usize, work_queue: Arc<WorkStealingQueue<T>>) -> Self {
        Self {
            _id: id,
            _work_queue: work_queue,
            is_active: Arc::new(AtomicBool::new(true)),
            processed_count: AtomicU64::new(0),
        }
    }

    /// Get worker processing count
    pub fn processed_count(&self) -> u64 {
        self.processed_count.load(Ordering::Relaxed)
    }

    /// Check if worker is active
    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::Relaxed)
    }

    /// Shutdown worker
    pub fn shutdown(&self) {
        self.is_active.store(false, Ordering::Relaxed);
    }
}

impl GlobalWorkStats {
    fn new() -> Self {
        Self {
            total_work_items: AtomicU64::new(0),
            completed_work_items: AtomicU64::new(0),
            failed_work_items: AtomicU64::new(0),
            avg_completion_time_ns: AtomicU64::new(0),
            active_workers: AtomicUsize::new(0),
            total_workers: AtomicUsize::new(0),
        }
    }

    /// Calculate completion rate
    pub fn completion_rate(&self) -> f64 {
        let total = self.total_work_items.load(Ordering::Relaxed);
        let completed = self.completed_work_items.load(Ordering::Relaxed);
        
        if total == 0 {
            0.0
        } else {
            completed as f64 / total as f64
        }
    }

    /// Calculate failure rate
    pub fn failure_rate(&self) -> f64 {
        let total = self.total_work_items.load(Ordering::Relaxed);
        let failed = self.failed_work_items.load(Ordering::Relaxed);
        
        if total == 0 {
            0.0
        } else {
            failed as f64 / total as f64
        }
    }
}

/// Concurrency optimization errors
#[derive(Debug, thiserror::Error)]
pub enum ConcurrencyError {
    #[error("Lock-free operation failed: {0}")]
    LockFreeFailed(String),
    #[error("Actor system error: {0}")]
    ActorSystemError(String),
    #[error("Work distribution failed: {0}")]
    WorkDistributionFailed(String),
    #[error("Concurrency limit exceeded: {0}")]
    ConcurrencyLimitExceeded(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tokio::time::{sleep, Duration};

    #[test]
    fn test_lock_free_ring_buffer() {
        let buffer = LockFreeRingBuffer::new(4);
        
        // Test basic operations
        assert!(buffer.try_push(1).is_ok());
        assert!(buffer.try_push(2).is_ok());
        assert_eq!(buffer.len(), 2);
        
        assert_eq!(buffer.try_pop(), Some(1));
        assert_eq!(buffer.try_pop(), Some(2));
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.try_pop(), None);
        
        // Test capacity limits
        for i in 0..4 {
            assert!(buffer.try_push(i).is_ok());
        }
        
        // Should reject when full
        assert!(buffer.try_push(99).is_err());
    }

    #[tokio::test]
    async fn test_work_stealing_queue() {
        let queue1 = Arc::new(RwLock::new(VecDeque::new()));
        let queue2 = Arc::new(RwLock::new(VecDeque::new()));
        
        let work_stealer = WorkStealingQueue::new();
        work_stealer.register_steal_target(queue1.clone()).await;
        work_stealer.register_steal_target(queue2.clone()).await;
        
        // Add work to local queue
        work_stealer.push("local_work".to_string()).await;
        assert_eq!(work_stealer.try_pop().await, Some("local_work".to_string()));
        
        // Add work to steal targets
        queue1.write().await.push_back("stolen_work".to_string());
        assert_eq!(work_stealer.try_pop().await, Some("stolen_work".to_string()));
        
        // Test statistics
        let stats = work_stealer.get_stats();
        assert_eq!(stats.local_pushes.load(Ordering::Relaxed), 1);
        assert_eq!(stats.local_pops.load(Ordering::Relaxed), 1);
        assert!(stats.steal_success_rate() > 0.0);
    }

    #[tokio::test]
    async fn test_optimized_actor() {
        #[derive(Debug, Clone)]
        struct TestMessage {
            id: u32,
            data: String,
        }
        
        #[derive(Debug)]
        struct TestState {
            processed_count: u32,
        }
        
        let actor = OptimizedActor::new(
            TestState { processed_count: 0 },
            100,  // mailbox capacity
            5,    // batch size
            Duration::from_millis(100), // batch timeout
        );
        
        // Send test messages
        for i in 0..10 {
            let msg = TestMessage {
                id: i,
                data: format!("test_message_{}", i),
            };
            assert!(actor.try_send(msg).is_ok());
        }
        
        assert_eq!(actor.mailbox_size(), 10);
        
        // Test metrics
        let metrics = actor.get_metrics();
        assert_eq!(metrics.messages_processed.load(Ordering::Relaxed), 0); // Not processed yet
        assert_eq!(metrics.mailbox_overflows.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_load_balancer() {
        let balancer = LoadBalancer::new(LoadBalancingStrategy::LeastLoaded, 3);
        
        // Initially all pools should have zero load
        for _ in 0..10 {
            let selected = balancer.select_pool(None);
            assert!(selected < 3);
        }
        
        // Add load to first pool
        balancer.update_pool_load(0, 10);
        
        // Should now prefer other pools
        let selections: Vec<usize> = (0..10).map(|_| balancer.select_pool(None)).collect();
        let pool_0_selections = selections.iter().filter(|&&x| x == 0).count();
        let other_selections = selections.len() - pool_0_selections;
        
        // Should prefer pools 1 and 2 over heavily loaded pool 0
        assert!(other_selections > pool_0_selections);
    }

    #[tokio::test]
    async fn test_work_distributor() {
        let distributor = WorkDistributor::new(
            2, // num pools
            2, // workers per pool
            LoadBalancingStrategy::RoundRobin,
        );
        
        // Distribute some work
        for i in 0..10 {
            assert!(distributor.distribute_work(format!("work_{}", i), None).await.is_ok());
        }
        
        let stats = distributor.get_global_stats();
        assert_eq!(stats.total_work_items.load(Ordering::Relaxed), 10);
        
        let pool_stats = distributor.get_pool_stats();
        assert_eq!(pool_stats.len(), 2);
    }

    #[tokio::test]
    async fn test_batch_processor() {
        let processor = BatchProcessor::new(3, Duration::from_millis(50));
        
        // Add messages one by one
        assert!(processor.add_to_batch("msg1".to_string()).await.is_none());
        assert!(processor.add_to_batch("msg2".to_string()).await.is_none());
        
        // Third message should trigger batch
        let batch = processor.add_to_batch("msg3".to_string()).await;
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), 3);
        
        // Test timeout-based flush
        processor.add_to_batch("msg4".to_string()).await;
        sleep(Duration::from_millis(60)).await;
        
        let timeout_batch = processor.flush_batch().await;
        assert_eq!(timeout_batch.len(), 1);
    }

    #[test]
    fn test_global_work_stats() {
        let stats = GlobalWorkStats::new();
        
        stats.total_work_items.store(100, Ordering::Relaxed);
        stats.completed_work_items.store(80, Ordering::Relaxed);
        stats.failed_work_items.store(10, Ordering::Relaxed);
        
        assert_eq!(stats.completion_rate(), 0.8);
        assert_eq!(stats.failure_rate(), 0.1);
    }
}