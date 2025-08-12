//! Processing Pipeline Optimization Module
//! 
//! This module provides advanced processing pipeline optimizations including
//! batch processing, intelligent caching, and cache-optimized data structures.
//! Enhanced for Story 6.2 to achieve 10-25% processing speed improvements.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Batch processor for aggregating similar operations
pub struct BatchProcessor<T, R> {
    batch_size: usize,
    flush_interval: Duration,
    processor_fn: Arc<dyn Fn(Vec<T>) -> Vec<R> + Send + Sync>,
    pending_items: Arc<RwLock<Vec<T>>>,
    last_flush: Arc<RwLock<Instant>>,
    result_sender: mpsc::UnboundedSender<R>,
    stats: BatchStats,
}

impl<T, R> BatchProcessor<T, R>
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    /// Create a new batch processor
    pub fn new(
        batch_size: usize,
        flush_interval: Duration,
        processor_fn: Arc<dyn Fn(Vec<T>) -> Vec<R> + Send + Sync>,
    ) -> (Self, mpsc::UnboundedReceiver<R>) {
        let (result_sender, result_receiver) = mpsc::unbounded_channel();
        
        let processor = Self {
            batch_size,
            flush_interval,
            processor_fn,
            pending_items: Arc::new(RwLock::new(Vec::new())),
            last_flush: Arc::new(RwLock::new(Instant::now())),
            result_sender,
            stats: BatchStats::new(),
        };
        
        processor.start_flush_timer();
        
        (processor, result_receiver)
    }
    
    /// Add item to batch for processing
    pub async fn add_item(&self, item: T) -> Result<(), &'static str> {
        let should_flush = {
            let mut pending = self.pending_items.write().unwrap();
            pending.push(item);
            pending.len() >= self.batch_size
        };
        
        if should_flush {
            self.flush_batch().await;
        }
        
        Ok(())
    }
    
    /// Force flush current batch
    pub async fn flush_batch(&self) {
        let batch = {
            let mut pending = self.pending_items.write().unwrap();
            if pending.is_empty() {
                return;
            }
            std::mem::take(&mut *pending)
        };
        
        let batch_size = batch.len();
        let start = Instant::now();
        
        // Process the batch
        let results = (self.processor_fn)(batch);
        
        // Send results
        for result in results {
            if self.result_sender.send(result).is_err() {
                warn!("Failed to send batch processing result - receiver dropped");
            }
        }
        
        // Update statistics
        self.stats.record_batch_processed(batch_size, start.elapsed());
        
        // Update last flush time
        if let Ok(mut last_flush) = self.last_flush.write() {
            *last_flush = Instant::now();
        }
        
        debug!("Processed batch of {} items in {:?}", batch_size, start.elapsed());
    }
    
    /// Start automatic flush timer
    fn start_flush_timer(&self) {
        let flush_interval = self.flush_interval;
        let pending_items = Arc::clone(&self.pending_items);
        let last_flush = Arc::clone(&self.last_flush);
        let processor_fn = Arc::clone(&self.processor_fn);
        let result_sender = self.result_sender.clone();
        let stats = self.stats.clone();
        
        tokio::spawn(async move {
            loop {
                sleep(flush_interval).await;
                
                let should_flush = {
                    let last_flush_time = last_flush.read().unwrap();
                    let pending = pending_items.read().unwrap();
                    !pending.is_empty() && last_flush_time.elapsed() >= flush_interval
                };
                
                if should_flush {
                    let batch = {
                        let mut pending = pending_items.write().unwrap();
                        if pending.is_empty() {
                            continue;
                        }
                        std::mem::take(&mut *pending)
                    };
                    
                    let batch_size = batch.len();
                    let start = Instant::now();
                    
                    // Process the batch
                    let results = processor_fn(batch);
                    
                    // Send results
                    for result in results {
                        if result_sender.send(result).is_err() {
                            warn!("Failed to send batch processing result - receiver dropped");
                            return;
                        }
                    }
                    
                    // Update statistics
                    stats.record_batch_processed(batch_size, start.elapsed());
                    
                    // Update last flush time
                    if let Ok(mut last_flush) = last_flush.write() {
                        *last_flush = Instant::now();
                    }
                    
                    debug!("Timer-flushed batch of {} items in {:?}", batch_size, start.elapsed());
                }
            }
        });
    }
    
    /// Get batch processing statistics
    pub fn get_stats(&self) -> BatchStats {
        self.stats.clone()
    }
}

/// Intelligent cache with LRU eviction and access pattern optimization
pub struct IntelligentCache<K, V> {
    capacity: usize,
    cache: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,
    access_order: Arc<RwLock<VecDeque<K>>>,
    stats: CacheStats,
    cleanup_threshold: usize,
}

#[derive(Clone)]
struct CacheEntry<V> {
    value: V,
    access_time: Instant,
    access_count: u64,
    size_estimate: usize,
}

impl<K, V> IntelligentCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Create a new intelligent cache
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            cache: Arc::new(RwLock::new(HashMap::new())),
            access_order: Arc::new(RwLock::new(VecDeque::new())),
            stats: CacheStats::new(),
            cleanup_threshold: capacity + (capacity / 4), // 25% over capacity
        }
    }
    
    /// Get value from cache
    pub fn get(&self, key: &K) -> Option<V> {
        let mut cache = self.cache.write().unwrap();
        
        if let Some(entry) = cache.get_mut(key) {
            // Update access statistics
            entry.access_time = Instant::now();
            entry.access_count += 1;
            
            // Update access order (move to back)
            let mut access_order = self.access_order.write().unwrap();
            if let Some(pos) = access_order.iter().position(|x| x == key) {
                access_order.remove(pos);
            }
            access_order.push_back(key.clone());
            
            self.stats.record_hit();
            Some(entry.value.clone())
        } else {
            self.stats.record_miss();
            None
        }
    }
    
    /// Insert value into cache
    pub fn insert(&self, key: K, value: V) {
        let size_estimate = std::mem::size_of::<V>();
        
        let mut cache = self.cache.write().unwrap();
        let mut access_order = self.access_order.write().unwrap();
        
        // If key already exists, remove it from access order
        if cache.contains_key(&key) {
            if let Some(pos) = access_order.iter().position(|x| x == &key) {
                access_order.remove(pos);
            }
        }
        
        // Insert new entry
        let entry = CacheEntry {
            value,
            access_time: Instant::now(),
            access_count: 1,
            size_estimate,
        };
        
        cache.insert(key.clone(), entry);
        access_order.push_back(key);
        
        // Check if cleanup is needed
        if cache.len() > self.cleanup_threshold {
            self.cleanup_lru(&mut cache, &mut access_order);
        }
        
        self.stats.record_insertion();
    }
    
    /// Remove least recently used items to maintain capacity
    fn cleanup_lru(
        &self,
        cache: &mut HashMap<K, CacheEntry<V>>,
        access_order: &mut VecDeque<K>,
    ) {
        while cache.len() > self.capacity {
            if let Some(oldest_key) = access_order.pop_front() {
                cache.remove(&oldest_key);
                self.stats.record_eviction();
            } else {
                break;
            }
        }
    }
    
    /// Get cache statistics
    pub fn get_stats(&self) -> CacheStats {
        let cache = self.cache.read().unwrap();
        let mut stats = self.stats.clone();
        stats.current_size = cache.len();
        stats.estimated_memory_usage = cache.values()
            .map(|entry| entry.size_estimate)
            .sum();
        stats
    }
    
    /// Clear all cached entries
    pub fn clear(&self) {
        let mut cache = self.cache.write().unwrap();
        let mut access_order = self.access_order.write().unwrap();
        
        cache.clear();
        access_order.clear();
        
        self.stats.record_clear();
    }
}

/// Cache-optimized data pipeline for sequential processing
pub struct CacheOptimizedPipeline<T> {
    cache: IntelligentCache<String, T>,
    batch_processor: BatchProcessor<String, Option<T>>,
    prefetch_queue: Arc<RwLock<VecDeque<String>>>,
    prefetch_batch_size: usize,
}

impl<T> CacheOptimizedPipeline<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Create a new cache-optimized pipeline
    pub fn new(
        cache_capacity: usize,
        batch_size: usize,
        flush_interval: Duration,
        loader_fn: Arc<dyn Fn(Vec<String>) -> Vec<Option<T>> + Send + Sync>,
    ) -> (Self, mpsc::UnboundedReceiver<Option<T>>) {
        let cache = IntelligentCache::new(cache_capacity);
        let (batch_processor, receiver) = BatchProcessor::new(
            batch_size,
            flush_interval,
            loader_fn,
        );
        
        let pipeline = Self {
            cache,
            batch_processor,
            prefetch_queue: Arc::new(RwLock::new(VecDeque::new())),
            prefetch_batch_size: batch_size,
        };
        
        (pipeline, receiver)
    }
    
    /// Get item from cache or queue for batch loading
    pub async fn get(&self, key: String) -> Option<T> {
        // Try cache first
        if let Some(value) = self.cache.get(&key) {
            return Some(value);
        }
        
        // Add to batch processor for loading
        if self.batch_processor.add_item(key).await.is_err() {
            warn!("Failed to add item to batch processor");
        }
        
        None
    }
    
    /// Prefetch items for better cache utilization
    pub fn prefetch(&self, keys: Vec<String>) {
        let mut prefetch_queue = self.prefetch_queue.write().unwrap();
        
        for key in keys {
            // Only prefetch if not already in cache
            if self.cache.get(&key).is_none() {
                prefetch_queue.push_back(key);
            }
        }
        
        // Trigger prefetch batch if queue is large enough
        if prefetch_queue.len() >= self.prefetch_batch_size {
            let batch: Vec<String> = prefetch_queue.drain(..self.prefetch_batch_size).collect();
            
            // Clone sender to avoid borrow issues
            let result_sender = self.batch_processor.result_sender.clone();
            let processor_fn = Arc::clone(&self.batch_processor.processor_fn);
            
            tokio::spawn(async move {
                // Process batch directly to avoid complex async closure issues
                let results = processor_fn(batch);
                for result in results {
                    let _ = result_sender.send(result);
                }
            });
        }
    }
    
    /// Get pipeline statistics
    pub fn get_stats(&self) -> PipelineStats {
        PipelineStats {
            cache_stats: self.cache.get_stats(),
            batch_stats: self.batch_processor.get_stats(),
            prefetch_queue_size: self.prefetch_queue.read().unwrap().len(),
        }
    }
}

/// Statistics tracking for batch processing
#[derive(Debug)]
pub struct BatchStats {
    pub total_batches: std::sync::atomic::AtomicU64,
    pub total_items: std::sync::atomic::AtomicU64,
    pub total_processing_time: std::sync::Arc<std::sync::RwLock<Duration>>,
    pub average_batch_size: std::sync::atomic::AtomicU64,
}

impl Clone for BatchStats {
    fn clone(&self) -> Self {
        use std::sync::atomic::Ordering;
        Self {
            total_batches: std::sync::atomic::AtomicU64::new(
                self.total_batches.load(Ordering::Relaxed)
            ),
            total_items: std::sync::atomic::AtomicU64::new(
                self.total_items.load(Ordering::Relaxed)
            ),
            total_processing_time: std::sync::Arc::new(
                std::sync::RwLock::new(
                    *self.total_processing_time.read().unwrap()
                )
            ),
            average_batch_size: std::sync::atomic::AtomicU64::new(
                self.average_batch_size.load(Ordering::Relaxed)
            ),
        }
    }
}

impl BatchStats {
    fn new() -> Self {
        Self {
            total_batches: std::sync::atomic::AtomicU64::new(0),
            total_items: std::sync::atomic::AtomicU64::new(0),
            total_processing_time: std::sync::Arc::new(std::sync::RwLock::new(Duration::new(0, 0))),
            average_batch_size: std::sync::atomic::AtomicU64::new(0),
        }
    }
    
    fn record_batch_processed(&self, batch_size: usize, duration: Duration) {
        use std::sync::atomic::Ordering;
        
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        self.total_items.fetch_add(batch_size as u64, Ordering::Relaxed);
        
        if let Ok(mut total_time) = self.total_processing_time.write() {
            *total_time += duration;
        }
        
        // Update average batch size
        let total_batches = self.total_batches.load(Ordering::Relaxed);
        let total_items = self.total_items.load(Ordering::Relaxed);
        if total_batches > 0 {
            self.average_batch_size.store(total_items / total_batches, Ordering::Relaxed);
        }
    }
}

/// Statistics tracking for intelligent cache
#[derive(Debug)]
pub struct CacheStats {
    pub hits: std::sync::atomic::AtomicU64,
    pub misses: std::sync::atomic::AtomicU64,
    pub insertions: std::sync::atomic::AtomicU64,
    pub evictions: std::sync::atomic::AtomicU64,
    pub current_size: usize,
    pub estimated_memory_usage: usize,
}

impl Clone for CacheStats {
    fn clone(&self) -> Self {
        use std::sync::atomic::Ordering;
        Self {
            hits: std::sync::atomic::AtomicU64::new(
                self.hits.load(Ordering::Relaxed)
            ),
            misses: std::sync::atomic::AtomicU64::new(
                self.misses.load(Ordering::Relaxed)
            ),
            insertions: std::sync::atomic::AtomicU64::new(
                self.insertions.load(Ordering::Relaxed)
            ),
            evictions: std::sync::atomic::AtomicU64::new(
                self.evictions.load(Ordering::Relaxed)
            ),
            current_size: self.current_size,
            estimated_memory_usage: self.estimated_memory_usage,
        }
    }
}

impl CacheStats {
    fn new() -> Self {
        Self {
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            insertions: std::sync::atomic::AtomicU64::new(0),
            evictions: std::sync::atomic::AtomicU64::new(0),
            current_size: 0,
            estimated_memory_usage: 0,
        }
    }
    
    fn record_hit(&self) {
        self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    fn record_miss(&self) {
        self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    fn record_insertion(&self) {
        self.insertions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    fn record_eviction(&self) {
        self.evictions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    fn record_clear(&self) {
        // Reset all counters on clear
        self.hits.store(0, std::sync::atomic::Ordering::Relaxed);
        self.misses.store(0, std::sync::atomic::Ordering::Relaxed);
        self.insertions.store(0, std::sync::atomic::Ordering::Relaxed);
        self.evictions.store(0, std::sync::atomic::Ordering::Relaxed);
    }
    
    /// Calculate hit rate as percentage
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = self.misses.load(std::sync::atomic::Ordering::Relaxed);
        let total = hits + misses;
        
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Combined statistics for cache-optimized pipeline
#[derive(Debug)]
pub struct PipelineStats {
    pub cache_stats: CacheStats,
    pub batch_stats: BatchStats,
    pub prefetch_queue_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    
    #[tokio::test]
    async fn test_batch_processor_basic() {
        let processor_fn = Arc::new(|batch: Vec<i32>| -> Vec<i32> {
            batch.into_iter().map(|x| x * 2).collect()
        });
        
        let (processor, mut receiver) = BatchProcessor::new(
            3, // batch_size
            Duration::from_millis(100),
            processor_fn,
        );
        
        // Add items to trigger batch processing
        processor.add_item(1).await.unwrap();
        processor.add_item(2).await.unwrap();
        processor.add_item(3).await.unwrap(); // Should trigger batch
        
        // Collect results
        let mut results = Vec::new();
        for _ in 0..3 {
            if let Some(result) = receiver.recv().await {
                results.push(result);
            }
        }
        
        results.sort();
        assert_eq!(results, vec![2, 4, 6]);
        
        let stats = processor.get_stats();
        assert_eq!(stats.total_batches.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_items.load(Ordering::Relaxed), 3);
    }
    
    #[test]
    fn test_intelligent_cache() {
        let cache = IntelligentCache::new(2);
        
        // Insert items
        cache.insert("key1".to_string(), "value1".to_string());
        cache.insert("key2".to_string(), "value2".to_string());
        
        // Test hits
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(cache.get(&"key2".to_string()), Some("value2".to_string()));
        
        // Test miss
        assert_eq!(cache.get(&"key3".to_string()), None);
        
        // Insert item that should evict oldest
        cache.insert("key3".to_string(), "value3".to_string());
        
        // key1 should be evicted (LRU)
        assert_eq!(cache.get(&"key1".to_string()), None);
        assert_eq!(cache.get(&"key3".to_string()), Some("value3".to_string()));
        
        let stats = cache.get_stats();
        assert!(stats.hit_rate() > 0.0);
        assert_eq!(stats.evictions.load(Ordering::Relaxed), 1);
    }
    
    #[tokio::test]
    async fn test_cache_optimized_pipeline() {
        let loader_fn = Arc::new(|keys: Vec<String>| -> Vec<Option<String>> {
            keys.into_iter().map(|key| Some(format!("loaded_{}", key))).collect()
        });
        
        let (pipeline, mut receiver) = CacheOptimizedPipeline::new(
            10, // cache_capacity
            2,  // batch_size
            Duration::from_millis(50),
            loader_fn,
        );
        
        // Test cache miss (should queue for batch loading)
        let result = pipeline.get("test_key".to_string()).await;
        assert_eq!(result, None); // Cache miss
        
        // Should receive loaded value from batch processor
        if let Some(loaded_value) = receiver.recv().await {
            assert_eq!(loaded_value, Some("loaded_test_key".to_string()));
        }
        
        let stats = pipeline.get_stats();
        assert_eq!(stats.cache_stats.misses.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_cache_stats() {
        let stats = CacheStats::new();
        
        stats.record_hit();
        stats.record_hit();
        stats.record_miss();
        
        assert_eq!(stats.hits.load(Ordering::Relaxed), 2);
        assert_eq!(stats.misses.load(Ordering::Relaxed), 1);
        assert!((stats.hit_rate() - 0.666).abs() < 0.1);
    }
}