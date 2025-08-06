//! Object pooling system for reducing memory allocations
//! 
//! This module provides high-performance object pools for frequently allocated
//! types like Vec<f64>, candle data structures, and other temporary objects.

use crate::historical::structs::FuturesOHLCVCandle;
use crate::record_memory_pool_hit;
use std::collections::VecDeque;
use std::sync::Mutex;
use tracing::debug;

/// Thread-safe object pool for any type that implements Default + Clone
pub struct ObjectPool<T> {
    pool: Mutex<VecDeque<T>>,
    max_size: usize,
    created_count: std::sync::atomic::AtomicU64,
    reused_count: std::sync::atomic::AtomicU64,
}

impl<T> ObjectPool<T> 
where 
    T: Default + Clone,
{
    /// Create a new object pool with specified maximum size
    pub fn new(max_size: usize) -> Self {
        Self {
            pool: Mutex::new(VecDeque::with_capacity(max_size.min(16))),
            max_size,
            created_count: std::sync::atomic::AtomicU64::new(0),
            reused_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get an object from the pool or create a new one
    pub fn get(&self) -> T {
        if let Ok(mut pool) = self.pool.lock() {
            if let Some(obj) = pool.pop_front() {
                self.reused_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                record_memory_pool_hit!("object_pool", "reuse");
                return obj;
            }
        }

        self.created_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        T::default()
    }

    /// Return an object to the pool for reuse
    pub fn put(&self, _obj: T) {
        if let Ok(mut pool) = self.pool.lock() {
            if pool.len() < self.max_size {
                // Reset the object to default state and store in pool
                let reset_obj = T::default();
                pool.push_back(reset_obj);
            }
        }
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        let pool_size = self.pool.lock().map(|p| p.len()).unwrap_or(0);
        PoolStats {
            pool_size,
            created_count: self.created_count.load(std::sync::atomic::Ordering::Relaxed),
            reused_count: self.reused_count.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Clear the pool
    pub fn clear(&self) {
        if let Ok(mut pool) = self.pool.lock() {
            pool.clear();
        }
    }
}

/// Pool statistics for monitoring
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub pool_size: usize,
    pub created_count: u64,
    pub reused_count: u64,
}

impl PoolStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.created_count + self.reused_count;
        if total == 0 {
            0.0
        } else {
            self.reused_count as f64 / total as f64
        }
    }
}

/// Specialized pool for Vec<f64> - commonly used for price/volume data
pub struct VecPool {
    pools: [ObjectPool<Vec<f64>>; 4], // Different sizes: 16, 64, 256, 1024
}

impl VecPool {
    pub fn new() -> Self {
        Self {
            pools: [
                ObjectPool::new(32),  // Pool for small vecs (<=16)
                ObjectPool::new(16),  // Pool for medium vecs (<=64)  
                ObjectPool::new(8),   // Pool for large vecs (<=256)
                ObjectPool::new(4),   // Pool for very large vecs (<=1024)
            ],
        }
    }

    /// Get a Vec with appropriate capacity
    pub fn get_vec(&self, capacity: usize) -> Vec<f64> {
        let pool_index = match capacity {
            0..=16 => 0,
            17..=64 => 1,
            65..=256 => 2,
            257..=1024 => 3,
            _ => return Vec::with_capacity(capacity), // Too large for pooling
        };

        let mut vec = self.pools[pool_index].get();
        vec.reserve(capacity);
        vec
    }

    /// Return a Vec to the appropriate pool
    pub fn put_vec(&self, vec: Vec<f64>) {
        let capacity = vec.capacity();
        let pool_index = match capacity {
            0..=16 => 0,
            17..=64 => 1,
            65..=256 => 2, 
            257..=1024 => 3,
            _ => return, // Too large for pooling
        };

        self.pools[pool_index].put(vec);
    }

    /// Get combined statistics for all pools
    pub fn stats(&self) -> Vec<PoolStats> {
        self.pools.iter().map(|pool| pool.stats()).collect()
    }
}

impl Default for VecPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Specialized pool for candle data structures
pub struct CandlePool {
    pool: ObjectPool<FuturesOHLCVCandle>,
}

impl CandlePool {
    pub fn new() -> Self {
        Self {
            pool: ObjectPool::new(128), // Keep up to 128 candles in pool
        }
    }

    /// Get a candle from the pool
    pub fn get_candle(&self) -> FuturesOHLCVCandle {
        self.pool.get()
    }

    /// Return a candle to the pool
    pub fn put_candle(&self, candle: FuturesOHLCVCandle) {
        self.pool.put(candle);
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        self.pool.stats()
    }
}

impl Default for CandlePool {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII wrapper for pooled objects
pub struct PooledObject<T, P> 
where 
    T: Default + Clone,
    P: std::ops::Deref<Target = ObjectPool<T>>,
{
    object: Option<T>,
    pool: std::sync::Arc<P>,
}

impl<T, P> PooledObject<T, P> 
where 
    T: Default + Clone,
    P: std::ops::Deref<Target = ObjectPool<T>>,
{
    pub fn new(object: T, pool: std::sync::Arc<P>) -> Self {
        Self {
            object: Some(object),
            pool,
        }
    }

    /// Get a reference to the pooled object
    pub fn get(&self) -> &T {
        self.object.as_ref().unwrap()
    }

    /// Get a mutable reference to the pooled object
    pub fn get_mut(&mut self) -> &mut T {
        self.object.as_mut().unwrap()
    }
}

impl<T, P> Drop for PooledObject<T, P>
where
    T: Default + Clone,
    P: std::ops::Deref<Target = ObjectPool<T>>,
{
    fn drop(&mut self) {
        if let Some(obj) = self.object.take() {
            self.pool.put(obj);
        }
    }
}

/// Global object pools for the application
pub struct GlobalPools {
    pub vec_pool: VecPool,
    pub candle_pool: CandlePool,
    pub string_pool: ObjectPool<String>,
}

impl GlobalPools {
    pub fn new() -> Self {
        Self {
            vec_pool: VecPool::new(),
            candle_pool: CandlePool::new(),
            string_pool: ObjectPool::new(64),
        }
    }

    /// Get a comprehensive stats report
    pub fn stats_report(&self) -> String {
        let vec_stats = self.vec_pool.stats();
        let candle_stats = self.candle_pool.stats();
        let string_stats = self.string_pool.stats();

        format!(
            "Object Pool Statistics:\n\
             Vec Pools: {:?}\n\
             Candle Pool: created={}, reused={}, hit_rate={:.1}%\n\
             String Pool: created={}, reused={}, hit_rate={:.1}%",
            vec_stats,
            candle_stats.created_count,
            candle_stats.reused_count,
            candle_stats.hit_rate() * 100.0,
            string_stats.created_count,
            string_stats.reused_count,
            string_stats.hit_rate() * 100.0
        )
    }
}

impl Default for GlobalPools {
    fn default() -> Self {
        Self::new()
    }
}

/// Global instance of object pools
static GLOBAL_POOLS: std::sync::OnceLock<GlobalPools> = std::sync::OnceLock::new();

/// Initialize global object pools
pub fn init_global_pools() -> &'static GlobalPools {
    GLOBAL_POOLS.get_or_init(|| {
        debug!("Initializing global object pools");
        GlobalPools::new()
    })
}

/// Get global object pools instance
pub fn get_global_pools() -> Option<&'static GlobalPools> {
    GLOBAL_POOLS.get()
}

/// Convenience macro for getting a pooled Vec with capacity
#[macro_export]
macro_rules! get_pooled_vec {
    ($capacity:expr) => {
        if let Some(pools) = $crate::common::object_pool::get_global_pools() {
            pools.vec_pool.get_vec($capacity)
        } else {
            Vec::with_capacity($capacity)
        }
    };
}

/// Convenience macro for returning a Vec to the pool
#[macro_export]
macro_rules! put_pooled_vec {
    ($vec:expr) => {
        if let Some(pools) = $crate::common::object_pool::get_global_pools() {
            pools.vec_pool.put_vec($vec);
        }
    };
}

/// Convenience macro for getting a pooled candle
#[macro_export]
macro_rules! get_pooled_candle {
    () => {
        if let Some(pools) = $crate::common::object_pool::get_global_pools() {
            pools.candle_pool.get_candle()
        } else {
            $crate::historical::structs::FuturesOHLCVCandle::default()
        }
    };
}

/// Convenience macro for returning a candle to the pool
#[macro_export]
macro_rules! put_pooled_candle {
    ($candle:expr) => {
        if let Some(pools) = $crate::common::object_pool::get_global_pools() {
            pools.candle_pool.put_candle($candle);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_pool_basic() {
        let pool: ObjectPool<Vec<i32>> = ObjectPool::new(4);
        
        // Get an object (should create new)
        let mut obj1 = pool.get();
        obj1.push(42);
        
        // Return it to pool
        pool.put(obj1);
        
        // Get another object (should reuse)
        let obj2 = pool.get();
        assert!(obj2.is_empty()); // Should be reset to default
        
        let stats = pool.stats();
        assert_eq!(stats.created_count, 1);
        assert_eq!(stats.reused_count, 1);
        assert_eq!(stats.hit_rate(), 0.5);
    }

    #[test]
    fn test_vec_pool() {
        let pool = VecPool::new();
        
        // Test different capacities
        let vec1 = pool.get_vec(10);   // Should use pool 0
        let vec2 = pool.get_vec(50);   // Should use pool 1 
        let vec3 = pool.get_vec(200);  // Should use pool 2
        let vec4 = pool.get_vec(500);  // Should use pool 3
        
        assert!(vec1.capacity() >= 10);
        assert!(vec2.capacity() >= 50);
        assert!(vec3.capacity() >= 200);
        assert!(vec4.capacity() >= 500);
        
        // Return to pools
        pool.put_vec(vec1);
        pool.put_vec(vec2);
        pool.put_vec(vec3);
        pool.put_vec(vec4);
        
        // Verify pools have objects
        let stats = pool.stats();
        assert!(stats.iter().any(|s| s.reused_count > 0));
    }

    #[test]
    fn test_candle_pool() {
        let pool = CandlePool::new();
        
        let candle1 = pool.get_candle();
        pool.put_candle(candle1);
        
        let _candle2 = pool.get_candle(); // Should reuse
        
        let stats = pool.stats();
        assert_eq!(stats.reused_count, 1);
    }

    #[test]
    fn test_global_pools() {
        let pools = init_global_pools();
        
        // Test macro usage
        let vec = get_pooled_vec!(100);
        assert!(vec.capacity() >= 100);
        put_pooled_vec!(vec);
        
        let candle = get_pooled_candle!();
        put_pooled_candle!(candle);
        
        // Verify stats
        let report = pools.stats_report();
        assert!(report.contains("Object Pool Statistics"));
    }

    #[test]
    fn test_pool_stats() {
        let pool: ObjectPool<String> = ObjectPool::new(2);
        
        let obj1 = pool.get();
        let obj2 = pool.get();
        
        pool.put(obj1);
        pool.put(obj2);
        
        let _obj3 = pool.get(); // Reuse
        let _obj4 = pool.get(); // Reuse
        
        let stats = pool.stats();
        assert_eq!(stats.created_count, 2);
        assert_eq!(stats.reused_count, 2);
        assert_eq!(stats.hit_rate(), 0.5);
    }
}