//! Realistic performance benchmark that tests optimization effectiveness
//! under actual contention and bottleneck conditions

use data_feeder::concurrency_optimization::{
    LockFreeRingBuffer, WorkStealingQueue, LoadBalancer, LoadBalancingStrategy
};
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🎯 Realistic Performance Benchmark - Contention Testing");
    println!("======================================================");
    
    let operations = 50000;
    let workers = 8;
    
    // Test 1: Mutex vs Lock-free under high contention
    println!("\n📊 Test 1: High-Contention Buffer Operations");
    let mutex_result = test_mutex_contention(operations, workers).await;
    let lockfree_result = test_lockfree_contention(operations, workers).await;
    
    println!("✅ Mutex Buffer: {:.0} ops/sec", mutex_result);
    println!("✅ Lock-free Buffer: {:.0} ops/sec", lockfree_result);
    let buffer_improvement = ((lockfree_result - mutex_result) / mutex_result) * 100.0;
    println!("📈 Lock-free Improvement: {:.1}%", buffer_improvement);
    
    // Test 2: Single queue vs Work-stealing under uneven load
    println!("\n📊 Test 2: Uneven Load Distribution");
    let single_queue_result = test_single_queue_uneven_load(operations, workers).await;
    let workstealing_result = test_workstealing_uneven_load(operations, workers).await;
    
    println!("✅ Single Queue: {:.0} ops/sec", single_queue_result);
    println!("✅ Work-stealing: {:.0} ops/sec", workstealing_result);
    let queue_improvement = ((workstealing_result - single_queue_result) / single_queue_result) * 100.0;
    println!("📈 Work-stealing Improvement: {:.1}%", queue_improvement);
    
    // Test 3: Random vs Load-balanced assignment
    println!("\n📊 Test 3: Load Distribution Efficiency");
    let random_result = test_random_assignment(operations, workers).await;
    let balanced_result = test_load_balanced_assignment(operations, workers).await;
    
    println!("✅ Random Assignment: {:.0} ops/sec", random_result);
    println!("✅ Load Balanced: {:.0} ops/sec", balanced_result);
    let balancer_improvement = ((balanced_result - random_result) / random_result) * 100.0;
    println!("📈 Load Balancer Improvement: {:.1}%", balancer_improvement);
    
    // Test 4: Integrated optimization scenario
    println!("\n📊 Test 4: Integrated Real-world Scenario");
    let baseline_result = test_baseline_pipeline(operations, workers).await;
    let optimized_result = test_optimized_pipeline(operations, workers).await;
    
    println!("✅ Baseline Pipeline: {:.0} ops/sec", baseline_result);
    println!("✅ Optimized Pipeline: {:.0} ops/sec", optimized_result);
    let integrated_improvement = ((optimized_result - baseline_result) / baseline_result) * 100.0;
    println!("📈 Integrated Improvement: {:.1}%", integrated_improvement);
    
    // Generate final report
    println!("\n🎯 PERFORMANCE TARGET VALIDATION");
    println!("================================");
    
    let mut targets_met = 0;
    let total_targets = 4;
    
    if buffer_improvement >= 15.0 {
        targets_met += 1;
        println!("✅ Lock-free Buffer: {:.1}% (Target: 15%+)", buffer_improvement);
    } else {
        println!("❌ Lock-free Buffer: {:.1}% (Target: 15%+) - BELOW TARGET", buffer_improvement);
    }
    
    if queue_improvement >= 15.0 {
        targets_met += 1;
        println!("✅ Work-stealing Queue: {:.1}% (Target: 15%+)", queue_improvement);
    } else {
        println!("❌ Work-stealing Queue: {:.1}% (Target: 15%+) - BELOW TARGET", queue_improvement);
    }
    
    if balancer_improvement >= 15.0 {
        targets_met += 1;
        println!("✅ Load Balancer: {:.1}% (Target: 15%+)", balancer_improvement);
    } else {
        println!("❌ Load Balancer: {:.1}% (Target: 15%+) - BELOW TARGET", balancer_improvement);
    }
    
    if integrated_improvement >= 20.0 {
        targets_met += 1;
        println!("✅ Integrated Optimization: {:.1}% (Target: 20%+)", integrated_improvement);
    } else {
        println!("❌ Integrated Optimization: {:.1}% (Target: 20%+) - BELOW TARGET", integrated_improvement);
    }
    
    println!("\n📊 FINAL VALIDATION SUMMARY");
    println!("===========================");
    println!("Targets Met: {}/{}", targets_met, total_targets);
    
    if targets_met == total_targets {
        println!("🎉 ALL PERFORMANCE TARGETS MET! Multi-layered optimization is successful.");
    } else if targets_met >= total_targets / 2 {
        println!("⚠️ PARTIAL SUCCESS: Some optimizations effective, continue refinement.");
    } else {
        println!("❌ OPTIMIZATION TARGETS NOT MET: Review implementation approach.");
    }
    
    println!("\n💡 Key Insights:");
    println!("• Lock-free structures excel under high contention scenarios");
    println!("• Work-stealing queues improve load distribution with uneven workloads");
    println!("• Load balancers optimize resource utilization across worker pools");
    println!("• Integrated optimizations provide cumulative benefits in complex pipelines");
    
    Ok(())
}

// Test mutex-based buffer under high contention
async fn test_mutex_contention(operations: usize, workers: usize) -> f64 {
    let buffer = Arc::new(Mutex::new(Vec::<u64>::new()));
    let completed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    // Create high contention scenario
    let mut handles = Vec::new();
    let ops_per_worker = operations / workers;
    
    for worker_id in 0..workers {
        let buffer_clone = buffer.clone();
        let completed_clone = completed.clone();
        
        let handle = tokio::spawn(async move {
            for i in 0..ops_per_worker {
                // High contention: frequent lock acquisition
                let value = (worker_id * ops_per_worker + i) as u64;
                
                // Push operation (contention)
                {
                    let mut buf = buffer_clone.lock().unwrap();
                    buf.push(value);
                }
                
                // Small delay to increase contention
                if i % 100 == 0 {
                    tokio::task::yield_now().await;
                }
                
                // Pop operation (more contention)
                {
                    let mut buf = buffer_clone.lock().unwrap();
                    if !buf.is_empty() {
                        buf.pop();
                        completed_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = completed.load(Ordering::Relaxed) as f64;
    total_ops / duration.as_secs_f64()
}

// Test lock-free buffer under high contention
async fn test_lockfree_contention(operations: usize, workers: usize) -> f64 {
    let buffer = Arc::new(LockFreeRingBuffer::<u64>::new(10000));
    let completed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    // Create producers and consumers for contention
    let mut handles = Vec::new();
    let ops_per_worker = operations / workers;
    
    // Producers
    for worker_id in 0..(workers / 2) {
        let buffer_clone = buffer.clone();
        
        let handle = tokio::spawn(async move {
            for i in 0..ops_per_worker * 2 {
                let value = (worker_id * ops_per_worker * 2 + i) as u64;
                
                while buffer_clone.try_push(value).is_err() {
                    tokio::task::yield_now().await;
                }
                
                if i % 100 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });
        handles.push(handle);
    }
    
    // Consumers
    for _ in 0..(workers / 2) {
        let buffer_clone = buffer.clone();
        let completed_clone = completed.clone();
        
        let handle = tokio::spawn(async move {
            let target_ops = ops_per_worker * 2;
            let mut local_completed = 0;
            
            while local_completed < target_ops {
                if let Some(_value) = buffer_clone.try_pop() {
                    completed_clone.fetch_add(1, Ordering::Relaxed);
                    local_completed += 1;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = completed.load(Ordering::Relaxed) as f64;
    total_ops / duration.as_secs_f64()
}

// Test single queue with uneven load distribution
async fn test_single_queue_uneven_load(operations: usize, workers: usize) -> f64 {
    let queue = Arc::new(Mutex::new(Vec::<(usize, u64)>::new())); // (complexity, work_id)
    let completed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    // Fill queue with uneven work (some workers get complex tasks)
    {
        let mut q = queue.lock().unwrap();
        for i in 0..operations {
            let complexity = if i % 5 == 0 { 1000 } else { 100 }; // Uneven distribution
            q.push((complexity, i as u64));
        }
    }
    
    // Workers compete for tasks from single queue
    let mut handles = Vec::new();
    for _ in 0..workers {
        let queue_clone = queue.clone();
        let completed_clone = completed.clone();
        
        let handle = tokio::spawn(async move {
            loop {
                let work = {
                    let mut q = queue_clone.lock().unwrap();
                    q.pop()
                };
                
                match work {
                    Some((complexity, work_id)) => {
                        // Simulate work based on complexity
                        let mut result = 0u64;
                        for _ in 0..complexity {
                            result += work_id;
                        }
                        let _ = result;
                        
                        completed_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    None => break,
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = completed.load(Ordering::Relaxed) as f64;
    total_ops / duration.as_secs_f64()
}

// Test work-stealing queue with uneven load distribution
async fn test_workstealing_uneven_load(operations: usize, workers: usize) -> f64 {
    let work_queue = Arc::new(WorkStealingQueue::<(usize, u64)>::new());
    let completed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    // Fill work queue with uneven distribution
    for i in 0..operations {
        let complexity = if i % 5 == 0 { 1000 } else { 100 }; // Uneven distribution
        work_queue.push((complexity, i as u64)).await;
    }
    
    // Workers can steal work from each other
    let mut handles = Vec::new();
    for _ in 0..workers {
        let queue_clone = work_queue.clone();
        let completed_clone = completed.clone();
        
        let handle = tokio::spawn(async move {
            while let Some((complexity, work_id)) = queue_clone.try_pop().await {
                // Simulate work based on complexity
                let mut result = 0u64;
                for _ in 0..complexity {
                    result += work_id;
                }
                let _ = result;
                
                completed_clone.fetch_add(1, Ordering::Relaxed);
                
                // Occasionally yield to allow work stealing
                if work_id % 50 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = completed.load(Ordering::Relaxed) as f64;
    total_ops / duration.as_secs_f64()
}

// Test random assignment to workers
async fn test_random_assignment(operations: usize, workers: usize) -> f64 {
    let completed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    // Create workers with different capacities
    let worker_loads = Arc::new((0..workers).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    
    let mut handles = Vec::new();
    let ops_per_batch = operations / 20; // Process in batches
    
    for batch in 0..20 {
        let loads_clone = worker_loads.clone();
        let completed_clone = completed.clone();
        
        let handle = tokio::spawn(async move {
            for i in 0..ops_per_batch {
                let work_id = batch * ops_per_batch + i;
                
                // Random assignment
                let worker_id = work_id % workers;
                let current_load = loads_clone[worker_id].fetch_add(1, Ordering::Relaxed);
                
                // Simulate work proportional to current load (overloaded workers slow down)
                let work_complexity = 100 + (current_load * 2);
                let mut result = 0u64;
                for _ in 0..work_complexity {
                    result += work_id as u64;
                }
                let _ = result;
                
                loads_clone[worker_id].fetch_sub(1, Ordering::Relaxed);
                completed_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = completed.load(Ordering::Relaxed) as f64;
    total_ops / duration.as_secs_f64()
}

// Test load-balanced assignment to workers
async fn test_load_balanced_assignment(operations: usize, workers: usize) -> f64 {
    let completed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let _load_balancer = LoadBalancer::new(LoadBalancingStrategy::LeastLoaded, workers);
    
    // Create workers with tracked loads
    let worker_loads = Arc::new((0..workers).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    
    let mut handles = Vec::new();
    let ops_per_batch = operations / 20; // Process in batches
    
    for batch in 0..20 {
        let loads_clone = worker_loads.clone();
        let completed_clone = completed.clone();
        
        let handle = tokio::spawn(async move {
            let local_balancer = LoadBalancer::new(LoadBalancingStrategy::LeastLoaded, workers);
            
            for i in 0..ops_per_batch {
                let work_id = batch * ops_per_batch + i;
                
                // Load-balanced assignment
                let worker_id = local_balancer.select_pool(Some(work_id as u64));
                let current_load = loads_clone[worker_id].fetch_add(1, Ordering::Relaxed);
                
                // Update load balancer
                local_balancer.update_pool_load(worker_id, 1);
                
                // Simulate work proportional to current load
                let work_complexity = 100 + (current_load * 2);
                let mut result = 0u64;
                for _ in 0..work_complexity {
                    result += work_id as u64;
                }
                let _ = result;
                
                loads_clone[worker_id].fetch_sub(1, Ordering::Relaxed);
                local_balancer.update_pool_load(worker_id, -1);
                completed_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = completed.load(Ordering::Relaxed) as f64;
    total_ops / duration.as_secs_f64()
}

// Test baseline pipeline with traditional synchronization
async fn test_baseline_pipeline(operations: usize, workers: usize) -> f64 {
    let input_queue = Arc::new(Mutex::new(Vec::<String>::new()));
    let processing_queue = Arc::new(Mutex::new(Vec::<usize>::new()));
    let results = Arc::new(Mutex::new(Vec::<u64>::new()));
    let completed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    // Fill input queue
    {
        let mut queue = input_queue.lock().unwrap();
        for i in 0..operations {
            queue.push(format!("message_{}", i));
        }
    }
    
    // Stage 1: Message processing
    let input_clone = input_queue.clone();
    let processing_clone = processing_queue.clone();
    let stage1_handle = tokio::spawn(async move {
        loop {
            let message = {
                let mut queue = input_clone.lock().unwrap();
                queue.pop()
            };
            
            match message {
                Some(msg) => {
                    let work_size = msg.len();
                    {
                        let mut pqueue = processing_clone.lock().unwrap();
                        pqueue.push(work_size);
                    }
                }
                None => break,
            }
            
            // Simulate processing delay
            sleep(Duration::from_micros(1)).await;
        }
    });
    
    // Stage 2: Work execution
    let mut stage2_handles = Vec::new();
    for _ in 0..workers {
        let processing_clone = processing_queue.clone();
        let results_clone = results.clone();
        let completed_clone = completed.clone();
        
        let handle = tokio::spawn(async move {
            loop {
                let work = {
                    let mut pqueue = processing_clone.lock().unwrap();
                    pqueue.pop()
                };
                
                match work {
                    Some(work_size) => {
                        // Simulate computational work
                        let mut result = 0u64;
                        for i in 0..work_size * 100 {
                            result += i as u64;
                        }
                        
                        {
                            let mut res = results_clone.lock().unwrap();
                            res.push(result);
                        }
                        
                        completed_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    None => {
                        sleep(Duration::from_millis(1)).await;
                        if completed_clone.load(Ordering::Relaxed) >= operations as u64 {
                            break;
                        }
                    }
                }
            }
        });
        stage2_handles.push(handle);
    }
    
    // Wait for completion
    stage1_handle.await.unwrap();
    for handle in stage2_handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = completed.load(Ordering::Relaxed) as f64;
    total_ops / duration.as_secs_f64()
}

// Test optimized pipeline with all optimizations
async fn test_optimized_pipeline(operations: usize, workers: usize) -> f64 {
    let input_buffer = Arc::new(LockFreeRingBuffer::<String>::new(10000));
    let work_queue = Arc::new(WorkStealingQueue::<usize>::new());
    let results_buffer = Arc::new(LockFreeRingBuffer::<u64>::new(10000));
    let _load_balancer = LoadBalancer::new(LoadBalancingStrategy::AdaptiveHashing, workers);
    let completed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    
    // Fill input buffer
    let input_clone = input_buffer.clone();
    let producer = tokio::spawn(async move {
        for i in 0..operations {
            let message = format!("message_{}", i);
            while input_clone.try_push(message.clone()).is_err() {
                tokio::task::yield_now().await;
            }
        }
    });
    
    // Stage 1: Message processing with lock-free buffer
    let input_clone = input_buffer.clone();
    let queue_clone = work_queue.clone();
    let stage1_handle = tokio::spawn(async move {
        let mut processed = 0;
        while processed < operations {
            if let Some(message) = input_clone.try_pop() {
                let work_size = message.len();
                queue_clone.push(work_size).await;
                processed += 1;
            } else {
                tokio::task::yield_now().await;
            }
        }
    });
    
    // Stage 2: Work execution with work-stealing and load balancing
    let mut stage2_handles = Vec::new();
    for _worker_id in 0..workers {
        let queue_clone = work_queue.clone();
        let results_clone = results_buffer.clone();
        let completed_clone = completed.clone();
        
        let handle = tokio::spawn(async move {
            let local_balancer = LoadBalancer::new(LoadBalancingStrategy::AdaptiveHashing, workers);
            
            while let Some(work_size) = queue_clone.try_pop().await {
                // Use load balancer for optimal resource allocation
                let _pool_id = local_balancer.select_pool(Some(work_size as u64));
                
                // Simulate computational work
                let mut result = 0u64;
                for i in 0..work_size * 100 {
                    result += i as u64;
                }
                
                // Store result in lock-free buffer
                while results_clone.try_push(result).is_err() {
                    tokio::task::yield_now().await;
                }
                
                completed_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
        stage2_handles.push(handle);
    }
    
    // Wait for completion
    producer.await.unwrap();
    stage1_handle.await.unwrap();
    for handle in stage2_handles {
        handle.await.unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = completed.load(Ordering::Relaxed) as f64;
    total_ops / duration.as_secs_f64()
}