use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use data_feeder::websocket::actor::WebSocketActor;
use data_feeder::websocket::types::StreamType;
use data_feeder::historical::structs::FuturesOHLCVCandle;
use data_feeder::common::shared_data::{shared_candle, intern_symbol};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn create_test_candle(timestamp: i64) -> FuturesOHLCVCandle {
    FuturesOHLCVCandle {
        open_time: timestamp - 60000,
        close_time: timestamp,
        open: 50000.0,
        high: 50100.0,
        low: 49900.0,
        close: 50050.0,
        volume: 1.5,
        number_of_trades: 100,
        taker_buy_base_asset_volume: 0.8,
        closed: true,
    }
}

fn benchmark_symbol_interning(c: &mut Criterion) {
    let mut group = c.benchmark_group("symbol_interning");
    
    let symbols = vec!["BTCUSDT", "ETHUSDT", "ADAUSDT", "BNBUSDT", "XRPUSDT"];
    
    group.bench_function("intern_symbols", |b| {
        b.iter(|| {
            for symbol in &symbols {
                black_box(intern_symbol(symbol));
            }
        })
    });
    
    group.bench_function("string_allocation", |b| {
        b.iter(|| {
            for symbol in &symbols {
                black_box(symbol.to_string());
            }
        })
    });
    
    group.finish();
}

fn benchmark_candle_storage(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("candle_storage");
    
    for num_candles in [1, 10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("shared_candle_batch", num_candles),
            num_candles,
            |b, &num_candles| {
                b.iter(|| {
                    rt.block_on(async {
                        let temp_dir = TempDir::new().unwrap();
                        let mut actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
                        
                        // Initialize database
                        actor.init_symbol_db("BTCUSDT").unwrap();
                        
                        // Create test candles
                        let base_time = chrono::Utc::now().timestamp_millis();
                        for i in 0..num_candles {
                            let candle = create_test_candle(base_time + (i * 60000));
                            let shared = shared_candle(candle);
                            black_box(actor.store_candle("BTCUSDT", &shared, true).unwrap());
                        }
                    })
                })
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("individual_candle_storage", num_candles),
            num_candles,
            |b, &num_candles| {
                b.iter(|| {
                    rt.block_on(async {
                        let temp_dir = TempDir::new().unwrap();
                        let mut actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
                        
                        // Initialize database
                        actor.init_symbol_db("BTCUSDT").unwrap();
                        
                        // Simulate old approach with immediate storage
                        let base_time = chrono::Utc::now().timestamp_millis();
                        for i in 0..num_candles {
                            let candle = create_test_candle(base_time + (i * 60000));
                            // Force immediate processing by setting batch time to past
                            actor.last_batch_time = Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
                            let shared = shared_candle(candle);
                            black_box(actor.store_candle("BTCUSDT", &shared, true).unwrap());
                        }
                    })
                })
            },
        );
    }
    
    group.finish();
}

fn benchmark_recent_candles_access(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("recent_candles_access");
    
    // Setup actor with data
    let temp_dir = TempDir::new().unwrap();
    let mut actor = rt.block_on(async {
        let mut actor = WebSocketActor::new(temp_dir.path().to_path_buf()).unwrap();
        actor.init_symbol_db("BTCUSDT").unwrap();
        
        // Fill with test data
        let base_time = chrono::Utc::now().timestamp_millis();
        for i in 0..1000 {
            let candle = create_test_candle(base_time + (i * 60000));
            let shared = shared_candle(candle);
            actor.store_candle("BTCUSDT", &shared, true).unwrap();
        }
        
        actor
    });
    
    for limit in [1, 10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("get_recent_candles", limit),
            limit,
            |b, &limit| {
                b.iter(|| {
                    black_box(actor.get_recent_candles("BTCUSDT", limit));
                })
            },
        );
    }
    
    group.finish();
}

fn benchmark_memory_usage(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("memory_usage");
    
    group.bench_function("arc_vs_clone", |b| {
        b.iter(|| {
            rt.block_on(async {
                let candle = create_test_candle(chrono::Utc::now().timestamp_millis());
                
                // Test Arc sharing (optimized approach)
                let shared = shared_candle(candle.clone());
                let mut arcs = Vec::new();
                for _ in 0..1000 {
                    arcs.push(Arc::clone(&shared));
                }
                black_box(arcs);
                
                // Test cloning (old approach)
                let mut clones = Vec::new();
                for _ in 0..1000 {
                    clones.push(candle.clone());
                }
                black_box(clones);
            })
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_symbol_interning,
    benchmark_candle_storage,
    benchmark_recent_candles_access,
    benchmark_memory_usage
);
criterion_main!(benches);