use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use data_feeder::common::shared_data::{
    intern_symbol, shared_candle, timeframe_to_string
};
use data_feeder::historical::structs::FuturesOHLCVCandle;
use data_feeder::technical_analysis::{
    actors::indicator::SymbolIndicatorState,
    structs::{TechnicalAnalysisConfig, TrendDirection, IndicatorOutput},
    simd_math::vectorized_ema_batch_update,
    fast_serialization::SerializationBuffer,
};
use std::sync::Arc;

fn bench_string_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_operations");
    
    // Benchmark string interning vs to_string
    group.bench_function("string_interning", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let _symbol = intern_symbol(black_box("BTCUSDT"));
            }
        })
    });
    
    group.bench_function("string_to_string", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let _symbol = black_box("BTCUSDT").to_string();
            }
        })
    });
    
    // Benchmark timeframe conversion
    group.bench_function("timeframe_optimized", |b| {
        b.iter(|| {
            for tf in [60, 300, 900, 3600, 14400, 86400] {
                let _s = timeframe_to_string(black_box(tf));
            }
        })
    });
    
    group.bench_function("timeframe_to_string", |b| {
        b.iter(|| {
            for tf in [60, 300, 900, 3600, 14400, 86400] {
                let _s = black_box(tf).to_string();
            }
        })
    });
    
    group.finish();
}

fn bench_candle_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("candle_operations");
    
    // Create test candle
    let candle = FuturesOHLCVCandle {
        open_time: 1640995200000,
        close_time: 1640995260000,
        open: 47000.0,
        high: 47100.0,
        low: 46900.0,
        close: 47050.0,
        volume: 1.25,
        number_of_trades: 15,
        taker_buy_base_asset_volume: 0.8,
        closed: true,
    };
    
    // Benchmark Arc vs Clone for candles
    group.bench_function("candle_arc_sharing", |b| {
        let shared = shared_candle(candle.clone());
        b.iter(|| {
            for _ in 0..1000 {
                let _clone = black_box(Arc::clone(&shared));
            }
        })
    });
    
    group.bench_function("candle_direct_clone", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let _clone = black_box(candle.clone());
            }
        })
    });
    
    group.finish();
}

fn bench_database_key_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("database_keys");
    
    // Benchmark key creation with interning vs String
    group.bench_function("interned_keys", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let _key = (intern_symbol(black_box("BTCUSDT")), black_box(60u64));
            }
        })
    });
    
    group.bench_function("string_keys", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let _key = (black_box("BTCUSDT").to_string(), black_box(60u64));
            }
        })
    });
    
    group.finish();
}

fn bench_memory_allocation_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocation");
    group.throughput(Throughput::Elements(1000));
    
    // Simulate creating many candles with different approaches
    let candle = FuturesOHLCVCandle {
        open_time: 1640995200000,
        close_time: 1640995260000,
        open: 47000.0,
        high: 47100.0,
        low: 46900.0,
        close: 47050.0,
        volume: 1.25,
        number_of_trades: 15,
        taker_buy_base_asset_volume: 0.8,
        closed: true,
    };
    
    group.bench_function("vec_of_arcs", |b| {
        b.iter(|| {
            let mut candles = Vec::new();
            for _ in 0..1000 {
                candles.push(shared_candle(black_box(candle.clone())));
            }
            black_box(candles)
        })
    });
    
    group.bench_function("vec_of_candles", |b| {
        b.iter(|| {
            let mut candles = Vec::new();
            for _ in 0..1000 {
                candles.push(black_box(candle.clone()));
            }
            black_box(candles)
        })
    });
    
    group.finish();
}

/// Benchmark SIMD EMA batch processing performance (PHASE 2 optimization)
fn bench_simd_ema_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_ema_operations");
    
    // Test data for different batch sizes to validate SIMD efficiency
    let batch_sizes = vec![2, 4, 8, 16, 32];
    
    for batch_size in batch_sizes {
        let price = 50000.0;
        let alphas: Vec<f64> = (0..batch_size).map(|i| 0.1 + (i as f64 * 0.01)).collect();
        let betas: Vec<f64> = (0..batch_size).map(|i| 0.9 - (i as f64 * 0.01)).collect();
        let prev_emas: Vec<f64> = (0..batch_size).map(|i| 49000.0 + (i as f64 * 100.0)).collect();
        
        group.bench_with_input(
            BenchmarkId::new("vectorized_ema_batch", batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    let mut test_emas = prev_emas.clone();
                    black_box(vectorized_ema_batch_update(
                        black_box(price),
                        black_box(&alphas),
                        black_box(&betas),
                        black_box(&mut test_emas),
                    ))
                })
            },
        );
    }
    
    group.finish();
}

/// Benchmark optimized indicator output generation (PHASE 3 optimization)
fn bench_indicator_output_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("indicator_output_generation");
    
    // Create realistic test configuration
    let config = TechnicalAnalysisConfig {
        symbols: vec!["BTCUSDT".to_string()],
        min_history_days: 30,
        ema_periods: vec![21, 89],
        timeframes: vec![60, 300, 900, 3600, 14400],
        volume_lookback_days: 30,
    };
    
    // Initialize indicator state with test data
    let mut state = SymbolIndicatorState::new(&config);
    
    // Add realistic test candles to simulate production environment
    let test_candles: Vec<FuturesOHLCVCandle> = (0..100).map(|i| {
        FuturesOHLCVCandle {
            open_time: i * 60_000,
            close_time: (i + 1) * 60_000 - 1,
            open: 50000.0 + (i as f64 * 0.1),
            high: 50100.0 + (i as f64 * 0.1),
            low: 49900.0 + (i as f64 * 0.1),
            close: 50000.0 + (i as f64 * 0.1),
            volume: 1000.0 + (i as f64 * 0.5),
            number_of_trades: (100 + i) as u64,
            taker_buy_base_asset_volume: 500.0 + (i as f64 * 0.25),
            closed: true,
        }
    }).collect();
    
    // Initialize with historical data for realistic state
    for candle in &test_candles {
        state.process_candle(60, candle);
    }
    
    // Benchmark the optimized output generation
    group.bench_function("generate_output_optimized", |b| {
        b.iter(|| {
            black_box(state.generate_output(black_box("BTCUSDT")))
        })
    });
    
    group.finish();
}

/// Benchmark fast serialization vs serde_json (PHASE 3 optimization)
fn bench_fast_serialization(c: &mut Criterion) {
    let output = IndicatorOutput {
        symbol: "BTCUSDT".to_string(),
        timestamp: 1672531200000,
        close_5m: Some(50000.0),
        close_15m: Some(50010.0),
        close_60m: Some(50020.0),
        close_4h: Some(50050.0),
        ema21_1min: Some(49950.0),
        ema89_1min: Some(49900.0),
        ema89_5min: Some(49910.0),
        ema89_15min: Some(49920.0),
        ema89_1h: Some(49930.0),
        ema89_4h: Some(49940.0),
        trend_1min: TrendDirection::Buy,
        trend_5min: TrendDirection::Buy,
        trend_15min: TrendDirection::Neutral,
        trend_1h: TrendDirection::Sell,
        trend_4h: TrendDirection::Neutral,
        max_volume: Some(10000.0),
        max_volume_price: Some(50025.0),
        max_volume_time: Some("2023-01-01T00:00:00Z".to_string()),
        max_volume_trend: Some(TrendDirection::Buy),
        volume_quantiles: None,
    };
    
    // Benchmark our custom fast serialization
    c.bench_function("fast_serialization", |b| {
        b.iter(|| {
            let mut buffer = SerializationBuffer::new();
            let result = buffer.serialize_indicator_output(black_box(&output));
            black_box(result.len()) // Avoid returning reference
        })
    });
    
    // Compare with serde_json baseline for performance comparison
    c.bench_function("serde_json_serialization", |b| {
        b.iter(|| {
            black_box(serde_json::to_string(black_box(&output)).unwrap())
        })
    });
}

/// Benchmark complete processing pipeline (end-to-end performance)
fn bench_full_processing_pipeline(c: &mut Criterion) {
    let config = TechnicalAnalysisConfig {
        symbols: vec!["BTCUSDT".to_string()],
        min_history_days: 30,
        ema_periods: vec![21, 89],
        timeframes: vec![60, 300, 900, 3600, 14400],
        volume_lookback_days: 30,
    };
    
    let mut state = SymbolIndicatorState::new(&config);
    
    // Realistic candle for processing (production-like data)
    let candle = FuturesOHLCVCandle {
        open_time: 1672531200000,
        close_time: 1672531259999,
        open: 50000.0,
        high: 50100.0,
        low: 49900.0,
        close: 50050.0,
        volume: 1000.0,
        number_of_trades: 150,
        taker_buy_base_asset_volume: 600.0,
        closed: true,
    };
    
    // Initialize with historical data for realistic benchmarking
    for i in 0..100 {
        let historical_candle = FuturesOHLCVCandle {
            close: 50000.0 + (i as f64),
            ..candle
        };
        state.process_candle(60, &historical_candle);
    }
    
    // Benchmark complete pipeline: candle processing + output generation
    c.bench_function("full_processing_pipeline", |b| {
        b.iter(|| {
            // Complete end-to-end pipeline
            state.process_candle(60, black_box(&candle));
            black_box(state.generate_output(black_box("BTCUSDT")))
        })
    });
}

criterion_group!(
    benches,
    bench_string_operations,
    bench_candle_operations,
    bench_database_key_creation,
    bench_memory_allocation_patterns,
    bench_simd_ema_operations,
    bench_indicator_output_generation,
    bench_fast_serialization,
    bench_full_processing_pipeline
);
criterion_main!(benches);