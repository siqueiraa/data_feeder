//! Realistic trading data serialization benchmark for Story 4.4
//! Tests output generation under high-volume professional trading scenarios

use std::time::{Duration, Instant};
use chrono::NaiveDate;
use rust_decimal_macros::dec;

use data_feeder::volume_profile::{
    calculator::DailyVolumeProfile,
    structs::{VolumeProfileConfig, PriceIncrementMode, VolumeDistributionMode, ValueAreaCalculationMode, VolumeProfileCalculationMode, UpdateFrequency}
};
use data_feeder::historical::structs::FuturesOHLCVCandle;
use data_feeder::technical_analysis::structs::{IndicatorOutput, TrendDirection};

fn main() {
    println!("=== Realistic Trading Data Serialization Benchmark ===");
    println!("🎯 Goal: Reproduce 30ms bottleneck with professional trading workloads");
    
    // Test different scenarios that could exhibit 30ms bottleneck
    test_high_frequency_trading_scenario();
    test_multi_asset_portfolio_scenario();  
    test_complex_market_profile_scenario();
    test_combined_technical_analysis_scenario();
    test_memory_pressure_scenario();
    
    println!("\n=== Summary: Path to 30ms Bottleneck Identified ===");
}

fn test_high_frequency_trading_scenario() {
    println!("\n=== Scenario 1: High-Frequency Trading (Multiple Assets) ===");
    
    let symbols = vec!["BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "SOLUSDT"];
    let mut profiles = Vec::new();
    
    // Create profiles for multiple assets
    let start_creation = Instant::now();
    for symbol in &symbols {
        let profile = create_professional_trading_profile(symbol, 1000); // 1000 price levels
        profiles.push(profile);
    }
    let creation_time = start_creation.elapsed();
    println!("📊 Multi-asset profile creation: {:?}", creation_time);
    
    // Serialize all profiles (simulating portfolio dashboard update)
    let start_serialization = Instant::now();
    let mut total_json_size = 0;
    let mut serialization_times = Vec::new();
    
    for (i, mut profile) in profiles.into_iter().enumerate() {
        let profile_start = Instant::now();
        let profile_data = profile.get_profile_data();
        let json_output = serde_json::to_string(&profile_data)
            .expect("Failed to serialize profile");
        let profile_time = profile_start.elapsed();
        
        serialization_times.push(profile_time);
        total_json_size += json_output.len();
        
        println!("📈 {}: {} levels, {:?}, {} KB", 
                 symbols[i], profile_data.price_levels.len(), profile_time, json_output.len() / 1024);
    }
    
    let total_serialization = start_serialization.elapsed();
    let avg_per_asset = total_serialization / symbols.len() as u32;
    
    println!("⏱️  Total portfolio serialization: {:?}", total_serialization);
    println!("📊 Average per asset: {:?}", avg_per_asset);
    println!("💾 Total portfolio JSON: {} MB", total_json_size / 1024 / 1024);
    
    if total_serialization > Duration::from_millis(30) {
        println!("🚨 BOTTLENECK REPRODUCED: Multi-asset serialization exceeds 30ms target");
    }
}

fn test_multi_asset_portfolio_scenario() {
    println!("\n=== Scenario 2: Portfolio Dashboard (10 Assets + Technical Analysis) ===");
    
    let symbols = vec!["BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "SOLUSDT", 
                      "LINKUSDT", "UNIUSDT", "LTCUSDT", "BCHUSDT", "XLMUSDT"];
    
    let start_combined = Instant::now();
    let mut combined_outputs = Vec::new();
    
    for symbol in &symbols {
        // Create both volume profile AND technical analysis output
        let mut volume_profile = create_professional_trading_profile(symbol, 800);
        let profile_data = volume_profile.get_profile_data();
        
        let technical_output = create_realistic_technical_analysis(symbol, Some(profile_data));
        combined_outputs.push(technical_output);
    }
    
    // Serialize complete portfolio dashboard
    let dashboard_start = Instant::now();
    let dashboard_json = serde_json::to_string(&combined_outputs)
        .expect("Failed to serialize dashboard");
    let dashboard_time = dashboard_start.elapsed();
    
    let total_time = start_combined.elapsed();
    
    println!("📊 Portfolio creation + analysis: {:?}", total_time);
    println!("📈 Dashboard serialization: {:?}", dashboard_time);
    println!("💾 Dashboard JSON size: {} MB", dashboard_json.len() / 1024 / 1024);
    println!("📏 Assets in portfolio: {}", symbols.len());
    
    if dashboard_time > Duration::from_millis(30) {
        println!("🚨 DASHBOARD BOTTLENECK: Portfolio serialization exceeds 30ms");
        println!("💡 This reproduces the Story 4.4 bottleneck scenario");
    }
}

fn test_complex_market_profile_scenario() {
    println!("\n=== Scenario 3: Complex Market Profile (Ultra-High Resolution) ===");
    
    // Test with extremely high price resolution (simulating professional market profile)
    let mut profile = create_ultra_high_resolution_profile("BTCUSDT", 2000); // 2000 price levels
    
    let profile_start = Instant::now();
    let profile_data = profile.get_profile_data();
    let profile_creation = profile_start.elapsed();
    
    let serialization_start = Instant::now();
    let json_output = serde_json::to_string(&profile_data)
        .expect("Failed to serialize complex profile");
    let serialization_time = serialization_start.elapsed();
    
    println!("📊 Ultra-high resolution profile creation: {:?}", profile_creation);
    println!("📈 Complex profile serialization: {:?}", serialization_time);
    println!("💾 Complex profile JSON: {} KB", json_output.len() / 1024);
    println!("📏 Price levels: {}", profile_data.price_levels.len());
    
    // Test repeated serialization (cache miss scenario)
    let iterations = 50;
    let batch_start = Instant::now();
    for i in 0..iterations {
        let _ = serde_json::to_string(&profile_data).unwrap();
        if i % 10 == 0 {
            println!("📈 Iteration {}: {:?} elapsed", i, batch_start.elapsed());
        }
    }
    let batch_time = batch_start.elapsed();
    let avg_batch_time = batch_time / iterations;
    
    println!("⏱️  Batch serialization (50x): {:?}", batch_time);
    println!("📊 Average per serialization: {:?}", avg_batch_time);
    
    if avg_batch_time > Duration::from_millis(1) {
        println!("🚨 COMPLEX PROFILE BOTTLENECK: Exceeds 1ms target per operation");
    }
}

fn test_combined_technical_analysis_scenario() {
    println!("\n=== Scenario 4: Combined Technical Analysis + Volume Profile ===");
    
    // Simulate real-time trading dashboard update with full technical analysis
    let symbols = vec!["BTCUSDT", "ETHUSDT"];
    
    let combined_start = Instant::now();
    let mut technical_outputs = Vec::new();
    
    for symbol in &symbols {
        // Create complex volume profile
        let mut volume_profile = create_professional_trading_profile(symbol, 1500);
        let profile_data = volume_profile.get_profile_data();
        
        // Create technical analysis with embedded volume profile
        let output = create_realistic_technical_analysis(symbol, Some(profile_data));
        technical_outputs.push(output);
    }
    
    // Serialize combined output (typical API response)
    let serialization_start = Instant::now();
    let combined_json = serde_json::to_string(&technical_outputs)
        .expect("Failed to serialize technical analysis");
    let serialization_time = serialization_start.elapsed();
    
    let total_time = combined_start.elapsed();
    
    println!("📊 Technical analysis + volume profiles: {:?}", total_time);
    println!("📈 Combined serialization: {:?}", serialization_time);  
    println!("💾 Combined JSON size: {} MB", combined_json.len() / 1024 / 1024);
    
    // Test the pipeline scenario mentioned in story (3.41s → 0.114s)
    let pipeline_iterations = 100;
    let pipeline_start = Instant::now();
    
    for _ in 0..pipeline_iterations {
        let _ = serde_json::to_string(&technical_outputs).unwrap();
    }
    
    let pipeline_time = pipeline_start.elapsed();
    let avg_pipeline = pipeline_time / pipeline_iterations;
    
    println!("\n=== Pipeline Performance Test ===");
    println!("📊 Pipeline time (100 ops): {:?}", pipeline_time);
    println!("⏱️  Average per operation: {:?}", avg_pipeline);
    
    // Check against Story 4.4 targets
    let target_total = Duration::from_millis(114); // 0.114s target
    let current_projected = avg_pipeline * pipeline_iterations;
    
    println!("🎯 Target pipeline time: {:?}", target_total);
    println!("📈 Current projected time: {:?}", current_projected);
    
    if current_projected > target_total {
        let improvement_needed = current_projected.as_millis() as f64 / target_total.as_millis() as f64;
        println!("❌ PIPELINE BOTTLENECK: {:.1}x improvement needed", improvement_needed);
        println!("💡 This confirms Story 4.4 performance requirements");
    }
}

fn test_memory_pressure_scenario() {
    println!("\n=== Scenario 5: Memory Pressure + Concurrent Serialization ===");
    
    // Simulate high memory pressure scenario that could cause 30ms delays
    let large_portfolios = 5;
    let assets_per_portfolio = 20;
    
    let stress_start = Instant::now();
    let mut all_outputs = Vec::new();
    
    for portfolio in 0..large_portfolios {
        let mut portfolio_outputs = Vec::new();
        
        for asset in 0..assets_per_portfolio {
            let symbol = format!("ASSET{}_{}", portfolio, asset);
            let mut profile = create_professional_trading_profile(&symbol, 600);
            let profile_data = profile.get_profile_data();
            let output = create_realistic_technical_analysis(&symbol, Some(profile_data));
            portfolio_outputs.push(output);
        }
        
        all_outputs.push(portfolio_outputs);
        
        if portfolio % 2 == 0 {
            println!("📊 Portfolio {} completed: {:?} elapsed", portfolio, stress_start.elapsed());
        }
    }
    
    // Serialize everything (memory pressure test)
    let serialization_start = Instant::now();
    let massive_json = serde_json::to_string(&all_outputs)
        .expect("Failed to serialize massive dataset");
    let massive_serialization = serialization_start.elapsed();
    
    let total_stress_time = stress_start.elapsed();
    
    println!("⏱️  Total stress test time: {:?}", total_stress_time);
    println!("📈 Massive serialization: {:?}", massive_serialization);
    println!("💾 Massive JSON size: {} MB", massive_json.len() / 1024 / 1024);
    println!("📏 Total assets: {}", large_portfolios * assets_per_portfolio);
    
    if massive_serialization > Duration::from_millis(30) {
        println!("🚨 MEMORY PRESSURE BOTTLENECK: Large dataset serialization exceeds 30ms");
        println!("💡 This could explain the 30ms bottleneck in production scenarios");
    }
}

fn create_professional_trading_profile(symbol: &str, target_levels: u32) -> DailyVolumeProfile {
    let config = VolumeProfileConfig {
        enabled: true,
        price_increment_mode: PriceIncrementMode::Adaptive,
        fixed_price_increment: dec!(0.001), // Higher precision
        min_price_increment: dec!(0.001),
        max_price_increment: dec!(1.0),
        target_price_levels: target_levels, // Variable complexity
        update_frequency: UpdateFrequency::EveryCandle,
        batch_size: 50,
        volume_distribution_mode: VolumeDistributionMode::HighLowWeighted,
        value_area_calculation_mode: ValueAreaCalculationMode::Traditional,
        value_area_percentage: dec!(70),
        calculation_mode: VolumeProfileCalculationMode::Volume,
        asset_overrides: Default::default(),
        historical_days: 60,
    };
    
    let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let mut profile = DailyVolumeProfile::new(symbol.to_string(), date, &config);
    
    // Add professional trading data volume (5000 candles for complexity)
    let base_price = match symbol {
        "BTCUSDT" => 45000.0,
        "ETHUSDT" => 3500.0,
        "ADAUSDT" => 1.2,
        _ => 100.0,
    };
    
    for i in 0..5000 {
        let time_offset = i * 12 * 1000; // 12-second intervals for high frequency
        let price_volatility = ((i as f64 * 0.01).sin() * 0.05 + (i as f64 * 0.003).cos() * 0.02) * base_price;
        let volume_spike = if i % 100 == 0 { 5.0 } else { 1.0 }; // Volume spikes
        
        let candle = FuturesOHLCVCandle {
            open_time: 1736985600000 + time_offset,
            close_time: 1736985611999 + time_offset,
            open: base_price + price_volatility,
            high: base_price + price_volatility + (base_price * 0.001),
            low: base_price + price_volatility - (base_price * 0.001),
            close: base_price + price_volatility + (base_price * 0.0005),
            volume: (200.0 + (i as f64 * 0.1)) * volume_spike,
            taker_buy_base_asset_volume: (100.0 + (i as f64 * 0.05)) * volume_spike,
            number_of_trades: (80 + (i % 200)) as u64,
            closed: true,
        };
        
        profile.add_candle(&candle);
    }
    
    profile
}

fn create_ultra_high_resolution_profile(symbol: &str, target_levels: u32) -> DailyVolumeProfile {
    let config = VolumeProfileConfig {
        enabled: true,
        price_increment_mode: PriceIncrementMode::Fixed,
        fixed_price_increment: dec!(0.0001), // Ultra-high precision
        min_price_increment: dec!(0.0001),
        max_price_increment: dec!(0.0001), 
        target_price_levels: target_levels,
        update_frequency: UpdateFrequency::EveryCandle,
        batch_size: 25,
        volume_distribution_mode: VolumeDistributionMode::HighLowWeighted,
        value_area_calculation_mode: ValueAreaCalculationMode::Traditional,
        value_area_percentage: dec!(70),
        calculation_mode: VolumeProfileCalculationMode::Volume,
        asset_overrides: Default::default(),
        historical_days: 60,
    };
    
    let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let mut profile = DailyVolumeProfile::new(symbol.to_string(), date, &config);
    
    // Create ultra-dense trading data
    for i in 0..8000 {
        let base_price = 50000.0;
        let micro_price_movement = (i as f64 * 0.001).sin() * 0.1; // Tiny price movements
        
        let candle = FuturesOHLCVCandle {
            open_time: 1736985600000 + (i * 5 * 1000), // 5-second candles
            close_time: 1736985604999 + (i * 5 * 1000),
            open: base_price + micro_price_movement,
            high: base_price + micro_price_movement + 0.05,
            low: base_price + micro_price_movement - 0.05,
            close: base_price + micro_price_movement + 0.02,
            volume: 50.0 + (i as f64 * 0.01),
            taker_buy_base_asset_volume: 25.0 + (i as f64 * 0.005),
            number_of_trades: 20 + (i % 50) as u64,
            closed: true,
        };
        
        profile.add_candle(&candle);
    }
    
    profile
}

fn create_realistic_technical_analysis(symbol: &str, volume_profile: Option<data_feeder::volume_profile::structs::VolumeProfileData>) -> IndicatorOutput {
    IndicatorOutput {
        symbol: symbol.to_string(),
        timestamp: 1736985600000,
        close_5m: Some(45000.0),
        close_15m: Some(44980.0),
        close_60m: Some(44950.0),
        close_4h: Some(44900.0),
        ema21_1min: Some(44995.0),
        ema89_1min: Some(44985.0),
        ema89_5min: Some(44975.0),
        ema89_15min: Some(44965.0),
        ema89_1h: Some(44955.0),
        ema89_4h: Some(44945.0),
        trend_1min: TrendDirection::Buy,
        trend_5min: TrendDirection::Buy,
        trend_15min: TrendDirection::Neutral,
        trend_1h: TrendDirection::Neutral,
        trend_4h: TrendDirection::Sell,
        max_volume: Some(1500.0),
        max_volume_price: Some(45020.0),
        max_volume_time: Some("2025-01-15T12:30:00Z".to_string()),
        max_volume_trend: Some(TrendDirection::Buy),
        volume_quantiles: None, // Simplified for benchmark
        #[cfg(feature = "volume_profile")]
        volume_profile,
        #[cfg(not(feature = "volume_profile"))]
        volume_profile: (),
    }
}