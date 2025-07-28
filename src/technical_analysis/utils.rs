use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};
use crate::technical_analysis::structs::{VolumeRecord, TrendDirection};
use heed::{Database};
use heed::types::{SerdeBincode, Str};
use std::path::PathBuf;
use tracing::{debug, info, warn};
use kameo::actor::ActorRef;
use crate::lmdb::{LmdbActor, LmdbActorMessage, LmdbActorResponse};
use crate::common::lmdb_config::open_lmdb_environment;
use crate::common::constants::CANDLES_DB_NAME;

/// Load recent candles from LMDB for initialization
/// Returns candles sorted by close_time (oldest first)
pub async fn load_recent_candles_from_db(
    symbol: &str,
    timeframe_seconds: u64,
    lookback_days: u32,
    base_path: &PathBuf,
    reference_timestamp: Option<i64>,
) -> Result<Vec<FuturesOHLCVCandle>, Box<dyn std::error::Error + Send + Sync>> {
    let db_name = format!("{}_{}", symbol, timeframe_seconds);
    let db_path = base_path.join(db_name);

    if !db_path.exists() {
        warn!("Database path does not exist: {}", db_path.display());
        return Ok(Vec::new());
    }

    // Use shared LMDB configuration to prevent "environment already opened with different options" errors
    let env = open_lmdb_environment(&db_path)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    let rtxn = env.read_txn()?;
    let candle_db: Database<Str, SerdeBincode<FuturesOHLCVCandle>> = 
        env.open_database(&rtxn, Some(CANDLES_DB_NAME))?.ok_or("Candles database not found")?;
    
    // Calculate cutoff time for lookback period using provided reference timestamp or current time
    let now = reference_timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    let cutoff_time = now - (lookback_days as i64 * 24 * 3600 * 1000);
    
    if reference_timestamp.is_some() {
        info!("🕐 TA loading using SYNCHRONIZED timestamp: {} (cutoff: {})", now, cutoff_time);
    } else {
        info!("🕐 TA loading using current timestamp: {} (cutoff: {})", now, cutoff_time);
    }
    
    // Add human-readable timestamp debugging for TA loading
    let now_str = chrono::DateTime::from_timestamp_millis(now)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("INVALID_TIME({})", now));
    let cutoff_str = chrono::DateTime::from_timestamp_millis(cutoff_time)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("INVALID_TIME({})", cutoff_time));
    info!("🕐 Human-readable TA loading: {} back to {} ({} days)", now_str, cutoff_str, lookback_days);

    let mut candles = Vec::new();
    let iter = candle_db.iter(&rtxn)?;

    for result in iter {
        let (key, candle) = result?;
        
        // Parse timestamp from key format: "timeframe:timestamp"
        if let Some(timestamp_str) = key.split(':').nth(1) {
            if let Ok(timestamp) = timestamp_str.parse::<i64>() {
                if timestamp >= cutoff_time {
                    candles.push(candle);
                }
            }
        }
    }

    // Sort by close_time (oldest first)
    candles.sort_by_key(|c| c.close_time);

    if !candles.is_empty() {
        let first_time = candles[0].open_time;
        let last_time = candles[candles.len()-1].close_time;
        debug!("Loaded {} recent candles for {} {}s (last {} days): {} to {}", 
              candles.len(), symbol, timeframe_seconds, lookback_days, first_time, last_time);
              
        // Debug the time range to understand boundary issues
        let time_span_hours = (last_time - first_time) / (1000 * 3600);
        debug!("📊 Historical data spans {} hours ({:.1} days)", time_span_hours, time_span_hours as f64 / 24.0);
    } else {
        info!("Loaded 0 candles for {} {}s (last {} days)", symbol, timeframe_seconds, lookback_days);
    }

    Ok(candles)
}

/// Load recent candles from LMDB using LmdbActor (prevents environment conflicts)
/// Returns candles sorted by close_time (oldest first)
pub async fn load_recent_candles_via_actor(
    symbol: &str,
    timeframe_seconds: u64,
    lookback_days: u32,
    lmdb_actor: &ActorRef<LmdbActor>,
    reference_timestamp: Option<i64>,
) -> Result<Vec<FuturesOHLCVCandle>, Box<dyn std::error::Error + Send + Sync>> {
    info!("🔧 Loading recent candles via LmdbActor for {} {}s (last {} days)", 
          symbol, timeframe_seconds, lookback_days);
    
    // Calculate cutoff time for lookback period using provided reference timestamp or current time
    let now = reference_timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    let cutoff_time = now - (lookback_days as i64 * 24 * 3600 * 1000);
    
    if reference_timestamp.is_some() {
        info!("🕐 TA loading using SYNCHRONIZED timestamp: {} (cutoff: {})", now, cutoff_time);
    } else {
        info!("🕐 TA loading using current timestamp: {} (cutoff: {})", now, cutoff_time);
    }
    
    // Get all candles from the database using LmdbActor
    let response = lmdb_actor
        .ask(LmdbActorMessage::GetCandles {
            symbol: symbol.to_string(),
            timeframe: timeframe_seconds,
            start: cutoff_time,
            end: now,
            limit: None,
        })
        .await
        .map_err(|e| format!("Failed to get candles from LmdbActor: {}", e))?;

    let candles = match response {
        LmdbActorResponse::Candles(candles) => candles,
        LmdbActorResponse::ErrorResponse(err) => {
            return Err(format!("LmdbActor returned error: {}", err).into());
        }
        _ => {
            return Err("Unexpected response from LmdbActor".into());
        }
    };

    // Sort by close_time (oldest first) - should already be sorted but ensure it
    let mut sorted_candles = candles;
    sorted_candles.sort_by_key(|c| c.close_time);

    if !sorted_candles.is_empty() {
        let first_time = sorted_candles[0].open_time;
        let last_time = sorted_candles[sorted_candles.len()-1].close_time;
        debug!("✅ Loaded {} recent candles via actor for {} {}s (last {} days): {} to {}", 
              sorted_candles.len(), symbol, timeframe_seconds, lookback_days, first_time, last_time);
              
        // Debug the time range to understand boundary issues
        let time_span_hours = (last_time - first_time) / (1000 * 3600);
        debug!("📊 Historical data spans {} hours ({:.1} days)", time_span_hours, time_span_hours as f64 / 24.0);
    } else {
        info!("✅ Loaded 0 candles via actor for {} {}s (last {} days)", symbol, timeframe_seconds, lookback_days);
    }

    Ok(sorted_candles)
}

/// Calculate the minimum number of 1-minute candles needed for initialization
pub fn calculate_min_candles_needed(
    timeframes: &[u64], 
    ema_periods: &[u32], 
    volume_lookback_days: u32
) -> usize {
    let max_ema_period = ema_periods.iter().max().unwrap_or(&89);
    let _max_timeframe = timeframes.iter().max().unwrap_or(&14400); // 4h in seconds
    
    // Calculate candles needed for each requirement
    let candles_for_ema = *max_ema_period as usize; // Just the EMA period, not multiplied by timeframe
    let candles_for_volume = (volume_lookback_days * 24 * 60) as usize; // 1-minute candles for volume analysis
    let candles_for_trends = 300; // Just need a few hundred candles for trends
    
    // Take the maximum requirement - volume is usually the limiting factor
    let min_needed = candles_for_ema
        .max(candles_for_volume)
        .max(candles_for_trends);
    
    // BUG FIX: Don't cap artificially low - let volume requirement drive the actual need
    // For 60 days: 60 * 24 * 60 = 86,400 candles is reasonable and expected
    let max_reasonable = (90 * 24 * 60) as usize; // Cap at 90 days max (129,600 candles)
    let with_buffer = min_needed.min(max_reasonable);
    
    info!("Minimum candles needed: EMA={}, Volume={}, Trends={}, Calculated={}, Final={}",
          candles_for_ema, candles_for_volume, candles_for_trends, min_needed, with_buffer);
    
    // Add detailed explanation when volume drives the requirement
    if candles_for_volume == min_needed {
        info!("📊 Volume analysis drives candle requirement: {} days = {} candles", 
              volume_lookback_days, candles_for_volume);
    }
    
    with_buffer
}

/// Pre-aggregate historical 1-minute candles into higher timeframes
/// Returns map of timeframe_seconds -> aggregated_candles
pub fn pre_aggregate_historical_candles(
    minute_candles: &[FuturesOHLCVCandle],
    target_timeframes: &[u64],
) -> std::collections::HashMap<u64, Vec<FuturesOHLCVCandle>> {
    let mut result = std::collections::HashMap::new();

    for &timeframe_seconds in target_timeframes {
        if timeframe_seconds == 60 {
            // 1-minute data is already available
            result.insert(timeframe_seconds, minute_candles.to_vec());
            continue;
        }

        let aggregated = aggregate_candles_to_timeframe(minute_candles, timeframe_seconds);
        result.insert(timeframe_seconds, aggregated);
    }

    result
}

/// Aggregate 1-minute candles into a specific timeframe
fn aggregate_candles_to_timeframe(
    minute_candles: &[FuturesOHLCVCandle],
    timeframe_seconds: u64,
) -> Vec<FuturesOHLCVCandle> {
    use tracing::{info, debug};
    let total_start = std::time::Instant::now();
    
    if minute_candles.is_empty() {
        return Vec::new();
    }

    let timeframe_ms = (timeframe_seconds * 1000) as i64;
    let mut aggregated = Vec::new();
    let mut current_period: Option<(TimestampMS, Vec<&FuturesOHLCVCandle>)> = None;
    
    info!("🔧 Aggregating {} 1m candles to {}s timeframe", minute_candles.len(), timeframe_seconds);

    let processing_start = std::time::Instant::now();
    for candle in minute_candles {
        // CRITICAL FIX: Use open_time for period calculation to match ring buffer logic
        let period_start = (candle.open_time / timeframe_ms) * timeframe_ms;

        match &mut current_period {
            Some((start, candles)) if *start == period_start => {
                // Add to current period
                candles.push(candle);
            }
            _ => {
                // Finalize previous period if exists
                if let Some((start, candles)) = current_period {
                    if let Some(agg_candle) = create_aggregated_candle(&candles, start, timeframe_ms) {
                        aggregated.push(agg_candle);
                    }
                }
                
                // Start new period
                current_period = Some((period_start, vec![candle]));
            }
        }
    }

    // Finalize the last period
    if let Some((start, candles)) = current_period {
        if let Some(agg_candle) = create_aggregated_candle(&candles, start, timeframe_ms) {
            aggregated.push(agg_candle);
        }
    }
    let processing_time = processing_start.elapsed();

    let validation_start = std::time::Instant::now();
    info!("🔧 Aggregated {} 1m candles into {} {}s candles", 
          minute_candles.len(), aggregated.len(), timeframe_seconds);
          
    // DEBUG: Show the final aggregated candle to detect boundary issues
    if !aggregated.is_empty() {
        let last_agg = &aggregated[aggregated.len()-1];
        debug!("🔍 FINAL {}s candle: close={:.2} @ open_time={} close_time={}", 
              timeframe_seconds, last_agg.close, last_agg.open_time, last_agg.close_time);
    }
    
    // Validate timing consistency in aggregated candles
    if aggregated.len() > 1 {
        let first_few = aggregated.iter().take(3);
        let last_few = aggregated.iter().rev().take(3).rev();

        debug!("🔍 First 3 {}s candles timing:", timeframe_seconds);
        for (i, candle) in first_few.enumerate() {
            debug!("  [{}]: open_time={}, close_time={}, close={:.2}", 
                  i, candle.open_time, candle.close_time, candle.close);
        }

        debug!("🔍 Last 3 {}s candles timing:", timeframe_seconds);
        for (i, candle) in last_few.enumerate() {
            debug!("  [{}]: open_time={}, close_time={}, close={:.2}", 
                  i, candle.open_time, candle.close_time, candle.close);
        }
        
        // Check time differences between consecutive candles
        let expected_diff = timeframe_ms;
        for i in 1..std::cmp::min(5, aggregated.len()) {
            let prev = &aggregated[i-1];
            let curr = &aggregated[i];
            let time_diff = curr.open_time - prev.open_time;
            
            if time_diff != expected_diff {
                info!("⚠️ Timing issue at candle {}: expected {}ms diff, got {}ms", 
                      i, expected_diff, time_diff);
            } else {
                debug!("✅ Candle {} timing correct: {}ms diff", i, time_diff);
            }
        }
        
        // Check aggregation quality
        let all_close_prices: Vec<f64> = aggregated.iter().map(|c| c.close).collect();
        let unique_closes: std::collections::HashSet<_> = all_close_prices.iter().map(|&f| (f * 100.0) as i64).collect();
        
        debug!("✅ Aggregated {}s: {} unique close values out of {} candles", 
              timeframe_seconds, unique_closes.len(), aggregated.len());
    }
    let validation_time = validation_start.elapsed();
    
    let total_time = total_start.elapsed();
    info!("⏱️ Historical aggregation ELAPSED TIMES for {}s: total={:?}, processing={:?}, validation={:?}", 
          timeframe_seconds, total_time, processing_time, validation_time);

    aggregated
}

/// Create an aggregated candle from multiple 1-minute candles
fn create_aggregated_candle(
    candles: &[&FuturesOHLCVCandle],
    period_start: TimestampMS,
    timeframe_ms: i64,
) -> Option<FuturesOHLCVCandle> {
    use tracing::debug;
    let start_time = std::time::Instant::now();
    
    if candles.is_empty() {
        return None;
    }

    let first = candles[0];
    let last = candles[candles.len() - 1];

    let calc_start = std::time::Instant::now();
    let high = candles.iter().map(|c| c.high).fold(f64::NEG_INFINITY, f64::max);
    let low = candles.iter().map(|c| c.low).fold(f64::INFINITY, f64::min);
    let volume = candles.iter().map(|c| c.volume).sum();
    let trades = candles.iter().map(|c| c.number_of_trades).sum();
    let taker_volume = candles.iter().map(|c| c.taker_buy_base_asset_volume).sum();
    let _calc_time = calc_start.elapsed();
    
    let _elapsed = start_time.elapsed();
    
    // Debug aggregation for first few candles AND the final period
    let is_debug_period = period_start % (5 * timeframe_ms) == 0; // Every 5th period
    let period_number = period_start / timeframe_ms;
    
    if is_debug_period {
        debug!("🔧 Aggregating {} candles into {}s period #{}: O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{:.0}", 
               candles.len(), timeframe_ms / 1000, period_number, first.open, high, low, last.close, volume);
    }
    
    // Special debug for the last few periods to see what's causing identical closes
    if period_number > 0 && (period_number % 50 == 0 || candles.len() < 5) {
        debug!("🔍 {}s period #{}: {} candles, first_1m_close={:.2}, last_1m_close={:.2}, final_agg_close={:.2}", 
              timeframe_ms / 1000, period_number, candles.len(), first.close, last.close, last.close);
              
        if candles.len() >= 2 {
            let second_last = candles[candles.len()-2];
            debug!("   → Last two 1m candles: [{:.2} @ {}] → [{:.2} @ {}]", 
                  second_last.close, second_last.close_time, last.close, last.close_time);
        }
    }

    let now = chrono::Utc::now().timestamp_millis();

    Some(FuturesOHLCVCandle {
        open_time: period_start,
        close_time: period_start + timeframe_ms - 1,
        open: first.open,
        high,
        low,
        close: last.close,
        volume,
        number_of_trades: trades,
        taker_buy_base_asset_volume: taker_volume,
        closed: (period_start + timeframe_ms - 1) < now,
    })
}

/// Extract price series from candles for EMA initialization
pub fn extract_close_prices(candles: &[FuturesOHLCVCandle]) -> Vec<f64> {
    
    let prices: Vec<f64> = candles.iter().map(|c| c.close).collect();
    
    // Debug logging for close price sequences
    if !prices.is_empty() {
        let total = prices.len();
        let first_few = prices.iter().take(5).map(|p| format!("{:.2}", p)).collect::<Vec<_>>().join(", ");
        let last_few = prices.iter().rev().take(5).rev().map(|p| format!("{:.2}", p)).collect::<Vec<_>>().join(", ");

        debug!("🔍 Extracted {} close prices: first=[{}] last=[{}]", 
              total, first_few, last_few);
        
        // Calculate basic stats for validation
        let min_price = prices.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_price = prices.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let avg_price = prices.iter().sum::<f64>() / prices.len() as f64;

        debug!("🔍 Price range: min={:.2}, max={:.2}, avg={:.2}", min_price, max_price, avg_price);
    }
    
    prices
}

/// Create volume records from 5-minute candles for volume analysis initialization
pub fn create_volume_records_from_candles(
    candles_5m: &[FuturesOHLCVCandle],
    trend_direction: TrendDirection,
) -> Vec<VolumeRecord> {
    candles_5m.iter().map(|candle| {
        VolumeRecord {
            volume: candle.volume,
            price: candle.close,
            timestamp: candle.close_time,
            trend: trend_direction.clone(),
        }
    }).collect()
}

/// Calculate timeframe string for logging
pub fn timeframe_to_string(timeframe_seconds: u64) -> String {
    match timeframe_seconds {
        60 => "1m".to_string(),
        300 => "5m".to_string(),
        900 => "15m".to_string(),
        3600 => "1h".to_string(),
        14400 => "4h".to_string(),
        _ => format!("{}s", timeframe_seconds),
    }
}

/// Format timestamp to ISO string for output
pub fn format_timestamp_iso(timestamp_ms: TimestampMS) -> String {
    let datetime = chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .unwrap_or_else(|| chrono::Utc::now());
    datetime.to_rfc3339()
}

/// Validate that we have sufficient data for initialization and return detected gaps
pub fn validate_initialization_data(
    symbol: &str,
    minute_candles: &[FuturesOHLCVCandle],
    required_candles: usize,
) -> Result<Vec<crate::historical::structs::TimeRange>, String> {
    if minute_candles.len() < required_candles {
        return Err(format!(
            "Insufficient data for {}: got {} candles, need {} for proper initialization",
            symbol, minute_candles.len(), required_candles
        ));
    }

    let mut detected_gaps = Vec::new();
    
    // Check for data gaps longer than 5 minutes
    for window in minute_candles.windows(2) {
        let gap_ms = window[1].close_time - window[0].close_time;
        let gap_minutes = gap_ms / (60 * 1000);
        
        if gap_minutes > 5 {
            warn!("Found data gap of {} minutes in {} between {} and {}", 
                  gap_minutes, symbol, window[0].close_time, window[1].close_time);
            
            // Add gap to detected gaps list (using close_time + 1 minute for start, open_time for end)
            detected_gaps.push(crate::historical::structs::TimeRange {
                start: window[0].close_time + 60000, // Start 1 minute after last candle
                end: window[1].open_time, // End at next candle open time
            });
        }
    }

    info!("✅ Data validation passed for {}: {} candles available, {} gaps detected", 
          symbol, minute_candles.len(), detected_gaps.len());
    
    Ok(detected_gaps)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_candle(open_time: i64, close_time: i64, ohlcv: (f64, f64, f64, f64, f64)) -> FuturesOHLCVCandle {
        FuturesOHLCVCandle {
            open_time,
            close_time,
            open: ohlcv.0,
            high: ohlcv.1,
            low: ohlcv.2,
            close: ohlcv.3,
            volume: ohlcv.4,
            number_of_trades: 100,
            taker_buy_base_asset_volume: ohlcv.4 * 0.6,
            closed: true,
        }
    }

    #[test]
    fn test_calculate_min_candles_needed() {
        let timeframes = vec![300, 900, 3600]; // 5m, 15m, 1h
        let ema_periods = vec![21, 89];
        let volume_days = 30;

        let min_needed = calculate_min_candles_needed(&timeframes, &ema_periods, volume_days);
        
        // Should be at least 30 days worth of minutes for volume analysis
        assert!(min_needed >= 30 * 24 * 60);
    }

    #[test] 
    fn test_aggregate_candles_to_timeframe() {
        // Create 5 consecutive 1-minute candles
        let candles = vec![
            create_test_candle(0, 59999, (100.0, 105.0, 95.0, 103.0, 1000.0)),
            create_test_candle(60000, 119999, (103.0, 110.0, 100.0, 107.0, 1200.0)),
            create_test_candle(120000, 179999, (107.0, 115.0, 105.0, 112.0, 800.0)),
            create_test_candle(180000, 239999, (112.0, 120.0, 108.0, 115.0, 900.0)),
            create_test_candle(240000, 299999, (115.0, 118.0, 110.0, 116.0, 700.0)),
        ];

        let aggregated = aggregate_candles_to_timeframe(&candles, 300); // 5-minute
        
        assert_eq!(aggregated.len(), 1);
        let agg = &aggregated[0];
        
        assert_eq!(agg.open, 100.0); // First candle's open
        assert_eq!(agg.close, 116.0); // Last candle's close
        assert_eq!(agg.high, 120.0);  // Max of all highs
        assert_eq!(agg.low, 95.0);    // Min of all lows
        assert_eq!(agg.volume, 4600.0); // Sum of all volumes
    }

    #[test]
    fn test_extract_close_prices() {
        let candles = vec![
            create_test_candle(0, 59999, (100.0, 105.0, 95.0, 103.0, 1000.0)),
            create_test_candle(60000, 119999, (103.0, 110.0, 100.0, 107.0, 1200.0)),
        ];

        let prices = extract_close_prices(&candles);
        assert_eq!(prices, vec![103.0, 107.0]);
    }

    #[test]
    fn test_timeframe_to_string() {
        assert_eq!(timeframe_to_string(60), "1m");
        assert_eq!(timeframe_to_string(300), "5m");
        assert_eq!(timeframe_to_string(3600), "1h");
        assert_eq!(timeframe_to_string(7200), "7200s");
    }
}