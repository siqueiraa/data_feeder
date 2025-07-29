use std::path::Path;
use std::time::Instant;
use std::fs;
use std::io::{self, Read};

use chrono::{NaiveDate, Utc, TimeZone, Datelike};
use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};
use zip::ZipArchive;
use rustc_hash::FxHashMap;

use super::errors::HistoricalDataError;
use super::structs::{FuturesOHLCVCandle, TimeRange, TimestampMS, Seconds, FuturesExchangeTrade};


// Convert Binance interval strings to Seconds
pub fn interval_to_seconds(interval: &str) -> Result<Seconds, HistoricalDataError> {
    match interval {
        "1m" => Ok(60),
        "3m" => Ok(180),
        "5m" => Ok(300),
        "15m" => Ok(900),
        "30m" => Ok(1800),
        "1h" => Ok(3600),
        "2h" => Ok(7200),
        "4h" => Ok(14400),
        "6h" => Ok(21600),
        "8h" => Ok(28800),
        "12h" => Ok(43200),
        "1d" => Ok(86400),
        "3d" => Ok(259200),
        _ => Err(HistoricalDataError::Validation(format!("Unsupported interval: {}", interval))),
    }
}

// Convert Seconds to Binance interval string
pub fn seconds_to_interval(seconds: Seconds) -> Result<&'static str, HistoricalDataError> {
    match seconds {
        60 => Ok("1m"),
        180 => Ok("3m"),
        300 => Ok("5m"),
        900 => Ok("15m"),
        1800 => Ok("30m"),
        3600 => Ok("1h"),
        7200 => Ok("2h"),
        14400 => Ok("4h"),
        21600 => Ok("6h"),
        28800 => Ok("8h"),
        43200 => Ok("12h"),
        86400 => Ok("1d"),
        259200 => Ok("3d"),
        _ => Err(HistoricalDataError::Validation(format!("No Binance interval for {} seconds", seconds))),
    }
}

// Check if a timeframe is supported by Binance klines
pub fn is_kline_supported_timeframe(seconds: Seconds) -> bool {
    matches!(seconds, 60 | 180 | 300 | 900 | 1800 | 3600 | 7200 | 14400 | 21600 | 28800 | 43200 | 86400 | 259200)
}

pub fn verify_checksum(zip_file: &Path) {
    // Placeholder for actual checksum verification
    info!("Checksum verification placeholder for: {:?}", zip_file);
}

pub fn compute_sha256(file_path: &Path) -> Result<String, HistoricalDataError> {
    let mut file = fs::File::open(file_path).map_err(HistoricalDataError::Io)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 1024];
    loop {
        let bytes_read = file.read(&mut buffer).map_err(HistoricalDataError::Io)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn format_timestamp(timestamp_ms: TimestampMS) -> String {
    let naive = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap() + chrono::Duration::milliseconds(timestamp_ms);
    let datetime: chrono::DateTime<Utc> = Utc.from_utc_datetime(&naive);
    datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

pub fn scan_for_candle_gaps(
    candles: &[FuturesOHLCVCandle],
    timeframe: Seconds,
    start_time: TimestampMS,
    end_time: TimestampMS,
) -> Vec<TimeRange> {
    let mut gaps = Vec::new();
    if candles.is_empty() {
        if start_time < end_time {
            gaps.push(TimeRange { start: start_time, end: end_time });
        }
        return gaps;
    }

    let expected_interval_ms = timeframe as i64 * 1000;

    // Check for gap at the beginning
    if candles[0].open_time() > start_time {
        gaps.push(TimeRange { start: candles[0].open_time(), end: candles[0].open_time() });
    }

    // Check for gaps between candles
    for i in 0..candles.len() - 1 {
        let current_candle = &candles[i];
        let next_candle = &candles[i + 1];
        let expected_next_open_time = current_candle.open_time() + expected_interval_ms;

        if next_candle.open_time() > expected_next_open_time {
            gaps.push(TimeRange { start: expected_next_open_time, end: next_candle.open_time() });
        }
    }

    // Check for gap at the end
    if candles.last().unwrap().open_time() + expected_interval_ms < end_time {
        gaps.push(TimeRange { start: candles.last().unwrap().open_time() + expected_interval_ms, end: end_time });
    }

    gaps
}

pub fn aggregate_candles_with_gap_filling(
    trades: &[FuturesExchangeTrade],
    timeframe: Seconds,
    start_time: TimestampMS,
    end_time: TimestampMS,
    fill_gaps: bool,
) -> Vec<FuturesOHLCVCandle> {
    let mut candles = Vec::new();
    if trades.is_empty() {
        return candles;
    }

    let interval_ms = timeframe as i64 * 1000;

    let mut current_bucket_start = (start_time / interval_ms) * interval_ms;
    if current_bucket_start < start_time {
        current_bucket_start += interval_ms;
    }

    let mut trade_idx = 0;

    while current_bucket_start < end_time {
        let bucket_end = current_bucket_start + interval_ms;
        let mut bucket_trades = Vec::new();

        while trade_idx < trades.len() && trades[trade_idx].timestamp < bucket_end {
            bucket_trades.push(&trades[trade_idx]);
            trade_idx += 1;
        }

        if bucket_trades.is_empty() {
            if fill_gaps && !candles.is_empty() {
                let prev_close = candles.last().unwrap().close();
                candles.push(FuturesOHLCVCandle::new_from_values(
                    current_bucket_start,
                    bucket_end - 1,
                    prev_close,
                    prev_close,
                    prev_close,
                    prev_close,
                    0.0, // No volume in gap
                    0, // No trades in gap
                    0.0, // No taker buy volume in gap
                    true,
                ));
            } else if fill_gaps {
                // If it's the very first candle and there are no trades,
                // we can't use a previous close, so we'll just create a zero-volume candle.
                candles.push(FuturesOHLCVCandle::new_from_values(
                    current_bucket_start,
                    bucket_end - 1,
                    0.0, 0.0, 0.0, 0.0, 0.0, // OHLCV all zeros for first gap
                    0, // No trades in gap
                    0.0, // No taker buy volume in gap
                    true,
                ));
            }
        } else {
            let open = bucket_trades.first().unwrap().price;
            let close = bucket_trades.last().unwrap().price;
            let high = bucket_trades.iter().map(|t| t.price).fold(f64::MIN, f64::max);
            let low = bucket_trades.iter().map(|t| t.price).fold(f64::MAX, f64::min);
            let volume = bucket_trades.iter().map(|t| t.size).sum();
            
            // Calculate taker buy volume (when buyer is the taker, not maker)
            let taker_buy_volume = bucket_trades.iter()
                .filter(|t| !t.is_buyer_maker) // Buyer is taker when not maker
                .map(|t| t.size)
                .sum();
            
            let number_of_trades = bucket_trades.len() as u64;

            candles.push(FuturesOHLCVCandle::new_from_values(
                current_bucket_start,
                bucket_end - 1,
                open,
                high,
                low,
                close,
                volume,
                number_of_trades,
                taker_buy_volume,
                true,
            ));
        }
        current_bucket_start = bucket_end;
    }
    candles
}

// Ultra-fast number parsing without string allocations
#[inline(always)]
#[allow(clippy::needless_range_loop)]
pub fn fast_parse_f64(bytes: &[u8]) -> Result<f64, HistoricalDataError> {
    let mut result = 0.0;
    let mut decimal_pos = None;
    let mut pos = 0;

    // Handle negative numbers
    let negative = if !bytes.is_empty() && bytes[0] == b'-' {
        pos = 1;
        true
    } else {
        false
    };

    for i in pos..bytes.len() {
        match bytes[i] {
            b'0'..=b'9' => {
                result = result * 10.0 + (bytes[i] - b'0') as f64;
                if let Some(dp) = decimal_pos {
                    decimal_pos = Some(dp + 1);
                }
            }
            b'.' => {
                if decimal_pos.is_some() {
                    return Err(HistoricalDataError::ParseString("Multiple decimal points".to_string()));
                }
                decimal_pos = Some(0);
            }
            _ => return Err(HistoricalDataError::ParseString("Invalid character in number".to_string())),
        }
    }

    if let Some(dp) = decimal_pos {
        result /= 10.0_f64.powi(dp);
    }

    Ok(if negative { -result } else { result })
}

#[inline(always)]
#[allow(clippy::needless_range_loop)]
pub fn fast_parse_i64(bytes: &[u8]) -> Result<i64, HistoricalDataError> {
    let mut result = 0i64;
    let mut pos = 0;

    // Handle negative numbers
    let negative = if !bytes.is_empty() && bytes[0] == b'-' {
        pos = 1;
        true
    } else {
        false
    };

    for i in pos..bytes.len() {
        match bytes[i] {
            b'0'..=b'9' => {
                result = result * 10 + (bytes[i] - b'0') as i64;
            }
            _ => return Err(HistoricalDataError::ParseString("Invalid character in integer".to_string())),
        }
    }

    Ok(if negative { -result } else { result })
}

#[inline(always)]
#[allow(clippy::needless_range_loop)]
pub fn fast_parse_u64(bytes: &[u8]) -> Result<u64, HistoricalDataError> {
    let mut result = 0u64;

    for i in 0..bytes.len() {
        match bytes[i] {
            b'0'..=b'9' => {
                result = result * 10 + (bytes[i] - b'0') as u64;
            }
            _ => return Err(HistoricalDataError::ParseString("Invalid character in unsigned integer".to_string())),
        }
    }

    Ok(result)
}

#[cfg(test)]
mod parsing_tests {
    use super::*;

    #[test]
    fn test_fast_parse_u64() {
        assert_eq!(fast_parse_u64(b"0").unwrap(), 0);
        assert_eq!(fast_parse_u64(b"123").unwrap(), 123);
        assert_eq!(fast_parse_u64(b"999999999999999999").unwrap(), 999999999999999999);
        
        // Test error cases
        assert!(fast_parse_u64(b"abc").is_err());
        assert!(fast_parse_u64(b"12.34").is_err());
        assert!(fast_parse_u64(b"-123").is_err()); // No negative numbers for u64
    }

    #[test]
    fn test_csv_parsing_with_complete_binance_data() {
        // Sample Binance kline CSV data with all fields
        let csv_data = b"1609459200000,29374.15,29433.73,29200.00,29374.15,508.24723000,1609459259999,14932134.23778210,2283,254.56784000,7477631.17645690,0";
        
        let candles = parse_csv_to_klines(csv_data, 300).expect("Should parse complete Binance data");
        assert_eq!(candles.len(), 1);
        
        let candle = &candles[0];
        assert_eq!(candle.open_time, 1609459200000);
        assert_eq!(candle.close_time, 1609459259999);
        assert_eq!(candle.volume, 508.24723000);
        assert_eq!(candle.number_of_trades, 2283);
        assert_eq!(candle.taker_buy_base_asset_volume, 254.56784000);
        
        // Verify that both taker buy and trade count are non-zero
        assert!(candle.number_of_trades > 0, "Trade count should be positive");
        assert!(candle.taker_buy_base_asset_volume > 0.0, "Taker buy volume should be positive");
        
        println!("✅ CSV parsing test passed with realistic data:");
        println!("   Volume: {:.8}", candle.volume);
        println!("   Trades: {}", candle.number_of_trades);
        println!("   Taker Buy Volume: {:.8}", candle.taker_buy_base_asset_volume);
    }
}

pub fn parse_csv_to_trade(csv_data: &[u8]) -> Result<Vec<FuturesExchangeTrade>, HistoricalDataError> {
    let estimated_lines = csv_data.iter().filter(|&&b| b == b'\n').count();
    let mut trades = Vec::with_capacity(estimated_lines);

    let mut pos = 0;
    while pos < csv_data.len() && csv_data[pos] != b'\n' {
        pos += 1;
    }
    pos += 1;

    while pos < csv_data.len() {
        let line_start = pos;

        let mut line_end = pos;
        while line_end < csv_data.len() && csv_data[line_end] != b'\n' {
            line_end += 1;
        }

        if line_end <= line_start {
            break;
        }

        let line = &csv_data[line_start..line_end];

        let mut field_start = 0;
        let mut field_idx = 0;
        let mut price = 0.0;
        let mut size = 0.0;
        let mut timestamp = 0i64;
        let mut is_buyer_maker = false;

        for i in 0..=line.len() {
            if i == line.len() || line[i] == b',' {
                let field = &line[field_start..i];

                match field_idx {
                    1 => price = fast_parse_f64(field)?,
                    2 => size = fast_parse_f64(field)?,
                    5 => timestamp = fast_parse_i64(field)?,
                    6 => is_buyer_maker = field == b"true",
                    _ => {}
                }

                field_start = i + 1;
                field_idx += 1;
            }
        }

        trades.push(FuturesExchangeTrade { timestamp, price, size, is_buyer_maker });

        pos = line_end + 1;
    }

    if trades.is_empty() {
        return Err(HistoricalDataError::NoData("No trades parsed".to_string()));
    }

    Ok(trades)
}

pub fn parse_csv_to_klines(csv_data: &[u8], _timeframe: Seconds) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
    let estimated_lines = csv_data.iter().filter(|&&b| b == b'\n').count();
    let mut candles = Vec::with_capacity(estimated_lines);

    let mut pos = 0;
    let has_header = csv_data.len() > 10 && csv_data.starts_with(b"open_time");

    if has_header {
        while pos < csv_data.len() && csv_data[pos] != b'\n' {
            pos += 1;
        }
        pos += 1;
    }

    while pos < csv_data.len() {
        let line_start = pos;

        let mut line_end = pos;
        while line_end < csv_data.len() && csv_data[line_end] != b'\n' {
            line_end += 1;
        }

        if line_end <= line_start {
            break;
        }

        let line = &csv_data[line_start..line_end];

        let mut field_start = 0;
        let mut field_idx = 0;
        let mut open_time = 0i64;
        let mut open = 0.0;
        let mut high = 0.0;
        let mut low = 0.0;
        let mut close = 0.0;
        let mut volume = 0.0;
        let mut close_time = 0i64;
        let mut number_of_trades = 0u64;
        let mut taker_buy_volume = 0.0;

        for i in 0..=line.len() {
            if i == line.len() || line[i] == b',' {
                let field = &line[field_start..i];

                match field_idx {
                    0 => open_time = fast_parse_i64(field)?,
                    1 => open = fast_parse_f64(field)?,
                    2 => high = fast_parse_f64(field)?,
                    3 => low = fast_parse_f64(field)?,
                    4 => close = fast_parse_f64(field)?,
                    5 => volume = fast_parse_f64(field)?,
                    6 => close_time = fast_parse_i64(field)?,
                    8 => number_of_trades = fast_parse_u64(field)?,
                    9 => taker_buy_volume = fast_parse_f64(field)?,
                    _ => {} // Skip unused fields (7=quoteAssetVolume, 10=takerBuyQuoteAssetVolume, 11=ignored)
                }

                field_start = i + 1;
                field_idx += 1;
            }
        }

        let candle = FuturesOHLCVCandle::new_from_values(
            open_time, close_time, open, high, low, close, volume,
            number_of_trades, taker_buy_volume, true,
        );

        candles.push(candle);
        pos = line_end + 1;
    }

    if candles.is_empty() {
        return Err(HistoricalDataError::NoData("No klines parsed".to_string()));
    }

    Ok(candles)
}

pub async fn process_single_file_pipeline(
    csv_path: &Path,
    symbol: &str,
    zip_url: &str,
    checksum_url: &str,
    date_str: &str,
    timeframes: &[Seconds],
) -> Result<FxHashMap<Seconds, Vec<FuturesOHLCVCandle>>, HistoricalDataError> {
    let file_name = zip_url.rsplit('/').next().unwrap_or("unknown_zip_file");
    let csv_file_name_str = format!("{}-aggTrades-{}.csv", symbol.to_uppercase(), date_str);
    let csv_local_path = csv_path.join(&csv_file_name_str);
    let zip_local_path = csv_path.join(file_name);
    let checksum_local_path = csv_path.join(checksum_url.rsplit('/').next().unwrap_or("unknown_checksum_file"));

    let thread_id = std::thread::current().id();
    info!("🧵 Thread {:?}: Starting pipeline for {}", thread_id, csv_file_name_str);

    let csv_exists = tokio::fs::try_exists(&csv_local_path).await.unwrap_or(false);
    let checksum_exists = tokio::fs::try_exists(&checksum_local_path).await.unwrap_or(false);

    let mut use_existing_csv = false;
    if csv_exists && checksum_exists {
        info!("🧵 Thread {:?}: 📂 Found existing CSV: {}", thread_id, csv_local_path.display());

        if let Ok(expected_checksum) = tokio::fs::read_to_string(&checksum_local_path).await {
            let expected_checksum = expected_checksum.trim();
            let csv_local_path_clone = csv_local_path.clone();
            if let Ok(Ok(actual_checksum)) = tokio::task::spawn_blocking(move || compute_sha256(&csv_local_path_clone)).await {
                if actual_checksum == expected_checksum {
                    info!("🧵 Thread {:?}: ✅ CSV checksum verified", thread_id);
                    use_existing_csv = true;
                } else {
                    warn!("🧵 Thread {:?}: ❌ CSV checksum mismatch - will re-download", thread_id);
                    warn!("🧵 Thread {:?}: Expected: {}, Actual: {}", thread_id, expected_checksum, actual_checksum);
                    if let Err(e) = tokio::fs::remove_file(&csv_local_path).await {
                        warn!("🧵 Thread {:?}: Failed to delete corrupted CSV: {}", thread_id, e);
                    }
                }
            } else {
                warn!("🧵 Thread {:?}: Failed to compute checksum - will re-download", thread_id);
            }
        } else {
            warn!("🧵 Thread {:?}: Failed to read checksum file - will re-download", thread_id);
        }
    } else if csv_exists && !checksum_exists {
        warn!("🧵 Thread {:?}: CSV exists but no checksum file - will re-download for safety", thread_id);
        if let Err(e) = tokio::fs::remove_file(&csv_local_path).await {
            warn!("🧵 Thread {:?}: Failed to delete CSV without checksum: {}", thread_id, e);
        }
    }

    let csv_data = if use_existing_csv {
        tokio::fs::read(&csv_local_path)
            .await
            .map_err(HistoricalDataError::Io)?
    } else {
        info!("🧵 Thread {:?}: 📥 Downloading and extracting {}", thread_id, zip_url);

        download_file(checksum_url, &checksum_local_path).await?;

        let temp_zip_path = zip_local_path.with_extension("zip.tmp");
        download_file(zip_url, &temp_zip_path).await?;

        let csv_data = tokio::task::spawn_blocking({
            let temp_zip_path_clone = temp_zip_path.clone();
            move || extract_csv_from_zip(&temp_zip_path_clone)
        })
        .await
        .map_err(|e| HistoricalDataError::StorageError(format!("Extract task failed: {}", e)))??;

        tokio::fs::write(&csv_local_path, &csv_data)
            .await
            .map_err(HistoricalDataError::Io)?;

        if let Err(e) = tokio::fs::remove_file(&temp_zip_path).await {
            warn!("🧵 Thread {:?}: Failed to cleanup temp ZIP: {}", thread_id, e);
        }

        info!("🧵 Thread {:?}: ✅ CSV extracted and saved: {} bytes", thread_id, csv_data.len());
        csv_data
    };

    let parse_start = Instant::now();
    let csv_data_len = csv_data.len();
    let trades = tokio::task::spawn_blocking(move || parse_csv_to_trade(&csv_data))
        .await
        .map_err(|e| HistoricalDataError::StorageError(format!("Parse task failed: {}", e)))??;

    let parse_elapsed = parse_start.elapsed();
    info!(
        "🧵 Thread {:?}: 🚀 Parsed {} trades in {:?} ({:.1} MB/s)",
        thread_id,
        trades.len(),
        parse_elapsed,
        csv_data_len as f64 / 1024.0 / 1024.0 / parse_elapsed.as_secs_f64()
    );

    let mut candles_by_timeframe = FxHashMap::default();

    if !trades.is_empty() {
        let first_timestamp = trades.first().unwrap().timestamp;
        let last_timestamp = trades.last().unwrap().timestamp;

        for &tf in timeframes {
            let aligned_start = (first_timestamp / (tf as i64 * 1000)) * (tf as i64 * 1000);
            let aligned_end = ((last_timestamp / (tf as i64 * 1000)) + 1) * (tf as i64 * 1000);

            let candles = aggregate_candles_with_gap_filling(&trades, tf, aligned_start, aligned_end, false);
            info!("🧵 Thread {:?}: Aggregated {} candles for timeframe {}s", thread_id, candles.len(), tf);
            candles_by_timeframe.insert(tf, candles);
        }
    }

    Ok(candles_by_timeframe)
}

pub async fn process_klines_pipeline(
    csv_path: &Path,
    symbol: &str,
    zip_url: &str,
    checksum_url: &str,
    date_str: &str,
    interval: &str,
) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
    let file_name = zip_url.rsplit('/').next().unwrap_or("unknown_zip_file");
    let csv_file_name_str = format!("{}-{}-{}.csv", symbol.to_uppercase(), interval, date_str);
    let csv_local_path = csv_path.join(&csv_file_name_str);
    let zip_local_path = csv_path.join(file_name);
    let checksum_local_path = csv_path.join(checksum_url.rsplit('/').next().unwrap_or("unknown_checksum_file"));

    let thread_id = std::thread::current().id();

    let csv_exists = tokio::fs::try_exists(&csv_local_path).await.unwrap_or(false);
    let checksum_exists = tokio::fs::try_exists(&checksum_local_path).await.unwrap_or(false);

    let mut use_existing_csv = false;
    if csv_exists && checksum_exists {
        info!("🧵 Thread {:?}: 📂 Found existing klines CSV: {}", thread_id, csv_local_path.display());

        if let Ok(expected_checksum) = tokio::fs::read_to_string(&checksum_local_path).await {
            let expected_checksum = expected_checksum.trim();
            let csv_local_path_clone = csv_local_path.clone();
            if let Ok(Ok(actual_checksum)) = tokio::task::spawn_blocking(move || compute_sha256(&csv_local_path_clone)).await {
                if actual_checksum == expected_checksum {
                    info!("🧵 Thread {:?}: ✅ Klines CSV checksum verified", thread_id);
                    use_existing_csv = true;
                } else {
                    warn!("🧵 Thread {:?}: ❌ Klines CSV checksum mismatch - will re-download", thread_id);
                    if let Err(e) = tokio::fs::remove_file(&csv_local_path).await {
                        warn!("🧵 Thread {:?}: Failed to delete corrupted klines CSV: {}", thread_id, e);
                    }
                }
            }
        }
    }

    let csv_data = if use_existing_csv {
        tokio::fs::read(&csv_local_path)
            .await
            .map_err(HistoricalDataError::Io)?
    } else {
        info!("🧵 Thread {:?}: 📥 Downloading klines {}", thread_id, zip_url);

        download_file(checksum_url, &checksum_local_path).await?;

        let temp_zip_path = zip_local_path.with_extension("zip.tmp");
        download_file(zip_url, &temp_zip_path).await?;

        let csv_data = tokio::task::spawn_blocking({
            let temp_zip_path_clone = temp_zip_path.clone();
            move || extract_csv_from_zip(&temp_zip_path_clone)
        })
        .await
        .map_err(|e| HistoricalDataError::StorageError(format!("Extract task failed: {}", e)))??;

        tokio::fs::write(&csv_local_path, &csv_data)
            .await
            .map_err(HistoricalDataError::Io)?;

        if let Err(e) = tokio::fs::remove_file(&temp_zip_path).await {
            warn!("🧵 Thread {:?}: Failed to cleanup temp ZIP: {}", thread_id, e);
        }

        csv_data
    };

    let parse_start = Instant::now();
    let csv_data_len = csv_data.len();
    let timeframe = interval_to_seconds(interval)?;

    let candles = tokio::task::spawn_blocking(move || parse_csv_to_klines(&csv_data, timeframe))
        .await
        .map_err(|e| HistoricalDataError::StorageError(format!("Parse task failed: {}", e)))??;

    let parse_elapsed = parse_start.elapsed();
    info!(
        "🧵 Thread {:?}: 🚀 Parsed {} klines in {:?} ({:.1} MB/s)",
        thread_id,
        candles.len(),
        parse_elapsed,
        csv_data_len as f64 / 1024.0 / 1024.0 / parse_elapsed.as_secs_f64()
    );

    Ok(candles)
}

// Build URLs for kline data
pub fn build_kline_urls(symbol: &str, interval: &str, dates: Vec<NaiveDate>) -> Vec<(String, String, String)> {
    build_kline_urls_with_path_type(symbol, interval, dates, PathType::Daily)
}

#[derive(Debug, Clone, Copy)]
pub enum PathType {
    Daily,
    Monthly,
}

pub fn build_kline_urls_with_path_type(symbol: &str, interval: &str, dates: Vec<NaiveDate>, path_type: PathType) -> Vec<(String, String, String)> {
    let base_url = "https://data.binance.vision";
    
    let path_segment = match path_type {
        PathType::Daily => "daily",
        PathType::Monthly => "monthly",
    };
    
    let prefix = format!("data/futures/um/{}/klines/{}/{}", path_segment, symbol.to_uppercase(), interval);

    dates
        .into_iter()
        .map(|date| {
            let date_str = match path_type {
                PathType::Daily => date.format("%Y-%m-%d").to_string(),
                PathType::Monthly => date.format("%Y-%m").to_string(), // Monthly format: YYYY-MM
            };
            let zip_file = format!("{}-{}-{}.zip", symbol.to_uppercase(), interval, date_str);
            let checksum_file = format!("{}-{}-{}.zip.CHECKSUM", symbol.to_uppercase(), interval, date_str);
            
            let zip_url = format!("{}/{}/{}", base_url, prefix, zip_file);
            let checksum_url = format!("{}/{}/{}", base_url, prefix, checksum_file);
            (zip_url, checksum_url, date_str)
        })
        .collect()
}

pub async fn download_file(url: &str, path: &Path) -> Result<(), HistoricalDataError> {
    let client = Client::new();
    let response = client.get(url).send().await.map_err(HistoricalDataError::Reqwest)?;

    if response.status().is_success() {
        let mut file = File::create(path).await.map_err(HistoricalDataError::Io)?;
        let bytes = response.bytes().await.map_err(HistoricalDataError::Reqwest)?;
        file.write_all(&bytes).await.map_err(HistoricalDataError::Io)?;
        Ok(())
    } else if response.status() == reqwest::StatusCode::NOT_FOUND {
        Err(HistoricalDataError::NotFound(url.to_string()))
    } else {
        let status = response.status();
        let text = response.text().await.unwrap_or_else(|_| "<no body>".to_string());
        Err(HistoricalDataError::HttpError(status, text))
    }
}

pub fn extract_csv_from_zip(zip_file: &Path) -> Result<Vec<u8>, HistoricalDataError> {
    let file = fs::File::open(zip_file).map_err(HistoricalDataError::Io)?;
    let mut archive = ZipArchive::new(file).map_err(HistoricalDataError::Zip)?;
    let mut csv_file = archive.by_index(0).map_err(HistoricalDataError::Zip)?;
    let mut buffer = Vec::new();
    io::copy(&mut csv_file, &mut buffer).map_err(HistoricalDataError::Io)?;
    Ok(buffer)
}

pub fn get_dates_in_range(start_time: TimestampMS, end_time: TimestampMS) -> Vec<NaiveDate> {
    let start_utc = Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()) + chrono::Duration::milliseconds(start_time);
    let end_utc = Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()) + chrono::Duration::milliseconds(end_time);

    let mut dates = Vec::new();
    let mut current_date = start_utc.date_naive();
    let end_date = end_utc.date_naive();

    while current_date <= end_date {
        dates.push(current_date);
        current_date += chrono::Duration::days(1);
    }
    dates
}

pub fn get_months_in_range(start_time: TimestampMS, end_time: TimestampMS) -> Vec<NaiveDate> {
    let start_utc = Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()) + chrono::Duration::milliseconds(start_time);
    let end_utc = Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()) + chrono::Duration::milliseconds(end_time);

    let mut months = Vec::new();
    let start_date = start_utc.date_naive();
    let end_date = end_utc.date_naive();
    
    // Start from the first day of the start month
    let mut current_date = NaiveDate::from_ymd_opt(start_date.year(), start_date.month(), 1).unwrap();
    
    while current_date <= end_date {
        months.push(current_date);
        // Move to first day of next month
        if current_date.month() == 12 {
            current_date = NaiveDate::from_ymd_opt(current_date.year() + 1, 1, 1).unwrap();
        } else {
            current_date = NaiveDate::from_ymd_opt(current_date.year(), current_date.month() + 1, 1).unwrap();
        }
    }
    
    months
}

const TWO_MONTHS_IN_MS: i64 = 60 * 24 * 60 * 60 * 1000; // ~2 months in milliseconds

pub fn should_use_monthly_data(start_time: TimestampMS, end_time: TimestampMS) -> bool {
    let duration = end_time - start_time;
    duration > TWO_MONTHS_IN_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_use_monthly_data() {
        let now = chrono::Utc::now().timestamp_millis();
        
        // Test case 1: Small gap (1 week) - should use daily
        let small_gap_start = now - (7 * 24 * 60 * 60 * 1000); // 1 week ago
        let small_gap_end = now;
        assert_eq!(should_use_monthly_data(small_gap_start, small_gap_end), false);
        
        // Test case 2: Medium gap (1 month) - should use daily
        let medium_gap_start = now - (30 * 24 * 60 * 60 * 1000); // 1 month ago
        let medium_gap_end = now;
        assert_eq!(should_use_monthly_data(medium_gap_start, medium_gap_end), false);
        
        // Test case 3: Large gap (3 months) - should use monthly
        let large_gap_start = now - (90 * 24 * 60 * 60 * 1000); // 3 months ago
        let large_gap_end = now;
        assert_eq!(should_use_monthly_data(large_gap_start, large_gap_end), true);
        
        // Test case 4: Very large gap (1 year) - should use monthly
        let very_large_gap_start = now - (365 * 24 * 60 * 60 * 1000); // 1 year ago
        let very_large_gap_end = now;
        assert_eq!(should_use_monthly_data(very_large_gap_start, very_large_gap_end), true);
    }

    #[test]
    fn test_path_type_url_generation() {
        let dates = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
        
        // Test daily path
        let daily_urls = build_kline_urls_with_path_type("BTCUSDT", "1m", dates.clone(), PathType::Daily);
        assert!(!daily_urls.is_empty());
        let (daily_url, _, _) = &daily_urls[0];
        assert!(daily_url.contains("/daily/klines/"));
        assert!(daily_url.contains("BTCUSDT-1m-2021-01-01.zip"));
        
        // Test monthly path  
        let monthly_urls = build_kline_urls_with_path_type("BTCUSDT", "1m", dates.clone(), PathType::Monthly);
        assert!(!monthly_urls.is_empty());
        let (monthly_url, _, _) = &monthly_urls[0];
        assert!(monthly_url.contains("/monthly/klines/"));
        assert!(monthly_url.contains("BTCUSDT-1m-2021-01.zip"));
    }

    #[test]
    fn test_get_months_in_range() {
        // Test range spanning 3 months
        let start_time = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
        let end_time = NaiveDate::from_ymd_opt(2024, 3, 20).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
        
        let months = get_months_in_range(start_time, end_time);
        assert_eq!(months.len(), 3); // Jan, Feb, Mar
        assert_eq!(months[0], NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(months[1], NaiveDate::from_ymd_opt(2024, 2, 1).unwrap());
        assert_eq!(months[2], NaiveDate::from_ymd_opt(2024, 3, 1).unwrap());
    }
}