use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::ops::Bound;
use std::time::Instant;

use futures::future::join_all;
// Database types moved to common::database_utils
use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, BoxError};
use kameo::{message::Message, Actor};
use kameo::message::Context;
use kameo::request::MessageSend;
use rustc_hash::FxHashMap;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use rayon::prelude::*;

use super::errors::HistoricalDataError;
use super::structs::{FuturesOHLCVCandle, TimeRange, TimestampMS, Seconds, FuturesExchangeTrade};
use crate::postgres::{PostgresActor, PostgresTell};
use crate::common::{constants::*, error_utils::ErrorContext, database_utils::DatabaseManager, shared_data::{intern_symbol, timeframe_to_string}};
use super::utils::{
    build_kline_urls_with_path_type, get_dates_in_range, get_months_in_range, should_use_monthly_data,
    is_kline_supported_timeframe, process_klines_pipeline, seconds_to_interval, PathType,
    format_timestamp, aggregate_candles_with_gap_filling, scan_for_candle_gaps, parse_csv_to_trade, download_file, extract_csv_from_zip, compute_sha256
};

// Moved to common::constants

pub struct HistoricalActor {
    database_manager: DatabaseManager,
    csv_path: PathBuf,
    postgres_actor: Option<ActorRef<PostgresActor>>,
}

impl HistoricalActor {
    pub fn new(symbols: &[String], timeframes: &[Seconds], base_path: &Path, csv_path: &Path) -> Result<Self, HistoricalDataError> {
        // Create CSV directory
        if !csv_path.exists() {
            info!("📁 Creating CSV directory at: {}", csv_path.display());
            std::fs::create_dir_all(csv_path)
                .with_io_context("Failed to create CSV directory")?;
        }

        // Initialize database manager
        let mut database_manager = DatabaseManager::new();
        database_manager.initialize_all(symbols, timeframes, base_path)?;

        info!("Initialized LMDB databases for {} symbols and {} timeframes", symbols.len(), timeframes.len());

        Ok(Self { 
            database_manager,
            csv_path: csv_path.to_path_buf(),
            postgres_actor: None,
        })
    }

    /// Set the PostgreSQL actor reference for dual storage
    pub fn set_postgres_actor(&mut self, postgres_actor: ActorRef<PostgresActor>) {
        self.postgres_actor = Some(postgres_actor);
    }

    fn get_certified_range(&self, symbol: &str, timeframe: Seconds) -> Result<Option<TimeRange>, HistoricalDataError> {
        let key = (intern_symbol(symbol), timeframe);
        let env = self
            .database_manager.envs
            .get(&key)
            .ok_or(HistoricalDataError::DatabaseNotFound(format!("Environment not found for {} {}", symbol, timeframe)))?;
        let certified_range_db = self
            .database_manager.certified_range_dbs
            .get(&key)
            .ok_or(HistoricalDataError::DatabaseNotFound(format!("Certified range DB not found for {} {}", symbol, timeframe)))?;

        let rtxn = env
            .read_txn()
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to create read transaction: {}", e)))?;
        let range_key = timeframe_to_string(timeframe);
        let range = certified_range_db
            .get(&rtxn, &range_key)
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to get certified range: {}", e)))?;

        Ok(range)
    }

    fn set_certified_range(&self, symbol: &str, timeframe: Seconds, range: TimeRange) -> Result<(), HistoricalDataError> {
        let key = (intern_symbol(symbol), timeframe);
        let env = self
            .database_manager.envs
            .get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseNotFound(format!("Environment not found for {} {}", symbol, timeframe)))?;
        let certified_range_db = self
            .database_manager.certified_range_dbs
            .get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseNotFound(format!("Certified range DB not found for {} {}", symbol, timeframe)))?;

        let mut wtxn = env
            .write_txn()
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to create write transaction: {}", e)))?;
        let range_key = timeframe_to_string(timeframe);
        certified_range_db
            .put(&mut wtxn, &range_key, &range)
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to store certified range: {}", e)))?;
        wtxn.commit()
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to commit transaction: {}", e)))?;
        info!("Set CertifiedRange for symbol {} timeframe {}: start={}, end={}", symbol, timeframe, range.start, range.end);
        Ok(())
    }

    fn store_candles(
        &self,
        symbol: &str,
        timeframe: Seconds,
        candles: &[FuturesOHLCVCandle],
        start_time: TimestampMS,
        end_time: TimestampMS,
    ) -> Result<(), HistoricalDataError> {
        info!(
            "store_candles called for {} timeframe {}s: {} candles, range {} to {}",
            symbol,
            timeframe,
            candles.len(),
            format_timestamp(start_time),
            format_timestamp(end_time)
        );
        let key = (intern_symbol(symbol), timeframe);
        let env = self
            .database_manager.envs
            .get(&key)
            .ok_or(HistoricalDataError::DatabaseNotFound(format!("Environment not found for {} {}", symbol, timeframe)))?;
        let candle_db = self
            .database_manager.candle_dbs
            .get(&key)
            .ok_or(HistoricalDataError::DatabaseNotFound(format!("Database not found for {} {}", symbol, timeframe)))?;

        let write_start = Instant::now();

        let mut wtxn = env
            .write_txn()
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to create write transaction: {}", e)))?;

        for chunk in candles.chunks(BATCH_SIZE) {
            for candle in chunk {
                let key = format!("{}:{}", timeframe, candle.close_time());
                candle_db
                    .put(&mut wtxn, &key, candle)
                    .map_err(|e| HistoricalDataError::StorageError(format!("Failed to store candle: {}", e)))?;
            }
        }

        wtxn.commit()
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to commit transaction: {}", e)))?;

        let write_duration = write_start.elapsed();
        info!("Time taken to write {} candles to LMDB: {:?}", candles.len(), write_duration);

        info!("Stored {} new candles to LMDB for {} timeframe {}s", candles.len(), symbol, timeframe);

        let (new_candles_start, new_candles_end) = match (candles.first(), candles.last()) {
            (Some(first), Some(last)) => (first.open_time(), last.close_time()),
            _ => {
                info!("No candles provided to store for symbol {}, timeframe {}s", symbol, timeframe);
                info!("NOT updating certified range - no candles to store");
                return Ok(());
            }
        };

        info!("Checking gaps in newly stored candles: {} to {}", format_timestamp(new_candles_start), format_timestamp(new_candles_end));

        let gaps_in_new = scan_for_candle_gaps(candles, timeframe, new_candles_start, new_candles_end);
        info!("Gap scan for new candles: start={}, end={}, candles={}, gaps={:?}", new_candles_start, new_candles_end, candles.len(), gaps_in_new);

        if !gaps_in_new.is_empty() {
            warn!("Gaps found in NEW candle data for symbol {}, timeframe {}s: {:?}", symbol, timeframe, gaps_in_new);
            warn!("NOT updating certified range due to gaps in new data");
            return Ok(());
        }

        let existing_certified = self.get_certified_range(symbol, timeframe)?;

        let new_range = TimeRange { start: new_candles_start, end: new_candles_end + 1 };

        if let Some(existing) = &existing_certified {
            info!("Existing certified range: {} to {}", format_timestamp(existing.start), format_timestamp(existing.end));

            let gap_between = if new_range.start > existing.end {
                new_range.start - existing.end
            } else if existing.start > new_range.end {
                existing.start - new_range.end
            } else {
                0
            };

            if gap_between == 0 {
                let merged_range = TimeRange { start: existing.start.min(new_range.start), end: existing.end.max(new_range.end) };

                self.set_certified_range(symbol, timeframe, merged_range)?;
                info!("✅ Merged adjacent certified ranges for symbol {}, timeframe {}s: {:?}", symbol, timeframe, merged_range);
            } else {
                let extended_range = TimeRange { start: existing.start.min(new_range.start), end: existing.end.max(new_range.end) };

                warn!("Gap of {} ms between existing and new certified ranges.", gap_between);
                info!("Extending certified range to cover both ranges for symbol {}, timeframe {}s: {:?}", symbol, timeframe, extended_range);

                self.set_certified_range(symbol, timeframe, extended_range)?;
            }
        } else {
            self.set_certified_range(symbol, timeframe, new_range)?;
            info!("Set initial certified range for symbol {}, timeframe {}s: {:?}", symbol, timeframe, new_range);
        }

        // Store to PostgreSQL for dual storage strategy
        if let Some(postgres_actor) = &self.postgres_actor {
            let symbol_owned = symbol.to_string();
            let candles_len = candles.len();
            let candles_vec: Vec<(String, FuturesOHLCVCandle)> = candles
                .iter()
                .map(|candle| (symbol_owned.clone(), candle.clone()))
                .collect();
            
            // Log time range of historical data being stored
            let start_time = candles.first().map(|c| c.open_time).unwrap_or(0);
            let end_time = candles.last().map(|c| c.close_time).unwrap_or(0);
            let start_timestamp = chrono::DateTime::from_timestamp_millis(start_time)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| format!("INVALID_TIME({})", start_time));
            let end_timestamp = chrono::DateTime::from_timestamp_millis(end_time)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| format!("INVALID_TIME({})", end_time));
                
            info!("🔄 [HistoricalActor] Sending batch of {} historical candles to PostgreSQL for {} (range: {} - {})", 
                  candles_len, symbol_owned, start_timestamp, end_timestamp);
            
            let postgres_msg = PostgresTell::StoreBatch {
                candles: candles_vec,
                source: "HistoricalActor".to_string(),
            };
            
            let postgres_ref = postgres_actor.clone();
            let symbol_for_log = symbol_owned.clone();
            tokio::spawn(async move {
                if let Err(e) = postgres_ref.tell(postgres_msg).send().await {
                    warn!("❌ [HistoricalActor] Failed to store historical batch to PostgreSQL for {}: {}", symbol_owned, e);
                } else {
                    info!("✅ [HistoricalActor] Successfully sent {} historical candles to PostgreSQL for {} (range: {} - {})", 
                          candles_len, symbol_for_log, start_timestamp, end_timestamp);
                }
            });
        } else {
            warn!("⚠️  [HistoricalActor] PostgreSQL actor not available for storing {} historical candles", candles.len());
        }

        Ok(())
    }

    pub fn get_candles(
        &self,
        symbol: &str,
        timeframe: Seconds,
        start_time: TimestampMS,
        end_time: TimestampMS,
    ) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
        let fetch_start = Instant::now();

        let key = (intern_symbol(symbol), timeframe);
        let env = self
            .database_manager.envs
            .get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseNotFound(format!("Environment not found for {} {}", symbol, timeframe)))?;
        let db = self
            .database_manager.candle_dbs
            .get(&key)
            .ok_or_else(|| HistoricalDataError::DatabaseNotFound(format!("Database not found for {} {}", symbol, timeframe)))?;

        let rtxn = env
            .read_txn()
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to create read transaction: {}", e)))?;

        let start_key = format!("{}:{}", timeframe, start_time);
        let end_key = format!("{}:{}", timeframe, end_time);
        let range_bounds = (Bound::Included(start_key.as_str()), Bound::Included(end_key.as_str()));

        let mut candles = Vec::new();
        let range_iter = db
            .range(&rtxn, &range_bounds)
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to apply range: {}", e)))?;
        for result in range_iter {
            let (_key, candle) = result.map_err(|e| HistoricalDataError::StorageError(format!("Failed to read candle: {}", e)))?;
            candles.push(candle);
        }

        let fetch_duration = fetch_start.elapsed();
        info!("Time taken to fetch {} candles from LMDB: {:?}", candles.len(), fetch_duration);

        if candles.is_empty() {
            return Ok(candles);
        }

        candles.sort_by_key(|c| c.open_time());

        let mut filled_candles = Vec::new();
        let mut last_candle: Option<&FuturesOHLCVCandle> = None;

        for candle in &candles {
            if let Some(prev_candle) = last_candle {
                let expected_next_open_time = prev_candle.open_time() + (timeframe as i64 * 1000);
                if candle.open_time() > expected_next_open_time {
                    let mut current_time = expected_next_open_time;
                    while current_time < candle.open_time() {
                        let filler_candle = FuturesOHLCVCandle::new_from_values(
                            current_time,
                            current_time + (timeframe as i64 * 1000) - 1,
                            prev_candle.close(),
                            prev_candle.close(),
                            prev_candle.close(),
                            prev_candle.close(),
                            0.0,
                            current_time,
                            true,
                        );
                        filled_candles.push(filler_candle);
                        current_time += timeframe as i64 * 1000;
                    }
                }
            }
            filled_candles.push(candle.clone());
            last_candle = Some(candle);
        }

        info!("Gap filling complete. Total candles: {}", filled_candles.len());
        Ok(filled_candles)
    }

    pub async fn orchestrate_aggtrade_to_candles_for_timeframe(
        &self,
        symbol: &str,
        start_time: TimestampMS,
        end_time: TimestampMS,
        specific_timeframe: Option<Seconds>,
    ) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
        let timeframes = if let Some(tf) = specific_timeframe { vec![tf] } else { vec![60, 1800, 3600] }; // Default timeframes

        let mut overall_continuous_start = start_time;
        let mut overall_continuous_end = end_time;

        let mut ranges_to_fetch = Vec::new();
        for &tf in &timeframes {
            let certified_range = self.get_certified_range(symbol, tf)?;
            let mut tf_ranges = Vec::new();

            info!(
                "Checking certified range for {} tf {}s: {:?} (requested: {} to {})",
                symbol,
                tf,
                certified_range,
                format_timestamp(start_time),
                format_timestamp(end_time)
            );

            if let Some(cert) = certified_range {
                let overlaps = !(end_time <= cert.start || start_time >= cert.end);

                if overlaps {
                    if start_time < cert.start {
                        let day_start = start_time - (start_time % (24 * 60 * 60 * 1000));
                        let fetch_range = (day_start, cert.start);
                        info!("Need to fetch BEFORE certified range: {} to {}", format_timestamp(fetch_range.0), format_timestamp(fetch_range.1));
                        tf_ranges.push(fetch_range);
                        overall_continuous_start = overall_continuous_start.min(start_time);
                    }

                    if end_time > cert.end {
                        let day_end = if end_time % (24 * 60 * 60 * 1000) == 0 {
                            end_time
                        } else {
                            end_time + (24 * 60 * 60 * 1000 - (end_time % (24 * 60 * 60 * 1000)))
                        };
                        let fetch_range = (cert.end, day_end);
                        info!("Need to fetch AFTER certified range: {} to {}", format_timestamp(fetch_range.0), format_timestamp(fetch_range.1));
                        tf_ranges.push(fetch_range);
                        overall_continuous_end = overall_continuous_end.max(end_time);
                    }

                    overall_continuous_start = overall_continuous_start.min(cert.start);
                    overall_continuous_end = overall_continuous_end.max(cert.end);
                } else {
                    info!("Requested range doesn't overlap with certified range.");

                    if end_time <= cert.start {
                        info!("Requested range is before certified range. Need to fetch requested range and gap.");
                        let day_start = start_time - (start_time % (24 * 60 * 60 * 1000));
                        tf_ranges.push((day_start, cert.start));
                        overall_continuous_start = overall_continuous_start.min(start_time);
                        overall_continuous_end = overall_continuous_end.max(cert.end);
                    } else {
                        info!("Requested range is after certified range. Need to fetch gap and requested range.");
                        let day_end = if end_time % (24 * 60 * 60 * 1000) == 0 {
                            end_time
                        } else {
                            end_time + (24 * 60 * 60 * 1000 - (end_time % (24 * 60 * 60 * 1000)))
                        };
                        tf_ranges.push((cert.end, day_end));
                        overall_continuous_start = overall_continuous_start.min(cert.start);
                        overall_continuous_end = overall_continuous_end.max(end_time);
                    }
                }
            } else {
                let day_start = start_time - (start_time % (24 * 60 * 60 * 1000));
                let day_end = if end_time % (24 * 60 * 60 * 1000) == 0 {
                    end_time
                } else {
                    end_time + (24 * 60 * 60 * 1000 - (end_time % (24 * 60 * 60 * 1000)))
                };
                info!(
                    "No certified range exists, fetching entire range (extended to full days): {} to {}",
                    format_timestamp(day_start),
                    format_timestamp(day_end)
                );
                tf_ranges.push((day_start, day_end));
            }
            ranges_to_fetch.push((tf, tf_ranges));
        }

        let needs_fetching = ranges_to_fetch.iter().any(|(_, ranges)| !ranges.is_empty());

        if !needs_fetching {
            info!(
                "All timeframes already have certified data for range {} - {}. Returning cached candles for primary timeframe.",
                format_timestamp(start_time),
                format_timestamp(end_time)
            );
            let primary_tf = timeframes[0];
            return self.get_candles(symbol, primary_tf, start_time, end_time);
        }

        let mut unique_ranges_to_process = std::collections::HashSet::new();
        for (_, ranges) in &ranges_to_fetch {
            for &(fetch_start, fetch_end) in ranges {
                if fetch_start < fetch_end {
                    let buffer = 24 * 60 * 60 * 1000;
                    let load_start_time = fetch_start.saturating_sub(buffer);
                    unique_ranges_to_process.insert((load_start_time, fetch_end));
                }
            }
        }
        info!("Unique daily ranges to fetch trades for {}: {:?}", symbol, unique_ranges_to_process.len());

        let mut final_candles_by_timeframe: FxHashMap<Seconds, Vec<FuturesOHLCVCandle>> = FxHashMap::default();
        for &tf in &timeframes {
            final_candles_by_timeframe.insert(tf, Vec::new());
        }

        for (load_start_time, fetch_end) in unique_ranges_to_process {
            let trades = self.fetch_trades_for_range(symbol, load_start_time, fetch_end).await?;
            if let (Some(first_trade), Some(last_trade)) = (trades.first(), trades.last()) {
                for &tf in &timeframes {
                    let aligned_start = (first_trade.timestamp / (tf as i64 * 1000)) * (tf as i64 * 1000);
                    let aligned_end = ((last_trade.timestamp / (tf as i64 * 1000)) + 1) * (tf as i64 * 1000);
                    let candles = aggregate_candles_with_gap_filling(&trades, tf, aligned_start, aligned_end, false);
                    final_candles_by_timeframe.entry(tf).or_default().extend(candles);
                }
            }
        }

        for (tf, candles) in &mut final_candles_by_timeframe {
            candles.par_sort_by_key(|c| c.open_time());
            candles.dedup_by_key(|c| c.open_time());
            info!("Final sort and dedup: {} candles for timeframe {}s", candles.len(), tf);

            if let (Some(first_candle), Some(last_candle)) = (candles.first(), candles.last()) {
                let candle_start = first_candle.open_time();
                let candle_end = last_candle.close_time() + 1;

                info!(
                    "Storing {} candles for timeframe {}s, range: {} to {}",
                    candles.len(),
                    tf,
                    format_timestamp(candle_start),
                    format_timestamp(candle_end)
                );

                self.store_candles(symbol, *tf, candles, candle_start, candle_end)?;

                self.set_certified_range(symbol, *tf, TimeRange { start: candle_start, end: candle_end })?;
                info!("✅ Updated certified range for symbol {}, timeframe {}s", symbol, tf);
            }
        }

        let smallest_tf = *timeframes.iter().min().unwrap_or(&60);
        let final_candles = final_candles_by_timeframe
            .get(&smallest_tf)
            .ok_or_else(|| HistoricalDataError::NoData(format!("Smallest timeframe {} candles not found after processing", smallest_tf)))?
            .clone();

        if final_candles.is_empty() && (start_time < end_time) {
            warn!(
                "No candles generated for smallest timeframe {}s after processing for range {} - {}. This might be expected if no trades occurred.",
                smallest_tf,
                format_timestamp(start_time),
                format_timestamp(end_time)
            );
        }

        Ok(final_candles)
    }

    /// Generate URLs for trade data files for a date range
    fn build_trade_urls(&self, symbol: &str, start_time: TimestampMS, end_time: TimestampMS) -> Vec<(String, String, String)> {
        let dates = get_dates_in_range(start_time, end_time);
        let base_url = "https://data.binance.vision/";
        let prefix = format!("data/futures/um/daily/klines/{}/", symbol.to_uppercase());

        dates
            .into_iter()
            .map(|date| {
                let date_str = date.format("%Y-%m-%d").to_string();
                let zip_file = format!("{}-aggTrades-{}.zip", symbol.to_uppercase(), date_str);
                let checksum_file = format!("{}-aggTrades-{}.zip.CHECKSUM", symbol.to_uppercase(), date_str);
                let zip_url = format!("{}/{}{}", base_url, prefix, zip_file);
                let checksum_url = format!("{}/{}{}", base_url, prefix, checksum_file);
                (zip_url, checksum_url, date_str)
            })
            .collect()
    }

    /// Download and parse a single trade file for a specific date
    async fn fetch_single_trade_file(
        &self,
        zip_url: String,
        checksum_url: String,
        date_str: String,
        symbol: String,
        semaphore: Arc<Semaphore>,
    ) -> Result<Vec<FuturesExchangeTrade>, HistoricalDataError> {
        let file_name = zip_url.rsplit('/').next().unwrap_or("unknown");
        let csv_file_name = format!("{}-aggTrades-{}.csv", symbol.to_uppercase(), date_str);
        let csv_local_path = self.csv_path.join(csv_file_name);
        let zip_local_path = self.csv_path.join(file_name);
        let checksum_local_path = self.csv_path.join(
            checksum_url.rsplit('/').next().unwrap_or("unknown_checksum"));

        let file_exists = tokio::fs::try_exists(&csv_local_path).await.unwrap_or(false);

        if file_exists {
            self.load_existing_trade_file(&csv_local_path, &semaphore).await
        } else {
            self.download_and_parse_trade_file(
                &zip_url, &checksum_url, &csv_local_path, 
                &zip_local_path, &checksum_local_path, &semaphore
            ).await
        }
    }

    /// Load an existing CSV trade file
    async fn load_existing_trade_file(
        &self,
        csv_local_path: &std::path::Path,
        semaphore: &Arc<Semaphore>,
    ) -> Result<Vec<FuturesExchangeTrade>, HistoricalDataError> {
        info!("📂 CSV file exists: {}", csv_local_path.display());

        let csv_data = {
            let _permit = semaphore.acquire().await
                .map_err(|e| HistoricalDataError::SemaphoreAcquisition(format!("Failed to acquire semaphore: {}", e)))?;
            Arc::new(
                tokio::fs::read(csv_local_path)
                    .await
                    .map_err(HistoricalDataError::Io)?,
            )
        };

        tokio::task::spawn_blocking({
            let csv_data = Arc::clone(&csv_data);
            move || parse_csv_to_trade(&csv_data)
        })
        .await
        .map_err(|e| HistoricalDataError::StorageError(format!("Failed to parse CSV: {}", e)))?
    }

    /// Download and parse a trade file from remote source
    async fn download_and_parse_trade_file(
        &self,
        zip_url: &str,
        checksum_url: &str,
        csv_local_path: &std::path::Path,
        zip_local_path: &std::path::Path,
        checksum_local_path: &std::path::Path,
        semaphore: &Arc<Semaphore>,
    ) -> Result<Vec<FuturesExchangeTrade>, HistoricalDataError> {
        // Download checksum
        info!("📥 Downloading checksum: {}", checksum_url);
        if let Err(e) = download_file(checksum_url, checksum_local_path).await {
            if let HistoricalDataError::NotFound(_) = e {
                warn!("Checksum not found for {}, assuming no data for this day.", 
                      checksum_local_path.file_name().unwrap_or_default().to_string_lossy());
                return Ok(Vec::new()); // No data for this day
            }
            return Err(e);
        }

        // Verify checksum
        let expected_hash = self.read_checksum_file(checksum_local_path).await?;

        // Download ZIP file
        info!("📥 Downloading ZIP: {}", zip_url);
        let temp_zip_path = zip_local_path.with_extension("zip.tmp");
        if let Err(e) = download_file(zip_url, &temp_zip_path).await {
            if let HistoricalDataError::NotFound(_) = e {
                warn!("ZIP not found for {}, assuming no data for this day.", 
                      zip_local_path.file_name().unwrap_or_default().to_string_lossy());
                return Ok(Vec::new()); // No data for this day
            }
            return Err(e);
        }

        // Verify downloaded file
        self.verify_and_extract_trade_file(&temp_zip_path, &expected_hash, csv_local_path, semaphore).await
    }

    /// Read and parse checksum file
    async fn read_checksum_file(&self, checksum_path: &std::path::Path) -> Result<String, HistoricalDataError> {
        let checksum_content = tokio::fs::read_to_string(checksum_path)
            .await
            .map_err(HistoricalDataError::Io)?;
        
        checksum_content
            .split_whitespace()
            .next()
            .map(|s| s.to_string())
            .ok_or_else(|| HistoricalDataError::Validation("Invalid checksum format".to_string()))
    }

    /// Verify ZIP file integrity and extract trades
    async fn verify_and_extract_trade_file(
        &self,
        temp_zip_path: &std::path::Path,
        expected_hash: &str,
        csv_local_path: &std::path::Path,
        semaphore: &Arc<Semaphore>,
    ) -> Result<Vec<FuturesExchangeTrade>, HistoricalDataError> {
        let downloaded_hash = compute_sha256(temp_zip_path)?;
        if downloaded_hash != expected_hash {
            tokio::fs::remove_file(temp_zip_path)
                .await
                .map_err(HistoricalDataError::Io)?;
            return Err(HistoricalDataError::ChecksumError(format!(
                "Checksum mismatch: expected {}, got {}",
                expected_hash, downloaded_hash
            )));
        }

        let csv_data = {
            let _permit = semaphore.acquire().await
                .map_err(|e| HistoricalDataError::SemaphoreAcquisition(format!("Failed to acquire semaphore: {}", e)))?;

            let csv_data = tokio::task::spawn_blocking({
                let temp_zip_path = temp_zip_path.to_path_buf();
                move || extract_csv_from_zip(&temp_zip_path)
            })
            .await
            .map_err(|e| HistoricalDataError::StorageError(format!("Failed to extract ZIP: {}", e)))??;

            tokio::fs::write(csv_local_path, &csv_data)
                .await
                .map_err(HistoricalDataError::Io)?;

            tokio::fs::remove_file(temp_zip_path)
                .await
                .map_err(HistoricalDataError::Io)?;

            Arc::new(csv_data)
        };

        tokio::task::spawn_blocking({
            let csv_data = Arc::clone(&csv_data);
            move || parse_csv_to_trade(&csv_data)
        })
        .await
        .map_err(|e| HistoricalDataError::StorageError(format!("Failed to parse CSV: {}", e)))?
    }

    /// Main entry point for fetching trades in a date range
    async fn fetch_trades_for_range(
        &self,
        symbol: &str,
        start_time: TimestampMS,
        end_time: TimestampMS,
    ) -> Result<Vec<FuturesExchangeTrade>, HistoricalDataError> {
        let pairs = self.build_trade_urls(symbol, start_time, end_time);
        let semaphore = Arc::new(Semaphore::new(3));
        let symbol_owned = symbol.to_string();

        // Create futures for parallel processing
        let trade_futures = pairs.into_iter().map(|(zip_url, checksum_url, date_str)| {
            let semaphore = Arc::clone(&semaphore);
            let symbol_clone = symbol_owned.clone();
            let self_ref = self;
            async move {
                self_ref.fetch_single_trade_file(zip_url, checksum_url, date_str, symbol_clone, semaphore).await
            }
        });

        // Execute all futures concurrently
        let processing_start = Instant::now();
        let futures_count = trade_futures.len();
        info!("Processing {} trade file futures concurrently...", futures_count);

        let all_results = join_all(trade_futures).await;
        
        let elapsed = processing_start.elapsed();
        info!("All futures completed in {:?} - {} futures/sec", elapsed, futures_count as f64 / elapsed.as_secs_f64());

        // Collect results and filter by time range
        let mut all_trades = Vec::new();
        for result in all_results {
            let trades = result?;
            let filtered_trades: Vec<FuturesExchangeTrade> = trades
                .into_iter()
                .filter(|t| t.timestamp >= start_time && t.timestamp <= end_time)
                .collect();
            
            if !filtered_trades.is_empty() {
                info!("Fetched {} trades for a sub-range.", filtered_trades.len());
                all_trades.extend(filtered_trades);
            }
        }

        if all_trades.is_empty() {
            return Err(HistoricalDataError::NoData(format!(
                "No aggTrades found for {} in range {} to {}", 
                symbol, start_time, end_time
            )));
        }

        all_trades.sort_by_key(|t| t.timestamp);
        Ok(all_trades)
    }

    async fn try_fetch_klines_with_fallback(
        &self,
        symbol: &str,
        start_time: TimestampMS,
        end_time: TimestampMS,
        timeframe: Seconds,
        interval: &str,
    ) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
        // Smart path selection: use monthly data for large gaps (> 2 months)
        let use_monthly = should_use_monthly_data(start_time, end_time);
        
        if use_monthly {
            info!("🗓️ Attempting to fetch MONTHLY data for large gap (> 2 months)");
            
            // Try monthly data first
            match self.fetch_klines_with_path_type(symbol, start_time, end_time, timeframe, interval, PathType::Monthly).await {
                Ok(candles) if !candles.is_empty() => {
                    info!("✅ Successfully fetched {} candles using MONTHLY data", candles.len());
                    return Ok(candles);
                }
                Ok(_) => {
                    info!("📊 Monthly data returned empty, falling back to daily");
                }
                Err(e) => {
                    warn!("⚠️ Monthly data fetch failed: {}. Falling back to daily data", e);
                }
            }
            
            // Fallback to daily data
            info!("📅 Falling back to DAILY data for large gap");
            self.fetch_klines_with_path_type(symbol, start_time, end_time, timeframe, interval, PathType::Daily).await
        } else {
            info!("📅 Using DAILY data for smaller gap (< 2 months)");
            self.fetch_klines_with_path_type(symbol, start_time, end_time, timeframe, interval, PathType::Daily).await
        }
    }

    async fn fetch_klines_with_path_type(
        &self,
        symbol: &str,
        start_time: TimestampMS,
        end_time: TimestampMS,
        timeframe: Seconds,
        interval: &str,
        path_type: PathType,
    ) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
        let urls = match path_type {
            PathType::Monthly => {
                let months = get_months_in_range(start_time, end_time);
                if months.is_empty() {
                    return Err(HistoricalDataError::NoData(format!("No months in range {} to {}", start_time, end_time)));
                }
                build_kline_urls_with_path_type(symbol, interval, months, PathType::Monthly)
            }
            PathType::Daily => {
                let dates = get_dates_in_range(start_time, end_time);
                if dates.is_empty() {
                    return Err(HistoricalDataError::NoData(format!("No dates in range {} to {}", start_time, end_time)));
                }
                build_kline_urls_with_path_type(symbol, interval, dates, PathType::Daily)
            }
        };
        
        if urls.is_empty() {
            return Err(HistoricalDataError::NoData(format!("No URLs generated for {:?} range {} to {}", path_type, start_time, end_time)));
        }
        
        let data_type = match path_type {
            PathType::Monthly => "monthly",
            PathType::Daily => "daily",
        };
        info!("📊 Will fetch {} {} periods of kline data", urls.len(), data_type);

        let concurrency = 8; 
        let semaphore = Arc::new(Semaphore::new(concurrency));
        info!("🚀 Using concurrency of {} for kline processing", concurrency);

        let klines_csv_path = self.csv_path.clone();

        let mut tasks = Vec::new();
        for (zip_url, checksum_url, date_str) in urls {
            let csv_path_clone = klines_csv_path.clone();
            let symbol_clone = symbol.to_string();
            let interval_clone = interval.to_string();
            let semaphore_clone = semaphore.clone();

            tasks.push(tokio::spawn(async move {
                let _permit = match semaphore_clone.acquire().await {
                    Ok(permit) => permit,
                    Err(e) => {
                        warn!("Failed to acquire semaphore for klines processing: {}", e);
                        return Err(HistoricalDataError::SemaphoreAcquisition(format!("Failed to acquire semaphore: {}", e)));
                    }
                };
                process_klines_pipeline(&csv_path_clone, &symbol_clone, &zip_url, &checksum_url, &date_str, &interval_clone).await
            }));
        }

        let processing_start = Instant::now();
        let num_tasks = tasks.len();
        info!("🚀 Started {} Tokio tasks for {} kline downloads...", num_tasks, data_type);

        let results = join_all(tasks).await;
        let mut all_candles = Vec::new();
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(Ok(candles)) => {
                    all_candles.extend(candles);
                }
                Ok(Err(e)) => {
                    if let HistoricalDataError::NotFound(_) = e {
                        warn!("Task {} failed as {} data was not found, skipping period.", i, data_type);
                        continue;
                    }
                    error!("❌ Task {} failed: {}", i, e);
                    return Err(e);
                }
                Err(e) => {
                    error!("❌ Task {} panicked: {:?}", i, e);
                    return Err(HistoricalDataError::StorageError("Tokio task panic".to_string()));
                }
            }
        }

        let elapsed = processing_start.elapsed();
        info!("🚀 ALL {} {} kline tasks completed in {:?} - {:.2} tasks/sec", num_tasks, data_type, elapsed, num_tasks as f64 / elapsed.as_secs_f64());

        all_candles.par_sort_by_key(|c| c.open_time());
        all_candles.retain(|c| c.open_time() >= start_time && c.close_time() < end_time);

        info!("📊 Total {} klines collected: {} for timeframe {}", data_type, all_candles.len(), interval);

        if let (Some(first_candle), Some(last_candle)) = (all_candles.first(), all_candles.last()) {
            let candle_start = first_candle.open_time();
            let candle_end = last_candle.close_time() + 1;

            self.store_candles(symbol, timeframe, &all_candles, candle_start, candle_end)?;

            self.set_certified_range(symbol, timeframe, TimeRange { start: candle_start, end: candle_end })?;
            info!("✅ Stored {} {} klines and updated certified range", all_candles.len(), data_type);
        }

        Ok(all_candles)
    }

    pub async fn fetch_and_process_klines(
        &self,
        symbol: &str,
        start_time: TimestampMS,
        end_time: TimestampMS,
        timeframe: Seconds,
    ) -> Result<Vec<FuturesOHLCVCandle>, HistoricalDataError> {
        let interval = seconds_to_interval(timeframe)?;
        info!(
            "📊 Fetching klines for {} timeframe {} ({}) from {} to {}",
            symbol,
            timeframe,
            interval,
            format_timestamp(start_time),
            format_timestamp(end_time)
        );

        let certified_range = self.get_certified_range(symbol, timeframe)?;
        if let Some(cert) = certified_range {
            if start_time >= cert.start && end_time <= cert.end {
                info!("📊 Klines already certified for requested range, returning from LMDB");
                return self.get_candles(symbol, timeframe, start_time, end_time);
            }
        }

        // Use the smart fallback method
        self.try_fetch_klines_with_fallback(symbol, start_time, end_time, timeframe, interval).await
    }

    pub fn invalidate_certified_range(&mut self, symbol: &str, timeframe: Seconds) -> Result<(), HistoricalDataError> {
        let key = (intern_symbol(symbol), timeframe);
        if let Some(db) = self.database_manager.certified_range_dbs.get(&key) {
            let mut wtxn = self.database_manager.envs[&key]
                .write_txn()
                .map_err(|e| HistoricalDataError::StorageError(e.to_string()))?;
            let db_key = timeframe_to_string(timeframe);
            db.delete(&mut wtxn, &db_key)
                .map_err(|e| HistoricalDataError::StorageError(e.to_string()))?;
            wtxn.commit().map_err(|e| HistoricalDataError::StorageError(e.to_string()))?;
            info!("Invalidated certified range for symbol {}, timeframe {}s", symbol, timeframe);
        }
        Ok(())
    }
}

impl Actor for HistoricalActor {
    type Mailbox = kameo::mailbox::unbounded::UnboundedMailbox<Self>;

    fn name() -> &'static str {
        "HistoricalActor"
    }

    async fn on_start(&mut self, _actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        info!("HistoricalActor started");
        Ok(())
    }

    async fn on_stop(&mut self, _actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> Result<(), BoxError> {
        error!("HistoricalActor stopping: {:?}", reason);
        Ok(())
    }
}

#[derive(Debug)]
pub enum HistoricalAsk {
    GetCandles {
        symbol: String,
        timeframe: Seconds,
        start_time: TimestampMS,
        end_time: TimestampMS,
    },
}

#[derive(Debug)]
pub enum HistoricalReply {
    Candles(Vec<FuturesOHLCVCandle>),
}

impl Message<HistoricalAsk> for HistoricalActor {
    type Reply = Result<HistoricalReply, HistoricalDataError>;

    async fn handle(&mut self, msg: HistoricalAsk, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        match msg {
            HistoricalAsk::GetCandles { symbol, timeframe, start_time, end_time } => {
                // Wrap the entire logic in a match to handle errors gracefully
                let result = async {
                    let range = TimeRange { start: start_time, end: end_time };

                    let certified_range = self.get_certified_range(&symbol, timeframe)?;
                    info!("certified_range: {:?}", certified_range);

                    if certified_range.as_ref().is_some_and(|cert| range.start >= cert.start && range.end <= cert.end) {
                        info!(
                            "Data is fully certified for symbol {} timeframe {}: requested range {} to {}, certified range: {:?}",
                            symbol, timeframe, range.start, range.end, certified_range
                        );

                        match self.get_candles(&symbol, timeframe, start_time, end_time) {
                            Ok(candles) => {
                                return Ok(candles);
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to fetch certified candles for symbol {}, timeframe {}s: {}. Invalidating certified range.",
                                    symbol, timeframe, e
                                );
                                self.invalidate_certified_range(&symbol, timeframe)?;
                            }
                        }
                    } else {
                        info!(
                            "Data is not fully certified for symbol {} timeframe {}: requested range {} to {}, certified range: {:?}",
                            symbol, timeframe, range.start, range.end, certified_range
                        );
                    }

                    if is_kline_supported_timeframe(timeframe) {
                        info!("Using klines for timeframe {}s (Binance supported interval)", timeframe);
                        self.fetch_and_process_klines(&symbol, range.start, range.end, timeframe).await?;
                    } else {
                        info!("Using aggTrades for timeframe {}s (non-standard interval)", timeframe);
                        self.orchestrate_aggtrade_to_candles_for_timeframe(&symbol, range.start, range.end, Some(timeframe))
                            .await?;
                    }

                    self.set_certified_range(&symbol, timeframe, range)?;
                    info!("✅ Set certified range for symbol {}, timeframe {}s after successful orchestration: {:?}", symbol, timeframe, range);

                    let candles = self.get_candles(&symbol, timeframe, start_time, end_time)?;
                    Ok(candles)
                }.await;

                match result {
                    Ok(candles) => Ok(HistoricalReply::Candles(candles)),
                    Err(e) => {
                        error!("Failed to handle GetCandles request: {:?}", e);
                        Err(e)
                    }
                }
            }
        }
    }
}
