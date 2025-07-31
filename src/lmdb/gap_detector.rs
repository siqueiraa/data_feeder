use tracing::{info, debug, warn};
use crate::historical::structs::{FuturesOHLCVCandle, TimestampMS};

pub struct GapDetector {
    min_gap_threshold_ms: i64,
}

impl GapDetector {
    pub fn new(min_gap_threshold_seconds: u64) -> Self {
        Self {
            min_gap_threshold_ms: (min_gap_threshold_seconds * 1000) as i64,
        }
    }

    /// Detect all types of gaps in candle data
    pub fn detect_gaps(
        &self,
        candles: &[FuturesOHLCVCandle],
        timeframe_seconds: u64,
        requested_start: TimestampMS,
        requested_end: TimestampMS,
    ) -> Vec<(TimestampMS, TimestampMS)> {
        let mut gaps = Vec::new();
        
        if candles.is_empty() {
            // No data - entire range is a gap
            gaps.push((requested_start, requested_end));
            info!("📉 No existing data - entire range is a gap: {} to {}", requested_start, requested_end);
            return self.filter_gaps(gaps, timeframe_seconds);
        }

        // Sort candles by open_time to ensure proper chronological ordering
        let mut sorted_candles = candles.to_vec();
        sorted_candles.sort_by_key(|candle| candle.open_time);

        // 1. Check for prefix gap (before first candle)
        if let Some(first_candle) = sorted_candles.first() {
            if first_candle.open_time > requested_start {
                gaps.push((requested_start, first_candle.open_time));
                info!("📉 Found prefix gap: {} to {}", requested_start, first_candle.open_time);
            }
        }

        // 2. Check for internal gaps (between consecutive candles)
        let internal_gaps = self.detect_internal_gaps(&sorted_candles, timeframe_seconds);
        for gap in &internal_gaps {
            info!("🕳️ Found internal gap: {} to {}", gap.0, gap.1);
        }
        gaps.extend(internal_gaps);

        // 3. Check for suffix gap (after last candle)
        if let Some(last_candle) = sorted_candles.last() {
            // Use open_time + timeframe to get the end of the last period
            let last_period_end = last_candle.open_time + (timeframe_seconds * 1000) as i64;
            if last_period_end < requested_end {
                gaps.push((last_period_end, requested_end));
                info!("📈 Found suffix gap: {} to {}", last_period_end, requested_end);
            }
        }

        // 4. Filter and normalize gaps
        self.filter_gaps(gaps, timeframe_seconds)
    }

    /// Detect gaps between consecutive candles (internal gaps)
    fn detect_internal_gaps(
        &self,
        sorted_candles: &[FuturesOHLCVCandle],
        timeframe_seconds: u64,
    ) -> Vec<(TimestampMS, TimestampMS)> {
        if sorted_candles.len() < 2 {
            return Vec::new();
        }

        let mut gaps = Vec::new();
        let interval_ms = (timeframe_seconds * 1000) as i64;

        for window in sorted_candles.windows(2) {
            let current = &window[0];
            let next = &window[1];

            let expected_next_open_time = current.open_time + interval_ms;
            
            // Check if there's a gap between current and next candle
            if next.open_time > expected_next_open_time {
                // Found an internal gap
                let gap_start = expected_next_open_time;
                let gap_end = next.open_time;
                
                debug!(
                    "Internal gap detected: current_open={}, expected_next_open={}, actual_next_open={}, gap={}ms",
                    current.open_time, expected_next_open_time, next.open_time, gap_end - gap_start
                );
                
                gaps.push((gap_start, gap_end));
            }
        }

        gaps
    }

    /// Filter gaps based on various criteria
    fn filter_gaps(
        &self,
        gaps: Vec<(TimestampMS, TimestampMS)>,
        timeframe_seconds: u64,
    ) -> Vec<(TimestampMS, TimestampMS)> {
        let original_count = gaps.len();
        
        let filtered_gaps: Vec<(TimestampMS, TimestampMS)> = gaps
            .into_iter()
            .filter(|(start, end)| {
                let gap_duration_ms = end - start;
                
                // Filter out gaps smaller than minimum threshold
                if gap_duration_ms < self.min_gap_threshold_ms {
                    debug!("Filtering micro-gap: {}ms < {}ms threshold", gap_duration_ms, self.min_gap_threshold_ms);
                    return false;
                }
                
                // For 1-minute candles, filter out normal minute boundary transitions
                if timeframe_seconds == 60 && self.is_minute_boundary_transition(*start, *end) {
                    debug!("Filtering minute boundary transition: {} to {}", start, end);
                    return false;
                }
                
                true
            })
            .collect();

        let filtered_count = original_count - filtered_gaps.len();
        if filtered_count > 0 {
            info!("🔍 Filtered out {} micro-gaps/boundary transitions", filtered_count);
        }

        filtered_gaps
    }

    /// Check if gap represents a normal minute boundary transition
    fn is_minute_boundary_transition(&self, start: TimestampMS, end: TimestampMS) -> bool {
        let start_minute = self.normalize_to_minute_boundary(start);
        let end_minute = self.normalize_to_minute_boundary(end);
        start_minute == end_minute
    }

    /// Normalize timestamp to minute boundary (round down to nearest minute)
    fn normalize_to_minute_boundary(&self, timestamp_ms: TimestampMS) -> TimestampMS {
        (timestamp_ms / 60000) * 60000
    }

    /// Provide gap analysis summary
    pub fn analyze_gaps(
        &self,
        gaps: &[(TimestampMS, TimestampMS)],
        symbol: &str,
        timeframe_seconds: u64,
    ) -> GapAnalysis {
        if gaps.is_empty() {
            return GapAnalysis::default();
        }

        let total_missing_time_ms: i64 = gaps.iter().map(|(start, end)| end - start).sum();
        let total_missing_candles = total_missing_time_ms / (timeframe_seconds as i64 * 1000);
        
        let largest_gap = gaps.iter()
            .max_by_key(|(start, end)| end - start)
            .map(|(start, end)| (*start, *end));

        let analysis = GapAnalysis {
            symbol: symbol.to_string(),
            timeframe_seconds,
            gap_count: gaps.len(),
            total_missing_time_ms,
            total_missing_candles: total_missing_candles as u64,
            largest_gap,
        };

        // Log analysis
        warn!("📈 Gap Analysis for {}: {} gaps found", symbol, gaps.len());
        warn!("⚠️  Total missing time: {}ms ({} hours)", 
              total_missing_time_ms, total_missing_time_ms / (1000 * 3600));
        warn!("📉 Total missing candles: {}", total_missing_candles);
        
        if let Some((start, end)) = largest_gap {
            let largest_duration_hours = (end - start) / (1000 * 3600);
            warn!("🕳️ Largest gap: {} hours ({} to {})", largest_duration_hours, start, end);
        }

        analysis
    }
}

#[derive(Debug, Default, Clone)]
pub struct GapAnalysis {
    pub symbol: String,
    pub timeframe_seconds: u64,
    pub gap_count: usize,
    pub total_missing_time_ms: i64,
    pub total_missing_candles: u64,
    pub largest_gap: Option<(TimestampMS, TimestampMS)>,
}

impl GapAnalysis {
    pub fn missing_percentage(&self, total_requested_time_ms: i64) -> f64 {
        if total_requested_time_ms == 0 {
            return 0.0;
        }
        (self.total_missing_time_ms as f64 / total_requested_time_ms as f64) * 100.0
    }
}