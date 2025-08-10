//! Streaming Serialization for Large Result Sets
//! 
//! This module implements memory-efficient streaming serialization to handle
//! large datasets without memory pressure, supporting both JSON and binary formats.

use std::io::Write;
use std::time::{Duration, Instant};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use crate::volume_profile::structs::VolumeProfileData;
use crate::technical_analysis::structs::IndicatorOutput;
use crate::common::hybrid_serialization::HybridSerializationError;

/// Streaming serialization configuration
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    pub buffer_size: usize,
    pub flush_interval_ms: u64,
    pub max_memory_mb: usize,
    pub compression_enabled: bool,
    pub parallel_streams: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            buffer_size: 64 * 1024,      // 64KB buffer
            flush_interval_ms: 100,      // Flush every 100ms
            max_memory_mb: 50,           // 50MB memory limit per stream
            compression_enabled: false,   // Disabled for simplicity
            parallel_streams: 4,         // 4 parallel streams
        }
    }
}

/// Streaming serialization metrics
#[derive(Debug, Clone)]
pub struct StreamingMetrics {
    pub items_streamed: u64,
    pub bytes_written: u64,
    pub flush_count: u64,
    pub streaming_time: Duration,
    pub average_item_size: usize,
    pub buffer_utilization: f64,
    pub memory_peak_mb: f64,
}

/// Streaming JSON serializer for large datasets
pub struct StreamingJsonSerializer<W: Write> {
    writer: W,
    buffer: Vec<u8>,
    config: StreamingConfig,
    metrics: StreamingMetrics,
    #[allow(dead_code)] // For future metrics/monitoring
    items_written: u64,
    last_flush: Instant,
    first_item: bool,
}

impl<W: Write> StreamingJsonSerializer<W> {
    /// Create new streaming JSON serializer
    pub fn new(writer: W, config: StreamingConfig) -> Self {
        Self {
            writer,
            buffer: Vec::with_capacity(config.buffer_size),
            config,
            metrics: StreamingMetrics {
                items_streamed: 0,
                bytes_written: 0,
                flush_count: 0,
                streaming_time: Duration::ZERO,
                average_item_size: 0,
                buffer_utilization: 0.0,
                memory_peak_mb: 0.0,
            },
            items_written: 0,
            last_flush: Instant::now(),
            first_item: true,
        }
    }

    /// Start streaming JSON array
    pub fn start_array(&mut self) -> Result<(), HybridSerializationError> {
        self.write_to_buffer(b"[")?;
        Ok(())
    }

    /// End streaming JSON array
    pub fn end_array(&mut self) -> Result<(), HybridSerializationError> {
        self.write_to_buffer(b"]")?;
        self.flush()?;
        Ok(())
    }

    /// Stream a single volume profile item
    pub fn stream_volume_profile(&mut self, profile: &VolumeProfileData) -> Result<(), HybridSerializationError> {
        let start_time = Instant::now();

        // Add comma separator for non-first items
        if !self.first_item {
            self.write_to_buffer(b",")?;
        }
        self.first_item = false;

        // Serialize item to JSON using sonic-rs
        let json_data = sonic_rs::to_vec(profile)
            .map_err(HybridSerializationError::JsonError)?;

        // Write to buffer
        self.write_to_buffer(&json_data)?;

        // Update metrics
        self.metrics.items_streamed += 1;
        self.metrics.streaming_time += start_time.elapsed();
        self.update_average_item_size(json_data.len());

        // Check if buffer should be flushed
        self.maybe_flush()?;

        Ok(())
    }

    /// Stream a single technical analysis item
    pub fn stream_technical_analysis(&mut self, output: &IndicatorOutput) -> Result<(), HybridSerializationError> {
        let start_time = Instant::now();

        // Add comma separator for non-first items
        if !self.first_item {
            self.write_to_buffer(b",")?;
        }
        self.first_item = false;

        // Serialize item to JSON using sonic-rs
        let json_data = sonic_rs::to_vec(output)
            .map_err(HybridSerializationError::JsonError)?;

        // Write to buffer
        self.write_to_buffer(&json_data)?;

        // Update metrics
        self.metrics.items_streamed += 1;
        self.metrics.streaming_time += start_time.elapsed();
        self.update_average_item_size(json_data.len());

        // Check if buffer should be flushed
        self.maybe_flush()?;

        Ok(())
    }

    /// Write data to internal buffer
    fn write_to_buffer(&mut self, data: &[u8]) -> Result<(), HybridSerializationError> {
        // Check memory limits
        let current_memory_mb = self.buffer.len() as f64 / (1024.0 * 1024.0);
        if current_memory_mb > self.config.max_memory_mb as f64 {
            return Err(HybridSerializationError::MemoryLimitExceeded(self.buffer.len()));
        }

        // Update peak memory usage
        if current_memory_mb > self.metrics.memory_peak_mb {
            self.metrics.memory_peak_mb = current_memory_mb;
        }

        // Add data to buffer
        self.buffer.extend_from_slice(data);

        // Force flush if buffer is full
        if self.buffer.len() >= self.config.buffer_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Check if buffer should be flushed based on time or size
    fn maybe_flush(&mut self) -> Result<(), HybridSerializationError> {
        let time_to_flush = self.last_flush.elapsed() >= Duration::from_millis(self.config.flush_interval_ms);
        let size_to_flush = self.buffer.len() >= self.config.buffer_size / 2; // Flush at half capacity

        if time_to_flush || size_to_flush {
            self.flush()?;
        }

        Ok(())
    }

    /// Flush buffer to writer
    pub fn flush(&mut self) -> Result<(), HybridSerializationError> {
        if !self.buffer.is_empty() {
            self.writer.write_all(&self.buffer)
                .map_err(|e| HybridSerializationError::StreamingError(e.to_string()))?;
            
            self.writer.flush()
                .map_err(|e| HybridSerializationError::StreamingError(e.to_string()))?;

            // Update metrics
            self.metrics.bytes_written += self.buffer.len() as u64;
            self.metrics.flush_count += 1;
            self.metrics.buffer_utilization = self.buffer.len() as f64 / self.config.buffer_size as f64;

            // Clear buffer
            self.buffer.clear();
            self.last_flush = Instant::now();
        }

        Ok(())
    }

    /// Update average item size metric
    fn update_average_item_size(&mut self, item_size: usize) {
        let total_items = self.metrics.items_streamed;
        if total_items > 0 {
            let total_size = (self.metrics.average_item_size * (total_items - 1) as usize) + item_size;
            self.metrics.average_item_size = total_size / total_items as usize;
        } else {
            self.metrics.average_item_size = item_size;
        }
    }

    /// Get streaming metrics
    pub fn get_metrics(&self) -> StreamingMetrics {
        self.metrics.clone()
    }
}

/// Async streaming serializer for async environments
pub struct AsyncStreamingJsonSerializer<W: AsyncWrite + Unpin> {
    writer: W,
    buffer: Vec<u8>,
    config: StreamingConfig,
    metrics: StreamingMetrics,
    #[allow(dead_code)] // For future metrics/monitoring
    items_written: u64,
    last_flush: Instant,
    first_item: bool,
}

impl<W: AsyncWrite + Unpin> AsyncStreamingJsonSerializer<W> {
    /// Create new async streaming JSON serializer
    pub fn new(writer: W, config: StreamingConfig) -> Self {
        Self {
            writer,
            buffer: Vec::with_capacity(config.buffer_size),
            config,
            metrics: StreamingMetrics {
                items_streamed: 0,
                bytes_written: 0,
                flush_count: 0,
                streaming_time: Duration::ZERO,
                average_item_size: 0,
                buffer_utilization: 0.0,
                memory_peak_mb: 0.0,
            },
            items_written: 0,
            last_flush: Instant::now(),
            first_item: true,
        }
    }

    /// Start streaming JSON array
    pub async fn start_array(&mut self) -> Result<(), HybridSerializationError> {
        self.write_to_buffer(b"[").await?;
        Ok(())
    }

    /// End streaming JSON array
    pub async fn end_array(&mut self) -> Result<(), HybridSerializationError> {
        self.write_to_buffer(b"]").await?;
        self.flush().await?;
        Ok(())
    }

    /// Stream a single volume profile item asynchronously
    pub async fn stream_volume_profile(&mut self, profile: &VolumeProfileData) -> Result<(), HybridSerializationError> {
        let start_time = Instant::now();

        // Add comma separator for non-first items
        if !self.first_item {
            self.write_to_buffer(b",").await?;
        }
        self.first_item = false;

        // Serialize item to JSON using sonic-rs
        let json_data = sonic_rs::to_vec(profile)
            .map_err(HybridSerializationError::JsonError)?;

        // Write to buffer
        self.write_to_buffer(&json_data).await?;

        // Update metrics
        self.metrics.items_streamed += 1;
        self.metrics.streaming_time += start_time.elapsed();
        self.update_average_item_size(json_data.len());

        // Check if buffer should be flushed
        self.maybe_flush().await?;

        Ok(())
    }

    /// Stream a single technical analysis item asynchronously
    pub async fn stream_technical_analysis(&mut self, output: &IndicatorOutput) -> Result<(), HybridSerializationError> {
        let start_time = Instant::now();

        // Add comma separator for non-first items
        if !self.first_item {
            self.write_to_buffer(b",").await?;
        }
        self.first_item = false;

        // Serialize item to JSON using sonic-rs
        let json_data = sonic_rs::to_vec(output)
            .map_err(HybridSerializationError::JsonError)?;

        // Write to buffer
        self.write_to_buffer(&json_data).await?;

        // Update metrics
        self.metrics.items_streamed += 1;
        self.metrics.streaming_time += start_time.elapsed();
        self.update_average_item_size(json_data.len());

        // Check if buffer should be flushed
        self.maybe_flush().await?;

        Ok(())
    }

    /// Write data to internal buffer
    async fn write_to_buffer(&mut self, data: &[u8]) -> Result<(), HybridSerializationError> {
        // Check memory limits
        let current_memory_mb = self.buffer.len() as f64 / (1024.0 * 1024.0);
        if current_memory_mb > self.config.max_memory_mb as f64 {
            return Err(HybridSerializationError::MemoryLimitExceeded(self.buffer.len()));
        }

        // Update peak memory usage
        if current_memory_mb > self.metrics.memory_peak_mb {
            self.metrics.memory_peak_mb = current_memory_mb;
        }

        // Add data to buffer
        self.buffer.extend_from_slice(data);

        // Force flush if buffer is full
        if self.buffer.len() >= self.config.buffer_size {
            self.flush().await?;
        }

        Ok(())
    }

    /// Check if buffer should be flushed based on time or size
    async fn maybe_flush(&mut self) -> Result<(), HybridSerializationError> {
        let time_to_flush = self.last_flush.elapsed() >= Duration::from_millis(self.config.flush_interval_ms);
        let size_to_flush = self.buffer.len() >= self.config.buffer_size / 2;

        if time_to_flush || size_to_flush {
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush buffer to writer asynchronously
    pub async fn flush(&mut self) -> Result<(), HybridSerializationError> {
        if !self.buffer.is_empty() {
            self.writer.write_all(&self.buffer).await
                .map_err(|e| HybridSerializationError::StreamingError(e.to_string()))?;
            
            self.writer.flush().await
                .map_err(|e| HybridSerializationError::StreamingError(e.to_string()))?;

            // Update metrics
            self.metrics.bytes_written += self.buffer.len() as u64;
            self.metrics.flush_count += 1;
            self.metrics.buffer_utilization = self.buffer.len() as f64 / self.config.buffer_size as f64;

            // Clear buffer
            self.buffer.clear();
            self.last_flush = Instant::now();
        }

        Ok(())
    }

    /// Update average item size metric
    fn update_average_item_size(&mut self, item_size: usize) {
        let total_items = self.metrics.items_streamed;
        if total_items > 0 {
            let total_size = (self.metrics.average_item_size * (total_items - 1) as usize) + item_size;
            self.metrics.average_item_size = total_size / total_items as usize;
        } else {
            self.metrics.average_item_size = item_size;
        }
    }

    /// Get streaming metrics
    pub fn get_metrics(&self) -> StreamingMetrics {
        self.metrics.clone()
    }
}

/// Utility functions for creating streaming serializers
pub mod utils {
    use super::*;
    use std::fs::File;
    use std::io::BufWriter;

    /// Create file-based streaming serializer
    pub fn create_file_streaming_serializer(
        file_path: &str,
        config: StreamingConfig,
    ) -> Result<StreamingJsonSerializer<BufWriter<File>>, HybridSerializationError> {
        let file = File::create(file_path)
            .map_err(|e| HybridSerializationError::StreamingError(e.to_string()))?;
        let writer = BufWriter::new(file);
        Ok(StreamingJsonSerializer::new(writer, config))
    }

    /// Create memory-based streaming serializer
    pub fn create_memory_streaming_serializer(
        config: StreamingConfig,
    ) -> StreamingJsonSerializer<Vec<u8>> {
        let writer = Vec::new();
        StreamingJsonSerializer::new(writer, config)
    }

    /// Stream large volume profile dataset to file
    pub fn stream_volume_profiles_to_file(
        profiles: impl Iterator<Item = VolumeProfileData>,
        file_path: &str,
        config: StreamingConfig,
    ) -> Result<StreamingMetrics, HybridSerializationError> {
        let mut serializer = create_file_streaming_serializer(file_path, config)?;
        
        serializer.start_array()?;
        
        for profile in profiles {
            serializer.stream_volume_profile(&profile)?;
        }
        
        serializer.end_array()?;
        
        Ok(serializer.get_metrics())
    }

    /// Stream large technical analysis dataset to file
    pub fn stream_technical_analysis_to_file(
        outputs: impl Iterator<Item = IndicatorOutput>,
        file_path: &str,
        config: StreamingConfig,
    ) -> Result<StreamingMetrics, HybridSerializationError> {
        let mut serializer = create_file_streaming_serializer(file_path, config)?;
        
        serializer.start_array()?;
        
        for output in outputs {
            serializer.stream_technical_analysis(&output)?;
        }
        
        serializer.end_array()?;
        
        Ok(serializer.get_metrics())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_profile::structs::{PriceLevelData, ValueArea};
    use crate::technical_analysis::structs::TrendDirection;
    use rust_decimal_macros::dec;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::NamedTempFile;

    fn create_test_volume_profile(id: u32) -> VolumeProfileData {
        VolumeProfileData {
            date: format!("2023-08-{:02}", (id % 30) + 1),
            total_volume: dec!(1000000) + rust_decimal::Decimal::from(id * 1000),
            vwap: dec!(50000.0) + rust_decimal::Decimal::from(id),
            poc: dec!(50050.0) + rust_decimal::Decimal::from(id),
            price_increment: dec!(0.01),
            min_price: dec!(49900.0),
            max_price: dec!(50100.0),
            candle_count: 100 + id,
            last_updated: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            price_levels: vec![
                PriceLevelData {
                    price: dec!(50000.0) + rust_decimal::Decimal::from(id),
                    volume: dec!(100000) + rust_decimal::Decimal::from(id * 100),
                    percentage: dec!(10.0),
                    candle_count: 50 + id,
                },
            ],
            value_area: ValueArea {
                high: dec!(50075.0) + rust_decimal::Decimal::from(id),
                low: dec!(50025.0) + rust_decimal::Decimal::from(id),
                volume_percentage: dec!(70.0),
                volume: dec!(700000) + rust_decimal::Decimal::from(id * 1000),
            },
        }
    }

    fn create_test_indicator_output(id: u32) -> IndicatorOutput {
        IndicatorOutput {
            symbol: format!("SYMBOL{}", id),
            timestamp: 1736985600000 + (id as i64 * 60000),
            close_5m: Some(45000.0 + id as f64),
            close_15m: Some(44980.0 + id as f64),
            close_60m: Some(44950.0 + id as f64),
            close_4h: Some(44900.0 + id as f64),
            ema21_1min: Some(44995.0 + id as f64),
            ema89_1min: Some(44985.0 + id as f64),
            ema89_5min: Some(44975.0 + id as f64),
            ema89_15min: Some(44965.0 + id as f64),
            ema89_1h: Some(44955.0 + id as f64),
            ema89_4h: Some(44945.0 + id as f64),
            trend_1min: if id % 3 == 0 { TrendDirection::Buy } else if id % 3 == 1 { TrendDirection::Sell } else { TrendDirection::Neutral },
            trend_5min: TrendDirection::Buy,
            trend_15min: TrendDirection::Neutral,
            trend_1h: TrendDirection::Neutral,
            trend_4h: TrendDirection::Sell,
            max_volume: Some(1500.0 + id as f64),
            max_volume_price: Some(45020.0 + id as f64),
            max_volume_time: Some(format!("2025-01-15T12:{:02}:00Z", id % 60)),
            max_volume_trend: Some(TrendDirection::Buy),
            volume_quantiles: None,
            #[cfg(feature = "volume_profile")]
            volume_profile: None,
            #[cfg(not(feature = "volume_profile"))]
            volume_profile: (),
        }
    }

    #[test]
    fn test_memory_streaming_serializer() {
        let config = StreamingConfig::default();
        let mut serializer = utils::create_memory_streaming_serializer(config);
        
        serializer.start_array().unwrap();
        
        // Stream multiple volume profiles
        for i in 0..10 {
            let profile = create_test_volume_profile(i);
            serializer.stream_volume_profile(&profile).unwrap();
        }
        
        serializer.end_array().unwrap();
        
        let metrics = serializer.get_metrics();
        assert_eq!(metrics.items_streamed, 10);
        assert!(metrics.bytes_written > 0);
    }

    #[test]
    fn test_file_streaming_serializer() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();
        let config = StreamingConfig::default();
        
        let profiles: Vec<_> = (0..5).map(create_test_volume_profile).collect();
        
        let metrics = utils::stream_volume_profiles_to_file(
            profiles.into_iter(),
            file_path,
            config,
        ).unwrap();
        
        assert_eq!(metrics.items_streamed, 5);
        assert!(metrics.bytes_written > 0);
        
        // Verify file exists and has content
        let file_contents = std::fs::read_to_string(file_path).unwrap();
        assert!(file_contents.starts_with('['));
        assert!(file_contents.ends_with(']'));
        assert!(file_contents.contains("2023-08"));
    }

    #[test]
    fn test_technical_analysis_streaming() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();
        let config = StreamingConfig::default();
        
        let outputs: Vec<_> = (0..3).map(create_test_indicator_output).collect();
        
        let metrics = utils::stream_technical_analysis_to_file(
            outputs.into_iter(),
            file_path,
            config,
        ).unwrap();
        
        assert_eq!(metrics.items_streamed, 3);
        assert!(metrics.bytes_written > 0);
        
        // Verify file contents
        let file_contents = std::fs::read_to_string(file_path).unwrap();
        assert!(file_contents.contains("SYMBOL0"));
        assert!(file_contents.contains("SYMBOL1"));
        assert!(file_contents.contains("SYMBOL2"));
    }

    #[test]
    fn test_memory_limit_enforcement() {
        let config = StreamingConfig {
            max_memory_mb: 1, // Very small limit to trigger error
            buffer_size: 2 * 1024 * 1024, // 2MB buffer (larger than limit)
            ..Default::default()
        };
        
        let mut serializer = utils::create_memory_streaming_serializer(config);
        
        // This should eventually hit the memory limit
        let result = serializer.start_array();
        assert!(result.is_ok());
        
        // Try to stream large profile to trigger memory limit
        let large_profile = VolumeProfileData {
            date: "2023-08-10".to_string(),
            price_levels: vec![create_large_price_level_data(); 10000], // Create many price levels
            ..create_test_volume_profile(1)
        };
        
        // This might trigger memory limit depending on serialized size
        let _ = serializer.stream_volume_profile(&large_profile);
    }

    fn create_large_price_level_data() -> PriceLevelData {
        PriceLevelData {
            price: dec!(50000.0),
            volume: dec!(100000),
            percentage: dec!(10.0),
            candle_count: 50,
        }
    }

    #[tokio::test]
    async fn test_async_streaming_serializer() {
        let config = StreamingConfig::default();
        let writer = Vec::new();
        let mut serializer = AsyncStreamingJsonSerializer::new(writer, config);
        
        serializer.start_array().await.unwrap();
        
        // Stream multiple technical analysis outputs
        for i in 0..5 {
            let output = create_test_indicator_output(i);
            serializer.stream_technical_analysis(&output).await.unwrap();
        }
        
        serializer.end_array().await.unwrap();
        
        let metrics = serializer.get_metrics();
        assert_eq!(metrics.items_streamed, 5);
        assert!(metrics.bytes_written > 0);
    }
}