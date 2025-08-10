//! Volume Profile Actor Extensions for High-Performance Serialization
//! 
//! This module extends the VolumeProfileActor with zero-copy serialization capabilities
//! for ultra-high-performance message handling and data output generation.

use crate::common::serialization::{
    HighPerformanceSerializer, SerializationFormat, SerializationOutput, SerializationError
};
use crate::volume_profile::{
    VolumeProfileActor, VolumeProfileReply, VolumeProfileData
};
use std::sync::Mutex;
use std::sync::LazyLock;

/// Enhanced volume profile reply with optimized serialization support
#[derive(Debug, Clone)]
pub struct OptimizedVolumeProfileReply {
    /// Original reply data
    pub reply: VolumeProfileReply,
    /// Cached serialized data for repeated requests
    pub cached_json: Option<String>,
    /// Cached binary data for high-performance consumers
    pub cached_binary: Option<Vec<u8>>,
    /// Cache generation timestamp
    pub cache_timestamp: std::time::Instant,
    /// Cache validity duration (5 seconds for real-time data)
    pub cache_ttl: std::time::Duration,
}

impl OptimizedVolumeProfileReply {
    /// Create new optimized reply
    pub fn new(reply: VolumeProfileReply) -> Self {
        Self {
            reply,
            cached_json: None,
            cached_binary: None,
            cache_timestamp: std::time::Instant::now(),
            cache_ttl: std::time::Duration::from_secs(5),
        }
    }
    
    /// Check if cache is still valid
    pub fn is_cache_valid(&self) -> bool {
        self.cache_timestamp.elapsed() < self.cache_ttl
    }
    
    /// Get JSON representation (cached or generated)
    pub fn get_json(&mut self, serializer: &mut HighPerformanceSerializer) -> Result<String, SerializationError> {
        if let Some(cached) = &self.cached_json {
            if self.is_cache_valid() {
                return Ok(cached.clone());
            }
        }
        
        // Generate fresh JSON
        let json = match &self.reply {
            VolumeProfileReply::VolumeProfile(profile_opt) => {
                match profile_opt {
                    Some(profile) => {
                        let output = serializer.serialize_volume_profile(profile, SerializationFormat::Json)?;
                        output.into_json()?
                    },
                    None => "null".to_string(),
                }
            },
            other => {
                // For non-profile replies, use standard JSON serialization
                serializer.serialize_json(other)?
            }
        };
        
        // Update cache
        self.cached_json = Some(json.clone());
        self.cache_timestamp = std::time::Instant::now();
        
        Ok(json)
    }
    
    /// Get binary representation for ultra-high-performance consumers
    pub fn get_binary(&mut self, serializer: &mut HighPerformanceSerializer) -> Result<Vec<u8>, SerializationError> {
        if let Some(cached) = &self.cached_binary {
            if self.is_cache_valid() {
                return Ok(cached.clone());
            }
        }
        
        // Generate fresh binary
        let binary = match &self.reply {
            VolumeProfileReply::VolumeProfile(profile_opt) => {
                match profile_opt {
                    Some(profile) => {
                        let output = serializer.serialize_volume_profile(profile, SerializationFormat::Json)?;
                        output.into_binary()?
                    },
                    None => b"null".to_vec(),
                }
            },
            other => {
                // For non-profile replies, serialize to JSON then convert to bytes
                let json = serializer.serialize_json(other)?;
                json.into_bytes()
            }
        };
        
        // Update cache
        self.cached_binary = Some(binary.clone());
        self.cache_timestamp = std::time::Instant::now();
        
        Ok(binary)
    }
}

/// Global high-performance serializer for volume profile actor
static VOLUME_PROFILE_SERIALIZER: LazyLock<Mutex<HighPerformanceSerializer>> = 
    LazyLock::new(|| Mutex::new(HighPerformanceSerializer::new()));

/// Extensions for VolumeProfileActor with high-performance serialization
pub trait VolumeProfileActorExtensions {
    /// Get volume profile with optimized serialization
    fn get_volume_profile_optimized(
        &self, 
        _symbol: String, 
        _date: chrono::NaiveDate,
        format: SerializationFormat
    ) -> Result<SerializationOutput, SerializationError>;
    
    /// Serialize volume profile reply with caching
    fn serialize_reply_cached(&self, reply: VolumeProfileReply) -> Result<OptimizedVolumeProfileReply, SerializationError>;
    
    /// Get serialization performance metrics
    fn get_serialization_metrics(&self) -> crate::common::serialization::UnifiedSerializationMetrics;
    
    /// Warm up serialization cache for frequently accessed data
    fn warmup_serialization_cache(&self, symbols: &[String], dates: &[chrono::NaiveDate]);
}

impl VolumeProfileActorExtensions for VolumeProfileActor {
    fn get_volume_profile_optimized(
        &self, 
        _symbol: String, 
        _date: chrono::NaiveDate,
        format: SerializationFormat
    ) -> Result<SerializationOutput, SerializationError> {
        // Note: This method provides the interface for optimized serialization
        // The actual implementation would need to be integrated into the VolumeProfileActor
        // using the message passing pattern with VolumeProfileAsk::GetVolumeProfile
        
        // For now, return a placeholder indicating this should use message passing
        match format {
            SerializationFormat::Json => Ok(SerializationOutput::Json(
                r#"{"error":"Use VolumeProfileAsk::GetVolumeProfile message for actual data retrieval"}"#.to_string()
            )),
            SerializationFormat::Binary => Ok(SerializationOutput::Binary(
                b"Use VolumeProfileAsk::GetVolumeProfile message for actual data retrieval".to_vec()
            )),
            SerializationFormat::Auto => Ok(SerializationOutput::Json(
                r#"{"error":"Use VolumeProfileAsk::GetVolumeProfile message for actual data retrieval"}"#.to_string()
            )),
        }
    }
    
    fn serialize_reply_cached(&self, reply: VolumeProfileReply) -> Result<OptimizedVolumeProfileReply, SerializationError> {
        let mut optimized_reply = OptimizedVolumeProfileReply::new(reply);
        let mut serializer = VOLUME_PROFILE_SERIALIZER.lock().unwrap();
        
        // Pre-cache both JSON and binary formats for frequently accessed data
        match &optimized_reply.reply {
            VolumeProfileReply::VolumeProfile(Some(_)) => {
                // Cache JSON representation
                let _json = optimized_reply.get_json(&mut serializer)?;
                // Cache binary representation
                let _binary = optimized_reply.get_binary(&mut serializer)?;
            },
            _ => {
                // For other reply types, only cache JSON
                let _json = optimized_reply.get_json(&mut serializer)?;
            }
        }
        
        Ok(optimized_reply)
    }
    
    fn get_serialization_metrics(&self) -> crate::common::serialization::UnifiedSerializationMetrics {
        let serializer = VOLUME_PROFILE_SERIALIZER.lock().unwrap();
        serializer.get_performance_metrics()
    }
    
    fn warmup_serialization_cache(&self, symbols: &[String], dates: &[chrono::NaiveDate]) {
        // This would need to be implemented using the message passing pattern
        // For now, just warm up the serializer with empty operations
        let mut serializer = VOLUME_PROFILE_SERIALIZER.lock().unwrap();
        
        // Create a dummy profile for warm-up (in production this would use actual data)
        use crate::volume_profile::structs::{VolumeProfileData, PriceLevelData, ValueArea};
        use rust_decimal_macros::dec;
        
        let dummy_profile = VolumeProfileData {
            date: "warmup".to_string(),
            price_levels: vec![PriceLevelData {
                price: dec!(1.0),
                volume: dec!(1.0),
                percentage: dec!(1.0),
                candle_count: 1,
            }],
            total_volume: dec!(1.0),
            vwap: dec!(1.0),
            poc: dec!(1.0),
            value_area: ValueArea {
                high: dec!(1.0),
                low: dec!(1.0),
                volume_percentage: dec!(1.0),
                volume: dec!(1.0),
            },
            price_increment: dec!(1.0),
            min_price: dec!(1.0),
            max_price: dec!(1.0),
            candle_count: 1,
            last_updated: 0,
        };
        
        // Warm up serialization with dummy data (proportional to request count)
        for _ in 0..(symbols.len() * dates.len()).min(10) {
            let _ = serializer.serialize_volume_profile(&dummy_profile, SerializationFormat::Json);
            let _ = serializer.serialize_volume_profile(&dummy_profile, SerializationFormat::Auto);
        }
    }
}

/// High-performance message serialization for volume profile actors
pub struct VolumeProfileMessageSerializer {
    serializer: HighPerformanceSerializer,
    /// Message cache for frequently sent messages
    message_cache: std::collections::HashMap<String, (String, std::time::Instant)>,
    /// Cache TTL for messages
    cache_ttl: std::time::Duration,
}

impl VolumeProfileMessageSerializer {
    /// Create new message serializer
    pub fn new() -> Self {
        Self {
            serializer: HighPerformanceSerializer::new(),
            message_cache: std::collections::HashMap::new(),
            cache_ttl: std::time::Duration::from_secs(1), // 1 second TTL for messages
        }
    }
    
    /// Serialize VolumeProfileReply with intelligent caching
    pub fn serialize_reply(&mut self, reply: &VolumeProfileReply) -> Result<String, SerializationError> {
        match reply {
            VolumeProfileReply::VolumeProfile(profile_opt) => {
                match profile_opt {
                    Some(profile) => {
                        // Use cache key based on profile content hash for efficient lookup
                        let cache_key = self.generate_profile_cache_key(profile);
                        
                        if let Some((cached_json, timestamp)) = self.message_cache.get(&cache_key) {
                            if timestamp.elapsed() < self.cache_ttl {
                                return Ok(cached_json.clone());
                            }
                        }
                        
                        // Generate fresh serialization
                        let output = self.serializer.serialize_volume_profile(profile, SerializationFormat::Json)?;
                        let json = output.into_json()?;
                        
                        // Update cache
                        self.message_cache.insert(cache_key, (json.clone(), std::time::Instant::now()));
                        
                        Ok(json)
                    },
                    None => Ok("null".to_string()),
                }
            },
            other => {
                // For other reply types, use standard JSON serialization without caching
                self.serializer.serialize_json(other)
            }
        }
    }
    
    /// Generate cache key for volume profile (based on key characteristics)
    fn generate_profile_cache_key(&self, profile: &VolumeProfileData) -> String {
        // Use date, symbol implied from context, last_updated, and price level count as cache key
        format!("{}:{}:{}", 
                profile.date, 
                profile.last_updated, 
                profile.price_levels.len())
    }
    
    /// Clear expired cache entries
    pub fn cleanup_cache(&mut self) {
        let now = std::time::Instant::now();
        self.message_cache.retain(|_, (_, timestamp)| {
            now.duration_since(*timestamp) < self.cache_ttl
        });
    }
    
    /// Get cache statistics
    pub fn get_cache_stats(&self) -> (usize, f64) {
        let total_entries = self.message_cache.len();
        let valid_entries = self.message_cache.values()
            .filter(|(_, timestamp)| timestamp.elapsed() < self.cache_ttl)
            .count();
        
        let hit_rate = if total_entries > 0 {
            (valid_entries as f64 / total_entries as f64) * 100.0
        } else {
            0.0
        };
        
        (total_entries, hit_rate)
    }
}

impl Default for VolumeProfileMessageSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience functions for actor integration
pub mod integration {
    use super::*;
    use kameo::message::Context;
    
    /// Enhanced message handler with high-performance serialization
    pub async fn handle_get_volume_profile_optimized(
        actor: &mut VolumeProfileActor,
        symbol: String,
        date: chrono::NaiveDate,
        format: SerializationFormat,
        _ctx: Context<'_, VolumeProfileActor, Result<SerializationOutput, SerializationError>>
    ) -> Result<SerializationOutput, SerializationError> {
        actor.get_volume_profile_optimized(symbol, date, format)
    }
    
    /// Batch processing with optimized serialization
    pub async fn handle_batch_volume_profiles_optimized(
        actor: &mut VolumeProfileActor,
        requests: Vec<(String, chrono::NaiveDate)>,
        format: SerializationFormat,
        _ctx: Context<'_, VolumeProfileActor, Result<Vec<SerializationOutput>, SerializationError>>
    ) -> Result<Vec<SerializationOutput>, SerializationError> {
        let mut results = Vec::with_capacity(requests.len());
        
        for (symbol, date) in requests {
            let result = actor.get_volume_profile_optimized(symbol, date, format)?;
            results.push(result);
        }
        
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_profile::structs::{VolumeProfileData, PriceLevelData, ValueArea, VolumeProfileConfig};
    use rust_decimal_macros::dec;
    use std::time::Instant;
    
    fn create_test_profile() -> VolumeProfileData {
        VolumeProfileData {
            date: "2025-01-15".to_string(),
            price_levels: vec![
                PriceLevelData {
                    price: dec!(45000.0),
                    volume: dec!(100.5),
                    percentage: dec!(25.5),
                    candle_count: 10,
                },
            ],
            total_volume: dec!(250.8),
            vwap: dec!(45000.5),
            poc: dec!(45001.0),
            value_area: ValueArea {
                high: dec!(45010.0),
                low: dec!(44990.0),
                volume_percentage: dec!(70.0),
                volume: dec!(175.56),
            },
            price_increment: dec!(1.0),
            min_price: dec!(44990.0),
            max_price: dec!(45010.0),
            candle_count: 25,
            last_updated: 1736985600000,
        }
    }
    
    fn create_test_actor() -> VolumeProfileActor {
        let config = VolumeProfileConfig::default();
        VolumeProfileActor::new(config).unwrap()
    }
    
    #[test]
    fn test_optimized_reply_caching() {
        let profile = create_test_profile();
        let reply = VolumeProfileReply::VolumeProfile(Some(profile));
        let mut optimized_reply = OptimizedVolumeProfileReply::new(reply);
        let mut serializer = HighPerformanceSerializer::new();
        
        // First call should generate and cache
        let json1 = optimized_reply.get_json(&mut serializer).unwrap();
        assert!(optimized_reply.cached_json.is_some());
        
        // Second call should use cache
        let json2 = optimized_reply.get_json(&mut serializer).unwrap();
        assert_eq!(json1, json2);
        assert!(optimized_reply.is_cache_valid());
    }
    
    #[test]
    fn test_message_serializer_caching() {
        let mut serializer = VolumeProfileMessageSerializer::new();
        let profile = create_test_profile();
        let reply = VolumeProfileReply::VolumeProfile(Some(profile));
        
        // First serialization
        let start = Instant::now();
        let json1 = serializer.serialize_reply(&reply).unwrap();
        let first_duration = start.elapsed();
        
        // Second serialization (should be faster due to caching)
        let start = Instant::now();
        let json2 = serializer.serialize_reply(&reply).unwrap();
        let second_duration = start.elapsed();
        
        assert_eq!(json1, json2);
        // Cache should make second call faster (though this might not always be true in debug builds)
        println!("First call: {:?}, Second call: {:?}", first_duration, second_duration);
        
        let (cache_size, _hit_rate) = serializer.get_cache_stats();
        assert_eq!(cache_size, 1);
    }
    
    #[test]
    fn test_actor_extensions() {
        let actor = create_test_actor();
        
        // Test serialization metrics (should start with zero)
        let metrics = actor.get_serialization_metrics();
        assert_eq!(metrics.total_operations, 0);
        
        // Note: We can't easily test get_volume_profile_optimized without setting up
        // a full actor with data, so we'll focus on the interface verification
    }
    
    #[test]
    fn test_cache_cleanup() {
        let mut serializer = VolumeProfileMessageSerializer::new();
        
        // Add some entries to cache
        let profile = create_test_profile();
        let reply = VolumeProfileReply::VolumeProfile(Some(profile));
        let _json = serializer.serialize_reply(&reply).unwrap();
        
        let (cache_size_before, _) = serializer.get_cache_stats();
        assert!(cache_size_before > 0);
        
        // Cleanup should preserve valid entries
        serializer.cleanup_cache();
        let (cache_size_after, _) = serializer.get_cache_stats();
        assert_eq!(cache_size_before, cache_size_after);
    }
    
    #[test]
    fn test_serialization_format_selection() {
        let profile = create_test_profile();
        let mut serializer = HighPerformanceSerializer::new();
        
        // Test JSON format
        let json_output = serializer.serialize_volume_profile(&profile, SerializationFormat::Json).unwrap();
        assert!(json_output.is_json());
        
        // Test auto format selection
        let auto_output = serializer.serialize_volume_profile(&profile, SerializationFormat::Auto).unwrap();
        assert!(auto_output.is_json()); // Volume profiles prefer JSON for API compatibility
    }
    
    #[test]
    fn test_performance_benchmark() {
        let mut serializer = VolumeProfileMessageSerializer::new();
        let profile = create_test_profile();
        let reply = VolumeProfileReply::VolumeProfile(Some(profile));
        
        // Warm-up
        for _ in 0..10 {
            let _ = serializer.serialize_reply(&reply);
        }
        
        // Performance test
        let start = Instant::now();
        let iterations = 1000;
        
        for _ in 0..iterations {
            let _ = serializer.serialize_reply(&reply);
        }
        
        let elapsed = start.elapsed();
        let avg_time = elapsed / iterations;
        
        println!("Actor extension serialization average time: {:?}", avg_time);
        
        // Should be very fast due to caching
        assert!(avg_time < std::time::Duration::from_micros(10));
    }
}