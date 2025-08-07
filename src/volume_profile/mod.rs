/// Volume Profile Module
/// 
/// This module implements daily volume profile calculation from 1-minute candle data.
/// It provides real-time volume profile updates for every new candle and supports
/// full day reconstruction on startup.
pub mod actor;
pub mod calculator;
pub mod database;
pub mod precision;
pub mod structs;
pub mod validation;

// Reprocessing modules (feature-gated)
#[cfg(feature = "volume_profile_reprocessing")]
pub mod reprocessing;
#[cfg(feature = "volume_profile_reprocessing")]
pub mod gap_detection;

pub use actor::{VolumeProfileActor, VolumeProfileTell, VolumeProfileAsk, VolumeProfileReply};
pub use calculator::DailyVolumeProfile;
#[cfg(feature = "postgres")]
pub use database::VolumeProfileDatabase;
pub use structs::{
    VolumeProfileConfig, VolumeProfileData, PriceLevelData, ValueArea,
    PriceIncrementMode, UpdateFrequency, AssetConfig, ResolvedAssetConfig,
    VolumeDistributionMode, ValueAreaCalculationMode
};

// Reprocessing exports (feature-gated)
#[cfg(feature = "volume_profile_reprocessing")]
pub use reprocessing::{ReprocessingCoordinator, VolumeProfileReprocessingConfig, ReprocessingMode};
#[cfg(feature = "volume_profile_reprocessing")]
pub use gap_detection::{GapDetector, GapDetectionResult, DateRange};