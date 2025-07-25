use data_feeder::historical::actor::{HistoricalActor, HistoricalAsk};
use data_feeder::websocket::{
    WebSocketActor, WebSocketTell, WebSocketAsk, WebSocketReply, StreamType
};
use data_feeder::api::{ApiActor, ApiAsk, ApiReply, ApiTell};
use data_feeder::technical_analysis::{
    TechnicalAnalysisConfig, TimeFrameActor, IndicatorActor, 
    TimeFrameAsk, IndicatorAsk
};
use data_feeder::postgres::{PostgresActor, PostgresConfig};
use data_feeder::kafka::{KafkaActor, KafkaConfig};
use data_feeder::health::{start_health_server, HealthConfig, HealthDependencies};
use data_feeder::lmdb::{LmdbActor, LmdbActorMessage, LmdbActorResponse};
use kameo::actor::ActorRef;
use kameo::request::MessageSend;
use serde::Deserialize;
use std::path::PathBuf;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

/// Application configuration from config.toml
#[derive(Debug, Clone, Deserialize)]
struct ApplicationConfig {
    pub symbols: Vec<String>,
    pub timeframes: Vec<u64>,      // Support multiple timeframes (seconds)
    pub storage_path: String,
    pub gap_detection_enabled: bool,
    pub start_date: Option<String>, // Optional start date for historical data (RFC3339 format: "2022-01-01T00:00:00Z")
    pub respect_config_start_date: bool, // If true, use config start_date as absolute reference, ignore existing DB data before it
    pub monthly_threshold_months: u32, // Use monthly data for gaps larger than this many months (default: 3)
    pub enable_technical_analysis: bool, // Enable technical analysis actors
    // WebSocket reconnection gap detection settings
    pub reconnection_gap_threshold_minutes: u32, // Minimum gap in minutes to trigger filling (default: 2)
    pub reconnection_gap_check_delay_seconds: u32, // Delay after reconnection before checking gaps (default: 5)
    // Periodic gap detection settings
    pub periodic_gap_detection_enabled: bool, // Enable periodic gap detection during stable connections
    pub periodic_gap_check_interval_minutes: u32, // How often to check for gaps (default: 5)
    pub periodic_gap_check_window_minutes: u32, // How far back to scan for gaps (default: 30)
}

/// Technical analysis configuration from config.toml
#[derive(Debug, Clone, Deserialize)]
struct TechnicalAnalysisTomlConfig {
    pub min_history_days: u32,
    pub ema_periods: Vec<u32>,
    pub timeframes: Vec<u64>,
    pub volume_lookback_days: u32,
}

/// Full TOML configuration structure
#[derive(Debug, Clone, Deserialize)]
struct TomlConfig {
    pub database: PostgresConfig,
    pub application: ApplicationConfig,
    pub technical_analysis: TechnicalAnalysisTomlConfig,
    pub kafka: Option<KafkaConfig>,
}

/// Production configuration (converted from TOML)
#[derive(Debug, Clone)]
struct DataFeederConfig {
    pub symbols: Vec<String>,
    pub timeframes: Vec<u64>,      // Support multiple timeframes (seconds)
    pub storage_path: PathBuf,
    pub gap_detection_enabled: bool,
    pub start_date: Option<String>, // Optional start date for historical data (RFC3339 format: "2022-01-01T00:00:00Z")
    pub respect_config_start_date: bool, // If true, use config start_date as absolute reference, ignore existing DB data before it
    pub monthly_threshold_months: u32, // Use monthly data for gaps larger than this many months (default: 3)
    pub enable_technical_analysis: bool, // Enable technical analysis actors
    pub ta_config: TechnicalAnalysisConfig, // Technical analysis configuration
    // WebSocket reconnection gap detection settings
    pub reconnection_gap_threshold_minutes: u32, // Minimum gap in minutes to trigger filling (default: 2)
    pub reconnection_gap_check_delay_seconds: u32, // Delay after reconnection before checking gaps (default: 5)
    // Periodic gap detection settings
    pub periodic_gap_detection_enabled: bool, // Enable periodic gap detection during stable connections
    pub periodic_gap_check_interval_minutes: u32, // How often to check for gaps (default: 5)
    pub periodic_gap_check_window_minutes: u32, // How far back to scan for gaps (default: 30)
    // PostgreSQL configuration
    pub postgres_config: PostgresConfig,
    // Kafka configuration
    pub kafka_config: KafkaConfig,
    // Historical validation tracking
    pub historical_validation_complete: bool, // Tracks completion of Phase 1 (full historical validation)
    pub recent_monitoring_days: u32, // Days to monitor in Phase 2 (default: 60)
    pub min_gap_seconds: u64, // Minimum gap duration to detect (default: 90 seconds for 1-minute candles)
}

impl DataFeederConfig {
    /// Load configuration from config.toml file
    pub fn from_toml<P: AsRef<std::path::Path>>(path: P) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config_content = std::fs::read_to_string(path)?;
        let toml_config: TomlConfig = toml::from_str(&config_content)?;
        
        Ok(Self::from_toml_config(toml_config))
    }
    
    /// Convert TOML configuration to DataFeederConfig
    fn from_toml_config(toml_config: TomlConfig) -> Self {
        let mut symbols = toml_config.application.symbols.clone();
        if symbols.is_empty() {
            symbols = vec!["BTCUSDT".to_string()];
        }
        
        let ta_config = TechnicalAnalysisConfig {
            symbols: symbols.clone(), // Sync with main symbols
            timeframes: toml_config.technical_analysis.timeframes.clone(),
            ema_periods: toml_config.technical_analysis.ema_periods.clone(),
            volume_lookback_days: toml_config.technical_analysis.volume_lookback_days,
            min_history_days: toml_config.technical_analysis.min_history_days,
        };
        
        Self {
            symbols,
            timeframes: toml_config.application.timeframes,
            storage_path: PathBuf::from(toml_config.application.storage_path),
            gap_detection_enabled: toml_config.application.gap_detection_enabled,
            start_date: toml_config.application.start_date,
            respect_config_start_date: toml_config.application.respect_config_start_date,
            monthly_threshold_months: toml_config.application.monthly_threshold_months,
            enable_technical_analysis: toml_config.application.enable_technical_analysis,
            ta_config,
            reconnection_gap_threshold_minutes: toml_config.application.reconnection_gap_threshold_minutes,
            reconnection_gap_check_delay_seconds: toml_config.application.reconnection_gap_check_delay_seconds,
            periodic_gap_detection_enabled: toml_config.application.periodic_gap_detection_enabled,
            periodic_gap_check_interval_minutes: toml_config.application.periodic_gap_check_interval_minutes,
            periodic_gap_check_window_minutes: toml_config.application.periodic_gap_check_window_minutes,
            postgres_config: toml_config.database,
            kafka_config: toml_config.kafka.unwrap_or_default(),
            // Historical validation defaults - start with Phase 1 (full validation)
            historical_validation_complete: false, // Always start with full historical validation
            recent_monitoring_days: 60, // Monitor last 60 days in Phase 2
            min_gap_seconds: 75, // Minimum 75 seconds for 1-minute candles (60s + 15s buffer)
        }
    }
}

impl Default for DataFeederConfig {
    fn default() -> Self {
        Self {
            symbols: vec!["BTCUSDT".to_string()],
            timeframes: vec![60], // Default to 1 minute
            storage_path: PathBuf::from("lmdb_data"),
            gap_detection_enabled: true,
            start_date: None, // Start from existing data or real-time
            respect_config_start_date: false, // Default: use existing DB-based gap detection
            monthly_threshold_months: 2, // Use monthly data for gaps > 2 months
            enable_technical_analysis: true, // Enable TA by default
            ta_config: TechnicalAnalysisConfig {
                symbols: vec!["BTCUSDT".to_string()], // Will be synced with main symbols
                timeframes: vec![60, 300, 900, 3600, 14400], // Always include 1m + higher TFs
                ema_periods: vec![21, 89],
                volume_lookback_days: 60,
                min_history_days: 60,
            },
            reconnection_gap_threshold_minutes: 1, // Fill gaps > 1 minute
            reconnection_gap_check_delay_seconds: 5, // Wait 5s after reconnection
            periodic_gap_detection_enabled: true, // Enable periodic gap detection
            periodic_gap_check_interval_minutes: 5, // Check every 5 minutes
            periodic_gap_check_window_minutes: 30, // Scan last 30 minutes
            postgres_config: PostgresConfig::default(),
            kafka_config: KafkaConfig::default(),
            // Historical validation defaults - start with Phase 1 (full validation)
            historical_validation_complete: true, // Always start with full historical validation
            recent_monitoring_days: 60, // Monitor last 60 days in Phase 2
            min_gap_seconds: 60, // Minimum 75 seconds for 1-minute candles (60s + 15s buffer)
        }
    }
}

/* Configuration Examples:

// For comprehensive historical backfill (first run only):
DataFeederConfig {
    symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
    timeframes: vec![60], // 1m
    start_date: Some("2025-06-01T00:00:00Z".to_string()), // June 1, 2025
    ..Default::default()
}

// For fetching older data (ignoring existing DB):
DataFeederConfig {
    symbols: vec!["BTCUSDT".to_string()],
    timeframes: vec![60], // 1m only
    start_date: Some("2021-01-01T00:00:00Z".to_string()), // Start from 2021
    respect_config_start_date: true, // Use config date as absolute reference
    monthly_threshold_months: 3, // Use monthly data for gaps > 3 months
    ..Default::default()
}

// For recent data only:
DataFeederConfig {
    symbols: vec!["BTCUSDT".to_string()],
    timeframes: vec![60], // 1m only
    start_date: Some("2024-07-14T00:00:00Z".to_string()), // 1 week ago
    ..Default::default()
}

// For real-time only (no historical backfill):
DataFeederConfig {
    start_date: None, // Will start from real-time
    gap_detection_enabled: false, // Skip gap detection
    ..Default::default()
}
*/

/// Parse human-readable datetime string to timestamp in milliseconds
fn parse_start_date(date_str: &str) -> Result<i64, String> {
    chrono::DateTime::parse_from_rfc3339(date_str)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| format!("Invalid date format '{}': {}. Use RFC3339 format like '2022-01-01T00:00:00Z'", date_str, e))
}

/// Simplified gap detection using LmdbActor
/// Delegates gap detection to LmdbActor and assigns gaps to optimal actors
async fn detect_and_fill_gaps(
    symbol: &str, 
    timeframe_seconds: u64,
    start_time: i64, 
    end_time: i64,
    config: &DataFeederConfig,
    lmdb_actor: &ActorRef<LmdbActor>,
    historical_actor: &ActorRef<HistoricalActor>,
    api_actor: &ActorRef<ApiActor>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    let interval = match timeframe_seconds {
        60 => "1m",
        300 => "5m", 
        3600 => "1h",
        86400 => "1d",
        _ => return Err(format!("Unsupported timeframe: {}s", timeframe_seconds).into()),
    };
    
    info!("🔍 Detecting gaps for {} {} from {} to {}", symbol, interval, start_time, end_time);
    
    // Phase detection logging
    if !config.historical_validation_complete {
        info!("🔄 Phase 1: Full historical validation (first-time setup)");
    } else {
        info!("✅ Phase 2: Recent monitoring (historical validation complete)");
        info!("📊 Checking last {} days for gaps", config.recent_monitoring_days);
    }
    
    // Reference date strategy logging  
    if config.respect_config_start_date {
        info!("🎯 Using config start_date as absolute reference (ignoring existing DB data before it)");
    } else {
        info!("📊 Using database-based gap detection (preserving existing data)");
    }
    
    // Step 1: Use LmdbActor for comprehensive gap detection
    let gaps = match lmdb_actor.ask(LmdbActorMessage::ValidateDataRange {
        symbol: symbol.to_string(),
        timeframe: timeframe_seconds,
        start: start_time,
        end: end_time,
    }).send().await {
        Ok(LmdbActorResponse::ValidationResult { gaps }) => {
            info!("📊 LmdbActor detected {} gaps for {} {}", gaps.len(), symbol, interval);
            gaps
        }
        Ok(LmdbActorResponse::ErrorResponse(e)) => {
            return Err(format!("LmdbActor validation failed: {}", e).into());
        }
        Ok(_) => return Err("Unexpected response from LmdbActor".into()),
        Err(e) => return Err(format!("Failed to communicate with LmdbActor: {}", e).into()),
    };
    
    if gaps.is_empty() {
        info!("✅ No gaps detected - data is complete");
        return Ok(());
    }
    
    // Step 2: Enhanced gap analysis and reporting
    let total_missing_time: i64 = gaps.iter().map(|(start, end)| end - start).sum();
    let total_missing_candles = total_missing_time / (60 * 1000);
    let total_missing_hours = total_missing_time / (1000 * 3600);
    let total_missing_days = total_missing_hours / 24;
    let expected_total_time = end_time - start_time;
    let missing_percentage = (total_missing_time as f64 / expected_total_time as f64) * 100.0;
    
    warn!("📈 Gap Analysis Summary for {}: {} gaps found", symbol, gaps.len());
    warn!("⚠️  Total missing time: {} hours ({} days)", total_missing_hours, total_missing_days);
    warn!("📉 Total missing candles: {} (should be 1440 per day)", total_missing_candles);
    warn!("💯 Missing data percentage: {:.1}%", missing_percentage);
    
    // Step 3: Smart actor delegation for each gap
    let now = chrono::Utc::now().timestamp_millis();
    
    for &(gap_start, gap_end) in &gaps {
        let gap_duration_hours = (gap_end - gap_start) / (1000 * 3600);
        let gap_duration_days = gap_duration_hours / 24;
        let gap_duration_months = gap_duration_days / 30;
        let gap_age_hours = (now - gap_end) / (1000 * 3600);
        
        // Convert timestamps to human-readable format
        let gap_start_str = chrono::DateTime::from_timestamp_millis(gap_start)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| format!("INVALID_TIME({})", gap_start));
        let gap_end_str = chrono::DateTime::from_timestamp_millis(gap_end)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| format!("INVALID_TIME({})", gap_end));
        
        let expected_candles = (gap_end - gap_start) / (60 * 1000);
        
        warn!("🕳️ Gap detected for {}: {} to {} ({} hours, {} days, ~{} months)", 
              symbol, gap_start_str, gap_end_str, gap_duration_hours, gap_duration_days, gap_duration_months);
        info!("📊 Gap details: {} expected candles, gap age: {} hours", expected_candles, gap_age_hours);
        
        // Enhanced classification: Monthly vs Daily vs Intraday
        let use_monthly_data = gap_duration_months >= config.monthly_threshold_months as i64;
        
        if gap_duration_hours > 24 {
            // Large gap → HistoricalActor (S3 bulk download)
            if use_monthly_data {
                info!("🏛️📅 Assigning large historical gap to HistoricalActor (S3 MONTHLY bulk) - {}+ months", config.monthly_threshold_months);
            } else {
                info!("🏛️📊 Assigning historical gap to HistoricalActor (S3 daily bulk)");
            }
            
            match historical_actor.ask(HistoricalAsk::GetCandles {
                symbol: symbol.to_string(),
                timeframe: timeframe_seconds,
                start_time: gap_start,
                end_time: gap_end,
            }).await {
                Ok(_) => {
                    if use_monthly_data {
                        info!("✅ HistoricalActor completed monthly gap filling");
                    } else {
                        info!("✅ HistoricalActor completed daily gap filling");
                    }
                },
                Err(e) => warn!("⚠️ HistoricalActor failed: {}. Falling back to ApiActor", e),
            }
        } else {
            // Small gap (≤24h) → ApiActor (targeted API calls)
            info!("⚡ Assigning small gap to ApiActor (targeted API)");
            if let Err(e) = api_actor.tell(ApiTell::FillGap {
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                start_time: gap_start,
                end_time: gap_end,
            }).send().await {
                warn!("⚠️ Failed to initiate API gap fill: {}", e);
            } else {
                info!("✅ ApiActor initiated gap filling");
            }
        }
    }
    
    // Step 4: Wait for completion and verify using LmdbActor
    info!("⏳ Waiting for gap filling to complete...");
    sleep(Duration::from_secs(10)).await;
    
    // Re-validate using LmdbActor to check if gaps were filled
    match lmdb_actor.ask(LmdbActorMessage::ValidateDataRange {
        symbol: symbol.to_string(),
        timeframe: timeframe_seconds,
        start: start_time,
        end: end_time,
    }).send().await {
        Ok(LmdbActorResponse::ValidationResult { gaps: remaining_gaps }) => {
            if remaining_gaps.is_empty() {
                info!("✅ Complete data coverage achieved!");
            } else {
                warn!("⚠️ {} gaps still exist after filling", remaining_gaps.len());
            }
        }
        _ => warn!("❌ Failed to verify data completeness"),
    }
    
    Ok(())
}

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info,data_feeder=info")
        .init();

    info!("🚀 Starting Data Feeder - Production Mode");
    
    // Try to load configuration from config.toml, fallback to default if not found
    let mut config = match DataFeederConfig::from_toml("config.toml") {
        Ok(config) => {
            info!("✅ Loaded configuration from config.toml");
            config
        }
        Err(e) => {
            warn!("⚠️ Failed to load config.toml: {}. Using default configuration", e);
            DataFeederConfig {
                symbols: vec!["BTCUSDT".to_string()],
                timeframes: vec![60], // 1m
                start_date: Some("2025-01-01T00:00:00Z".to_string()), // January 1, 2025
                ..Default::default()
            }
        }
    };
    
    // Ensure TA config symbols match main config symbols
    config.ta_config.symbols = config.symbols.clone();
    info!("⚙️ Configuration: {} symbols, {} timeframes: {:?}", 
          config.symbols.len(), config.timeframes.len(), config.timeframes);
    
    if config.postgres_config.enabled {
        info!("🐘 PostgreSQL storage enabled - {}:{}", 
              config.postgres_config.host, config.postgres_config.port);
    } else {
        info!("🚫 PostgreSQL storage disabled");
    }
    
    // Start the production data pipeline
    if let Err(e) = run_data_pipeline(config).await {
        error!("💥 Data pipeline failed: {}", e);
        std::process::exit(1);
    }
}

/// Main production data pipeline
async fn run_data_pipeline(config: DataFeederConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("🔧 Initializing data pipeline...");
    
    // Step 1: Launch basic actors (no TA actors yet)
    let (lmdb_actor, historical_actor, api_actor, ws_actor, postgres_actor, kafka_actor) = launch_basic_actors(&config).await?;
    
    // Step 2: Start real-time data streaming FIRST
    info!("📡 Starting real-time data streaming...");
    start_realtime_streaming(&config, &ws_actor).await?;
    
    // Step 2.5: WebSocket is running, but TA actors will be added after gap filling
    
    // Step 3: Single gap detection with smart actor delegation - SYNCHRONIZED TIMESTAMP
    let synchronized_timestamp = chrono::Utc::now().timestamp_millis();
    info!("🕐 SYNCHRONIZED TIMESTAMP for gap detection and TA: {}", synchronized_timestamp);
    
    if config.gap_detection_enabled {
        info!("🔍 Starting unified gap detection and filling...");
        for symbol in &config.symbols {
            for &timeframe_seconds in &config.timeframes {
                // Use synchronized timestamp for consistent time ranges
                let now = synchronized_timestamp;
                // Align with TA requirements: use TA's min_history_days for proper initialization
                let ta_history_days = config.ta_config.min_history_days as i64;
                
                let start_time = if let Some(ref date_str) = config.start_date {
                    parse_start_date(date_str).unwrap_or_else(|e| {
                        warn!("❌ Failed to parse start_date: {}. Using {}d ago for TA alignment", e, ta_history_days);
                        now - (ta_history_days * 24 * 3600 * 1000)
                    })
                } else {
                    // Use TA's min_history_days to ensure sufficient data for technical analysis
                    now - (ta_history_days * 24 * 3600 * 1000)
                };
                
                info!("📊 Gap detection range: {} days back (aligned with TA requirements)", ta_history_days);
                info!("🕐 Gap detection time range: {} to {} (start to synchronized_now)", start_time, now);
                
                // Add human-readable timestamps for debugging
                let start_str = chrono::DateTime::from_timestamp_millis(start_time)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| format!("INVALID_TIME({})", start_time));
                let now_str = chrono::DateTime::from_timestamp_millis(now)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| format!("INVALID_TIME({})", now));
                info!("🕐 Human-readable gap detection: {} to {}", start_str, now_str);

                // Use single gap detection that delegates to optimal actors
                if let Err(e) = detect_and_fill_gaps(
                    symbol, 
                    timeframe_seconds, 
                    start_time, 
                    now,
                    &config,
                    &lmdb_actor,
                    &historical_actor, 
                    &api_actor
                ).await {
                    warn!("⚠️ Gap detection failed for {} {}s: {}", symbol, timeframe_seconds, e);
                } else {
                    info!("✅ Gap detection completed for {} {}s", symbol, timeframe_seconds);
                }
            }
        }
        info!("✅ Unified gap detection and filling completed");
    }
    
    // Step 3.5: Now launch Technical Analysis actors with complete historical data using SAME timestamp
    let ta_actors = launch_ta_actors(&config, &api_actor, &ws_actor, &postgres_actor, &kafka_actor, synchronized_timestamp).await?;
    
    // Set TimeFrame actor reference on WebSocketActor
    if let Some((timeframe_actor, _)) = &ta_actors {
        use data_feeder::websocket::WebSocketTell;
        let set_timeframe_msg = WebSocketTell::SetTimeFrameActor {
            timeframe_actor: timeframe_actor.clone(),
        };
        if let Err(e) = ws_actor.tell(set_timeframe_msg).send().await {
            error!("Failed to set TimeFrame actor on WebSocketActor: {}", e);
        } else {
            info!("✅ Connected WebSocketActor to TimeFrameActor");
        }
    }
    
    // Step 4: Continuous monitoring mode (WebSocket already running)
    info!("♾️  Entering continuous monitoring mode...");
    run_continuous_monitoring(config, lmdb_actor, historical_actor, api_actor, ws_actor, postgres_actor, ta_actors).await?;
    
    Ok(())
}

/// Launch basic actors (without TA actors) 
async fn launch_basic_actors(config: &DataFeederConfig) -> Result<(
    ActorRef<LmdbActor>,
    ActorRef<HistoricalActor>, 
    ActorRef<ApiActor>, 
    ActorRef<WebSocketActor>,
    Option<ActorRef<PostgresActor>>,
    Option<ActorRef<KafkaActor>>
), Box<dyn std::error::Error + Send + Sync>> {
    
    info!("🎭 Launching actor system...");
    
    // LMDB Actor - for centralized LMDB operations and gap detection (create first)
    let lmdb_actor = {
        info!("🗃️ Launching LMDB actor...");
        let mut actor = LmdbActor::new(&config.storage_path, config.min_gap_seconds)
            .map_err(|e| format!("Failed to create LMDB actor: {}", e))?;
        
        // Initialize databases for all symbols and timeframes
        actor.initialize_databases(&config.symbols, &config.timeframes)
            .map_err(|e| format!("Failed to initialize LMDB databases: {}", e))?;
        
        kameo::spawn(actor)
    };
    
    // PostgreSQL Actor - for dual storage strategy
    let postgres_actor = if config.postgres_config.enabled {
        info!("🐘 Launching PostgreSQL actor...");
        let actor = PostgresActor::new(config.postgres_config.clone())
            .map_err(|e| format!("Failed to create PostgreSQL actor: {}", e))?;
        Some(kameo::spawn(actor))
    } else {
        info!("🚫 PostgreSQL actor disabled");
        None
    };
    
    // Kafka Actor - for publishing technical analysis data
    let kafka_actor = if config.kafka_config.enabled {
        info!("📨 Launching Kafka actor...");
        let actor = KafkaActor::new(config.kafka_config.clone())
            .map_err(|e| format!("Failed to create Kafka actor: {}", e))?;
        Some(kameo::spawn(actor))
    } else {
        info!("🚫 Kafka actor disabled");
        None
    };
    
    // Historical Actor - for S3/bulk data (will delegate LMDB operations to LmdbActor)
    let historical_actor = {
        let csv_temp_path = config.storage_path.join("temp_csv"); // Temporary staging within main storage
        let mut actor = HistoricalActor::new(&config.symbols, &config.timeframes, &config.storage_path, &csv_temp_path)
            .map_err(|e| format!("Failed to create HistoricalActor: {}", e))?;
        if let Some(ref postgres_ref) = postgres_actor {
            actor.set_postgres_actor(postgres_ref.clone());
        }
        actor.set_lmdb_actor(lmdb_actor.clone());
        kameo::spawn(actor)
    };
    
    // API Actor - for gap filling
    let api_actor = {
        let mut actor = ApiActor::new(lmdb_actor.clone())
            .map_err(|e| format!("Failed to create API actor: {}", e))?;
        if let Some(ref postgres_ref) = postgres_actor {
            actor.set_postgres_actor(postgres_ref.clone());
        }
        kameo::spawn(actor)
    };
    
    // WebSocket Actor without TA integration (TA actors will be added later)
    let ws_actor = {
        let mut actor = WebSocketActor::new_with_config(
            config.storage_path.clone(), 
            300, // 5min health check timeout
            config.reconnection_gap_threshold_minutes,
            config.reconnection_gap_check_delay_seconds
        ).map_err(|e| format!("Failed to create WebSocket actor: {}", e))?;
        
        // Set API and PostgreSQL actor references
        actor.set_api_actor(api_actor.clone());
        if let Some(ref postgres_ref) = postgres_actor {
            actor.set_postgres_actor(postgres_ref.clone());
        }
        kameo::spawn(actor)
    };
    
    // Allow actors to initialize
    sleep(Duration::from_millis(500)).await;
    
    // Start health check server
    info!("🏥 Starting health check server...");
    let health_config = HealthConfig::default();
    let health_dependencies = HealthDependencies {
        kafka_actor: kafka_actor.clone(),
        postgres_actor: postgres_actor.clone(),
    };
    
    // Start health server as background task
    tokio::spawn(async move {
        if let Err(e) = start_health_server(health_config, health_dependencies).await {
            error!("💥 Health server failed: {}", e);
        }
    });
    
    info!("✅ Basic actors launched successfully");
    Ok((lmdb_actor, historical_actor, api_actor, ws_actor, postgres_actor, kafka_actor))
}

/// Launch Technical Analysis actors after gap filling is complete
async fn launch_ta_actors(
    config: &DataFeederConfig,
    api_actor: &ActorRef<ApiActor>,
    _ws_actor: &ActorRef<WebSocketActor>,
    _postgres_actor: &Option<ActorRef<PostgresActor>>,
    kafka_actor: &Option<ActorRef<KafkaActor>>,
    synchronized_timestamp: i64
) -> Result<Option<(ActorRef<TimeFrameActor>, ActorRef<IndicatorActor>)>, Box<dyn std::error::Error + Send + Sync>> {
    
    if !config.enable_technical_analysis {
        info!("🚫 Technical Analysis disabled");
        return Ok(None);
    }
    
    info!("🧮 Launching Technical Analysis actors after gap filling...");
    
    // Create indicator actor first
    let indicator_actor = {
        let actor = IndicatorActor::new(config.ta_config.clone());
        let actor_ref = kameo::spawn(actor);
        
        // Set Kafka actor reference if available
        if let Some(ref kafka_ref) = kafka_actor {
            use data_feeder::technical_analysis::actors::indicator::IndicatorTell;
            let set_kafka_msg = IndicatorTell::SetKafkaActor {
                kafka_actor: kafka_ref.clone(),
            };
            if let Err(e) = actor_ref.tell(set_kafka_msg).send().await {
                error!("Failed to set Kafka actor on IndicatorActor: {}", e);
            } else {
                info!("✅ Connected IndicatorActor to KafkaActor");
            }
        }
        
        actor_ref
    };
    
    // Create timeframe actor and set its indicator and API actor references
    let timeframe_actor = {
        let mut actor = TimeFrameActor::new(config.ta_config.clone(), config.storage_path.clone());
        actor.set_indicator_actor(indicator_actor.clone());
        actor.set_api_actor(api_actor.clone());
        let actor_ref = kameo::spawn(actor);
        
        // Send synchronized timestamp before initialization
        use data_feeder::technical_analysis::actors::timeframe::TimeFrameTell;
        let set_timestamp_msg = TimeFrameTell::SetReferenceTimestamp {
            timestamp: synchronized_timestamp,
        };
        if let Err(e) = actor_ref.tell(set_timestamp_msg).send().await {
            error!("Failed to set synchronized timestamp on TimeFrameActor: {}", e);
        } else {
            info!("✅ Synchronized timestamp sent to TimeFrameActor: {}", synchronized_timestamp);
        }
        
        actor_ref
    };
    
    // Note: WebSocket → TimeFrame forwarding will be enabled when we restart WebSocket
    // or add a message to update the timeframe actor reference
    info!("✅ TimeFrame actor ready for WebSocket forwarding");
    
    info!("✅ Technical Analysis actors launched successfully");
    Ok(Some((timeframe_actor, indicator_actor)))
}

/// Start real-time WebSocket streaming
async fn start_realtime_streaming(
    config: &DataFeederConfig,
    ws_actor: &ActorRef<WebSocketActor>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    info!("📡 Initializing real-time streaming for {} symbols...", config.symbols.len());
    
    // Subscribe to 1-minute klines for all symbols
    let subscribe_msg = WebSocketTell::Subscribe {
        stream_type: StreamType::Kline1m,
        symbols: config.symbols.clone(),
    };
    
    if let Err(e) = ws_actor.tell(subscribe_msg).send().await {
        return Err(format!("Failed to subscribe to WebSocket: {}", e).into());
    }
    
    // Start the connection
    if let Err(e) = ws_actor.tell(WebSocketTell::Reconnect).send().await {
        return Err(format!("Failed to start WebSocket connection: {}", e).into());
    }
    
    // Wait for connection to establish
    sleep(Duration::from_secs(3)).await;
    
    // Verify connection status
    match ws_actor.ask(WebSocketAsk::GetConnectionStatus).await {
        Ok(WebSocketReply::ConnectionStatus { status, stats: _ }) => {
            info!("🔗 WebSocket connection status: {}", status);
        }
        Ok(_) => warn!("❌ Unexpected reply type for GetConnectionStatus"),
        Err(e) => warn!("❌ Failed to get connection status: {}", e),
    }
    
    info!("✅ Real-time streaming started");
    Ok(())
}

/// Main continuous monitoring loop (WebSocket already running)
async fn run_continuous_monitoring(
    config: DataFeederConfig,
    _lmdb_actor: ActorRef<LmdbActor>,
    _historical_actor: ActorRef<HistoricalActor>,
    api_actor: ActorRef<ApiActor>,
    ws_actor: ActorRef<WebSocketActor>,
    _postgres_actor: Option<ActorRef<PostgresActor>>,
    ta_actors: Option<(ActorRef<TimeFrameActor>, ActorRef<IndicatorActor>)>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    info!("♾️  Starting continuous monitoring...");
    info!("📝 Real-time data is streaming and being stored automatically");
    if ta_actors.is_some() {
        info!("🧮 Technical analysis is running and processing indicators");
    }
    let health_check_interval_secs = if config.periodic_gap_detection_enabled {
        config.periodic_gap_check_interval_minutes * 60
    } else {
        300 // Default 5 minutes
    };
    
    info!("🔄 Health checks every {} minutes", health_check_interval_secs / 60);
    if config.periodic_gap_detection_enabled {
        info!("🕳️ Periodic gap detection enabled - scanning last {} minutes", config.periodic_gap_check_window_minutes);
    }
    info!("⏹️  Press Ctrl+C to stop");
    
    let mut health_check_interval = tokio::time::interval(Duration::from_secs(health_check_interval_secs as u64));
    
    loop {
        tokio::select! {
            _ = health_check_interval.tick() => {
                info!("💓 Performing health check...");
                perform_health_check(&config, &api_actor, &ws_actor, &ta_actors).await;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("🛑 Shutdown signal received");
                break;
            }
        }
    }
    
    info!("🏁 Graceful shutdown completed");
    Ok(())
}

/// Perform periodic health checks
async fn perform_health_check(
    config: &DataFeederConfig,
    api_actor: &ActorRef<ApiActor>,
    ws_actor: &ActorRef<WebSocketActor>,
    ta_actors: &Option<(ActorRef<TimeFrameActor>, ActorRef<IndicatorActor>)>
) {
    info!("🔍 Health check for {} symbols...", config.symbols.len());
    
    // Check API actor health
    match api_actor.ask(ApiAsk::GetStats).await {
        Ok(ApiReply::Stats(stats)) => {
            info!("📊 API: {} requests, {:.1}% success, {} candles",
                  stats.requests_made, stats.success_rate() * 100.0, stats.total_candles_fetched);
        }
        _ => warn!("⚠️ API actor health check failed"),
    }
    
    // Check WebSocket health
    match ws_actor.ask(WebSocketAsk::GetConnectionStatus).await {
        Ok(WebSocketReply::ConnectionStatus { status, stats }) => {
            info!("📡 WebSocket: {}, {} messages, {:.1}% parsed",
                  status, stats.messages_received, stats.parse_success_rate() * 100.0);
        }
        _ => warn!("⚠️ WebSocket actor health check failed"),
    }
    
    // Check recent data for each symbol
    for symbol in &config.symbols {
        match ws_actor.ask(WebSocketAsk::GetRecentCandles {
            symbol: symbol.clone(),
            limit: 1,
        }).await {
            Ok(WebSocketReply::RecentCandles(candles)) => {
                if let Some(latest) = candles.first() {
                    let age_minutes = (chrono::Utc::now().timestamp_millis() - latest.close_time) / (1000 * 60);
                    if age_minutes > 5 {
                        warn!("⚠️ {} latest data is {} minutes old", symbol, age_minutes);
                    } else {
                        info!("✅ {} data is current ({}min old)", symbol, age_minutes);
                    }
                } else {
                    warn!("⚠️ No recent data for {}", symbol);
                }
            }
            _ => warn!("⚠️ Failed to check recent data for {}", symbol),
        }
    }
    
    // Check technical analysis actors if enabled
    if let Some((timeframe_actor, indicator_actor)) = ta_actors {
        info!("🧮 Checking Technical Analysis health...");
        
        // Check TimeFrame actor initialization status
        match timeframe_actor.ask(TimeFrameAsk::GetInitializationStatus).await {
            Ok(timeframe_reply) => {
                debug!("TimeFrame actor status: {:?}", timeframe_reply);
            }
            Err(e) => warn!("⚠️ Failed to get TimeFrame actor status: {}", e),
        }
        
        // Check Indicator actor initialization status
        match indicator_actor.ask(IndicatorAsk::GetInitializationStatus).await {
            Ok(indicator_reply) => {
                debug!("Indicator actor status: {:?}", indicator_reply);
            }
            Err(e) => warn!("⚠️ Failed to get Indicator actor status: {}", e),
        }
        
        // Get sample indicators for the first symbol
        if let Some(first_symbol) = config.symbols.first() {
            match indicator_actor.ask(IndicatorAsk::GetIndicators { 
                symbol: first_symbol.clone() 
            }).await {
                Ok(indicator_reply) => {
                    info!("📈 Sample indicators for {}: {:?}", first_symbol, indicator_reply);
                }
                Err(e) => warn!("⚠️ Failed to get indicators for {}: {}", first_symbol, e),
            }
        }
    }
    
    // Periodic gap detection (if enabled)
    if config.periodic_gap_detection_enabled {
        info!("🕳️ Performing periodic gap detection...");
        
        let now = chrono::Utc::now().timestamp_millis();
        let window_start = now - (config.periodic_gap_check_window_minutes as i64 * 60 * 1000);
        
        for symbol in &config.symbols {
            for &timeframe_seconds in &config.timeframes {
                if let Err(e) = check_for_recent_gaps(
                    symbol,
                    timeframe_seconds,
                    window_start,
                    now,
                    api_actor
                ).await {
                    warn!("⚠️ Periodic gap detection failed for {} {}s: {}", symbol, timeframe_seconds, e);
                }
            }
        }
    }
}

/// Check for gaps in recent data and trigger filling if needed
async fn check_for_recent_gaps(
    symbol: &str,
    timeframe_seconds: u64,
    _start_time: i64,
    end_time: i64,
    api_actor: &ActorRef<ApiActor>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let interval = match timeframe_seconds {
        60 => "1m",
        300 => "5m", 
        3600 => "1h",
        86400 => "1d",
        _ => return Err(format!("Unsupported timeframe: {}s", timeframe_seconds).into()),
    };
    
    // Get current data range to check for gaps
    match api_actor.ask(ApiAsk::GetDataRange {
        symbol: symbol.to_string(),
        interval: interval.to_string(),
    }).await {
        Ok(ApiReply::DataRange { earliest, latest, count, .. }) => {
            debug!("📊 {} {} data check: earliest={:?}, latest={:?}, count={}", 
                   symbol, interval, earliest, latest, count);
            
            // Check if we have recent data
            if let Some(latest_data) = latest {
                let gap_duration_ms = end_time - latest_data;
                let gap_minutes = gap_duration_ms / (60 * 1000);
                
                // If gap is > 2 minutes (allows for normal 1-minute delay), fill it
                if gap_minutes > 2 {
                    info!("🕳️ Found {} minute gap in {} {} - triggering fill", gap_minutes, symbol, interval);
                    
                    let gap_fill_msg = ApiTell::FillGap {
                        symbol: symbol.to_string(),
                        interval: interval.to_string(),
                        start_time: latest_data + (timeframe_seconds * 1000) as i64, // Start from next candle
                        end_time: end_time - (timeframe_seconds * 1000) as i64, // End at previous candle
                    };
                    
                    if let Err(e) = api_actor.tell(gap_fill_msg).send().await {
                        warn!("⚠️ Failed to trigger gap fill for {} {}: {}", symbol, interval, e);
                    } else {
                        info!("✅ Triggered gap fill for {} {} ({} minutes)", symbol, interval, gap_minutes);
                    }
                } else {
                    debug!("✅ {} {} data is current (gap: {} minutes)", symbol, interval, gap_minutes);
                }
            } else {
                warn!("⚠️ No data found for {} {} - may need historical backfill", symbol, interval);
            }
        }
        Ok(_) => {
            warn!("⚠️ Unexpected reply type for GetDataRange");
        }
        Err(e) => {
            warn!("⚠️ Failed to get data range for {} {}: {}", symbol, interval, e);
        }
    }
    
    Ok(())
}

