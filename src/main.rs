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
            monthly_threshold_months: 3, // Use monthly data for gaps > 3 months
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

/// Single gap detection with smart actor delegation
/// Checks the unified database and assigns gaps to optimal actors
async fn detect_and_fill_gaps(
    symbol: &str, 
    timeframe_seconds: u64,
    start_time: i64, 
    end_time: i64,
    config: &DataFeederConfig,
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
    
    // Step 1: Check existing data coverage in the unified database
    let data_range = match api_actor.ask(ApiAsk::GetDataRange {
        symbol: symbol.to_string(),
        interval: interval.to_string(),
    }).await {
        Ok(ApiReply::DataRange { earliest, latest, count, .. }) => {
            info!("📊 Current data: earliest={:?}, latest={:?}, count={}", earliest, latest, count);
            (earliest, latest, count)
        }
        Ok(_) => return Err("Unexpected reply type for GetDataRange".into()),
        Err(e) => return Err(format!("Failed to get data range: {}", e).into()),
    };
    
    let now = chrono::Utc::now().timestamp_millis();
    let (existing_earliest, existing_latest, _count) = data_range;
    
    // Step 2: Determine what gaps need to be filled
    let mut gaps = Vec::new();
    
    // Add logging to show reference date strategy
    if config.respect_config_start_date {
        info!("🎯 Using config start_date as absolute reference (ignoring existing DB data before it)");
    } else {
        info!("📊 Using database-based gap detection (preserving existing data)");
    }
    
    // Check for gap before existing data (only if respecting DB dates)
    if let Some(earliest) = existing_earliest {
        if config.respect_config_start_date {
            // When respecting config date, only consider data after start_time
            if earliest < start_time {
                info!("🗑️ Ignoring existing DB data before config start_date (earliest: {}, config: {})", earliest, start_time);
                // Treat as if no existing data before our desired start time
                gaps.push((start_time, end_time));
                info!("📉 Treating as fresh data request: {} to {}", start_time, end_time);
            } else {
                // Existing data is after our start time, check for gaps normally
                if start_time < earliest {
                    gaps.push((start_time, earliest));
                    info!("📉 Found historical gap: {} to {}", start_time, earliest);
                }
            }
        } else {
            // Original behavior: respect existing database data
            if start_time < earliest {
                gaps.push((start_time, earliest));
                info!("📉 Found historical gap: {} to {}", start_time, earliest);
            }
        }
    } else {
        // No existing data - entire range is a gap
        gaps.push((start_time, end_time));
        info!("📉 No existing data - filling entire range: {} to {}", start_time, end_time);
    }
    
    // Check for gap after existing data (real-time gap)
    if let Some(latest) = existing_latest {
        if latest < end_time {
            gaps.push((latest, end_time));
            info!("📈 Found real-time gap: {} to {}", latest, end_time);
        }
    }
    
    // Step 3: Smart actor delegation for each gap
    for &(gap_start, gap_end) in &gaps {
        let gap_duration_hours = (gap_end - gap_start) / (1000 * 3600);
        let gap_duration_days = gap_duration_hours / 24;
        let gap_duration_months = gap_duration_days / 30; // Approximate months
        let gap_age_hours = (now - gap_end) / (1000 * 3600);
        
        info!("🔧 Processing gap: start={}, end={}, duration={}h ({}d, ~{}mo), age={}h", 
              gap_start, gap_end, gap_duration_hours, gap_duration_days, gap_duration_months, gap_age_hours);
        
        // Enhanced classification: Monthly vs Daily vs Intraday
        let use_monthly_data = gap_duration_months >= config.monthly_threshold_months as i64;
        
        if gap_age_hours > 48 && gap_duration_hours > 24 {
            // Historical gap → HistoricalActor (S3 bulk download)
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
            // Intraday/Recent gap → ApiActor (targeted API calls)
            info!("⚡ Assigning intraday gap to ApiActor (targeted API)");
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
    
    // Step 4: Wait for completion and verify
    if !gaps.is_empty() {
        info!("⏳ Waiting for gap filling to complete...");
        sleep(Duration::from_secs(10)).await;
        
        // Verify completeness
        match api_actor.ask(ApiAsk::GetDataRange {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
        }).await {
            Ok(ApiReply::DataRange { earliest, latest, count, .. }) => {
                info!("✅ Post-fill verification: earliest={:?}, latest={:?}, count={}", 
                      earliest, latest, count);
                
                // Check if we achieved complete coverage
                if let (Some(earliest), Some(latest)) = (earliest, latest) {
                    if earliest <= start_time && latest >= (end_time - (timeframe_seconds * 1000) as i64) {
                        info!("✅ Complete data coverage achieved!");
                    } else {
                        warn!("⚠️ Gaps may still exist after filling");
                    }
                }
            }
            _ => warn!("❌ Failed to verify data completeness"),
        }
    } else {
        info!("✅ No gaps detected - data is complete");
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
    
    // Step 1: Launch actors with unified storage
    let (historical_actor, api_actor, ws_actor, postgres_actor, _kafka_actor, ta_actors) = launch_actors(&config).await?;
    
    // Step 2: Start real-time data streaming FIRST
    info!("📡 Starting real-time data streaming...");
    start_realtime_streaming(&config, &ws_actor).await?;
    
    // Step 2.5: Direct WebSocket → TimeFrame forwarding (no bridge needed)
    if ta_actors.is_some() {
        info!("⚡ Direct WebSocket → TimeFrame forwarding active (no bridge delays)");
    }
    
    // Step 3: Single gap detection with smart actor delegation
    if config.gap_detection_enabled {
        info!("🔍 Starting unified gap detection and filling...");
        for symbol in &config.symbols {
            for &timeframe_seconds in &config.timeframes {
                let now = chrono::Utc::now().timestamp_millis();
                let start_time = if let Some(ref date_str) = config.start_date {
                    parse_start_date(date_str).unwrap_or_else(|e| {
                        warn!("❌ Failed to parse start_date: {}. Using 24h ago", e);
                        now - (24 * 3600 * 1000)
                    })
                } else {
                    now - (24 * 3600 * 1000) // Default 24h ago
                };

                // Use single gap detection that delegates to optimal actors
                if let Err(e) = detect_and_fill_gaps(
                    symbol, 
                    timeframe_seconds, 
                    start_time, 
                    now,
                    &config,
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
    
    // Step 4: Continuous monitoring mode (WebSocket already running)
    info!("♾️  Entering continuous monitoring mode...");
    run_continuous_monitoring(config, historical_actor, api_actor, ws_actor, postgres_actor, ta_actors).await?;
    
    Ok(())
}

/// Launch all actors with unified configuration
async fn launch_actors(config: &DataFeederConfig) -> Result<(
    ActorRef<HistoricalActor>, 
    ActorRef<ApiActor>, 
    ActorRef<WebSocketActor>,
    Option<ActorRef<PostgresActor>>,
    Option<ActorRef<KafkaActor>>,
    Option<(ActorRef<TimeFrameActor>, ActorRef<IndicatorActor>)>
), Box<dyn std::error::Error + Send + Sync>> {
    
    info!("🎭 Launching actor system...");
    
    // PostgreSQL Actor - for dual storage strategy (create first)
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
    
    // Historical Actor - for S3/bulk data (unified storage)
    let historical_actor = {
        let csv_temp_path = config.storage_path.join("temp_csv"); // Temporary staging within main storage
        let mut actor = HistoricalActor::new(&config.symbols, &config.timeframes, &config.storage_path, &csv_temp_path)
            .map_err(|e| format!("Failed to create HistoricalActor: {}", e))?;
        if let Some(ref postgres_ref) = postgres_actor {
            actor.set_postgres_actor(postgres_ref.clone());
        }
        kameo::spawn(actor)
    };
    
    // API Actor - for gap filling
    let api_actor = {
        let mut actor = ApiActor::new(config.storage_path.clone())
            .map_err(|e| format!("Failed to create API actor: {}", e))?;
        if let Some(ref postgres_ref) = postgres_actor {
            actor.set_postgres_actor(postgres_ref.clone());
        }
        kameo::spawn(actor)
    };
    
    // Technical Analysis Actors - if enabled
    let (ws_actor, ta_actors) = if config.enable_technical_analysis {
        info!("🧮 Launching Technical Analysis actors...");
        
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
            kameo::spawn(actor)
        };
        
        // WebSocket Actor with direct TimeFrame actor reference
        let ws_actor = {
            let mut actor = WebSocketActor::new_with_config(
                config.storage_path.clone(), 
                300, // 5min health check timeout
                config.reconnection_gap_threshold_minutes,
                config.reconnection_gap_check_delay_seconds
            ).map_err(|e| format!("Failed to create WebSocket actor: {}", e))?;
            
            // Set API, PostgreSQL, and TimeFrame actor references
            actor.set_api_actor(api_actor.clone());
            if let Some(ref postgres_ref) = postgres_actor {
                actor.set_postgres_actor(postgres_ref.clone());
            }
            actor.set_timeframe_actor(timeframe_actor.clone());
            
            kameo::spawn(actor)
        };
        
        info!("✅ Connected WebSocket → TimeFrame direct forwarding");
        (ws_actor, Some((timeframe_actor, indicator_actor)))
    } else {
        info!("🚫 Technical Analysis disabled");
        
        // WebSocket Actor without TA integration
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
        
        (ws_actor, None)
    };
    
    // Allow actors to initialize
    sleep(Duration::from_millis(500)).await;
    
    info!("✅ All actors launched successfully");
    Ok((historical_actor, api_actor, ws_actor, postgres_actor, kafka_actor, ta_actors))
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

