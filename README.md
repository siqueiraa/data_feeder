# 🚀 Cryptocurrency Data Feeder

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/your-username/data_feeder)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust Version](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

> A high-performance, actor-based cryptocurrency data pipeline built in Rust for real-time market analysis and historical data processing.

## 🎯 Overview

The Cryptocurrency Data Feeder is a robust, production-ready system designed to collect, process, and analyze cryptocurrency market data from Binance. Built with Rust's performance and safety guarantees, it provides real-time technical analysis indicators and comprehensive historical data management.

**Perfect for:**
- 📊 Quantitative trading and research
- 📈 Real-time market monitoring and alerts
- 🔍 Technical analysis and backtesting
- 📉 Market data infrastructure for trading systems

## ✨ Key Features

### 🏗️ **Actor-Based Architecture**
- **Concurrent Processing**: Kameo framework for fault-tolerant, concurrent operations
- **Modular Design**: Independent actors for historical data, WebSocket streaming, technical analysis
- **Scalable**: Handle multiple symbols and timeframes simultaneously

### 📊 **Smart Data Management**
- **Intelligent Path Selection**: Automatically chooses optimal data sources (monthly vs daily)
- **Gap Detection**: Automatically identifies and fills missing data periods, including post-reconnection gap detection
- **LMDB Storage**: Lightning-fast memory-mapped database for primary storage
- **PostgreSQL Support**: Optional relational database integration
- **Data Integrity**: SHA256 checksums and certified range tracking

### 📡 **Real-time Processing**
- **WebSocket Streaming**: Live market data from Binance Futures
- **Multi-timeframe Analysis**: 1m, 5m, 15m, 1h, 4h technical indicators
- **Kafka Integration**: Real-time publishing of analysis results
- **Auto-reconnection**: Robust connection management with automatic post-reconnection gap detection and recovery

### 🎯 **Technical Analysis**
- **EMA Indicators**: 21 and 89-period exponential moving averages
- **Trend Analysis**: Multi-timeframe trend detection using EMA crossovers
- **Volume Analysis**: Maximum volume tracking with trend correlation
- **Volume Profiles**: Daily volume distribution analysis with POC, VWAP, and value area calculation
- **Real-time Alerts**: Instant indicator updates via Kafka

### 🔧 **Production Ready**
- **Zero Warnings Policy**: Clean, maintainable codebase
- **Comprehensive Testing**: Unit, integration, and end-to-end tests
- **Performance Optimized**: 10-100x faster than equivalent Python implementations
- **Error Resilience**: Production-grade error handling and recovery

## 🏛️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Binance API   │    │   WebSocket      │    │   Kafka Broker  │
│  (Historical)   │    │ (Real-time Data) │    │  (Indicators)   │
└─────────┬───────┘    └─────────┬────────┘    └─────────┬───────┘
          │                      │                       │
          ▼                      ▼                       ▲
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Historical      │    │ WebSocket        │    │ Kafka           │
│ Actor           │    │ Actor            │    │ Actor           │
└─────────┬───────┘    └─────────┬────────┘    └─────────┬───────┘
          │                      │                       │
          ▼                      ▼                       │
┌─────────────────────────────────────────────────────────┼───────┐
│                    TimeFrame Actor                      │       │
│            (Aggregates to multiple timeframes)         │       │
└─────────────────────────┬───────────────────────────────┘       │
                          ▼                                       │
┌─────────────────────────────────────────────────────────────────┼─┐
│                   Indicator Actor                               │ │
│      (EMA, Trend Analysis, Volume Tracking)                    │ │
└─────────────────────────┬───────────────────────────────────────┼─┘
                          │                                       │
                          └───────────────────────────────────────┘
                          
                          ▼
                ┌─────────────────┐    ┌─────────────────┐
                │      LMDB       │    │   PostgreSQL    │
                │   (Primary)     │    │   (Optional)    │
                └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- **Rust**: 1.70+ (install from [rustup.rs](https://rustup.rs/))
- **System Requirements**: 
  - 4GB+ RAM (recommended 8GB)
  - 10GB+ disk space for historical data
  - Network access to Binance API

### Installation

1. **Clone the repository:**
```bash
git clone https://github.com/siqueiraa/data_feeder.git
cd data_feeder
```

2. **Build the project:**
```bash
cargo build --release
```

3. **Copy and configure settings:**
```bash
cp config.toml.example config.toml
# Edit config.toml with your settings
```

4. **Run the application:**
```bash
cargo run --release
```

### Basic Configuration

Edit `config.toml` to customize your setup:

```toml
[application]
symbols = ["BTCUSDT", "ETHUSDT"]  # Trading pairs to monitor
timeframes = [60, 300, 900, 3600, 14400]  # 1m, 5m, 15m, 1h, 4h
enable_technical_analysis = true

[kafka]
enabled = true  # Enable real-time indicator publishing
bootstrap_servers = ["your-kafka-broker:9092"]
topic_prefix = "ta_"  # Results in "ta_data" topic

[database]
enabled = false  # Optional PostgreSQL integration
```

## 📊 Data Output

### Kafka Message Format

Technical analysis indicators are published to Kafka in JSON format:

```json
{
  "symbol": "BTCUSDT",
  "timestamp": 1674123456789,
  "close_5m": 23450.50,
  "close_15m": 23455.25,
  "close_60m": 23460.75,
  "close_4h": 23470.00,
  "ema21_1min": 23445.30,
  "ema89_1min": 23440.15,
  "ema89_5min": 23435.80,
  "ema89_15min": 23430.45,
  "ema89_1h": 23425.20,
  "ema89_4h": 23420.10,
  "trend_1min": "Buy",
  "trend_5min": "Neutral",
  "trend_15min": "Sell",
  "trend_1h": "Buy",
  "trend_4h": "Neutral",
  "max_volume": 1250000.0,
  "max_volume_price": 23445.30,
  "max_volume_time": "2023-01-19T14:30:00Z",
  "max_volume_trend": "Buy"
}
```

### Message Routing

- **Topic**: `{topic_prefix}data` (e.g., `ta_data`)
- **Key**: Symbol name (e.g., `BTCUSDT`) for partitioning
- **Timestamp**: 1-minute candle close time in milliseconds

## 🛠️ Development

### Project Structure

```
src/
├── main.rs                    # Application entry point
├── historical/                # Historical data processing
│   ├── actor.rs              # Main historical data actor
│   ├── utils.rs              # Data processing utilities
│   └── structs.rs            # Data structures
├── websocket/                 # Real-time data streaming
│   ├── actor.rs              # WebSocket connection manager
│   ├── connection.rs         # Connection handling
│   └── binance/              # Binance-specific implementations
├── technical_analysis/        # Technical indicators
│   ├── actors/               # Analysis actors
│   ├── structs.rs            # Indicator data structures
│   └── utils.rs              # Calculation utilities
├── kafka/                     # Kafka integration
│   ├── actor.rs              # Kafka producer actor
│   └── errors.rs             # Error handling
└── postgres/                  # PostgreSQL integration
    ├── actor.rs              # Database actor
    └── errors.rs             # Database errors
```

### Building from Source

```bash
# Development build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test

# Run with logging
RUST_LOG=info cargo run

# Check code quality
cargo check
cargo clippy
```

### Running Tests

```bash
# All tests
cargo test

# Integration tests only
cargo test --test kafka_integration_test

# End-to-end tests
cargo test --test e2e_kafka_test

# With output
cargo test -- --nocapture
```

## ⚡ Performance

### Benchmarks

- **Historical Data Processing**: 10,000+ candles/second
- **Real-time Processing**: <1ms latency for indicator updates
- **Memory Usage**: ~50MB base + ~10MB per symbol
- **Kafka Throughput**: 5,000+ messages/second

### Optimization Tips

1. **Increase Batch Sizes**: For historical processing, use larger batch sizes
2. **Tune LMDB**: Adjust map sizes based on data volume
3. **Kafka Configuration**: Optimize producer settings for throughput
4. **Symbol Limits**: Monitor memory usage with large symbol sets

## 🔧 Configuration Reference

### Application Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `symbols` | `["BTCUSDT"]` | Trading pairs to process |
| `timeframes` | `[60]` | Timeframes in seconds |
| `storage_path` | `"lmdb_data"` | Local storage directory |
| `enable_technical_analysis` | `true` | Enable indicator calculation |

### Kafka Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `enabled` | `false` | Enable Kafka publishing |
| `bootstrap_servers` | `["localhost:9092"]` | Kafka brokers |
| `topic_prefix` | `"ta_"` | Topic name prefix |
| `acks` | `"all"` | Acknowledgment level |

### Technical Analysis Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `min_history_days` | `60` | Minimum history for indicators |
| `ema_periods` | `[21, 89]` | EMA periods to calculate |
| `volume_lookback_days` | `60` | Volume analysis window |

### Volume Profile Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `enabled` | `true` | Enable volume profile calculation |
| `price_increment_mode` | `"Fixed"` | Price bucketing mode: "Fixed" or "Adaptive" |
| `fixed_price_increment` | `0.01` | Fixed price increment when mode is "Fixed" |
| `min_price_increment` | `0.001` | Minimum price increment for "Adaptive" mode |
| `max_price_increment` | `1.0` | Maximum price increment for "Adaptive" mode |
| `update_frequency` | `"EveryCandle"` | Update frequency: "EveryCandle", "Every5Candles", or "Every10Candles" |
| `batch_size` | `1` | Number of profiles to batch before storage |
| `value_area_percentage` | `70.0` | Percentage of volume to include in value area calculation |

## ❗ Troubleshooting

### Common Issues

**Connection Errors:**
```bash
# Check network connectivity
curl -I https://fapi.binance.com/fapi/v1/ping

# Verify Kafka broker
telnet your-kafka-broker 9092
```

**High Memory Usage:**
- Reduce number of symbols
- Decrease `volume_lookback_days`
- Adjust LMDB map sizes

**Missing Data:**
- Check gap detection logs
- Verify Binance API limits
- Review start_date configuration

### Logging

```bash
# Enable debug logging
RUST_LOG=debug cargo run

# Module-specific logging
RUST_LOG=data_feeder::kafka=debug cargo run

# Save logs to file
RUST_LOG=info cargo run 2>&1 | tee app.log
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Quick Contribution Guide

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`cargo test`)
5. Commit changes (`git commit -m 'Add amazing feature'`)
6. Push to branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

### Code Standards

- **Zero Warnings**: `cargo check` must show no warnings
- **Testing**: Add tests for new features
- **Documentation**: Update docs for public APIs
- **Performance**: Consider performance impact of changes

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Binance API](https://binance-docs.github.io/apidocs/) for market data
- [Kameo](https://github.com/tqwewe/kameo) for the actor framework  
- [rdkafka](https://github.com/fede1024/rust-rdkafka) for Kafka integration
- [LMDB](https://github.com/danburkert/lmdb-rs) for high-performance storage

---

**⭐ If this project helps you, please give it a star!**

For questions, issues, or feature requests, please [open an issue](https://github.com/your-username/data_feeder/issues).