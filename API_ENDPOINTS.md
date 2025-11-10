# Data Feeder - Comprehensive API Endpoints Documentation

## Project Overview
**Framework**: Rust with Warp (HTTP) + Hyper (Metrics Server) + Actor-based system (Kameo)
**Main Purpose**: High-performance cryptocurrency data pipeline for real-time market analysis
**Supported Exchanges**: Binance Futures, Gate.io Futures

---

## HTTP API ENDPOINTS

### A. Metrics Server (Port: Configurable, default 8080)
Base URL: `http://localhost:8080`

#### Health & Readiness Endpoints

| HTTP Method | Endpoint | Purpose | Parameters | Response |
|------------|----------|---------|-----------|----------|
| GET | `/health` | Liveness probe (process running) | None | JSON: `{status: "healthy", timestamp, service}` |
| GET | `/ready` | Readiness probe (dependencies healthy) | None | JSON: `{status: "ready", checks: {...}}` |
| GET | `/startup` | Startup probe (service starting) | None | JSON: `{status: "starting", message, timestamp}` |
| GET | `/metrics` | Prometheus metrics export | None | Prometheus-format metrics text |
| GET | `/validate` | Resource-aware validation | None | JSON: `{overall_status, environment, performance_thresholds}` |

#### Deployment Validation Endpoints

| HTTP Method | Endpoint | Purpose | Parameters | Response |
|------------|----------|---------|-----------|----------|
| GET | `/deploy/validate` | Full deployment validation | None | JSON: `{overall_status, validation_results[]}` |
| GET | `/deploy/check` | Quick deployment readiness check | None | JSON: `{deployable: bool, message, timestamp}` |

#### Performance & Dashboard Endpoints

| HTTP Method | Endpoint | Purpose | Parameters | Response |
|------------|----------|---------|-----------|----------|
| GET | `/dashboard` | CPU Performance Dashboard (HTML) | None | HTML page with interactive charts |
| GET | `/dashboard/multidimensional` | Multi-dimensional performance metrics | None | JSON: Multi-dimensional performance report |
| GET | `/api/performance/report` | Enhanced performance report (JSON) | None | JSON: `{timestamp, version, story, report}` |
| GET | `/api/performance/flame-graph` | Flame graph profiling data | None | JSON: `{call_stacks[], total_samples, depth}` |
| GET | `/api/performance/threads` | Thread-level profiling data | None | JSON: `{thread_profiling[], context_switching[]}` |

---

## INTERNAL EXCHANGE API CLIENTS

### B. Binance Futures API Client

**Base URL**: `https://fapi.binance.com`
**Framework**: Custom HTTP client using reqwest + sonic-rs for parsing
**Authentication**: Not required for public endpoints

#### Klines (Candlestick) Data Endpoint

| Method | Operation | Path | Purpose | Parameters | Response |
|--------|-----------|------|---------|-----------|----------|
| GET (REST) | `fetch_klines()` | `/fapi/v1/klines` | Fetch OHLCV candle data | `ApiRequest{symbol, interval, start_time?, end_time?, limit?}` | `ApiResponse<Vec<FuturesOHLCVCandle>>` |

#### Ticker Endpoint

| Method | Operation | Path | Purpose | Parameters | Response |
|--------|-----------|------|---------|-----------|----------|
| GET (REST) | `fetch_ticker_24hr()` | `/fapi/v1/ticker/24hr` | Get 24hr ticker stats | symbol: `&str` | `ApiResponse<Ticker24hr>` |

#### Exchange Info Endpoint

| Method | Operation | Path | Purpose | Parameters | Response |
|--------|-----------|------|---------|-----------|----------|
| GET (REST) | `fetch_exchange_info()` | `/fapi/v1/exchangeInfo` | Get exchange symbols & limits | None | `ApiResponse<ExchangeInfo>` |

**Configuration**:
```
base_url: "https://fapi.binance.com"
timeout: 30 seconds
rate_limit: 20 requests/second (50ms min interval)
max_retries: 3
max_requests_per_minute: 1200
```

---

### C. Gate.io Futures API Client

**Base URL**: `https://api.gateio.ws`
**Framework**: Custom HTTP client using reqwest with HMAC-SHA256 authentication
**Authentication**: Required for trading operations (API Key + Secret)

#### Klines (Candlestick) Data Endpoint

| Method | Operation | Path | Purpose | Parameters | Response |
|--------|-----------|------|---------|-----------|----------|
| GET (REST) | `fetch_klines()` | `/api/v4/futures/usdt/candlesticks` | Fetch OHLCV candle data | `ApiRequest{symbol, interval, start_time?, end_time?, limit?}` | `ApiResponse<Vec<FuturesOHLCVCandle>>` |

#### Ticker Endpoint

| Method | Operation | Path | Purpose | Parameters | Response |
|--------|-----------|------|---------|-----------|----------|
| GET (REST) | `fetch_ticker_24hr()` | `/api/v4/futures/usdt/tickers` | Get 24hr ticker stats | symbol: `&str` | `ApiResponse<Ticker24hr>` |

#### Exchange Info Endpoint

| Method | Operation | Path | Purpose | Parameters | Response |
|--------|-----------|------|---------|-----------|----------|
| GET (REST) | `fetch_exchange_info()` | `/api/v4/futures/usdt/symbols` | Get exchange symbols & info | None | `ApiResponse<ExchangeInfo>` |

#### Trading Operations (Authenticated)

| HTTP Method | Operation | Path | Purpose | Request Body | Response |
|------------|-----------|------|---------|--------------|----------|
| POST | `place_order()` | `/api/v4/futures/usdt/orders` | Place a new order | `OrderRequest{symbol, side, order_type, quantity, price?, time_in_force?, client_order_id?, reduce_only}` | `OrderResponse{order, success, message?, timestamp}` |
| DELETE | `cancel_order()` | `/api/v4/futures/usdt/orders/{order_id}` | Cancel existing order | None (order_id in path) | `OrderResponse{order, success, message?, timestamp}` |
| PUT | `modify_order()` | `/api/v4/futures/usdt/orders` | Modify existing order | `ModifyOrderRequest{symbol, order_id, quantity?, price?}` | `OrderResponse{order, success, message?, timestamp}` |
| GET | `get_open_orders()` | `/api/v4/futures/usdt/orders` | Fetch open orders | Query: `contract=symbol&status=open` (optional) | `Vec<Order>` |
| GET | `get_positions()` | `/api/v4/futures/usdt/positions` | Fetch open positions | Query: `contract=symbol` (optional) | `Vec<Position>` |
| GET | `get_balance()` | `/api/v4/futures/usdt/accounts` | Fetch account balances | None | `Vec<Balance>` |

**Configuration**:
```
base_url: "https://api.gateio.ws"
timeout: 30 seconds
rate_limit: Conservative at 50ms min interval
max_retries: 3
max_requests_per_minute: 1000
authentication: HMAC-SHA256 (custom header format)
```

**Authentication Headers**:
```
KEY: {api_key}
Timestamp: {unix_timestamp}
SIGN: HMAC-SHA256(METHOD\nPATH\nBODY\nTIMESTAMP\nNONCE)
Content-Type: application/json
```

---

## INTERNAL ACTOR-BASED MESSAGE PASSING APIs

### D. API Actor Messages
**Framework**: Kameo actor system with async/await

#### Tell Messages (Fire-and-forget)

| Message | Parameters | Purpose |
|---------|-----------|---------|
| `FillGap` | symbol, interval, start_time, end_time | Asynchronously fill missing data gaps |
| `FetchRecent` | symbol, interval, limit | Fetch recent data to bridge to real-time |

#### Ask Messages (Request-Response)

| Message | Parameters | Response | Purpose |
|---------|-----------|----------|---------|
| `GetStats` | None | `ApiStats` | Get API request statistics |
| `GetDataRange` | symbol, interval | `DataRange{earliest, latest, count}` | Get available data range for symbol/interval |
| `FetchKlines` | symbol, interval, start_time?, end_time?, limit? | `Vec<FuturesOHLCVCandle>` | Fetch klines with custom parameters |

---

### E. WebSocket Actor Messages
**Framework**: Kameo actor system with fastwebsockets

#### Data Streaming Messages

| Message | Stream Type | Exchange | Purpose |
|---------|-----------|----------|---------|
| `Kline` | Real-time | Binance, Gate.io | Live candlestick updates |
| `Depth` | Real-time | Binance, Gate.io | Order book depth updates |
| `Ticker` | Real-time | Binance, Gate.io | Price ticker updates |
| `Aggr Trade` | Real-time | Binance | Aggregate trade stream |

**Gate.io Trading WebSocket Messages**:
- `Orders` - Real-time order updates
- `Positions` - Real-time position updates
- `Trades` - Real-time trade executions
- `Balances` - Account balance updates

---

### F. Technical Analysis Actor Messages
**Framework**: Kameo actor system

#### TimeFrame Actor Messages
- Aggregates real-time candles to multiple timeframes (1m, 5m, 15m, 1h, 4h)
- Computes OHLCV data
- Emits higher-timeframe closed candles

#### Indicator Actor Messages
- Calculates EMA (21, 89 periods)
- Trend analysis via EMA crossovers
- Volume tracking and alerts
- Volume profile calculations

---

## DATA STRUCTURES

### Request Types

```rust
// Klines Request
ApiRequest {
    endpoint: ApiEndpoint,
    symbol: String,
    interval: String,          // "1m", "5m", "15m", "1h", "4h", etc.
    start_time: Option<TimestampMS>,
    end_time: Option<TimestampMS>,
    limit: Option<u32>,
}

// Order Request
OrderRequest {
    symbol: String,
    side: OrderSide,           // Buy | Sell
    order_type: OrderType,     // Market | Limit | StopLoss | StopLimit | TakeProfit
    quantity: f64,
    price: Option<f64>,
    time_in_force: Option<String>,  // "IOC", "GTC", etc.
    client_order_id: Option<String>,
    reduce_only: bool,
}

// Modify Order Request
ModifyOrderRequest {
    symbol: String,
    order_id: String,
    quantity: Option<f64>,
    price: Option<f64>,
}
```

### Response Types

```rust
// Candle/OHLCV Data
FuturesOHLCVCandle {
    timestamp: TimestampMS,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    quote_volume: Decimal,
    trades: u64,
}

// Order Structure
Order {
    id: String,
    symbol: String,
    side: OrderSide,
    order_type: OrderType,
    quantity: f64,
    price: Option<f64>,
    status: OrderStatus,      // New | PartiallyFilled | Filled | Cancelled | Rejected
    timestamp: i64,
}

// Position Structure
Position {
    symbol: String,
    side: PositionSide,       // Long | Short | None
    size: f64,
    entry_price: f64,
    mark_price: f64,
    unrealized_pnl: f64,
    margin_required: f64,
    timestamp: i64,
}

// Balance Structure
Balance {
    asset: String,
    available: f64,
    locked: f64,
    total: f64,
    timestamp: i64,
}

// Order Response
OrderResponse {
    order: Order,
    success: bool,
    message: Option<String>,
    timestamp: i64,
}

// Generic API Response
ApiResponse<T> {
    data: T,
    timestamp: TimestampMS,
    rate_limit_info: Option<RateLimitInfo>,
}
```

### Error Types

```rust
pub enum ApiError {
    Http(String),              // HTTP protocol errors
    Parse(String),             // JSON/data parsing errors
    RateLimit(String),         // Rate limit exceeded
    InvalidSymbol(String),     // Invalid trading pair
    InvalidTimeframe(String),  // Invalid time interval
    Authentication(String),    // Auth failures
    Network(String),           // Network connectivity issues
    Timeout(String),           // Request timeouts
    Unknown(String),           // Generic errors
}
```

---

## RATE LIMITING

### Binance Futures
- **Limit**: 1200 requests per minute (20 req/sec)
- **Interval**: 50ms minimum between requests
- **Rate Limit Headers**: Read from response headers
- **Retry**: Respects `Retry-After` header on 429 responses

### Gate.io Futures
- **Limit**: 1000 requests per minute (conservative)
- **Interval**: 50ms minimum between requests
- **Rate Limit Headers**: Read from response headers
- **Retry**: Respects `Retry-After` header on 429 responses

---

## SUPPORTED TIMEFRAMES

- `1m` - 1 minute
- `3m` - 3 minutes
- `5m` - 5 minutes
- `15m` - 15 minutes
- `30m` - 30 minutes
- `1h` - 1 hour
- `2h` - 2 hours
- `4h` - 4 hours
- `6h` - 6 hours
- `8h` - 8 hours
- `12h` - 12 hours
- `1d` - 1 day
- `3d` - 3 days
- `1w` - 1 week
- `1M` - 1 month

---

## CONFIGURATION

### Default Configuration (Binance)
```rust
ApiConfig {
    base_url: "https://fapi.binance.com",
    timeout_seconds: 30,
    max_retries: 3,
    rate_limit_delay_ms: 50,
    max_requests_per_minute: 1200,
}
```

### Gate.io Configuration
```rust
ApiConfig {
    base_url: "https://api.gateio.ws",
    timeout_seconds: 30,
    max_retries: 3,
    rate_limit_delay_ms: 50,
    max_requests_per_minute: 1000,
}
```

---

## API STATISTICS & MONITORING

### Tracked Metrics
- Total requests made
- Successful requests
- Failed requests
- Rate limit hits
- Total candles fetched
- Last request timestamp

### Performance Metrics (Trading Operations)
- Order placement latency
- Order cancellation latency
- Order modification latency
- Position fetch latency
- Balance fetch latency
- Authentication generation latency
- JSON parsing latency

---

## DEPLOYMENT VALIDATION

### Validation Endpoints Check

1. **Memory Availability**: Minimum required percentage
2. **CPU Availability**: Recommended thread pool size
3. **Database Health**: LMDB availability
4. **Kafka Health**: (if enabled) Connection status
5. **PostgreSQL Health**: (if enabled) Connection pool status
6. **Resource Classification**:
   - Low Resource: <2 CPU or <2GB RAM
   - Standard Resource: 2-4 CPU, 2-8GB RAM
   - High Resource: >4 CPU, >8GB RAM
   - Containerized: Detected in container

---

## FEATURE FLAGS & COMPILATION

```bash
# Minimal build (LMDB only)
cargo build --release --no-default-features

# Full featured build
cargo build --release --features="kafka,postgres,volume_profile,volume_profile_reprocessing"
```

### Optional Features
- `kafka` - Real-time indicator publishing
- `postgres` - PostgreSQL dual storage
- `volume_profile` - Daily volume profile analysis
- `volume_profile_reprocessing` - Volume profile reprocessing utilities

