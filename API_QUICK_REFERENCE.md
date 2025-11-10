# API Quick Reference Guide

## API Framework Summary

| Aspect | Details |
|--------|---------|
| **Language** | Rust (Edition 2021) |
| **HTTP Framework** | Warp 0.3 (REST), Hyper 1.0 (Metrics Server) |
| **Actor Framework** | Kameo 0.14 |
| **Primary Purpose** | Cryptocurrency data pipeline with real-time streaming |
| **Main HTTP Port** | 8080 (configurable) |
| **Exchange APIs** | Binance Futures, Gate.io Futures |

---

## File Structure Overview

### Core API Files
```
src/api/
├── mod.rs                      # Module exports
├── actor.rs                    # API actor for REST data fetching
├── exchange.rs                 # Exchange trait and common types
├── exchange_manager.rs         # Multi-exchange coordination
├── types.rs                    # Request/response structures, enums
├── trading_metrics.rs          # Trading operation latency tracking
├── optimized_parsing.rs        # Performance-optimized JSON parsing
├── binance/
│   ├── mod.rs
│   └── klines.rs              # Binance Futures klines client
└── gate_io/
    ├── mod.rs
    ├── klines.rs              # Gate.io Futures klines client
    └── trading.rs             # Gate.io Futures trading client
```

### HTTP Server Files
```
src/
├── metrics_server.rs           # Hyper-based metrics/health server
├── health/mod.rs              # Warp-based health check routes
└── websocket/
    ├── mod.rs
    ├── actor.rs               # WebSocket streaming actor
    ├── connection.rs
    ├── binance/               # Binance stream implementations
    │   ├── depth.rs
    │   ├── kline.rs
    │   ├── ticker.rs
    │   └── trade.rs
    └── gate_io/               # Gate.io stream implementations
        ├── balances.rs
        ├── orders.rs
        ├── positions.rs
        └── trades.rs
```

---

## HTTP Endpoints Summary

### Metrics Server Endpoints (13 total)

**Health & Status** (4):
- `GET /health` - Liveness
- `GET /ready` - Readiness
- `GET /startup` - Startup probe
- `GET /metrics` - Prometheus metrics

**Validation & Deployment** (3):
- `GET /validate` - Resource validation
- `GET /deploy/validate` - Full deployment check
- `GET /deploy/check` - Quick deployment check

**Performance & Monitoring** (6):
- `GET /dashboard` - HTML dashboard
- `GET /dashboard/multidimensional` - Multi-dimensional metrics
- `GET /api/performance/report` - Performance report JSON
- `GET /api/performance/flame-graph` - Flame graph data
- `GET /api/performance/threads` - Thread profiling data
- `GET /api/performance/threads` - Thread metrics

---

## Exchange API Endpoints Summary

### Binance Futures (3 endpoints)
- `GET /fapi/v1/klines` - Fetch OHLCV candles
- `GET /fapi/v1/ticker/24hr` - 24h ticker stats
- `GET /fapi/v1/exchangeInfo` - Exchange symbols & limits

### Gate.io Futures (9 endpoints)
**Public** (3):
- `GET /api/v4/futures/usdt/candlesticks` - Fetch OHLCV candles
- `GET /api/v4/futures/usdt/tickers` - 24h ticker stats
- `GET /api/v4/futures/usdt/symbols` - Exchange symbols

**Trading** (6, requires auth):
- `POST /api/v4/futures/usdt/orders` - Place order
- `DELETE /api/v4/futures/usdt/orders/{order_id}` - Cancel order
- `PUT /api/v4/futures/usdt/orders` - Modify order
- `GET /api/v4/futures/usdt/orders` - Get open orders
- `GET /api/v4/futures/usdt/positions` - Get positions
- `GET /api/v4/futures/usdt/accounts` - Get balance

---

## Actor-Based APIs

### API Actor Messages
**Tell (fire-and-forget)**: `FillGap`, `FetchRecent`
**Ask (request-response)**: `GetStats`, `GetDataRange`, `FetchKlines`

### WebSocket Actor Messages
**Binance**: Kline, Depth, Ticker, AggregatedTrade
**Gate.io**: Orders, Positions, Trades, Balances

### Technical Analysis Actors
**TimeFrame Actor**: Multi-timeframe OHLCV aggregation
**Indicator Actor**: EMA, Trend, Volume analysis

---

## Key Data Types

### Enumerations

**OrderSide**: `Buy | Sell`
**OrderType**: `Market | Limit | StopLoss | StopLimit | TakeProfit`
**OrderStatus**: `New | PartiallyFilled | Filled | Cancelled | Rejected`
**PositionSide**: `Long | Short | None`
**ApiError**: `Http | Parse | RateLimit | InvalidSymbol | InvalidTimeframe | Authentication | Network | Timeout | Unknown`

### Structures

**ApiRequest**: Symbol, interval, time range, limit
**OrderRequest**: Symbol, side, type, quantity, price, time-in-force, client ID, reduce-only
**Order**: ID, symbol, side, type, quantity, price, status, timestamp
**Position**: Symbol, side, size, entry price, mark price, unrealized PnL
**Balance**: Asset, available, locked, total
**FuturesOHLCVCandle**: Timestamp, OHLCV, volume, trade count

---

## Configuration Quick Reference

### Binance Config
```
URL: https://fapi.binance.com
Rate Limit: 1200 req/min (20 req/sec)
Timeout: 30s
Auth: None (public data)
```

### Gate.io Config
```
URL: https://api.gateio.ws
Rate Limit: 1000 req/min (conservative)
Timeout: 30s
Auth: HMAC-SHA256 (KEY, Timestamp, SIGN headers)
```

---

## Performance Features

- **Zero-copy parsing**: sonic-rs for JSON
- **SIMD optimizations**: Volume profile calculations
- **Rkyv serialization**: Fast binary serialization
- **Connection pooling**: Reused HTTP clients
- **Rate limiting**: Built-in delay management
- **Health monitoring**: Multi-dimensional performance tracking
- **Latency tracking**: Trading operation metrics
- **Flame graph profiling**: Performance visualization

---

## WebSocket Streams

### Binance Streams
- Real-time klines (candlesticks)
- Order book depth updates
- Price tickers
- Aggregated trades

### Gate.io Streams (with auth)
- Order updates
- Position updates
- Trade executions
- Account balance changes

---

## Error Handling

- **Recoverable errors**: Network, Timeout, Http, Unknown
- **Non-recoverable**: InvalidSymbol, InvalidTimeframe, Authentication, RateLimit
- **Automatic retry**: Respects Retry-After header
- **Health tracking**: Marks exchanges unhealthy after 5 consecutive failures
- **Fallback support**: Switches to secondary exchange if primary fails

---

## Supported Symbols

Any valid trading pair on Binance Futures or Gate.io Futures:
- Examples: BTCUSDT, ETHUSDT, BNBUSDT, ADAUSDT, etc.
- Symbol validation: Done at API request time
- Rate limiting: Per exchange, not per symbol

---

## Integration Points

### Kafka Integration (Optional)
- Publishes real-time indicators
- Triggered by technical analysis actors
- Topic structure: Symbol-based routing

### PostgreSQL Integration (Optional)
- Dual storage with LMDB
- Candle persistence
- Analytics queries

### Volume Profile Module (Optional)
- POC (Point of Control) calculation
- VWAP (Volume Weighted Average Price)
- Value Area detection
- Reprocessing capabilities

---

## Testing & Validation

### Deployment Checks
1. Memory availability validation
2. CPU resource validation
3. Database connectivity
4. Kafka connectivity (if enabled)
5. PostgreSQL connectivity (if enabled)

### Health Probes
- Liveness: Service is running
- Readiness: All dependencies ready
- Startup: Service initializing
- Custom: Resource-aware health checks

