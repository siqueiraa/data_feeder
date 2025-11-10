# Data Feeder Dashboard

A comprehensive SvelteUI-based dashboard for monitoring and interacting with the Data Feeder API. This dashboard provides real-time insights into system health, performance metrics, and cryptocurrency market data from multiple exchanges.

## Features

### 📊 Dashboard Overview
- System health status at a glance
- Exchange connection monitoring
- Quick access to all features

### ❤️ Health & Status Monitoring
- Liveness, readiness, and startup probes
- Real-time Prometheus metrics
- Auto-refresh every 5 seconds

### 📈 Performance Monitoring
- CPU and memory usage tracking
- Request rate and latency metrics (P50, P95, P99)
- Thread profiling and performance reports
- Interactive time-series charts
- Auto-refresh every 10 seconds

### 🔶 Binance Futures Data
- Real-time candlestick charts
- 24-hour ticker statistics
- Multiple timeframes (1m, 5m, 15m, 30m, 1h, 4h, 1d)
- Popular trading pairs (BTC, ETH, BNB, ADA, SOL, DOGE)

### 🟦 Gate.io Futures Data
- Market data with candlestick charts
- Trading account information (requires authentication)
- Position monitoring
- Balance tracking
- Multiple timeframes and contracts

### 🚀 Deployment Validation
- Resource validation (memory, CPU)
- Full deployment checks
- Quick deployment readiness tests
- Optional service validation (Kafka, PostgreSQL)

## Tech Stack

- **Framework**: SvelteKit 5
- **UI Library**: SvelteUI
- **Language**: TypeScript
- **Charts**: Custom Canvas-based visualizations
- **API**: REST API integration with Data Feeder backend

## Prerequisites

- Node.js 18+ and npm
- Data Feeder API server running (default: `http://localhost:8080`)

## Installation

1. Install dependencies:
```sh
npm install
```

2. Configure the API endpoint:
```sh
cp .env.example .env
# Edit .env and set VITE_API_BASE_URL to your API server URL
```

## Development

Start the development server:

```sh
npm run dev

# or open in browser automatically
npm run dev -- --open
```

The dashboard will be available at `http://localhost:5173`

## Building for Production

Create a production build:

```sh
npm run build
```

Preview the production build:

```sh
npm run preview
```

## Configuration

### Environment Variables

Create a `.env` file in the dashboard directory:

```env
VITE_API_BASE_URL=http://localhost:8080
```

Update the URL to point to your Data Feeder API server.

### API Endpoints

The dashboard consumes the following API endpoints:

**Health & Status:**
- `GET /health` - Liveness probe
- `GET /ready` - Readiness probe
- `GET /startup` - Startup probe
- `GET /metrics` - Prometheus metrics

**Performance:**
- `GET /dashboard/multidimensional` - Multi-dimensional metrics
- `GET /api/performance/report` - Performance report
- `GET /api/performance/threads` - Thread metrics

**Deployment:**
- `GET /validate` - Resource validation
- `GET /deploy/validate` - Full deployment validation
- `GET /deploy/check` - Quick deployment check

**Binance Futures:**
- `GET /fapi/v1/klines` - Candlestick data
- `GET /fapi/v1/ticker/24hr` - 24h ticker
- `GET /fapi/v1/exchangeInfo` - Exchange info

**Gate.io Futures:**
- `GET /api/v4/futures/usdt/candlesticks` - Candlestick data
- `GET /api/v4/futures/usdt/tickers` - Ticker info
- `GET /api/v4/futures/usdt/positions` - Positions
- `GET /api/v4/futures/usdt/accounts` - Balance

## Project Structure

```
dashboard/
├── src/
│   ├── lib/
│   │   ├── api/
│   │   │   └── client.ts          # API client with type definitions
│   │   └── components/
│   │       ├── StatusCard.svelte   # Status display component
│   │       ├── MetricsChart.svelte # Line chart for metrics
│   │       └── CandlestickChart.svelte # Candlestick chart
│   ├── routes/
│   │   ├── +layout.svelte          # Main layout with navigation
│   │   ├── +page.svelte            # Dashboard overview
│   │   ├── health/                 # Health monitoring page
│   │   ├── performance/            # Performance metrics page
│   │   ├── binance/                # Binance data page
│   │   ├── gateio/                 # Gate.io data page
│   │   └── deployment/             # Deployment validation page
│   └── app.html                    # HTML template
├── static/                         # Static assets
├── .env                            # Environment configuration
└── package.json                    # Dependencies

```

## Features by Page

### Overview (`/`)
- Quick system health checks
- Exchange symbol counts
- Navigation cards to all features

### Health (`/health`)
- Real-time health probe status
- Prometheus metrics display
- 5-second auto-refresh

### Performance (`/performance`)
- CPU, memory, and latency charts
- Request rate monitoring
- Thread profiling table
- 10-second auto-refresh

### Binance (`/binance`)
- Symbol selector (BTCUSDT, ETHUSDT, etc.)
- Timeframe selector (1m to 1d)
- Candlestick chart
- 24h statistics (price, volume, high/low)

### Gate.io (`/gateio`)
- Market data tab with candlestick charts
- Trading tab with positions and balance
- Contract and timeframe selectors
- Authentication required for trading data

### Deployment (`/deployment`)
- Three validation endpoints
- Resource checks (memory, CPU)
- Optional service checks (Kafka, PostgreSQL, Database)
- Visual status indicators

## Development Notes

- All API calls include error handling
- Components use Svelte 5 runes (`$state`, `$derived`, `$effect`)
- Charts are custom Canvas-based for better performance
- Auto-refresh intervals are configurable per page
- SvelteUI components provide consistent styling

## Contributing

When adding new features:

1. Add API endpoints to `src/lib/api/client.ts`
2. Create reusable components in `src/lib/components/`
3. Add new routes in `src/routes/`
4. Update navigation in `src/routes/+layout.svelte`
5. Update this README

## License

Part of the Data Feeder project. See main repository for license details.

## Support

For issues or questions:
- Check the main Data Feeder documentation
- Review API endpoints documentation
- Ensure the API server is running and accessible
