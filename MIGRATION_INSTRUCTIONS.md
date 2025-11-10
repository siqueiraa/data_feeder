# Migration Instructions: Moving Dashboard to rust_bot

## Overview
This document provides instructions for moving the complete SvelteUI dashboard from `data_feeder` to `rust_bot` repository.

## What's Being Moved

### 1. Dashboard Application (`dashboard/`)
Complete SvelteKit 5 application with:
- User authentication (login/register)
- Trading strategy management
- Strategy creation and configuration
- Backtesting interface
- Signal monitoring
- Health & performance monitoring
- Exchange data visualization (Binance & Gate.io)
- Deployment validation

### 2. API Documentation
- `API_ENDPOINTS.md` - Detailed endpoint specifications
- `API_QUICK_REFERENCE.md` - Quick lookup guide
- `API_ANALYSIS_SUMMARY.txt` - Comprehensive API analysis

## Migration Steps

### Step 1: Copy Files from data_feeder

On your local machine:

```bash
# Navigate to data_feeder directory
cd /Users/rafael.siqueira/dev/personal/data_feeder

# Copy dashboard folder to rust_bot
cp -r dashboard/ ../rust_bot/

# Copy API documentation
cp API_ENDPOINTS.md ../rust_bot/
cp API_QUICK_REFERENCE.md ../rust_bot/
cp API_ANALYSIS_SUMMARY.txt ../rust_bot/
```

### Step 2: Update API Base URL

The dashboard is currently configured to call APIs at `http://localhost:8080` by default.

Update `.env` in rust_bot dashboard:
```bash
cd ../rust_bot/dashboard
cp .env.example .env

# Edit .env
VITE_API_BASE_URL=http://localhost:9876  # or your rust_bot API port
```

### Step 3: Install Dependencies

```bash
cd /Users/rafael.siqueira/dev/personal/rust_bot/dashboard
npm install
```

### Step 4: Review API Integration

The dashboard expects these endpoints (update to match your rust_bot implementation):

**Authentication:**
- `POST /auth/register` - User registration
- `POST /auth/login` - User login
- `POST /auth/logout` - User logout
- `GET /api/users/me` - Get current user

**Strategies:**
- `POST /api/strategies` - Create strategy
- `GET /api/strategies` - List strategies
- `GET /api/strategies/:id` - Get strategy details
- `PUT /api/strategies/:id` - Update strategy
- `DELETE /api/strategies/:id` - Delete strategy
- `POST /api/strategies/:id/start` - Start strategy
- `POST /api/strategies/:id/pause` - Pause strategy
- `POST /api/strategies/:id/stop` - Stop strategy

**Backtesting:**
- `POST /api/backtests` - Run backtest
- `GET /api/backtests` - List backtests
- `GET /api/backtests/:id` - Get backtest details

**Signals:**
- `GET /api/signals` - List signals
- `GET /api/signals/:id` - Get signal details
- `POST /api/signals/:id/execute` - Execute signal
- `POST /api/signals/:id/cancel` - Cancel signal

**Health & Monitoring (existing in your code):**
- `GET /health`
- `GET /ready`
- `GET /startup`
- `GET /metrics`
- `GET /validate`
- `GET /deploy/validate`
- `GET /deploy/check`

**Exchange Data (existing in your code):**
- Binance endpoints
- Gate.io endpoints
- Trading operations

### Step 5: Update API Client

The API client is located at `dashboard/src/lib/api/strategy.ts`. Review and update to match your actual backend implementation.

Key things to check:
1. **Endpoint paths** - Ensure they match your rust_bot routes
2. **Request/Response types** - Verify data structures match your backend
3. **Authentication** - JWT token handling (Bearer token in headers)
4. **Error handling** - Make sure error responses are handled correctly

### Step 6: Test the Dashboard

```bash
# Start your rust_bot backend
cd /Users/rafael.siqueira/dev/personal/rust_bot
cargo run

# In another terminal, start the dashboard
cd dashboard
npm run dev
```

Visit `http://localhost:5173`

### Step 7: Commit to rust_bot

```bash
cd /Users/rafael.siqueira/dev/personal/rust_bot
git add dashboard/ API_*.md API_*.txt
git commit -m "Add comprehensive SvelteUI dashboard with strategy management

Features:
- User authentication (login/register)
- Trading strategy management (CRUD operations)
- 9 strategy types with configurable parameters
- Backtesting engine with performance metrics
- Real-time signal monitoring
- Health and performance monitoring
- Exchange data visualization (Binance & Gate.io)
- Deployment validation

Technical implementation:
- SvelteKit 5 with TypeScript
- SvelteUI components
- JWT-based authentication
- Type-safe API client
- Real-time updates with auto-refresh
- Canvas-based chart visualizations"

git push origin <your-branch>
```

## Files Structure After Migration

```
rust_bot/
├── src/                        # Your existing Rust backend
│   ├── api/
│   │   └── auth_hyper.rs      # Your auth implementation
│   ├── user_management/        # User management module
│   ├── strategies/             # Strategy management
│   └── ...
├── dashboard/                  # NEW: SvelteUI dashboard
│   ├── src/
│   │   ├── lib/
│   │   │   ├── api/
│   │   │   │   ├── client.ts       # Exchange/health APIs
│   │   │   │   └── strategy.ts     # Strategy/auth APIs
│   │   │   └── components/
│   │   │       ├── StatusCard.svelte
│   │   │       ├── MetricsChart.svelte
│   │   │       └── CandlestickChart.svelte
│   │   └── routes/
│   │       ├── +layout.svelte      # Main layout with navigation
│   │       ├── +page.svelte        # Dashboard overview
│   │       ├── login/              # Login page
│   │       ├── register/           # Registration page
│   │       ├── strategies/         # Strategy management
│   │       │   ├── +page.svelte        # List strategies
│   │       │   ├── new/                # Create strategy
│   │       │   └── [id]/               # Strategy detail/backtest
│   │       ├── signals/            # Signal monitoring
│   │       ├── health/             # Health monitoring
│   │       ├── performance/        # Performance metrics
│   │       ├── binance/            # Binance data
│   │       ├── gateio/             # Gate.io data
│   │       └── deployment/         # Deployment validation
│   ├── static/
│   ├── package.json
│   ├── .env.example
│   ├── README.md
│   └── STRATEGY_USER_FLOW.md
├── API_ENDPOINTS.md            # NEW: API documentation
├── API_QUICK_REFERENCE.md      # NEW: Quick reference
├── API_ANALYSIS_SUMMARY.txt    # NEW: Comprehensive analysis
├── Cargo.toml
└── README.md
```

## Integration Checklist

- [ ] Copy dashboard folder to rust_bot
- [ ] Copy API documentation files
- [ ] Update .env with correct API_BASE_URL
- [ ] Install npm dependencies
- [ ] Review API endpoints and update if needed
- [ ] Update TypeScript types to match backend
- [ ] Test authentication flow
- [ ] Test strategy creation and management
- [ ] Test backtesting functionality
- [ ] Test signal monitoring
- [ ] Test exchange data visualization
- [ ] Update rust_bot README with dashboard instructions
- [ ] Commit and push to rust_bot repository

## Next Steps: Backend Integration

Since your rust_bot already has auth_hyper.rs with user authentication, you'll need to:

1. **Map existing endpoints** - Document what endpoints you already have
2. **Implement missing endpoints** - Add strategy, backtest, and signal endpoints
3. **Update types** - Ensure backend types match the TypeScript interfaces
4. **Test integration** - Verify frontend works with backend

## Support

See `dashboard/STRATEGY_USER_FLOW.md` for complete user flow documentation and `dashboard/README.md` for technical details.
