/**
 * API Client for Data Feeder API
 * Base URL can be configured via environment variable
 */

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8080';

export interface ApiResponse<T> {
	data?: T;
	error?: string;
	status: number;
}

/**
 * Generic fetch wrapper with error handling
 */
async function apiFetch<T>(endpoint: string, options?: RequestInit): Promise<ApiResponse<T>> {
	try {
		const response = await fetch(`${API_BASE_URL}${endpoint}`, {
			...options,
			headers: {
				'Content-Type': 'application/json',
				...options?.headers
			}
		});

		const data = await response.text();
		let parsedData: T;

		try {
			parsedData = JSON.parse(data) as T;
		} catch {
			// If not JSON, return as string
			parsedData = data as T;
		}

		return {
			data: parsedData,
			status: response.status
		};
	} catch (error) {
		return {
			error: error instanceof Error ? error.message : 'Unknown error',
			status: 0
		};
	}
}

/**
 * Health & Status API
 */
export const healthApi = {
	getHealth: () => apiFetch<{ status: string }>('/health'),
	getReady: () => apiFetch<{ status: string }>('/ready'),
	getStartup: () => apiFetch<{ status: string }>('/startup'),
	getMetrics: () => apiFetch<string>('/metrics')
};

/**
 * Deployment & Validation API
 */
export const deploymentApi = {
	validate: () => apiFetch<ValidationResponse>('/validate'),
	deployValidate: () => apiFetch<ValidationResponse>('/deploy/validate'),
	deployCheck: () => apiFetch<ValidationResponse>('/deploy/check')
};

export interface ValidationResponse {
	status: string;
	checks: {
		memory: boolean;
		cpu: boolean;
		database?: boolean;
		kafka?: boolean;
		postgres?: boolean;
	};
	details?: Record<string, any>;
}

/**
 * Performance & Monitoring API
 */
export const performanceApi = {
	getDashboard: () => apiFetch<string>('/dashboard'),
	getMultidimensional: () => apiFetch<MultidimensionalMetrics>('/dashboard/multidimensional'),
	getPerformanceReport: () => apiFetch<PerformanceReport>('/api/performance/report'),
	getFlameGraph: () => apiFetch<FlameGraphData>('/api/performance/flame-graph'),
	getThreads: () => apiFetch<ThreadMetrics>('/api/performance/threads')
};

export interface MultidimensionalMetrics {
	timestamp: string;
	metrics: {
		cpu_usage: number;
		memory_usage: number;
		request_rate: number;
		latency_p50: number;
		latency_p95: number;
		latency_p99: number;
	};
}

export interface PerformanceReport {
	summary: string;
	metrics: Record<string, number>;
	timestamp: string;
}

export interface FlameGraphData {
	name: string;
	value: number;
	children?: FlameGraphData[];
}

export interface ThreadMetrics {
	threads: Array<{
		id: number;
		name: string;
		cpu_time: number;
		state: string;
	}>;
}

/**
 * Exchange Data API (Binance)
 */
export const binanceApi = {
	getKlines: (params: KlinesParams) =>
		apiFetch<Candle[]>(`/fapi/v1/klines?${new URLSearchParams(params as any).toString()}`),
	getTicker24hr: (symbol?: string) =>
		apiFetch<Ticker24hr | Ticker24hr[]>(
			`/fapi/v1/ticker/24hr${symbol ? `?symbol=${symbol}` : ''}`
		),
	getExchangeInfo: () => apiFetch<ExchangeInfo>('/fapi/v1/exchangeInfo')
};

/**
 * Exchange Data API (Gate.io)
 */
export const gateioApi = {
	getCandlesticks: (params: GateioKlinesParams) =>
		apiFetch<Candle[]>(
			`/api/v4/futures/usdt/candlesticks?${new URLSearchParams(params as any).toString()}`
		),
	getTickers: (contract?: string) =>
		apiFetch<GateioTicker[]>(
			`/api/v4/futures/usdt/tickers${contract ? `?contract=${contract}` : ''}`
		),
	getSymbols: () => apiFetch<GateioSymbol[]>('/api/v4/futures/usdt/symbols'),

	// Trading endpoints (require authentication)
	placeOrder: (order: OrderRequest) =>
		apiFetch<Order>('/api/v4/futures/usdt/orders', {
			method: 'POST',
			body: JSON.stringify(order)
		}),
	cancelOrder: (orderId: string) =>
		apiFetch<Order>(`/api/v4/futures/usdt/orders/${orderId}`, {
			method: 'DELETE'
		}),
	modifyOrder: (order: Partial<OrderRequest>) =>
		apiFetch<Order>('/api/v4/futures/usdt/orders', {
			method: 'PUT',
			body: JSON.stringify(order)
		}),
	getOrders: (contract: string, status?: string) =>
		apiFetch<Order[]>(
			`/api/v4/futures/usdt/orders?contract=${contract}${status ? `&status=${status}` : ''}`
		),
	getPositions: (contract?: string) =>
		apiFetch<Position[]>(
			`/api/v4/futures/usdt/positions${contract ? `?contract=${contract}` : ''}`
		),
	getBalance: () => apiFetch<Balance>('/api/v4/futures/usdt/accounts')
};

// Type definitions for Exchange APIs
export interface KlinesParams {
	symbol: string;
	interval: string;
	startTime?: number;
	endTime?: number;
	limit?: number;
}

export interface GateioKlinesParams {
	contract: string;
	interval: string;
	from?: number;
	to?: number;
	limit?: number;
}

export interface Candle {
	timestamp: number;
	open: number;
	high: number;
	low: number;
	close: number;
	volume: number;
	trades?: number;
}

export interface Ticker24hr {
	symbol: string;
	priceChange: string;
	priceChangePercent: string;
	lastPrice: string;
	volume: string;
	quoteVolume: string;
	highPrice: string;
	lowPrice: string;
}

export interface GateioTicker {
	contract: string;
	last: string;
	change_percentage: string;
	total_size: string;
	volume_24h: string;
	high_24h: string;
	low_24h: string;
}

export interface ExchangeInfo {
	symbols: Array<{
		symbol: string;
		status: string;
		baseAsset: string;
		quoteAsset: string;
	}>;
}

export interface GateioSymbol {
	name: string;
	type: string;
	quanto_multiplier: string;
	leverage_min: string;
	leverage_max: string;
}

export interface OrderRequest {
	contract: string;
	size: number;
	price?: string;
	tif?: string;
	text?: string;
	reduce_only?: boolean;
}

export interface Order {
	id: string;
	contract: string;
	size: number;
	price: string;
	status: string;
	create_time: number;
	finish_time?: number;
}

export interface Position {
	contract: string;
	size: number;
	entry_price: string;
	mark_price: string;
	unrealised_pnl: string;
	realised_pnl: string;
}

export interface Balance {
	total: string;
	available: string;
	position_margin: string;
	order_margin: string;
}
