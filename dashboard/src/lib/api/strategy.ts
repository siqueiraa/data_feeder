/**
 * Strategy Management API Client
 * Handles user authentication, strategy CRUD, and execution
 */

import type { ApiResponse } from './client';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8080';

async function apiFetch<T>(endpoint: string, options?: RequestInit): Promise<ApiResponse<T>> {
	try {
		const token = localStorage.getItem('auth_token');
		const response = await fetch(`${API_BASE_URL}${endpoint}`, {
			...options,
			headers: {
				'Content-Type': 'application/json',
				...(token && { Authorization: `Bearer ${token}` }),
				...options?.headers
			}
		});

		if (response.status === 401) {
			localStorage.removeItem('auth_token');
			window.location.href = '/login';
		}

		const data = await response.text();
		let parsedData: T;

		try {
			parsedData = JSON.parse(data) as T;
		} catch {
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
 * User Management Types
 */
export interface User {
	id: string;
	username: string;
	email: string;
	created_at: string;
	updated_at: string;
	is_active: boolean;
	role: 'admin' | 'trader' | 'viewer';
}

export interface CreateUserRequest {
	username: string;
	email: string;
	password: string;
	role?: 'admin' | 'trader' | 'viewer';
}

export interface LoginRequest {
	username: string;
	password: string;
}

export interface LoginResponse {
	token: string;
	user: User;
	expires_at: string;
}

/**
 * Strategy Types
 */
export interface Strategy {
	id: string;
	user_id: string;
	name: string;
	description: string;
	strategy_type: StrategyType;
	parameters: StrategyParameters;
	status: StrategyStatus;
	created_at: string;
	updated_at: string;
	last_executed: string | null;
	performance: StrategyPerformance | null;
}

export type StrategyType =
	| 'ema_crossover'
	| 'rsi_divergence'
	| 'macd_signal'
	| 'bollinger_bands'
	| 'volume_profile'
	| 'trend_following'
	| 'mean_reversion'
	| 'breakout'
	| 'custom';

export type StrategyStatus = 'draft' | 'active' | 'paused' | 'stopped' | 'backtesting';

export interface StrategyParameters {
	symbol: string;
	timeframe: string;
	exchange: 'binance' | 'gateio';

	// Common parameters
	stop_loss?: number;
	take_profit?: number;
	position_size?: number;
	max_positions?: number;

	// Indicator parameters
	indicators?: {
		ema_fast?: number;
		ema_slow?: number;
		rsi_period?: number;
		rsi_overbought?: number;
		rsi_oversold?: number;
		macd_fast?: number;
		macd_slow?: number;
		macd_signal?: number;
		bb_period?: number;
		bb_deviation?: number;
		volume_ma?: number;
	};

	// Entry/Exit conditions
	entry_conditions?: EntryCondition[];
	exit_conditions?: ExitCondition[];

	// Risk management
	risk_management?: {
		max_drawdown?: number;
		daily_loss_limit?: number;
		position_sizing_method?: 'fixed' | 'percent' | 'kelly' | 'risk_based';
		risk_per_trade?: number;
	};
}

export interface EntryCondition {
	type: 'indicator' | 'price' | 'volume' | 'time';
	indicator?: string;
	operator: '>' | '<' | '>=' | '<=' | '==' | 'crosses_above' | 'crosses_below';
	value: number | string;
	logic?: 'and' | 'or';
}

export interface ExitCondition {
	type: 'indicator' | 'price' | 'volume' | 'time' | 'stop_loss' | 'take_profit';
	indicator?: string;
	operator: '>' | '<' | '>=' | '<=' | '==' | 'crosses_above' | 'crosses_below';
	value: number | string;
	logic?: 'and' | 'or';
}

export interface StrategyPerformance {
	total_trades: number;
	winning_trades: number;
	losing_trades: number;
	win_rate: number;
	total_pnl: number;
	total_pnl_percent: number;
	avg_win: number;
	avg_loss: number;
	profit_factor: number;
	sharpe_ratio: number;
	max_drawdown: number;
	max_drawdown_percent: number;
}

export interface CreateStrategyRequest {
	name: string;
	description: string;
	strategy_type: StrategyType;
	parameters: StrategyParameters;
}

export interface UpdateStrategyRequest {
	name?: string;
	description?: string;
	parameters?: StrategyParameters;
	status?: StrategyStatus;
}

/**
 * Backtest Types
 */
export interface BacktestRequest {
	strategy_id: string;
	start_date: string;
	end_date: string;
	initial_capital: number;
	commission?: number;
	slippage?: number;
}

export interface BacktestResult {
	id: string;
	strategy_id: string;
	start_date: string;
	end_date: string;
	initial_capital: number;
	final_capital: number;
	performance: StrategyPerformance;
	trades: BacktestTrade[];
	equity_curve: EquityPoint[];
	created_at: string;
}

export interface BacktestTrade {
	timestamp: string;
	type: 'buy' | 'sell';
	symbol: string;
	price: number;
	quantity: number;
	pnl: number;
	pnl_percent: number;
	reason: string;
}

export interface EquityPoint {
	timestamp: string;
	equity: number;
	drawdown: number;
}

/**
 * Signal Types
 */
export interface Signal {
	id: string;
	strategy_id: string;
	timestamp: string;
	symbol: string;
	action: 'buy' | 'sell' | 'close';
	price: number;
	quantity: number;
	confidence: number;
	reason: string;
	status: 'pending' | 'executed' | 'cancelled' | 'failed';
}

/**
 * User API
 */
export const userApi = {
	register: (data: CreateUserRequest) =>
		apiFetch<User>('/api/users/register', {
			method: 'POST',
			body: JSON.stringify(data)
		}),

	login: (data: LoginRequest) =>
		apiFetch<LoginResponse>('/api/users/login', {
			method: 'POST',
			body: JSON.stringify(data)
		}),

	logout: () =>
		apiFetch<{ message: string }>('/api/users/logout', {
			method: 'POST'
		}),

	getCurrentUser: () => apiFetch<User>('/api/users/me'),

	updateUser: (id: string, data: Partial<User>) =>
		apiFetch<User>(`/api/users/${id}`, {
			method: 'PUT',
			body: JSON.stringify(data)
		}),

	deleteUser: (id: string) =>
		apiFetch<{ message: string }>(`/api/users/${id}`, {
			method: 'DELETE'
		}),

	listUsers: () => apiFetch<User[]>('/api/users')
};

/**
 * Strategy API
 */
export const strategyApi = {
	create: (data: CreateStrategyRequest) =>
		apiFetch<Strategy>('/api/strategies', {
			method: 'POST',
			body: JSON.stringify(data)
		}),

	list: (user_id?: string) => {
		const query = user_id ? `?user_id=${user_id}` : '';
		return apiFetch<Strategy[]>(`/api/strategies${query}`);
	},

	get: (id: string) => apiFetch<Strategy>(`/api/strategies/${id}`),

	update: (id: string, data: UpdateStrategyRequest) =>
		apiFetch<Strategy>(`/api/strategies/${id}`, {
			method: 'PUT',
			body: JSON.stringify(data)
		}),

	delete: (id: string) =>
		apiFetch<{ message: string }>(`/api/strategies/${id}`, {
			method: 'DELETE'
		}),

	start: (id: string) =>
		apiFetch<Strategy>(`/api/strategies/${id}/start`, {
			method: 'POST'
		}),

	stop: (id: string) =>
		apiFetch<Strategy>(`/api/strategies/${id}/stop`, {
			method: 'POST'
		}),

	pause: (id: string) =>
		apiFetch<Strategy>(`/api/strategies/${id}/pause`, {
			method: 'POST'
		}),

	clone: (id: string) =>
		apiFetch<Strategy>(`/api/strategies/${id}/clone`, {
			method: 'POST'
		})
};

/**
 * Backtest API
 */
export const backtestApi = {
	run: (data: BacktestRequest) =>
		apiFetch<BacktestResult>('/api/backtests', {
			method: 'POST',
			body: JSON.stringify(data)
		}),

	list: (strategy_id?: string) => {
		const query = strategy_id ? `?strategy_id=${strategy_id}` : '';
		return apiFetch<BacktestResult[]>(`/api/backtests${query}`);
	},

	get: (id: string) => apiFetch<BacktestResult>(`/api/backtests/${id}`),

	delete: (id: string) =>
		apiFetch<{ message: string }>(`/api/backtests/${id}`, {
			method: 'DELETE'
		})
};

/**
 * Signal API
 */
export const signalApi = {
	list: (strategy_id?: string, limit: number = 100) => {
		let query = `?limit=${limit}`;
		if (strategy_id) query += `&strategy_id=${strategy_id}`;
		return apiFetch<Signal[]>(`/api/signals${query}`);
	},

	get: (id: string) => apiFetch<Signal>(`/api/signals/${id}`),

	execute: (id: string) =>
		apiFetch<Signal>(`/api/signals/${id}/execute`, {
			method: 'POST'
		}),

	cancel: (id: string) =>
		apiFetch<Signal>(`/api/signals/${id}/cancel`, {
			method: 'POST'
		})
};

/**
 * Authentication helpers
 */
export function setAuthToken(token: string) {
	localStorage.setItem('auth_token', token);
}

export function getAuthToken(): string | null {
	return localStorage.getItem('auth_token');
}

export function clearAuthToken() {
	localStorage.removeItem('auth_token');
}

export function isAuthenticated(): boolean {
	return !!getAuthToken();
}
