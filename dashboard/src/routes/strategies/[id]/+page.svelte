<script lang="ts">
	import { page } from '$app/stores';
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import {
		Grid,
		Title,
		Text,
		Card,
		Button,
		Badge,
		Tabs,
		Modal,
		TextInput,
		Alert
	} from '@svelteuidev/core';
	import {
		strategyApi,
		backtestApi,
		signalApi,
		type Strategy,
		type BacktestResult,
		type Signal
	} from '$lib/api/strategy';
	import StatusCard from '$lib/components/StatusCard.svelte';
	import MetricsChart from '$lib/components/MetricsChart.svelte';

	const strategyId = $derived($page.params.id);

	let strategy = $state<Strategy | null>(null);
	let backtestResults = $state<BacktestResult[]>([]);
	let signals = $state<Signal[]>([]);
	let loading = $state(true);
	let backtestModalOpen = $state(false);

	// Backtest form
	let backtestStartDate = $state('');
	let backtestEndDate = $state('');
	let backtestCapital = $state(10000);
	let backtestLoading = $state(false);
	let backtestError = $state<string | null>(null);

	let activeTab = $state('overview');

	onMount(async () => {
		await loadStrategy();
		await loadBacktests();
		await loadSignals();
	});

	async function loadStrategy() {
		loading = true;
		const result = await strategyApi.get(strategyId);
		if (result.data) {
			strategy = result.data;
		}
		loading = false;
	}

	async function loadBacktests() {
		const result = await backtestApi.list(strategyId);
		if (result.data) {
			backtestResults = result.data;
		}
	}

	async function loadSignals() {
		const result = await signalApi.list(strategyId, 50);
		if (result.data) {
			signals = result.data;
		}
	}

	async function runBacktest() {
		if (!backtestStartDate || !backtestEndDate) {
			backtestError = 'Please select start and end dates';
			return;
		}

		backtestLoading = true;
		backtestError = null;

		const result = await backtestApi.run({
			strategy_id: strategyId,
			start_date: backtestStartDate,
			end_date: backtestEndDate,
			initial_capital: backtestCapital,
			commission: 0.001,
			slippage: 0.0005
		});

		if (result.data) {
			backtestModalOpen = false;
			await loadBacktests();
		} else {
			backtestError = result.error || 'Backtest failed';
		}

		backtestLoading = false;
	}

	function getStatusColor(
		status: string
	): 'success' | 'error' | 'warning' | 'gray' | 'blue' | 'yellow' {
		switch (status) {
			case 'active':
				return 'success';
			case 'paused':
				return 'yellow';
			case 'stopped':
				return 'error';
			case 'draft':
				return 'gray';
			case 'backtesting':
				return 'blue';
			default:
				return 'gray';
		}
	}

	function formatDate(date: string) {
		return new Date(date).toLocaleString();
	}

	function editStrategy() {
		goto(`/strategies/${strategyId}/edit`);
	}

	async function toggleStrategy() {
		if (strategy) {
			if (strategy.status === 'active') {
				await strategyApi.pause(strategyId);
			} else {
				await strategyApi.start(strategyId);
			}
			await loadStrategy();
		}
	}

	// Chart data
	const equityChartData = $derived(
		backtestResults.length > 0 && backtestResults[0].equity_curve
			? backtestResults[0].equity_curve.map((point) => ({
					timestamp: point.timestamp,
					value: point.equity
			  }))
			: []
	);
</script>

<div class="page-container">
	{#if loading || !strategy}
		<Card shadow="sm" padding="lg" radius="md">
			<Text>Loading strategy...</Text>
		</Card>
	{:else}
		<div class="page-header">
			<div>
				<div class="title-row">
					<Title order={1}>{strategy.name}</Title>
					<Badge color={getStatusColor(strategy.status)} size="lg" variant="filled">
						{strategy.status.toUpperCase()}
					</Badge>
				</div>
				<Text size="lg" override={{ color: '#868e96' }}>
					{strategy.description}
				</Text>
			</div>
			<div class="header-actions">
				<Button onclick={editStrategy} variant="light">Edit</Button>
				<Button onclick={toggleStrategy} color={strategy.status === 'active' ? 'yellow' : 'green'}>
					{strategy.status === 'active' ? 'Pause' : 'Start'}
				</Button>
			</div>
		</div>

		<Tabs value={activeTab} onchange={(e) => (activeTab = e.detail)}>
			<Tabs.Tab value="overview">Overview</Tabs.Tab>
			<Tabs.Tab value="backtest">Backtesting</Tabs.Tab>
			<Tabs.Tab value="signals">Signals</Tabs.Tab>
			<Tabs.Tab value="settings">Settings</Tabs.Tab>
		</Tabs>

		<div class="tab-content">
			{#if activeTab === 'overview'}
				<Grid cols={12} gutter="lg">
					<!-- Performance Metrics -->
					{#if strategy.performance}
						<Grid.Col span={3}>
							<StatusCard
								title="Win Rate"
								status="success"
								value={strategy.performance.win_rate.toFixed(1) + '%'}
								description={`${strategy.performance.winning_trades}/${strategy.performance.total_trades} trades`}
							/>
						</Grid.Col>

						<Grid.Col span={3}>
							<StatusCard
								title="Total PnL"
								status={strategy.performance.total_pnl >= 0 ? 'success' : 'error'}
								value={'$' + strategy.performance.total_pnl.toFixed(2)}
								description={strategy.performance.total_pnl_percent.toFixed(2) + '% return'}
							/>
						</Grid.Col>

						<Grid.Col span={3}>
							<StatusCard
								title="Sharpe Ratio"
								status="success"
								value={strategy.performance.sharpe_ratio.toFixed(2)}
								description="Risk-adjusted return"
							/>
						</Grid.Col>

						<Grid.Col span={3}>
							<StatusCard
								title="Max Drawdown"
								status="warning"
								value={strategy.performance.max_drawdown_percent.toFixed(2) + '%'}
								description={'$' + strategy.performance.max_drawdown.toFixed(2)}
							/>
						</Grid.Col>
					{/if}

					<!-- Strategy Configuration -->
					<Grid.Col span={6}>
						<Card shadow="sm" padding="lg" radius="md">
							<Title order={3} override={{ marginBottom: '16px' }}>Configuration</Title>
							<div class="config-grid">
								<div class="config-item">
									<Text size="xs" weight="bold">Type</Text>
									<Text>{strategy.strategy_type}</Text>
								</div>
								<div class="config-item">
									<Text size="xs" weight="bold">Symbol</Text>
									<Text>{strategy.parameters.symbol}</Text>
								</div>
								<div class="config-item">
									<Text size="xs" weight="bold">Timeframe</Text>
									<Text>{strategy.parameters.timeframe}</Text>
								</div>
								<div class="config-item">
									<Text size="xs" weight="bold">Exchange</Text>
									<Text>{strategy.parameters.exchange}</Text>
								</div>
								<div class="config-item">
									<Text size="xs" weight="bold">Position Size</Text>
									<Text>${strategy.parameters.position_size}</Text>
								</div>
								<div class="config-item">
									<Text size="xs" weight="bold">Stop Loss</Text>
									<Text>{strategy.parameters.stop_loss}%</Text>
								</div>
								<div class="config-item">
									<Text size="xs" weight="bold">Take Profit</Text>
									<Text>{strategy.parameters.take_profit}%</Text>
								</div>
								<div class="config-item">
									<Text size="xs" weight="bold">Created</Text>
									<Text>{formatDate(strategy.created_at)}</Text>
								</div>
							</div>
						</Card>
					</Grid.Col>

					<!-- Indicator Settings -->
					<Grid.Col span={6}>
						<Card shadow="sm" padding="lg" radius="md">
							<Title order={3} override={{ marginBottom: '16px' }}>Indicator Settings</Title>
							{#if strategy.parameters.indicators}
								<div class="config-grid">
									{#if strategy.parameters.indicators.ema_fast}
										<div class="config-item">
											<Text size="xs" weight="bold">Fast EMA</Text>
											<Text>{strategy.parameters.indicators.ema_fast}</Text>
										</div>
									{/if}
									{#if strategy.parameters.indicators.ema_slow}
										<div class="config-item">
											<Text size="xs" weight="bold">Slow EMA</Text>
											<Text>{strategy.parameters.indicators.ema_slow}</Text>
										</div>
									{/if}
									{#if strategy.parameters.indicators.rsi_period}
										<div class="config-item">
											<Text size="xs" weight="bold">RSI Period</Text>
											<Text>{strategy.parameters.indicators.rsi_period}</Text>
										</div>
									{/if}
									{#if strategy.parameters.indicators.rsi_overbought}
										<div class="config-item">
											<Text size="xs" weight="bold">RSI Overbought</Text>
											<Text>{strategy.parameters.indicators.rsi_overbought}</Text>
										</div>
									{/if}
									{#if strategy.parameters.indicators.rsi_oversold}
										<div class="config-item">
											<Text size="xs" weight="bold">RSI Oversold</Text>
											<Text>{strategy.parameters.indicators.rsi_oversold}</Text>
										</div>
									{/if}
								</div>
							{:else}
								<Text size="sm" override={{ color: '#868e96' }}>
									No indicator settings configured
								</Text>
							{/if}
						</Card>
					</Grid.Col>
				</Grid>
			{:else if activeTab === 'backtest'}
				<div class="backtest-section">
					<div class="backtest-header">
						<Title order={3}>Backtest Results</Title>
						<Button onclick={() => (backtestModalOpen = true)}>Run New Backtest</Button>
					</div>

					{#if backtestResults.length === 0}
						<Card shadow="sm" padding="lg" radius="md">
							<div class="empty-state">
								<span class="empty-icon">📊</span>
								<Title order={4}>No Backtest Results</Title>
								<Text>Run a backtest to see how your strategy performs on historical data</Text>
							</div>
						</Card>
					{:else}
						{#each backtestResults as backtest}
							<Card shadow="sm" padding="lg" radius="md" override={{ marginBottom: '20px' }}>
								<Title order={4} override={{ marginBottom: '16px' }}>
									Backtest: {formatDate(backtest.created_at)}
								</Title>

								<Grid cols={12} gutter="md">
									<Grid.Col span={3}>
										<StatusCard
											title="Initial Capital"
											status="success"
											value={'$' + backtest.initial_capital.toFixed(2)}
										/>
									</Grid.Col>
									<Grid.Col span={3}>
										<StatusCard
											title="Final Capital"
											status={backtest.final_capital >= backtest.initial_capital
												? 'success'
												: 'error'}
											value={'$' + backtest.final_capital.toFixed(2)}
										/>
									</Grid.Col>
									<Grid.Col span={3}>
										<StatusCard
											title="Return"
											status={backtest.final_capital >= backtest.initial_capital
												? 'success'
												: 'error'}
											value={((backtest.final_capital / backtest.initial_capital - 1) * 100).toFixed(
												2
											) + '%'}
										/>
									</Grid.Col>
									<Grid.Col span={3}>
										<StatusCard
											title="Total Trades"
											status="success"
											value={backtest.performance.total_trades}
										/>
									</Grid.Col>

									{#if equityChartData.length > 0}
										<Grid.Col span={12}>
											<MetricsChart
												title="Equity Curve"
												data={equityChartData}
												color="#228be6"
												unit="$"
											/>
										</Grid.Col>
									{/if}
								</Grid>
							</Card>
						{/each}
					{/if}
				</div>
			{:else if activeTab === 'signals'}
				<Card shadow="sm" padding="lg" radius="md">
					<Title order={3} override={{ marginBottom: '16px' }}>Recent Signals</Title>

					{#if signals.length === 0}
						<div class="empty-state">
							<Text>No signals generated yet</Text>
						</div>
					{:else}
						<div class="signals-table">
							<table>
								<thead>
									<tr>
										<th>Time</th>
										<th>Action</th>
										<th>Symbol</th>
										<th>Price</th>
										<th>Quantity</th>
										<th>Confidence</th>
										<th>Status</th>
										<th>Reason</th>
									</tr>
								</thead>
								<tbody>
									{#each signals as signal}
										<tr>
											<td>{formatDate(signal.timestamp)}</td>
											<td>
												<Badge color={signal.action === 'buy' ? 'green' : 'red'}>
													{signal.action.toUpperCase()}
												</Badge>
											</td>
											<td>{signal.symbol}</td>
											<td>${signal.price.toFixed(2)}</td>
											<td>{signal.quantity}</td>
											<td>{(signal.confidence * 100).toFixed(0)}%</td>
											<td>
												<Badge
													color={signal.status === 'executed'
														? 'green'
														: signal.status === 'pending'
															? 'yellow'
															: 'gray'}
												>
													{signal.status}
												</Badge>
											</td>
											<td>{signal.reason}</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				</Card>
			{:else if activeTab === 'settings'}
				<Card shadow="sm" padding="lg" radius="md">
					<Title order={3} override={{ marginBottom: '16px' }}>Risk Management</Title>
					{#if strategy.parameters.risk_management}
						<div class="config-grid">
							<div class="config-item">
								<Text size="xs" weight="bold">Max Drawdown</Text>
								<Text>{strategy.parameters.risk_management.max_drawdown}%</Text>
							</div>
							<div class="config-item">
								<Text size="xs" weight="bold">Daily Loss Limit</Text>
								<Text>{strategy.parameters.risk_management.daily_loss_limit}%</Text>
							</div>
							<div class="config-item">
								<Text size="xs" weight="bold">Position Sizing</Text>
								<Text>{strategy.parameters.risk_management.position_sizing_method}</Text>
							</div>
							<div class="config-item">
								<Text size="xs" weight="bold">Risk Per Trade</Text>
								<Text>{strategy.parameters.risk_management.risk_per_trade}%</Text>
							</div>
						</div>
					{/if}
				</Card>
			{/if}
		</div>
	{/if}
</div>

<Modal
	opened={backtestModalOpen}
	onClose={() => (backtestModalOpen = false)}
	title="Run Backtest"
	size="lg"
>
	<form on:submit|preventDefault={runBacktest}>
		<div class="modal-form">
			<TextInput
				type="date"
				label="Start Date"
				bind:value={backtestStartDate}
				required
				size="md"
			/>

			<TextInput type="date" label="End Date" bind:value={backtestEndDate} required size="md" />

			<TextInput
				type="number"
				label="Initial Capital (USDT)"
				bind:value={backtestCapital}
				required
				size="md"
			/>

			{#if backtestError}
				<Alert title="Error" color="red">
					{backtestError}
				</Alert>
			{/if}

			<div class="modal-actions">
				<Button type="button" onclick={() => (backtestModalOpen = false)} variant="light">
					Cancel
				</Button>
				<Button type="submit" loading={backtestLoading}>
					{backtestLoading ? 'Running...' : 'Run Backtest'}
				</Button>
			</div>
		</div>
	</form>
</Modal>

<style>
	.page-container {
		padding: 20px;
	}

	.page-header {
		margin-bottom: 24px;
		display: flex;
		justify-content: space-between;
		align-items: flex-start;
	}

	.title-row {
		display: flex;
		align-items: center;
		gap: 12px;
		margin-bottom: 8px;
	}

	.header-actions {
		display: flex;
		gap: 8px;
	}

	.tab-content {
		margin-top: 24px;
	}

	.config-grid {
		display: grid;
		grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
		gap: 16px;
	}

	.config-item {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.backtest-section {
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.backtest-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
	}

	.empty-state {
		text-align: center;
		padding: 40px 20px;
	}

	.empty-icon {
		font-size: 4rem;
		display: block;
		margin-bottom: 16px;
	}

	.signals-table {
		overflow-x: auto;
	}

	table {
		width: 100%;
		border-collapse: collapse;
	}

	th,
	td {
		padding: 12px;
		text-align: left;
		border-bottom: 1px solid #e9ecef;
	}

	th {
		font-weight: 600;
		color: #495057;
		background: #f8f9fa;
	}

	tbody tr:hover {
		background: #f8f9fa;
	}

	.modal-form {
		display: flex;
		flex-direction: column;
		gap: 16px;
	}

	.modal-actions {
		display: flex;
		gap: 12px;
		justify-content: flex-end;
		margin-top: 8px;
	}
</style>
