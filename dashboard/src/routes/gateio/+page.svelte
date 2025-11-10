<script lang="ts">
	import { onMount } from 'svelte';
	import { Grid, Title, Text, Card, Button, Badge, Tabs } from '@svelteuidev/core';
	import StatusCard from '$lib/components/StatusCard.svelte';
	import CandlestickChart from '$lib/components/CandlestickChart.svelte';
	import { gateioApi } from '$lib/api/client';
	import type { Candle, GateioTicker, Position, Balance } from '$lib/api/client';

	let selectedContract = $state('BTC_USDT');
	let selectedInterval = $state('15m');
	let candles = $state<Candle[]>([]);
	let tickers = $state<GateioTicker[]>([]);
	let positions = $state<Position[]>([]);
	let balance = $state<Balance | null>(null);
	let loading = $state(false);
	let activeTab = $state('market');

	const contracts = ['BTC_USDT', 'ETH_USDT', 'BNB_USDT', 'ADA_USDT', 'SOL_USDT'];
	const intervals = ['1m', '5m', '15m', '30m', '1h', '4h', '1d'];

	async function fetchMarketData() {
		loading = true;

		try {
			const [candlesRes, tickersRes] = await Promise.all([
				gateioApi.getCandlesticks({
					contract: selectedContract,
					interval: selectedInterval,
					limit: 100
				}),
				gateioApi.getTickers(selectedContract)
			]);

			if (candlesRes.data && Array.isArray(candlesRes.data)) {
				candles = candlesRes.data.map((k: any) => ({
					timestamp: k.t * 1000,
					open: parseFloat(k.o),
					high: parseFloat(k.h),
					low: parseFloat(k.l),
					close: parseFloat(k.c),
					volume: parseFloat(k.v)
				}));
			}

			if (tickersRes.data && Array.isArray(tickersRes.data)) {
				tickers = tickersRes.data;
			}
		} catch (error) {
			console.error('Error fetching Gate.io market data:', error);
		} finally {
			loading = false;
		}
	}

	async function fetchTradingData() {
		loading = true;

		try {
			const [positionsRes, balanceRes] = await Promise.all([
				gateioApi.getPositions(),
				gateioApi.getBalance()
			]);

			if (positionsRes.data && Array.isArray(positionsRes.data)) {
				positions = positionsRes.data;
			}

			if (balanceRes.data) {
				balance = balanceRes.data;
			}
		} catch (error) {
			console.error('Error fetching Gate.io trading data:', error);
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		fetchMarketData();
	});

	function handleContractChange(event: Event) {
		selectedContract = (event.target as HTMLSelectElement).value;
		fetchMarketData();
	}

	function handleIntervalChange(event: Event) {
		selectedInterval = (event.target as HTMLSelectElement).value;
		fetchMarketData();
	}

	function handleTabChange(value: string) {
		activeTab = value;
		if (value === 'trading' && positions.length === 0) {
			fetchTradingData();
		}
	}

	const currentTicker = $derived(
		tickers.find((t) => t.contract === selectedContract) || tickers[0]
	);
</script>

<div class="page-container">
	<div class="page-header">
		<div>
			<Title order={1}>Gate.io Futures Data</Title>
			<Text size="lg" override={{ color: '#868e96' }}>
				Real-time market data and trading info from Gate.io Futures
			</Text>
		</div>
		<Badge color="blue" variant="filled" size="lg">
			GATE.IO
		</Badge>
	</div>

	<Card shadow="sm" padding="lg" radius="md" override={{ marginBottom: '24px' }}>
		<Tabs value={activeTab} onchange={(e) => handleTabChange(e.detail)}>
			<Tabs.Tab value="market">Market Data</Tabs.Tab>
			<Tabs.Tab value="trading">Trading (Auth Required)</Tabs.Tab>
		</Tabs>
	</Card>

	{#if activeTab === 'market'}
		<Grid cols={12} gutter="lg">
			<Grid.Col span={12}>
				<Card shadow="sm" padding="lg" radius="md">
					<div class="controls">
						<div class="control-group">
							<label for="contract">Contract</label>
							<select id="contract" bind:value={selectedContract} onchange={handleContractChange}>
								{#each contracts as contract}
									<option value={contract}>{contract}</option>
								{/each}
							</select>
						</div>

						<div class="control-group">
							<label for="interval">Interval</label>
							<select id="interval" bind:value={selectedInterval} onchange={handleIntervalChange}>
								{#each intervals as interval}
									<option value={interval}>{interval}</option>
								{/each}
							</select>
						</div>

						<Button onclick={fetchMarketData} loading={loading}>
							{loading ? 'Loading...' : 'Refresh'}
						</Button>
					</div>
				</Card>
			</Grid.Col>

			{#if currentTicker}
				<Grid.Col span={3}>
					<StatusCard
						title="Last Price"
						status="success"
						value={'$' + parseFloat(currentTicker.last).toFixed(2)}
						description={selectedContract}
					/>
				</Grid.Col>

				<Grid.Col span={3}>
					<StatusCard
						title="24h Change"
						status={parseFloat(currentTicker.change_percentage) >= 0 ? 'success' : 'error'}
						value={currentTicker.change_percentage + '%'}
						description="Price change"
					/>
				</Grid.Col>

				<Grid.Col span={3}>
					<StatusCard
						title="24h Volume"
						status="success"
						value={parseFloat(currentTicker.volume_24h).toFixed(0)}
						description="24h volume (USDT)"
					/>
				</Grid.Col>

				<Grid.Col span={3}>
					<StatusCard
						title="24h High/Low"
						status="success"
						value={'$' +
							parseFloat(currentTicker.high_24h).toFixed(2) +
							' / $' +
							parseFloat(currentTicker.low_24h).toFixed(2)}
						description="24h range"
					/>
				</Grid.Col>
			{/if}

			{#if candles.length > 0}
				<Grid.Col span={12}>
					<CandlestickChart title="Price Chart" data={candles} symbol={selectedContract} />
				</Grid.Col>
			{/if}
		</Grid>
	{:else}
		<Grid cols={12} gutter="lg">
			<Grid.Col span={12}>
				<div class="action-buttons">
					<Button onclick={fetchTradingData} loading={loading}>
						{loading ? 'Loading...' : 'Refresh Trading Data'}
					</Button>
				</div>
			</Grid.Col>

			{#if balance}
				<Grid.Col span={4}>
					<StatusCard
						title="Total Balance"
						status="success"
						value={'$' + parseFloat(balance.total).toFixed(2)}
						description="Total account balance"
					/>
				</Grid.Col>

				<Grid.Col span={4}>
					<StatusCard
						title="Available"
						status="success"
						value={'$' + parseFloat(balance.available).toFixed(2)}
						description="Available for trading"
					/>
				</Grid.Col>

				<Grid.Col span={4}>
					<StatusCard
						title="Position Margin"
						status="success"
						value={'$' + parseFloat(balance.position_margin).toFixed(2)}
						description="In open positions"
					/>
				</Grid.Col>
			{/if}

			{#if positions.length > 0}
				<Grid.Col span={12}>
					<Card shadow="sm" padding="lg" radius="md">
						<Title order={3} override={{ marginBottom: '16px' }}>Open Positions</Title>
						<div class="positions-table">
							<table>
								<thead>
									<tr>
										<th>Contract</th>
										<th>Size</th>
										<th>Entry Price</th>
										<th>Mark Price</th>
										<th>Unrealized PnL</th>
										<th>Realized PnL</th>
									</tr>
								</thead>
								<tbody>
									{#each positions as position}
										<tr>
											<td>{position.contract}</td>
											<td>{position.size}</td>
											<td>${parseFloat(position.entry_price).toFixed(2)}</td>
											<td>${parseFloat(position.mark_price).toFixed(2)}</td>
											<td
												class:positive={parseFloat(position.unrealised_pnl) >= 0}
												class:negative={parseFloat(position.unrealised_pnl) < 0}
											>
												${parseFloat(position.unrealised_pnl).toFixed(2)}
											</td>
											<td
												class:positive={parseFloat(position.realised_pnl) >= 0}
												class:negative={parseFloat(position.realised_pnl) < 0}
											>
												${parseFloat(position.realised_pnl).toFixed(2)}
											</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</Card>
				</Grid.Col>
			{:else if !loading && activeTab === 'trading'}
				<Grid.Col span={12}>
					<Card shadow="sm" padding="lg" radius="md">
						<Text align="center" override={{ color: '#868e96' }}>
							No open positions or authentication required. Make sure your API keys are configured.
						</Text>
					</Card>
				</Grid.Col>
			{/if}
		</Grid>
	{/if}
</div>

<style>
	.page-container {
		padding: 20px;
	}

	.page-header {
		margin-bottom: 32px;
		display: flex;
		justify-content: space-between;
		align-items: flex-start;
	}

	.controls {
		display: flex;
		gap: 16px;
		align-items: flex-end;
	}

	.control-group {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	label {
		font-size: 0.9rem;
		font-weight: 600;
		color: #495057;
	}

	select {
		padding: 8px 12px;
		border: 1px solid #ced4da;
		border-radius: 4px;
		background: white;
		font-size: 0.9rem;
		min-width: 150px;
	}

	select:focus {
		outline: none;
		border-color: #228be6;
	}

	.action-buttons {
		display: flex;
		justify-content: flex-end;
	}

	.positions-table {
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

	.positive {
		color: #37b24d;
		font-weight: 600;
	}

	.negative {
		color: #f03e3e;
		font-weight: 600;
	}
</style>
