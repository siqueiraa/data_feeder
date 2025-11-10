<script lang="ts">
	import { onMount } from 'svelte';
	import { Grid, Title, Text, Card, Select, Button, Badge, TextInput } from '@svelteuidev/core';
	import StatusCard from '$lib/components/StatusCard.svelte';
	import CandlestickChart from '$lib/components/CandlestickChart.svelte';
	import { binanceApi } from '$lib/api/client';
	import type { Candle, Ticker24hr } from '$lib/api/client';

	let selectedSymbol = $state('BTCUSDT');
	let selectedInterval = $state('15m');
	let candles = $state<Candle[]>([]);
	let ticker = $state<Ticker24hr | null>(null);
	let loading = $state(false);

	const symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT', 'DOGEUSDT'];
	const intervals = ['1m', '5m', '15m', '30m', '1h', '4h', '1d'];

	async function fetchData() {
		loading = true;

		try {
			const [candlesRes, tickerRes] = await Promise.all([
				binanceApi.getKlines({
					symbol: selectedSymbol,
					interval: selectedInterval,
					limit: 100
				}),
				binanceApi.getTicker24hr(selectedSymbol)
			]);

			if (candlesRes.data && Array.isArray(candlesRes.data)) {
				// Transform Binance kline data to Candle format
				candles = candlesRes.data.map((k: any) => ({
					timestamp: k[0],
					open: parseFloat(k[1]),
					high: parseFloat(k[2]),
					low: parseFloat(k[3]),
					close: parseFloat(k[4]),
					volume: parseFloat(k[5]),
					trades: k[8]
				}));
			}

			if (tickerRes.data && !Array.isArray(tickerRes.data)) {
				ticker = tickerRes.data;
			}
		} catch (error) {
			console.error('Error fetching Binance data:', error);
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		fetchData();
	});

	function handleSymbolChange(event: Event) {
		selectedSymbol = (event.target as HTMLSelectElement).value;
		fetchData();
	}

	function handleIntervalChange(event: Event) {
		selectedInterval = (event.target as HTMLSelectElement).value;
		fetchData();
	}
</script>

<div class="page-container">
	<div class="page-header">
		<div>
			<Title order={1}>Binance Futures Data</Title>
			<Text size="lg" override={{ color: '#868e96' }}>
				Real-time market data from Binance Futures
			</Text>
		</div>
		<Badge color="orange" variant="filled" size="lg">
			BINANCE
		</Badge>
	</div>

	<Grid cols={12} gutter="lg">
		<Grid.Col span={12}>
			<Card shadow="sm" padding="lg" radius="md">
				<div class="controls">
					<div class="control-group">
						<label for="symbol">Symbol</label>
						<select id="symbol" bind:value={selectedSymbol} onchange={handleSymbolChange}>
							{#each symbols as symbol}
								<option value={symbol}>{symbol}</option>
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

					<Button onclick={fetchData} loading={loading}>
						{loading ? 'Loading...' : 'Refresh'}
					</Button>
				</div>
			</Card>
		</Grid.Col>

		{#if ticker}
			<Grid.Col span={3}>
				<StatusCard
					title="Last Price"
					status="success"
					value={'$' + parseFloat(ticker.lastPrice).toFixed(2)}
					description={selectedSymbol}
				/>
			</Grid.Col>

			<Grid.Col span={3}>
				<StatusCard
					title="24h Change"
					status={parseFloat(ticker.priceChangePercent) >= 0 ? 'success' : 'error'}
					value={ticker.priceChangePercent + '%'}
					description="Price change"
				/>
			</Grid.Col>

			<Grid.Col span={3}>
				<StatusCard
					title="24h Volume"
					status="success"
					value={parseFloat(ticker.volume).toFixed(0)}
					description="Volume (base)"
				/>
			</Grid.Col>

			<Grid.Col span={3}>
				<StatusCard
					title="24h High/Low"
					status="success"
					value={'$' +
						parseFloat(ticker.highPrice).toFixed(2) +
						' / $' +
						parseFloat(ticker.lowPrice).toFixed(2)}
					description="24h range"
				/>
			</Grid.Col>
		{/if}

		{#if candles.length > 0}
			<Grid.Col span={12}>
				<CandlestickChart title="Price Chart" data={candles} symbol={selectedSymbol} />
			</Grid.Col>
		{/if}

		{#if !loading && candles.length === 0}
			<Grid.Col span={12}>
				<Card shadow="sm" padding="lg" radius="md">
					<Text align="center" override={{ color: '#868e96' }}>
						No data available. Click Refresh to load data.
					</Text>
				</Card>
			</Grid.Col>
		{/if}
	</Grid>
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
</style>
