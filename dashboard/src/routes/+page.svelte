<script lang="ts">
	import { onMount } from 'svelte';
	import { Grid, Title, Text } from '@svelteuidev/core';
	import StatusCard from '$lib/components/StatusCard.svelte';
	import { healthApi, performanceApi, binanceApi, gateioApi } from '$lib/api/client';

	let healthStatus = $state<'success' | 'error' | 'loading'>('loading');
	let readyStatus = $state<'success' | 'error' | 'loading'>('loading');
	let metricsData = $state<string>('');
	let binanceSymbols = $state<number>(0);
	let gateioSymbols = $state<number>(0);

	onMount(async () => {
		// Check health status
		const health = await healthApi.getHealth();
		healthStatus = health.status === 200 ? 'success' : 'error';

		// Check ready status
		const ready = await healthApi.getReady();
		readyStatus = ready.status === 200 ? 'success' : 'error';

		// Get metrics
		const metrics = await healthApi.getMetrics();
		if (metrics.data) {
			metricsData = metrics.data;
		}

		// Get exchange info
		const binanceInfo = await binanceApi.getExchangeInfo();
		if (binanceInfo.data?.symbols) {
			binanceSymbols = binanceInfo.data.symbols.length;
		}

		const gateioSymbolsRes = await gateioApi.getSymbols();
		if (gateioSymbolsRes.data) {
			gateioSymbols = gateioSymbolsRes.data.length;
		}
	});
</script>

<div class="page-container">
	<div class="page-header">
		<Title order={1}>Dashboard Overview</Title>
		<Text size="lg" override={{ color: '#868e96' }}>
			Monitor your Data Feeder infrastructure and exchange connections
		</Text>
	</div>

	<Grid cols={12} gutter="lg">
		<Grid.Col span={12}>
			<Title order={2}>System Health</Title>
		</Grid.Col>

		<Grid.Col span={3}>
			<StatusCard title="Service Health" status={healthStatus} description="Liveness probe" />
		</Grid.Col>

		<Grid.Col span={3}>
			<StatusCard title="Readiness" status={readyStatus} description="Service ready" />
		</Grid.Col>

		<Grid.Col span={3}>
			<StatusCard
				title="Binance Symbols"
				status="success"
				value={binanceSymbols}
				description="Available trading pairs"
			/>
		</Grid.Col>

		<Grid.Col span={3}>
			<StatusCard
				title="Gate.io Symbols"
				status="success"
				value={gateioSymbols}
				description="Available trading pairs"
			/>
		</Grid.Col>

		<Grid.Col span={12}>
			<Title order={2} override={{ marginTop: '24px' }}>Quick Links</Title>
		</Grid.Col>

		<Grid.Col span={4}>
			<a href="/strategies" class="link-card">
				<div class="link-card-content">
					<span class="link-icon">⚡</span>
					<div>
						<h3>Trading Strategies</h3>
						<p>Create and manage automated trading strategies</p>
					</div>
				</div>
			</a>
		</Grid.Col>

		<Grid.Col span={4}>
			<a href="/signals" class="link-card">
				<div class="link-card-content">
					<span class="link-icon">📡</span>
					<div>
						<h3>Trading Signals</h3>
						<p>Monitor real-time trading signals and execution</p>
					</div>
				</div>
			</a>
		</Grid.Col>

		<Grid.Col span={4}>
			<a href="/health" class="link-card">
				<div class="link-card-content">
					<span class="link-icon">❤️</span>
					<div>
						<h3>Health & Status</h3>
						<p>Monitor system health, readiness, and metrics</p>
					</div>
				</div>
			</a>
		</Grid.Col>

		<Grid.Col span={4}>
			<a href="/performance" class="link-card">
				<div class="link-card-content">
					<span class="link-icon">📊</span>
					<div>
						<h3>Performance</h3>
						<p>View performance metrics and flame graphs</p>
					</div>
				</div>
			</a>
		</Grid.Col>

		<Grid.Col span={4}>
			<a href="/binance" class="link-card">
				<div class="link-card-content">
					<span class="link-icon">🔶</span>
					<div>
						<h3>Binance Data</h3>
						<p>View Binance market data and candlesticks</p>
					</div>
				</div>
			</a>
		</Grid.Col>

		<Grid.Col span={4}>
			<a href="/gateio" class="link-card">
				<div class="link-card-content">
					<span class="link-icon">🟦</span>
					<div>
						<h3>Gate.io Data</h3>
						<p>View Gate.io market data and positions</p>
					</div>
				</div>
			</a>
		</Grid.Col>

		<Grid.Col span={4}>
			<a href="/deployment" class="link-card">
				<div class="link-card-content">
					<span class="link-icon">🚀</span>
					<div>
						<h3>Deployment</h3>
						<p>Validate deployment and check resources</p>
					</div>
				</div>
			</a>
		</Grid.Col>
	</Grid>
</div>

<style>
	.page-container {
		padding: 20px;
	}

	.page-header {
		margin-bottom: 32px;
	}

	.link-card {
		display: block;
		padding: 24px;
		background: white;
		border-radius: 8px;
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
		text-decoration: none;
		color: inherit;
		transition: all 0.2s;
	}

	.link-card:hover {
		box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
		transform: translateY(-2px);
	}

	.link-card-content {
		display: flex;
		gap: 16px;
		align-items: flex-start;
	}

	.link-icon {
		font-size: 2.5rem;
	}

	.link-card h3 {
		margin: 0 0 8px 0;
		font-size: 1.2rem;
		font-weight: 600;
		color: #228be6;
	}

	.link-card p {
		margin: 0;
		font-size: 0.9rem;
		color: #868e96;
	}
</style>
