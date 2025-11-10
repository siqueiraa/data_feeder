<script lang="ts">
	import { onMount } from 'svelte';
	import { Grid, Title, Text, Card, Code } from '@svelteuidev/core';
	import StatusCard from '$lib/components/StatusCard.svelte';
	import { healthApi } from '$lib/api/client';

	let healthStatus = $state<'success' | 'error' | 'loading'>('loading');
	let readyStatus = $state<'success' | 'error' | 'loading'>('loading');
	let startupStatus = $state<'success' | 'error' | 'loading'>('loading');
	let metricsData = $state<string>('');
	let lastUpdate = $state<string>('');

	async function refreshData() {
		const timestamp = new Date().toLocaleTimeString();
		lastUpdate = timestamp;

		// Check all health endpoints
		const [health, ready, startup, metrics] = await Promise.all([
			healthApi.getHealth(),
			healthApi.getReady(),
			healthApi.getStartup(),
			healthApi.getMetrics()
		]);

		healthStatus = health.status === 200 ? 'success' : 'error';
		readyStatus = ready.status === 200 ? 'success' : 'error';
		startupStatus = startup.status === 200 ? 'success' : 'error';

		if (metrics.data) {
			metricsData = metrics.data;
		}
	}

	onMount(() => {
		refreshData();
		// Auto-refresh every 5 seconds
		const interval = setInterval(refreshData, 5000);
		return () => clearInterval(interval);
	});
</script>

<div class="page-container">
	<div class="page-header">
		<div>
			<Title order={1}>Health & Status Monitoring</Title>
			<Text size="lg" override={{ color: '#868e96' }}>
				Real-time health checks and system metrics
			</Text>
		</div>
		{#if lastUpdate}
			<Text size="sm" override={{ color: '#868e96' }}>
				Last updated: {lastUpdate}
			</Text>
		{/if}
	</div>

	<Grid cols={12} gutter="lg">
		<Grid.Col span={4}>
			<StatusCard
				title="Liveness Probe"
				status={healthStatus}
				description="Service is running"
			/>
		</Grid.Col>

		<Grid.Col span={4}>
			<StatusCard
				title="Readiness Probe"
				status={readyStatus}
				description="Service is ready to accept traffic"
			/>
		</Grid.Col>

		<Grid.Col span={4}>
			<StatusCard
				title="Startup Probe"
				status={startupStatus}
				description="Service has finished starting up"
			/>
		</Grid.Col>

		{#if metricsData}
			<Grid.Col span={12}>
				<Card shadow="sm" padding="lg" radius="md">
					<Title order={3} override={{ marginBottom: '16px' }}>Prometheus Metrics</Title>
					<div class="metrics-container">
						<Code block>{metricsData}</Code>
					</div>
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

	.metrics-container {
		max-height: 600px;
		overflow-y: auto;
	}
</style>
