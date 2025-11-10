<script lang="ts">
	import { onMount } from 'svelte';
	import { Grid, Title, Text, Card, Tabs, Badge } from '@svelteuidev/core';
	import StatusCard from '$lib/components/StatusCard.svelte';
	import MetricsChart from '$lib/components/MetricsChart.svelte';
	import { performanceApi } from '$lib/api/client';
	import type { MultidimensionalMetrics, PerformanceReport, ThreadMetrics } from '$lib/api/client';

	let metricsHistory = $state<MultidimensionalMetrics[]>([]);
	let report = $state<PerformanceReport | null>(null);
	let threads = $state<ThreadMetrics | null>(null);
	let lastUpdate = $state<string>('');

	async function refreshData() {
		const timestamp = new Date().toLocaleTimeString();
		lastUpdate = timestamp;

		const [metricsRes, reportRes, threadsRes] = await Promise.all([
			performanceApi.getMultidimensional(),
			performanceApi.getPerformanceReport(),
			performanceApi.getThreads()
		]);

		if (metricsRes.data) {
			metricsHistory = [...metricsHistory, metricsRes.data].slice(-30); // Keep last 30 data points
		}

		if (reportRes.data) {
			report = reportRes.data;
		}

		if (threadsRes.data) {
			threads = threadsRes.data;
		}
	}

	onMount(() => {
		refreshData();
		const interval = setInterval(refreshData, 10000); // Update every 10 seconds
		return () => clearInterval(interval);
	});

	// Transform data for charts
	$effect(() => {
		if (metricsHistory.length > 0) {
			cpuData = metricsHistory.map((m) => ({
				timestamp: m.timestamp,
				value: m.metrics.cpu_usage
			}));

			memoryData = metricsHistory.map((m) => ({
				timestamp: m.timestamp,
				value: m.metrics.memory_usage
			}));

			latencyData = metricsHistory.map((m) => ({
				timestamp: m.timestamp,
				value: m.metrics.latency_p95
			}));
		}
	});

	let cpuData = $state<Array<{ timestamp: string; value: number }>>([]);
	let memoryData = $state<Array<{ timestamp: string; value: number }>>([]);
	let latencyData = $state<Array<{ timestamp: string; value: number }>>([]);

	const currentMetrics = $derived(metricsHistory[metricsHistory.length - 1]);
</script>

<div class="page-container">
	<div class="page-header">
		<div>
			<Title order={1}>Performance Monitoring</Title>
			<Text size="lg" override={{ color: '#868e96' }}>
				System performance metrics and profiling data
			</Text>
		</div>
		{#if lastUpdate}
			<Text size="sm" override={{ color: '#868e96' }}>
				Last updated: {lastUpdate}
			</Text>
		{/if}
	</div>

	<Grid cols={12} gutter="lg">
		{#if currentMetrics}
			<Grid.Col span={3}>
				<StatusCard
					title="CPU Usage"
					status="success"
					value={currentMetrics.metrics.cpu_usage.toFixed(1) + '%'}
					description="Current CPU utilization"
				/>
			</Grid.Col>

			<Grid.Col span={3}>
				<StatusCard
					title="Memory Usage"
					status="success"
					value={currentMetrics.metrics.memory_usage.toFixed(1) + '%'}
					description="Current memory usage"
				/>
			</Grid.Col>

			<Grid.Col span={3}>
				<StatusCard
					title="Request Rate"
					status="success"
					value={currentMetrics.metrics.request_rate.toFixed(0) + '/s'}
					description="Requests per second"
				/>
			</Grid.Col>

			<Grid.Col span={3}>
				<StatusCard
					title="P95 Latency"
					status="success"
					value={currentMetrics.metrics.latency_p95.toFixed(2) + 'ms'}
					description="95th percentile latency"
				/>
			</Grid.Col>
		{/if}

		<Grid.Col span={12}>
			<Title order={2} override={{ marginTop: '24px', marginBottom: '16px' }}>
				Performance Trends
			</Title>
		</Grid.Col>

		<Grid.Col span={12}>
			<MetricsChart title="CPU Usage Over Time" data={cpuData} color="#f03e3e" unit="%" />
		</Grid.Col>

		<Grid.Col span={12}>
			<MetricsChart title="Memory Usage Over Time" data={memoryData} color="#228be6" unit="%" />
		</Grid.Col>

		<Grid.Col span={12}>
			<MetricsChart title="P95 Latency Over Time" data={latencyData} color="#7950f2" unit="ms" />
		</Grid.Col>

		{#if threads && threads.threads}
			<Grid.Col span={12}>
				<Card shadow="sm" padding="lg" radius="md">
					<Title order={3} override={{ marginBottom: '16px' }}>Thread Metrics</Title>
					<div class="threads-table">
						<table>
							<thead>
								<tr>
									<th>Thread ID</th>
									<th>Name</th>
									<th>State</th>
									<th>CPU Time</th>
								</tr>
							</thead>
							<tbody>
								{#each threads.threads.slice(0, 10) as thread}
									<tr>
										<td>{thread.id}</td>
										<td>{thread.name}</td>
										<td>
											<Badge color="green" variant="filled">{thread.state}</Badge>
										</td>
										<td>{thread.cpu_time.toFixed(2)}ms</td>
									</tr>
								{/each}
							</tbody>
						</table>
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

	.threads-table {
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
</style>
