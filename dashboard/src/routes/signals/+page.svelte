<script lang="ts">
	import { onMount } from 'svelte';
	import { Grid, Title, Text, Card, Badge, Button, Select } from '@svelteuidev/core';
	import { signalApi, strategyApi, type Signal, type Strategy } from '$lib/api/strategy';
	import StatusCard from '$lib/components/StatusCard.svelte';

	let signals = $state<Signal[]>([]);
	let strategies = $state<Strategy[]>([]);
	let selectedStrategy = $state<string>('all');
	let loading = $state(true);
	let lastUpdate = $state<string>('');

	onMount(async () => {
		await loadStrategies();
		await loadSignals();
		// Auto-refresh every 10 seconds
		const interval = setInterval(loadSignals, 10000);
		return () => clearInterval(interval);
	});

	async function loadStrategies() {
		const result = await strategyApi.list();
		if (result.data) {
			strategies = result.data;
		}
	}

	async function loadSignals() {
		const strategyFilter = selectedStrategy === 'all' ? undefined : selectedStrategy;
		const result = await signalApi.list(strategyFilter, 100);

		if (result.data) {
			signals = result.data;
			lastUpdate = new Date().toLocaleTimeString();
		}

		loading = false;
	}

	async function executeSignal(signalId: string) {
		await signalApi.execute(signalId);
		await loadSignals();
	}

	async function cancelSignal(signalId: string) {
		await signalApi.cancel(signalId);
		await loadSignals();
	}

	function formatDate(date: string) {
		return new Date(date).toLocaleString();
	}

	function handleStrategyChange(event: Event) {
		selectedStrategy = (event.target as HTMLSelectElement).value;
		loadSignals();
	}

	const pendingCount = $derived(signals.filter((s) => s.status === 'pending').length);
	const executedCount = $derived(signals.filter((s) => s.status === 'executed').length);
	const failedCount = $derived(signals.filter((s) => s.status === 'failed').length);
</script>

<div class="page-container">
	<div class="page-header">
		<div>
			<Title order={1}>Trading Signals</Title>
			<Text size="lg" override={{ color: '#868e96' }}>
				Monitor and manage trading signals from your strategies
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
				title="Pending Signals"
				status="warning"
				value={pendingCount}
				description="Awaiting execution"
			/>
		</Grid.Col>

		<Grid.Col span={4}>
			<StatusCard
				title="Executed Signals"
				status="success"
				value={executedCount}
				description="Successfully executed"
			/>
		</Grid.Col>

		<Grid.Col span={4}>
			<StatusCard
				title="Failed Signals"
				status="error"
				value={failedCount}
				description="Execution failed"
			/>
		</Grid.Col>

		<Grid.Col span={12}>
			<Card shadow="sm" padding="lg" radius="md">
				<div class="filters">
					<Title order={3}>Signal History</Title>
					<div class="filter-controls">
						<div class="form-field">
							<label for="strategy-filter">Filter by Strategy</label>
							<select
								id="strategy-filter"
								bind:value={selectedStrategy}
								onchange={handleStrategyChange}
							>
								<option value="all">All Strategies</option>
								{#each strategies as strategy}
									<option value={strategy.id}>{strategy.name}</option>
								{/each}
							</select>
						</div>
						<Button onclick={loadSignals}>Refresh</Button>
					</div>
				</div>
			</Card>
		</Grid.Col>

		<Grid.Col span={12}>
			<Card shadow="sm" padding="lg" radius="md">
				{#if loading}
					<Text align="center">Loading signals...</Text>
				{:else if signals.length === 0}
					<div class="empty-state">
						<span class="empty-icon">📡</span>
						<Title order={4}>No Signals Found</Title>
						<Text>
							{#if selectedStrategy === 'all'}
								Start your strategies to generate trading signals
							{:else}
								This strategy hasn't generated any signals yet
							{/if}
						</Text>
					</div>
				{:else}
					<div class="signals-table">
						<table>
							<thead>
								<tr>
									<th>Time</th>
									<th>Strategy</th>
									<th>Action</th>
									<th>Symbol</th>
									<th>Price</th>
									<th>Quantity</th>
									<th>Confidence</th>
									<th>Status</th>
									<th>Reason</th>
									<th>Actions</th>
								</tr>
							</thead>
							<tbody>
								{#each signals as signal}
									{@const strategy = strategies.find((s) => s.id === signal.strategy_id)}
									<tr>
										<td>{formatDate(signal.timestamp)}</td>
										<td>
											<Text size="sm">{strategy?.name || 'Unknown'}</Text>
										</td>
										<td>
											<Badge color={signal.action === 'buy' ? 'green' : 'red'} variant="filled">
												{signal.action.toUpperCase()}
											</Badge>
										</td>
										<td>
											<Text size="sm" weight="bold">{signal.symbol}</Text>
										</td>
										<td>${signal.price.toFixed(2)}</td>
										<td>{signal.quantity}</td>
										<td>
											<div class="confidence-bar">
												<div
													class="confidence-fill"
													style="width: {signal.confidence * 100}%; background-color: {signal.confidence >
													0.7
														? '#37b24d'
														: signal.confidence > 0.4
															? '#fab005'
															: '#f03e3e'}"
												></div>
												<span class="confidence-text">{(signal.confidence * 100).toFixed(0)}%</span>
											</div>
										</td>
										<td>
											<Badge
												color={signal.status === 'executed'
													? 'green'
													: signal.status === 'pending'
														? 'yellow'
														: signal.status === 'cancelled'
															? 'gray'
															: 'red'}
												variant="filled"
											>
												{signal.status}
											</Badge>
										</td>
										<td>
											<Text size="sm" override={{ color: '#868e96' }}>{signal.reason}</Text>
										</td>
										<td>
											<div class="action-buttons">
												{#if signal.status === 'pending'}
													<Button
														size="xs"
														color="green"
														onclick={() => executeSignal(signal.id)}
													>
														Execute
													</Button>
													<Button size="xs" color="red" onclick={() => cancelSignal(signal.id)}>
														Cancel
													</Button>
												{/if}
											</div>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</Card>
		</Grid.Col>
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

	.filters {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 20px;
	}

	.filter-controls {
		display: flex;
		gap: 16px;
		align-items: flex-end;
	}

	.form-field {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.form-field label {
		font-size: 0.9rem;
		font-weight: 600;
		color: #495057;
	}

	.form-field select {
		padding: 8px 12px;
		border: 1px solid #ced4da;
		border-radius: 4px;
		background: white;
		font-size: 0.9rem;
		min-width: 200px;
	}

	.empty-state {
		text-align: center;
		padding: 60px 20px;
	}

	.empty-icon {
		font-size: 5rem;
		display: block;
		margin-bottom: 20px;
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
		position: sticky;
		top: 0;
	}

	tbody tr:hover {
		background: #f8f9fa;
	}

	.confidence-bar {
		position: relative;
		width: 80px;
		height: 24px;
		background: #e9ecef;
		border-radius: 4px;
		overflow: hidden;
	}

	.confidence-fill {
		position: absolute;
		left: 0;
		top: 0;
		height: 100%;
		transition: width 0.3s ease;
	}

	.confidence-text {
		position: absolute;
		left: 50%;
		top: 50%;
		transform: translate(-50%, -50%);
		font-size: 0.75rem;
		font-weight: 600;
		color: #212529;
	}

	.action-buttons {
		display: flex;
		gap: 4px;
	}
</style>
