<script lang="ts">
	import { onMount } from 'svelte';
	import { Grid, Title, Text, Button, Card, Badge, Modal } from '@svelteuidev/core';
	import { strategyApi, isAuthenticated, type Strategy } from '$lib/api/strategy';
	import { goto } from '$app/navigation';
	import StatusCard from '$lib/components/StatusCard.svelte';

	let strategies = $state<Strategy[]>([]);
	let loading = $state(true);
	let deleteModalOpen = $state(false);
	let strategyToDelete = $state<string | null>(null);

	onMount(async () => {
		if (!isAuthenticated()) {
			goto('/login');
			return;
		}

		await loadStrategies();
	});

	async function loadStrategies() {
		loading = true;
		const result = await strategyApi.list();
		if (result.data) {
			strategies = result.data;
		}
		loading = false;
	}

	function createNew() {
		goto('/strategies/new');
	}

	function viewStrategy(id: string) {
		goto(`/strategies/${id}`);
	}

	function editStrategy(id: string) {
		goto(`/strategies/${id}/edit`);
	}

	async function toggleStrategy(id: string, currentStatus: string) {
		if (currentStatus === 'active') {
			await strategyApi.pause(id);
		} else {
			await strategyApi.start(id);
		}
		await loadStrategies();
	}

	function confirmDelete(id: string) {
		strategyToDelete = id;
		deleteModalOpen = true;
	}

	async function deleteStrategy() {
		if (strategyToDelete) {
			await strategyApi.delete(strategyToDelete);
			deleteModalOpen = false;
			strategyToDelete = null;
			await loadStrategies();
		}
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

	function formatDate(date: string | null) {
		if (!date) return 'Never';
		return new Date(date).toLocaleString();
	}

	const activeCount = $derived(strategies.filter((s) => s.status === 'active').length);
	const draftCount = $derived(strategies.filter((s) => s.status === 'draft').length);
	const pausedCount = $derived(strategies.filter((s) => s.status === 'paused').length);
</script>

<div class="page-container">
	<div class="page-header">
		<div>
			<Title order={1}>Trading Strategies</Title>
			<Text size="lg" override={{ color: '#868e96' }}>
				Manage and monitor your trading strategies
			</Text>
		</div>
		<Button onclick={createNew} size="lg">+ New Strategy</Button>
	</div>

	<Grid cols={12} gutter="lg">
		<Grid.Col span={4}>
			<StatusCard
				title="Active Strategies"
				status="success"
				value={activeCount}
				description="Currently running"
			/>
		</Grid.Col>

		<Grid.Col span={4}>
			<StatusCard title="Draft Strategies" status="warning" value={draftCount} description="Not yet deployed" />
		</Grid.Col>

		<Grid.Col span={4}>
			<StatusCard title="Paused Strategies" status="warning" value={pausedCount} description="Temporarily stopped" />
		</Grid.Col>

		<Grid.Col span={12}>
			<Title order={2} override={{ marginTop: '24px', marginBottom: '16px' }}>
				All Strategies
			</Title>
		</Grid.Col>

		{#if loading}
			<Grid.Col span={12}>
				<Card shadow="sm" padding="lg" radius="md">
					<Text align="center">Loading strategies...</Text>
				</Card>
			</Grid.Col>
		{:else if strategies.length === 0}
			<Grid.Col span={12}>
				<Card shadow="sm" padding="lg" radius="md">
					<div class="empty-state">
						<span class="empty-icon">📊</span>
						<Title order={3}>No Strategies Yet</Title>
						<Text size="md" override={{ color: '#868e96', marginBottom: '20px' }}>
							Get started by creating your first trading strategy
						</Text>
						<Button onclick={createNew}>Create Strategy</Button>
					</div>
				</Card>
			</Grid.Col>
		{:else}
			{#each strategies as strategy}
				<Grid.Col span={12}>
					<Card shadow="sm" padding="lg" radius="md" override={{ cursor: 'pointer' }}>
						<div class="strategy-card">
							<div class="strategy-header">
								<div class="strategy-info">
									<div class="title-row">
										<Title order={3}>{strategy.name}</Title>
										<Badge color={getStatusColor(strategy.status)} variant="filled">
											{strategy.status.toUpperCase()}
										</Badge>
									</div>
									<Text size="sm" override={{ color: '#868e96' }}>
										{strategy.description}
									</Text>
								</div>
								<div class="strategy-actions">
									<Button onclick={() => viewStrategy(strategy.id)} variant="light">
										View
									</Button>
									<Button onclick={() => editStrategy(strategy.id)} variant="light">
										Edit
									</Button>
									<Button
										onclick={() => toggleStrategy(strategy.id, strategy.status)}
										color={strategy.status === 'active' ? 'yellow' : 'green'}
									>
										{strategy.status === 'active' ? 'Pause' : 'Start'}
									</Button>
									<Button onclick={() => confirmDelete(strategy.id)} color="red" variant="light">
										Delete
									</Button>
								</div>
							</div>

							<div class="strategy-details">
								<div class="detail-item">
									<Text size="xs" weight="bold">TYPE</Text>
									<Text size="sm">{strategy.strategy_type}</Text>
								</div>
								<div class="detail-item">
									<Text size="xs" weight="bold">SYMBOL</Text>
									<Text size="sm">{strategy.parameters.symbol}</Text>
								</div>
								<div class="detail-item">
									<Text size="xs" weight="bold">TIMEFRAME</Text>
									<Text size="sm">{strategy.parameters.timeframe}</Text>
								</div>
								<div class="detail-item">
									<Text size="xs" weight="bold">EXCHANGE</Text>
									<Text size="sm">{strategy.parameters.exchange}</Text>
								</div>
								<div class="detail-item">
									<Text size="xs" weight="bold">CREATED</Text>
									<Text size="sm">{formatDate(strategy.created_at)}</Text>
								</div>
								<div class="detail-item">
									<Text size="xs" weight="bold">LAST EXECUTED</Text>
									<Text size="sm">{formatDate(strategy.last_executed)}</Text>
								</div>
							</div>

							{#if strategy.performance}
								<div class="performance-summary">
									<div class="perf-item">
										<Text size="xs">Win Rate</Text>
										<Text size="lg" weight="bold" override={{ color: '#37b24d' }}>
											{strategy.performance.win_rate.toFixed(1)}%
										</Text>
									</div>
									<div class="perf-item">
										<Text size="xs">Total PnL</Text>
										<Text
											size="lg"
											weight="bold"
											override={{
												color: strategy.performance.total_pnl >= 0 ? '#37b24d' : '#f03e3e'
											}}
										>
											${strategy.performance.total_pnl.toFixed(2)}
										</Text>
									</div>
									<div class="perf-item">
										<Text size="xs">Trades</Text>
										<Text size="lg" weight="bold">{strategy.performance.total_trades}</Text>
									</div>
									<div class="perf-item">
										<Text size="xs">Sharpe Ratio</Text>
										<Text size="lg" weight="bold">
											{strategy.performance.sharpe_ratio.toFixed(2)}
										</Text>
									</div>
								</div>
							{/if}
						</div>
					</Card>
				</Grid.Col>
			{/each}
		{/if}
	</Grid>
</div>

<Modal opened={deleteModalOpen} onClose={() => (deleteModalOpen = false)} title="Confirm Delete">
	<Text size="md" override={{ marginBottom: '20px' }}>
		Are you sure you want to delete this strategy? This action cannot be undone.
	</Text>
	<div class="modal-actions">
		<Button onclick={() => (deleteModalOpen = false)} variant="light">Cancel</Button>
		<Button onclick={deleteStrategy} color="red">Delete Strategy</Button>
	</div>
</Modal>

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

	.empty-state {
		text-align: center;
		padding: 60px 20px;
	}

	.empty-icon {
		font-size: 5rem;
		display: block;
		margin-bottom: 20px;
	}

	.strategy-card {
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.strategy-header {
		display: flex;
		justify-content: space-between;
		align-items: flex-start;
	}

	.strategy-info {
		flex: 1;
	}

	.title-row {
		display: flex;
		align-items: center;
		gap: 12px;
		margin-bottom: 8px;
	}

	.strategy-actions {
		display: flex;
		gap: 8px;
	}

	.strategy-details {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
		gap: 16px;
		padding: 16px;
		background: #f8f9fa;
		border-radius: 8px;
	}

	.detail-item {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.performance-summary {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
		gap: 16px;
		padding: 20px;
		background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
		border-radius: 8px;
		color: white;
	}

	.perf-item {
		text-align: center;
	}

	.modal-actions {
		display: flex;
		gap: 12px;
		justify-content: flex-end;
	}
</style>
