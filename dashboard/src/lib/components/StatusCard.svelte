<script lang="ts">
	import { Card, Badge } from '@svelteuidev/core';

	interface Props {
		title: string;
		status?: 'success' | 'error' | 'warning' | 'loading';
		value?: string | number;
		description?: string;
	}

	let { title, status = 'loading', value, description }: Props = $props();

	const statusColors = {
		success: 'green',
		error: 'red',
		warning: 'yellow',
		loading: 'gray'
	};

	const statusLabels = {
		success: 'Healthy',
		error: 'Error',
		warning: 'Warning',
		loading: 'Loading...'
	};
</script>

<Card shadow="sm" padding="lg" radius="md">
	<div class="status-card">
		<div class="header">
			<h3>{title}</h3>
			<Badge color={statusColors[status]} variant="filled">
				{statusLabels[status]}
			</Badge>
		</div>

		{#if value !== undefined}
			<div class="value">{value}</div>
		{/if}

		{#if description}
			<p class="description">{description}</p>
		{/if}
	</div>
</Card>

<style>
	.status-card {
		display: flex;
		flex-direction: column;
		gap: 12px;
	}

	.header {
		display: flex;
		justify-content: space-between;
		align-items: center;
	}

	h3 {
		margin: 0;
		font-size: 1.1rem;
		font-weight: 600;
	}

	.value {
		font-size: 2rem;
		font-weight: 700;
		color: var(--primary-color, #228be6);
	}

	.description {
		margin: 0;
		font-size: 0.9rem;
		color: var(--text-secondary, #868e96);
	}
</style>
