<script lang="ts">
	import { onMount } from 'svelte';
	import { Grid, Title, Text, Card, Button, Badge, Alert } from '@svelteuidev/core';
	import StatusCard from '$lib/components/StatusCard.svelte';
	import { deploymentApi } from '$lib/api/client';
	import type { ValidationResponse } from '$lib/api/client';

	let validateResult = $state<ValidationResponse | null>(null);
	let deployValidateResult = $state<ValidationResponse | null>(null);
	let deployCheckResult = $state<ValidationResponse | null>(null);
	let loading = $state({ validate: false, deployValidate: false, deployCheck: false });
	let lastUpdate = $state<string>('');

	async function runValidate() {
		loading.validate = true;
		const result = await deploymentApi.validate();
		if (result.data) {
			validateResult = result.data;
		}
		loading.validate = false;
		lastUpdate = new Date().toLocaleTimeString();
	}

	async function runDeployValidate() {
		loading.deployValidate = true;
		const result = await deploymentApi.deployValidate();
		if (result.data) {
			deployValidateResult = result.data;
		}
		loading.deployValidate = false;
		lastUpdate = new Date().toLocaleTimeString();
	}

	async function runDeployCheck() {
		loading.deployCheck = true;
		const result = await deploymentApi.deployCheck();
		if (result.data) {
			deployCheckResult = result.data;
		}
		loading.deployCheck = false;
		lastUpdate = new Date().toLocaleTimeString();
	}

	onMount(() => {
		runValidate();
		runDeployCheck();
	});

	function getStatusFromChecks(checks: ValidationResponse['checks']): 'success' | 'error' | 'warning' {
		const values = Object.values(checks);
		if (values.every((v) => v === true)) return 'success';
		if (values.some((v) => v === false)) return 'error';
		return 'warning';
	}
</script>

<div class="page-container">
	<div class="page-header">
		<div>
			<Title order={1}>Deployment Validation</Title>
			<Text size="lg" override={{ color: '#868e96' }}>
				Validate deployment configuration and resource availability
			</Text>
		</div>
		{#if lastUpdate}
			<Text size="sm" override={{ color: '#868e96' }}>
				Last updated: {lastUpdate}
			</Text>
		{/if}
	</div>

	<Grid cols={12} gutter="lg">
		<Grid.Col span={12}>
			<Alert title="About Deployment Validation" color="blue">
				These endpoints check whether your deployment has sufficient resources and proper
				configuration. Use them before deploying to production or after infrastructure changes.
			</Alert>
		</Grid.Col>

		<Grid.Col span={12}>
			<Title order={2}>Validation Checks</Title>
		</Grid.Col>

		<Grid.Col span={4}>
			<Card shadow="sm" padding="lg" radius="md">
				<div class="validation-card">
					<div class="validation-header">
						<h3>Resource Validation</h3>
						<Badge
							color={validateResult ? (validateResult.status === 'healthy' ? 'green' : 'red') : 'gray'}
							variant="filled"
						>
							{validateResult?.status || 'Not Run'}
						</Badge>
					</div>
					<Text size="sm" override={{ color: '#868e96', marginBottom: '16px' }}>
						Checks memory and CPU availability
					</Text>

					{#if validateResult}
						<div class="checks">
							<div class="check-item">
								<span>Memory:</span>
								<Badge color={validateResult.checks.memory ? 'green' : 'red'}>
									{validateResult.checks.memory ? '✓ OK' : '✗ Failed'}
								</Badge>
							</div>
							<div class="check-item">
								<span>CPU:</span>
								<Badge color={validateResult.checks.cpu ? 'green' : 'red'}>
									{validateResult.checks.cpu ? '✓ OK' : '✗ Failed'}
								</Badge>
							</div>
						</div>
					{/if}

					<Button onclick={runValidate} loading={loading.validate} fullSize>
						{loading.validate ? 'Running...' : 'Run Validation'}
					</Button>
				</div>
			</Card>
		</Grid.Col>

		<Grid.Col span={4}>
			<Card shadow="sm" padding="lg" radius="md">
				<div class="validation-card">
					<div class="validation-header">
						<h3>Full Deployment Check</h3>
						<Badge
							color={deployValidateResult
								? deployValidateResult.status === 'healthy'
									? 'green'
									: 'red'
								: 'gray'}
							variant="filled"
						>
							{deployValidateResult?.status || 'Not Run'}
						</Badge>
					</div>
					<Text size="sm" override={{ color: '#868e96', marginBottom: '16px' }}>
						Comprehensive deployment validation
					</Text>

					{#if deployValidateResult}
						<div class="checks">
							<div class="check-item">
								<span>Memory:</span>
								<Badge color={deployValidateResult.checks.memory ? 'green' : 'red'}>
									{deployValidateResult.checks.memory ? '✓ OK' : '✗ Failed'}
								</Badge>
							</div>
							<div class="check-item">
								<span>CPU:</span>
								<Badge color={deployValidateResult.checks.cpu ? 'green' : 'red'}>
									{deployValidateResult.checks.cpu ? '✓ OK' : '✗ Failed'}
								</Badge>
							</div>
							{#if deployValidateResult.checks.database !== undefined}
								<div class="check-item">
									<span>Database:</span>
									<Badge color={deployValidateResult.checks.database ? 'green' : 'red'}>
										{deployValidateResult.checks.database ? '✓ OK' : '✗ Failed'}
									</Badge>
								</div>
							{/if}
							{#if deployValidateResult.checks.kafka !== undefined}
								<div class="check-item">
									<span>Kafka:</span>
									<Badge color={deployValidateResult.checks.kafka ? 'green' : 'red'}>
										{deployValidateResult.checks.kafka ? '✓ OK' : '✗ Failed'}
									</Badge>
								</div>
							{/if}
						</div>
					{/if}

					<Button onclick={runDeployValidate} loading={loading.deployValidate} fullSize>
						{loading.deployValidate ? 'Running...' : 'Run Full Check'}
					</Button>
				</div>
			</Card>
		</Grid.Col>

		<Grid.Col span={4}>
			<Card shadow="sm" padding="lg" radius="md">
				<div class="validation-card">
					<div class="validation-header">
						<h3>Quick Deployment Check</h3>
						<Badge
							color={deployCheckResult
								? deployCheckResult.status === 'healthy'
									? 'green'
									: 'red'
								: 'gray'}
							variant="filled"
						>
							{deployCheckResult?.status || 'Not Run'}
						</Badge>
					</div>
					<Text size="sm" override={{ color: '#868e96', marginBottom: '16px' }}>
						Fast deployment readiness check
					</Text>

					{#if deployCheckResult}
						<div class="checks">
							<div class="check-item">
								<span>Memory:</span>
								<Badge color={deployCheckResult.checks.memory ? 'green' : 'red'}>
									{deployCheckResult.checks.memory ? '✓ OK' : '✗ Failed'}
								</Badge>
							</div>
							<div class="check-item">
								<span>CPU:</span>
								<Badge color={deployCheckResult.checks.cpu ? 'green' : 'red'}>
									{deployCheckResult.checks.cpu ? '✓ OK' : '✗ Failed'}
								</Badge>
							</div>
						</div>
					{/if}

					<Button onclick={runDeployCheck} loading={loading.deployCheck} fullSize>
						{loading.deployCheck ? 'Running...' : 'Run Quick Check'}
					</Button>
				</div>
			</Card>
		</Grid.Col>

		<Grid.Col span={12}>
			<Title order={2} override={{ marginTop: '24px' }}>Endpoint Information</Title>
		</Grid.Col>

		<Grid.Col span={12}>
			<Card shadow="sm" padding="lg" radius="md">
				<div class="endpoints-info">
					<div class="endpoint-item">
						<h4>GET /validate</h4>
						<p>Basic resource validation checking memory and CPU availability.</p>
					</div>
					<div class="endpoint-item">
						<h4>GET /deploy/validate</h4>
						<p>
							Comprehensive deployment validation including optional services like Kafka and
							PostgreSQL.
						</p>
					</div>
					<div class="endpoint-item">
						<h4>GET /deploy/check</h4>
						<p>Quick deployment readiness check for core resources.</p>
					</div>
				</div>
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

	.validation-card {
		display: flex;
		flex-direction: column;
		gap: 16px;
		min-height: 300px;
	}

	.validation-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
	}

	.validation-card h3 {
		margin: 0;
		font-size: 1.1rem;
		font-weight: 600;
	}

	.checks {
		display: flex;
		flex-direction: column;
		gap: 8px;
		flex-grow: 1;
	}

	.check-item {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 8px 0;
		border-bottom: 1px solid #e9ecef;
	}

	.check-item span {
		font-weight: 500;
	}

	.endpoints-info {
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.endpoint-item h4 {
		margin: 0 0 8px 0;
		font-family: monospace;
		color: #228be6;
	}

	.endpoint-item p {
		margin: 0;
		color: #868e96;
		line-height: 1.6;
	}
</style>
