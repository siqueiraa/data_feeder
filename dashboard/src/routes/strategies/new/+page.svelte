<script lang="ts">
	import { goto } from '$app/navigation';
	import {
		Grid,
		Title,
		Text,
		Card,
		TextInput,
		Textarea,
		Button,
		Alert,
		NumberInput
	} from '@svelteuidev/core';
	import {
		strategyApi,
		type CreateStrategyRequest,
		type StrategyType,
		type StrategyParameters
	} from '$lib/api/strategy';

	let name = $state('');
	let description = $state('');
	let strategyType = $state<StrategyType>('ema_crossover');
	let symbol = $state('BTCUSDT');
	let timeframe = $state('15m');
	let exchange = $state<'binance' | 'gateio'>('binance');

	// Risk management
	let stopLoss = $state<number>(2);
	let takeProfit = $state<number>(4);
	let positionSize = $state<number>(100);
	let maxPositions = $state<number>(3);

	// Indicator parameters
	let emaFast = $state<number>(21);
	let emaSlow = $state<number>(89);
	let rsiPeriod = $state<number>(14);
	let rsiOverbought = $state<number>(70);
	let rsiOversold = $state<number>(30);

	// Risk management
	let maxDrawdown = $state<number>(20);
	let dailyLossLimit = $state<number>(10);
	let riskPerTrade = $state<number>(1);

	let loading = $state(false);
	let error = $state<string | null>(null);
	let success = $state(false);

	const strategyTypes: Array<{ value: StrategyType; label: string }> = [
		{ value: 'ema_crossover', label: 'EMA Crossover' },
		{ value: 'rsi_divergence', label: 'RSI Divergence' },
		{ value: 'macd_signal', label: 'MACD Signal' },
		{ value: 'bollinger_bands', label: 'Bollinger Bands' },
		{ value: 'volume_profile', label: 'Volume Profile' },
		{ value: 'trend_following', label: 'Trend Following' },
		{ value: 'mean_reversion', label: 'Mean Reversion' },
		{ value: 'breakout', label: 'Breakout' },
		{ value: 'custom', label: 'Custom Strategy' }
	];

	const timeframes = ['1m', '5m', '15m', '30m', '1h', '4h', '1d'];
	const symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT', 'DOGEUSDT'];

	async function handleSubmit() {
		if (!name || !description) {
			error = 'Please fill in all required fields';
			return;
		}

		loading = true;
		error = null;

		const parameters: StrategyParameters = {
			symbol,
			timeframe,
			exchange,
			stop_loss: stopLoss,
			take_profit: takeProfit,
			position_size: positionSize,
			max_positions: maxPositions,
			indicators: {
				ema_fast: emaFast,
				ema_slow: emaSlow,
				rsi_period: rsiPeriod,
				rsi_overbought: rsiOverbought,
				rsi_oversold: rsiOversold
			},
			risk_management: {
				max_drawdown: maxDrawdown,
				daily_loss_limit: dailyLossLimit,
				position_sizing_method: 'risk_based',
				risk_per_trade: riskPerTrade
			}
		};

		const request: CreateStrategyRequest = {
			name,
			description,
			strategy_type: strategyType,
			parameters
		};

		const result = await strategyApi.create(request);

		if (result.data && result.status === 201) {
			success = true;
			setTimeout(() => goto('/strategies'), 1500);
		} else {
			error = result.error || 'Failed to create strategy';
		}

		loading = false;
	}

	function cancel() {
		goto('/strategies');
	}
</script>

<div class="page-container">
	<div class="page-header">
		<Title order={1}>Create New Strategy</Title>
		<Text size="lg" override={{ color: '#868e96' }}>
			Configure your trading strategy parameters
		</Text>
	</div>

	<form on:submit|preventDefault={handleSubmit}>
		<Grid cols={12} gutter="lg">
			<!-- Basic Information -->
			<Grid.Col span={12}>
				<Card shadow="sm" padding="lg" radius="md">
					<Title order={3} override={{ marginBottom: '20px' }}>Basic Information</Title>
					<div class="form-group">
						<TextInput
							label="Strategy Name"
							placeholder="My EMA Crossover Strategy"
							bind:value={name}
							required
							size="md"
						/>

						<Textarea
							label="Description"
							placeholder="Describe your strategy..."
							bind:value={description}
							required
							minRows={3}
							size="md"
						/>

						<div class="form-row">
							<div class="form-field">
								<label for="strategy-type">Strategy Type</label>
								<select id="strategy-type" bind:value={strategyType}>
									{#each strategyTypes as type}
										<option value={type.value}>{type.label}</option>
									{/each}
								</select>
							</div>

							<div class="form-field">
								<label for="exchange">Exchange</label>
								<select id="exchange" bind:value={exchange}>
									<option value="binance">Binance</option>
									<option value="gateio">Gate.io</option>
								</select>
							</div>
						</div>
					</div>
				</Card>
			</Grid.Col>

			<!-- Market Configuration -->
			<Grid.Col span={6}>
				<Card shadow="sm" padding="lg" radius="md">
					<Title order={3} override={{ marginBottom: '20px' }}>Market Configuration</Title>
					<div class="form-group">
						<div class="form-field">
							<label for="symbol">Symbol</label>
							<select id="symbol" bind:value={symbol}>
								{#each symbols as sym}
									<option value={sym}>{sym}</option>
								{/each}
							</select>
						</div>

						<div class="form-field">
							<label for="timeframe">Timeframe</label>
							<select id="timeframe" bind:value={timeframe}>
								{#each timeframes as tf}
									<option value={tf}>{tf}</option>
								{/each}
							</select>
						</div>

						<NumberInput
							label="Position Size (USDT)"
							bind:value={positionSize}
							min={10}
							step={10}
							size="md"
						/>

						<NumberInput
							label="Max Concurrent Positions"
							bind:value={maxPositions}
							min={1}
							max={10}
							step={1}
							size="md"
						/>
					</div>
				</Card>
			</Grid.Col>

			<!-- Risk Management -->
			<Grid.Col span={6}>
				<Card shadow="sm" padding="lg" radius="md">
					<Title order={3} override={{ marginBottom: '20px' }}>Risk Management</Title>
					<div class="form-group">
						<NumberInput
							label="Stop Loss (%)"
							bind:value={stopLoss}
							min={0.1}
							max={10}
							step={0.1}
							size="md"
						/>

						<NumberInput
							label="Take Profit (%)"
							bind:value={takeProfit}
							min={0.1}
							max={20}
							step={0.1}
							size="md"
						/>

						<NumberInput
							label="Max Drawdown (%)"
							bind:value={maxDrawdown}
							min={1}
							max={50}
							step={1}
							size="md"
						/>

						<NumberInput
							label="Daily Loss Limit (%)"
							bind:value={dailyLossLimit}
							min={1}
							max={25}
							step={1}
							size="md"
						/>

						<NumberInput
							label="Risk Per Trade (%)"
							bind:value={riskPerTrade}
							min={0.1}
							max={5}
							step={0.1}
							size="md"
						/>
					</div>
				</Card>
			</Grid.Col>

			<!-- Indicator Parameters -->
			<Grid.Col span={12}>
				<Card shadow="sm" padding="lg" radius="md">
					<Title order={3} override={{ marginBottom: '20px' }}>Indicator Parameters</Title>

					{#if strategyType === 'ema_crossover' || strategyType === 'trend_following'}
						<div class="form-row">
							<NumberInput
								label="Fast EMA Period"
								bind:value={emaFast}
								min={5}
								max={50}
								step={1}
								size="md"
							/>

							<NumberInput
								label="Slow EMA Period"
								bind:value={emaSlow}
								min={20}
								max={200}
								step={1}
								size="md"
							/>
						</div>
					{/if}

					{#if strategyType === 'rsi_divergence' || strategyType === 'mean_reversion'}
						<div class="form-row">
							<NumberInput
								label="RSI Period"
								bind:value={rsiPeriod}
								min={5}
								max={30}
								step={1}
								size="md"
							/>

							<NumberInput
								label="RSI Overbought"
								bind:value={rsiOverbought}
								min={60}
								max={90}
								step={1}
								size="md"
							/>

							<NumberInput
								label="RSI Oversold"
								bind:value={rsiOversold}
								min={10}
								max={40}
								step={1}
								size="md"
							/>
						</div>
					{/if}

					<Text size="sm" override={{ color: '#868e96', marginTop: '12px' }}>
						{#if strategyType === 'ema_crossover'}
							EMA Crossover: Generates buy signals when fast EMA crosses above slow EMA, and sell
							signals when it crosses below.
						{:else if strategyType === 'rsi_divergence'}
							RSI Divergence: Identifies overbought/oversold conditions and potential trend
							reversals based on RSI divergence.
						{:else if strategyType === 'trend_following'}
							Trend Following: Uses EMA trends to identify and follow strong market trends.
						{:else if strategyType === 'mean_reversion'}
							Mean Reversion: Trades based on RSI extremes, expecting price to revert to the mean.
						{:else}
							Configure indicator parameters based on your strategy type.
						{/if}
					</Text>
				</Card>
			</Grid.Col>

			<!-- Messages -->
			{#if error}
				<Grid.Col span={12}>
					<Alert title="Error" color="red">
						{error}
					</Alert>
				</Grid.Col>
			{/if}

			{#if success}
				<Grid.Col span={12}>
					<Alert title="Success!" color="green">
						Strategy created successfully! Redirecting...
					</Alert>
				</Grid.Col>
			{/if}

			<!-- Actions -->
			<Grid.Col span={12}>
				<div class="actions">
					<Button type="button" onclick={cancel} variant="light" size="lg">Cancel</Button>
					<Button type="submit" size="lg" loading={loading} disabled={success}>
						{loading ? 'Creating...' : 'Create Strategy'}
					</Button>
				</div>
			</Grid.Col>
		</Grid>
	</form>
</div>

<style>
	.page-container {
		padding: 20px;
		max-width: 1400px;
		margin: 0 auto;
	}

	.page-header {
		margin-bottom: 32px;
	}

	.form-group {
		display: flex;
		flex-direction: column;
		gap: 16px;
	}

	.form-row {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
		gap: 16px;
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

	.form-field select,
	select {
		padding: 10px 12px;
		border: 1px solid #ced4da;
		border-radius: 4px;
		background: white;
		font-size: 0.9rem;
		width: 100%;
	}

	.form-field select:focus,
	select:focus {
		outline: none;
		border-color: #228be6;
	}

	.actions {
		display: flex;
		gap: 12px;
		justify-content: flex-end;
		padding-top: 20px;
		border-top: 1px solid #e9ecef;
	}
</style>
