<script lang="ts">
	import { Card } from '@svelteuidev/core';
	import { onMount } from 'svelte';
	import type { Candle } from '$lib/api/client';

	interface Props {
		title: string;
		data: Candle[];
		symbol: string;
	}

	let { title, data, symbol }: Props = $props();
	let canvas: HTMLCanvasElement;
	let ctx: CanvasRenderingContext2D | null;

	onMount(() => {
		ctx = canvas.getContext('2d');
		drawChart();
	});

	$effect(() => {
		if (ctx && data.length > 0) {
			drawChart();
		}
	});

	function drawChart() {
		if (!ctx || !canvas) return;

		const width = canvas.width;
		const height = canvas.height;
		const padding = 50;

		// Clear canvas
		ctx.clearRect(0, 0, width, height);

		if (data.length === 0) {
			ctx.fillStyle = '#868e96';
			ctx.font = '14px sans-serif';
			ctx.textAlign = 'center';
			ctx.fillText('No data available', width / 2, height / 2);
			return;
		}

		// Find min/max prices
		const allPrices = data.flatMap((d) => [d.high, d.low]);
		const minPrice = Math.min(...allPrices);
		const maxPrice = Math.max(...allPrices);
		const priceRange = maxPrice - minPrice || 1;

		// Draw background grid
		ctx.strokeStyle = '#e9ecef';
		ctx.lineWidth = 1;
		for (let i = 0; i <= 5; i++) {
			const y = padding + ((height - 2 * padding) / 5) * i;
			ctx.beginPath();
			ctx.moveTo(padding, y);
			ctx.lineTo(width - padding, y);
			ctx.stroke();

			// Y-axis labels
			const price = maxPrice - (priceRange / 5) * i;
			ctx.fillStyle = '#868e96';
			ctx.font = '11px sans-serif';
			ctx.textAlign = 'right';
			ctx.fillText('$' + price.toFixed(2), padding - 5, y + 4);
		}

		// Draw candlesticks
		const candleWidth = Math.min(
			(width - 2 * padding) / data.length - 2,
			20
		);

		data.forEach((candle, index) => {
			const x = padding + ((width - 2 * padding) / data.length) * (index + 0.5);
			const yOpen = padding + ((maxPrice - candle.open) / priceRange) * (height - 2 * padding);
			const yClose = padding + ((maxPrice - candle.close) / priceRange) * (height - 2 * padding);
			const yHigh = padding + ((maxPrice - candle.high) / priceRange) * (height - 2 * padding);
			const yLow = padding + ((maxPrice - candle.low) / priceRange) * (height - 2 * padding);

			const isGreen = candle.close >= candle.open;
			const color = isGreen ? '#37b24d' : '#f03e3e';

			// Draw wick
			ctx.strokeStyle = color;
			ctx.lineWidth = 1;
			ctx.beginPath();
			ctx.moveTo(x, yHigh);
			ctx.lineTo(x, yLow);
			ctx.stroke();

			// Draw body
			ctx.fillStyle = color;
			const bodyHeight = Math.abs(yClose - yOpen) || 1;
			const bodyY = Math.min(yOpen, yClose);
			ctx.fillRect(x - candleWidth / 2, bodyY, candleWidth, bodyHeight);
		});

		// Draw symbol label
		ctx.fillStyle = '#228be6';
		ctx.font = 'bold 14px sans-serif';
		ctx.textAlign = 'left';
		ctx.fillText(symbol, padding, padding - 20);
	}
</script>

<Card shadow="sm" padding="lg" radius="md">
	<div class="chart-container">
		<h3>{title}</h3>
		<canvas bind:this={canvas} width="800" height="400"></canvas>
	</div>
</Card>

<style>
	.chart-container {
		display: flex;
		flex-direction: column;
		gap: 16px;
	}

	h3 {
		margin: 0;
		font-size: 1.1rem;
		font-weight: 600;
	}

	canvas {
		width: 100%;
		height: auto;
		max-height: 400px;
	}
</style>
