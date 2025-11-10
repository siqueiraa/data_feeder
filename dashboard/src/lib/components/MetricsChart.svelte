<script lang="ts">
	import { Card } from '@svelteuidev/core';
	import { onMount } from 'svelte';

	interface DataPoint {
		timestamp: string;
		value: number;
	}

	interface Props {
		title: string;
		data: DataPoint[];
		color?: string;
		unit?: string;
	}

	let { title, data, color = '#228be6', unit = '' }: Props = $props();
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
		const padding = 40;

		// Clear canvas
		ctx.clearRect(0, 0, width, height);

		if (data.length === 0) {
			ctx.fillStyle = '#868e96';
			ctx.font = '14px sans-serif';
			ctx.textAlign = 'center';
			ctx.fillText('No data available', width / 2, height / 2);
			return;
		}

		// Find min/max values
		const values = data.map((d) => d.value);
		const minValue = Math.min(...values);
		const maxValue = Math.max(...values);
		const range = maxValue - minValue || 1;

		// Draw grid lines
		ctx.strokeStyle = '#e9ecef';
		ctx.lineWidth = 1;
		for (let i = 0; i <= 5; i++) {
			const y = padding + ((height - 2 * padding) / 5) * i;
			ctx.beginPath();
			ctx.moveTo(padding, y);
			ctx.lineTo(width - padding, y);
			ctx.stroke();

			// Draw y-axis labels
			const value = maxValue - (range / 5) * i;
			ctx.fillStyle = '#868e96';
			ctx.font = '12px sans-serif';
			ctx.textAlign = 'right';
			ctx.fillText(value.toFixed(2) + unit, padding - 5, y + 4);
		}

		// Draw line chart
		ctx.strokeStyle = color;
		ctx.lineWidth = 2;
		ctx.beginPath();

		data.forEach((point, index) => {
			const x = padding + ((width - 2 * padding) / (data.length - 1)) * index;
			const y = padding + ((maxValue - point.value) / range) * (height - 2 * padding);

			if (index === 0) {
				ctx.moveTo(x, y);
			} else {
				ctx.lineTo(x, y);
			}
		});

		ctx.stroke();

		// Draw points
		ctx.fillStyle = color;
		data.forEach((point, index) => {
			const x = padding + ((width - 2 * padding) / (data.length - 1)) * index;
			const y = padding + ((maxValue - point.value) / range) * (height - 2 * padding);

			ctx.beginPath();
			ctx.arc(x, y, 4, 0, Math.PI * 2);
			ctx.fill();
		});
	}
</script>

<Card shadow="sm" padding="lg" radius="md">
	<div class="chart-container">
		<h3>{title}</h3>
		<canvas bind:this={canvas} width="600" height="300"></canvas>
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
		max-height: 300px;
	}
</style>
