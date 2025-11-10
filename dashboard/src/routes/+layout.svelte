<script lang="ts">
	import favicon from '$lib/assets/favicon.svg';
	import '../app.css';
	import { page } from '$app/stores';

	let { children } = $props();

	const navItems = [
		{ href: '/', label: 'Overview', icon: '🏠' },
		{ href: '/strategies', label: 'Strategies', icon: '⚡' },
		{ href: '/signals', label: 'Signals', icon: '📡' },
		{ href: '/health', label: 'Health & Status', icon: '❤️' },
		{ href: '/performance', label: 'Performance', icon: '📊' },
		{ href: '/binance', label: 'Binance Data', icon: '🔶' },
		{ href: '/gateio', label: 'Gate.io Data', icon: '🟦' },
		{ href: '/deployment', label: 'Deployment', icon: '🚀' }
	];
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
</svelte:head>

<div class="app-layout">
	<header class="app-header">
		<div class="header-content">
			<div class="logo">
				<span class="logo-icon">📈</span>
				<h1>Data Feeder Dashboard</h1>
			</div>
			<p class="subtitle">Real-time Cryptocurrency Data Pipeline</p>
		</div>
	</header>

	<div class="app-body">
		<nav class="app-sidebar">
			<div class="nav-content">
				<p class="nav-title">NAVIGATION</p>
				{#each navItems as item}
					<a
						href={item.href}
						class="nav-link"
						class:active={$page.url.pathname === item.href}
					>
						<span class="nav-icon">{item.icon}</span>
						<span>{item.label}</span>
					</a>
				{/each}

				<div class="nav-footer">
					<p class="footer-text">Version 0.1.0</p>
					<p class="footer-text">Powered by Rust & Svelte</p>
				</div>
			</div>
		</nav>

		<main class="app-main">
			{@render children()}
		</main>
	</div>
</div>

<style>
	.app-layout {
		display: flex;
		flex-direction: column;
		min-height: 100vh;
	}

	.app-header {
		background: white;
		border-bottom: 1px solid #e9ecef;
		padding: 1rem 2rem;
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
	}

	.header-content {
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
	}

	.logo {
		display: flex;
		align-items: center;
		gap: 0.75rem;
	}

	.logo h1 {
		margin: 0;
		font-size: 1.5rem;
		color: #228be6;
	}

	.logo-icon {
		font-size: 2rem;
	}

	.subtitle {
		margin: 0;
		font-size: 0.875rem;
		color: #868e96;
	}

	.app-body {
		display: flex;
		flex: 1;
	}

	.app-sidebar {
		width: 250px;
		background: white;
		border-right: 1px solid #e9ecef;
		padding: 1.5rem 1rem;
	}

	.nav-content {
		display: flex;
		flex-direction: column;
		height: 100%;
	}

	.nav-title {
		font-size: 0.75rem;
		font-weight: 700;
		color: #868e96;
		margin: 0 0 0.75rem 0;
		padding: 0 0.75rem;
	}

	.nav-link {
		display: flex;
		align-items: center;
		gap: 0.75rem;
		padding: 0.625rem 0.75rem;
		margin-bottom: 0.25rem;
		border-radius: 4px;
		text-decoration: none;
		color: #495057;
		font-size: 0.875rem;
		font-weight: 500;
		transition: background 0.2s;
	}

	.nav-link:hover {
		background: #f8f9fa;
	}

	.nav-link.active {
		background: #e7f5ff;
		color: #228be6;
		font-weight: 600;
	}

	.nav-icon {
		font-size: 1.2rem;
	}

	.nav-footer {
		margin-top: auto;
		padding-top: 1rem;
		border-top: 1px solid #e9ecef;
	}

	.footer-text {
		font-size: 0.75rem;
		color: #adb5bd;
		margin: 0.25rem 0;
		padding: 0 0.75rem;
	}

	.app-main {
		flex: 1;
		padding: 2rem;
		overflow-y: auto;
		background: #f8f9fa;
	}

	@media (max-width: 768px) {
		.app-sidebar {
			position: fixed;
			left: -250px;
			height: 100%;
			z-index: 100;
			transition: left 0.3s;
		}

		.app-main {
			padding: 1rem;
		}
	}
</style>
