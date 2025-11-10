<script lang="ts">
	import favicon from '$lib/assets/favicon.svg';
	import { SvelteUIProvider, AppShell, Navbar, Header, Text, NavLink } from '@svelteuidev/core';
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

<SvelteUIProvider>
	<AppShell
		navbar={{
			width: 250,
			breakpoint: 'sm'
		}}
		header={{
			height: 70
		}}
		padding="md"
	>
		<svelte:fragment slot="header">
			<Header height={70} padding="md">
				<div class="header-content">
					<div class="logo">
						<span class="logo-icon">📈</span>
						<Text size="xl" weight="bold" override={{ color: '#228be6' }}>
							Data Feeder Dashboard
						</Text>
					</div>
					<div class="header-info">
						<Text size="sm" override={{ color: '#868e96' }}>
							Real-time Cryptocurrency Data Pipeline
						</Text>
					</div>
				</div>
			</Header>
		</svelte:fragment>

		<svelte:fragment slot="navbar">
			<Navbar width={{ base: 250 }} padding="md">
				<div class="nav-content">
					<Text size="xs" weight="bold" override={{ color: '#868e96', marginBottom: '12px' }}>
						NAVIGATION
					</Text>
					{#each navItems as item}
						<NavLink
							label={item.label}
							href={item.href}
							active={$page.url.pathname === item.href}
							variant="filled"
							override={{
								marginBottom: '4px'
							}}
						>
							<svelte:fragment slot="icon">
								<span class="nav-icon">{item.icon}</span>
							</svelte:fragment>
						</NavLink>
					{/each}

					<div class="nav-footer">
						<Text size="xs" override={{ color: '#adb5bd', marginTop: '20px' }}>
							Version 0.1.0
						</Text>
						<Text size="xs" override={{ color: '#adb5bd' }}>
							Powered by Rust & SvelteUI
						</Text>
					</div>
				</div>
			</Navbar>
		</svelte:fragment>

		<main>
			{@render children()}
		</main>
	</AppShell>
</SvelteUIProvider>

<style>
	:global(body) {
		margin: 0;
		padding: 0;
		font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell,
			'Helvetica Neue', sans-serif;
	}

	.header-content {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.logo {
		display: flex;
		align-items: center;
		gap: 12px;
	}

	.logo-icon {
		font-size: 2rem;
	}

	.nav-content {
		display: flex;
		flex-direction: column;
		height: 100%;
	}

	.nav-icon {
		font-size: 1.2rem;
	}

	.nav-footer {
		margin-top: auto;
		padding-top: 20px;
		border-top: 1px solid #e9ecef;
	}

	main {
		min-height: calc(100vh - 140px);
	}
</style>
