<script lang="ts">
	import { Card, TextInput, PasswordInput, Button, Title, Text, Alert } from '@svelteuidev/core';
	import { userApi, setAuthToken } from '$lib/api/strategy';
	import { goto } from '$app/navigation';

	let username = $state('');
	let password = $state('');
	let loading = $state(false);
	let error = $state<string | null>(null);

	async function handleLogin() {
		if (!username || !password) {
			error = 'Please enter both username and password';
			return;
		}

		loading = true;
		error = null;

		const result = await userApi.login({ username, password });

		if (result.data && result.status === 200) {
			setAuthToken(result.data.token);
			goto('/strategies');
		} else {
			error = result.error || 'Login failed. Please check your credentials.';
		}

		loading = false;
	}

	function goToRegister() {
		goto('/register');
	}
</script>

<div class="login-container">
	<div class="login-box">
		<div class="header">
			<span class="logo">📈</span>
			<Title order={2}>Data Feeder Dashboard</Title>
			<Text size="sm" override={{ color: '#868e96' }}>Sign in to manage your strategies</Text>
		</div>

		<Card shadow="lg" padding="xl" radius="md">
			<form on:submit|preventDefault={handleLogin}>
				<div class="form-content">
					<TextInput
						label="Username"
						placeholder="Enter your username"
						bind:value={username}
						required
						size="md"
					/>

					<PasswordInput
						label="Password"
						placeholder="Enter your password"
						bind:value={password}
						required
						size="md"
					/>

					{#if error}
						<Alert title="Login Error" color="red">
							{error}
						</Alert>
					{/if}

					<Button type="submit" fullSize size="md" loading={loading}>
						{loading ? 'Signing in...' : 'Sign In'}
					</Button>

					<div class="register-link">
						<Text size="sm">
							Don't have an account?
							<button type="button" class="link-button" onclick={goToRegister}>Register</button>
						</Text>
					</div>
				</div>
			</form>
		</Card>
	</div>
</div>

<style>
	.login-container {
		min-height: 100vh;
		display: flex;
		align-items: center;
		justify-content: center;
		background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
		padding: 20px;
	}

	.login-box {
		width: 100%;
		max-width: 450px;
	}

	.header {
		text-align: center;
		margin-bottom: 32px;
		color: white;
	}

	.logo {
		font-size: 4rem;
		display: block;
		margin-bottom: 16px;
	}

	.form-content {
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.register-link {
		text-align: center;
		margin-top: 8px;
	}

	.link-button {
		background: none;
		border: none;
		color: #228be6;
		text-decoration: underline;
		cursor: pointer;
		font-size: 0.875rem;
		padding: 0;
		margin-left: 4px;
	}

	.link-button:hover {
		color: #1c7ed6;
	}
</style>
