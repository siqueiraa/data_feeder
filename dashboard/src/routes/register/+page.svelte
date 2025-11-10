<script lang="ts">
	import {
		Card,
		TextInput,
		PasswordInput,
		Button,
		Title,
		Text,
		Alert,
		Select
	} from '@svelteuidev/core';
	import { userApi, setAuthToken } from '$lib/api/strategy';
	import { goto } from '$app/navigation';

	let username = $state('');
	let email = $state('');
	let password = $state('');
	let confirmPassword = $state('');
	let role = $state<'admin' | 'trader' | 'viewer'>('trader');
	let loading = $state(false);
	let error = $state<string | null>(null);
	let success = $state(false);

	async function handleRegister() {
		error = null;

		// Validation
		if (!username || !email || !password || !confirmPassword) {
			error = 'Please fill in all fields';
			return;
		}

		if (password !== confirmPassword) {
			error = 'Passwords do not match';
			return;
		}

		if (password.length < 8) {
			error = 'Password must be at least 8 characters long';
			return;
		}

		if (!email.includes('@')) {
			error = 'Please enter a valid email address';
			return;
		}

		loading = true;

		const result = await userApi.register({
			username,
			email,
			password,
			role
		});

		if (result.data && result.status === 201) {
			success = true;
			// Auto-login after registration
			const loginResult = await userApi.login({ username, password });
			if (loginResult.data && loginResult.status === 200) {
				setAuthToken(loginResult.data.token);
				setTimeout(() => goto('/strategies'), 1500);
			}
		} else {
			error = result.error || 'Registration failed. Please try again.';
		}

		loading = false;
	}

	function goToLogin() {
		goto('/login');
	}
</script>

<div class="register-container">
	<div class="register-box">
		<div class="header">
			<span class="logo">📈</span>
			<Title order={2}>Create Account</Title>
			<Text size="sm" override={{ color: '#868e96' }}>Join Data Feeder Dashboard</Text>
		</div>

		<Card shadow="lg" padding="xl" radius="md">
			<form on:submit|preventDefault={handleRegister}>
				<div class="form-content">
					<TextInput
						label="Username"
						placeholder="Choose a username"
						bind:value={username}
						required
						size="md"
					/>

					<TextInput
						label="Email"
						type="email"
						placeholder="your@email.com"
						bind:value={email}
						required
						size="md"
					/>

					<PasswordInput
						label="Password"
						placeholder="At least 8 characters"
						bind:value={password}
						required
						size="md"
					/>

					<PasswordInput
						label="Confirm Password"
						placeholder="Re-enter your password"
						bind:value={confirmPassword}
						required
						size="md"
					/>

					<div class="select-wrapper">
						<label for="role">Role</label>
						<select id="role" bind:value={role}>
							<option value="trader">Trader</option>
							<option value="viewer">Viewer</option>
							<option value="admin">Admin</option>
						</select>
					</div>

					{#if error}
						<Alert title="Registration Error" color="red">
							{error}
						</Alert>
					{/if}

					{#if success}
						<Alert title="Success!" color="green">
							Account created successfully! Redirecting...
						</Alert>
					{/if}

					<Button type="submit" fullSize size="md" loading={loading} disabled={success}>
						{loading ? 'Creating Account...' : 'Create Account'}
					</Button>

					<div class="login-link">
						<Text size="sm">
							Already have an account?
							<button type="button" class="link-button" onclick={goToLogin}>Sign In</button>
						</Text>
					</div>
				</div>
			</form>
		</Card>
	</div>
</div>

<style>
	.register-container {
		min-height: 100vh;
		display: flex;
		align-items: center;
		justify-content: center;
		background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
		padding: 20px;
	}

	.register-box {
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

	.select-wrapper {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.select-wrapper label {
		font-size: 0.9rem;
		font-weight: 600;
		color: #495057;
	}

	.select-wrapper select {
		padding: 10px 12px;
		border: 1px solid #ced4da;
		border-radius: 4px;
		background: white;
		font-size: 0.9rem;
	}

	.login-link {
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
