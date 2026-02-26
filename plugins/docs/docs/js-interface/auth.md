---
sidebar_position: 2
---

# Authentication API

trexsql uses [Better Auth](https://www.better-auth.com/) for authentication, providing email/password login, social providers, JWT tokens, and OIDC provider capabilities.

## Base Path

All auth endpoints are at: `/trex/api/auth/*`

## Methods

### Email & Password

- `POST /trex/api/auth/sign-up/email` — Register with email and password
- `POST /trex/api/auth/sign-in/email` — Sign in with email and password

### Social Providers

Configured via environment variables or the `trex.sso_provider` database table:

| Provider | Environment Variables |
|----------|----------------------|
| Google | `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` |
| GitHub | `GITHUB_CLIENT_ID`, `GITHUB_CLIENT_SECRET` |
| Microsoft | `MICROSOFT_CLIENT_ID`, `MICROSOFT_CLIENT_SECRET` |

Database-driven SSO providers (from the `trex.sso_provider` table) take precedence when available.

### Sessions

- `GET /trex/api/auth/get-session` — Get current session
- `POST /trex/api/auth/sign-out` — Sign out

Sessions expire after 7 days and are stored in PostgreSQL.

### Admin

Admin endpoints are available for users with the `admin` role:

- User management (list, create, ban, remove)
- Session management (list, revoke)

## First User

The first user to register is automatically assigned the `admin` role. Additional users matching the `ADMIN_EMAIL` environment variable also receive admin privileges.

## User Model

Users have the following additional fields beyond Better Auth defaults:

| Field | Type | Description |
|-------|------|-------------|
| `role` | string | `admin` or `user` (default: `user`) |
| `deletedAt` | string | Soft delete timestamp |
| `mustChangePassword` | boolean | Force password change on next login |

## OIDC Provider

trexsql can act as an OpenID Connect provider for downstream applications:

- Login page: `/trex/login`
- Consent page: `/trex/consent`
