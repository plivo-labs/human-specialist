# Security Policy

## Supported versions

Only the latest release receives security fixes.

| Version | Supported |
| ------- | --------- |
| latest  | yes       |
| older   | no        |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Use GitHub's private vulnerability reporting:
**Security tab → "Report a vulnerability"**
(direct link: `https://github.com/plivo-labs/human-specialist/security/advisories/new`)

Include:
- Description of the vulnerability and potential impact
- Steps to reproduce or a minimal proof-of-concept
- The version (image tag or commit) and the affected route, callback, stream or webhook

We aim to acknowledge reports within 2 business days and provide a fix timeline within 7 days.

## Scope notes

human-specialist handles sensitive surface areas. Please pay particular attention to:

- **Tenant isolation**: the tenant comes only from the credential. Ids in paths and bodies are resolved inside that tenant: another tenant's id returns 404, or 422 when it appears in a body. Any way to read or change another tenant's data is in scope.
- **Plivo callbacks**: `/plivo/{tenant_id}/...` require a valid Plivo signature V3, checked with the tenant's token against a URL built from `PUBLIC_BASE_URL`, not from the Host header.
- **Webhook SSRF guard**: webhook URLs must be https and must resolve to public addresses; the sender connects to the address it checked and does not follow redirects. `WEBHOOK_PRIVATE_HOSTS` is the only exception.
- **API keys and tokens**: tenant API keys are scoped, shown once and stored as SHA-256 hashes. Agent and stream tokens are opaque, expire, and see only their own agent's or the supervised agents' data. Only a stream token is accepted in `?access_token=`.
- **Dialing rights**: `agent_direct_dial` defaults to off, so an agent token, the browser SDK or a SIP phone cannot start an outside call; only a tenant key with the `calls` scope can, through `POST /calls`.
- **Signed recording URLs**: the raw Plivo recording URL is never returned. Responses carry an `audio_url` signed for five minutes, and the audio route fetches only from `PLIVO_MEDIA_URL` or `*.plivo.com`.
- **Secrets at rest**: Plivo tokens are encrypted at rest under `HS_SECRET_KEY` and never returned. Logs never contain tokens, keys or signatures.
- **SQL**: every value is bound; a test fails on interpolated SQL.
