# Infrastructure

**STATUS:** live.

## Purpose

Documents the compute, networking, and integration layer that underlies all sports: Azure VM for self-hosted ETL, Azure Static Web Apps for web, Schnapp Ops MCP server, Cloudflare tunnels, Flask live runner. Credentials and endpoints are centralized in `/docs/CONNECTIONS.md`.

## Files

Infrastructure-relevant code and config:

- `mcp/server.py` - FastMCP server for Schnapp Ops
- `etl/runner.py` - Flask service on the VM for live data
- `.github/workflows/*.yml` - all automation, 27 workflows total
- `web/staticwebapp.config.json` - SWA routing and auth config

Operational runbooks for recurring tasks will live in `/infrastructure/runbooks/` as they are authored. None yet.

## Key Concepts

The self-hosted GitHub Actions runner on the Azure VM (Central US, B1s, Ubuntu 24.04) runs nearly all ETL. Python venv at `~/venv` with ODBC Driver 18 pre-installed. Service runs as `actions.runner.SchnappAPI-sports-modeling.schnapp-runner.service`.

Schnapp Ops MCP is the remote control plane. It reaches the VM over a Cloudflare named tunnel to `127.0.0.1:8000`. Tools expose Flask status, live scoreboard, workflow triggers, and a guarded `shell_exec` for Python one-offs.

The Flask runner handles NBA CDN live data with no proxy needed. Bound to `127.0.0.1:5000`. Internal VM IP is used from SWA API routes.

## Invariants

- ETL secrets live in GitHub repository secrets or VM systemd environment files. Never hardcoded in code.
- Azure SWA app settings duplicate a few secrets (connection string, admin refresh code) for API routes that need them directly.
- Changes to `mcp/server.py` require triggering `install-mcp.yml` to redeploy. `cloudflared` and `schnapp-mcp` run as systemd services.
- Cloudflare tunnel recovery: `sudo systemctl restart cloudflared && sudo systemctl restart schnapp-mcp` on the VM.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[infra]`.

## Open Questions

- Whether to formalize runbooks for common operations (Flask restart, tunnel restart, VM reboot, Odds API key rotation).
- Whether to add health-check automation beyond the current Uptime Robot ping.
