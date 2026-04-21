# Infrastructure

**STATUS:** live.

## Purpose

Documents the compute, networking, and integration layer that underlies all sports: the Azure VM self-hosted GitHub Actions runner, Azure Static Web Apps for the web front end, the Schnapp Ops MCP server, Cloudflare tunnels, and the Flask live-data runner. Credentials and endpoints are centralized in `/docs/CONNECTIONS.md`.

## Files

Infrastructure-relevant code and config:

- `mcp/server.py` - FastMCP server for Schnapp Ops, runs on the VM at port 8000
- `etl/runner.py` - Flask live-data service, runs on the VM at port 5000
- `.github/workflows/*.yml` - all automation, 27 workflows total
- `web/staticwebapp.config.json` - SWA routing and auth config

Operational runbooks will live in `/infrastructure/runbooks/` as they are authored. None yet.

## Key Concepts

### Self-hosted runner (VM)

`schnapp-runner-2` Azure VM.

- Resource group: `SPORTS-MODELING`
- Subscription: `sports-modeling-subscription`
- Region: Central US
- Size: Standard B1s (1 vCPU, 1 GiB RAM, x64, V2 generation)
- OS: Ubuntu 24.04 LTS
- Admin user: `schnapp-admin`
- Public IP: `172.173.126.81` (NIC `schnapp-runner-2254`)
- Private IP: `10.0.0.4`
- VNet/subnet: `schnapp-runner-2-vnet/default`
- Created: 2026-04-10
- Python venv: `~/venv` with pinned deps pre-installed
- ODBC Driver 18 pre-installed
- 1 GB swap at `/swapfile`, persistent, `swappiness = 80`
- Runner systemd service: `actions.runner.SchnappAPI-sports-modeling.schnapp-runner.service` with `Restart=always`

All active workflows use `runs-on: [self-hosted, schnapp-runner]`. No ODBC or pip install steps inside any workflow; everything is pre-installed on the image. ETL runs dropped from 2-4 minutes to around 25 seconds after the move off GitHub-hosted runners.

B1s is sufficient because ETL is I/O-bound against Azure SQL and The Odds API. Memory pressure is managed by the persistent 1 GB swap with `swappiness = 80`.

Workflows execute in the runner's work directory. The MCP server deliberately clones the repo separately to `~/sports-modeling` and uses that as `WorkingDirectory` so it can start before the runner has executed any job.

### Flask live-data runner

`etl/runner.py` on the VM. Systemd service `schnapp-flask.service`. Listens on `0.0.0.0:5000` (all interfaces), so it is reachable locally via `127.0.0.1:5000`, internally via the VM's private IP, and externally via a Cloudflare-proxied DNS name.

- `GET /ping` - health. **No auth.** Used by `flask_status` MCP tool and for debugging. Returns `{"ok": true}`
- `GET /scoreboard` - today's game statuses from `cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json`. Requires `X-Runner-Key` header. Returns `{ games: [...] }` with `gameStatus` 1 (upcoming), 2 (live), 3 (final)
- `GET /boxscore?gameId=` - live player stats + score, directly from `cdn.nba.com/static/json/liveData/boxscore/boxscore_{gameId}.json`. Requires `X-Runner-Key` header

Auth: `X-Runner-Key: runner-Lake4971` matches `RUNNER_API_KEY` env var. Enforced on `/scoreboard` and `/boxscore` only; `/ping` is open so external health-check callers can hit it without a secret.

**Public hostname for Flask: `https://live.schnapp.bet`.** Cloudflare-proxied. Always use this DNS name from web code, never a hardcoded IP. If the VM's public IP changes, Cloudflare DNS gets updated and web routes keep working without a code change.

Both CDN sources are public. `NBA_PROXY_URL` is present in the systemd environment file but the runner does not use it.

### Schnapp Ops MCP server

`mcp/server.py`, FastMCP, port 8000 bound to `127.0.0.1`. Systemd service `schnapp-mcp.service`. Exposed through a Cloudflare named tunnel at `https://mcp.schnapp.bet/mcp`. Connected as "Schnapp Ops" in claude.ai.

Tools:

- `flask_status`, `flask_restart`
- `live_scoreboard`, `live_boxscore`
- `workflow_status`, `workflow_trigger`
- `shell_exec`, `read_file` (both require the MCP auth token)

MCP venv: `~/mcp-venv`. `WorkingDirectory` for the service is `/home/schnapp-admin/sports-modeling` (direct clone, not the actions-runner work dir). Any change to `mcp/server.py` requires triggering `install-mcp.yml`; the redeploy completes in ~18-30 seconds.

Auth is via the Cloudflare tunnel credential plus the shared MCP token for `shell_exec` / `read_file`. No bearer token on top of the tunnel because the claude.ai connector UI supports OAuth fields only.

### Cloudflare subdomains in front of the VM

| Subdomain | Backend | Purpose |
|-----------|---------|---------|
| `mcp.schnapp.bet` | MCP server on `127.0.0.1:8000` via named tunnel | Claude.ai MCP connector |
| `live.schnapp.bet` | Flask on `0.0.0.0:5000` | Web app live-data routes (`/api/games`, `/api/scoreboard`, `/api/live-boxscore`) |
| `schnapp.bet`, `www.schnapp.bet` | Azure Static Web Apps | The web app itself (DNS-only, not proxied) |

Different proxy modes: the web app domains are **DNS-only** (Azure SWA issues SSL and needs the client to hit Azure directly). The Flask and MCP subdomains are **Cloudflare-proxied** (Cloudflare terminates SSL and tunnels to the VM). Do not flip these without understanding why.

### Failure modes and recovery

- Tunnel down (every `shell_exec` returns without output): `sudo systemctl restart cloudflared && sudo systemctl restart schnapp-mcp` on the VM
- Runner offline: check the runner service with `sudo systemctl status actions.runner.SchnappAPI-sports-modeling.schnapp-runner.service`. `Restart=always` covers most crashes
- MCP out of date after code change: trigger `install-mcp.yml`
- Azure SQL auto-pause cold start: first connection 20-60s. The 3-retry, 45-second-wait pattern in ETL handles this
- Azure SWA deploys showing "Deployment Canceled" on older runs when a newer commit supersedes them is expected and not a real failure

### Azure Static Web Apps

- Resource `sports-modeling-web`
- Default URL: `https://red-smoke-0bbe1fb10.2.azurestaticapps.net`
- Custom domains: `schnapp.bet`, `www.schnapp.bet`. Cloudflare DNS-only (not proxied). SSL active
- Deploys auto-trigger on push to `main`, complete in ~90 seconds
- Next.js 15.2.8, React 19
- App settings: `AZURE_SQL_CONNECTION_STRING`, `GITHUB_PAT` (workflow scope, used by refresh routes), `ADMIN_REFRESH_CODE` (four-step refresh passcode)

### PWA

- Manifest: `web/public/manifest.json`. Name "Schnapp". Start URL `/nba`. Standalone display
- Service worker: `web/public/sw.js`. Network-first for HTML, cache-first for static assets, never caches API routes
- Icon: `web/public/icon.svg` with `sizes: "any"` covers all modern browsers

### Keep-alive

Uptime Robot pings `https://schnapp.bet/api/ping` (the SWA API route, not the Flask `/ping`) every 30 minutes. The web `/api/ping` runs `SELECT 1` against Azure SQL and keeps the DB from pausing during active hours. It replaces a previous `keepalive.yml` workflow that consumed runner minutes; the workflow is now dispatch-only.

### Secrets catalog

GitHub repository secrets: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`, `NBA_PROXY_URL`, `ODDS_API_KEY`, `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10`, `GITHUB_PAT`, `MCP_AUTH_TOKEN`, `GH_PAT`.

Use `GH_PAT`, not `GITHUB_PAT`, for new workflow-referenced tokens. GitHub reserves the `GITHUB_` prefix for built-in secrets and workflow inputs are not masked in logs.

## Invariants

- ETL secrets live in GitHub repository secrets or the VM's systemd environment files. Never hardcoded
- Web routes that call Flask use `https://live.schnapp.bet` (Cloudflare-proxied). Never hardcode VM IPs in web code
- Changes to `mcp/server.py` require triggering `install-mcp.yml` to redeploy
- `cloudflared` and `schnapp-mcp` run as systemd services; the recovery pattern is restart both
- Runner systemd service has `Restart=always`
- Flask listens on `0.0.0.0:5000` (all interfaces). MCP binds to `127.0.0.1:8000` only and is exposed via the Cloudflare tunnel
- Flask `/ping` is unauthenticated by design. `/scoreboard` and `/boxscore` require `X-Runner-Key`
- `schnapp-mcp.service` `WorkingDirectory` is `/home/schnapp-admin/sports-modeling` (direct clone), not the actions-runner work dir
- Cloudflare DNS for `schnapp.bet` and `www.schnapp.bet` is DNS-only (not proxied). Azure SWA needs direct DNS resolution for SSL issuance. `mcp.schnapp.bet` and `live.schnapp.bet` are Cloudflare-proxied
- Uptime Robot replaces `keepalive.yml`. Do not reintroduce a scheduled keep-alive workflow
- Azure SWA "Deployment Canceled" on older runs when superseded by a newer commit is expected. Not a real failure

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[infra]`. Historical entries before the restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- Whether to formalize runbooks for common operations (Flask restart, tunnel restart, VM reboot, Odds API key rotation)
- Whether to add health-check automation beyond the current Uptime Robot ping
