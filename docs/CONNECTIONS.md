# Connections

This file is the single source of truth for every external system the project connects to. For each system it documents the resource, the credentials that access it, the name under which each credential is stored, and every location that stores it.

Secret values are inlined. This repo is private; if it ever becomes public, rotate every secret in this file before exposing the repo.

Last verified against Azure Portal and VM state: 2026-04-24.

## Azure SQL Database

Server: `sports-modeling-server.database.windows.net`
Database: `sports-modeling`
Tier: General Purpose Serverless (auto-pauses)
Driver: ODBC Driver 18, accessed via SQLAlchemy + pyodbc

Schemas: `nba`, `mlb`, `nfl`, `odds`, `common`

Cold start: 20 to 60 seconds on first connection after auto-pause. ETL retries 3 times with 45-second waits.

Credentials:
- GitHub Actions: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD` repository secrets
- Azure SWA app: `AZURE_SQL_CONNECTION_STRING` app setting
- VM: same secrets exported via systemd environment file

Firewall: allows all IPs (0.0.0.0 to 255.255.255.255) plus Azure Services. Required because GitHub-hosted runners use rotating IPs even though most ETL now runs on the self-hosted runner.

Keep-alive: Uptime Robot pings `https://schnapp.bet/api/ping` every 30 minutes to prevent auto-pause.

## DB queries from Claude sessions

MSSQL MCP is not available on the corporate machine (ThreatLocker blocks it). Do not attempt to use it. For ad-hoc DB queries during a session, write a Python script to `/tmp/` via `shell_exec` and execute it with `~/venv/bin/python`. Connection string: server `sports-modeling-server.database.windows.net,1433`, database `sports-modeling`, uid `sqladmin`, password in VM systemd env as `AZURE_SQL_PASSWORD`. Use pyodbc directly for diagnostic queries.

## Azure Static Web Apps

Resource: `sports-modeling-web`
Default URL: `https://red-smoke-0bbe1fb10.2.azurestaticapps.net`
Custom domains: `schnapp.bet`, `www.schnapp.bet`
DNS: Cloudflare DNS-only (not proxied; required for Azure SSL issuance)

Deploy trigger: any push to `main`. Workflow: `.github/workflows/azure-static-web-apps-red-smoke-0bbe1fb10.yml`. Builds and deploys `/web/` only. Pushes outside `/web/` still trigger a deploy run, but the build is a no-op.

App settings (configured in Azure portal):
- `AZURE_SQL_CONNECTION_STRING` for DB access from API routes
- `GITHUB_PAT` (workflow scope) for triggering Actions from API routes
- `ADMIN_REFRESH_CODE` for the admin refresh button passcode

## GitHub Actions

Repo: `SchnappAPI/sports-modeling` (private)
Default branch: `main`
Self-hosted runner: `schnapp-runner` on Azure VM (Central US, B1s, Ubuntu 24.04). Persistent systemd service. Python venv at `~/venv` with pinned dependencies and ODBC Driver 18 pre-installed. Most workflows declare `runs-on: [self-hosted, schnapp-runner]`.

Repository secrets:
- `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`
- `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10` for SWA deploy
- `NBA_PROXY_URL` for stats.nba.com calls (Webshare rotating residential proxy)
- `ODDS_API_KEY` for The Odds API
- `GITHUB_PAT` for cross-workflow triggers (also stored in SWA app settings)
- `MCP_AUTH_TOKEN` for `shell_exec` and `read_file` authorization on the Schnapp Ops MCP
- `GH_PAT` (alias of GITHUB_PAT for older workflow references)

Reserved prefix note: GitHub will not accept new secrets starting with `GITHUB_`. The existing `GITHUB_PAT` is grandfathered. New cross-workflow tokens should use the `GH_` prefix.

## Schnapp Ops MCP

URL: `https://mcp.schnapp.bet/mcp`
Tunnel: Cloudflare named tunnel routing to `127.0.0.1:8000` on the VM
Backend: FastMCP server (`mcp/server.py`) running as `schnapp-mcp.service` (systemd)

Tools:
- `flask_status`, `flask_restart` (no auth)
- `live_scoreboard`, `live_boxscore` (no auth, calls Flask runner internally)
- `workflow_trigger`, `workflow_status` (uses `GH_PAT` from VM env)
- `shell_exec`, `read_file` (require `token` parameter matching `MCP_AUTH_TOKEN` env var)

Token: `da1c12150e2f7b784d423f9e1865bf78503fcc5d34f5d710446845d898b54f48`

Connected in claude.ai as the "Schnapp Ops" connector.

If every `shell_exec` call returns no output, the Cloudflare tunnel is down. Recovery: SSH to VM, then `sudo systemctl restart cloudflared` and `sudo systemctl restart schnapp-mcp`. After any change to `mcp/server.py`, trigger `install-mcp.yml` to redeploy (typically 18 to 30 seconds).

## Flask Runner on VM

URL (internal): `http://127.0.0.1:5000` from the VM, `http://172.173.126.81:5000` from Azure SWA API routes
Service: `schnapp-flask.service` (systemd)
Code: `etl/runner.py`

Endpoints:
- `/ping` (health check)
- `/scoreboard` (today's NBA game statuses from CDN)
- `/boxscore?gameId=` (live player stats)

Auth: every request requires the `X-Runner-Key` header matching `RUNNER_API_KEY` (default value defined in `mcp/server.py`).

## Other MCPs (per-environment availability)

- **GitHub MCP**: scope locked to `SchnappAPI/sports-modeling`, branch `main`. Available in every Claude session.
- **Power BI MCP** (`powerbi-modeling-mcp`): used for PBI work. Auto-connect to the local instance whose `parentWindowTitle` is `sports-model`.
- **Filesystem MCP**: Windows machine paths under `C:\Users\1stLake\OneDrive - Schnapp\` and adjacent allowed directories. Used for accessing local data files.
- **Desktop Commander**: available on corporate machine. Used for file operations and config editing.
- **windows-node-mcp**: available on corporate machine.

## External APIs

### NBA Stats API (stats.nba.com)
- Requires Webshare rotating residential proxy from GitHub Actions IPs. Proxy URL stored in `NBA_PROXY_URL`.
- PT stats (`leaguedashptstats`) do not require proxy.

### NBA CDN (cdn.nba.com)
- Public, no proxy, no auth. Used for live scoreboard and live box scores via Flask `/scoreboard` and `/boxscore`.
- Endpoints: `todaysScoreboard_00.json`, `boxscore_{game_id}.json` under `/static/json/liveData/`.

### MLB Stats API (statsapi.mlb.com)
- Public, no auth. Primary source for MLB schedule, box scores, play-by-play, season stats, splits.
- Main game endpoint: `/api/v1/game/{gameID}/withMetrics` returns box score plus play-by-play in one call.

### Baseball Savant (baseballsavant.mlb.com)
- Public, no auth. Source for Statcast pitch-level data and career batter-vs-pitcher matchup stats.
- Endpoints: `/statcast_search`, `/gf?game_pk=`.

### The Odds API (api.the-odds-api.com)
- Auth: `ODDS_API_KEY` repository secret.
- FanDuel only (`bookmakers=fanduel`). Other books deferred (see ROADMAP and DECISIONS).
- `includeLinks=true` is valid only on the per-event endpoint, not bulk.

## Local development

Laptop runs Node.js 24.12.0. `npm run dev` is blocked by ThreatLocker on the corporate machine. Test by pushing to `main` and waiting for the SWA deploy.

Repo path on laptop: `C:\Users\1stLake\sports-modeling`. Git push works.

Repo path on VM: `/home/schnapp-admin/sports-modeling`. This is the working directory for `schnapp-mcp.service`. `git pull` on VM is the standard way to sync after a push from the laptop.
