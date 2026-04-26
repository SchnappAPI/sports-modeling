# Connections

This file is the single source of truth for every external system the project connects to. For each system it documents the resource, the credentials that access it, the name under which each credential is stored, and every location that stores it.

Secret values are inlined. This repo is private; if it ever becomes public, rotate every secret in this file before exposing the repo.

Last verified against Azure Portal and VM state: 2026-04-24.

## Azure subscription

Account login: `API@schnapp.bet`.
Subscription name: `sports-modeling-subscription`.
Subscription ID: `a8dd9b1d-cb98-4dad-a73f-d42c4bfce6a8`.
Primary resource group: `sports-modeling` in Central US. Contains all ten application resources.
Secondary resource group: `NetworkWatcherRG` (Azure-managed, holds `NetworkWatcher_centralus`; no operator action needed).

All application resources are in Central US. The VM, SQL server, and SWA are colocated to minimize query latency.

## Azure SQL Database

Server: `sports-modeling-server.database.windows.net`
Database: `sports-modeling`
Tier: General Purpose Serverless, Gen5, 2 vCores. Auto-pauses after 60 minutes of idle.
Driver: ODBC Driver 18, accessed via SQLAlchemy + pyodbc
Region: Central US (geo-secondary East US 2)
Max storage: 32 GB
Collation: `SQL_Latin1_General_CP1_CI_AS`
Backups: 7-day PITR, differential every 12 hours, locally-redundant storage

Schemas: `nba`, `mlb`, `nfl`, `odds`, `common`

Cold start: 20 to 60 seconds on first connection after auto-pause. ETL retries 3 times with 45-second waits.

Web API routes must degrade gracefully on cold start. A cold start exceeds the 15-second SWA function timeout, so any route that queries the DB should treat 500 responses as non-fatal (pattern used in `/api/player-grades` after the 2026-04-23 fix).

Admin login:
- User: `sqladmin`
- Password: `Sports#2026`
- Authentication: SQL authentication only (no Microsoft Entra admin configured)
- Minimum TLS version: 1.2

Connection strings:
- ODBC (Python ETL): `Driver={ODBC Driver 18 for SQL Server};Server=tcp:sports-modeling-server.database.windows.net,1433;Database=sports-modeling;Uid=sqladmin;Pwd=Sports#2026;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;`
- .NET (Azure SWA API routes): `Server=sports-modeling-server.database.windows.net;Database=sports-modeling;User Id=sqladmin;Password=Sports#2026;Encrypt=true;TrustServerCertificate=false;`

Credentials (where each secret is stored):
- `AZURE_SQL_SERVER` (value: `sports-modeling-server.database.windows.net`): GitHub Actions repository secret
- `AZURE_SQL_DATABASE` (value: `sports-modeling`): GitHub Actions repository secret
- `AZURE_SQL_USERNAME` (value: `sqladmin`): GitHub Actions repository secret
- `AZURE_SQL_PASSWORD` (value: `Sports#2026`): GitHub Actions repository secret
- `AZURE_SQL_CONNECTION_STRING` (value: the .NET string above): Azure SWA environment variable
- VM: none. Workflows on the self-hosted runner read these from GitHub Actions secrets at runtime; verified 2026-04-24 that no `AZURE_SQL_*` values live in any systemd service file or `.env` on the VM.

Firewall (public access enabled, 2 rules):
- `ClientIp-2026-3-12_22-4-5`: `23.126.39.201` (home IP, added for local development)
- `GitHub-Actions-All`: `0.0.0.0` to `255.255.255.255` (allows all internet)

"Allow Azure services and resources to access this server" exception is enabled.

The wide-open rule is required because GitHub-hosted runners use rotating IPs. Most ETL now runs on the self-hosted runner with a static IP (`172.173.126.81`), so the rule could be tightened to the VM IP plus the home IP if GitHub-hosted runners are no longer a consideration.

Keep-alive: Uptime Robot monitor `schnapp-bet-ping` is paused as of 2026-04-23. It previously pinged `https://schnapp.bet/api/ping` every 30 minutes to prevent the database from ever reaching its auto-pause idle threshold. The monitor was paused to let the 60-minute auto-pause take effect and cut continuous-compute billing (was running at ~$181/month in April 2026). Cold-start latency on first request after idle is the accepted tradeoff. Resume the monitor if a paying user tier ever requires warm DB response times.

## DB queries from Claude sessions

MSSQL MCP is not available on the corporate machine (ThreatLocker blocks it). Do not attempt to use it. For ad-hoc DB queries during a session, write a Python script to `/tmp/` via `shell_exec` and execute it with `~/venv/bin/python`. Connection details: server `sports-modeling-server.database.windows.net,1433`, database `sports-modeling`, uid `sqladmin`, password `Sports#2026`. Use pyodbc directly for diagnostic queries.

The database auto-pauses after 60 minutes of idle. If the first query in a session fails with a timeout or connection error, wait 20 to 60 seconds and retry; the query likely triggered a cold start.

## Azure Static Web Apps

Resource: `sports-modeling-web`
Location: Central US (content delivery is global via Microsoft's edge network)
SKU: Free
Status: Ready (Production)
Default URL: `https://red-smoke-0bbe1fb10.2.azurestaticapps.net`
Custom domains: `schnapp.bet`, `www.schnapp.bet` (both Validated; managed SSL certs expire 2026-10-01 18:59:59 UTC)
Stable inbound IP: `20.84.233.22`
DNS: Cloudflare DNS-only (not proxied; required for Azure SSL issuance)
Deployment authorization policy: DeploymentToken
Staging environment policy: Enabled (PR preview environments supported)
Enterprise-grade CDN: Disabled (Cloudflare handles edge caching)

Deploy trigger: any push to `main`. Workflow: `.github/workflows/azure-static-web-apps-red-smoke-0bbe1fb10.yml`. Builds and deploys `/web/` only. Pushes outside `/web/` still trigger a deploy run, but the build is a no-op. "Deployment Canceled" on superseded runs is expected Azure behavior, not a failure.

Build details:
- App location: `./web`
- API location: (empty, not using SWA managed API)
- App artifact location: `build`

App settings (Azure portal → sports-modeling-web → Environment variables):
- `ADMIN_PASSCODE` = `Sports#2026` — admin passcode gate on site. Same string as the SQL admin password by design.
- `ADMIN_REFRESH_CODE` = `GO` — Refresh Data button passcode.
- `AUTH_TOKEN_SECRET` = `schnapp-secret-2026-xk9` — session token signing secret.
- `AZURE_SQL_CONNECTION_STRING` = the .NET connection string from the SQL section — DB access from API routes.
- `GITHUB_PAT` = fine-grained PAT from the GitHub section — used by API routes to dispatch workflows, e.g. the Refresh Data button.

## GitHub Actions

Repo: `SchnappAPI/sports-modeling` (private)
Default branch: `main`

Self-hosted runner:
- Registration name in GitHub Actions: `schnapp-runner`
- Host VM: `schnapp-runner-2` (Central US, Standard B1s, Ubuntu 24.04, public IP `172.173.126.81`, private IP `10.0.0.4`, admin user `schnapp-admin`, created 2026-04-10)
- Runner service: `actions.runner.SchnappAPI-sports-modeling.schnapp-runner.service`, systemd, `Restart=always`
- Python venvs: `~/venv` for ETL (pinned deps + ODBC Driver 18 pre-installed), `~/mcp-venv` for the MCP server
- Most workflows declare `runs-on: [self-hosted, schnapp-runner]`

The VM was renamed from `schnapp-runner` to `schnapp-runner-2` during a West US 2 → Central US migration. The runner's registration name (`schnapp-runner`) was kept so workflow `runs-on` labels did not need to change.

VM filesystem landmarks:
- `/home/schnapp-admin/sports-modeling` — repo clone used by `schnapp-mcp.service`
- `/home/schnapp-admin/actions-runner` — GitHub Actions runner install directory
- `/home/schnapp-admin/actions-runner/_work/...` — runner working directory for each workflow job
- `/home/schnapp-admin/.git-credentials` — PAT used for git push from VM (token is `schnapp-vm-push`, a separate fine-grained PAT; not the `GITHUB_PAT` used elsewhere)
- `/swapfile` — 1 GB swap, `swappiness=80`, persistent across reboot

VM network security group (`schnapp-runner-2-nsg`, attached at NIC level): inbound allows only SSH on port 22 (priority 300) plus Azure defaults. No inbound port for Flask (5000) or MCP (8000) — those are reachable externally only via the outbound Cloudflare tunnel. Outbound allows all internet.

Repository secrets (settings → Secrets and variables → Actions):
- `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD` (values in the Azure SQL Database section)
- `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10` for SWA deploy
- `NBA_PROXY_URL` = `http://bfoopdzv-rotate:eftihw9lhmd7@p.webshare.io:80/` for stats.nba.com calls (Webshare rotating residential proxy)
- `ODDS_API_KEY` = `e79c9e6b3d9a5e7166602935ee0fb9f6` for The Odds API
- `GH_PAT` holds the same value as `GITHUB_PAT` in Azure SWA and in the VM's `schnapp-mcp.service` (see Personal Access Tokens below)
- `MCP_AUTH_TOKEN` for `install-mcp.yml` (value in the Schnapp Ops MCP section)

Reserved prefix note: GitHub will not accept new secrets starting with `GITHUB_`. The `GITHUB_PAT` name is only used in Azure SWA; the GitHub Actions equivalent is named `GH_PAT`. They hold the same token value.

Personal Access Tokens (account level, at `github.com/settings/tokens`):
- `GITHUB_PAT` — fine-grained; scoped to SchnappAPI/sports-modeling; permissions Metadata read + Actions read and write; expires 2027-03-29. Stored in three places: Azure SWA env var `GITHUB_PAT`, GitHub Actions secret `GH_PAT`, VM `schnapp-mcp.service` `Environment="GH_PAT=..."` directive. All three must be updated together on rotation.
- `schnapp-vm-push` — fine-grained; used by the VM for direct git push (stored at `/home/schnapp-admin/.git-credentials`); expires 2027-04-10.
- `Claude Code MCP - sports-modeling` — fine-grained; scoped to all SchnappAPI repos; permissions Actions/Contents/Issues/Pull requests/Workflows read+write, Metadata read; expires 2027-04-25. Stored in `~/.claude.json` on the Windows laptop (1stLake) under MCP server `github` as `GITHUB_PERSONAL_ACCESS_TOKEN`. Local-only; not synced anywhere.
- `Claude MCP` — classic; used by the Claude.ai GitHub connector for browser sessions.

Active workflows live in `.github/workflows/`. Workflow-level details (schedules, inputs, per-workflow behavior) are documented in each workflow file and in `/infrastructure/README.md`; this document only catalogs the secrets those workflows consume.

## Schnapp Ops MCP

URL: `https://mcp.schnapp.bet/mcp`
Tunnel: Cloudflare named tunnel, tunnel ID `6725bd14-5cd9-480a-8420-618f50e96b69`, config at `/home/schnapp-admin/.cloudflared/config.yml` on the VM, routing `mcp.schnapp.bet` to `127.0.0.1:8000`
Service: `schnapp-mcp.service` (systemd, `Restart=always`, `RestartSec=5`)
Code: `mcp/server.py` (FastMCP)
Working directory: `/home/schnapp-admin/sports-modeling` (direct clone, not the actions-runner work dir, so the service boots independently of any workflow run)
Python venv: `/home/schnapp-admin/mcp-venv`

Tools:
- `flask_status`, `flask_restart` (no auth)
- `live_scoreboard`, `live_boxscore` (no auth, calls Flask runner internally)
- `workflow_trigger`, `workflow_status` (uses `GH_PAT` from VM env)
- `shell_exec`, `read_file` (require `token` parameter matching `MCP_AUTH_TOKEN` env var)

Credentials (set via `Environment=` directives in `/etc/systemd/system/schnapp-mcp.service`):
- `MCP_AUTH_TOKEN` = `da1c12150e2f7b784d423f9e1865bf78503fcc5d34f5d710446845d898b54f48`. Also stored in GitHub Actions secret `MCP_AUTH_TOKEN` (used by `install-mcp.yml`). Both locations must be updated together on rotation.
- `RUNNER_API_KEY` = `runner-Lake4971`. Must match the identical value in `schnapp-flask.service` so MCP can call Flask on the VM.
- `GH_PAT` = the fine-grained PAT. Must match GitHub Actions secret `GH_PAT` and Azure SWA env var `GITHUB_PAT`; all three rotate together.

Connected in claude.ai as the "Schnapp Ops" connector.

Recovery if tool calls return no output or error out before the VM logs a response:
1. SSH to VM: `ssh schnapp-admin@172.173.126.81`
2. Restart tunnel: `sudo systemctl restart cloudflared`
3. Restart MCP: `sudo systemctl restart schnapp-mcp`
4. If still broken, reconnect the Schnapp Ops connector in claude.ai (Settings → Connectors → Disconnect → Connect). The claude.ai side of the connector can hold a stale session state even when the tunnel and service are healthy.

After any change to `mcp/server.py`, trigger `install-mcp.yml` to redeploy (typically 18 to 30 seconds).

## Flask Runner on VM

Service: `schnapp-flask.service` (systemd, `Restart=always`, `RestartSec=5`)
Code: `etl/runner.py`
Bind address: `0.0.0.0:5000` (all interfaces)
Access paths:
- Internal (from the VM): `http://127.0.0.1:5000`
- Private VNet: `http://10.0.0.4:5000`
- Public (via Cloudflare tunnel): `https://live.schnapp.bet`

Endpoints:
- `GET /ping` — health check, no auth. Returns `{"ok": true}`. Used by the MCP `flask_status` tool.
- `GET /scoreboard` — today's NBA game statuses from `cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json`. Requires `X-Runner-Key` header.
- `GET /boxscore?gameId=` — live player stats from `cdn.nba.com/static/json/liveData/boxscore/boxscore_{gameId}.json`. Requires `X-Runner-Key` header.

Auth: `X-Runner-Key: runner-Lake4971` matches `RUNNER_API_KEY` env var. Enforced on `/scoreboard` and `/boxscore`; `/ping` is unauthenticated by design so health-check callers can hit it without a secret.

Credentials (set via `Environment=` directives in `/etc/systemd/system/schnapp-flask.service`):
- `RUNNER_API_KEY` = `runner-Lake4971`. Must match the identical value in `schnapp-mcp.service`.
- `NBA_PROXY_URL` = `http://bfoopdzv-rotate:eftihw9lhmd7@p.webshare.io:80/`. Present in the env but unused by the Flask runner (the CDN endpoints it queries are public and require no proxy).

Web routes that call Flask use `https://live.schnapp.bet`, never a hardcoded VM IP. If the VM's public IP changes, Cloudflare DNS is updated and web code keeps working with no change.

## Cloudflare

Cloudflare handles DNS for `schnapp.bet` and runs the named tunnel that exposes Flask and the MCP server. Four subdomains, intentionally split between proxy modes:

| Subdomain | Backend | Proxy mode | Why |
|---|---|---|---|
| `schnapp.bet` | Azure SWA (`sports-modeling-web`) | DNS-only (grey cloud) | Azure SWA requires direct DNS resolution to issue and renew SSL certs |
| `www.schnapp.bet` | Azure SWA (`sports-modeling-web`) | DNS-only (grey cloud) | Same as apex |
| `live.schnapp.bet` | VM Flask on `:5000` via tunnel | Proxied (orange cloud) | Cloudflare terminates SSL and tunnels to the VM |
| `mcp.schnapp.bet` | VM MCP on `:8000` via tunnel | Proxied (orange cloud) | Cloudflare terminates SSL and tunnels to the VM |

Tunnel:
- Tunnel ID: `6725bd14-5cd9-480a-8420-618f50e96b69`
- Tunnel config: `/home/schnapp-admin/.cloudflared/config.yml` on the VM
- Tunnel service: `cloudflared.service` (systemd, `Restart=always`)
- Protocol: QUIC, multiple outbound connections to Cloudflare edge nodes

Invariants:
- Never flip `schnapp.bet` or `www.schnapp.bet` to Proxied — Azure SSL issuance breaks.
- Never flip `live.` or `mcp.` to DNS-only — the tunnel stops routing externally.

Because the tunnel is outbound from the VM, no inbound NSG port is opened for Flask (5000) or MCP (8000). The NSG inbound rules allow only SSH on 22.

## Other MCPs (per-environment availability)

- **GitHub MCP**: scope locked to `SchnappAPI/sports-modeling`, branch `main`. Available in every Claude session.
- **Power BI MCP** (`powerbi-modeling-mcp`): used for PBI work. Auto-connect to the local instance whose `parentWindowTitle` is `sports-model`.
- **Filesystem MCP**: Windows machine paths under `C:\Users\1stLake\OneDrive - Schnapp\` and adjacent allowed directories. Used for accessing local data files.
- **Desktop Commander**: available on corporate machine. Used for file operations and config editing.
- **windows-node-mcp**: available on corporate machine.

## External APIs

### NBA Stats API (stats.nba.com)
- Requires Webshare rotating residential proxy from GitHub Actions IPs. Datacenter proxies and direct Azure IPs have been observed to be blocked.
- Proxy URL value: `http://bfoopdzv-rotate:eftihw9lhmd7@p.webshare.io:80/`. Stored as GitHub Actions secret `NBA_PROXY_URL` and as an `Environment=` directive in `/etc/systemd/system/schnapp-flask.service` (present in the env but unused by Flask).
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
- API key: `e79c9e6b3d9a5e7166602935ee0fb9f6`. Stored as GitHub Actions secret `ODDS_API_KEY`.
- FanDuel only (`bookmakers=fanduel`). Other books deferred (see ROADMAP and DECISIONS).
- NBA sport key: `basketball_nba`.
- `includeLinks=true` is valid only on the per-event endpoint, not bulk.

### nflverse via nflreadpy (NFL)
- Public, no auth.
- Python package: `nflreadpy` 0.1.5.
- `update_config(cache_mode='off')` is called at the top of every ETL run because GitHub Actions runners have no persistent filesystem.
- Upstream source: nflverse community-maintained data pipeline (multiple feeder sources; see ADR-0015).

## Local development

Corporate laptop (Windows): runs Node.js 24.12.0. `npm run dev` and local Python execution are both blocked by ThreatLocker. Test code changes by pushing to `main` and waiting for the SWA deploy.

Repo path on laptop: `C:\Users\1stLake\sports-modeling`. Git push works.

Repo path on VM: `/home/schnapp-admin/sports-modeling`. This is the working directory for `schnapp-mcp.service`. `git pull` on VM is the standard way to sync after a push from the laptop. Direct `git push` from the VM also works via the PAT stored at `/home/schnapp-admin/.git-credentials` (see ADR-20260422-2).

Azure Cloud Shell: available via the Azure Portal as a fallback when ThreatLocker blocks something locally. Cloud Shell runs in an ephemeral container and does not allow `sudo`, but you can SSH into the VM from it and run admin commands there.

MacBook Pro (Schnapps-MBP, pilot): live as of 2026-04-26 for limited use. Hosts a SQL Server 2022 Docker container on Colima with the full `sports-modeling` database imported from BACPAC (14.8M rows, row-count parity verified vs Azure SQL). The container has `max server memory (MB) = 4500` configured (sp_configure, persists in the master DB inside the named volume `mssql-data`); Colima's allocation is 6 GiB total, container limit ~5.78 GiB, so 4500 MB caps SQL Server's working set with ~1.4 GiB headroom for the host VM. Also hosts a second self-hosted GitHub Actions runner (`mac-runner-1`, label `mac-runner`) for the migration evaluation; launchd plist has `RunAtLoad` and `KeepAlive` both set. Python 3.12 venv at `/Users/schnapp/venv` with the same pinned deps as the VM. ODBC Driver 18 + unixODBC installed via Microsoft's Homebrew tap. SQL credentials at `/Users/schnapp/sql-server.env` (`MSSQL_SA_PASSWORD`); only the local DB uses the SA account. Workflows targeting this host source that file and re-export `MSSQL_SA_PASSWORD` as `AZURE_SQL_PASSWORD` (plus `AZURE_SQL_SERVER=localhost,1433`, `AZURE_SQL_DATABASE=sports-modeling`, `AZURE_SQL_USERNAME=sa`, `AZURE_SQL_TRUST_CERT=yes`) so production scripts run unmodified; pattern in ADR-20260426-2. Schnapp Mac MCP at `https://mac-mcp.schnapp.bet/mcp` provides shell_exec / read_file / write_file tools for remote management. Four workflows currently target this host: `mac-runner-pilot.yml` (read-only), `db_inventory-mac.yml` (read-only), `odds-etl-mac.yml` (write-path; manual dispatch only), and `compute-patterns-mac.yml` (write-path; manual dispatch only). The VM remains authoritative for every scheduled and write-path workflow.
