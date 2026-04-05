# Schnapp MCP Server

Remote MCP server for schnapp.bet operational tools. Runs on the schnapp-runner VM and is exposed to Claude via Cloudflare Tunnel.

## Tools

| Tool | Description |
|------|-------------|
| `flask_status` | Check if schnapp-flask.service is running |
| `flask_restart` | Restart schnapp-flask.service |
| `live_scoreboard` | Today's NBA game statuses from CDN |
| `live_boxscore` | Live player stats for a game ID |
| `workflow_trigger` | Trigger a GitHub Actions workflow |
| `workflow_status` | Check the last run status of a workflow |

## Setup

Run the install workflow from GitHub Actions:

```
Actions > Install MCP Server > Run workflow
```

This installs the venv, service file, and starts the service. You then set up Cloudflare Tunnel separately (see below).

## Cloudflare Tunnel

After install, run on the VM:

```bash
cloudflared tunnel login
cloudflared tunnel create schnapp-mcp
cloudflared tunnel route dns schnapp-mcp mcp.schnapp.bet
```

Then add the tunnel URL to claude.ai Settings > Connectors.

## Environment variables (in systemd service)

- `MCP_AUTH_TOKEN` — Bearer token for Claude to authenticate
- `RUNNER_API_KEY` — Key for Flask runner (runner-Lake4971)
- `GITHUB_PAT` — Personal access token with workflow scope
