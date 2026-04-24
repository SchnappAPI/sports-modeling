# Schnapp MCP Server

Remote MCP server for schnapp.bet operational tools. Runs on the schnapp-runner VM and is exposed to Claude via Cloudflare Tunnel.

## Tools

| Tool | Description | Auth |
|------|-------------|------|
| `flask_status` | Check if schnapp-flask.service is running | None |
| `flask_restart` | Restart schnapp-flask.service | None |
| `live_scoreboard` | Today's NBA game statuses from CDN | None |
| `live_boxscore` | Live player stats for a game ID | None |
| `workflow_trigger` | Trigger a GitHub Actions workflow | None (uses `GH_PAT` server-side) |
| `workflow_status` | Check the last run status of a workflow | None |
| `shell_exec` | Run an arbitrary shell command on the VM as the schnapp-mcp user | `token` param must match `MCP_AUTH_TOKEN` |
| `read_file` | Read any file on the VM filesystem | `token` param must match `MCP_AUTH_TOKEN` |

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

- `MCP_AUTH_TOKEN` = `da1c12150e2f7b784d423f9e1865bf78503fcc5d34f5d710446845d898b54f48`. Verified against the `token` parameter passed to `shell_exec` and `read_file` only; other tools have no token check.
- `RUNNER_API_KEY` = `runner-Lake4971`. Sent as `X-Runner-Key` header when the MCP calls the local Flask runner.
- `GH_PAT` = fine-grained GitHub PAT with Actions read/write on `SchnappAPI/sports-modeling`. Used by `workflow_trigger` and `workflow_status`.
