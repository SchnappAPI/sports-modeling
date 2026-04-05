"""
schnapp_mcp/server.py

Remote MCP server for schnapp.bet operational tools.
Runs on the schnapp-runner VM alongside the Flask runner.
Exposed to Claude via Cloudflare Tunnel.

Tools:
  flask_status     -- Is the Flask service running? Uptime, last restart.
  flask_restart    -- Restart schnapp-flask.service.
  live_scoreboard  -- Today's NBA game statuses from CDN via Flask.
  live_boxscore    -- Live player stats for a specific game.
  workflow_trigger -- Trigger a GitHub Actions workflow by filename.
  workflow_status  -- Check the last run status of a workflow.

Auth: Bearer token in Authorization header (MCP_AUTH_TOKEN env var).

Start: uvicorn mcp.server:app --host 0.0.0.0 --port 8000
Managed by: systemd (schnapp-mcp.service)
"""

import os
import subprocess
import requests
from datetime import datetime, timezone
from mcp.server.fastmcp import FastMCP

MCP_AUTH_TOKEN  = os.environ.get("MCP_AUTH_TOKEN", "")
RUNNER_KEY      = os.environ.get("RUNNER_API_KEY", "runner-Lake4971")
FLASK_BASE      = "http://localhost:5000"
GITHUB_PAT      = os.environ.get("GITHUB_PAT", "")
GITHUB_REPO     = "SchnappAPI/sports-modeling"
GITHUB_API      = "https://api.github.com"

mcp = FastMCP(
    name="schnapp-ops",
    instructions="Operational tools for schnapp.bet: Flask service management, live NBA data, and GitHub Actions workflow control.",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flask_headers():
    return {"X-Runner-Key": RUNNER_KEY}


def _github_headers():
    return {
        "Authorization": f"Bearer {GITHUB_PAT}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _run(cmd: list[str]) -> tuple[int, str]:
    """Run a shell command and return (returncode, output)."""
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    output = (result.stdout + result.stderr).strip()
    return result.returncode, output


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def flask_status() -> dict:
    """
    Check the status of the schnapp-flask systemd service.
    Returns whether it is running, the active state, and recent log lines.
    """
    code, output = _run(["sudo", "systemctl", "status", "schnapp-flask.service", "--no-pager", "-l"])
    is_active = "Active: active (running)" in output

    # Also ping Flask directly
    flask_ok = False
    try:
        resp = requests.get(f"{FLASK_BASE}/ping", headers=_flask_headers(), timeout=5)
        flask_ok = resp.status_code == 200 and resp.json().get("ok") is True
    except Exception:
        pass

    return {
        "service_running": is_active,
        "flask_ping_ok": flask_ok,
        "systemctl_output": output[:2000],
    }


@mcp.tool()
def flask_restart() -> dict:
    """
    Restart the schnapp-flask systemd service.
    Waits 3 seconds then pings /ping to confirm it came back up.
    """
    code, output = _run(["sudo", "systemctl", "restart", "schnapp-flask.service"])
    if code != 0:
        return {"success": False, "error": output}

    import time
    time.sleep(3)

    try:
        resp = requests.get(f"{FLASK_BASE}/ping", headers=_flask_headers(), timeout=5)
        ok = resp.status_code == 200 and resp.json().get("ok") is True
    except Exception as e:
        return {"success": False, "error": f"Restart issued but ping failed: {e}"}

    return {"success": ok, "message": "Flask restarted and ping confirmed." if ok else "Restarted but ping did not respond."}


@mcp.tool()
def live_scoreboard() -> dict:
    """
    Fetch today's NBA game statuses directly from the CDN via the Flask runner.
    Returns game IDs, status (1=pre/2=live/3=final), scores, and clock.
    """
    try:
        resp = requests.get(
            f"{FLASK_BASE}/scoreboard",
            headers=_flask_headers(),
            timeout=15,
        )
        if resp.status_code != 200:
            return {"error": f"Flask returned {resp.status_code}"}
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def live_boxscore(game_id: str) -> dict:
    """
    Fetch live player stats for a specific NBA game from the CDN via the Flask runner.
    game_id: NBA game ID string, e.g. '0022501234'
    Returns player stats, scores, and game status.
    """
    try:
        resp = requests.get(
            f"{FLASK_BASE}/boxscore",
            headers=_flask_headers(),
            params={"gameId": game_id},
            timeout=15,
        )
        if resp.status_code != 200:
            return {"error": f"Flask returned {resp.status_code}"}
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def workflow_trigger(workflow_filename: str, ref: str = "main") -> dict:
    """
    Trigger a GitHub Actions workflow by its filename.
    workflow_filename: e.g. 'restart-flask.yml', 'refresh-data.yml', 'nba-game-day.yml'
    ref: branch to run on (default: main)
    """
    if not GITHUB_PAT:
        return {"error": "GITHUB_PAT not configured on MCP server"}

    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/actions/workflows/{workflow_filename}/dispatches"
    resp = requests.post(
        url,
        headers=_github_headers(),
        json={"ref": ref},
        timeout=15,
    )
    if resp.status_code == 204:
        return {"success": True, "message": f"Workflow '{workflow_filename}' triggered on {ref}."}
    return {"success": False, "status_code": resp.status_code, "error": resp.text[:500]}


@mcp.tool()
def workflow_status(workflow_filename: str) -> dict:
    """
    Get the status of the most recent run of a GitHub Actions workflow.
    workflow_filename: e.g. 'nba-game-day.yml', 'grading.yml'
    Returns status, conclusion, run time, and a link to the run.
    """
    if not GITHUB_PAT:
        return {"error": "GITHUB_PAT not configured on MCP server"}

    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/actions/workflows/{workflow_filename}/runs"
    resp = requests.get(
        url,
        headers=_github_headers(),
        params={"per_page": 1},
        timeout=15,
    )
    if resp.status_code != 200:
        return {"error": f"GitHub API returned {resp.status_code}: {resp.text[:200]}"}

    runs = resp.json().get("workflow_runs", [])
    if not runs:
        return {"message": f"No runs found for {workflow_filename}"}

    r = runs[0]
    started  = r.get("run_started_at", "")
    updated  = r.get("updated_at", "")

    # Compute duration if both timestamps available
    duration_seconds = None
    try:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        start_dt = datetime.strptime(started, fmt).replace(tzinfo=timezone.utc)
        end_dt   = datetime.strptime(updated, fmt).replace(tzinfo=timezone.utc)
        duration_seconds = int((end_dt - start_dt).total_seconds())
    except Exception:
        pass

    return {
        "workflow": workflow_filename,
        "run_id": r.get("id"),
        "status": r.get("status"),
        "conclusion": r.get("conclusion"),
        "started_at": started,
        "updated_at": updated,
        "duration_seconds": duration_seconds,
        "url": r.get("html_url"),
        "triggered_by": r.get("event"),
    }
