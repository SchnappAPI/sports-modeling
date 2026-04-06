"""
schnapp_mcp/server.py

Remote MCP server for schnapp.bet operational tools.
Runs on the schnapp-runner VM alongside the Flask runner.
Exposed to Claude via Cloudflare Tunnel at https://mcp.schnapp.bet/mcp

Tools:
  flask_status     -- Is the Flask service running?
  flask_restart    -- Restart schnapp-flask.service.
  live_scoreboard  -- Today's NBA game statuses from CDN via Flask.
  live_boxscore    -- Live player stats for a specific game.
  workflow_trigger -- Trigger a GitHub Actions workflow by filename.
  workflow_status  -- Check the last run status of a workflow.
  shell_exec       -- Run an arbitrary shell command on the VM.
  read_file        -- Read a file from the VM filesystem.

Start: python mcp/server.py
Managed by: systemd (schnapp-mcp.service)
Transport: streamable-http on port 8000
"""

import os
import subprocess
import requests
from datetime import datetime, timezone
from mcp.server.fastmcp import FastMCP

RUNNER_KEY  = os.environ.get("RUNNER_API_KEY", "runner-Lake4971")
FLASK_BASE  = "http://localhost:5000"
GH_PAT      = os.environ.get("GH_PAT", "")
MCP_TOKEN   = os.environ.get("MCP_AUTH_TOKEN", "")
GITHUB_REPO = "SchnappAPI/sports-modeling"
GITHUB_API  = "https://api.github.com"

# host and port must be set on the constructor in mcp 1.9.0, not passed to run()
mcp = FastMCP(
    name="schnapp-ops",
    instructions="Operational tools for schnapp.bet: Flask service management, live NBA data, GitHub Actions workflow control, and VM shell access.",
    host="127.0.0.1",
    port=8000,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flask_headers():
    return {"X-Runner-Key": RUNNER_KEY}


def _github_headers():
    return {
        "Authorization": f"Bearer {GH_PAT}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _run(cmd: list[str], timeout: int = 30) -> tuple[int, str]:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return result.returncode, (result.stdout + result.stderr).strip()


def _check_token(token: str) -> bool:
    """Verify the caller supplied the correct MCP_AUTH_TOKEN."""
    return bool(MCP_TOKEN) and token == MCP_TOKEN


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def flask_status() -> dict:
    """Check the status of the schnapp-flask systemd service."""
    code, output = _run(["sudo", "systemctl", "status", "schnapp-flask.service", "--no-pager", "-l"])
    is_active = "Active: active (running)" in output
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
    """Restart the schnapp-flask systemd service and confirm it comes back up."""
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
    """Fetch today's NBA game statuses from the CDN via the Flask runner."""
    try:
        resp = requests.get(f"{FLASK_BASE}/scoreboard", headers=_flask_headers(), timeout=15)
        if resp.status_code != 200:
            return {"error": f"Flask returned {resp.status_code}"}
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def live_boxscore(game_id: str) -> dict:
    """Fetch live player stats for a specific NBA game. game_id e.g. '0022501234'"""
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
    """Trigger a GitHub Actions workflow. workflow_filename e.g. 'restart-flask.yml'"""
    if not GH_PAT:
        return {"error": "GH_PAT not configured"}
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/actions/workflows/{workflow_filename}/dispatches"
    resp = requests.post(url, headers=_github_headers(), json={"ref": ref}, timeout=15)
    if resp.status_code == 204:
        return {"success": True, "message": f"Workflow '{workflow_filename}' triggered on {ref}."}
    return {"success": False, "status_code": resp.status_code, "error": resp.text[:500]}


@mcp.tool()
def workflow_status(workflow_filename: str) -> dict:
    """Get the last run status of a GitHub Actions workflow."""
    if not GH_PAT:
        return {"error": "GH_PAT not configured"}
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/actions/workflows/{workflow_filename}/runs"
    resp = requests.get(url, headers=_github_headers(), params={"per_page": 1}, timeout=15)
    if resp.status_code != 200:
        return {"error": f"GitHub API returned {resp.status_code}"}
    runs = resp.json().get("workflow_runs", [])
    if not runs:
        return {"message": f"No runs found for {workflow_filename}"}
    r = runs[0]
    duration_seconds = None
    try:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        s = datetime.strptime(r.get("run_started_at", ""), fmt).replace(tzinfo=timezone.utc)
        e = datetime.strptime(r.get("updated_at", ""), fmt).replace(tzinfo=timezone.utc)
        duration_seconds = int((e - s).total_seconds())
    except Exception:
        pass
    return {
        "workflow": workflow_filename,
        "run_id": r.get("id"),
        "status": r.get("status"),
        "conclusion": r.get("conclusion"),
        "started_at": r.get("run_started_at"),
        "updated_at": r.get("updated_at"),
        "duration_seconds": duration_seconds,
        "url": r.get("html_url"),
        "triggered_by": r.get("event"),
    }


@mcp.tool()
def shell_exec(command: str, token: str, timeout: int = 60) -> dict:
    """
    Run an arbitrary shell command on the VM as the schnapp-mcp service user.
    Requires the MCP_AUTH_TOKEN for authorization.
    command: shell command string to execute (runs via bash -c)
    token: must match MCP_AUTH_TOKEN environment variable
    timeout: max seconds to wait (default 60, max 300)
    """
    if not _check_token(token):
        return {"error": "Unauthorized: invalid token"}
    timeout = min(int(timeout), 300)
    try:
        result = subprocess.run(
            ["bash", "-c", command],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout.strip()[:10000],
            "stderr": result.stderr.strip()[:2000],
        }
    except subprocess.TimeoutExpired:
        return {"error": f"Command timed out after {timeout}s"}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def read_file(path: str, token: str, tail: int = 0) -> dict:
    """
    Read a file from the VM filesystem.
    Requires the MCP_AUTH_TOKEN for authorization.
    path: absolute path to the file
    token: must match MCP_AUTH_TOKEN environment variable
    tail: if > 0, return only the last N lines (like tail -n)
    """
    if not _check_token(token):
        return {"error": "Unauthorized: invalid token"}
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        if tail > 0:
            lines = lines[-tail:]
        content = "".join(lines)
        return {
            "path": path,
            "lines": len(lines),
            "content": content[:20000],
            "truncated": len(content) > 20000,
        }
    except FileNotFoundError:
        return {"error": f"File not found: {path}"}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
