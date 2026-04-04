"""
runner.py

Lightweight Flask proxy that runs as a persistent service on the schnapp-runner VM.
Fetches live NBA box scores from the public CDN endpoint (no proxy needed).

Endpoints:
  GET /ping                        -- health check, returns {"ok": true}
  GET /boxscore?gameId=<game_id>   -- returns live player stats for a game

CDN endpoint: https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json
Top-level key: "game" (not "boxScoreTraditional")
statistics: single dict per player (cumulative), not a list

Start manually:  source ~/venv/bin/activate && python etl/runner.py
Managed by:      systemd (schnapp-flask.service)
Port:            5000
Auth:            X-Runner-Key header must match RUNNER_API_KEY env var
"""

import os
import re
import logging

import requests
from flask import Flask, request, jsonify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

app = Flask(__name__)

RUNNER_KEY = os.environ.get("RUNNER_API_KEY", "runner-Lake4971")


def check_auth():
    key = request.headers.get("X-Runner-Key", "")
    if key != RUNNER_KEY:
        return jsonify({"error": "unauthorized"}), 401
    return None


def parse_minutes(clock: str) -> float:
    if not clock:
        return 0.0
    m = re.match(r"PT(\d+)M([\d.]+)S", clock)
    if m:
        return round(int(m.group(1)) + float(m.group(2)) / 60, 4)
    try:
        return float(clock)
    except (ValueError, TypeError):
        return 0.0


@app.route("/ping")
def ping():
    return jsonify({"ok": True})


@app.route("/boxscore")
def boxscore():
    auth_error = check_auth()
    if auth_error:
        return auth_error

    game_id = request.args.get("gameId", "").strip()
    if not game_id:
        return jsonify({"error": "gameId required"}), 400

    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            log.warning(f"CDN returned {resp.status_code} for game {game_id}")
            return jsonify({"error": f"CDN returned {resp.status_code}"}), 502
        data = resp.json()
    except requests.Timeout:
        return jsonify({"error": "CDN timed out"}), 504
    except Exception as exc:
        log.error(f"boxscore fetch failed: {exc}")
        return jsonify({"error": str(exc)}), 500

    game = data.get("game")
    if not game:
        return jsonify({"error": "Unexpected CDN response shape"}), 502

    players = []
    for team in [game.get("homeTeam"), game.get("awayTeam")]:
        if not team:
            continue
        team_id   = int(team.get("teamId", 0))
        team_abbr = str(team.get("teamTricode", ""))
        for player in team.get("players", []):
            s = player.get("statistics", {})
            if not isinstance(s, dict):
                s = {}
            players.append({
                "playerId":   int(player.get("personId", 0)),
                "playerName": str(player.get("name", "")),
                "teamId":     team_id,
                "teamAbbr":   team_abbr,
                "starter":    player.get("starter") == "1",
                "oncourt":    player.get("oncourt") == "1",
                "pts":        int(s.get("points", 0) or 0),
                "reb":        int(s.get("reboundsTotal", 0) or 0),
                "ast":        int(s.get("assists", 0) or 0),
                "stl":        int(s.get("steals", 0) or 0),
                "blk":        int(s.get("blocks", 0) or 0),
                "tov":        int(s.get("turnovers", 0) or 0),
                "fg3m":       int(s.get("threePointersMade", 0) or 0),
                "fg3a":       int(s.get("threePointersAttempted", 0) or 0),
                "fgm":        int(s.get("fieldGoalsMade", 0) or 0),
                "fga":        int(s.get("fieldGoalsAttempted", 0) or 0),
                "ftm":        int(s.get("freeThrowsMade", 0) or 0),
                "fta":        int(s.get("freeThrowsAttempted", 0) or 0),
                "min":        round(parse_minutes(s.get("minutes", "")), 1),
            })

    return jsonify({
        "gameId":         game_id,
        "gameStatusText": str(game.get("gameStatusText", "")),
        "players":        players,
    })


if __name__ == "__main__":
    log.info("Starting runner on port 5000 (CDN endpoint, no proxy needed)")
    app.run(host="0.0.0.0", port=5000, debug=False)
