"""
runner.py

Lightweight Flask proxy that runs as a persistent service on the schnapp-runner VM.
Proxies BoxScoreTraditionalV3 calls from Azure SWA through the Webshare residential
proxy, bypassing the stats.nba.com IP block on Azure datacenter IPs.

Endpoints:
  GET /ping                        -- health check, returns {"ok": true}
  GET /boxscore?gameId=<game_id>   -- returns live player stats for a game

Start manually:  source ~/venv/bin/activate && python etl/runner.py
Managed by:      systemd (schnapp-runner-flask.service)
Port:            5000
Auth:            X-Runner-Key header must match RUNNER_API_KEY env var
"""

import os
import re
import time
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

PROXY_URL   = os.environ.get("NBA_PROXY_URL")
RUNNER_KEY  = os.environ.get("RUNNER_API_KEY", "runner-Lake4971")
NBA_HEADERS = {
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
    "Origin":             "https://www.nba.com",
    "Referer":            "https://www.nba.com/",
}


def get_proxies():
    if not PROXY_URL:
        return None
    return {"http": PROXY_URL, "https": PROXY_URL}


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

    url = "https://stats.nba.com/stats/boxscoretraditionalv3"
    params = {
        "GameID":      game_id,
        "StartPeriod": 0,
        "EndPeriod":   0,
        "StartRange":  0,
        "EndRange":    0,
        "RangeType":   0,
    }

    try:
        resp = requests.get(
            url,
            headers=NBA_HEADERS,
            params=params,
            proxies=get_proxies(),
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning(f"NBA API returned {resp.status_code} for game {game_id}")
            return jsonify({"error": f"NBA API returned {resp.status_code}"}), 502
        data = resp.json()
    except requests.Timeout:
        return jsonify({"error": "NBA API timed out"}), 504
    except Exception as exc:
        log.error(f"boxscore fetch failed: {exc}")
        return jsonify({"error": str(exc)}), 500

    game = data.get("boxScoreTraditional")
    if not game:
        return jsonify({"error": "Unexpected NBA API response shape"}), 502

    players = []
    for team in [game.get("homeTeam"), game.get("awayTeam")]:
        if not team:
            continue
        team_id   = int(team.get("teamId", 0))
        team_abbr = str(team.get("teamTricode", ""))
        for player in team.get("players", []):
            stats_arr = player.get("statistics", [])
            if not isinstance(stats_arr, list):
                stats_arr = [stats_arr] if stats_arr else []

            pts = reb = ast = stl = blk = tov = 0
            fg3m = fg3a = fgm = fga = ftm = fta = 0
            minutes = 0.0

            for s in stats_arr:
                pts  += int(s.get("points",              s.get("pts",  0)) or 0)
                reb  += int(s.get("reboundsTotal",        s.get("reb",  0)) or 0)
                ast  += int(s.get("assists",              s.get("ast",  0)) or 0)
                stl  += int(s.get("steals",               s.get("stl",  0)) or 0)
                blk  += int(s.get("blocks",               s.get("blk",  0)) or 0)
                tov  += int(s.get("turnovers",            s.get("tov",  0)) or 0)
                fg3m += int(s.get("threePointersMade",    s.get("fg3m", 0)) or 0)
                fg3a += int(s.get("threePointersAttempted", s.get("fg3a", 0)) or 0)
                fgm  += int(s.get("fieldGoalsMade",       s.get("fgm",  0)) or 0)
                fga  += int(s.get("fieldGoalsAttempted",  s.get("fga",  0)) or 0)
                ftm  += int(s.get("freeThrowsMade",       s.get("ftm",  0)) or 0)
                fta  += int(s.get("freeThrowsAttempted",  s.get("fta",  0)) or 0)
                minutes += parse_minutes(s.get("clock") or s.get("minutesCalculated", ""))

            players.append({
                "playerId":   int(player.get("personId", 0)),
                "playerName": str(player.get("name", "")),
                "teamId":     team_id,
                "teamAbbr":   team_abbr,
                "pts":        pts,
                "reb":        reb,
                "ast":        ast,
                "stl":        stl,
                "blk":        blk,
                "tov":        tov,
                "fg3m":       fg3m,
                "fg3a":       fg3a,
                "fgm":        fgm,
                "fga":        fga,
                "ftm":        ftm,
                "fta":        fta,
                "min":        round(minutes, 1),
            })

    return jsonify({
        "gameId":         game_id,
        "gameStatusText": str(game.get("gameStatusText", "")),
        "players":        players,
    })


if __name__ == "__main__":
    log.info(f"Starting runner on port 5000 (proxy: {'active' if PROXY_URL else 'NONE'})")
    app.run(host="0.0.0.0", port=5000, debug=False)
