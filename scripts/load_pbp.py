"""Load NBA play-by-play data from stats.nba.com (via nba_api) into DuckDB.

Resumable: games already present in the `pbp` table are skipped, so you can
stop the script at any time and re-run it to pick up where it left off.

Usage:
    python scripts/load_pbp.py                          # current season, all game types
    python scripts/load_pbp.py --season 2024-25
    python scripts/load_pbp.py --season-type Playoffs
    python scripts/load_pbp.py --limit 5                # quick test
"""

import argparse
import random
import re
import sys
import time
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder, playbyplayv3

DEFAULT_DB = Path(__file__).resolve().parent.parent / "db" / "nba_pbp.duckdb"
REQUEST_DELAY = 1.5  # seconds between API calls (plus jitter); stats.nba.com throttles aggressively
MAX_RETRIES = 3
MAX_CONSECUTIVE_FAILURES = 3  # stop early when the API is throttling us; re-run later to resume

#######################################################################
# Schema

SCHEMA = """
CREATE TABLE IF NOT EXISTS games (
    game_id         VARCHAR PRIMARY KEY,
    season          VARCHAR,
    season_type     VARCHAR,
    game_date       DATE,
    home_team_id    BIGINT,
    home_team       VARCHAR,
    away_team_id    BIGINT,
    away_team       VARCHAR,
    home_pts        INTEGER,
    away_pts        INTEGER
);

CREATE TABLE IF NOT EXISTS pbp (
    game_id         VARCHAR,
    action_number   INTEGER,
    action_id       INTEGER,
    period          INTEGER,
    clock           VARCHAR,     -- raw ISO duration, e.g. PT11M42.00S
    clock_seconds   DOUBLE,      -- seconds remaining in the period
    team_id         BIGINT,
    team_tricode    VARCHAR,
    person_id       BIGINT,
    player_name     VARCHAR,
    player_name_i   VARCHAR,
    action_type     VARCHAR,
    sub_type        VARCHAR,
    description     VARCHAR,
    location        VARCHAR,     -- 'h' / 'v'
    is_field_goal   BOOLEAN,
    shot_result     VARCHAR,
    shot_value      INTEGER,
    shot_distance   INTEGER,
    x_legacy        INTEGER,
    y_legacy        INTEGER,
    points_total    INTEGER,
    score_home      INTEGER,     -- forward-filled running score
    score_away      INTEGER,
    video_available BOOLEAN,
    PRIMARY KEY (game_id, action_id)   -- action_number is not unique (steals share it with the turnover)
);
"""

PBP_COLUMNS = {
    "gameId": "game_id",
    "actionNumber": "action_number",
    "actionId": "action_id",
    "period": "period",
    "clock": "clock",
    "teamId": "team_id",
    "teamTricode": "team_tricode",
    "personId": "person_id",
    "playerName": "player_name",
    "playerNameI": "player_name_i",
    "actionType": "action_type",
    "subType": "sub_type",
    "description": "description",
    "location": "location",
    "isFieldGoal": "is_field_goal",
    "shotResult": "shot_result",
    "shotValue": "shot_value",
    "shotDistance": "shot_distance",
    "xLegacy": "x_legacy",
    "yLegacy": "y_legacy",
    "pointsTotal": "points_total",
    "scoreHome": "score_home",
    "scoreAway": "score_away",
    "videoAvailable": "video_available",
}

# Column order must match the pbp table definition
PBP_ORDER = [
    "game_id", "action_number", "action_id", "period", "clock", "clock_seconds",
    "team_id", "team_tricode", "person_id", "player_name", "player_name_i",
    "action_type", "sub_type", "description", "location", "is_field_goal",
    "shot_result", "shot_value", "shot_distance", "x_legacy", "y_legacy",
    "points_total", "score_home", "score_away", "video_available",
]

#######################################################################
# Helpers

CLOCK_RE = re.compile(r"PT(\d+)M([\d.]+)S")


def clock_to_seconds(clock):
    m = CLOCK_RE.match(clock or "")
    return int(m.group(1)) * 60 + float(m.group(2)) if m else None


def with_retries(fn, what):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            wait = 5 * attempt
            print(f"  {what} failed ({e.__class__.__name__}: {e}); retry {attempt} in {wait}s")
            time.sleep(wait)


def fetch_games(season, season_type):
    """One row per game with home/away split out of the per-team rows."""
    df = with_retries(
        lambda: leaguegamefinder.LeagueGameFinder(
            season_nullable=season,
            league_id_nullable="00",
            season_type_nullable=season_type,
            timeout=60,
        ).get_data_frames()[0],
        "game list",
    )
    # Some games (e.g. neutral-site) list both teams as "X @ Y", so derive the
    # home team from the matchup text rather than relying on "vs." vs "@".
    home_abbr = df.MATCHUP.map(
        lambda m: m.split(" vs. ")[0] if " vs. " in m else m.split(" @ ")[1]
    )
    is_home = df.TEAM_ABBREVIATION == home_abbr
    home, away = df[is_home], df[~is_home]
    games = home.merge(away, on="GAME_ID", suffixes=("_h", "_a"))
    return pd.DataFrame({
        "game_id": games.GAME_ID,
        "season": season,
        "season_type": season_type,
        "game_date": pd.to_datetime(games.GAME_DATE_h).dt.date,
        "home_team_id": games.TEAM_ID_h,
        "home_team": games.TEAM_ABBREVIATION_h,
        "away_team_id": games.TEAM_ID_a,
        "away_team": games.TEAM_ABBREVIATION_a,
        "home_pts": games.PTS_h,
        "away_pts": games.PTS_a,
    }).sort_values("game_id").reset_index(drop=True)


def fetch_pbp(game_id):
    df = playbyplayv3.PlayByPlayV3(game_id=game_id, timeout=30).get_data_frames()[0]
    df = df.rename(columns=PBP_COLUMNS)
    df["clock_seconds"] = df.clock.map(clock_to_seconds)
    # Score columns are blank strings except on scoring plays; forward-fill a running score
    for col in ("score_home", "score_away"):
        df[col] = pd.to_numeric(df[col].replace("", None), errors="coerce").ffill().fillna(0).astype(int)
    df["is_field_goal"] = df.is_field_goal.astype(bool)
    df["video_available"] = df.video_available.astype(bool)
    return df[PBP_ORDER]

#######################################################################
# Main


SEASON_TYPES = ["Regular Season", "PlayIn", "Playoffs"]


def current_season(today=None):
    """NBA seasons start in October: Oct 2026 -> '2026-27', Mar 2026 -> '2025-26'."""
    today = today or date.today()
    start = today.year if today.month >= 10 else today.year - 1
    return f"{start}-{(start + 1) % 100:02d}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--season", default=current_season(), help="e.g. 2025-26 (default: current season)")
    ap.add_argument("--season-type", choices=SEASON_TYPES + ["Pre Season"],
                    help="default: Regular Season, PlayIn and Playoffs")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--limit", type=int, help="only load this many new games (for testing)")
    args = ap.parse_args()
    season_types = [args.season_type] if args.season_type else SEASON_TYPES

    print(f"=== {time.strftime('%Y-%m-%d %H:%M:%S')}  season {args.season} ===")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db))
    con.execute(SCHEMA)

    frames = []
    for season_type in season_types:
        df = fetch_games(args.season, season_type)
        print(f"  {season_type}: {len(df)} games")
        frames.append(df)
        time.sleep(REQUEST_DELAY)
    games_df = pd.concat(frames, ignore_index=True)
    # Games still in progress (today's) are left for the next run
    games_df = games_df[games_df.game_date < date.today()]
    if not games_df.empty:
        con.execute("INSERT OR REPLACE INTO games SELECT * FROM games_df")

    loaded = {r[0] for r in con.execute("SELECT DISTINCT game_id FROM pbp").fetchall()}
    todo = games_df[~games_df.game_id.isin(loaded)].itertuples(index=False)
    todo = list(todo)[:args.limit] if args.limit else list(todo)
    print(f"  {len(loaded & set(games_df.game_id))} already loaded, {len(todo)} to fetch")

    failed = []
    consecutive_failures = 0
    for i, game in enumerate(todo, 1):
        game_id = game.game_id
        try:
            df = with_retries(lambda: fetch_pbp(game_id), game_id)
        except Exception as e:
            print(f"[{i}/{len(todo)}] {game_id} FAILED: {e}")
            failed.append(game_id)
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n{consecutive_failures} failures in a row; the API is likely throttling. "
                      "Stopping early. Wait 15-30 minutes and re-run to resume.")
                break
            continue
        consecutive_failures = 0
        # Only save complete games: final PBP score must match the box score
        final = (df.score_home.max(), df.score_away.max())
        if final != (game.home_pts, game.away_pts):
            print(f"[{i}/{len(todo)}] {game_id} skipped: PBP final {final[1]}-{final[0]} "
                  f"!= box {game.away_pts}-{game.home_pts} (incomplete data; retry next run)")
            failed.append(game_id)
            time.sleep(REQUEST_DELAY)
            continue
        con.execute("BEGIN")
        con.execute("DELETE FROM pbp WHERE game_id = ?", [game_id])
        con.execute("INSERT INTO pbp SELECT * FROM df")
        con.execute("COMMIT")
        print(f"[{i}/{len(todo)}] {game_id}: {len(df)} actions")
        time.sleep(REQUEST_DELAY + random.uniform(0, 1))

    total = con.execute("SELECT count(*), count(DISTINCT game_id) FROM pbp").fetchone()
    print(f"\nDone. pbp has {total[0]:,} rows across {total[1]} games -> {args.db}")
    if failed:
        print(f"{len(failed)} games failed (re-run to retry): {', '.join(failed)}")
    con.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
