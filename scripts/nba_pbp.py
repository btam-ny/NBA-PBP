"""NBA play-by-play -> DuckDB / MotherDuck, from the NBA's public live-data CDN (cdn.nba.com).

Commands:
    python scripts/nba_pbp.py update                    # load newly finished games (current season)
    python scripts/nba_pbp.py backfill --season 2025-26 # load a past season (2019-20 onward)
    python scripts/nba_pbp.py matchups                  # defensive matchups for loaded games
    python scripts/nba_pbp.py rosters                   # refresh dim_teams and dim_players

Database: --db, else $NBA_DB, else "md:nba" (MotherDuck; needs $MOTHERDUCK_TOKEN).
Pass a local path such as db/test.duckdb to test without MotherDuck.

All commands skip games already loaded, so they are safe to re-run at any time.
`backfill` and `matchups` use nba_api (stats.nba.com), which is often blocked
from cloud servers, so run them from your own machine.
"""

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import date, datetime, timezone

import duckdb
import pandas as pd
import requests

SCHEDULE_URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
PBP_URL = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{}.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Accept": "application/json",
}
REQUEST_DELAY = 1.0  # plus jitter; the CDN starts returning 403s after ~600 rapid requests
MAX_RETRIES = 3
MAX_CONSECUTIVE_FAILURES = 3
STALE_DAYS = 3  # after this, save games whose PBP total is off (a box-score correction never applied to the PBP)
STATS_DELAY = 1.5  # stats.nba.com throttles much harder than the CDN

# Game IDs look like 00 2 25 00001: league, game type, season start year, game number
GAME_TYPES = {
    "2": "Regular Season",
    "4": "Playoffs",
    "5": "PlayIn",
    "6": "NBA Cup Final",  # championship game; not part of the regular-season record
}

#######################################################################
# Schema

SCHEMA = """
CREATE TABLE IF NOT EXISTS games (
    game_id       VARCHAR PRIMARY KEY,
    season        VARCHAR,
    season_type   VARCHAR,
    game_date     DATE,       -- US Eastern date
    game_label    VARCHAR,    -- e.g. 'Emirates NBA Cup', 'NBA Paris Game'
    arena         VARCHAR,
    is_neutral    BOOLEAN,
    home_team_id  BIGINT,
    home_team     VARCHAR,
    away_team_id  BIGINT,
    away_team     VARCHAR,
    home_pts      INTEGER,
    away_pts      INTEGER,
    loaded_at     TIMESTAMP   -- when play-by-play was loaded (NULL = not yet)
);

CREATE TABLE IF NOT EXISTS pbp (
    game_id              VARCHAR,
    action_number        INTEGER,
    order_number         BIGINT,     -- true event order; sort by this
    period               INTEGER,
    period_type          VARCHAR,    -- REGULAR / OVERTIME
    clock                VARCHAR,    -- raw ISO duration, e.g. PT11M42.00S
    clock_seconds        DOUBLE,     -- seconds remaining in the period
    time_actual          TIMESTAMP,  -- wall-clock time, UTC
    team_id              BIGINT,
    team_tricode         VARCHAR,
    possession_team_id   BIGINT,
    person_id            BIGINT,
    player_name          VARCHAR,
    player_name_i        VARCHAR,
    action_type          VARCHAR,    -- 2pt, 3pt, freethrow, rebound, foul, turnover, steal, ...
    sub_type             VARCHAR,
    descriptor           VARCHAR,
    qualifiers           VARCHAR[],
    description          VARCHAR,
    is_field_goal        BOOLEAN,
    shot_result          VARCHAR,    -- Made / Missed
    shot_value           INTEGER,    -- 3, 2, or 1 (free throw)
    shot_distance        DOUBLE,
    area                 VARCHAR,
    area_detail          VARCHAR,
    side                 VARCHAR,
    x                    DOUBLE,
    y                    DOUBLE,
    x_legacy             INTEGER,
    y_legacy             INTEGER,
    score_home           INTEGER,    -- running score, filled on every row
    score_away           INTEGER,
    points_total         INTEGER,
    assist_person_id     BIGINT,
    block_person_id      BIGINT,
    steal_person_id      BIGINT,
    foul_drawn_person_id BIGINT,
    shot_action_number   INTEGER,    -- for rebounds/blocks: the shot it relates to
    person_ids           BIGINT[],   -- every player involved in the event
    PRIMARY KEY (game_id, action_number)
);

-- Player-tracking defensive matchups: one row per offensive player x defender per game.
-- "matchup_*" stats are the offensive player's while guarded by that defender.
CREATE TABLE IF NOT EXISTS matchups (
    game_id                   VARCHAR,
    off_team_id               BIGINT,
    off_team                  VARCHAR,
    off_person_id             BIGINT,
    off_player                VARCHAR,
    off_position              VARCHAR,
    def_person_id             BIGINT,
    def_player                VARCHAR,
    matchup_seconds           DOUBLE,
    partial_possessions       DOUBLE,
    pct_defender_time         DOUBLE,  -- share of the defender's time spent on this player
    pct_offense_time          DOUBLE,  -- share of the offensive player's time guarded by this defender
    pct_time_both_on          DOUBLE,
    switches_on               INTEGER,
    player_points             INTEGER,
    team_points               INTEGER,
    assists                   INTEGER,
    potential_assists         INTEGER,
    turnovers                 INTEGER,
    blocks                    INTEGER,
    fgm                       INTEGER,
    fga                       INTEGER,
    fg3m                      INTEGER,
    fg3a                      INTEGER,
    ftm                       INTEGER,
    fta                       INTEGER,
    shooting_fouls            INTEGER,
    help_blocks               INTEGER,
    help_fgm                  INTEGER,
    help_fga                  INTEGER,
    PRIMARY KEY (game_id, off_person_id, def_person_id)
);

-- Which games' matchups have been fetched (some games return no tracking data)
CREATE TABLE IF NOT EXISTS matchups_loaded (
    game_id    VARCHAR PRIMARY KEY,
    n_rows     INTEGER,
    loaded_at  TIMESTAMP
);

-- Current teams (refreshed by `rosters`)
CREATE TABLE IF NOT EXISTS dim_teams (
    team_id       BIGINT PRIMARY KEY,
    team          VARCHAR,   -- abbreviation, e.g. OKC (matches team_tricode in pbp)
    city          VARCHAR,
    name          VARCHAR,
    full_name     VARCHAR,
    conference    VARCHAR,
    division      VARCHAR,
    state         VARCHAR,
    year_founded  INTEGER,
    updated_at    TIMESTAMP
);

-- Players currently on an NBA roster, two-way contracts included (refreshed by `rosters`)
CREATE TABLE IF NOT EXISTS dim_players (
    player_id     BIGINT PRIMARY KEY,  -- matches person_id in pbp and matchups
    first_name    VARCHAR,
    last_name     VARCHAR,
    full_name     VARCHAR,
    team_id       BIGINT,
    team          VARCHAR,
    jersey        VARCHAR,
    position      VARCHAR,
    height        VARCHAR,   -- e.g. 6-8
    height_inches INTEGER,
    weight_lbs    INTEGER,
    college       VARCHAR,
    country       VARCHAR,
    draft_year    INTEGER,
    draft_round   INTEGER,
    draft_number  INTEGER,
    from_year     INTEGER,   -- first NBA season
    to_year       INTEGER,
    updated_at    TIMESTAMP
);
"""

MATCHUP_COLUMNS = {
    "gameId": "game_id",
    "teamId": "off_team_id",
    "teamTricode": "off_team",
    "personIdOff": "off_person_id",
    "nameIOff": "off_player",
    "positionOff": "off_position",
    "personIdDef": "def_person_id",
    "nameIDef": "def_player",
    "matchupMinutesSort": "matchup_seconds",
    "partialPossessions": "partial_possessions",
    "percentageDefenderTotalTime": "pct_defender_time",
    "percentageOffensiveTotalTime": "pct_offense_time",
    "percentageTotalTimeBothOn": "pct_time_both_on",
    "switchesOn": "switches_on",
    "playerPoints": "player_points",
    "teamPoints": "team_points",
    "matchupAssists": "assists",
    "matchupPotentialAssists": "potential_assists",
    "matchupTurnovers": "turnovers",
    "matchupBlocks": "blocks",
    "matchupFieldGoalsMade": "fgm",
    "matchupFieldGoalsAttempted": "fga",
    "matchupThreePointersMade": "fg3m",
    "matchupThreePointersAttempted": "fg3a",
    "matchupFreeThrowsMade": "ftm",
    "matchupFreeThrowsAttempted": "fta",
    "shootingFouls": "shooting_fouls",
    "helpBlocks": "help_blocks",
    "helpFieldGoalsMade": "help_fgm",
    "helpFieldGoalsAttempted": "help_fga",
}

PBP_COLUMNS = {
    "actionNumber": "action_number",
    "orderNumber": "order_number",
    "period": "period",
    "periodType": "period_type",
    "clock": "clock",
    "timeActual": "time_actual",
    "teamId": "team_id",
    "teamTricode": "team_tricode",
    "possession": "possession_team_id",
    "personId": "person_id",
    "playerName": "player_name",
    "playerNameI": "player_name_i",
    "actionType": "action_type",
    "subType": "sub_type",
    "descriptor": "descriptor",
    "qualifiers": "qualifiers",
    "description": "description",
    "isFieldGoal": "is_field_goal",
    "shotResult": "shot_result",
    "shotDistance": "shot_distance",
    "area": "area",
    "areaDetail": "area_detail",
    "side": "side",
    "x": "x",
    "y": "y",
    "xLegacy": "x_legacy",
    "yLegacy": "y_legacy",
    "scoreHome": "score_home",
    "scoreAway": "score_away",
    "pointsTotal": "points_total",
    "assistPersonId": "assist_person_id",
    "blockPersonId": "block_person_id",
    "stealPersonId": "steal_person_id",
    "foulDrawnPersonId": "foul_drawn_person_id",
    "shotActionNumber": "shot_action_number",
    "personIdsFilter": "person_ids",
}

# Insert column-by-column with explicit casts so pandas' inferred types never matter
PBP_SELECT = """
SELECT ?::VARCHAR, action_number, order_number, period, period_type, clock, clock_seconds,
       time_actual::TIMESTAMP, team_id, team_tricode, possession_team_id, person_id,
       player_name, player_name_i, action_type, sub_type, descriptor,
       qualifiers::VARCHAR[], description, is_field_goal, shot_result, shot_value,
       shot_distance, area, area_detail, side, x, y, x_legacy, y_legacy,
       score_home, score_away, points_total, assist_person_id, block_person_id,
       steal_person_id, foul_drawn_person_id, shot_action_number, person_ids::BIGINT[]
FROM df
"""

#######################################################################
# Fetching

session = requests.Session()
session.headers.update(HEADERS)


class Blocked(Exception):
    pass


def get_json(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 404:
                return None  # not published (yet)
            if r.status_code == 403:
                # The CDN answers 403 both for seasons it doesn't carry and when it is
                # rate-limiting us; either way, retrying immediately won't help.
                raise Blocked(f"403 from {url.rsplit('/', 1)[-1]}")
            r.raise_for_status()
            return json.loads(r.content.decode("utf-8-sig"))  # schedule file starts with a BOM
        except (requests.RequestException, ValueError) as e:  # Blocked is not retried
            if attempt == MAX_RETRIES:
                raise
            print(f"  {url.rsplit('/', 1)[-1]} failed ({e.__class__.__name__}); retry {attempt} in {3 * attempt}s")
            time.sleep(3 * attempt)


def season_of(game_id):
    start = 2000 + int(game_id[3:5])
    return f"{start}-{(start + 1) % 100:02d}"


def schedule_games():
    """Every game on the current season's schedule (games with gameStatus 3 are final)."""
    sched = get_json(SCHEDULE_URL)["leagueSchedule"]
    rows = []
    for day in sched["gameDates"]:
        for g in day["games"]:
            if g["gameId"][2] not in GAME_TYPES:
                continue
            rows.append({
                "game_id": g["gameId"],
                "final": g["gameStatus"] == 3,
                "game_date": date.fromisoformat(g["gameDateEst"][:10]),
                "game_label": g.get("gameLabel") or None,
                "arena": g.get("arenaName") or None,
                "is_neutral": bool(g.get("isNeutral")),
                "home_team_id": g["homeTeam"]["teamId"],
                "home_team": g["homeTeam"]["teamTricode"],
                "away_team_id": g["awayTeam"]["teamId"],
                "away_team": g["awayTeam"]["teamTricode"],
                "home_pts": g["homeTeam"]["score"],
                "away_pts": g["awayTeam"]["score"],
            })
    return pd.DataFrame(rows)


def season_games(season):
    """Game list for a past season, via stats.nba.com (nba_api)."""
    from nba_api.stats.endpoints import leaguegamefinder

    frames = []
    for season_type in ("Regular Season", "PlayIn", "Playoffs"):
        df = leaguegamefinder.LeagueGameFinder(
            season_nullable=season, league_id_nullable="00",
            season_type_nullable=season_type, timeout=60,
        ).get_data_frames()[0]
        frames.append(df)
        time.sleep(1)
    df = pd.concat(frames, ignore_index=True)
    # Neutral-site games can list both teams as "X @ Y", so take the home team from the text
    home_abbr = df.MATCHUP.map(lambda m: m.split(" vs. ")[0] if " vs. " in m else m.split(" @ ")[1])
    is_home = df.TEAM_ABBREVIATION == home_abbr
    g = df[is_home].merge(df[~is_home], on="GAME_ID", suffixes=("_h", "_a"))
    return pd.DataFrame({
        "game_id": g.GAME_ID,
        "final": True,
        "game_date": pd.to_datetime(g.GAME_DATE_h).dt.date,
        "game_label": None,
        "arena": None,
        "is_neutral": None,
        "home_team_id": g.TEAM_ID_h,
        "home_team": g.TEAM_ABBREVIATION_h,
        "away_team_id": g.TEAM_ID_a,
        "away_team": g.TEAM_ABBREVIATION_a,
        "home_pts": g.PTS_h,
        "away_pts": g.PTS_a,
    })


CLOCK_RE = re.compile(r"PT(\d+)M([\d.]+)S")


def clock_to_seconds(clock):
    m = CLOCK_RE.match(clock or "")
    return int(m.group(1)) * 60 + float(m.group(2)) if m else None


def fetch_pbp(game_id):
    data = get_json(PBP_URL.format(game_id))
    if not data:
        return None
    df = pd.DataFrame(data["game"]["actions"]).rename(columns=PBP_COLUMNS)
    df = df.reindex(columns=list(PBP_COLUMNS.values()))  # add any columns this game lacks
    df["clock_seconds"] = df.clock.map(clock_to_seconds)
    df["time_actual"] = pd.to_datetime(df.time_actual, utc=True, errors="coerce").dt.tz_localize(None)
    for col in ("score_home", "score_away"):
        df[col] = pd.to_numeric(df[col], errors="coerce").ffill().fillna(0).astype(int)
    df["is_field_goal"] = df.is_field_goal.fillna(0).astype(bool)
    df["shot_value"] = df.action_type.map({"3pt": 3, "2pt": 2, "freethrow": 1})
    for col in ("qualifiers", "person_ids"):
        df[col] = df[col].map(lambda v: v if isinstance(v, list) else [])
    for col in ("sub_type", "descriptor", "shot_result", "area", "area_detail", "side", "team_tricode"):
        df[col] = df[col].replace("", None)
    return df.sort_values("order_number").reset_index(drop=True)

#######################################################################
# Loading


def connect(db):
    if db.startswith("md:"):
        if not os.getenv("MOTHERDUCK_TOKEN") and not os.getenv("motherduck_token"):
            sys.exit("MOTHERDUCK_TOKEN is not set")
        con = duckdb.connect("md:")
        name = db[3:] or "nba"
        con.execute(f"CREATE DATABASE IF NOT EXISTS {name}")
        con.execute(f"USE {name}")
    else:
        os.makedirs(os.path.dirname(os.path.abspath(db)), exist_ok=True)
        con = duckdb.connect(db)
    con.execute(SCHEMA)
    return con


def load_games(con, games, limit=None):
    """Load play-by-play for final games that aren't in the database yet."""
    games = games[games.final].copy()
    games["season"] = games.game_id.map(season_of)
    games["season_type"] = games.game_id.str[2].map(GAME_TYPES)

    loaded = {r[0] for r in con.execute("SELECT DISTINCT game_id FROM pbp").fetchall()}
    todo = games[~games.game_id.isin(loaded)].sort_values("game_id")
    if limit:
        todo = todo.head(limit)
    print(f"  {len(games)} final games, {len(loaded & set(games.game_id))} already loaded, {len(todo)} to fetch")

    loaded_now, failed, pending = 0, [], []
    consecutive_failures = 0
    for i, game in enumerate(todo.itertuples(index=False), 1):
        tag = f"[{i}/{len(todo)}] {game.game_id} {game.away_team}@{game.home_team}"
        try:
            df = fetch_pbp(game.game_id)
        except Exception as e:
            print(f"{tag} FAILED: {e}")
            failed.append(game.game_id)
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"\n{consecutive_failures} failures in a row; stopping early. Re-run later to resume.")
                break
            continue
        consecutive_failures = 0
        time.sleep(REQUEST_DELAY + random.uniform(0, 0.5))

        if df is None:
            print(f"{tag}: play-by-play not published yet")
            pending.append(game.game_id)
            continue
        # Only save complete games: the final PBP score must match the final box score.
        # Older games are saved anyway, since a mismatch then is a stat correction, not missing plays.
        final = (int(df.score_home.iloc[-1]), int(df.score_away.iloc[-1]))
        if final != (game.home_pts, game.away_pts):
            msg = f"{tag}: PBP ends {final[1]}-{final[0]}, box score {game.away_pts}-{game.home_pts}"
            if (date.today() - game.game_date).days <= STALE_DAYS:
                print(f"{msg}; not saved (data still updating, retry next run)")
                pending.append(game.game_id)
                continue
            print(f"{msg}; saving anyway (game is more than {STALE_DAYS} days old)")

        game_row = pd.DataFrame([game._asdict()]).drop(columns="final")
        game_row["loaded_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
        con.execute("BEGIN")
        con.execute("DELETE FROM pbp WHERE game_id = ?", [game.game_id])
        con.execute("INSERT INTO pbp " + PBP_SELECT, [game.game_id])
        con.execute("INSERT OR REPLACE INTO games BY NAME SELECT * FROM game_row")
        con.execute("COMMIT")
        loaded_now += 1
        print(f"{tag}: {len(df)} actions, final {game.away_pts}-{game.home_pts}")

    total = con.execute("SELECT count(*), count(DISTINCT game_id) FROM pbp").fetchone()
    print(f"\nLoaded {loaded_now} games. pbp now has {total[0]:,} rows across {total[1]:,} games.")
    if pending:
        print(f"{len(pending)} games not ready yet (will retry next run): {', '.join(pending)}")
    if failed:
        print(f"{len(failed)} games failed: {', '.join(failed)}")
    return 1 if failed else 0


def fetch_matchups(game_id):
    from nba_api.stats.endpoints import boxscorematchupsv3

    df = boxscorematchupsv3.BoxScoreMatchupsV3(game_id=game_id, timeout=30).get_data_frames()[0]
    return df.reindex(columns=list(MATCHUP_COLUMNS)).rename(columns=MATCHUP_COLUMNS)


def load_matchups(con, season=None, limit=None):
    """Load defensive matchups for every game in `games` that doesn't have them yet."""
    todo = con.execute("""
        SELECT g.game_id, g.away_team, g.home_team FROM games g
        WHERE g.game_id NOT IN (SELECT game_id FROM matchups_loaded)
          AND (? IS NULL OR g.season = ?)
        ORDER BY g.game_id
    """, [season, season]).fetchall()
    if limit:
        todo = todo[:limit]
    print(f"  {len(todo)} games need matchups")

    loaded_now, failed = 0, []
    consecutive_failures = 0
    for i, (game_id, away, home) in enumerate(todo, 1):
        tag = f"[{i}/{len(todo)}] {game_id} {away}@{home}"
        try:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    df = fetch_matchups(game_id)
                    break
                except Exception as e:
                    if attempt == MAX_RETRIES:
                        raise
                    print(f"  {game_id} failed ({e.__class__.__name__}); retry {attempt} in {5 * attempt}s")
                    time.sleep(5 * attempt)
        except Exception as e:
            print(f"{tag} FAILED: {e}")
            failed.append(game_id)
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print("\n3 failures in a row; stats.nba.com is likely throttling. "
                      "Stopping early. Wait 15-30 minutes and re-run to resume.")
                break
            continue
        consecutive_failures = 0

        con.execute("BEGIN")
        con.execute("DELETE FROM matchups WHERE game_id = ?", [game_id])
        if len(df):
            con.execute("INSERT INTO matchups BY NAME SELECT * FROM df")
        con.execute("INSERT OR REPLACE INTO matchups_loaded VALUES (?, ?, ?)",
                    [game_id, len(df), datetime.now(timezone.utc).replace(tzinfo=None)])
        con.execute("COMMIT")
        loaded_now += 1
        print(f"{tag}: {len(df)} matchups")
        time.sleep(STATS_DELAY + random.uniform(0, 1))

    total = con.execute("SELECT count(*), count(DISTINCT game_id) FROM matchups").fetchone()
    print(f"\nLoaded {loaded_now} games. matchups now has {total[0]:,} rows across {total[1]:,} games.")
    if failed:
        print(f"{len(failed)} games failed (re-run to retry): {', '.join(failed)}")
    return 1 if failed else 0


def current_season(today=None):
    """NBA seasons start in October: Oct 2026 -> '2026-27', Mar 2026 -> '2025-26'."""
    today = today or date.today()
    start = today.year if today.month >= 10 else today.year - 1
    return f"{start}-{(start + 1) % 100:02d}"


def load_rosters(con):
    """Replace dim_teams and dim_players with today's teams and rosters."""
    from nba_api.stats.endpoints import leaguestandingsv3, playerindex
    from nba_api.stats.static import teams as static_teams

    season = current_season()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    standings = leaguestandingsv3.LeagueStandingsV3(season=season, timeout=60).get_data_frames()[0]
    conf = standings.set_index("TeamID")[["Conference", "Division"]]
    teams = pd.DataFrame(static_teams.get_teams()).rename(columns={
        "id": "team_id", "abbreviation": "team", "nickname": "name", "year_founded": "year_founded",
    })
    teams["conference"] = teams.team_id.map(conf.Conference)
    teams["division"] = teams.team_id.map(conf.Division)
    teams["updated_at"] = now
    time.sleep(STATS_DELAY)

    p = playerindex.PlayerIndex(season=season, league_id="00", historical_nullable=0, timeout=60).get_data_frames()[0]
    p = p[(p.ROSTER_STATUS == 1) & (p.TEAM_ID > 0)]
    height = p.HEIGHT.str.extract(r"(\d+)-(\d+)").astype(float)
    players = pd.DataFrame({
        "player_id": p.PERSON_ID,
        "first_name": p.PLAYER_FIRST_NAME,
        "last_name": p.PLAYER_LAST_NAME,
        "full_name": p.PLAYER_FIRST_NAME + " " + p.PLAYER_LAST_NAME,
        "team_id": p.TEAM_ID,
        "team": p.TEAM_ABBREVIATION,
        "jersey": p.JERSEY_NUMBER.replace("", None),
        "position": p.POSITION.replace("", None),
        "height": p.HEIGHT.replace("", None),
        "height_inches": height[0] * 12 + height[1],
        "weight_lbs": pd.to_numeric(p.WEIGHT, errors="coerce"),
        "college": p.COLLEGE.replace("", None),
        "country": p.COUNTRY.replace("", None),
        "draft_year": p.DRAFT_YEAR,
        "draft_round": p.DRAFT_ROUND,
        "draft_number": p.DRAFT_NUMBER,
        "from_year": pd.to_numeric(p.FROM_YEAR, errors="coerce"),
        "to_year": pd.to_numeric(p.TO_YEAR, errors="coerce"),
        "updated_at": now,
    })

    con.execute("BEGIN")
    con.execute("DELETE FROM dim_teams")
    con.execute("INSERT INTO dim_teams BY NAME SELECT team_id, team, city, name, full_name, conference, "
                "division, state, year_founded, updated_at FROM teams")
    con.execute("DELETE FROM dim_players")
    con.execute("INSERT INTO dim_players BY NAME SELECT * FROM players")
    con.execute("COMMIT")
    print(f"  {season}: {len(teams)} teams, {len(players)} rostered players")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["update", "backfill", "matchups", "rosters"])
    ap.add_argument("--season", help="e.g. 2025-26 (required for backfill; optional filter for matchups)")
    ap.add_argument("--db", default=os.getenv("NBA_DB", "md:nba"))
    ap.add_argument("--limit", type=int, help="only load this many games (for testing)")
    args = ap.parse_args()

    print(f"=== {datetime.now():%Y-%m-%d %H:%M:%S}  {args.command} {args.season or ''} -> {args.db} ===")
    con = connect(args.db)
    if args.command in ("matchups", "rosters"):
        status = load_matchups(con, args.season, args.limit) if args.command == "matchups" else load_rosters(con)
        con.close()
        return status
    if args.command == "update":
        games = schedule_games()
    else:
        if not args.season:
            sys.exit("backfill needs --season, e.g. --season 2025-26")
        games = season_games(args.season)
        games = games[games.game_date < date.today()]
    status = load_games(con, games, args.limit)
    con.close()
    return status


if __name__ == "__main__":
    sys.exit(main())
