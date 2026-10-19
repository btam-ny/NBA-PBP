# NBA Play-by-Play (MotherDuck)

Loads NBA play-by-play, defensive matchups, and current rosters into a DuckDB database on [MotherDuck](https://motherduck.com) (`md:nba`), and keeps it up to date.

## Setup

```bash
pip install -r requirements.txt nba_api
```

Set `MOTHERDUCK_TOKEN` in your environment (MotherDuck → Settings → Access Tokens). Every command also takes `--db path/to/file.duckdb` to work against a local file instead.

## Commands

```bash
python scripts/nba_pbp.py update                     # newly finished games, current season (cdn.nba.com)
python scripts/nba_pbp.py backfill --season 2025-26  # a past season, 2019-20 onward
python scripts/nba_pbp.py matchups                   # defensive matchups for loaded games (stats.nba.com)
python scripts/nba_pbp.py rosters                    # refresh dim_teams and dim_players (stats.nba.com)
```

Every command skips what's already loaded, so you can stop it and re-run it at any time. If a source starts blocking requests (403s or timeouts), the script stops after a few failures in a row; wait 30–60 minutes and re-run.

Games are only saved once final, and only if the play-by-play's final score matches the box score. Games more than 3 days old are saved even with a small mismatch (a box-score correction the play-by-play never got).

## Keeping it up to date

| What | Where it runs | When |
|---|---|---|
| `update` (play-by-play) | GitHub Actions, [.github/workflows/update.yml](.github/workflows/update.yml) | Hourly from about 11 PM to 4 AM Eastern, plus about noon |
| `rosters` + `matchups` | Windows Task Scheduler on your PC ("NBA PBP Daily Update"), [scripts/daily_update.cmd](scripts/daily_update.cmd) | 6 AM and 12 PM |

stats.nba.com blocks most cloud servers, so the matchups and rosters jobs run locally. The GitHub Action needs a `MOTHERDUCK_TOKEN` repository secret.

## Tables

| Table | Grain | Notes |
|---|---|---|
| `games` | one row per game | season, type, date, teams, final score, arena |
| `pbp` | one row per event, keyed by `(game_id, action_number)` | Sort by `order_number`. Event types are lowercase (`2pt`, `3pt`, `freethrow`, `rebound`, `steal`, ...). Includes shot location (`x_legacy`/`y_legacy` in tenths of a foot from the basket), the running score, and assist, block, steal, and foul-drawn player IDs. |
| `matchups` | one row per offensive player × defender per game | Minutes and possessions guarded, plus the offensive player's points, FG, 3P, and FT against that defender. Some points (e.g. in transition) aren't credited to any defender. |
| `dim_players` | one row per player currently on a roster | `player_id` = `pbp.person_id`. `team` is the *current* team; use `pbp.team_tricode` for the team at the time of a game. |
| `dim_teams` | one row per team | Conference and division |

## Exploring

- **SQL in VS Code:** the DuckDB extension attaches `md:nba` as `nba`; see [queries/examples.sql](queries/examples.sql).
- **Python:** [notebooks/explore.ipynb](notebooks/explore.ipynb), using `duckdb.connect("md:nba")` and `.df()` for pandas.

## Legacy

[scripts/load_pbp.py](scripts/load_pbp.py) is the original loader: stats.nba.com `PlayByPlayV3` into a local `db/nba_pbp.duckdb`, in a different format. It's kept for reference only.
