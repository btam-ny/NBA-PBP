-- "nba" is your MotherDuck database and attaches automatically. Run any statement with Ctrl+Enter.
USE nba;

-- How much is loaded
SELECT season, season_type, count(*) AS games
FROM games g
WHERE game_id IN (SELECT DISTINCT game_id FROM pbp)
GROUP BY ALL ORDER BY ALL;

-- One game's play-by-play in order
SELECT period, clock_seconds, team_tricode, player_name, action_type, description, score_away, score_home
FROM pbp
WHERE game_id = '0022500001'
ORDER BY order_number;

-- Who guarded SGA the most this game, and how he shot against each defender
SELECT def_player, round(matchup_seconds / 60, 1) AS minutes, player_points, fgm, fga, fg3m, fg3a
FROM matchups
WHERE game_id = '0022500001' AND off_person_id = 1628983
ORDER BY matchup_seconds DESC;

-- Current rosters with conference
SELECT t.conference, t.team, p.full_name, p.position, p.height, p.jersey
FROM dim_players p JOIN dim_teams t USING (team_id)
ORDER BY t.conference, t.team, p.last_name;

-- Top 3PT shooters
SELECT player_name,
       count(*) FILTER (shot_result = 'Made') AS made,
       count(*) AS att,
       round(made / att, 3) AS pct
FROM pbp
WHERE shot_value = 3 AND is_field_goal
GROUP BY 1
ORDER BY made DESC
LIMIT 15;

-- Clutch field-goal points: last 5 min of 4th/OT, margin <= 5
SELECT player_name, sum(shot_value) FILTER (shot_result = 'Made') AS clutch_fg_pts
FROM pbp
WHERE period >= 4 AND clock_seconds <= 300
  AND abs(score_home - score_away) <= 5
  AND is_field_goal
GROUP BY 1
ORDER BY clutch_fg_pts DESC NULLS LAST
LIMIT 15;
