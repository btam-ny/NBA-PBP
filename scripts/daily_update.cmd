@echo off
rem Daily local job for the stats.nba.com data (which blocks cloud servers):
rem refreshes current rosters, then loads defensive matchups for games the
rem GitHub Action has already loaded into MotherDuck.
rem Run by the "NBA PBP Daily Update" scheduled task; safe to run by hand too.
set PYTHONIOENCODING=utf-8
set PY="C:\Users\Brian\AppData\Local\Programs\Python\Python313\python.exe"
cd /d "%~dp0.."
if not exist logs mkdir logs
%PY% -u scripts\nba_pbp.py rosters >> logs\daily_update.log 2>&1
%PY% -u scripts\nba_pbp.py matchups >> logs\daily_update.log 2>&1
