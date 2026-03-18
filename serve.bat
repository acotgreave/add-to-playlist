@echo off
start "" http://localhost:5051
cd /d "%~dp0docs"
python -m http.server 5051
