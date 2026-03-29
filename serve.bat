@echo off
start "" http://localhost:5051
cd /d "%~dp0docs"
npx serve . -l 5051
