@echo off
title MCPGate Project Launcher
echo ========================================================
echo Starting MCPGate Agentic Orchestration Environment...
echo ========================================================
echo.

echo [1/4] Installing Python Requirements...
pip install -r requirements.txt
echo.

echo [2/4] Starting Docker Services (PostgreSQL ^& Redis)...
docker-compose up -d
echo.

echo [3/4] Launching FastAPI Backend (Port 8000)...
:: Open the backend server in a new command prompt window
start "MCPGate Backend" cmd /k "python main.py"
echo.

echo [4/4] Opening the Dashboard UI...
:: Give the backend a second to start
timeout /t 2 /nobreak > nul
start mcpgate.html

echo.
echo ========================================================
echo MCPGate is now running! 
echo - Backend: http://localhost:8000
echo - Dashboard UI is opening in your default web browser.
echo - Firebase tracking is active on the dashboard.
echo ========================================================
pause
