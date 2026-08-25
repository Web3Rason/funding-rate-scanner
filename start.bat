@echo off
cd /d "%~dp0"

:: 清掉還佔著埠號的舊程序（5011 = 後端、5173 = Vite dev server）
for %%P in (5011 5173) do (
    for /f "tokens=5" %%A in ('netstat -ano ^| findstr ":%%P .*LISTENING"') do (
        taskkill /F /PID %%A >nul 2>&1
    )
)

:: 啟動後端（同時 serve API 與 frontend/dist）
python launcher.py
