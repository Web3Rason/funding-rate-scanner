@echo off
cd /d "%~dp0"
python monitor_launcher.py
echo.
echo LAB 指數監控已在背景啟動，log: logs\lab_monitor.log
echo 關閉本視窗不會停止監控。要停止請用工作管理員結束對應的 python。
pause
