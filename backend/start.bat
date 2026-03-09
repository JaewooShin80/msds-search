@echo off
chcp 65001 > nul

echo [1/3] 8000번 포트 점유 프로세스 종료 중...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8000 " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a > nul 2>&1
)
timeout /t 1 /nobreak > nul

echo [2/3] UTF-8 인코딩 설정...
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo [3/3] 서버 시작 (http://localhost:8000)
call venv\Scripts\python.exe -X utf8 main.py
