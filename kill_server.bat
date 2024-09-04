@echo off
echo Killing ICAP and FastAPI server...

:: Find the process ID (PID) of the Python script running main.py
for /f "tokens=2" %%a in ('tasklist /fi "imagename eq python.exe" /fo table ^| findstr /i "python"') do (
    echo Found Python process with PID: %%a
    taskkill /PID %%a /F
)

echo Done.

