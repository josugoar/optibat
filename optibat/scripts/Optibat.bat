REM Follow this template for the rest of installations, like in the new XXXX_XXXX (and even XXXX_XXXX?).
@echo off
setlocal
chcp 65001 >nul
echo Optibat
echo ------------------
pushd "%~dp0\.."
call conda activate optibat
REM Headless execution MUST be synchonized with market using the internal XXXX_XXXX.
REM 12:00, 14:00, 21:00, etc.
call optibat.exe
set "exitcode=%errorlevel%"
call conda deactivate
popd
endlocal
exit /b %exitcode%
