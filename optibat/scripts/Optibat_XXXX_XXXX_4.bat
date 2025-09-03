@echo off
setlocal
chcp 65001 >nul
echo Optibat URKI
echo ------------------
pushd "%~dp0\.."
set "OPTIBAT_ENV=URKI"
set "OPTIBAT_HEADLESS=true"
call conda activate optibat
for /l %%i in (1, 1, 3) do (
    call optibat.exe
    if not errorlevel 1 goto :exit
    timeout /t 60 /nobreak >nul
)
:exit
set "exitcode=%errorlevel%"
call conda deactivate
popd
endlocal
exit /b %exitcode%
