@echo off
setlocal
chcp 65001 >nul
echo Optibat
echo ------------------
pushd "%~dp0\.."
call conda activate optibat
call optibat.exe
set "exitcode=%errorlevel%"
call conda deactivate
popd
endlocal
exit /b %exitcode%
