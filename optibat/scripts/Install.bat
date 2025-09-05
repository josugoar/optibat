@echo off
pushd "%~dp0\.."
call conda create --name optibat --yes python=3.11
call conda activate optibat
REM Use glpk INSTEAD of ipopt (why ipopt? XXXX_XXXX).
call conda install --yes glpk
REM In production use versioned deployments.
call pip install --editable .
call conda deactivate
popd
