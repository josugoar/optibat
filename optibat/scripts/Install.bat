@echo off
pushd "%~dp0\.."
call conda create --name optibat --yes python=3.11
call conda activate optibat
call conda install --yes glpk
call pip install --editable .
call conda deactivate
popd
