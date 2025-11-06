@echo off
setlocal
cd /d "C:\Users\Thiago Doria\Desktop\Relatorio diário de opções"
call .venv\Scripts\activate.bat
python src\python\orchestrator.py --debug
endlocal
