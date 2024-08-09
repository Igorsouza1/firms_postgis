@echo off
REM Define o caminho para o interpretador Python
set PYTHON_PATH=C:\Users\IHP-Sistemas\AppData\Local\Programs\Python\Python312\python.exe

REM Define o caminho para o script Python
set SCRIPT_PATH=E:\BD_IHP\Automacoes\FIRMS\FirmsAtualizacao\main.py

REM Define o caminho para o arquivo GeoJSON
set GEOJSON_PATH=E:\BD_IHP\Automacoes\FIRMS\FirmsAtualizacao\geometry.geojson

REM Define o caminho para o arquivo de log
set LOG_PATH=E:\BD_IHP\Automacoes\FIRMS\FirmsAtualizacao\execution.log

REM Executa o script Python com os argumentos
%PYTHON_PATH% %SCRIPT_PATH% %GEOJSON_PATH% %LOG_PATH%

exit
