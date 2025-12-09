@echo off
REM Script de limpieza para EVAQUA antes del deploy
echo ========================================
echo EVAQUA - Limpieza Pre-Deploy
echo ========================================
echo.

echo [1/4] Eliminando archivos __pycache__...
for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"
echo OK

echo [2/4] Eliminando archivos .pyc...
del /s /q *.pyc 2>nul
echo OK

echo [3/4] Eliminando archivos .pyo...
del /s /q *.pyo 2>nul
echo OK

echo [4/4] Eliminando logs...
del /s /q *.log 2>nul
echo OK

echo.
echo ========================================
echo Limpieza completada!
echo ========================================
echo.
echo Ahora puedes hacer commit y push a Streamlit Cloud
pause
