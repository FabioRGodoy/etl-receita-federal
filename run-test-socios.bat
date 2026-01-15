@echo off
REM Script para rodar teste de sócios
REM OTIMIZADO: Não precisa mais de --max-old-space-size!
REM Correções aplicadas:
REM   1. Backpressure controlado (pause/resume do stream)
REM   2. Batch size otimizado (500 registros)
REM   3. Limpeza agressiva de arrays após cada batch
REM   4. Commit imediato por batch

echo ================================================
echo Teste de Socios - ETL Otimizado
echo ================================================
echo.
echo Configuracoes:
echo - Batch Size: 500 registros
echo - Backpressure: Controlado (pause/resume)
echo - Memory: Otimizado (sem necessidade de aumentar heap)
echo - Commit: Por batch (liberacao imediata)
echo.

node test-socios-simples.js

echo.
echo ================================================
echo Teste finalizado
echo ================================================
