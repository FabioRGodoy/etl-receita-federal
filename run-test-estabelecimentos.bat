@echo off
REM Script para rodar teste de estabelecimentos
REM OTIMIZADO: Não precisa de --max-old-space-size!
REM Usa o mesmo padrão otimizado de sócios:
REM   1. Backpressure controlado (pause/resume do stream)
REM   2. Batch size otimizado (500 registros)
REM   3. Limpeza agressiva de arrays após cada batch
REM   4. Commit imediato por batch

echo ================================================
echo Teste de Estabelecimentos - ETL Otimizado
echo ================================================
echo.
echo Configuracoes:
echo - Batch Size: 500 registros (31 colunas = 15.500 valores)
echo - Backpressure: Controlado (pause/resume)
echo - Memory: Otimizado (sem necessidade de aumentar heap)
echo - Commit: Por batch (liberacao imediata)
echo.
echo AVISO: Este arquivo e MAIOR que socios
echo        Pode levar 30-60 minutos para processar
echo.

node test-estabelecimentos-simples.js

echo.
echo ================================================
echo Teste finalizado
echo ================================================
