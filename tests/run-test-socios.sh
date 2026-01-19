#!/bin/bash
# Script para rodar teste de sócios
# OTIMIZADO: Não precisa mais de --max-old-space-size!
# Correções aplicadas:
#   1. Backpressure controlado (pause/resume do stream)
#   2. Batch size otimizado (500 registros)
#   3. Limpeza agressiva de arrays após cada batch
#   4. Commit imediato por batch

echo "================================================"
echo "Teste de Socios - ETL Otimizado"
echo "================================================"
echo ""
echo "Configurações:"
echo "- Batch Size: 500 registros"
echo "- Backpressure: Controlado (pause/resume)"
echo "- Memory: Otimizado (sem necessidade de aumentar heap)"
echo "- Commit: Por batch (liberação imediata)"
echo ""

node test-socios-simples.js

echo ""
echo "================================================"
echo "Teste finalizado"
echo "================================================"
