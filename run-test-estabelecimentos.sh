#!/bin/bash
# Script para rodar teste de estabelecimentos
# OTIMIZADO: Não precisa de --max-old-space-size!
# Usa o mesmo padrão otimizado de sócios:
#   1. Backpressure controlado (pause/resume do stream)
#   2. Batch size otimizado (500 registros)
#   3. Limpeza agressiva de arrays após cada batch
#   4. Commit imediato por batch

echo "================================================"
echo "Teste de Estabelecimentos - ETL Otimizado"
echo "================================================"
echo ""
echo "Configurações:"
echo "- Batch Size: 500 registros (31 colunas = 15.500 valores)"
echo "- Backpressure: Controlado (pause/resume)"
echo "- Memory: Otimizado (sem necessidade de aumentar heap)"
echo "- Commit: Por batch (liberação imediata)"
echo ""
echo "AVISO: Este arquivo é MAIOR que sócios"
echo "       Pode levar 30-60 minutos para processar"
echo ""

node test-estabelecimentos-simples.js

echo ""
echo "================================================"
echo "Teste finalizado"
echo "================================================"
