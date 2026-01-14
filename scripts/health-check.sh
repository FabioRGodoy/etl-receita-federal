#!/bin/bash
#
# Script de health check para monitoramento
#

# Configurações
DB_NAME="${DB_NAME:-etl_receita_federal}"
DB_USER="${DB_USER:-postgres}"

echo "========================================="
echo "Health Check - $(date)"
echo "========================================="
echo

# 1. Verificar conexão com banco
echo "🔍 Verificando conexão com banco..."
psql -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "❌ ERRO: Banco de dados inacessível"
    exit 1
fi
echo "✅ Banco de dados OK"
echo

# 2. Verificar última execução
echo "🔍 Verificando última execução..."
LAST_RUN=$(psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "
  SELECT 
    run_type || ' - ' || 
    status || ' - ' || 
    completed_at::text
  FROM etl_control_runs 
  ORDER BY id DESC 
  LIMIT 1;
")

if [ -z "$LAST_RUN" ]; then
    echo "⚠️  Nenhuma execução encontrada"
else
    echo "ℹ️  Última execução: $LAST_RUN"
fi
echo

# 3. Contar registros principais
echo "🔍 Contando registros..."
MUNICIPIOS=$(psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT COUNT(*) FROM municipios;")
ESTABELECIMENTOS=$(psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT COUNT(*) FROM estabelecimentos;")
SOCIOS=$(psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT COUNT(*) FROM socios;")

echo "   Municípios: $MUNICIPIOS"
echo "   Estabelecimentos: $ESTABELECIMENTOS"
echo "   Sócios: $SOCIOS"
echo

# 4. Verificar arquivos pendentes ou com erro
echo "🔍 Verificando status de arquivos..."
PENDING=$(psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT COUNT(*) FROM etl_control_files WHERE status = 'pending';")
ERROR=$(psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT COUNT(*) FROM etl_control_files WHERE status = 'error';")
DONE=$(psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "SELECT COUNT(*) FROM etl_control_files WHERE status = 'done';")

echo "   Concluídos: $DONE"
echo "   Pendentes: $PENDING"
echo "   Com erro: $ERROR"
echo

# 5. Verificar espaço em disco
echo "🔍 Verificando espaço em disco..."
df -h / | tail -1
echo

echo "========================================="
if [ "$ERROR" -gt 0 ]; then
    echo "⚠️  Sistema OK com ressalvas ($ERROR arquivos com erro)"
    exit 2
else
    echo "✅ Sistema OK"
    exit 0
fi
echo "========================================="
