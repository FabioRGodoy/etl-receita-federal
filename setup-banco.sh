#!/bin/bash
#
# Script para configurar o banco de dados na VPS
#

set -e

echo ""
echo "======================================================================="
echo "🔧 Setup do Banco de Dados - ETL Receita Federal"
echo "======================================================================="
echo ""

# 🆕 Carregar variáveis do .env
if [ -f .env ]; then
    echo "📁 Carregando configurações do .env..."
    export $(grep -v '^#' .env | grep -v '^$' | xargs)
    echo "   ✅ .env carregado!"
else
    echo "   ❌ Arquivo .env não encontrado!"
    exit 1
fi
echo ""

# Configurações (agora vem do .env)
echo "📋 Configurações:"
echo "   Host: $DB_HOST"
echo "   Porta: $DB_PORT"
echo "   Usuário: $DB_USER"
echo "   Banco: $DB_NAME"
echo ""

# Verificar se variáveis foram carregadas
if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_USER" ] || [ -z "$DB_NAME" ]; then
    echo "❌ ERRO: Variáveis de ambiente não configuradas corretamente!"
    echo "   Verifique se o arquivo .env tem:"
    echo "   - DB_HOST"
    echo "   - DB_PORT"
    echo "   - DB_USER"
    echo "   - DB_NAME"
    echo "   - DB_PASSWORD"
    exit 1
fi

# 🆕 Configurar senha (para não pedir interativamente)
export PGPASSWORD="$DB_PASSWORD"

# 1. Criar banco
echo "1️⃣  Criando banco de dados..."
createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME 2>/dev/null && echo "   ✅ Banco criado!" || echo "   ⚠️  Banco já existe ou erro na criação"
echo ""

# 2. Criar schema
echo "2️⃣  Criando schema (tabelas)..."
if [ ! -f "sql/schema.sql" ]; then
    echo "   ❌ Arquivo sql/schema.sql não encontrado!"
    exit 1
fi

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f sql/schema.sql
echo "   ✅ Schema criado!"
echo ""

# 3. Verificar tabelas
echo "3️⃣  Verificando tabelas criadas..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\dt"
echo ""

# 4. Verificar contagem de registros
echo "4️⃣  Verificando registros existentes..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT 
    'municipios' as tabela, COUNT(*) as registros FROM municipios
UNION ALL
SELECT 'estabelecimentos', COUNT(*) FROM estabelecimentos
UNION ALL
SELECT 'socios', COUNT(*) FROM socios;
"
echo ""

# Limpar senha da memória
unset PGPASSWORD

echo "======================================================================="
echo "✅ SETUP CONCLUÍDO!"
echo "======================================================================="
echo ""
echo "💡 Próximos passos:"
echo "   - Testar conexão: node test-conexao.js"
echo "   - Rodar ETL: npm run full-load -- --yes"
echo ""
