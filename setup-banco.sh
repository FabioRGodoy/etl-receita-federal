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

# Configurações (lê do .env)
DB_HOST="145.223.94.201"
DB_PORT="5431"
DB_USER="postgres"
DB_NAME="etl_receita_federal"

echo "📋 Configurações:"
echo "   Host: $DB_HOST"
echo "   Porta: $DB_PORT"
echo "   Usuário: $DB_USER"
echo "   Banco: $DB_NAME"
echo ""

# 1. Criar banco
echo "1️⃣  Criando banco de dados..."
createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME && echo "   ✅ Banco criado!" || echo "   ⚠️  Banco já existe ou erro na criação"
echo ""

# 2. Criar schema
echo "2️⃣  Criando schema (tabelas)..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f sql/schema.sql
echo "   ✅ Schema criado!"
echo ""

# 3. Verificar tabelas
echo "3️⃣  Verificando tabelas criadas..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\dt"
echo ""

echo "======================================================================="
echo "✅ SETUP CONCLUÍDO!"
echo "======================================================================="
echo ""
echo "💡 Próximo passo: node test-municipios-simples.js"
echo ""
