#!/bin/bash
#
# Script para fazer backup do banco de dados PostgreSQL
# Configurar no cron para execução diária
#

# Configurações
BACKUP_DIR="${BACKUP_DIR:-/opt/backups/etl-receita-federal}"
DB_NAME="${DB_NAME:-etl_receita_federal}"
DB_USER="${DB_USER:-postgres}"
DAYS_TO_KEEP=7

# Criar diretório se não existir
mkdir -p "$BACKUP_DIR"

# Timestamp
DATE=$(date +%Y%m%d_%H%M%S)
FILENAME="etl_receita_federal_$DATE.sql.gz"

echo "========================================="
echo "Backup - Início: $(date)"
echo "Arquivo: $FILENAME"
echo "========================================="

# Realizar backup
pg_dump -U "$DB_USER" -d "$DB_NAME" | gzip > "$BACKUP_DIR/$FILENAME"

if [ $? -eq 0 ]; then
    echo "✅ Backup concluído com sucesso"
    
    # Remover backups antigos (manter últimos N dias)
    find "$BACKUP_DIR" -name "etl_receita_federal_*.sql.gz" -mtime +$DAYS_TO_KEEP -delete
    echo "🗑️  Backups antigos removidos (mantidos últimos $DAYS_TO_KEEP dias)"
else
    echo "❌ Erro no backup"
    exit 1
fi

echo "========================================="
echo "Backup - Fim: $(date)"
echo "========================================="
