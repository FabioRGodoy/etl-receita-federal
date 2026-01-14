#!/bin/bash
#
# Script para executar carga DELTA automaticamente
# Configurar no cron para execução diária
#

set -e

# Diretório do projeto
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Timestamp
echo "========================================="
echo "ETL Delta - Início: $(date)"
echo "========================================="

# Executar DELTA
node src/cli/delta.js --yes

EXIT_CODE=$?

# Log final
echo "========================================="
echo "ETL Delta - Fim: $(date)"
echo "Exit code: $EXIT_CODE"
echo "========================================="

# Enviar notificação em caso de erro (opcional)
# Descomentar e configurar se tiver sendmail ou similar
# if [ $EXIT_CODE -ne 0 ]; then
#     echo "ETL DELTA falhou em $(date)" | mail -s "ETL Error" seu-email@example.com
# fi

exit $EXIT_CODE
