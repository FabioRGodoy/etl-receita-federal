#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-${ETL_MODE:-full}}"
shift || true

# Permite sobrescrever totalmente o comando (útil no Coolify)
if [[ -n "${ETL_COMMAND:-}" ]]; then
  exec bash -lc "$ETL_COMMAND"
fi

BASE_URL="${ETL_BASE_URL:-${BASE_URL:-}}"
TEMP_DIR="${TEMP_DIR:-/data/temp}"
LOG_DIR="${LOG_DIR:-/data/logs}"

mkdir -p "$TEMP_DIR" "$LOG_DIR"

case "$MODE" in
  full|full-load|full_load)
    cmd=(node src/cli/full-load.js)
    [[ -n "$BASE_URL" ]] && cmd+=("$BASE_URL")
    cmd+=(--yes)
    ;;
  delta)
    cmd=(node src/cli/delta.js)
    [[ -n "$BASE_URL" ]] && cmd+=("$BASE_URL")
    cmd+=(--yes)
    ;;
  discovery|discover)
    cmd=(node src/cli/discovery-test.js)
    [[ -n "$BASE_URL" ]] && cmd+=("$BASE_URL")
    ;;
  *)
    echo "Modo inválido: '$MODE' (use: full | delta | discovery)" >&2
    exit 2
    ;;
esac

# Encaminha args extras do container (opcional)
if [[ $# -gt 0 ]]; then
  cmd+=("$@")
fi

echo "[entrypoint] mode=$MODE baseUrl=${BASE_URL:-<default>} tempDir=$TEMP_DIR logDir=$LOG_DIR" >&2

# Executar comando
"${cmd[@]}"

EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
  echo "" >&2
  echo "[entrypoint] ✅ ETL concluído com sucesso!" >&2
  echo "[entrypoint] Dados preservados no banco." >&2
  echo "[entrypoint] Container permanecerá ativo para consultas." >&2
  echo "" >&2
  echo "[entrypoint] Para executar novamente:" >&2
  echo "[entrypoint]   1. Execute: node limpar-controle.js" >&2
  echo "[entrypoint]   2. Execute: npm run full-load -- --yes" >&2
  echo "" >&2
  
  # Manter container vivo (não reiniciar automaticamente)
  tail -f /dev/null
else
  echo "[entrypoint] ❌ ETL falhou com código: $EXIT_CODE" >&2
  exit $EXIT_CODE
fi
