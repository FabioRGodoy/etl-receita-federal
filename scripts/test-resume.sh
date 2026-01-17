#!/bin/bash

# Script de teste para validar resume capability do ETL

echo "🧪 Teste de Resume Capability"
echo "=============================="
echo ""

# 1. Verificar se checkpoint existe no banco
echo "1️⃣  Verificando coluna checkpoint..."
node -e "
import { pool } from './src/config/database.js';

async function check() {
  const result = await pool.query(\`
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'etl_control_files' 
      AND column_name = 'checkpoint'
  \`);
  
  if (result.rows.length > 0) {
    console.log('✅ Coluna checkpoint existe');
  } else {
    console.log('❌ Coluna checkpoint NÃO existe - rode a migração primeiro!');
    console.log('   node run-migration.js');
    process.exit(1);
  }
  
  await pool.end();
}

check();
"

if [ $? -ne 0 ]; then
  exit 1
fi

echo ""
echo "2️⃣  Iniciando ETL em background..."
npm run full-load -- --yes > test-etl.log 2>&1 &
ETL_PID=$!
echo "   PID: $ETL_PID"
echo "   Aguardando 60 segundos..."
sleep 60

echo ""
echo "3️⃣  Simulando crash (kill -9)..."
kill -9 $ETL_PID 2>/dev/null || echo "   Processo já terminou"

echo ""
echo "4️⃣  Verificando checkpoint salvo no banco..."
node -e "
import { pool } from './src/config/database.js';

async function check() {
  const result = await pool.query(\`
    SELECT 
      file_name,
      status,
      checkpoint->>'linesProcessed' as lines_processed,
      checkpoint->>'lastCheckpoint' as last_checkpoint
    FROM etl_control_files
    WHERE checkpoint IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 5
  \`);
  
  if (result.rows.length > 0) {
    console.log('✅ Checkpoint encontrado:');
    console.table(result.rows);
  } else {
    console.log('⚠️  Nenhum checkpoint encontrado (pode ser muito rápido)');
  }
  
  await pool.end();
}

check();
"

echo ""
echo "5️⃣  Retomando ETL..."
echo "   Logs serão salvos em test-etl-resume.log"
npm run full-load -- --yes > test-etl-resume.log 2>&1

echo ""
echo "6️⃣  Resultado:"
if [ $? -eq 0 ]; then
  echo "✅ ETL retomado com sucesso!"
else
  echo "❌ ETL falhou - verifique test-etl-resume.log"
fi

echo ""
echo "📋 Para verificar logs:"
echo "   tail -f test-etl.log"
echo "   tail -f test-etl-resume.log"
