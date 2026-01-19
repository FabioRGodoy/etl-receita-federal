import { pool } from '../../src/config/database.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function runMigration(sqlFilePath) {
  if (!sqlFilePath) {
    console.error('❌ Uso: npm run migrate <arquivo-sql>');
    console.error('   Exemplo: npm run migrate sql/migrations/migrate-add-last-modified.sql');
    process.exit(1);
  }

  // Resolver caminho absoluto
  const fullPath = path.resolve(sqlFilePath);
  
  if (!fs.existsSync(fullPath)) {
    console.error(`❌ Arquivo não encontrado: ${fullPath}`);
    process.exit(1);
  }

  console.log(`📄 Arquivo: ${path.basename(sqlFilePath)}`);
  console.log(`📁 Caminho: ${fullPath}`);
  console.log();

  const sql = fs.readFileSync(fullPath, 'utf8');
  
  const client = await pool.connect();
  
  try {
    console.log('🔧 Aplicando migração...');
    console.log();
    
    await client.query(sql);
    
    console.log('✅ Migração aplicada com sucesso!');
    console.log();
  } catch (error) {
    console.error('❌ Erro ao aplicar migração:', error.message);
    console.error();
    console.error('Detalhes:', error);
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

// Obter arquivo da linha de comando
const sqlFile = process.argv[2];
runMigration(sqlFile);
