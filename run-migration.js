import { pool } from './src/config/database.js';
import fs from 'fs';

async function runMigration() {
  const sql = fs.readFileSync('./sql/migrate-add-checkpoint.sql', 'utf8');
  
  const client = await pool.connect();
  
  try {
    console.log('🔧 Aplicando migração...');
    const result = await client.query(sql);
    console.log('✅ Migração aplicada com sucesso!');
    console.log(result);
  } catch (error) {
    console.error('❌ Erro ao aplicar migração:', error.message);
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

runMigration();
