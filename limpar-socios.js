#!/usr/bin/env node

import { pool } from './src/config/database.js';

async function limpar() {
  const client = await pool.connect();
  try {
    console.log('🗑️  Limpando tabela socios...');
    const result = await client.query('TRUNCATE TABLE socios');
    console.log('✅ Tabela limpa com sucesso!');
    process.exit(0);
  } catch (error) {
    console.error('❌ Erro:', error.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

limpar();
