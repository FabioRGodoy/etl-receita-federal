#!/usr/bin/env node

/**
 * Verifica datas de modificação registradas no banco
 */

import { pool } from '../src/config/database.js';

async function main() {
  console.log('='.repeat(80));
  console.log('Verificar Datas de Modificação');
  console.log('='.repeat(80));
  console.log();
  
  const client = await pool.connect();
  
  try {
    // Contar total de registros
    const totalResult = await client.query(
      'SELECT COUNT(*) as total FROM etl_control_files'
    );
    console.log(`📦 Total de arquivos no banco: ${totalResult.rows[0].total}`);
    console.log();
    
    // Contar com e sem data
    const statsResult = await client.query(`
      SELECT 
        COUNT(*) FILTER (WHERE last_modified_date IS NOT NULL) as com_data,
        COUNT(*) FILTER (WHERE last_modified_date IS NULL) as sem_data
      FROM etl_control_files
    `);
    
    const { com_data, sem_data } = statsResult.rows[0];
    console.log(`✅ Com data: ${com_data}`);
    console.log(`⚠️  Sem data: ${sem_data}`);
    console.log();
    
    // Mostrar últimos 10 registros
    const filesResult = await client.query(`
      SELECT 
        file_name,
        file_type,
        status,
        last_modified_date,
        updated_at
      FROM etl_control_files
      ORDER BY updated_at DESC
      LIMIT 10
    `);
    
    if (filesResult.rows.length > 0) {
      console.log('📋 Últimos 10 arquivos (ordenados por atualização):');
      console.log();
      filesResult.rows.forEach(f => {
        const date = f.last_modified_date 
          ? new Date(f.last_modified_date).toLocaleString('pt-BR')
          : 'SEM DATA';
        const updated = new Date(f.updated_at).toLocaleString('pt-BR');
        console.log(`  ${f.file_name.padEnd(30)} | ${f.status.padEnd(10)} | ${date.padEnd(20)} | Atualizado: ${updated}`);
      });
    }
    
    console.log();
    console.log('='.repeat(80));
    
  } catch (error) {
    console.error('❌ Erro:', error.message);
  } finally {
    client.release();
    await pool.end();
  }
}

main();
