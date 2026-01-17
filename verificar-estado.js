import { pool } from './src/config/database.js';

/**
 * Script para verificar o estado do ETL e checkpoints
 */

async function verificarEstado() {
  try {
    console.log('🔍 Verificando estado do ETL...\n');
    
    // 1. Runs em andamento
    const runs = await pool.query(`
      SELECT 
        id,
        run_type,
        status,
        total_files,
        files_completed,
        files_failed,
        started_at,
        completed_at
      FROM etl_control_runs
      ORDER BY started_at DESC
      LIMIT 5
    `);
    
    console.log('📊 Últimos Runs:');
    console.table(runs.rows);
    
    // 2. Arquivos em processamento
    const processing = await pool.query(`
      SELECT 
        file_name,
        status,
        checkpoint->>'linesProcessed' as lines_processed,
        checkpoint->>'lastCheckpoint' as last_checkpoint,
        started_at
      FROM etl_control_files
      WHERE status = 'processing'
      ORDER BY started_at DESC
    `);
    
    if (processing.rows.length > 0) {
      console.log('\n⏳ Arquivos em processamento:');
      console.table(processing.rows);
    }
    
    // 3. Arquivos pendentes
    const pending = await pool.query(`
      SELECT 
        file_name,
        file_type,
        status
      FROM etl_control_files
      WHERE status IN ('pending', 'error')
      ORDER BY 
        CASE file_type
          WHEN 'municipios' THEN 1
          WHEN 'estabelecimentos' THEN 2
          WHEN 'socios' THEN 3
          ELSE 999
        END,
        file_name
    `);
    
    if (pending.rows.length > 0) {
      console.log(`\n📋 ${pending.rows.length} arquivos pendentes:`);
      console.table(pending.rows);
    } else {
      console.log('\n✅ Nenhum arquivo pendente');
    }
    
    // 4. Arquivos com checkpoint
    const withCheckpoint = await pool.query(`
      SELECT 
        file_name,
        status,
        checkpoint->>'linesProcessed' as lines_processed,
        checkpoint->>'completed' as completed,
        checkpoint->>'lastCheckpoint' as last_checkpoint
      FROM etl_control_files
      WHERE checkpoint IS NOT NULL
      ORDER BY created_at DESC
      LIMIT 10
    `);
    
    if (withCheckpoint.rows.length > 0) {
      console.log('\n💾 Arquivos com checkpoint:');
      console.table(withCheckpoint.rows);
    }
    
    // 5. Estatísticas gerais
    const stats = await pool.query(`
      SELECT 
        status,
        COUNT(*) as total,
        SUM(records_inserted) as total_inserted
      FROM etl_control_files
      GROUP BY status
      ORDER BY 
        CASE status
          WHEN 'completed' THEN 1
          WHEN 'processing' THEN 2
          WHEN 'pending' THEN 3
          WHEN 'error' THEN 4
          ELSE 999
        END
    `);
    
    console.log('\n📈 Estatísticas por Status:');
    console.table(stats.rows);
    
    // 6. Verificar arquivos baixados mas não processados
    const fs = await import('fs');
    const path = await import('path');
    const tempDir = process.env.TEMP_DIR || './temp';
    
    if (fs.existsSync(tempDir)) {
      const files = fs.readdirSync(tempDir).filter(f => f.endsWith('.zip'));
      
      if (files.length > 0) {
        console.log(`\n📦 ${files.length} arquivos ZIP no diretório temp:`);
        
        for (const file of files) {
          const filePath = path.join(tempDir, file);
          const stats = fs.statSync(filePath);
          const sizeMB = (stats.size / 1024 / 1024).toFixed(2);
          console.log(`   - ${file} (${sizeMB} MB)`);
        }
      } else {
        console.log('\n📦 Nenhum arquivo ZIP no diretório temp');
      }
    }
    
  } catch (error) {
    console.error('❌ Erro:', error.message);
  } finally {
    await pool.end();
  }
}

verificarEstado();
