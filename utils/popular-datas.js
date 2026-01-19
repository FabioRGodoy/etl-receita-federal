#!/usr/bin/env node

/**
 * Popula datas de modificação no banco sem reprocessar arquivos
 * 
 * Útil após aplicar a migration que adiciona a coluna last_modified_date.
 * Em vez de esperar o DELTA reprocessar todos os arquivos (7-10h), 
 * este script apenas busca as datas no site e atualiza o banco (rápido).
 * 
 * USO:
 *   node utils/popular-datas.js
 *   node utils/popular-datas.js https://url-customizada/
 */

import { pool } from '../src/config/database.js';
import { discoverFiles } from '../src/services/discovery.js';
import { CONFIG } from '../src/config/constants.js';
import logger from '../src/config/logger.js';

async function main() {
  console.log('='.repeat(80));
  console.log('Popular Datas de Modificação (sem reprocessar)');
  console.log('='.repeat(80));
  console.log();
  
  const args = process.argv.slice(2).filter(arg => !arg.startsWith('--'));
  const baseUrl = args[0] || CONFIG.BASE_URL;
  
  console.log(`URL Base: ${baseUrl}`);
  console.log();
  console.log('🔍 Buscando arquivos no site...');
  
  const client = await pool.connect();
  
  try {
    // 1. Descobrir arquivos (com datas)
    const discoveredFiles = await discoverFiles(baseUrl, { latestOnly: true });
    console.log(`📦 Arquivos encontrados: ${discoveredFiles.length}`);
    console.log();
    
    let updated = 0;
    let notFound = 0;
    let noDate = 0;
    
    // 2. Para cada arquivo, atualizar data no banco
    for (const file of discoveredFiles) {
      if (!file.lastModified) {
        console.log(`⚠️  ${file.fileName} - SEM DATA no site`);
        noDate++;
        continue;
      }
      
      const result = await client.query(
        `UPDATE etl_control_files 
         SET last_modified_date = $1, updated_at = NOW()
         WHERE file_name = $2`,
        [file.lastModified, file.fileName]
      );
      
      if (result.rowCount > 0) {
        const dateStr = new Date(file.lastModified).toLocaleString('pt-BR');
        console.log(`✅ ${file.fileName} → ${dateStr}`);
        updated++;
      } else {
        console.log(`⚠️  ${file.fileName} - NÃO ENCONTRADO no banco`);
        notFound++;
      }
    }
    
    console.log();
    console.log('='.repeat(80));
    console.log('📊 Resumo:');
    console.log(`   ✅ Atualizados: ${updated}`);
    console.log(`   ⚠️  Não encontrados no banco: ${notFound}`);
    console.log(`   ⚠️  Sem data no site: ${noDate}`);
    console.log('='.repeat(80));
    console.log();
    
    if (updated > 0) {
      console.log('✅ Datas populadas com sucesso!');
      console.log('   Agora o DELTA só reprocessará arquivos realmente atualizados.');
      console.log();
    } else {
      console.log('⚠️  Nenhuma data foi atualizada.');
      console.log('   Verifique se já executou um FULL LOAD antes.');
      console.log();
    }
    
  } catch (error) {
    console.error('❌ Erro ao popular datas:', error.message);
    logger.error('popular-datas', 'Erro fatal', error);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

main();
