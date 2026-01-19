#!/usr/bin/env node

import { downloadFile, getTempFilePath, cleanupFile } from '../src/services/downloader.js';
import { processZipFile } from '../src/services/processor.js';
import { transformMunicipio } from '../src/transformers/municipio.js';
import { truncateTable } from '../src/services/loader.js';
import { pool, testConnection } from '../src/config/database.js';
import logger from '../src/config/logger.js';

/**
 * Teste rápido com arquivo de Municípios de 2025-12 (mais recente)
 */

async function loadMunicipiosData(records) {
  const client = await pool.connect();
  
  try {
    let inserted = 0;
    
    for (const municipio of records) {
      try {
        await client.query(
          `INSERT INTO municipios (codigo_municipio, nome_municipio)
           VALUES ($1, $2)
           ON CONFLICT (codigo_municipio) DO UPDATE SET
             nome_municipio = EXCLUDED.nome_municipio,
             updated_at = NOW()`,
          [municipio.codigo_municipio, municipio.nome_municipio]
        );
        inserted++;
      } catch (error) {
        logger.warn('test-municipios', `Erro ao inserir município ${municipio.codigo_municipio}`, error.message);
      }
    }
    
    logger.info('test-municipios', `${inserted} municípios carregados no banco`);
    return inserted;
  } finally {
    client.release();
  }
}

async function main() {
  console.log('='.repeat(60));
  console.log('Teste Rápido - Municípios 2025-12');
  console.log('='.repeat(60));
  console.log();

  const municipiosUrl = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-12/Municipios.zip';
  
  console.log(`📦 Arquivo: ${municipiosUrl.split('/').pop()}`);
  console.log(`🔗 URL: ${municipiosUrl}`);
  console.log();

  try {
    // 1. Testar conexão
    console.log('1️⃣  Testando conexão...');
    const dbTest = await testConnection();
    if (!dbTest.success) {
      throw new Error(`Falha na conexão: ${dbTest.error}`);
    }
    console.log(`✅ Banco conectado (${dbTest.timestamp})`);
    console.log();

    // 2. Truncar (opcional)
    if (process.argv.includes('--truncate')) {
      console.log('2️⃣  Truncando tabela municipios...');
      await truncateTable('municipios');
      console.log('✅ Tabela limpa');
      console.log();
    }

    // 3. Download
    console.log('3️⃣  Baixando arquivo...');
    const tempPath = getTempFilePath('Municipios.zip');
    await downloadFile(municipiosUrl, tempPath);
    console.log('✅ Download concluído');
    console.log();

    // 4. Processar
    console.log('4️⃣  Processando arquivo...');
    let totalLoaded = 0;
    
    const result = await processZipFile(
      tempPath,
      transformMunicipio,
      async (records) => {
        const loaded = await loadMunicipiosData(records);
        totalLoaded += loaded;
        console.log(`   📦 Batch: ${loaded} municípios`);
      }
    );
    
    console.log('✅ Processamento concluído');
    console.log();

    // 5. Cleanup
    console.log('5️⃣  Limpando arquivo temporário...');
    cleanupFile(tempPath);
    console.log('✅ Arquivo removido');
    console.log();

    // 6. Verificar
    console.log('6️⃣  Verificando dados no banco...');
    const client = await pool.connect();
    try {
      const countResult = await client.query('SELECT COUNT(*) FROM municipios');
      const count = parseInt(countResult.rows[0].count);
      console.log(`✅ Total de municípios no banco: ${count}`);
      
      const sampleResult = await client.query('SELECT * FROM municipios LIMIT 5');
      console.log('\n📋 Amostra:');
      sampleResult.rows.forEach(m => {
        console.log(`   ${m.codigo_municipio} - ${m.nome_municipio}`);
      });
    } finally {
      client.release();
    }

    console.log();
    console.log('='.repeat(60));
    console.log('✅ TESTE CONCLUÍDO COM SUCESSO!');
    console.log('='.repeat(60));
    console.log();
    console.log(`📊 Estatísticas:`);
    console.log(`   - Registros processados: ${result.totalRecords}`);
    console.log(`   - Registros carregados: ${totalLoaded}`);
    console.log(`   - Erros: ${result.errors}`);
    console.log();

    process.exit(0);
  } catch (error) {
    console.error();
    console.error('❌ ERRO:');
    console.error(error.message);
    console.error();
    console.error(error.stack);
    process.exit(1);
  }
}

main();
