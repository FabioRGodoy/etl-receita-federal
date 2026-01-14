#!/usr/bin/env node

import { downloadFile, getTempFilePath, cleanupFile } from '../services/downloader.js';
import { processZipFile } from '../services/processor.js';
import { transformMunicipio } from '../transformers/municipio.js';
import { truncateTable } from '../services/loader.js';
import { pool, testConnection } from '../config/database.js';
import logger from '../config/logger.js';

/**
 * CLI para testar pipeline completo de Municípios
 * Baixa, processa e carrega arquivo de municípios
 */

async function loadMunicipiosData(records) {
  const client = await pool.connect();
  
  try {
    let inserted = 0;
    
    // Inserir em batch
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
  console.log('ETL Receita Federal - Teste de Pipeline (Municípios)');
  console.log('='.repeat(60));
  console.log();

  // URL do arquivo de municípios (ajustar se necessário)
  const municipiosUrl = process.argv[2] || 'https://dadosabertos.rfb.gov.br/CNPJ/Municipios.zip';
  
  console.log(`URL: ${municipiosUrl}`);
  console.log();

  try {
    // 1. Testar conexão com banco
    console.log('1️⃣  Testando conexão com banco...');
    const dbTest = await testConnection();
    if (!dbTest.success) {
      throw new Error(`Falha na conexão: ${dbTest.error}`);
    }
    console.log(`✅ Conectado ao banco (${dbTest.timestamp})`);
    console.log();

    // 2. Truncar tabela (opcional)
    const shouldTruncate = process.argv.includes('--truncate');
    if (shouldTruncate) {
      console.log('2️⃣  Truncando tabela municipios...');
      await truncateTable('municipios');
      console.log('✅ Tabela truncada');
      console.log();
    }

    // 3. Download
    console.log('3️⃣  Baixando arquivo...');
    const tempPath = getTempFilePath('Municipios.zip');
    await downloadFile(municipiosUrl, tempPath);
    console.log('✅ Download concluído');
    console.log();

    // 4. Processar ZIP
    console.log('4️⃣  Processando arquivo ZIP...');
    let totalLoaded = 0;
    
    const result = await processZipFile(
      tempPath,
      transformMunicipio,
      async (records) => {
        const loaded = await loadMunicipiosData(records);
        totalLoaded += loaded;
        console.log(`   📦 Batch processado: ${loaded} municípios`);
      }
    );
    
    console.log('✅ Processamento concluído');
    console.log();

    // 5. Limpar arquivo temporário
    console.log('5️⃣  Limpando arquivo temporário...');
    cleanupFile(tempPath);
    console.log('✅ Arquivo removido');
    console.log();

    // 6. Verificar dados no banco
    console.log('6️⃣  Verificando dados no banco...');
    const client = await pool.connect();
    try {
      const countResult = await client.query('SELECT COUNT(*) FROM municipios');
      const count = parseInt(countResult.rows[0].count);
      console.log(`✅ Total de municípios no banco: ${count}`);
      
      // Amostra
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
    console.log('✅ Pipeline de Municípios concluído com sucesso!');
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
    console.error('❌ Erro no pipeline:');
    console.error(error.message);
    console.error();
    console.error(error.stack);
    process.exit(1);
  }
}

main();
