#!/usr/bin/env node

import { downloadFile, getTempFilePath, cleanupFile } from '../src/services/downloader.js';
import { processZipFile } from '../src/services/processor.js';
import { transformMunicipio } from '../src/transformers/municipio.js';
import { pool, testConnection } from '../src/config/database.js';
import logger from '../src/config/logger.js';
import fs from 'fs';

/**
 * Teste SIMPLES - Apenas 1 arquivo de Municípios
 * Para validar que o fluxo básico funciona
 */

async function loadMunicipiosData(records) {
  const client = await pool.connect();
  
  try {
    let inserted = 0;
    let updated = 0;
    
    for (const municipio of records) {
      try {
        const result = await client.query(
          `INSERT INTO municipios (codigo_municipio, nome_municipio)
           VALUES ($1, $2)
           ON CONFLICT (codigo_municipio) DO UPDATE SET
             nome_municipio = EXCLUDED.nome_municipio,
             updated_at = NOW()`,
          [municipio.codigo_municipio, municipio.nome_municipio]
        );
        
        if (result.rowCount > 0) {
          inserted++;
        }
      } catch (error) {
        logger.warn('test', `Erro ao inserir município ${municipio.codigo_municipio}: ${error.message}`);
      }
    }
    
    console.log(`   ✅ Batch processado: ${inserted} municípios inseridos/atualizados`);
    return inserted;
  } finally {
    client.release();
  }
}

async function main() {
  console.log('\n' + '='.repeat(70));
  console.log('🧪 TESTE SIMPLES - 1 Arquivo de Municípios');
  console.log('='.repeat(70) + '\n');

  // Arquivo mais recente (2025-12)
  const arquivo = {
    nome: 'Municipios.zip',
    url: 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-12/Municipios.zip'
  };
  
  console.log(`📦 Arquivo: ${arquivo.nome}`);
  console.log(`🔗 URL: ${arquivo.url}`);
  console.log();

  try {
    // 1. Testar conexão com banco
    console.log('1️⃣  Testando conexão com PostgreSQL...');
    const dbTest = await testConnection();
    if (!dbTest.success) {
      throw new Error(`❌ Falha na conexão com banco: ${dbTest.error}`);
    }
    console.log(`   ✅ Conectado ao banco (${dbTest.timestamp})\n`);

    // 2. Verificar se tabela existe
    console.log('2️⃣  Verificando tabela municipios...');
    const client = await pool.connect();
    try {
      await client.query('SELECT 1 FROM municipios LIMIT 1');
      console.log('   ✅ Tabela municipios existe\n');
    } catch (error) {
      throw new Error(`❌ Tabela municipios não existe. Execute: psql -d etl_receita_federal -f sql/schema.sql`);
    } finally {
      client.release();
    }

    // 3. Download do arquivo
    console.log('3️⃣  Verificando arquivo...');
    const tempPath = getTempFilePath(arquivo.nome);
    console.log(`   📁 Destino: ${tempPath}`);
    
    if (fs.existsSync(tempPath)) {
      const stats = fs.statSync(tempPath);
      const sizeMB = (stats.size / (1024 * 1024)).toFixed(2);
      console.log(`   ✅ Arquivo já existe (${sizeMB} MB) - pulando download\n`);
    } else {
      console.log('   📥 Baixando arquivo...');
      await downloadFile(arquivo.url, tempPath);
      console.log('   ✅ Download concluído\n');
    }

    // 4. Processar arquivo
    console.log('4️⃣  Processando arquivo ZIP...');
    let totalCarregados = 0;
    
    const resultado = await processZipFile(
      tempPath,
      transformMunicipio,
      async (records) => {
        const carregados = await loadMunicipiosData(records);
        totalCarregados += carregados;
      }
    );
    
    console.log('   ✅ Processamento concluído\n');

    // 5. Limpar arquivo temporário
    console.log('5️⃣  Limpando arquivo temporário...');
    cleanupFile(tempPath);
    console.log('   ✅ Arquivo removido\n');

    // 6. Verificar dados no banco
    console.log('6️⃣  Verificando dados carregados...');
    const clientVerify = await pool.connect();
    try {
      const countResult = await clientVerify.query('SELECT COUNT(*) FROM municipios');
      const total = parseInt(countResult.rows[0].count);
      console.log(`   📊 Total de municípios no banco: ${total}`);
      
      // Mostrar amostra
      const sampleResult = await clientVerify.query('SELECT * FROM municipios ORDER BY codigo_municipio LIMIT 5');
      console.log('\n   📋 Amostra (primeiros 5):');
      sampleResult.rows.forEach(m => {
        console.log(`      ${String(m.codigo_municipio).padStart(7, ' ')} - ${m.nome_municipio}`);
      });
    } finally {
      clientVerify.release();
    }

    // Resumo final
    console.log('\n' + '='.repeat(70));
    console.log('✅ TESTE CONCLUÍDO COM SUCESSO!');
    console.log('='.repeat(70));
    console.log('\n📊 Estatísticas:');
    console.log(`   • Registros parseados: ${resultado.totalRecords}`);
    console.log(`   • Registros carregados: ${totalCarregados}`);
    console.log(`   • Erros de parsing: ${resultado.errors}`);
    console.log('\n💡 Próximo passo: Teste com arquivo de Estabelecimentos');
    console.log();

    process.exit(0);
  } catch (error) {
    console.error('\n' + '='.repeat(70));
    console.error('❌ ERRO NO TESTE');
    console.error('='.repeat(70));
    console.error(`\n${error.message}\n`);
    
    if (error.stack) {
      console.error('Stack trace:');
      console.error(error.stack);
    }
    
    console.error();
    process.exit(1);
  }
}

main();
