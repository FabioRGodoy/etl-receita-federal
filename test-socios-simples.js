#!/usr/bin/env node

import { downloadFile, getTempFilePath, cleanupFile } from './src/services/downloader.js';
import { processZipFile } from './src/services/processor.js';
import { transformSocio } from './src/transformers/socio.js';
import { pool, testConnection } from './src/config/database.js';
import logger from './src/config/logger.js';
import fs from 'fs';

/**
 * Teste SIMPLES - Apenas 1 arquivo de Sócios
 * Para validar que o fluxo básico funciona
 */

// Função modificada para receber conexão como parâmetro
async function loadSociosData(client, records) {
  const columns = [
    'cnpj_basico',
    'identificador_socio',
    'nome_socio',
    'cpf_cnpj_socio',
    'qualificacao_socio',
    'data_entrada_sociedade',
    'codigo_pais',
    'cpf_representante_legal',
    'nome_representante_legal',
    'qualificacao_representante_legal',
    'faixa_etaria',
  ];
  
  try {
    let inserted = 0;
    const batchSize = 1000;
    
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      
      const placeholders = [];
      const values = [];
      let paramIndex = 1;

      batch.forEach(record => {
        const recordPlaceholders = [];
        columns.forEach(col => {
          recordPlaceholders.push(`$${paramIndex++}`);
          values.push(record[col]);
        });
        placeholders.push(`(${recordPlaceholders.join(', ')})`);
      });

      const query = `
        INSERT INTO socios (${columns.join(', ')}) 
        VALUES ${placeholders.join(', ')}
        ON CONFLICT DO NOTHING
      `;

      const result = await client.query(query, values);
      inserted += result.rowCount;
    }
    
    return inserted;
  } catch (error) {
    logger.error('test', `Erro ao carregar sócios: ${error.message}`);
    throw error;
  }
}

// Variável global para controle
let totalCarregados = 0;

async function main() {
  console.log('\n' + '='.repeat(70));
  console.log('🧪 TESTE SIMPLES - 1 Arquivo de Sócios');
  console.log('='.repeat(70) + '\n');

  // Arquivo mais recente (2025-12) - Socios0.zip é o primeiro
  const arquivo = {
    nome: 'Socios0.zip',
    url: 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-12/Socios0.zip'
  };
  
  console.log(`📦 Arquivo: ${arquivo.nome}`);
  console.log(`🔗 URL: ${arquivo.url}`);
  console.log(`⚠️  Arquivo maior que Municípios (~47MB compactado)`);
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
    console.log('2️⃣  Verificando tabela socios...');
    const client = await pool.connect();
    try {
      await client.query('SELECT 1 FROM socios LIMIT 1');
      console.log('   ✅ Tabela socios existe\n');
    } catch (error) {
      throw new Error(`❌ Tabela socios não existe. Execute: psql -d etl_receita_federal -f sql/schema.sql`);
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
    console.log('   (Isso pode levar alguns minutos...)\n');
    totalCarregados = 0;
    
    // Criar UMA conexão para todo o processamento
    const loadClient = await pool.connect();
    
    try {
      const resultado = await processZipFile(
        tempPath,
        transformSocio,
        async (records) => {
          const carregados = await loadSociosData(loadClient, records);
          totalCarregados += carregados;
        }
      );
      
      console.log('\n   ✅ Processamento concluído\n');
    } finally {
      loadClient.release();
    }

    // 5. Limpar arquivo temporário
    console.log('5️⃣  Limpando arquivo temporário...');
    cleanupFile(tempPath);
    console.log('   ✅ Arquivo removido\n');

    // 6. Verificar dados no banco
    console.log('6️⃣  Verificando dados carregados...');
    const clientVerify = await pool.connect();
    try {
      const countResult = await clientVerify.query('SELECT COUNT(*) FROM socios');
      const total = parseInt(countResult.rows[0].count);
      console.log(`   📊 Total de sócios no banco: ${total}`);
      
      // Mostrar amostra
      const sampleResult = await clientVerify.query(`
        SELECT cnpj_basico, nome_socio, cpf_cnpj_socio 
        FROM socios 
        ORDER BY id 
        LIMIT 5
      `);
      console.log('\n   📋 Amostra (primeiros 5):');
      sampleResult.rows.forEach(s => {
        const cpfCnpj = s.cpf_cnpj_socio || 'N/A';
        console.log(`      CNPJ: ${s.cnpj_basico} | Sócio: ${s.nome_socio} | CPF/CNPJ: ${cpfCnpj}`);
      });
      
      // Estatísticas adicionais
      const statsResult = await clientVerify.query(`
        SELECT 
          COUNT(*) as total,
          COUNT(DISTINCT cnpj_basico) as empresas_distintas,
          COUNT(CASE WHEN cpf_cnpj_socio IS NOT NULL THEN 1 END) as com_cpf_cnpj
        FROM socios
      `);
      console.log('\n   📈 Estatísticas:');
      const stats = statsResult.rows[0];
      console.log(`      • Total de sócios: ${stats.total}`);
      console.log(`      • Empresas distintas: ${stats.empresas_distintas}`);
      console.log(`      • Sócios com CPF/CNPJ: ${stats.com_cpf_cnpj}`);
    } finally {
      clientVerify.release();
    }

    // Resumo final
    console.log('\n' + '='.repeat(70));
    console.log('✅ TESTE CONCLUÍDO COM SUCESSO!');
    console.log('='.repeat(70));
    console.log('\n📊 Estatísticas do processamento:');
    console.log(`   • Registros parseados: ${resultado.totalRecords}`);
    console.log(`   • Registros carregados: ${totalCarregados}`);
    console.log(`   • Erros de parsing: ${resultado.errors}`);
    console.log('\n💡 Próximo passo: Teste com arquivo de Estabelecimentos (maior)');
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
