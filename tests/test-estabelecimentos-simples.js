#!/usr/bin/env node

import { downloadFile, getTempFilePath, cleanupFile } from '../src/services/downloader.js';
import { processZipFile } from '../src/services/processor.js';
import { transformEstabelecimento } from '../src/transformers/estabelecimento.js';
import { pool, testConnection } from '../src/config/database.js';
import logger from '../src/config/logger.js';
import fs from 'fs';

/**
 * Teste SIMPLES - Apenas 1 arquivo de Estabelecimentos
 * Para validar que o fluxo otimizado funciona com arquivo maior
 * 
 * Estabelecimentos tem 31 colunas (vs 11 de sócios)
 * Arquivos são maiores (~100-300MB compactados)
 */

/**
 * ⚠️ Função OTIMIZADA para evitar OOM:
 * - Batch INSERT com múltiplos VALUES (rápido)
 * - Transação por batch (commit imediato)
 * - Limpeza agressiva de arrays após uso
 * - Batch size controlado em 500 registros
 * - 31 colunas × 500 registros = 15.500 valores por batch (aceitável)
 */
async function loadEstabelecimentosData(client, records) {
  if (records.length === 0) return 0;
  
  // ⚠️ IMPORTANTE: Criar arrays locais que serão destruídos após o INSERT
  let placeholders = [];
  let values = [];
  
  try {
    // Iniciar transação
    await client.query('BEGIN');
    
    // Construir query com múltiplos VALUES
    let paramIndex = 1;
    
    for (const record of records) {
      // 31 colunas = $1 até $31 por registro
      const params = [];
      for (let i = 0; i < 31; i++) {
        params.push(`$${paramIndex + i}`);
      }
      placeholders.push(`(${params.join(', ')})`);
      
      // Adicionar valores na ordem correta (31 colunas)
      values.push(
        record.cnpj_basico,
        record.cnpj_ordem,
        record.cnpj_dv,
        record.cnpj,
        record.identificador_matriz_filial,
        record.nome_fantasia,
        record.situacao_cadastral,
        record.data_situacao_cadastral,
        record.motivo_situacao_cadastral,
        record.nome_cidade_exterior,
        record.codigo_pais,
        record.data_inicio_atividade,
        record.cnae_fiscal_principal,
        record.cnae_fiscal_secundaria,
        record.tipo_logradouro,
        record.logradouro,
        record.numero,
        record.complemento,
        record.bairro,
        record.cep,
        record.uf,
        record.codigo_municipio,
        record.ddd1,
        record.telefone1,
        record.ddd2,
        record.telefone2,
        record.ddd_fax,
        record.fax,
        record.correio_eletronico,
        record.situacao_especial,
        record.data_situacao_especial
      );
      paramIndex += 31;
    }
    
    // ⚠️ Construir query (será destruída após o INSERT)
    // ⚠️ ON CONFLICT permite retomar processamento após interrupção
    const query = `
      INSERT INTO estabelecimentos (
        cnpj_basico, cnpj_ordem, cnpj_dv, cnpj,
        identificador_matriz_filial, nome_fantasia, situacao_cadastral,
        data_situacao_cadastral, motivo_situacao_cadastral,
        nome_cidade_exterior, codigo_pais, data_inicio_atividade,
        cnae_fiscal_principal, cnae_fiscal_secundaria,
        tipo_logradouro, logradouro, numero, complemento, bairro,
        cep, uf, codigo_municipio,
        ddd1, telefone1, ddd2, telefone2, ddd_fax, fax,
        correio_eletronico, situacao_especial, data_situacao_especial
      ) VALUES ${placeholders.join(', ')}
      ON CONFLICT (cnpj) DO NOTHING
    `;
    
    // Executar INSERT
    const result = await client.query(query, values);
    
    // ⚠️ COMMIT IMEDIATO - libera locks e buffers do PostgreSQL
    await client.query('COMMIT');
    
    // ⚠️ LIMPEZA AGRESSIVA: Destruir arrays grandes IMEDIATAMENTE
    // Isso sinaliza ao GC que pode liberar memória
    placeholders.length = 0;
    placeholders = null;
    values.length = 0;
    values = null;
    
    return result.rowCount || 0;
  } catch (error) {
    // Rollback em caso de erro
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      // Ignorar erro de rollback se conexão foi perdida
    }
    
    // ⚠️ Limpar arrays mesmo em caso de erro
    if (placeholders) {
      placeholders.length = 0;
      placeholders = null;
    }
    if (values) {
      values.length = 0;
      values = null;
    }
    
    logger.error('test', `Erro ao inserir batch de estabelecimentos: ${error.message}`);
    return 0;
  }
}

// Variáveis globais para controle e progresso
let totalCarregados = 0;
let totalProcessado = 0;
let ultimoLog = Date.now();

async function main() {
  console.log('\n' + '='.repeat(70));
  console.log('🧪 TESTE SIMPLES - 1 Arquivo de Estabelecimentos');
  console.log('='.repeat(70) + '\n');

  // Arquivo mais recente (2025-12) - Estabelecimentos0.zip é o primeiro
  const arquivo = {
    nome: 'Estabelecimentos0.zip',
    url: 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-12/Estabelecimentos0.zip'
  };
  
  console.log(`📦 Arquivo: ${arquivo.nome}`);
  console.log(`🔗 URL: ${arquivo.url}`);
  console.log(`⚠️  Arquivo GRANDE (~100-300MB compactado, milhões de linhas)`);
  console.log(`⚠️  31 colunas por registro (vs 11 de sócios)`);
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
    console.log('2️⃣  Verificando tabela estabelecimentos...');
    const client = await pool.connect();
    try {
      await client.query('SELECT 1 FROM estabelecimentos LIMIT 1');
      console.log('   ✅ Tabela estabelecimentos existe\n');
    } catch (error) {
      throw new Error(`❌ Tabela estabelecimentos não existe. Execute: psql -d etl_receita_federal -f sql/schema.sql`);
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
    console.log('   (Isso pode levar 30-60 minutos para arquivo completo...)\n');
    totalCarregados = 0;
    totalProcessado = 0;
    ultimoLog = Date.now();
    
    // Criar UMA conexão para todo o processamento
    const loadClient = await pool.connect();
    
    // ⚠️ Declarar 'resultado' fora do try para usar depois
    let resultado;
    
    try {
      // ⚠️ BATCH_SIZE OTIMIZADO: 500 registros
      // - Com 31 colunas = 15.500 valores por batch
      // - Maior que sócios (11 colunas = 5.500 valores)
      // - Mas ainda gerenciável e eficiente
      const BATCH_SIZE = 500;
      
      resultado = await processZipFile(
        tempPath,
        transformEstabelecimento,
        async (records) => {
          const carregados = await loadEstabelecimentosData(loadClient, records);
          totalCarregados += carregados;
          totalProcessado += records.length;
          
          // ⚠️ Derreferenciar explicitamente após uso
          records.length = 0;
          
          // Log de progresso a cada 10 segundos
          const agora = Date.now();
          if (agora - ultimoLog > 10000) {
            console.log(`   📊 Progresso: ${totalProcessado.toLocaleString('pt-BR')} registros processados, ${totalCarregados.toLocaleString('pt-BR')} carregados`);
            ultimoLog = agora;
          }
        },
        BATCH_SIZE // ⚠️ Backpressure controlado + batch otimizado
      );
      
      console.log(`   📊 Final: ${totalProcessado.toLocaleString('pt-BR')} registros processados`);
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
      const countResult = await clientVerify.query('SELECT COUNT(*) FROM estabelecimentos');
      const total = parseInt(countResult.rows[0].count);
      console.log(`   📊 Total de estabelecimentos no banco: ${total.toLocaleString('pt-BR')}`);
      
      // Mostrar amostra
      const sampleResult = await clientVerify.query(`
        SELECT cnpj, nome_fantasia, uf, situacao_cadastral
        FROM estabelecimentos 
        ORDER BY cnpj 
        LIMIT 5
      `);
      console.log('\n   📋 Amostra (primeiros 5):');
      sampleResult.rows.forEach(e => {
        const nomeFantasia = e.nome_fantasia || '(sem nome fantasia)';
        const situacao = e.situacao_cadastral === 2 ? 'ATIVA' : 'BAIXADA';
        console.log(`      CNPJ: ${e.cnpj} | ${nomeFantasia} | ${e.uf} | ${situacao}`);
      });
      
      // Estatísticas adicionais
      const statsResult = await clientVerify.query(`
        SELECT 
          COUNT(*) as total,
          COUNT(DISTINCT cnpj_basico) as empresas_distintas,
          COUNT(CASE WHEN situacao_cadastral = 2 THEN 1 END) as ativos,
          COUNT(CASE WHEN identificador_matriz_filial = 1 THEN 1 END) as matrizes,
          COUNT(CASE WHEN identificador_matriz_filial = 2 THEN 1 END) as filiais
        FROM estabelecimentos
      `);
      console.log('\n   📈 Estatísticas:');
      const stats = statsResult.rows[0];
      console.log(`      • Total de estabelecimentos: ${parseInt(stats.total).toLocaleString('pt-BR')}`);
      console.log(`      • Empresas distintas: ${parseInt(stats.empresas_distintas).toLocaleString('pt-BR')}`);
      console.log(`      • Estabelecimentos ativos: ${parseInt(stats.ativos).toLocaleString('pt-BR')}`);
      console.log(`      • Matrizes: ${parseInt(stats.matrizes).toLocaleString('pt-BR')}`);
      console.log(`      • Filiais: ${parseInt(stats.filiais).toLocaleString('pt-BR')}`);
    } finally {
      clientVerify.release();
    }

    // Resumo final
    console.log('\n' + '='.repeat(70));
    console.log('✅ TESTE CONCLUÍDO COM SUCESSO!');
    console.log('='.repeat(70));
    console.log('\n📊 Estatísticas do processamento:');
    console.log(`   • Registros parseados: ${resultado.totalRecords.toLocaleString('pt-BR')}`);
    console.log(`   • Registros carregados: ${totalCarregados.toLocaleString('pt-BR')}`);
    console.log(`   • Erros de parsing: ${resultado.errors}`);
    console.log('\n💡 Sistema pronto para FULL LOAD completo!');
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
