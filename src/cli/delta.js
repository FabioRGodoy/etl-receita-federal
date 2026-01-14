#!/usr/bin/env node

import { runETL } from '../orchestrator.js';
import { discoverFiles } from '../services/discovery.js';
import { getProcessedFiles } from '../services/control.js';
import { CONFIG } from '../config/constants.js';
import logger from '../config/logger.js';

/**
 * CLI para executar DELTA (carga incremental)
 * Processa apenas arquivos novos (ano/mês não processados)
 */

/**
 * Filtra arquivos novos com base em ano/mês
 */
function filterNewFiles(allFiles, processedFiles) {
  const processedSet = new Set();
  
  processedFiles.forEach(pf => {
    const key = `${pf.file_type}_${pf.file_year}_${pf.file_month}`;
    processedSet.add(key);
  });

  return allFiles.filter(file => {
    const key = `${file.fileType}_${file.fileYear}_${file.fileMonth}`;
    return !processedSet.has(key);
  });
}

async function main() {
  console.log('='.repeat(80));
  console.log('ETL Receita Federal - DELTA (Carga Incremental)');
  console.log('='.repeat(80));
  console.log();
  
  const baseUrl = process.argv[2] || CONFIG.BASE_URL;
  
  console.log(`URL Base: ${baseUrl}`);
  console.log(`Tipo: DELTA (apenas arquivos novos, usa UPSERT)`);
  console.log();
  console.log('🔍 Buscando arquivos novos...');
  console.log();

  const startTime = Date.now();

  try {
    // 1. Descobrir todos os arquivos disponíveis
    const allFiles = await discoverFiles(baseUrl);
    console.log(`📦 Total de arquivos disponíveis: ${allFiles.length}`);

    // 2. Buscar arquivos já processados
    const processedFiles = await getProcessedFiles();
    console.log(`✅ Arquivos já processados: ${processedFiles.length}`);

    // 3. Filtrar apenas novos
    const newFiles = filterNewFiles(allFiles, processedFiles);
    console.log(`🆕 Arquivos novos a processar: ${newFiles.length}`);
    console.log();

    if (newFiles.length === 0) {
      console.log('✅ Nenhum arquivo novo encontrado!');
      console.log('   Todos os arquivos disponíveis já foram processados.');
      console.log();
      process.exit(0);
    }

    // Mostrar resumo
    console.log('📋 Arquivos novos por tipo:');
    const byType = {};
    newFiles.forEach(f => {
      byType[f.fileType] = (byType[f.fileType] || 0) + 1;
    });
    Object.entries(byType).forEach(([type, count]) => {
      console.log(`   - ${type}: ${count} arquivos`);
    });
    console.log();

    // Confirmação
    if (!process.argv.includes('--yes')) {
      console.log('❓ Para confirmar, execute com --yes:');
      console.log(`   node src/cli/delta.js ${baseUrl !== CONFIG.BASE_URL ? baseUrl + ' ' : ''}--yes`);
      console.log();
      process.exit(0);
    }

    console.log('🚀 Iniciando DELTA...');
    console.log();

    // 4. Executar ETL apenas com arquivos novos
    const result = await runETL({
      baseUrl,
      loadType: CONFIG.LOAD_TYPE.DELTA,
      filesToProcess: newFiles,
    });

    const endTime = Date.now();
    const durationMin = Math.round((endTime - startTime) / 1000 / 60);

    console.log();
    console.log('='.repeat(80));
    console.log('✅ DELTA CONCLUÍDO COM SUCESSO!');
    console.log('='.repeat(80));
    console.log();
    console.log(`📊 Estatísticas:`);
    console.log(`   - Run ID: ${result.runId}`);
    console.log(`   - Arquivos novos: ${result.total}`);
    console.log(`   - Arquivos processados: ${result.completed}`);
    console.log(`   - Arquivos com erro: ${result.failed}`);
    console.log(`   - Duração: ${durationMin} minutos`);
    console.log();

    if (result.failed > 0) {
      console.log('⚠️  Alguns arquivos falharam. Verifique os logs para detalhes.');
      console.log('   Você pode executar o DELTA novamente para reprocessar apenas os arquivos com erro.');
      console.log();
    }

    process.exit(result.failed > 0 ? 1 : 0);
  } catch (error) {
    const endTime = Date.now();
    const durationMin = Math.round((endTime - startTime) / 1000 / 60);

    console.error();
    console.error('='.repeat(80));
    console.error('❌ ERRO NO DELTA');
    console.error('='.repeat(80));
    console.error();
    console.error(`Erro: ${error.message}`);
    console.error();
    console.error(`Duração até falha: ${durationMin} minutos`);
    console.error();
    console.error('Stack trace:');
    console.error(error.stack);
    console.error();

    process.exit(1);
  }
}

main();
