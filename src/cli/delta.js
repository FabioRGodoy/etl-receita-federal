#!/usr/bin/env node

import { runETL } from '../orchestrator.js';
import { discoverFiles } from '../services/discovery.js';
import { getFilesToReprocess } from '../services/control.js';
import { CONFIG } from '../config/constants.js';
import logger from '../config/logger.js';

/**
 * CLI para executar DELTA (carga incremental)
 * Processa arquivos que foram atualizados no site (compara last_modified_date)
 */

async function main() {
  console.log('='.repeat(80));
  console.log('ETL Receita Federal - DELTA (Carga Incremental)');
  console.log('='.repeat(80));
  console.log();
  
  // Filtrar apenas argumentos que não são flags (não começam com --)
  const args = process.argv.slice(2).filter(arg => !arg.startsWith('--'));
  const baseUrl = args[0] || CONFIG.BASE_URL;
  
  console.log(`URL Base: ${baseUrl}`);
  console.log(`Tipo: DELTA (detecta arquivos atualizados por data de modificação)`);
  console.log();
  console.log('🔍 Buscando arquivos...');
  console.log();

  const startTime = Date.now();

  try {
    // 1. Descobrir arquivos do mês/ano mais recente (com datas de modificação)
    const allFiles = await discoverFiles(baseUrl, { latestOnly: true });
    console.log(`📦 Total de arquivos disponíveis (mês mais recente): ${allFiles.length}`);

    // 2. Identificar quais precisam ser reprocessados (compara datas)
    const filesToProcess = await getFilesToReprocess(allFiles);
    console.log(`🔄 Arquivos a reprocessar: ${filesToProcess.length}`);
    console.log();

    if (filesToProcess.length === 0) {
      console.log('✅ Nenhum arquivo atualizado encontrado!');
      console.log('   Todos os arquivos estão sincronizados com o site.');
      console.log();
      process.exit(0);
    }

    // Mostrar detalhes dos arquivos
    console.log('📋 Arquivos a processar:');
    filesToProcess.forEach(f => {
      const dateStr = f.lastModified 
        ? new Date(f.lastModified).toLocaleString('pt-BR')
        : 'sem data';
      console.log(`   • ${f.fileName} - ${f.reprocessReason}`);
      console.log(`     Data no site: ${dateStr}`);
    });
    console.log();

    // Resumo por tipo
    const byType = {};
    filesToProcess.forEach(f => {
      byType[f.fileType] = (byType[f.fileType] || 0) + 1;
    });
    console.log('📊 Resumo por tipo:');
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

    // 3. Executar ETL apenas com arquivos atualizados
    const result = await runETL({
      baseUrl,
      loadType: CONFIG.LOAD_TYPE.DELTA,
      filesToProcess: filesToProcess,
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
