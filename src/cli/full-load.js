#!/usr/bin/env node

import { runETL } from '../orchestrator.js';
import { CONFIG } from '../config/constants.js';
import logger from '../config/logger.js';

/**
 * CLI para executar FULL LOAD
 * Processa todos os arquivos disponíveis e carrega no banco
 */

async function main() {
  console.log('='.repeat(80));
  console.log('ETL Receita Federal - FULL LOAD');
  console.log('='.repeat(80));
  console.log();
  console.log('⚠️  ATENÇÃO: Este processo pode demorar algumas horas!');
  console.log('⚠️  Recomendado rodar overnight ou em sessão screen/tmux');
  console.log();
  
  // Filtrar apenas argumentos que não são flags (não começam com --)
  const args = process.argv.slice(2).filter(arg => !arg.startsWith('--'));
  const baseUrl = args[0] || CONFIG.BASE_URL;
  
  console.log(`URL Base: ${baseUrl}`);
  console.log(`Tipo: FULL LOAD (trunca tabelas e recarrega tudo)`);
  console.log(`Modo: Apenas mês/ano mais recente`);
  console.log();
  
  // Confirmação
  if (!process.argv.includes('--yes')) {
    console.log('❓ Para confirmar, execute com --yes:');
    console.log(`   node src/cli/full-load.js ${baseUrl !== CONFIG.BASE_URL ? baseUrl + ' ' : ''}--yes`);
    console.log();
    process.exit(0);
  }

  console.log('🚀 Iniciando FULL LOAD...');
  console.log();

  const startTime = Date.now();

  try {
    const result = await runETL({
      baseUrl,
      loadType: CONFIG.LOAD_TYPE.FULL,
      latestOnly: true, // Apenas mês/ano mais recente
    });

    const endTime = Date.now();
    const durationMin = Math.round((endTime - startTime) / 1000 / 60);
    const durationHours = (durationMin / 60).toFixed(1);

    console.log();
    console.log('='.repeat(80));
    console.log('✅ FULL LOAD CONCLUÍDO COM SUCESSO!');
    console.log('='.repeat(80));
    console.log();
    console.log(`📊 Estatísticas:`);
    console.log(`   - Run ID: ${result.runId}`);
    console.log(`   - Total de arquivos: ${result.total}`);
    console.log(`   - Arquivos processados: ${result.completed}`);
    console.log(`   - Arquivos com erro: ${result.failed}`);
    console.log(`   - Duração: ${durationMin} minutos (${durationHours}h)`);
    console.log();
    
    if (result.failed > 0) {
      console.log('⚠️  Alguns arquivos falharam. Verifique os logs para detalhes.');
      console.log('   Você pode executar o FULL LOAD novamente para reprocessar apenas os arquivos com erro.');
      console.log();
    }

    console.log('💡 Próximos passos:');
    console.log('   1. Verificar dados no banco');
    console.log('   2. Configurar carga DELTA diária');
    console.log('   3. Criar índices adicionais se necessário');
    console.log();

    process.exit(result.failed > 0 ? 1 : 0);
  } catch (error) {
    const endTime = Date.now();
    const durationMin = Math.round((endTime - startTime) / 1000 / 60);

    console.error();
    console.error('='.repeat(80));
    console.error('❌ ERRO NO FULL LOAD');
    console.error('='.repeat(80));
    console.error();
    console.error(`Erro: ${error.message}`);
    console.error();
    console.error(`Duração até falha: ${durationMin} minutos`);
    console.error();
    console.error('💡 O ETL pode ser retomado executando o comando novamente.');
    console.error('   Arquivos já processados serão ignorados.');
    console.error();
    console.error('Stack trace completo:');
    console.error(error.stack);
    console.error();

    process.exit(1);
  }
}

main();
