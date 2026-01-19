#!/usr/bin/env node

import { discoverFiles } from '../src/services/discovery.js';
import { CONFIG } from '../src/config/constants.js';

/**
 * Teste do serviço de descoberta de arquivos
 * Verifica se está buscando apenas o mês/ano mais recente
 */

async function testDiscovery() {
  console.log('\n' + '='.repeat(70));
  console.log('🔍 TESTE DE DISCOVERY - Apenas Mês Mais Recente');
  console.log('='.repeat(70) + '\n');

  const baseUrl = CONFIG.BASE_URL || 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/';
  console.log(`📍 Base URL: ${baseUrl}\n`);

  try {
    console.log('⏳ Descobrindo arquivos (apenas mais recente)...\n');
    
    const startTime = Date.now();
    
    // Teste 1: Buscar apenas o mais recente (padrão)
    const files = await discoverFiles(baseUrl, {
      latestOnly: true,
    });
    
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
    
    console.log('\n' + '─'.repeat(70));
    console.log(`✅ Descoberta completa em ${elapsed}s`);
    console.log('─'.repeat(70) + '\n');

    // Resumo
    console.log(`📊 Total de arquivos encontrados: ${files.length}\n`);

    if (files.length === 0) {
      console.log('⚠️  Nenhum arquivo encontrado!\n');
      process.exit(1);
    }

    // Agrupar por tipo
    const porTipo = files.reduce((acc, file) => {
      acc[file.fileType] = (acc[file.fileType] || 0) + 1;
      return acc;
    }, {});

    console.log('📦 Arquivos por tipo:');
    Object.entries(porTipo).forEach(([tipo, count]) => {
      console.log(`   ${tipo.padEnd(18)}: ${count} arquivo(s)`);
    });
    console.log();

    // Verificar competências
    const competencias = [...new Set(
      files
        .filter(f => f.fileYear && f.fileMonth)
        .map(f => `${f.fileYear}-${String(f.fileMonth).padStart(2, '0')}`)
    )];

    if (competencias.length > 0) {
      console.log(`📅 Competência(s) encontrada(s): ${competencias.join(', ')}`);
      
      if (competencias.length > 1) {
        console.log('\n⚠️  ATENÇÃO: Múltiplas competências detectadas!');
        console.log('   O discovery deveria retornar apenas UMA competência.');
      } else {
        console.log('\n✅ Apenas uma competência (correto!)');
      }
    }
    console.log();

    // Listar primeiros arquivos de cada tipo
    console.log('📄 Primeiros arquivos por tipo:');
    console.log();
    
    ['municipios', 'estabelecimentos', 'socios'].forEach(tipo => {
      const filesOfType = files.filter(f => f.fileType === tipo);
      
      if (filesOfType.length > 0) {
        console.log(`   ${tipo.toUpperCase()}:`);
        filesOfType.slice(0, 3).forEach(f => {
          const competencia = f.fileYear && f.fileMonth 
            ? `${f.fileYear}-${String(f.fileMonth).padStart(2, '0')}`
            : 'N/A';
          console.log(`      • ${f.fileName.padEnd(30)} [${competencia}]`);
        });
        
        if (filesOfType.length > 3) {
          console.log(`      ... e mais ${filesOfType.length - 3} arquivo(s)`);
        }
        console.log();
      }
    });

    // Estimativa de dados
    console.log('💾 Estimativa de processamento:');
    const estabelecimentos = porTipo.estabelecimentos || 0;
    const socios = porTipo.socios || 0;
    const municipios = porTipo.municipios || 0;
    
    const tamanhoEstimado = (estabelecimentos * 200) + (socios * 50) + (municipios * 1);
    const tempoEstimado = files.length * 8; // ~8min por arquivo
    
    console.log(`   Espaço: ~${(tamanhoEstimado / 1024).toFixed(2)} GB`);
    console.log(`   Tempo:  ~${(tempoEstimado / 60).toFixed(1)} horas`);
    console.log();

    console.log('='.repeat(70));
    console.log('✅ Teste de Discovery concluído com sucesso!');
    console.log('='.repeat(70) + '\n');

    process.exit(0);
  } catch (error) {
    console.error('\n❌ Erro no teste de discovery:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

// Executar
testDiscovery();
