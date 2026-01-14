#!/usr/bin/env node

import { discoverFiles } from '../services/discovery.js';
import { CONFIG } from '../config/constants.js';

/**
 * CLI para testar o Discovery Service
 */

async function main() {
  console.log('='.repeat(60));
  console.log('ETL Receita Federal - Teste de Discovery');
  console.log('='.repeat(60));
  console.log();
  
  const baseUrl = process.argv[2] || CONFIG.BASE_URL;
  
  console.log(`URL Base: ${baseUrl}`);
  console.log();
  console.log('Buscando arquivos disponíveis...');
  console.log();

  try {
    const files = await discoverFiles(baseUrl);
    
    console.log('='.repeat(60));
    console.log(`TOTAL: ${files.length} arquivos encontrados`);
    console.log('='.repeat(60));
    console.log();

    // Agrupar por tipo
    const byType = {};
    files.forEach(file => {
      if (!byType[file.fileType]) {
        byType[file.fileType] = [];
      }
      byType[file.fileType].push(file);
    });

    // Exibir resumo
    for (const [type, typeFiles] of Object.entries(byType)) {
      console.log(`\n📦 ${type.toUpperCase()}: ${typeFiles.length} arquivos`);
      console.log('-'.repeat(60));
      
      // Mostrar primeiros 5 de cada tipo
      const sample = typeFiles.slice(0, 5);
      sample.forEach(file => {
        const yearMonth = file.fileYear 
          ? `[${file.fileYear}${file.fileMonth ? '-' + String(file.fileMonth).padStart(2, '0') : ''}]`
          : '[sem data]';
        console.log(`  ${yearMonth} ${file.fileName}`);
      });
      
      if (typeFiles.length > 5) {
        console.log(`  ... e mais ${typeFiles.length - 5} arquivos`);
      }
    }

    console.log();
    console.log('='.repeat(60));
    console.log('✅ Discovery concluído com sucesso!');
    console.log('='.repeat(60));
    
  } catch (error) {
    console.error();
    console.error('❌ Erro na descoberta de arquivos:');
    console.error(error.message);
    console.error();
    process.exit(1);
  }
}

main();
