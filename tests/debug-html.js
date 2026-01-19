#!/usr/bin/env node

/**
 * Debug: Mostra estrutura HTML real da página
 */

import axios from 'axios';
import * as cheerio from 'cheerio';

async function debugHtml() {
  const url = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2026-01/';
  
  console.log('🔍 Buscando HTML de:', url);
  console.log();
  
  const response = await axios.get(url, {
    timeout: 30000,
    headers: {
      'User-Agent': 'Mozilla/5.0 (ETL Receita Federal)',
    },
  });

  const $ = cheerio.load(response.data);
  
  // Mostrar estrutura de tabelas
  console.log('='.repeat(80));
  console.log('TABELAS ENCONTRADAS:');
  console.log('='.repeat(80));
  
  $('table').each((i, table) => {
    console.log(`\nTabela ${i + 1}:`);
    
    $(table).find('tr').slice(0, 3).each((j, row) => {
      console.log(`  Linha ${j}:`);
      
      $(row).find('th').each((k, cell) => {
        console.log(`    <th>${$(cell).text().trim()}</th>`);
      });
      
      $(row).find('td').each((k, cell) => {
        const text = $(cell).text().trim();
        const link = $(cell).find('a').attr('href');
        console.log(`    <td>${text}${link ? ` [link: ${link}]` : ''}</td>`);
      });
    });
  });
  
  console.log('\n' + '='.repeat(80));
  console.log('LINKS .ZIP ENCONTRADOS:');
  console.log('='.repeat(80));
  
  $('a').each((i, element) => {
    const href = $(element).attr('href');
    if (href && href.endsWith('.zip')) {
      const text = $(element).text().trim();
      const parent = $(element).parent();
      const parentTag = parent.prop('tagName');
      
      console.log(`\n${href}`);
      console.log(`  Texto: "${text}"`);
      console.log(`  Parent: <${parentTag}>`);
      
      // Buscar siblings (células adjacentes)
      if (parentTag === 'TD') {
        const row = parent.parent();
        console.log(`  Células da linha:`);
        row.find('td').each((j, cell) => {
          console.log(`    [${j}] "${$(cell).text().trim()}"`);
        });
      }
      
      if (i >= 2) {
        console.log('\n  ... (mostrando apenas 3 primeiros)');
        return false;
      }
    }
  });
  
  console.log('\n' + '='.repeat(80));
  console.log('HTML COMPLETO (primeiras 50 linhas):');
  console.log('='.repeat(80));
  
  const lines = response.data.split('\n');
  lines.slice(0, 50).forEach((line, i) => {
    console.log(`${(i + 1).toString().padStart(3, ' ')}: ${line}`);
  });
  
  console.log('\n' + '='.repeat(80));
}

debugHtml().catch(console.error);
