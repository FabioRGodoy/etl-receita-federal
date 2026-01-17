import fs from 'fs';
import path from 'path';
import { CONFIG } from './src/config/constants.js';

/**
 * Limpa arquivos temporários corrompidos ou incompletos
 */

const TEMP_DIR = process.env.TEMP_DIR || CONFIG.TEMP_DIR || './temp';

async function limparTemporarios() {
  console.log('🧹 Limpando arquivos temporários...\n');
  
  if (!fs.existsSync(TEMP_DIR)) {
    console.log(`⚠️  Diretório ${TEMP_DIR} não existe`);
    return;
  }
  
  const arquivos = fs.readdirSync(TEMP_DIR).filter(f => f.endsWith('.zip'));
  
  if (arquivos.length === 0) {
    console.log('✅ Nenhum arquivo temporário para limpar');
    return;
  }
  
  console.log(`📦 Encontrados ${arquivos.length} arquivos ZIP:\n`);
  
  let totalRemovido = 0;
  let tamanhoLiberado = 0;
  
  for (const arquivo of arquivos) {
    const caminhoCompleto = path.join(TEMP_DIR, arquivo);
    const stats = fs.statSync(caminhoCompleto);
    const tamanhoMB = (stats.size / 1024 / 1024).toFixed(2);
    
    try {
      fs.unlinkSync(caminhoCompleto);
      console.log(`   ✅ ${arquivo} (${tamanhoMB} MB) - REMOVIDO`);
      totalRemovido++;
      tamanhoLiberado += stats.size;
    } catch (error) {
      console.log(`   ❌ ${arquivo} - ERRO: ${error.message}`);
    }
  }
  
  const totalLiberadoMB = (tamanhoLiberado / 1024 / 1024).toFixed(2);
  const totalLiberadoGB = (tamanhoLiberado / 1024 / 1024 / 1024).toFixed(2);
  
  console.log(`\n📊 Resumo:`);
  console.log(`   Arquivos removidos: ${totalRemovido}`);
  console.log(`   Espaço liberado: ${totalLiberadoMB} MB (${totalLiberadoGB} GB)`);
  console.log(`\n🎉 Limpeza concluída!`);
}

limparTemporarios();
