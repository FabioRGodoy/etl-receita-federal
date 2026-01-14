import axios from 'axios';
import fs from 'fs';
import path from 'path';
import { pipeline } from 'stream/promises';
import logger from '../config/logger.js';
import { CONFIG } from '../config/constants.js';

/**
 * File Downloader
 * Baixa arquivos ZIP da Receita Federal via streaming
 */

/**
 * Garante que o diretório de destino existe
 */
function ensureDirectory(filePath) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/**
 * Baixa um arquivo via streaming HTTP
 */
export async function downloadFile(url, destPath) {
  logger.info('downloader', `Iniciando download: ${url}`);
  
  ensureDirectory(destPath);

  try {
    const response = await axios({
      method: 'get',
      url: url,
      responseType: 'stream',
      timeout: CONFIG.DOWNLOAD_TIMEOUT,
      headers: {
        'User-Agent': 'Mozilla/5.0 (ETL Receita Federal)',
      },
    });

    // Stream direto para disco
    await pipeline(
      response.data,
      fs.createWriteStream(destPath)
    );

    const stats = fs.statSync(destPath);
    const sizeMB = (stats.size / 1024 / 1024).toFixed(2);
    
    logger.info('downloader', `Download concluído: ${destPath} (${sizeMB} MB)`);
    
    return {
      success: true,
      filePath: destPath,
      sizeBytes: stats.size,
    };
  } catch (error) {
    logger.error('downloader', `Erro no download: ${url}`, error.message);
    
    // Limpar arquivo parcial
    if (fs.existsSync(destPath)) {
      fs.unlinkSync(destPath);
    }
    
    throw error;
  }
}

/**
 * Gera caminho de destino para arquivo temporário
 */
export function getTempFilePath(fileName) {
  return path.join(CONFIG.TEMP_DIR, fileName);
}

/**
 * Remove arquivo temporário
 */
export function cleanupFile(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      logger.info('downloader', `Arquivo removido: ${filePath}`);
    }
  } catch (error) {
    logger.warn('downloader', `Erro ao remover arquivo: ${filePath}`, error.message);
  }
}

export default { downloadFile, getTempFilePath, cleanupFile };
