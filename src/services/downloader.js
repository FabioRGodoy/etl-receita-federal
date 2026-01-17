import axios from 'axios';
import http from 'http';
import https from 'https';
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
 * Formata bytes para MB/GB
 */
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

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
 * Parseia Content-Range: "bytes start-end/total"
 */
function parseContentRange(contentRange) {
  if (!contentRange) return null;
  const match = /^bytes\s+(\d+)-(\d+)\/(\d+|\*)$/i.exec(contentRange.trim());
  if (!match) return null;
  return {
    start: Number(match[1]),
    end: Number(match[2]),
    total: match[3] === '*' ? null : Number(match[3]),
  };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function isRetryableDownloadError(error) {
  const msg = String(error?.message || '').toLowerCase();
  const code = String(error?.code || '').toUpperCase();
  return (
    msg.includes('aborted') ||
    msg.includes('socket hang up') ||
    msg.includes('inactivity timeout') ||
    code === 'ECONNRESET' ||
    code === 'ETIMEDOUT' ||
    code === 'EPIPE'
  );
}

/**
 * Baixa um arquivo via streaming HTTP com retry e resume (Range)
 */
export async function downloadFile(url, destPath) {
  ensureDirectory(destPath);

  const fileName = path.basename(destPath);
  const maxRetries = Number(CONFIG.DOWNLOAD_RETRIES ?? 8);
  const inactivityTimeoutMs = Number(CONFIG.DOWNLOAD_INACTIVITY_TIMEOUT ?? 120000);

  const httpAgent = new http.Agent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    timeout: 0,
  });
  const httpsAgent = new https.Agent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    timeout: 0,
  });

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const existingBytes = fs.existsSync(destPath) ? fs.statSync(destPath).size : 0;
    const headers = {
      'User-Agent': 'Mozilla/5.0 (ETL Receita Federal)',
      'Accept-Encoding': 'identity',
    };
    if (existingBytes > 0) {
      headers.Range = `bytes=${existingBytes}-`;
    }

    const startTime = Date.now();
    let downloadedThisAttempt = 0;
    let totalBytes = 0;
    let lastLogTime = Date.now();
    let lastLogBytes = 0;

    try {
      logger.info(
        'downloader',
        `Iniciando download${existingBytes > 0 ? ' (resume)' : ''}: ${url} ` +
          `| tentativa ${attempt}/${maxRetries}`
      );

      const response = await axios({
        method: 'GET',
        url,
        responseType: 'stream',
        timeout: 0,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        headers,
        httpAgent,
        httpsAgent,
        validateStatus: (status) => status === 200 || status === 206,
      });

      // Se pedimos Range mas o servidor respondeu 200, ele ignorou Range.
      // Nesse caso, recomeçar do zero (evita arquivo corrompido por append).
      if (existingBytes > 0 && response.status === 200) {
        logger.warn(
          'downloader',
          `Servidor ignorou Range para ${fileName}. Recomeçando do zero.`
        );
        fs.unlinkSync(destPath);
        // tenta novamente já no próximo loop
        continue;
      }

      const contentRange = response.headers['content-range'];
      const parsedRange = parseContentRange(contentRange);

      if (response.status === 206 && parsedRange?.total) {
        totalBytes = parsedRange.total;
      } else {
        const len = Number.parseInt(response.headers['content-length'] || '0', 10);
        totalBytes = existingBytes > 0 ? existingBytes + (Number.isFinite(len) ? len : 0) : len;
      }

      if (totalBytes > 0) {
        logger.info('downloader', `Tamanho do arquivo: ${formatBytes(totalBytes)} (${totalBytes} bytes)`);
      } else {
        logger.info('downloader', `Tamanho do arquivo: desconhecido (sem Content-Length)`);
      }

      const writer = fs.createWriteStream(destPath, { flags: existingBytes > 0 ? 'a' : 'w' });

      // Detectar timeout de inatividade (sem dados recebidos)
      let lastDataTime = Date.now();
      const inactivityCheck = setInterval(() => {
        const now = Date.now();
        if (now - lastDataTime > inactivityTimeoutMs) {
          clearInterval(inactivityCheck);
          response.data.destroy(new Error('Inactivity timeout'));
        }
      }, 10000);

      response.data.on('data', (chunk) => {
        lastDataTime = Date.now();
        downloadedThisAttempt += chunk.length;

        const downloadedTotal = existingBytes + downloadedThisAttempt;

        // Log a cada 30 segundos ou 100 MB
        const now = Date.now();
        const bytesSinceLastLog = downloadedTotal - lastLogBytes;
        if (now - lastLogTime >= 30000 || bytesSinceLastLog >= 100 * 1024 * 1024) {
          const elapsed = (now - startTime) / 1000;
          const speed = downloadedThisAttempt / Math.max(elapsed, 1);
          const progress = totalBytes > 0 ? ((downloadedTotal / totalBytes) * 100).toFixed(1) : '?';
          const eta = totalBytes > 0 && speed > 0 ? Math.round((totalBytes - downloadedTotal) / speed) : 0;

          logger.info(
            'downloader',
            `📥 ${fileName}: ${progress}% (${formatBytes(downloadedTotal)}/${formatBytes(totalBytes)}) ` +
              `| ${formatBytes(speed)}/s | ETA: ${Math.floor(eta / 60)}min ${eta % 60}s`
          );

          lastLogTime = now;
          lastLogBytes = downloadedTotal;
        }
      });

      await pipeline(response.data, writer);
      clearInterval(inactivityCheck);

      const finalSize = fs.statSync(destPath).size;
      const durationSec = Math.round((Date.now() - startTime) / 1000);
      const avgSpeed = downloadedThisAttempt / Math.max(durationSec, 1);

      logger.info(
        'downloader',
        `✅ Download concluído: ${fileName} | ${formatBytes(finalSize)} ` +
          `| tentativa ${attempt}/${maxRetries} | média: ${formatBytes(avgSpeed)}/s`
      );

      return {
        success: true,
        filePath: destPath,
        sizeBytes: finalSize,
      };
    } catch (error) {
      const retryable = isRetryableDownloadError(error);
      const currentSize = fs.existsSync(destPath) ? fs.statSync(destPath).size : 0;
      logger.error(
        'downloader',
        `Erro no download: ${url} | ${error.message} | ` +
          `parcial: ${formatBytes(currentSize)} | tentativa ${attempt}/${maxRetries}`
      );

      if (!retryable || attempt === maxRetries) {
        // Se falhou de forma não recuperável (ou esgotou tentativas), remover arquivo parcial para evitar lixo.
        if (fs.existsSync(destPath)) {
          fs.unlinkSync(destPath);
          logger.warn('downloader', `Arquivo parcial removido: ${destPath}`);
        }
        throw new Error(`Falha no download de ${fileName}: ${error.message}`);
      }

      // Backoff exponencial com teto
      const baseDelay = Number(CONFIG.DOWNLOAD_RETRY_DELAY_MS ?? 5000);
      const delay = Math.min(baseDelay * Math.pow(2, attempt - 1), 120000);
      logger.warn('downloader', `Retry em ${Math.round(delay / 1000)}s (mantendo parcial para resume): ${fileName}`);
      await sleep(delay);
    }
  }

  throw new Error(`Falha no download de ${path.basename(destPath)}: excedeu tentativas`);
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
