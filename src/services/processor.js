import fs from 'fs';
import yauzl from 'yauzl';
import csv from 'csv-parser';
import { Readable } from 'stream';
import logger from '../config/logger.js';
import { updateFileCheckpoint, getFileCheckpoint } from './control.js';

/**
 * Stream Processor
 * Descompacta ZIP e parseia CSV linha a linha via streaming
 * com suporte a checkpoint e resume
 */

/**
 * Processa arquivo ZIP extraindo CSVs e parseando linha a linha
 * @param {string} zipPath - Caminho do arquivo ZIP
 * @param {function} transformer - Função que transforma cada linha
 * @param {function} onData - Callback async para processar batches
 * @param {number} batchSize - Tamanho do batch (default: 500)
 * @param {number} fileId - ID do arquivo no controle (para checkpoint)
 */
export async function processZipFile(zipPath, transformer, onData, batchSize = 500, fileId = null) {
  // Recuperar checkpoint se houver
  let checkpoint = null;
  let skipLines = 0;
  
  if (fileId) {
    checkpoint = await getFileCheckpoint(fileId);
    if (checkpoint?.linesProcessed) {
      skipLines = checkpoint.linesProcessed;
      logger.info('processor', `📋 Retomando do checkpoint: linha ${skipLines.toLocaleString()}`);
    }
  }
  
  return new Promise((resolve, reject) => {
    logger.info('processor', `Processando arquivo: ${zipPath} (batch size: ${batchSize})`);
    
    let totalRecords = skipLines; // Começar da linha salva
    let currentLine = 0;
    let errors = 0;
    let csvFileName = null;
    let lastCheckpointTime = Date.now();
    const CHECKPOINT_INTERVAL = 30000; // Salvar checkpoint a cada 30s

    yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
      if (err) {
        logger.error('processor', 'Erro ao abrir ZIP', err.message);
        return reject(err);
      }

      zipfile.readEntry();

      zipfile.on('entry', (entry) => {
        // Ignorar diretórios
        if (/\/$/.test(entry.fileName)) {
          zipfile.readEntry();
          return;
        }

        // Processar apenas arquivos CSV (aceita .csv ou padrões da Receita Federal)
        const fileName = entry.fileName.toLowerCase();
        const isCSV = 
          fileName.endsWith('.csv') || 
          fileName.includes('csv') ||
          fileName.endsWith('.estabele') ||  // Estabelecimentos: K3241.K03200Y0.D51213.ESTABELE
          fileName.endsWith('.sociocsv') ||  // Sócios: K3241.K03200Y0.D51213.SOCIOCSV
          fileName.endsWith('.municcsv') ||  // Municípios: K3241.K03200Y0.D51213.MUNICCSV
          fileName.includes('estabele') ||   // Qualquer variação de estabelecimentos
          fileName.includes('socio') ||      // Qualquer variação de sócios
          fileName.includes('munic');         // Qualquer variação de municípios
        
        if (!isCSV) {
          logger.warn('processor', `Ignorando arquivo não-CSV: ${entry.fileName}`);
          zipfile.readEntry();
          return;
        }

        csvFileName = entry.fileName;
        logger.info('processor', `Extraindo e processando: ${entry.fileName}`);

        zipfile.openReadStream(entry, (err, readStream) => {
          if (err) {
            logger.error('processor', 'Erro ao abrir stream do CSV', err.message);
            zipfile.readEntry();
            return;
          }

          let records = [];
          let isProcessing = false; // Flag para controlar backpressure
          
          const csvStream = readStream.pipe(csv({
            separator: ';',
            headers: false,
            skipLines: 0,
            quote: '"',
          }));
          
          csvStream.on('data', (row) => {
            try {
              currentLine++;
              
              // Pular linhas já processadas (resume)
              if (currentLine <= skipLines) {
                return;
              }
              
              // Transformar dados
              const transformed = transformer(row);
              if (transformed) {
                records.push(transformed);
                totalRecords++;
                
                // ⚠️ CORREÇÃO CRÍTICA: PAUSAR stream durante processamento do batch
                // Isso evita acúmulo de dados em memória (backpressure)
                if (records.length >= batchSize && !isProcessing) {
                  isProcessing = true;
                  
                  // Pausar o stream ANTES de processar
                  csvStream.pause();
                  
                  // Fazer cópia do array e limpar imediatamente
                  const batchToProcess = records;
                  records = []; // ⚠️ Limpar ANTES do await para liberar referência
                  
                  // Processar batch de forma assíncrona
                  onData(batchToProcess)
                    .then(async () => {
                      // Salvar checkpoint periodicamente
                      const now = Date.now();
                      if (fileId && (now - lastCheckpointTime) >= CHECKPOINT_INTERVAL) {
                        await updateFileCheckpoint(fileId, {
                          linesProcessed: totalRecords,
                          csvFileName: csvFileName,
                          lastCheckpoint: new Date().toISOString()
                        });
                        lastCheckpointTime = now;
                        logger.info('processor', `💾 Checkpoint salvo: ${totalRecords.toLocaleString()} linhas`);
                      }
                      
                      // ⚠️ Derreferenciar explicitamente o batch processado
                      batchToProcess.length = 0;
                      isProcessing = false;
                      
                      // Retomar o stream após processar com sucesso
                      csvStream.resume();
                    })
                    .catch((error) => {
                      // ⚠️ CRÍTICO: Em caso de erro, DESTRUIR o stream
                      // Não faz sentido continuar processando se INSERT falhou
                      logger.error('processor', 'Erro FATAL ao processar batch - abortando stream', error.message);
                      errors++;
                      batchToProcess.length = 0;
                      isProcessing = false;
                      
                      // Destruir o stream com erro - isso propaga para .on('error')
                      csvStream.destroy(error);
                    });
                }
              }
            } catch (error) {
              errors++;
              logger.warn('processor', `Erro ao transformar linha ${totalRecords}`, error.message);
            }
          });
          
          csvStream.on('end', async () => {
            logger.info('processor', `CSV processado: ${entry.fileName} (${totalRecords} registros)`);
            
            // Aguardar processamento em andamento terminar
            while (isProcessing) {
              await new Promise(resolve => setTimeout(resolve, 100));
            }
            
            // Processar registros restantes
            if (records.length > 0) {
              try {
                await onData(records);
                records.length = 0; // ⚠️ Limpar explicitamente
                records = null; // ⚠️ Derreferenciar
              } catch (error) {
                // ⚠️ CRÍTICO: Erro no batch final também deve abortar
                logger.error('processor', 'Erro FATAL ao processar batch final', error.message);
                errors++;
                
                // Emitir erro para ser capturado pelo handler
                csvStream.emit('error', error);
                return; // Não chamar readEntry() - arquivo falhou
              }
            }
            
            // Salvar checkpoint final
            if (fileId) {
              await updateFileCheckpoint(fileId, {
                linesProcessed: totalRecords,
                csvFileName: csvFileName,
                completed: true,
                lastCheckpoint: new Date().toISOString()
              });
              logger.info('processor', `✅ Checkpoint final salvo: ${totalRecords.toLocaleString()} linhas`);
            }
            
            zipfile.readEntry();
          });
          
          csvStream.on('error', (error) => {
            // ⚠️ CRÍTICO: Erro fatal (ex: falha no INSERT) deve abortar todo o processamento
            logger.error('processor', 'Erro FATAL no processamento do CSV', error.message);
            errors++;
            
            // Fechar o zipfile e rejeitar a Promise
            // Isso garante que o ETL saiba que houve falha
            zipfile.close();
            reject(new Error(`Erro fatal ao processar ${entry.fileName}: ${error.message}`));
          });
        });
      });

      zipfile.on('end', () => {
        logger.info('processor', `Processamento concluído: ${totalRecords} registros, ${errors} erros`);
        resolve({ totalRecords, errors });
      });

      zipfile.on('error', (error) => {
        logger.error('processor', 'Erro no processamento do ZIP', error.message);
        reject(error);
      });
    });
  });
}

export default { processZipFile };
