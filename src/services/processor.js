import fs from 'fs';
import yauzl from 'yauzl';
import csv from 'csv-parser';
import { Readable } from 'stream';
import logger from '../config/logger.js';

/**
 * Stream Processor
 * Descompacta ZIP e parseia CSV linha a linha via streaming
 */

/**
 * Processa arquivo ZIP extraindo CSVs e parseando linha a linha
 */
export async function processZipFile(zipPath, transformer, onData) {
  return new Promise((resolve, reject) => {
    logger.info('processor', `Processando arquivo: ${zipPath}`);
    
    let totalRecords = 0;
    let errors = 0;

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

        // Processar apenas arquivos CSV (aceita .csv ou que contenha CSV no nome)
        const fileName = entry.fileName.toLowerCase();
        if (!fileName.endsWith('.csv') && !fileName.includes('csv')) {
          logger.warn('processor', `Ignorando arquivo não-CSV: ${entry.fileName}`);
          zipfile.readEntry();
          return;
        }

        logger.info('processor', `Extraindo e processando: ${entry.fileName}`);

        zipfile.openReadStream(entry, (err, readStream) => {
          if (err) {
            logger.error('processor', 'Erro ao abrir stream do CSV', err.message);
            zipfile.readEntry();
            return;
          }

          let records = [];
          const BATCH_SIZE = 5000; // Processar a cada 5000 registros
          
          readStream
            .pipe(csv({
              separator: ';',
              headers: false,
              skipLines: 0,
              quote: '"',
            }))
            .on('data', async (row) => {
              try {
                // Transformar dados
                const transformed = transformer(row);
                if (transformed) {
                  records.push(transformed);
                  totalRecords++;
                  
                  // Processar batch quando atingir o tamanho
                  if (records.length >= BATCH_SIZE) {
                    try {
                      await onData(records);
                      records = []; // Limpar array para liberar memória
                    } catch (error) {
                      logger.error('processor', 'Erro ao processar batch', error.message);
                      errors++;
                    }
                  }
                }
              } catch (error) {
                errors++;
                logger.warn('processor', `Erro ao transformar linha ${totalRecords}`, error.message);
              }
            })
            .on('end', async () => {
              logger.info('processor', `CSV processado: ${entry.fileName} (${totalRecords} registros)`);
              
              // Processar registros restantes
              if (records.length > 0) {
                try {
                  await onData(records);
                  records = [];
                } catch (error) {
                  logger.error('processor', 'Erro ao processar batch final', error.message);
                }
              }
              
              zipfile.readEntry();
            })
            .on('error', (error) => {
              logger.error('processor', 'Erro no parse do CSV', error.message);
              zipfile.readEntry();
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
