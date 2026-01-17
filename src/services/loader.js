import { pool } from '../config/database.js';
import logger from '../config/logger.js';
import { CONFIG } from '../config/constants.js';

/**
 * Database Loader
 * Carrega dados no PostgreSQL em batches
 */

/**
 * Monta query de INSERT em batch
 */
function buildBatchInsertQuery(tableName, columns, records, mode = 'insert', conflictColumns = null) {
  const placeholders = [];
  const values = [];
  let paramIndex = 1;

  records.forEach(record => {
    const recordPlaceholders = [];
    columns.forEach(col => {
      recordPlaceholders.push(`$${paramIndex++}`);
      values.push(record[col]);
    });
    placeholders.push(`(${recordPlaceholders.join(', ')})`);
  });

  let query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${placeholders.join(', ')}`;

  // Se modo UPSERT
  if (mode === 'upsert') {
    const updateColumns = columns.filter(col => !['id', 'created_at'].includes(col));
    const updateSet = updateColumns.map(col => `${col} = EXCLUDED.${col}`).join(', ');
    
    // Usar conflictColumns se fornecido, caso contrário usar primeira coluna
    const conflictCols = conflictColumns || [columns[0]];
    query += ` ON CONFLICT (${conflictCols.join(', ')}) DO UPDATE SET ${updateSet}, updated_at = NOW()`;
  }

  return { query, values };
}

/**
 * Carrega registros em batch
 */
export async function loadBatch(tableName, columns, records, mode = 'insert', conflictColumns = null) {
  if (records.length === 0) {
    return { inserted: 0, updated: 0 };
  }

  const client = await pool.connect();
  
  try {
    const { query, values } = buildBatchInsertQuery(tableName, columns, records, mode, conflictColumns);
    
    const result = await client.query(query, values);
    
    const inserted = mode === 'insert' ? result.rowCount : 0;
    const updated = mode === 'upsert' ? result.rowCount : 0;
    
    return { inserted, updated };
  } catch (error) {
    // Se erro de duplicate key e modo insert, logar mas não falhar
    if (error.code === '23505' && mode === 'insert') {
      logger.warn('loader', `Registro duplicado ignorado: ${error.detail}`);
      return { inserted: 0, updated: 0 };
    }
    
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Processa stream de dados em batches
 */
export async function processInBatches(tableName, columns, dataStream, mode = 'insert') {
  const batchSize = CONFIG.BATCH_SIZE;
  let batch = [];
  let totalInserted = 0;
  let totalUpdated = 0;
  let totalErrors = 0;

  for (const records of dataStream) {
    for (const record of records) {
      batch.push(record);

      if (batch.length >= batchSize) {
        try {
          const result = await loadBatch(tableName, columns, batch, mode);
          totalInserted += result.inserted;
          totalUpdated += result.updated;
          
          logger.info('loader', `Batch carregado: ${result.inserted || result.updated} registros`);
        } catch (error) {
          totalErrors++;
          logger.error('loader', 'Erro ao carregar batch', error.message);
        }
        
        batch = [];
      }
    }
  }

  // Processar batch final
  if (batch.length > 0) {
    try {
      const result = await loadBatch(tableName, columns, batch, mode);
      totalInserted += result.inserted;
      totalUpdated += result.updated;
      
      logger.info('loader', `Batch final carregado: ${result.inserted || result.updated} registros`);
    } catch (error) {
      totalErrors++;
      logger.error('loader', 'Erro ao carregar batch final', error.message);
    }
  }

  return { totalInserted, totalUpdated, totalErrors };
}

/**
 * Trunca tabela (para FULL LOAD)
 */
export async function truncateTable(tableName) {
  const client = await pool.connect();
  
  try {
    await client.query(`TRUNCATE TABLE ${tableName} CASCADE`);
    logger.info('loader', `Tabela truncada: ${tableName}`);
  } finally {
    client.release();
  }
}

export default { loadBatch, processInBatches, truncateTable };
