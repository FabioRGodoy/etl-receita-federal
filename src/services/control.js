import { pool } from '../config/database.js';
import logger from '../config/logger.js';
import { CONFIG } from '../config/constants.js';

/**
 * ETL Control Service
 * Gerencia estado de processamento e permite retomada
 */

/**
 * Cria novo run
 */
export async function createRun(runType) {
  const client = await pool.connect();
  
  try {
    const result = await client.query(
      `INSERT INTO etl_control_runs (run_type, status) 
       VALUES ($1, $2) 
       RETURNING id`,
      [runType, 'running']
    );
    
    const runId = result.rows[0].id;
    logger.info('control', `Run criado: ID=${runId}, tipo=${runType}`);
    
    return runId;
  } finally {
    client.release();
  }
}

/**
 * Atualiza run
 */
export async function updateRun(runId, updates) {
  const client = await pool.connect();
  
  try {
    const setClause = Object.keys(updates)
      .map((key, i) => `${key} = $${i + 2}`)
      .join(', ');
    
    const values = [runId, ...Object.values(updates)];
    
    await client.query(
      `UPDATE etl_control_runs SET ${setClause} WHERE id = $1`,
      values
    );
    
    logger.info('control', `Run atualizado: ID=${runId}`, updates);
  } finally {
    client.release();
  }
}

/**
 * Finaliza run
 */
export async function finalizeRun(runId, status, errorMessage = null) {
  await updateRun(runId, {
    status,
    completed_at: new Date(),
    error_message: errorMessage,
  });
}

/**
 * Registra arquivo descoberto
 */
export async function registerFile(fileInfo) {
  const client = await pool.connect();
  
  try {
    const result = await client.query(
      `INSERT INTO etl_control_files 
       (file_name, file_url, file_type, file_year, file_month, load_type, run_id, status, last_modified_date) 
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (file_name) DO UPDATE SET
         file_url = EXCLUDED.file_url,
         run_id = EXCLUDED.run_id,
         status = EXCLUDED.status,
         last_modified_date = EXCLUDED.last_modified_date,
         updated_at = NOW()
       RETURNING id`,
      [
        fileInfo.fileName,
        fileInfo.fileUrl,
        fileInfo.fileType,
        fileInfo.fileYear,
        fileInfo.fileMonth,
        fileInfo.loadType,
        fileInfo.runId,
        CONFIG.STATUS.PENDING,
        fileInfo.lastModified || null, // 🆕 Data de modificação
      ]
    );
    
    return result.rows[0].id;
  } finally {
    client.release();
  }
}

/**
 * Atualiza status de arquivo
 */
export async function updateFileStatus(fileId, status, updates = {}) {
  const client = await pool.connect();
  
  try {
    const allUpdates = { status, ...updates };
    
    const setClause = Object.keys(allUpdates)
      .map((key, i) => `${key} = $${i + 2}`)
      .join(', ');
    
    const values = [fileId, ...Object.values(allUpdates)];
    
    await client.query(
      `UPDATE etl_control_files SET ${setClause} WHERE id = $1`,
      values
    );
  } finally {
    client.release();
  }
}

/**
 * Marca arquivo como processando
 */
export async function markFileAsProcessing(fileId) {
  await updateFileStatus(fileId, CONFIG.STATUS.PROCESSING, {
    started_at: new Date(),
  });
}

/**
 * Marca arquivo como concluído
 */
export async function markFileAsDone(fileId, stats) {
  await updateFileStatus(fileId, CONFIG.STATUS.DONE, {
    records_inserted: stats.inserted || 0,
    records_updated: stats.updated || 0,
    completed_at: new Date(),
  });
}

/**
 * Marca arquivo como erro
 */
export async function markFileAsError(fileId, errorMessage) {
  await updateFileStatus(fileId, CONFIG.STATUS.ERROR, {
    error_message: errorMessage,
    completed_at: new Date(),
  });
}

/**
 * Busca arquivos pendentes
 * @param {number} runId - ID do run (opcional, se não informado pega todos)
 */
export async function getPendingFiles(runId = null) {
  const client = await pool.connect();
  
  try {
    let query = `
      SELECT * FROM etl_control_files 
      WHERE status IN ($1, $2)
    `;
    
    const params = [CONFIG.STATUS.PENDING, CONFIG.STATUS.ERROR];
    
    // Filtrar por runId se informado
    if (runId) {
      query += ` AND run_id = $3`;
      params.push(runId);
    }
    
    query += `
      ORDER BY 
        file_year, 
        file_month,
        CASE file_type
          WHEN 'municipios' THEN 1
          WHEN 'estabelecimentos' THEN 2
          WHEN 'socios' THEN 3
          ELSE 999
        END,
        id
    `;
    
    const result = await client.query(query, params);
    
    return result.rows;
  } finally {
    client.release();
  }
}

/**
 * Busca arquivos já processados para DELTA (OBSOLETO - usar getFilesToReprocess)
 */
export async function getProcessedFiles(fileType = null) {
  const client = await pool.connect();
  
  try {
    let query = `
      SELECT file_type, file_year, file_month 
      FROM etl_control_files 
      WHERE status = $1
    `;
    const params = [CONFIG.STATUS.DONE];
    
    if (fileType) {
      query += ` AND file_type = $2`;
      params.push(fileType);
    }
    
    query += ` ORDER BY file_year DESC, file_month DESC`;
    
    const result = await client.query(query, params);
    return result.rows;
  } finally {
    client.release();
  }
}

/**
 * Identifica arquivos que precisam ser reprocessados (DELTA)
 * Compara data de modificação no site vs banco de dados
 * 
 * @param {Array} discoveredFiles - Arquivos descobertos no site (com lastModified)
 * @returns {Array} - Arquivos que precisam ser reprocessados com motivo
 */
export async function getFilesToReprocess(discoveredFiles) {
  const client = await pool.connect();
  
  try {
    const filesToProcess = [];
    
    for (const file of discoveredFiles) {
      // Buscar data de modificação no banco
      const result = await client.query(
        `SELECT last_modified_date, status
         FROM etl_control_files
         WHERE file_name = $1`,
        [file.fileName]
      );
      
      let reason = null;
      let shouldProcess = false;
      
      if (result.rows.length === 0) {
        // Arquivo nunca foi processado
        reason = 'Novo arquivo (nunca processado)';
        shouldProcess = true;
      } else {
        const dbDate = result.rows[0].last_modified_date;
        const fileStatus = result.rows[0].status;
        
        if (!dbDate) {
          // Arquivo já existe mas sem data (migração)
          reason = 'Sem data registrada (pós-migração)';
          shouldProcess = true;
        } else if (!file.lastModified) {
          // Arquivo no site sem data (erro no scraping)
          reason = 'Arquivo no site sem data (por segurança)';
          shouldProcess = true;
        } else {
          // Comparar datas
          const siteDate = new Date(file.lastModified);
          const dbDateObj = new Date(dbDate);
          
          if (siteDate > dbDateObj) {
            const diff = Math.round((siteDate - dbDateObj) / 1000 / 60); // minutos
            reason = `Arquivo atualizado (${diff} min mais recente)`;
            shouldProcess = true;
          }
        }
      }
      
      if (shouldProcess) {
        filesToProcess.push({
          ...file,
          reprocessReason: reason,
        });
      }
    }
    
    return filesToProcess;
  } finally {
    client.release();
  }
}

/**
 * Atualiza checkpoint de progresso do arquivo
 */
export async function updateFileCheckpoint(fileId, checkpoint) {
  const client = await pool.connect();
  
  try {
    await client.query(
      `UPDATE etl_control_files 
       SET checkpoint = $1, updated_at = NOW()
       WHERE id = $2`,
      [JSON.stringify(checkpoint), fileId]
    );
    
    logger.debug('control', `Checkpoint atualizado: fileId=${fileId}`);
  } finally {
    client.release();
  }
}

/**
 * Obtém checkpoint do arquivo
 */
export async function getFileCheckpoint(fileId) {
  const client = await pool.connect();
  
  try {
    const result = await client.query(
      'SELECT checkpoint FROM etl_control_files WHERE id = $1',
      [fileId]
    );
    
    if (result.rows[0]?.checkpoint) {
      return result.rows[0].checkpoint;
    }
    
    return null;
  } finally {
    client.release();
  }
}

export default {
  createRun,
  updateRun,
  finalizeRun,
  registerFile,
  updateFileStatus,
  markFileAsProcessing,
  markFileAsDone,
  markFileAsError,
  getPendingFiles,
  getProcessedFiles, // Mantido por compatibilidade (obsoleto)
  getFilesToReprocess, // 🆕 Nova função para DELTA
  updateFileCheckpoint,
  getFileCheckpoint,
};
