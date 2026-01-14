import { discoverFiles } from './services/discovery.js';
import { downloadFile, getTempFilePath, cleanupFile } from './services/downloader.js';
import { processZipFile } from './services/processor.js';
import { transformMunicipio } from './transformers/municipio.js';
import { transformEstabelecimento } from './transformers/estabelecimento.js';
import { transformSocio } from './transformers/socio.js';
import { pool, testConnection } from './config/database.js';
import logger from './config/logger.js';
import { CONFIG } from './config/constants.js';
import * as control from './services/control.js';

/**
 * ETL Orchestrator
 * Coordena o fluxo completo do ETL
 */

// Mapeamento de transformers por tipo
const TRANSFORMERS = {
  [CONFIG.FILE_TYPES.MUNICIPIOS]: transformMunicipio,
  [CONFIG.FILE_TYPES.ESTABELECIMENTOS]: transformEstabelecimento,
  [CONFIG.FILE_TYPES.SOCIOS]: transformSocio,
};

// Mapeamento de tabelas por tipo
const TABLES = {
  [CONFIG.FILE_TYPES.MUNICIPIOS]: {
    name: 'municipios',
    columns: ['codigo_municipio', 'nome_municipio'],
  },
  [CONFIG.FILE_TYPES.ESTABELECIMENTOS]: {
    name: 'estabelecimentos',
    columns: [
      'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'cnpj',
      'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral',
      'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior',
      'codigo_pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
      'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf',
      'codigo_municipio', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax',
      'correio_eletronico', 'situacao_especial', 'data_situacao_especial',
    ],
  },
  [CONFIG.FILE_TYPES.SOCIOS]: {
    name: 'socios',
    columns: [
      'cnpj_basico', 'identificador_socio', 'nome_socio', 'cpf_cnpj_socio',
      'qualificacao_socio', 'data_entrada_sociedade', 'codigo_pais',
      'cpf_representante_legal', 'nome_representante_legal',
      'qualificacao_representante_legal', 'faixa_etaria',
    ],
  },
};

/**
 * Carrega dados no banco em batches
 */
async function loadDataBatch(fileType, records, mode = 'insert') {
  const table = TABLES[fileType];
  if (!table) {
    throw new Error(`Tipo de arquivo desconhecido: ${fileType}`);
  }

  const client = await pool.connect();
  
  try {
    let inserted = 0;
    let updated = 0;

    // Processar em batches
    const batchSize = CONFIG.BATCH_SIZE;
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      
      // Montar query
      const placeholders = [];
      const values = [];
      let paramIndex = 1;

      batch.forEach(record => {
        const recordPlaceholders = [];
        table.columns.forEach(col => {
          recordPlaceholders.push(`$${paramIndex++}`);
          values.push(record[col]);
        });
        placeholders.push(`(${recordPlaceholders.join(', ')})`);
      });

      let query = `INSERT INTO ${table.name} (${table.columns.join(', ')}) VALUES ${placeholders.join(', ')}`;

      // UPSERT para DELTA
      if (mode === 'upsert') {
        const updateColumns = table.columns.filter(col => !['id', 'created_at'].includes(col));
        const updateSet = updateColumns.map(col => `${col} = EXCLUDED.${col}`).join(', ');
        query += ` ON CONFLICT (${table.columns[0]}) DO UPDATE SET ${updateSet}, updated_at = NOW()`;
      } else {
        // INSERT com ignore de duplicatas
        query += ` ON CONFLICT (${table.columns[0]}) DO NOTHING`;
      }

      const result = await client.query(query, values);
      
      if (mode === 'insert') {
        inserted += result.rowCount;
      } else {
        updated += result.rowCount;
      }
    }

    return { inserted, updated };
  } finally {
    client.release();
  }
}

/**
 * Processa um arquivo individual
 */
async function processFile(fileInfo, mode = 'insert') {
  const { fileName, fileUrl, fileType } = fileInfo;
  
  logger.info('orchestrator', `Processando arquivo: ${fileName} (${fileType})`);
  
  const transformer = TRANSFORMERS[fileType];
  if (!transformer) {
    throw new Error(`Transformer não encontrado para tipo: ${fileType}`);
  }

  // 1. Download
  const tempPath = getTempFilePath(fileName);
  await downloadFile(fileUrl, tempPath);

  // 2. Processar e carregar
  let totalInserted = 0;
  let totalUpdated = 0;

  const result = await processZipFile(
    tempPath,
    transformer,
    async (records) => {
      const stats = await loadDataBatch(fileType, records, mode);
      totalInserted += stats.inserted;
      totalUpdated += stats.updated;
      
      logger.info('orchestrator', `Batch carregado: ${stats.inserted || stats.updated} registros`);
    }
  );

  // 3. Cleanup
  cleanupFile(tempPath);

  return {
    totalRecords: result.totalRecords,
    inserted: totalInserted,
    updated: totalUpdated,
    errors: result.errors,
  };
}

/**
 * Trunca tabelas para FULL LOAD
 */
async function truncateTables() {
  logger.info('orchestrator', 'Truncando tabelas...');
  
  const client = await pool.connect();
  try {
    await client.query('TRUNCATE TABLE socios CASCADE');
    await client.query('TRUNCATE TABLE estabelecimentos CASCADE');
    await client.query('TRUNCATE TABLE municipios CASCADE');
    
    logger.info('orchestrator', 'Tabelas truncadas');
  } finally {
    client.release();
  }
}

/**
 * Executa ETL completo
 */
export async function runETL(options = {}) {
  const {
    baseUrl = CONFIG.BASE_URL,
    loadType = CONFIG.LOAD_TYPE.FULL,
    filesToProcess = null, // Se null, processa todos
  } = options;

  logger.info('orchestrator', `Iniciando ETL - Tipo: ${loadType}`);

  // 1. Testar conexão
  const dbTest = await testConnection();
  if (!dbTest.success) {
    throw new Error(`Falha na conexão com banco: ${dbTest.error}`);
  }
  logger.info('orchestrator', 'Conexão com banco OK');

  // 2. Criar run
  const runId = await control.createRun(loadType);

  try {
    // 3. Truncar tabelas se FULL LOAD
    if (loadType === CONFIG.LOAD_TYPE.FULL) {
      await truncateTables();
    }

    // 4. Descobrir arquivos
    let files;
    if (filesToProcess) {
      files = filesToProcess;
    } else {
      files = await discoverFiles(baseUrl);
    }

    await control.updateRun(runId, { total_files: files.length });
    logger.info('orchestrator', `${files.length} arquivos para processar`);

    // 5. Registrar arquivos no controle
    for (const file of files) {
      await control.registerFile({
        fileName: file.fileName,
        fileUrl: file.fileUrl,
        fileType: file.fileType,
        fileYear: file.fileYear,
        fileMonth: file.fileMonth,
        loadType: loadType,
      });
    }

    // 6. Buscar arquivos pendentes (para retomada)
    const pendingFiles = await control.getPendingFiles();
    logger.info('orchestrator', `${pendingFiles.length} arquivos pendentes`);

    // 7. Processar cada arquivo
    let completed = 0;
    let failed = 0;

    for (const fileRecord of pendingFiles) {
      const fileId = fileRecord.id;
      
      try {
        logger.info('orchestrator', `[${completed + 1}/${pendingFiles.length}] ${fileRecord.file_name}`);
        
        // Marcar como processando
        await control.markFileAsProcessing(fileId);

        // Processar
        const mode = loadType === CONFIG.LOAD_TYPE.DELTA ? 'upsert' : 'insert';
        const stats = await processFile(
          {
            fileName: fileRecord.file_name,
            fileUrl: fileRecord.file_url,
            fileType: fileRecord.file_type,
          },
          mode
        );

        // Marcar como concluído
        await control.markFileAsDone(fileId, stats);
        
        completed++;
        logger.info('orchestrator', `✅ Arquivo concluído: ${stats.totalRecords} registros, ${stats.inserted || stats.updated} carregados`);
      } catch (error) {
        failed++;
        logger.error('orchestrator', `❌ Erro no arquivo ${fileRecord.file_name}`, error.message);
        
        await control.markFileAsError(fileId, error.message);
        
        // Continuar com próximo arquivo
      }
    }

    // 8. Finalizar run
    await control.finalizeRun(runId, 'completed');
    await control.updateRun(runId, {
      files_completed: completed,
      files_failed: failed,
    });

    logger.info('orchestrator', `ETL concluído: ${completed} arquivos processados, ${failed} falhas`);

    return {
      success: true,
      runId,
      completed,
      failed,
      total: pendingFiles.length,
    };
  } catch (error) {
    logger.error('orchestrator', 'Erro no ETL', error.message);
    await control.finalizeRun(runId, 'error', error.message);
    throw error;
  }
}

export default { runETL };
