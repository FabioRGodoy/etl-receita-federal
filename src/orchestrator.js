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

// Variáveis globais para graceful shutdown
let isShuttingDown = false;
let currentFileId = null;

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  logger.warn('orchestrator', '⚠️  SIGTERM recebido - iniciando shutdown gracioso');
  isShuttingDown = true;
  
  if (currentFileId) {
    logger.info('orchestrator', '💾 Salvando checkpoint do arquivo em processamento...');
    await new Promise(resolve => setTimeout(resolve, 2000)); // Aguardar salvamento
  }
  
  try {
    await pool.end();
  } catch (err) {
    logger.error('orchestrator', 'Erro ao fechar pool', err.message);
  }
  
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.warn('orchestrator', '⚠️  SIGINT recebido (Ctrl+C) - iniciando shutdown gracioso');
  isShuttingDown = true;
  
  if (currentFileId) {
    logger.info('orchestrator', '💾 Salvando checkpoint do arquivo em processamento...');
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  try {
    await pool.end();
  } catch (err) {
    logger.error('orchestrator', 'Erro ao fechar pool', err.message);
  }
  
  process.exit(0);
});

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
    conflictColumns: ['codigo_municipio'],
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
    conflictColumns: ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'],
  },
  [CONFIG.FILE_TYPES.SOCIOS]: {
    name: 'socios',
    columns: [
      'cnpj_basico', 'identificador_socio', 'nome_socio', 'cpf_cnpj_socio',
      'qualificacao_socio', 'data_entrada_sociedade', 'codigo_pais',
      'cpf_representante_legal', 'nome_representante_legal',
      'qualificacao_representante_legal', 'faixa_etaria',
    ],
    conflictColumns: ['cnpj_basico', 'identificador_socio', 'cpf_cnpj_socio'],
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
        const conflictCols = table.conflictColumns || [table.columns[0]];
        query += ` ON CONFLICT (${conflictCols.join(', ')}) DO UPDATE SET ${updateSet}, updated_at = NOW()`;
      } else {
        // INSERT com ignore de duplicatas
        const conflictCols = table.conflictColumns || [table.columns[0]];
        query += ` ON CONFLICT (${conflictCols.join(', ')}) DO NOTHING`;
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
async function processFile(fileInfo, mode = 'insert', fileId = null) {
  const { fileName, fileUrl, fileType } = fileInfo;
  
  logger.info('orchestrator', `Processando arquivo: ${fileName} (${fileType})`);
  
  const transformer = TRANSFORMERS[fileType];
  if (!transformer) {
    throw new Error(`Transformer não encontrado para tipo: ${fileType}`);
  }

  // 1. Download (verificar se já existe primeiro)
  const tempPath = getTempFilePath(fileName);
  
  // Verificar se arquivo já foi processado completamente
  if (fileId) {
    const checkpoint = await control.getFileCheckpoint(fileId);
    if (checkpoint?.completed) {
      logger.info('orchestrator', `✅ Arquivo já processado: ${fileName}`);
      return {
        totalRecords: checkpoint.linesProcessed || 0,
        inserted: 0,
        updated: 0,
        errors: 0,
        skipped: true,
      };
    }
  }
  
  // Download apenas se necessário
  const fs = await import('fs');
  const fileExists = fs.existsSync(tempPath);
  
  if (fileExists) {
    // Verificar se arquivo está completo/válido
    const stats = fs.statSync(tempPath);
    
    // Se arquivo tem menos de 1KB, provavelmente está corrompido
    if (stats.size < 1024) {
      logger.warn('orchestrator', `⚠️  Arquivo muito pequeno (${stats.size} bytes) - re-baixando: ${fileName}`);
      fs.unlinkSync(tempPath);
      await downloadFile(fileUrl, tempPath);
    } else {
      logger.info('orchestrator', `♻️  Reutilizando arquivo existente: ${fileName} (${(stats.size / 1024 / 1024).toFixed(2)} MB)`);
    }
  } else {
    await downloadFile(fileUrl, tempPath);
  }

  // 2. Processar e carregar (com checkpoint)
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
    },
    CONFIG.BATCH_SIZE,
    fileId // Passar fileId para checkpoint
  );

  // 3. Cleanup APENAS se processou com sucesso
  // ⚠️ Se der erro, mantém o arquivo para retry
  if (result.totalRecords > 0 && result.errors === 0) {
    cleanupFile(tempPath);
  } else {
    logger.warn('orchestrator', `⚠️  Mantendo arquivo para retry: ${fileName}`);
  }

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
    latestOnly = true, // Padrão: apenas mês mais recente
    year = null,
    month = null,
    fileTypes = null,
    force = false, // Nova opção: forçar execução mesmo com run recente
  } = options;

  logger.info('orchestrator', `Iniciando ETL - Tipo: ${loadType}`, {
    latestOnly,
    year,
    month,
    fileTypes,
    force,
  });

  // 1. Testar conexão
  const dbTest = await testConnection();
  if (!dbTest.success) {
    throw new Error(`Falha na conexão com banco: ${dbTest.error}`);
  }
  logger.info('orchestrator', 'Conexão com banco OK');

  // 2. Verificar se já existe FULL LOAD recente (evitar loop infinito)
  if (loadType === CONFIG.LOAD_TYPE.FULL && !force) {
    const client = await pool.connect();
    try {
      const result = await client.query(`
        SELECT id, status, completed_at, files_completed, files_failed
        FROM etl_control_runs
        WHERE load_type = 'FULL'
          AND status = 'completed'
          AND completed_at > NOW() - INTERVAL '2 hours'
        ORDER BY completed_at DESC
        LIMIT 1
      `);

      if (result.rows.length > 0) {
        const recentRun = result.rows[0];
        const completedAt = new Date(recentRun.completed_at);
        const minutesAgo = Math.round((Date.now() - completedAt.getTime()) / 1000 / 60);
        
        logger.warn(
          'orchestrator',
          `⚠️  FULL LOAD já foi concluído há ${minutesAgo} minutos (Run ID: ${recentRun.id})`
        );
        logger.warn(
          'orchestrator',
          `   Arquivos: ${recentRun.files_completed} concluídos, ${recentRun.files_failed} falhas`
        );
        logger.warn('orchestrator', '');
        logger.warn('orchestrator', '🛑 Abortando para evitar TRUNCATE acidental dos dados!');
        logger.warn('orchestrator', '');
        logger.warn('orchestrator', 'Para executar novamente:');
        logger.warn('orchestrator', '  1. Execute: node limpar-controle.js');
        logger.warn('orchestrator', '  2. Execute: npm run full-load -- --yes');
        logger.warn('orchestrator', '');
        logger.warn('orchestrator', 'Ou use --force para ignorar esta proteção (cuidado!)');
        
        throw new Error('FULL LOAD recente detectado - operação cancelada por segurança');
      }
    } finally {
      client.release();
    }
  }

  // 3. Criar run
  const runId = await control.createRun(loadType);

  try {
    // 4. Limpar arquivos de controle pendentes antigos se FULL LOAD
    if (loadType === CONFIG.LOAD_TYPE.FULL) {
      await truncateTables();
      
      // Limpar arquivos pendentes de runs anteriores
      const client = await pool.connect();
      try {
        await client.query(`
          DELETE FROM etl_control_files 
          WHERE status IN ('pending', 'error')
        `);
        logger.info('orchestrator', 'Arquivos pendentes antigos removidos');
      } finally {
        client.release();
      }
    }

    // 5. Descobrir arquivos
    let files;
    if (filesToProcess) {
      files = filesToProcess;
    } else {
      files = await discoverFiles(baseUrl, {
        latestOnly,
        year,
        month,
        fileTypes,
      });
    }

    if (files.length === 0) {
      logger.warn('orchestrator', 'Nenhum arquivo encontrado para processar');
      await control.updateRun(runId, {
        total_files: 0,
        status: 'completed',
        completed_at: new Date(),
      });
      return {
        success: false,
        completed: 0,
        failed: 0,
        message: 'Nenhum arquivo encontrado',
      };
    }

    await control.updateRun(runId, { total_files: files.length });
    logger.info('orchestrator', `${files.length} arquivos para processar`);

    // 6. Ordenar arquivos: municipios -> estabelecimentos -> socios
    // Isso garante que as FKs sejam respeitadas
    const sortedFiles = files.sort((a, b) => {
      const order = {
        [CONFIG.FILE_TYPES.MUNICIPIOS]: 1,
        [CONFIG.FILE_TYPES.ESTABELECIMENTOS]: 2,
        [CONFIG.FILE_TYPES.SOCIOS]: 3,
      };
      return (order[a.fileType] || 999) - (order[b.fileType] || 999);
    });
    
    // 7. Registrar arquivos no controle (já ordenados)
    for (const file of sortedFiles) {
      await control.registerFile({
        fileName: file.fileName,
        fileUrl: file.fileUrl,
        fileType: file.fileType,
        fileYear: file.fileYear,
        fileMonth: file.fileMonth,
        loadType: loadType,
        runId: runId, // Associar ao run atual
      });
    }

    // 8. Buscar arquivos pendentes APENAS do run atual
    const pendingFiles = await control.getPendingFiles(runId);
    logger.info('orchestrator', `${pendingFiles.length} arquivos pendentes para este run`);

    // 9. Processar cada arquivo (já vem ordenado do getPendingFiles)
    let completed = 0;
    let failed = 0;

    for (const fileRecord of pendingFiles) {
      // Verificar se está em shutdown
      if (isShuttingDown) {
        logger.warn('orchestrator', '⚠️  Shutdown em andamento - parando processamento');
        break;
      }
      
      const fileId = fileRecord.id;
      currentFileId = fileId; // Armazenar para graceful shutdown
      
      try {
        logger.info('orchestrator', `[${completed + 1}/${pendingFiles.length}] ${fileRecord.file_name}`);
        
        // Marcar como processando
        await control.markFileAsProcessing(fileId);

        // Processar (com checkpoint)
        const mode = loadType === CONFIG.LOAD_TYPE.DELTA ? 'upsert' : 'insert';
        const stats = await processFile(
          {
            fileName: fileRecord.file_name,
            fileUrl: fileRecord.file_url,
            fileType: fileRecord.file_type,
          },
          mode,
          fileId // Passar fileId para checkpoint
        );

        // Marcar como concluído
        await control.markFileAsDone(fileId, stats);
        
        completed++;
        logger.info('orchestrator', `✅ Arquivo concluído: ${stats.totalRecords} registros, ${stats.inserted || stats.updated} carregados`);
        
        currentFileId = null; // Limpar após sucesso
      } catch (error) {
        failed++;
        logger.error('orchestrator', `❌ Erro no arquivo ${fileRecord.file_name}`, error.message);
        
        await control.markFileAsError(fileId, error.message);
        
        currentFileId = null; // Limpar após erro
        
        // Se arquivo ZIP está corrompido, deletar para forçar re-download na próxima execução
        if (error.message.includes('zip') || error.message.includes('truncated') || error.message.includes('signature not found')) {
          const fs = await import('fs');
          const tempPath = getTempFilePath(fileRecord.file_name);
          if (fs.existsSync(tempPath)) {
            fs.unlinkSync(tempPath);
            logger.warn('orchestrator', `⚠️  Arquivo corrompido removido: ${fileRecord.file_name}`);
          }
        }
        
        // ⚠️ Continuar com próximo arquivo (não abortar ETL inteiro)
      }
    }
    
    // 9. Verificar se foi interrompido
    if (isShuttingDown) {
      logger.warn('orchestrator', '⚠️  ETL interrompido por shutdown - pode ser retomado');
      
      await control.updateRun(runId, {
        files_completed: completed,
        files_failed: failed,
        status: 'interrupted',
        completed_at: new Date(),
      });
      
      return {
        success: false,
        completed,
        failed,
        message: 'ETL interrompido - retome executando novamente',
      };
    }

    // 10. Finalizar run
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
