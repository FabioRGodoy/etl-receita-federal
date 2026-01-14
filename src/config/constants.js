import dotenv from 'dotenv';

dotenv.config();

export const CONFIG = {
  // URLs
  BASE_URL: process.env.BASE_URL || 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/',
  
  // Processamento
  BATCH_SIZE: parseInt(process.env.BATCH_SIZE || '1000'),
  DOWNLOAD_TIMEOUT: parseInt(process.env.DOWNLOAD_TIMEOUT || '600000'), // 10 minutos
  
  // Diretórios
  TEMP_DIR: process.env.TEMP_DIR || './temp',
  LOG_DIR: process.env.LOG_DIR || './logs',
  
  // Estados
  STATUS: {
    PENDING: 'pending',
    PROCESSING: 'processing',
    DONE: 'done',
    ERROR: 'error',
  },
  
  // Tipos de carga
  LOAD_TYPE: {
    FULL: 'FULL',
    DELTA: 'DELTA',
  },
  
  // Tipos de arquivo
  FILE_TYPES: {
    ESTABELECIMENTOS: 'estabelecimentos',
    SOCIOS: 'socios',
    MUNICIPIOS: 'municipios',
  },
};

export default CONFIG;
