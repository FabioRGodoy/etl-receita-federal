import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';

dotenv.config();

const LOG_DIR = process.env.LOG_DIR || './logs';

// Criar diretório de logs se não existir
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

const LOG_LEVELS = {
  ERROR: 'ERROR',
  WARN: 'WARN',
  INFO: 'INFO',
  DEBUG: 'DEBUG',
};

function formatTimestamp() {
  return new Date().toISOString().replace('T', ' ').substring(0, 19);
}

function formatMessage(level, component, message, details = null) {
  const timestamp = formatTimestamp();
  let logLine = `[${timestamp}] ${level.padEnd(5)} | ${component.padEnd(15)} | ${message}`;
  
  if (details) {
    logLine += ` | ${typeof details === 'object' ? JSON.stringify(details) : details}`;
  }
  
  return logLine;
}

function writeToFile(logLine) {
  const today = new Date().toISOString().split('T')[0];
  const logFile = path.join(LOG_DIR, `etl-${today}.log`);
  
  fs.appendFileSync(logFile, logLine + '\n', 'utf8');
}

function log(level, component, message, details = null) {
  const logLine = formatMessage(level, component, message, details);
  
  // Console
  if (level === LOG_LEVELS.ERROR) {
    console.error(logLine);
  } else if (level === LOG_LEVELS.WARN) {
    console.warn(logLine);
  } else {
    console.log(logLine);
  }
  
  // Arquivo
  try {
    writeToFile(logLine);
  } catch (error) {
    console.error('[LOGGER] Erro ao escrever no arquivo de log:', error.message);
  }
}

export const logger = {
  error: (component, message, details) => log(LOG_LEVELS.ERROR, component, message, details),
  warn: (component, message, details) => log(LOG_LEVELS.WARN, component, message, details),
  info: (component, message, details) => log(LOG_LEVELS.INFO, component, message, details),
  debug: (component, message, details) => log(LOG_LEVELS.DEBUG, component, message, details),
};

export default logger;
