/**
 * Utilitários para sanitização de dados
 * Remove caracteres problemáticos que causam erros no PostgreSQL
 */

/**
 * Remove bytes nulos (0x00) e outros caracteres problemáticos
 * PostgreSQL não aceita \0 em campos TEXT/VARCHAR
 */
export function sanitizeString(str) {
  if (!str) return null;
  
  // Converter para string se não for
  let cleaned = String(str);
  
  // Remover bytes nulos (0x00) - causa erro "invalid byte sequence for encoding UTF8"
  cleaned = cleaned.replace(/\0/g, '');
  
  // Remover outros caracteres de controle problemáticos (exceto \n, \r, \t)
  cleaned = cleaned.replace(/[\x01-\x08\x0B-\x0C\x0E-\x1F\x7F]/g, '');
  
  // Trim espaços
  cleaned = cleaned.trim();
  
  return cleaned === '' ? null : cleaned;
}

/**
 * Sanitiza objeto completo recursivamente
 */
export function sanitizeRecord(record) {
  if (!record || typeof record !== 'object') {
    return record;
  }
  
  const sanitized = {};
  
  for (const [key, value] of Object.entries(record)) {
    if (typeof value === 'string') {
      sanitized[key] = sanitizeString(value);
    } else if (Array.isArray(value)) {
      sanitized[key] = value.map(item => 
        typeof item === 'string' ? sanitizeString(item) : item
      );
    } else {
      sanitized[key] = value;
    }
  }
  
  return sanitized;
}

/**
 * Parse de data com sanitização
 */
export function parseDate(dateStr) {
  if (!dateStr || dateStr.trim() === '' || dateStr === '0') {
    return null;
  }
  
  // Sanitizar primeiro
  const cleaned = sanitizeString(dateStr);
  if (!cleaned || cleaned.length !== 8) {
    return null;
  }
  
  // Formato: YYYYMMDD
  const year = cleaned.substring(0, 4);
  const month = cleaned.substring(4, 6);
  const day = cleaned.substring(6, 8);
  
  return `${year}-${month}-${day}`;
}

/**
 * Parse de inteiro com sanitização
 */
export function parseInteger(str) {
  if (!str || str.trim() === '') return null;
  
  const cleaned = sanitizeString(str);
  if (!cleaned) return null;
  
  const num = Number.parseInt(cleaned);
  return isNaN(num) ? null : num;
}

/**
 * Trim com sanitização
 */
export function safeTrim(str) {
  if (!str) return null;
  return sanitizeString(str);
}
