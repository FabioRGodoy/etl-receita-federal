/**
 * Transformer de Municípios
 * Transforma dados brutos do CSV em formato adequado ao banco
 */

import { sanitizeRecord, parseInteger, safeTrim } from '../utils/sanitize.js';

/**
 * Layout do arquivo de municípios da Receita Federal:
 * 0: Código do Município
 * 1: Nome do Município
 */

export function transformMunicipio(row) {
  try {
    const codigo = parseInteger(row[0]);
    const nome = safeTrim(row[1]);

    // Validação básica
    if (!codigo || !nome) {
      return null;
    }

    const record = {
      codigo_municipio: codigo,
      nome_municipio: nome,
    };
    
    return sanitizeRecord(record);
  } catch (error) {
    throw new Error(`Erro ao transformar município: ${error.message}`);
  }
}

export default { transformMunicipio };
