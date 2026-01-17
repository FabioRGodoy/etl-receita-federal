/**
 * Transformer de Sócios
 * Transforma dados brutos do CSV em formato adequado ao banco
 */

import { sanitizeRecord, parseDate, parseInteger, safeTrim } from '../utils/sanitize.js';

/**
 * Layout do arquivo de sócios da Receita Federal:
 * 0: CNPJ Básico
 * 1: Identificador de Sócio
 * 2: Nome do Sócio
 * 3: CPF/CNPJ do Sócio
 * 4: Qualificação do Sócio
 * 5: Data de Entrada na Sociedade
 * 6: Código do País
 * 7: CPF do Representante Legal
 * 8: Nome do Representante Legal
 * 9: Qualificação do Representante Legal
 * 10: Faixa Etária
 */

export function transformSocio(row) {
  try {
    const cnpjBasico = safeTrim(row[0]);
    const identificadorSocio = safeTrim(row[1]);
    
    if (!cnpjBasico || !identificadorSocio) {
      return null;
    }

    const record = {
      cnpj_basico: cnpjBasico,
      identificador_socio: identificadorSocio,
      nome_socio: safeTrim(row[2]),
      cpf_cnpj_socio: safeTrim(row[3]),
      qualificacao_socio: parseInteger(row[4]),
      data_entrada_sociedade: parseDate(row[5]),
      codigo_pais: parseInteger(row[6]),
      cpf_representante_legal: safeTrim(row[7]),
      nome_representante_legal: safeTrim(row[8]),
      qualificacao_representante_legal: parseInteger(row[9]),
      faixa_etaria: parseInteger(row[10]),
    };
    
    return sanitizeRecord(record);
  } catch (error) {
    console.error('Erro ao transformar sócio:', error.message);
    return null;
  }
}

export default { transformSocio };
