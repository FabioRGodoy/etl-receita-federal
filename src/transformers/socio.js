/**
 * Transformer de Sócios
 * Transforma dados brutos do CSV em formato adequado ao banco
 */

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

function parseDate(dateStr) {
  if (!dateStr || dateStr.trim() === '' || dateStr === '0') {
    return null;
  }
  
  // Formato: YYYYMMDD
  const year = dateStr.substring(0, 4);
  const month = dateStr.substring(4, 6);
  const day = dateStr.substring(6, 8);
  
  return `${year}-${month}-${day}`;
}

function trim(str) {
  return str ? str.trim() : null;
}

function parseInt(str) {
  if (!str || str.trim() === '') return null;
  const num = Number.parseInt(str);
  return isNaN(num) ? null : num;
}

export function transformSocio(row) {
  try {
    const cnpjBasico = trim(row[0]);
    
    if (!cnpjBasico) {
      return null;
    }

    return {
      cnpj_basico: cnpjBasico,
      identificador_socio: parseInt(row[1]),
      nome_socio: trim(row[2]),
      cpf_cnpj_socio: trim(row[3]),
      qualificacao_socio: parseInt(row[4]),
      data_entrada_sociedade: parseDate(row[5]),
      codigo_pais: parseInt(row[6]),
      cpf_representante_legal: trim(row[7]),
      nome_representante_legal: trim(row[8]),
      qualificacao_representante_legal: parseInt(row[9]),
      faixa_etaria: parseInt(row[10]),
    };
  } catch (error) {
    throw new Error(`Erro ao transformar sócio: ${error.message}`);
  }
}

export default { transformSocio };
