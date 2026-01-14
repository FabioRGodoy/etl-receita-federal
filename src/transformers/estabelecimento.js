/**
 * Transformer de Estabelecimentos
 * Transforma dados brutos do CSV em formato adequado ao banco
 */

/**
 * Layout do arquivo de estabelecimentos da Receita Federal:
 * 0: CNPJ Básico
 * 1: CNPJ Ordem
 * 2: CNPJ DV
 * 3: Identificador Matriz/Filial
 * 4: Nome Fantasia
 * 5: Situação Cadastral
 * 6: Data Situação Cadastral
 * 7: Motivo Situação Cadastral
 * 8: Nome da Cidade no Exterior
 * 9: Código do País
 * 10: Data de Início da Atividade
 * 11: CNAE Fiscal Principal
 * 12: CNAE Fiscal Secundária
 * 13: Tipo de Logradouro
 * 14: Logradouro
 * 15: Número
 * 16: Complemento
 * 17: Bairro
 * 18: CEP
 * 19: UF
 * 20: Código do Município
 * 21: DDD 1
 * 22: Telefone 1
 * 23: DDD 2
 * 24: Telefone 2
 * 25: DDD Fax
 * 26: Fax
 * 27: Correio Eletrônico
 * 28: Situação Especial
 * 29: Data Situação Especial
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

export function transformEstabelecimento(row) {
  try {
    const cnpjBasico = trim(row[0]);
    const cnpjOrdem = trim(row[1]);
    const cnpjDv = trim(row[2]);
    
    if (!cnpjBasico || !cnpjOrdem || !cnpjDv) {
      return null;
    }

    const cnpj = cnpjBasico + cnpjOrdem + cnpjDv;

    return {
      cnpj_basico: cnpjBasico,
      cnpj_ordem: cnpjOrdem,
      cnpj_dv: cnpjDv,
      cnpj: cnpj,
      identificador_matriz_filial: parseInt(row[3]),
      nome_fantasia: trim(row[4]),
      situacao_cadastral: parseInt(row[5]),
      data_situacao_cadastral: parseDate(row[6]),
      motivo_situacao_cadastral: parseInt(row[7]),
      nome_cidade_exterior: trim(row[8]),
      codigo_pais: parseInt(row[9]),
      data_inicio_atividade: parseDate(row[10]),
      cnae_fiscal_principal: parseInt(row[11]),
      cnae_fiscal_secundaria: trim(row[12]),
      tipo_logradouro: trim(row[13]),
      logradouro: trim(row[14]),
      numero: trim(row[15]),
      complemento: trim(row[16]),
      bairro: trim(row[17]),
      cep: trim(row[18]),
      uf: trim(row[19]),
      codigo_municipio: parseInt(row[20]),
      ddd1: trim(row[21]),
      telefone1: trim(row[22]),
      ddd2: trim(row[23]),
      telefone2: trim(row[24]),
      ddd_fax: trim(row[25]),
      fax: trim(row[26]),
      correio_eletronico: trim(row[27]),
      situacao_especial: trim(row[28]),
      data_situacao_especial: parseDate(row[29]),
    };
  } catch (error) {
    throw new Error(`Erro ao transformar estabelecimento: ${error.message}`);
  }
}

export default { transformEstabelecimento };
