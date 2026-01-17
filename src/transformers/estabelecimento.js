/**
 * Transformer de Estabelecimentos
 * Transforma dados brutos do CSV em formato adequado ao banco
 */

import { sanitizeRecord, parseDate, parseInteger, safeTrim } from '../utils/sanitize.js';

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

export function transformEstabelecimento(row) {
  try {
    const cnpjBasico = safeTrim(row[0]);
    const cnpjOrdem = safeTrim(row[1]);
    const cnpjDv = safeTrim(row[2]);
    
    if (!cnpjBasico || !cnpjOrdem || !cnpjDv) {
      return null;
    }

    const cnpj = cnpjBasico + cnpjOrdem + cnpjDv;

    const record = {
      cnpj_basico: cnpjBasico,
      cnpj_ordem: cnpjOrdem,
      cnpj_dv: cnpjDv,
      cnpj: cnpj,
      identificador_matriz_filial: parseInteger(row[3]),
      nome_fantasia: safeTrim(row[4]),
      situacao_cadastral: parseInteger(row[5]),
      data_situacao_cadastral: parseDate(row[6]),
      motivo_situacao_cadastral: parseInteger(row[7]),
      nome_cidade_exterior: safeTrim(row[8]),
      codigo_pais: safeTrim(row[9]),
      data_inicio_atividade: parseDate(row[10]),
      cnae_fiscal_principal: safeTrim(row[11]),
      cnae_fiscal_secundaria: safeTrim(row[12]),
      tipo_logradouro: safeTrim(row[13]),
      logradouro: safeTrim(row[14]),
      numero: safeTrim(row[15]),
      complemento: safeTrim(row[16]),
      bairro: safeTrim(row[17]),
      cep: safeTrim(row[18]),
      uf: safeTrim(row[19]),
      codigo_municipio: parseInteger(row[20]),
      ddd1: safeTrim(row[21]),
      telefone1: safeTrim(row[22]),
      ddd2: safeTrim(row[23]),
      telefone2: safeTrim(row[24]),
      ddd_fax: safeTrim(row[25]),
      fax: safeTrim(row[26]),
      correio_eletronico: safeTrim(row[27]),
      situacao_especial: safeTrim(row[28]),
      data_situacao_especial: parseDate(row[29]),
    };
    
    // Sanitizar todo o registro para garantir
    return sanitizeRecord(record);
  } catch (error) {
    console.error('Erro ao transformar estabelecimento:', error.message);
    return null;
  }
}

export default { transformEstabelecimento };
