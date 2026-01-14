/**
 * Transformer de Municípios
 * Transforma dados brutos do CSV em formato adequado ao banco
 */

/**
 * Layout do arquivo de municípios da Receita Federal:
 * 0: Código do Município
 * 1: Nome do Município
 */

export function transformMunicipio(row) {
  try {
    const codigo = row[0] ? parseInt(row[0].trim()) : null;
    const nome = row[1] ? row[1].trim() : null;

    // Validação básica
    if (!codigo || !nome) {
      return null;
    }

    return {
      codigo_municipio: codigo,
      nome_municipio: nome,
    };
  } catch (error) {
    throw new Error(`Erro ao transformar município: ${error.message}`);
  }
}

export default { transformMunicipio };
