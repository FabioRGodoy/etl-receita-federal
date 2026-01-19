import axios from 'axios';
import * as cheerio from 'cheerio';
import logger from '../config/logger.js';
import { CONFIG } from '../config/constants.js';

/**
 * Discovery Service
 * Navega na estrutura de diretórios da Receita Federal e descobre arquivos disponíveis
 */

/**
 * Extrai metadados do nome do arquivo
 * Ex: "Estabelecimentos0.zip" -> { type: 'estabelecimentos', sequence: 0 }
 */
function parseFileName(fileName) {
  const patterns = {
    estabelecimentos: /^(Estabelecimentos?)(\d+)\.zip$/i,
    socios: /^(Socios?)(\d+)\.zip$/i,
    municipios: /^(Municipios?)\.zip$/i,
  };

  for (const [type, pattern] of Object.entries(patterns)) {
    const match = fileName.match(pattern);
    if (match) {
      return {
        type,
        sequence: match[2] ? parseInt(match[2]) : 0,
        fileName,
      };
    }
  }

  return null;
}

/**
 * Busca arquivos ZIP em uma URL específica
 */
async function fetchFilesFromUrl(url) {
  try {
    logger.info('discovery', `Buscando arquivos em: ${url}`);
    
    const response = await axios.get(url, {
      timeout: 30000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (ETL Receita Federal)',
      },
    });

    const $ = cheerio.load(response.data);
    const files = [];

    // Tentar extrair de tabela (com data de modificação)
    const filesFromTable = [];
    $('tr').each((i, row) => {
      const cells = $(row).find('td');
      if (cells.length < 3) return; // Precisa ter pelo menos ícone, nome e data

      // Coluna 1 (índice 1): Link do arquivo
      const link = $(cells[1]).find('a');
      const href = link.attr('href');
      
      if (!href || !href.endsWith('.zip')) return;

      const fileName = href.split('/').pop();
      const fileInfo = parseFileName(fileName);
      
      if (!fileInfo) return;

      // Coluna 2 (índice 2): Data de modificação (formato: 2026-01-11 14:59)
      let lastModified = null;
      const dateText = $(cells[2]).text().trim();
      
      if (dateText && dateText.match(/\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}/)) {
        try {
          // Formato: YYYY-MM-DD HH:MM
          lastModified = new Date(dateText);
          
          // Validar data
          if (isNaN(lastModified.getTime())) {
            logger.warn('discovery', `Data inválida para ${fileName}: ${dateText}`);
            lastModified = null;
          }
        } catch (error) {
          logger.warn('discovery', `Erro ao parsear data de ${fileName}: ${dateText}`, error.message);
        }
      }

      // Construir URL completa do arquivo
      let fileUrl;
      if (href.startsWith('http://') || href.startsWith('https://')) {
        fileUrl = href;
      } else if (href.startsWith('/')) {
        const urlObj = new URL(url);
        fileUrl = urlObj.origin + href;
      } else {
        fileUrl = url + href;
      }
      
      filesFromTable.push({
        fileName: fileName,
        fileUrl: fileUrl,
        fileType: fileInfo.type,
        sequence: fileInfo.sequence,
        lastModified: lastModified,
      });
    });

    // Se encontrou arquivos na tabela, usar esses
    if (filesFromTable.length > 0) {
      files.push(...filesFromTable);
    } else {
      // Fallback: buscar apenas links (sem data)
      $('a').each((i, element) => {
        const href = $(element).attr('href');
        if (href && href.endsWith('.zip')) {
          const fileName = href.split('/').pop();
          const fileInfo = parseFileName(fileName);
          if (fileInfo) {
            // Construir URL completa do arquivo
            let fileUrl;
            if (href.startsWith('http://') || href.startsWith('https://')) {
              fileUrl = href;
            } else if (href.startsWith('/')) {
              const urlObj = new URL(url);
              fileUrl = urlObj.origin + href;
            } else {
              fileUrl = url + href;
            }
            
            files.push({
              fileName: fileName,
              fileUrl: fileUrl,
              fileType: fileInfo.type,
              sequence: fileInfo.sequence,
              lastModified: null, // Sem data disponível
            });
          }
        }
      });
    }

    logger.info('discovery', `Encontrados ${files.length} arquivos ZIP em ${url}`);
    return files;
  } catch (error) {
    logger.error('discovery', `Erro ao buscar arquivos em ${url}`, error.message);
    throw error;
  }
}

/**
 * Busca diretórios de ano/mês
 */
async function fetchDirectories(baseUrl) {
  try {
    logger.info('discovery', `Buscando diretórios em: ${baseUrl}`);
    
    const response = await axios.get(baseUrl, {
      timeout: 30000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (ETL Receita Federal)',
      },
    });

    const $ = cheerio.load(response.data);
    const directories = [];

    // Buscar links que são diretórios (terminam com /)
    $('a').each((i, element) => {
      const href = $(element).attr('href');
      if (href && href.endsWith('/') && href !== '../') {
        // Se href começa com http:// ou https://, usar como está
        // Se começa com /, é caminho absoluto, construir URL completa
        // Caso contrário, é caminho relativo, concatenar com baseUrl
        let fullUrl;
        if (href.startsWith('http://') || href.startsWith('https://')) {
          fullUrl = href;
        } else if (href.startsWith('/')) {
          // Pegar apenas o domínio da baseUrl
          const urlObj = new URL(baseUrl);
          fullUrl = urlObj.origin + href;
        } else {
          fullUrl = baseUrl + href;
        }
        
        directories.push({
          name: href.replace(/\//g, ''),
          url: fullUrl,
        });
      }
    });

    logger.info('discovery', `Encontrados ${directories.length} diretórios em ${baseUrl}`);
    return directories;
  } catch (error) {
    logger.error('discovery', `Erro ao buscar diretórios em ${baseUrl}`, error.message);
    return [];
  }
}

/**
 * Extrai ano e mês do nome do diretório
 */
function parseDirectoryName(dirName) {
  // Padrões possíveis: "2024", "2024-01", "202401", etc
  const yearMatch = dirName.match(/(\d{4})/);
  const monthMatch = dirName.match(/[-_]?(\d{2})$/);

  return {
    year: yearMatch ? parseInt(yearMatch[1]) : null,
    month: monthMatch ? parseInt(monthMatch[1]) : null,
  };
}

/**
 * Encontra o diretório mais recente (ano/mês)
 */
function findLatestDirectory(directories) {
  if (directories.length === 0) {
    return null;
  }

  // Parsear e ordenar por ano/mês decrescente
  const parsed = directories
    .map(dir => ({
      ...dir,
      ...parseDirectoryName(dir.name),
    }))
    .filter(d => d.year) // Apenas diretórios com ano válido
    .sort((a, b) => {
      // Ordenar por ano DESC, depois mês DESC
      const yearDiff = (b.year || 0) - (a.year || 0);
      if (yearDiff !== 0) return yearDiff;
      return (b.month || 0) - (a.month || 0);
    });

  return parsed.length > 0 ? parsed[0] : null;
}

/**
 * Descobre arquivos do mês/ano mais recente disponível
 * 
 * @param {string} baseUrl - URL base da Receita Federal
 * @param {Object} options - Opções de busca
 * @param {boolean} options.latestOnly - Se true, busca apenas mês mais recente (padrão: true)
 * @param {number} options.year - Ano específico (opcional)
 * @param {number} options.month - Mês específico (opcional)
 * @param {Array<string>} options.fileTypes - Tipos de arquivo para filtrar (opcional)
 */
export async function discoverFiles(baseUrl = CONFIG.BASE_URL, options = {}) {
  const {
    latestOnly = true, // Padrão: apenas mais recente
    year = null,
    month = null,
    fileTypes = null,
  } = options;

  logger.info('discovery', 'Iniciando descoberta de arquivos', {
    latestOnly,
    year,
    month,
    fileTypes,
  });

  const allFiles = [];

  try {
    // Primeiro, tentar buscar arquivos diretamente na URL base
    const baseFiles = await fetchFilesFromUrl(baseUrl);
    
    if (baseFiles.length > 0) {
      logger.info('discovery', `Arquivos encontrados na raiz: ${baseFiles.length}`);
      
      // Filtrar por tipo se especificado
      const filteredBaseFiles = fileTypes
        ? baseFiles.filter(f => fileTypes.includes(f.fileType))
        : baseFiles;

      filteredBaseFiles.forEach(file => {
        allFiles.push({
          ...file,
          fileYear: null,
          fileMonth: null,
        });
      });
    }

    // Buscar diretórios (anos/meses)
    const directories = await fetchDirectories(baseUrl);
    
    let dirsToProcess = directories;

    // FILTRO: Se latestOnly = true, processar apenas o mais recente
    if (latestOnly && directories.length > 0) {
      const latest = findLatestDirectory(directories);
      
      if (latest) {
        logger.info('discovery', `📅 Usando apenas o diretório mais recente: ${latest.name} (${latest.year}-${String(latest.month || 0).padStart(2, '0')})`);
        dirsToProcess = [latest];
      } else {
        logger.warn('discovery', 'Nenhum diretório válido encontrado');
        dirsToProcess = [];
      }
    }

    // FILTRO: Por ano/mês específicos (sobrescreve latestOnly)
    if (year || month) {
      logger.info('discovery', `🔍 Filtrando por ano=${year || 'qualquer'}, mês=${month || 'qualquer'}`);
      
      dirsToProcess = dirsToProcess.filter(dir => {
        const parsed = parseDirectoryName(dir.name);
        if (year && parsed.year !== year) return false;
        if (month && parsed.month !== month) return false;
        return true;
      });
      
      logger.info('discovery', `${dirsToProcess.length} diretórios após filtros`);
    }

    // Processar diretórios selecionados
    for (const dir of dirsToProcess) {
      const { year: dirYear, month: dirMonth } = parseDirectoryName(dir.name);
      
      // Buscar arquivos no diretório
      const dirFiles = await fetchFilesFromUrl(dir.url);
      
      // Filtrar por tipo se especificado
      const filteredDirFiles = fileTypes
        ? dirFiles.filter(f => fileTypes.includes(f.fileType))
        : dirFiles;

      filteredDirFiles.forEach(file => {
        allFiles.push({
          ...file,
          fileYear: dirYear,
          fileMonth: dirMonth,
        });
      });

      // Se o diretório parece ser um ano (4 dígitos), buscar subdiretórios de mês
      if (dirYear && !dirMonth) {
        logger.info('discovery', `Buscando subdiretórios de mês em ${dir.name}`);
        
        const subDirs = await fetchDirectories(dir.url);
        
        // Se latestOnly, pegar apenas o subdiretório mais recente
        const subDirsToProcess = latestOnly && subDirs.length > 0
          ? [findLatestDirectory(subDirs)].filter(Boolean)
          : subDirs;

        for (const subDir of subDirsToProcess) {
          const subParsed = parseDirectoryName(subDir.name);
          
          // Aplicar filtro de mês se especificado
          if (month && subParsed.month !== month) {
            continue;
          }
          
          logger.info('discovery', `📂 Processando subdiretório: ${subDir.name}`);
          
          const subFiles = await fetchFilesFromUrl(subDir.url);
          
          // Filtrar por tipo se especificado
          const filteredSubFiles = fileTypes
            ? subFiles.filter(f => fileTypes.includes(f.fileType))
            : subFiles;
          
          filteredSubFiles.forEach(file => {
            allFiles.push({
              ...file,
              fileYear: dirYear,
              fileMonth: subParsed.month || subParsed.year,
            });
          });
        }
      }
    }

    // Ordenar por tipo, ano, mês e sequência
    allFiles.sort((a, b) => {
      // Ordem de prioridade: municipios -> estabelecimentos -> socios
      const typeOrder = { municipios: 0, estabelecimentos: 1, socios: 2 };
      const typeCompare = (typeOrder[a.fileType] || 99) - (typeOrder[b.fileType] || 99);
      if (typeCompare !== 0) return typeCompare;

      const yearCompare = (a.fileYear || 0) - (b.fileYear || 0);
      if (yearCompare !== 0) return yearCompare;

      const monthCompare = (a.fileMonth || 0) - (b.fileMonth || 0);
      if (monthCompare !== 0) return monthCompare;

      return a.sequence - b.sequence;
    });

    logger.info('discovery', `✅ Descoberta completa: ${allFiles.length} arquivos encontrados`);
    
    // Agrupar por tipo
    const summary = allFiles.reduce((acc, file) => {
      acc[file.fileType] = (acc[file.fileType] || 0) + 1;
      return acc;
    }, {});
    
    logger.info('discovery', '📊 Resumo por tipo:', summary);

    // Log dos arquivos encontrados (primeiros 5 de cada tipo)
    ['municipios', 'estabelecimentos', 'socios'].forEach(type => {
      const filesOfType = allFiles.filter(f => f.fileType === type);
      if (filesOfType.length > 0) {
        logger.info('discovery', `📄 ${type.toUpperCase()}:`);
        filesOfType.slice(0, 5).forEach(f => {
          logger.info('discovery', `   - ${f.fileName} (${f.fileYear}-${String(f.fileMonth || 0).padStart(2, '0')})`);
        });
        if (filesOfType.length > 5) {
          logger.info('discovery', `   ... e mais ${filesOfType.length - 5} arquivos`);
        }
      }
    });

    return allFiles;
  } catch (error) {
    logger.error('discovery', 'Erro na descoberta de arquivos', error.message);
    throw error;
  }
}

export default { discoverFiles };
