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

    // Buscar links que terminam com .zip
    $('a').each((i, element) => {
      const href = $(element).attr('href');
      if (href && href.endsWith('.zip')) {
        // Extrair apenas o nome do arquivo do href
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
          });
        }
      }
    });

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
 * Descobre todos os arquivos disponíveis
 */
export async function discoverFiles(baseUrl = CONFIG.BASE_URL) {
  logger.info('discovery', 'Iniciando descoberta de arquivos');
  const allFiles = [];

  try {
    // Primeiro, tentar buscar arquivos diretamente na URL base
    const baseFiles = await fetchFilesFromUrl(baseUrl);
    
    if (baseFiles.length > 0) {
      // Arquivos encontrados diretamente na raiz
      baseFiles.forEach(file => {
        allFiles.push({
          ...file,
          fileYear: null,
          fileMonth: null,
        });
      });
    }

    // Buscar diretórios (anos/meses)
    const directories = await fetchDirectories(baseUrl);
    
    for (const dir of directories) {
      const { year, month } = parseDirectoryName(dir.name);
      
      // Buscar arquivos no diretório
      const dirFiles = await fetchFilesFromUrl(dir.url);
      
      dirFiles.forEach(file => {
        allFiles.push({
          ...file,
          fileYear: year,
          fileMonth: month,
        });
      });

      // Se o diretório parece ser um ano (4 dígitos), buscar subdiretórios de mês
      if (year && !month) {
        const subDirs = await fetchDirectories(dir.url);
        
        for (const subDir of subDirs) {
          const subParsed = parseDirectoryName(subDir.name);
          const subFiles = await fetchFilesFromUrl(subDir.url);
          
          subFiles.forEach(file => {
            allFiles.push({
              ...file,
              fileYear: year,
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

    logger.info('discovery', `Descoberta completa: ${allFiles.length} arquivos encontrados`);
    
    // Agrupar por tipo
    const summary = allFiles.reduce((acc, file) => {
      acc[file.fileType] = (acc[file.fileType] || 0) + 1;
      return acc;
    }, {});
    
    logger.info('discovery', 'Resumo por tipo:', summary);

    return allFiles;
  } catch (error) {
    logger.error('discovery', 'Erro na descoberta de arquivos', error.message);
    throw error;
  }
}

export default { discoverFiles };
