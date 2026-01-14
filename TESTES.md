# Guia de Testes - ETL Receita Federal

## 🧪 Testes Disponíveis (do mais simples ao mais completo)

### 1. Testar Discovery (sem download)

```bash
npm run discovery
```

**O que faz:**
- Lista todos os arquivos disponíveis na Receita Federal
- Não baixa nada, apenas navega na estrutura
- **Tempo**: 5-10 segundos

**Resultado esperado:** Lista de 600+ arquivos encontrados

---

### 2. Teste Simples - 1 Arquivo de Municípios ⭐ RECOMENDADO

```bash
node test-municipios-simples.js
```

**O que faz:**
- Baixa apenas 1 arquivo (Municipios.zip - ~42KB)
- Testa o pipeline completo: download → unzip → parse → transform → load
- Valida conexão com banco
- Mostra estatísticas e amostra dos dados

**Tempo**: 10-30 segundos  
**Pré-requisitos:**
- PostgreSQL rodando
- Banco criado: `createdb etl_receita_federal`
- Schema criado: `psql -d etl_receita_federal -f sql/schema.sql`

**Por que começar com este:**
- ✅ Arquivo pequeno (rápido)
- ✅ Valida todo o pipeline
- ✅ Fácil de debugar se algo der errado
- ✅ Mostra resultados imediatamente

---

### 3. Teste com 1 Arquivo de Estabelecimentos

Depois que o teste de Municípios funcionar, crie:

```bash
node test-estabelecimento-simples.js
```

**Arquivo:** `test-estabelecimento-simples.js`

```javascript
#!/usr/bin/env node

import { downloadFile, getTempFilePath, cleanupFile } from './src/services/downloader.js';
import { processZipFile } from './src/services/processor.js';
import { transformEstabelecimento } from './src/transformers/estabelecimento.js';
import { pool, testConnection } from './src/config/database.js';

async function loadEstabelecimentosData(records) {
  const client = await pool.connect();
  const columns = [
    'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'cnpj',
    'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral',
    'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior',
    'codigo_pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
    'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf',
    'codigo_municipio', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax',
    'correio_eletronico', 'situacao_especial', 'data_situacao_especial',
  ];
  
  try {
    const batchSize = 1000;
    let inserted = 0;
    
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      
      const placeholders = [];
      const values = [];
      let paramIndex = 1;

      batch.forEach(record => {
        const recordPlaceholders = [];
        columns.forEach(col => {
          recordPlaceholders.push(`$${paramIndex++}`);
          values.push(record[col]);
        });
        placeholders.push(`(${recordPlaceholders.join(', ')})`);
      });

      const query = `INSERT INTO estabelecimentos (${columns.join(', ')}) 
                     VALUES ${placeholders.join(', ')} 
                     ON CONFLICT (cnpj) DO NOTHING`;

      const result = await client.query(query, values);
      inserted += result.rowCount;
      console.log(`   ✅ Batch: ${result.rowCount} estabelecimentos`);
    }
    
    return inserted;
  } finally {
    client.release();
  }
}

async function main() {
  console.log('\n' + '='.repeat(70));
  console.log('🧪 TESTE - 1 Arquivo de Estabelecimentos');
  console.log('='.repeat(70) + '\n');

  const arquivo = {
    nome: 'Estabelecimentos0.zip',
    url: 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-12/Estabelecimentos0.zip'
  };
  
  console.log(`📦 Arquivo: ${arquivo.nome} (~1.8GB descompactado)`);
  console.log(`⚠️  Este teste vai demorar mais (5-15 min dependendo da conexão)`);
  console.log();

  try {
    console.log('1️⃣  Verificando conexão...');
    const dbTest = await testConnection();
    if (!dbTest.success) throw new Error(`Falha no banco: ${dbTest.error}`);
    console.log('   ✅ Conectado\n');

    console.log('2️⃣  Baixando arquivo...');
    const tempPath = getTempFilePath(arquivo.nome);
    await downloadFile(arquivo.url, tempPath);
    console.log('   ✅ Download concluído\n');

    console.log('3️⃣  Processando arquivo...');
    let totalCarregados = 0;
    
    const resultado = await processZipFile(
      tempPath,
      transformEstabelecimento,
      async (records) => {
        const carregados = await loadEstabelecimentosData(records);
        totalCarregados += carregados;
      }
    );
    
    console.log('   ✅ Processamento concluído\n');

    console.log('4️⃣  Limpando...');
    cleanupFile(tempPath);
    console.log('   ✅ Arquivo removido\n');

    console.log('5️⃣  Verificando dados...');
    const client = await pool.connect();
    try {
      const countResult = await client.query('SELECT COUNT(*) FROM estabelecimentos');
      console.log(`   📊 Total: ${countResult.rows[0].count} estabelecimentos\n`);
      
      const sampleResult = await client.query('SELECT cnpj, nome_fantasia, uf FROM estabelecimentos LIMIT 5');
      console.log('   📋 Amostra:');
      sampleResult.rows.forEach(e => {
        console.log(`      ${e.cnpj} - ${e.nome_fantasia || '(sem nome fantasia)'} - ${e.uf}`);
      });
    } finally {
      client.release();
    }

    console.log('\n' + '='.repeat(70));
    console.log('✅ TESTE CONCLUÍDO!');
    console.log('='.repeat(70));
    console.log(`\n📊 Estatísticas:`);
    console.log(`   • Parseados: ${resultado.totalRecords}`);
    console.log(`   • Carregados: ${totalCarregados}`);
    console.log(`   • Erros: ${resultado.errors}\n`);

    process.exit(0);
  } catch (error) {
    console.error('\n❌ ERRO:', error.message, '\n');
    process.exit(1);
  }
}

main();
```

**Tempo**: 5-15 minutos  
**Tamanho**: ~320MB compactado, ~1.8GB descompactado

---

### 4. FULL LOAD Completo

Quando tudo estiver funcionando:

```bash
npm run full-load -- --yes
```

**O que faz:**
- Processa TODOS os 693 arquivos
- Trunca tabelas e recarrega tudo
- **Tempo estimado: 10-15 horas**

⚠️ **ATENÇÃO:** Só rode isso quando tiver certeza que tudo funciona!

---

## 📋 Checklist Antes de Rodar os Testes

### Pré-requisitos básicos:

```bash
# 1. Node.js instalado
node --version  # deve ser 18+

# 2. PostgreSQL instalado e rodando
sudo systemctl status postgresql

# 3. Dependências instaladas
npm install

# 4. Variáveis de ambiente configuradas
cat .env  # verificar se está correto
```

### Setup do banco:

```bash
# 1. Criar banco
createdb etl_receita_federal

# 2. Criar schema (tabelas)
psql -d etl_receita_federal -f sql/schema.sql

# 3. Verificar tabelas criadas
psql -d etl_receita_federal -c "\dt"

# Deve mostrar:
# - etl_control_files
# - etl_control_runs
# - municipios
# - estabelecimentos
# - socios
```

### Configurar conexão no .env:

```env
DB_HOST=localhost          # ou IP da VPS
DB_PORT=5432
DB_NAME=etl_receita_federal
DB_USER=postgres           # seu usuário
DB_PASSWORD=sua_senha      # sua senha
```

---

## 🐛 Troubleshooting

### Erro de conexão com banco

```bash
# Verificar se PostgreSQL está rodando
sudo systemctl status postgresql

# Testar conexão manualmente
psql -h localhost -U postgres -d etl_receita_federal -c "SELECT 1"
```

### Tabelas não existem

```bash
# Criar schema
psql -d etl_receita_federal -f sql/schema.sql
```

### Erro de timeout no download

O timeout está configurado para 10 minutos (600000ms). Se sua conexão for lenta, aumente em `.env`:

```env
DOWNLOAD_TIMEOUT=1200000  # 20 minutos
```

### Arquivos temporários acumulando

```bash
# Limpar pasta temp
rm -rf temp/*
```

---

## 📊 Ordem Recomendada

1. ✅ **Discovery** - Validar que consegue acessar a Receita Federal
2. ✅ **Teste Simples (Municípios)** - Validar pipeline completo
3. ✅ **Teste Estabelecimento** - Validar com arquivo grande
4. ✅ **FULL LOAD** - Carga completa (quando tudo estiver OK)

---

## 💡 Dicas

- **Use screen/tmux** para processos longos
- **Monitore logs**: `tail -f logs/etl-*.log`
- **Verifique espaço em disco**: `df -h`
- **Se falhar, pode retomar**: basta rodar novamente, ele continua de onde parou

---

## 🎯 Resultado Esperado do Teste Simples

Quando rodar `node test-municipios-simples.js`, deve ver algo assim:

```
======================================================================
🧪 TESTE SIMPLES - 1 Arquivo de Municípios
======================================================================

📦 Arquivo: Municipios.zip
🔗 URL: https://arquivos.receitafederal.gov.br/...

1️⃣  Testando conexão com PostgreSQL...
   ✅ Conectado ao banco (2026-01-14...)

2️⃣  Verificando tabela municipios...
   ✅ Tabela municipios existe

3️⃣  Baixando arquivo...
   📥 Destino: ./temp/Municipios.zip
   ✅ Download concluído

4️⃣  Processando arquivo ZIP...
   ✅ Batch processado: 5570 municípios inseridos/atualizados
   ✅ Processamento concluído

5️⃣  Limpando arquivo temporário...
   ✅ Arquivo removido

6️⃣  Verificando dados carregados...
   📊 Total de municípios no banco: 5570

   📋 Amostra (primeiros 5):
      1100015 - ALTA FLORESTA D'OESTE
      1100023 - ARIQUEMES
      1100031 - CABIXI
      1100049 - CACOAL
      1100056 - CEREJEIRAS

======================================================================
✅ TESTE CONCLUÍDO COM SUCESSO!
======================================================================

📊 Estatísticas:
   • Registros parseados: 5570
   • Registros carregados: 5570
   • Erros de parsing: 0

💡 Próximo passo: Teste com arquivo de Estabelecimentos
```

Se você ver isso, significa que **TUDO ESTÁ FUNCIONANDO!** 🎉
