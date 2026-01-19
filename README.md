# 🏛️ ETL Receita Federal - Dados Abertos CNPJ

Sistema ETL robusto e profissional para processar dumps públicos de CNPJ da Receita Federal do Brasil.

**Status:** ✅ Produção | **Versão:** 1.0.0 | **Última carga:** 96M+ registros

## ✨ Características

- **🔄 Processamento Streaming**: Arquivos processados via streaming, otimizado para não causar OOM
- **💾 Checkpoint System**: Salva progresso a cada 30 segundos, permite retomar de onde parou
- **🛡️ Proteção Anti-Loop**: Previne perda de dados em redeploys acidentais (janela de 2 horas)
- **🧹 Sanitização UTF-8**: Remove bytes nulos (0x00) e caracteres inválidos automaticamente
- **📦 FULL LOAD e DELTA**: Suporte para carga inicial completa (TRUNCATE) e incremental
- **🔐 Graceful Shutdown**: Shutdown elegante com salvamento de checkpoint (SIGTERM/SIGINT)
- **📊 Logging Estruturado**: Logs detalhados com timestamps, categorias e rotação diária
- **🚀 Docker Ready**: Totalmente containerizado, testado em produção com Coolify

## 📋 Requisitos

- **Node.js** 20+ com ES modules
- **PostgreSQL** 17+ (testado com 17.7)
- **Disco:** 20GB para temp + 5GB para logs
- **Memória:** 512MB+ (otimizado para baixo consumo)
- **Rede:** Conexão estável (downloads de ~20GB total)

## 🚀 Setup Inicial

### 1️⃣ Instalação Local

```bash
# Clonar repositório
git clone https://github.com/seu-usuario/etl-receita-federal.git
cd etl-receita-federal

# Instalar dependências
npm install

# Configurar variáveis de ambiente
cp .env.example .env
# Edite o .env com suas credenciais PostgreSQL

# Criar banco e schema
bash sql/setup-banco.sh

# Aplicar migrations
npm run migrate

# Testar conexão
npm run test:conexao

# Executar primeira carga (FULL LOAD)
npm run full-load -- --yes
```

### 2️⃣ Deploy com Docker/Coolify

```bash
# 1. Configure no Coolify:
#    - Repository: seu-repo-git
#    - Build Pack: Dockerfile
#    - Porta: 3000 (opcional, para health check)

# 2. Configure Environment Variables:
DB_HOST=seu-host-postgres      # Use IP privado ou domínio interno
DB_PORT=5432                   # Porta padrão ou customizada
DB_NAME=etl_receita_federal
DB_USER=postgres
DB_PASSWORD=sua_senha_segura   # Use secrets do Coolify
MODE=manual  # IMPORTANTE: não executar automaticamente

# 3. Configure Persistent Storage:
/data/temp → 20GB (arquivos ZIP temporários)
/data/logs → 5GB (logs do sistema)

# 4. Configure no Coolify:
#    - Restart Policy: "no" ou "on-failure"
#    - NUNCA use "always" (pode causar loop infinito)

# 5. Deploy e aguarde container subir

# 6. Execute comandos via Coolify "Execute Command":
npm run full-load -- --yes
```

### 3️⃣ Configuração do Banco de Dados

O sistema precisa das seguintes tabelas (criadas automaticamente pelo setup-banco.sh):

**Tabelas de Dados:**

- `municipios` (5.570 registros)
- `estabelecimentos` (~50M registros)
- `socios` (~20M registros)

**Tabelas de Controle:**

- `etl_control_runs` (histórico de execuções)
- `etl_control_files` (controle e checkpoint por arquivo)

## ⚙️ Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
# ==============================================
# Database Configuration
# ==============================================
DB_HOST=localhost              # Host do PostgreSQL (ex: localhost ou IP da VPS)
DB_PORT=5432                   # Porta do PostgreSQL (padrão: 5432)
DB_NAME=etl_receita_federal    # Nome do banco
DB_USER=postgres               # Usuário
DB_PASSWORD=sua_senha_segura   # Senha forte (NUNCA commitar!)

# ==============================================
# ETL Configuration
# ==============================================
BASE_URL=https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/
BATCH_SIZE=1000               # Registros por batch INSERT
DOWNLOAD_TIMEOUT=600000       # Timeout de download (10 min)
TEMP_DIR=./temp               # Diretório temporário
LOG_DIR=./logs                # Diretório de logs

# ==============================================
# Optional - Test Mode
# ==============================================
TESTMODE=false                # true para modo de teste
```

**⚠️ Segurança:**

- NUNCA commite o arquivo `.env` no Git
- Use senhas fortes em produção
- Considere usar secrets do Coolify/Docker

## 💻 Comandos Disponíveis

### 📦 Operações Principais

| Comando                      | Descrição                                     | Tempo Estimado |
| ---------------------------- | --------------------------------------------- | -------------- |
| `npm run full-load -- --yes` | **Carga completa** (TRUNCATE + carga)         | 7-10 horas     |
| `npm run delta -- --yes`     | **Carga incremental** (apenas novos)          | 1-3 horas      |
| `npm run discovery`          | Lista arquivos disponíveis na Receita Federal | 10 segundos    |
| `npm run status`             | Mostra estado atual do ETL                    | 2 segundos     |

### 🔧 Utilitários de Manutenção

| Comando                   | Descrição                              | Uso               |
| ------------------------- | -------------------------------------- | ----------------- |
| `npm run limpar-controle` | Remove checkpoints e histórico de runs | Recomeçar do zero |
| `npm run limpar-temp`     | Remove arquivos ZIP temporários        | Liberar espaço    |
| `npm run migrate`         | Executa migrations pendentes           | Setup inicial     |

### 🧪 Testes e Validação

| Comando                         | Descrição                              | Tempo  |
| ------------------------------- | -------------------------------------- | ------ |
| `npm run test:conexao`          | Testa conexão com PostgreSQL           | 2 seg  |
| `npm run test:discovery`        | Testa discovery de arquivos            | 10 seg |
| `npm run test:municipios`       | Processa 1 arquivo de municípios       | 30 seg |
| `npm run test:estabelecimentos` | Processa 1 arquivo de estabelecimentos | 20 min |
| `npm run test:socios`           | Processa 1 arquivo de sócios           | 15 min |

### 🛠️ Scripts SQL e Banco

| Script                                 | Descrição                        | Quando usar        |
| -------------------------------------- | -------------------------------- | ------------------ |
| `bash sql/setup-banco.sh`              | Setup completo do banco          | Primeira vez       |
| `psql ... -f sql/schema.sql`           | Cria apenas tabelas              | Reconstruir schema |
| `psql ... -f sql/drop.sql`             | Remove todas as tabelas          | Reset completo     |
| `node sql/migrations/run-migration.js` | Executa uma migration específica | Aplicar mudanças   |

### 📊 Exemplos de Uso

```bash
# ============================================
# Primeira execução (setup completo)
# ============================================
npm install
bash sql/setup-banco.sh
npm run migrate
npm run test:conexao
npm run full-load -- --yes

# ============================================
# Execução diária (delta)
# ============================================
npm run delta -- --yes

# ============================================
# Verificar progresso durante execução
# ============================================
npm run status
tail -f logs/etl-$(date +%Y-%m-%d).log

# ============================================
# Recomeçar do zero
# ============================================
npm run limpar-controle
npm run limpar-temp
npm run full-load -- --yes

# ============================================
# Troubleshooting
# ============================================
npm run test:conexao         # Testa banco
npm run status               # Estado atual
tail -n 100 logs/etl-*.log   # Últimos logs
node tests/check-constraints.js  # Verifica constraints
```

### ⚠️ IMPORTANTE: FULL LOAD vs DELTA

#### FULL LOAD

```bash
npm run full-load -- --yes
```

**O que faz:**

- ✅ Descobre arquivos mais recentes na Receita Federal
- ⚠️ **TRUNCATE** em todas as tabelas (municipios, estabelecimentos, socios)
- ✅ Processa **TODOS** os arquivos encontrados (~21 arquivos)
- ✅ Carrega ~50 milhões de estabelecimentos + ~20M sócios + 5.570 municípios

**Quando usar:**

- Primeira execução do sistema
- Quando precisa reconstruir base completa
- Após alterações no schema do banco

**Proteções:**

- 🛡️ **Anti-loop:** Não executa se houver FULL LOAD concluído nas últimas 2 horas
- 🛡️ Requer flag `--yes` explícita para evitar execução acidental
- 🛡️ Em produção, considere usar `--force` apenas com extremo cuidado

**Tempo estimado:** 7-10 horas (depende da conexão e hardware)

#### DELTA LOAD

```bash
npm run delta -- --yes
```

**O que faz:**

- ✅ Descobre apenas arquivos **NOVOS** (não processados)
- ✅ **NÃO** faz TRUNCATE (mantém dados existentes)
- ✅ Usa `ON CONFLICT DO UPDATE` para atualizar registros
- ✅ Processa apenas diferenças desde última execução

**Quando usar:**

- Execução diária/mensal (automação)
- Atualizar base com dados mais recentes
- Manter dados sincronizados com Receita Federal

**Tempo estimado:** 1-3 horas (apenas arquivos novos)

#### Comparação

| Característica     | FULL LOAD         | DELTA LOAD      |
| ------------------ | ----------------- | --------------- |
| Trunca tabelas     | ✅ Sim            | ❌ Não          |
| Processa tudo      | ✅ Todos arquivos | 📁 Apenas novos |
| Tempo              | 7-10h             | 1-3h            |
| Uso em prod        | Raramente         | Diariamente     |
| Proteção anti-loop | ✅ 2 horas        | ❌ Não precisa  |

### 🔄 Checkpoint e Retomada

O sistema salva checkpoints automaticamente a cada 30 segundos com as seguintes informações:

- Arquivo sendo processado
- Linha atual
- Registros processados
- Timestamp

**Se o processo for interrompido:**

```bash
# Basta rodar novamente - retoma automaticamente do checkpoint
npm run full-load -- --yes

# O sistema detecta:
# - Arquivos com status 'processing'
# - Checkpoint salvo no JSONB
# - Continua de onde parou
```

**Para começar do zero (ignorar checkpoints):**

```bash
# 1. Limpar controle
npm run limpar-controle

# 2. Limpar arquivos temporários (opcional)
npm run limpar-temp

# 3. Rodar novamente
npm run full-load -- --yes
```

### 📅 Automação com Coolify

**⚠️ IMPORTANTE:** Este sistema **NÃO** usa cron. Configure a automação diretamente no Coolify.

**Configuração recomendada:**

1. **No Coolify** → Seu serviço → **Scheduled Tasks**
2. **Para DELTA diário às 3h da manhã:**

   ```
   Frequency: 0 3 * * *
   Command: npm run delta -- --yes
   ```

3. **Para FULL LOAD mensal (1º dia às 2h):**
   ```
   Frequency: 0 2 1 * *
   Command: npm run full-load -- --yes
   ```

**Exemplos de frequências:**

- `0 3 * * *` - Diariamente às 3h
- `0 2 1 * *` - Todo dia 1º às 2h
- `0 4 * * 0` - Domingos às 4h
- `0 */6 * * *` - A cada 6 horas

**Monitoramento:**

- Configure alertas no Coolify para falhas
- Verifique logs regularmente
- Use `npm run status` para validar execuções

## 📁 Estrutura do Projeto

> 📘 **Veja [STRUCTURE.md](STRUCTURE.md) para documentação detalhada da estrutura**

```
etl-receita-federal/
├── docker/                # Configurações Docker
│   └── entrypoint.sh
├── logs/                  # Logs da aplicação (persistente)
├── scripts/               # Scripts auxiliares de produção
│   ├── backup-db.sh
│   ├── health-check.sh
│   ├── run-delta.sh
│   └── test-resume.sh
├── sql/                   # Scripts SQL e migrations
│   ├── schema.sql
│   ├── drop.sql
│   ├── setup-banco.sh
│   └── migrations/        # Migrations SQL
│       ├── run-migration.js
│       └── migrate-*.sql
├── src/                   # Código-fonte principal
│   ├── cli/              # Interfaces CLI
│   ├── config/           # Configurações
│   ├── services/         # Serviços principais
│   ├── transformers/     # Transformadores de dados
│   ├── utils/            # Utilitários internos
│   └── orchestrator.js   # Orquestrador principal
├── temp/                  # Downloads temporários (persistente)
├── tests/                 # Scripts de teste
│   ├── test-*.js
│   └── run-test-*.sh
├── utils/                 # Utilitários de manutenção
│   ├── limpar-*.js
│   └── verificar-estado.js
├── .env                   # Variáveis de ambiente
├── Dockerfile             # Build do container
└── package.json           # Dependencies
```

## 🏗️ Arquitetura

### Fluxo de Processamento

```
┌─────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR                            │
│                    (Coordenação Central)                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                ┌────────────┼────────────┐
                │            │            │
                ▼            ▼            ▼
    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
    │  DISCOVERY   │ │  DOWNLOADER  │ │   CONTROL    │
    │   SERVICE    │ │   SERVICE    │ │   SERVICE    │
    └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
           │                │                │
           │                │                │
           ▼                ▼                ▼
    Lista arquivos    Download ZIP    Checkpoint/Resume
    da Receita Fed    (com retry)     Estado do ETL
                             │
                             ▼
                    ┌──────────────┐
                    │  PROCESSOR   │
                    │   SERVICE    │
                    │ (Streaming)  │
                    └──────┬───────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
            ▼              ▼              ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │TRANSFORMER  │ │TRANSFORMER  │ │TRANSFORMER  │
    │ Municipio   │ │Estabelec.   │ │   Socio     │
    └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
           │               │               │
           └───────────────┼───────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │   SANITIZER     │
                  │ (Remove \0)     │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  BATCH LOADER   │
                  │  (PostgreSQL)   │
                  └─────────────────┘
```

### Componentes Principais

1. **Discovery Service** (`src/services/discovery.js`)
   - Descobre arquivos disponíveis na Receita Federal
   - Filtra por data/tipo
   - Retorna lista ordenada

2. **File Downloader** (`src/services/downloader.js`)
   - Download streaming (não carrega na memória)
   - Retry automático (8 tentativas com backoff exponencial)
   - Validação de integridade (tamanho > 1KB)

3. **Stream Processor** (`src/services/processor.js`)
   - Descompacta ZIP via streaming (yauzl)
   - Parseia CSV linha por linha
   - Checkpoint a cada 30 segundos

4. **Data Transformers** (`src/transformers/*.js`)
   - Transforma dados brutos em formato do banco
   - Validação de campos obrigatórios
   - Parsing de datas (YYYYMMDD)

5. **Sanitizer** (`src/utils/sanitize.js`)
   - Remove bytes nulos (`\0`)
   - Remove caracteres de controle inválidos
   - Preserva `\n`, `\r`, `\t`

6. **Batch Loader** (`src/services/loader.js`)
   - Carrega dados em batches (1000 registros)
   - ON CONFLICT DO NOTHING (evita duplicatas)
   - Suporte para UPSERT (DELTA mode)

7. **Control Service** (`src/services/control.js`)
   - Gerencia runs e arquivos
   - Salva/recupera checkpoints (JSONB)
   - Filtra arquivos por run_id

8. **Orchestrator** (`src/orchestrator.js`)
   - Coordena todo o fluxo
   - Graceful shutdown (SIGTERM/SIGINT)
   - Proteção anti-loop (verifica runs recentes)

## 🗄️ Schema do Banco de Dados

### Tabelas de Dados

```sql
-- ============================================
-- Municípios (5.570 registros)
-- ============================================
CREATE TABLE municipios (
  codigo_municipio INTEGER PRIMARY KEY,
  nome_municipio VARCHAR(200) NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- Estabelecimentos (~50M registros)
-- ============================================
CREATE TABLE estabelecimentos (
  id SERIAL PRIMARY KEY,
  cnpj_basico VARCHAR(8) NOT NULL,
  cnpj_ordem VARCHAR(4) NOT NULL,
  cnpj_dv VARCHAR(2) NOT NULL,
  cnpj VARCHAR(14) NOT NULL UNIQUE,
  identificador_matriz_filial INTEGER,
  nome_fantasia VARCHAR(500),
  situacao_cadastral INTEGER,
  data_situacao_cadastral DATE,
  codigo_municipio INTEGER REFERENCES municipios(codigo_municipio),
  -- ... 31 campos total (ver sql/schema.sql)
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_estabelecimentos_cnpj_parts
ON estabelecimentos(cnpj_basico, cnpj_ordem, cnpj_dv);

-- ============================================
-- Sócios (~20M registros)
-- ============================================
CREATE TABLE socios (
  id SERIAL PRIMARY KEY,
  cnpj_basico VARCHAR(8) NOT NULL,
  identificador_socio INTEGER NOT NULL,
  nome_socio VARCHAR(500),
  cpf_cnpj_socio VARCHAR(14),
  qualificacao_socio INTEGER,
  data_entrada_sociedade DATE,
  -- ... 11 campos total (ver sql/schema.sql)
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_socios_unique
ON socios(cnpj_basico, identificador_socio, cpf_cnpj_socio);
```

### Tabelas de Controle

```sql
-- ============================================
-- Controle de Runs (execuções do ETL)
-- ============================================
CREATE TABLE etl_control_runs (
  id SERIAL PRIMARY KEY,
  run_type VARCHAR(10) NOT NULL,           -- 'FULL' ou 'DELTA'
  status VARCHAR(20) NOT NULL,             -- 'running', 'completed', 'error', 'interrupted'
  total_files INTEGER DEFAULT 0,
  files_completed INTEGER DEFAULT 0,
  files_failed INTEGER DEFAULT 0,
  started_at TIMESTAMP DEFAULT NOW(),
  completed_at TIMESTAMP
);

-- ============================================
-- Controle de Arquivos (com checkpoint)
-- ============================================
CREATE TABLE etl_control_files (
  id SERIAL PRIMARY KEY,
  file_name VARCHAR(255) UNIQUE NOT NULL,
  file_url TEXT NOT NULL,
  file_type VARCHAR(50) NOT NULL,         -- 'municipios', 'estabelecimentos', 'socios'
  run_id INTEGER REFERENCES etl_control_runs(id),
  status VARCHAR(20) DEFAULT 'pending',    -- 'pending', 'processing', 'done', 'error'
  checkpoint JSONB,                        -- { linesProcessed: 123456, completed: false }
  records_inserted INTEGER DEFAULT 0,
  error_message TEXT,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_etl_files_status ON etl_control_files(status);
CREATE INDEX idx_etl_files_run_id ON etl_control_files(run_id);
```

### Exemplo de Checkpoint JSONB

```json
{
  "linesProcessed": 1250000,
  "csvFileName": "Estabelecimentos0",
  "completed": false,
  "lastCheckpoint": "2026-01-19T15:30:45.123Z"
}
```

## 📈 Performance e Estatísticas

### Tempos de Execução (Produção)

| Operação                     | Arquivos | Registros | Tempo    | Observações                     |
| ---------------------------- | -------- | --------- | -------- | ------------------------------- |
| **FULL LOAD**                | 21       | ~96M      | 7-10h    | Inclui download + processamento |
| **DELTA**                    | 3-5      | ~15M      | 1-3h     | Apenas arquivos novos           |
| Municipios                   | 1        | 5.570     | 30s      | Arquivo pequeno                 |
| Estabelecimentos (1 arquivo) | 1        | ~4-5M     | 30-60min | Arquivo maior                   |
| Socios (1 arquivo)           | 1        | ~2-3M     | 15-30min | 11 colunas                      |

### Configurações Otimizadas

```env
# Configurações testadas em produção
BATCH_SIZE=500            # Ideal para 11-31 colunas
DOWNLOAD_TIMEOUT=600000   # 10 minutos
MAX_RETRIES=8             # Download retry
```

### Otimizações Implementadas

✅ **Streaming completo** - Nenhum arquivo carregado na memória  
✅ **Batch INSERT** - 500 registros por query  
✅ **Backpressure control** - Pausa stream quando batch está processando  
✅ **Connection reuse** - Uma conexão por arquivo  
✅ **ON CONFLICT DO NOTHING** - Evita duplicatas sem erro  
✅ **Checkpoint autosave** - A cada 30 segundos  
✅ **Graceful shutdown** - Salva estado no SIGTERM/SIGINT  
✅ **Memory cleanup** - Arrays destruídos após cada batch

## 📝 Logging e Monitoramento

### Estrutura de Logs

```
logs/
├── etl-2026-01-19.log       # Log do dia atual
├── etl-2026-01-18.log       # Logs por dia
├── etl-2026-01-17.log
└── ...
```

### Categorias de Log

```javascript
// Formato dos logs
[TIMESTAMP] [CATEGORIA] [NÍVEL] Mensagem

// Exemplos:
[2026-01-19T15:30:45.123Z] [orchestrator] [INFO] Iniciando FULL LOAD
[2026-01-19T15:30:50.456Z] [discovery] [INFO] 21 arquivos descobertos
[2026-01-19T15:31:00.789Z] [processor] [INFO] Processando Municipios.zip
[2026-01-19T15:35:20.012Z] [loader] [ERROR] Erro ao inserir batch: duplicate key
```

### Monitoramento em Tempo Real

```bash
# Ver logs em tempo real
tail -f logs/etl-$(date +%Y-%m-%d).log

# Filtrar apenas erros
tail -f logs/etl-*.log | grep ERROR

# Ver progresso do processamento
watch -n 5 "npm run status"

# Contar registros processados
psql -c "SELECT
  (SELECT COUNT(*) FROM municipios) as municipios,
  (SELECT COUNT(*) FROM estabelecimentos) as estabelecimentos,
  (SELECT COUNT(*) FROM socios) as socios;"
```

### Alertas Recomendados

Configure alertas no Coolify para:

- ❌ Status 'error' em etl_control_files
- ⏰ Runs com duração > 12 horas
- 💾 Disco com < 5GB livre
- 📉 Progresso parado há > 30 minutos

## 🔧 Troubleshooting

### Problemas Comuns

#### 1. "invalid byte sequence for encoding UTF8: 0x00"

**Causa:** Dados brutos contêm bytes nulos  
**Solução:** Já implementado! Sanitização automática em `src/utils/sanitize.js`

```bash
# Verificar se sanitização está ativa
grep "sanitizeString" src/transformers/*.js
```

#### 2. "Timeout downloading file"

**Causa:** Arquivo muito grande ou conexão lenta  
**Solução:** Aumentar timeout no .env

```env
DOWNLOAD_TIMEOUT=900000  # 15 minutos
```

#### 3. "ECONNREFUSED connecting to database"

**Causa:** PostgreSQL não acessível  
**Solução:** Verificar credenciais e firewall

```bash
# Testar conexão
npm run test:conexao

# Testar direto com psql
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT NOW();"
```

#### 4. Run travado em "processing"

**Causa:** Processo interrompido sem salvar estado  
**Solução:** Atualizar status manualmente

```sql
-- Ver runs travados
SELECT * FROM etl_control_runs WHERE status = 'running';

-- Atualizar status
UPDATE etl_control_runs
SET status = 'interrupted', completed_at = NOW()
WHERE id = <run_id>;
```

#### 5. Disco cheio durante processamento

**Causa:** Muitos ZIPs no /temp  
**Solução:** Limpar temporários

```bash
npm run limpar-temp
# ou manualmente
rm -rf temp/*.zip
```

### Comandos de Diagnóstico

```bash
# ============================================
# Verificar estado geral
# ============================================
npm run status

# ============================================
# Verificar últimos 100 logs
# ============================================
tail -n 100 logs/etl-$(date +%Y-%m-%d).log

# ============================================
# Verificar constraints do banco
# ============================================
node tests/check-constraints.js

# ============================================
# Verificar espaço em disco
# ============================================
df -h temp/
df -h logs/

# ============================================
# Verificar conexões PostgreSQL
# ============================================
psql -c "SELECT count(*) as connections
FROM pg_stat_activity
WHERE datname = 'postgres';"

# ============================================
# Verificar tabelas e registros
# ============================================
psql -c "SELECT
  'municipios' as tabela, COUNT(*) FROM municipios
UNION ALL SELECT 'estabelecimentos', COUNT(*) FROM estabelecimentos
UNION ALL SELECT 'socios', COUNT(*) FROM socios;"
```

### Recovery Procedures

**Cenário 1: Processo morreu no meio do FULL LOAD**

```bash
# 1. Verificar status
npm run status

# 2. Retomar (continuará do checkpoint)
npm run full-load -- --yes

# 3. Se quiser recomeçar do zero
npm run limpar-controle
npm run limpar-temp
npm run full-load -- --yes
```

**Cenário 2: Dados corrompidos, precisa recarregar tudo**

```bash
# 1. Limpar TUDO (CUIDADO!)
npm run limpar-controle
psql -f sql/drop.sql       # Remove tabelas
psql -f sql/schema.sql     # Recria tabelas
npm run migrate            # Aplica migrations

# 2. Reprocessar
npm run full-load -- --yes
```

**Cenário 3: Apenas um arquivo falhou**

```sql
-- 1. Verificar qual arquivo
SELECT * FROM etl_control_files WHERE status = 'error';

-- 2. Resetar arquivo específico
UPDATE etl_control_files
SET status = 'pending',
    checkpoint = NULL,
    error_message = NULL
WHERE file_name = 'Estabelecimentos5.zip';

-- 3. Rodar novamente (processará apenas pendentes)
npm run full-load -- --yes
```

├── etl-2026-01-17.log # Log dia anterior
├── cron-delta.log # Logs do cron DELTA
└── backup.log # Logs de backup

```

### Formato de Log

```

[2026-01-18 01:32:15] INFO | orchestrator | Iniciando ETL - Tipo: FULL
[2026-01-18 01:32:20] INFO | discovery | 21 arquivos encontrados
[2026-01-18 01:32:25] INFO | downloader | ⬇️ Baixando: Municipios.zip
[2026-01-18 01:33:00] INFO | processor | 📋 Retomando do checkpoint: linha 793,000
[2026-01-18 01:45:00] INFO | orchestrator | ✅ Arquivo concluído: 5,572 registros

````

### Monitorar Logs em Tempo Real

```bash
# Últimas 100 linhas
tail -n 100 logs/etl-$(date +%Y-%m-%d).log

# Follow (tempo real)
tail -f logs/etl-$(date +%Y-%m-%d).log

# Filtrar apenas erros
grep "ERROR" logs/etl-$(date +%Y-%m-%d).log

# Buscar por arquivo específico
grep "Estabelecimentos0" logs/etl-$(date +%Y-%m-%d).log
````

## 🛠️ Troubleshooting

### ❌ Erro: "invalid byte sequence for encoding UTF8: 0x00"

**Causa:** Dados da Receita Federal contêm bytes nulos  
**Solução:** Já implementada via `src/utils/sanitize.js` - remove automaticamente

### ❌ Erro: "column load_type does not exist"

**Causa:** Nome incorreto da coluna  
**Solução:** Use `run_type` ao invés de `load_type`

### ❌ Erro: "End of central directory record signature not found"

**Causa:** Arquivo ZIP corrompido/incompleto  
**Solução:**

```bash
node limpar-temp.js  # Remove ZIPs corrompidos
npm run full-load -- --yes  # Re-download automático
```

### ⚠️ ETL rodou novamente após deploy e perdeu dados

**Causa:** Restart automático do container executando FULL LOAD  
**Solução:**

1. Configure `MODE=manual` no .env
2. Configure Restart Policy como `no` ou `on-failure` no Coolify
3. Use proteção anti-loop já implementada (2 horas)

### 🔍 Verificar arquivos com erro

```bash
# Via CLI
npm run status

# Via SQL
psql -d postgres -c "
  SELECT file_name, error_message, started_at
  FROM etl_control_files
  WHERE status = 'error'
  ORDER BY started_at DESC;
"
```

### 🔄 Reprocessar arquivos com erro

```bash
# Marcar como pending
psql -d postgres -c "
  UPDATE etl_control_files
  SET status = 'pending', error_message = NULL
  WHERE status = 'error';
"

# Executar novamente
npm run full-load -- --yes
```

### 🗑️ Limpar tudo e recomeçar do zero

```bash
# 1. Limpar controle (checkpoints e runs)
node limpar-controle.js

# 2. Limpar arquivos temporários
node limpar-temp.js

# 3. Limpar dados (opcional - apenas se quiser truncar)
node limpar-estabelecimentos.js
node limpar-socios.js

# 4. Executar FULL LOAD
npm run full-load -- --yes
```

### 📊 Verificar progresso em tempo real

```bash
# Via comando
npm run status

# Via SQL
psql -d postgres -c "
  SELECT
    r.id as run_id,
    r.run_type,
    r.status as run_status,
    r.total_files,
    r.files_completed,
    r.files_failed,
    r.started_at,
    COUNT(f.id) FILTER (WHERE f.status = 'done') as arquivos_concluidos,
    COUNT(f.id) FILTER (WHERE f.status = 'processing') as arquivos_processando,
    COUNT(f.id) FILTER (WHERE f.status = 'error') as arquivos_erro
  FROM etl_control_runs r
  LEFT JOIN etl_control_files f ON f.run_id = r.id
  WHERE r.status = 'running'
  GROUP BY r.id
  ORDER BY r.started_at DESC
  LIMIT 1;
"
```

### 🔌 Erro de conexão ao banco

```bash
# Verificar se PostgreSQL está acessível (use valores do seu .env)
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME

# Testar conexão via Node.js
node test-conexao.js
```

### 💾 Checkpoint não está salvando

**Verificar:**

1. Coluna `checkpoint` existe na tabela `etl_control_files`?

```bash
psql -d postgres -c "\d etl_control_files"
```

2. Aplicar migration se necessário:

```bash
psql -d postgres -f sql/migrate-add-checkpoint.sql
```

## 🐳 Deploy com Docker/Coolify

### Pré-requisitos

1. PostgreSQL configurado e acessível
2. Persistent Storage configurado (20GB temp + 5GB logs)
3. Variáveis de ambiente configuradas

### Passo a Passo no Coolify

#### 1. Criar Aplicação

- **Type:** Application
- **Source:** Git Repository
- **Build Pack:** Dockerfile
- **Branch:** main

#### 2. Configurar Environment Variables

```env
MODE=manual
DB_HOST=seu-host-postgres      # IP privado ou domínio interno
DB_PORT=5432                   # Porta padrão (ajustar se necessário)
DB_NAME=etl_receita_federal
DB_USER=postgres
DB_PASSWORD=${DB_PASSWORD}     # Use secrets do Coolify
BASE_URL=https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/
TEMP_DIR=/data/temp
LOG_DIR=/data/logs
```

#### 3. Configurar Persistent Storage

| Source Path  | Destination  | Size |
| ------------ | ------------ | ---- |
| `/data/temp` | `/data/temp` | 20GB |
| `/data/logs` | `/data/logs` | 5GB  |

#### 4. Configurar Restart Policy

- **Restart Policy:** `no` ou `on-failure`
- ❌ **NUNCA use:** `always` (causa loop infinito)

#### 5. Deploy

Clique em **Deploy** e aguarde build completar.

#### 6. Executar ETL Manualmente

Após deploy, acesse **Execute Command** no Coolify:

```bash
# Primeira execução (FULL LOAD)
npm run full-load -- --yes

# Verificar status
npm run status

# Próximas execuções (DELTA)
npm run delta -- --yes
```

### Manutenção Pós-Deploy

```bash
# Limpar antes de nova execução
node limpar-controle.js
node limpar-temp.js

# Verificar logs
tail -f /data/logs/etl-$(date +%Y-%m-%d).log

# Backup do banco (executar fora do container)
pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME > backup-$(date +%Y%m%d).sql
```

### Proteções Implementadas

✅ **Anti-Loop:** Não executa FULL LOAD se houver run concluído nas últimas 2h  
✅ **Graceful Shutdown:** SIGTERM/SIGINT salvam checkpoint antes de sair  
✅ **Checkpoint Automático:** Salva progresso a cada 30 segundos  
✅ **File Integrity:** Valida arquivos antes de reusar  
✅ **Auto-Cleanup:** Remove ZIPs corrompidos automaticamente

## 🔐 Segurança

### Variáveis Sensíveis

**NUNCA commite:**

- `.env` (arquivo de ambiente local)
- Senhas de banco de dados
- URLs com credenciais

**Use:**

- `.env.example` como template (sem valores reais)
- Variables de ambiente no Coolify/Docker
- Secrets management em produção

### Conexão PostgreSQL

```env
# ✅ Recomendado: URL sem credenciais hardcoded
DATABASE_URL=postgres://user:pass@host:port/db

# ✅ Ou variáveis separadas
DB_HOST=seu-host-postgres      # Ex: localhost, IP privado ou domínio
DB_PORT=5432                   # Porta padrão (ajustar se necessário)
DB_USER=postgres
DB_PASSWORD=${POSTGRES_PASSWORD}  # Injetado via secrets
```

### Firewall

- PostgreSQL: Permitir apenas IPs do servidor ETL
- SSH: Usar chaves ao invés de senha
- Coolify: Configurar SSL/TLS

## 🧪 Testes

### Testes Disponíveis

```bash
# Testar conexão com banco
node test-conexao.js

# Testar discovery de arquivos
node test-discovery.js

# Testar processamento de Municipios
node test-municipios-simples.js

# Testar processamento de Estabelecimentos
node test-estabelecimentos-simples.js

# Testar processamento de Socios
node test-socios-simples.js
```

### Testes Manuais

```bash
# Processar apenas Municipios (rápido - ~5 min)
npm run test:municipios

# Verificar constraints do banco
node check-constraints.js

# Testar retomada de checkpoint
./scripts/test-resume.sh
```

## � Segurança e Boas Práticas

### Proteção de Credenciais

```bash
# ✅ CORRETO: Use .env e NUNCA commite
cat .env.example  # Template público
cat .env          # Valores reais (em .gitignore)

# ❌ ERRADO: Hardcoded no código
const password = 'minha_senha_123';  // NUNCA faça isso!
```

### Configuração Segura no Coolify

```env
# Use o recurso de Environment Variables do Coolify
DB_PASSWORD=${COOLIFY_SECRET_DB_PASSWORD}

# Ou defina como "Secret" no painel
# Coolify → Environment → Add Secret
```

### Backup e Disaster Recovery

```bash
# Backup automático (configure no Coolify)
# Frequency: 0 3 * * *  (diário às 3h)
pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME | \
  gzip > /data/backups/backup-$(date +\%Y\%m\%d).sql.gz

# Manter apenas últimos 30 dias
find /data/backups -name "*.sql.gz" -mtime +30 -delete

# Restore de backup
gunzip -c backup-20260119.sql.gz | \
  psql -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME
```

## 🏆 Melhores Práticas

### ✅ DO (Faça)

- ✅ Use `npm run status` frequentemente para monitorar
- ✅ Configure alertas no Coolify para falhas
- ✅ Faça backup antes de FULL LOAD
- ✅ Teste em ambiente de staging primeiro
- ✅ Use DELTA para atualizações incrementais
- ✅ Monitore logs durante execução
- ✅ Documente mudanças no schema

### ❌ DON'T (Não faça)

- ❌ Nunca use restart policy "always" no Coolify
- ❌ Não execute FULL LOAD sem backup
- ❌ Não ignore mensagens de erro nos logs
- ❌ Não modifique schema sem migration
- ❌ Não commite senhas ou .env no Git
- ❌ Não execute FULL LOAD em horário de pico

## 📚 Referências

### Documentação Oficial

- **Dados Abertos CNPJ:** https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj
- **Receita Federal:** https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj
- **Layout dos arquivos:** https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf

### Dependências Principais

```json
{
  "axios": "^1.6.5", // HTTP client
  "cheerio": "^1.0.0-rc.12", // Parse HTML
  "csv-parser": "^3.0.0", // Parse CSV
  "dotenv": "^16.4.1", // Environment
  "pg": "^8.11.3", // PostgreSQL
  "yauzl": "^3.1.2" // Unzip streaming
}
```

## 🤝 Contribuindo

### Para Novos Desenvolvedores

```
1. Clone e leia README.md + STRUCTURE.md
2. Configure .env e execute npm install
3. Rode testes: npm run test:conexao
4. Processe sample: npm run test:municipios
5. Analise código e logs
```

### Como Contribuir

1. Fork o repositório
2. Crie branch: `git checkout -b feature/minha-feature`
3. Commit: `git commit -m 'feat: adiciona feature'`
4. Push: `git push origin feature/minha-feature`
5. Abra Pull Request

### Convenções

- ES Modules (import/export)
- async/await (não callbacks)
- JSDoc para funções públicas
- Try/catch para erros
- Logs em operações críticas

## 📄 Licença

MIT License - Veja arquivo LICENSE

## 👨‍💻 Autor

Sistema desenvolvido para processar dados públicos da Receita Federal do Brasil de forma robusta e eficiente.

**Última atualização:** Janeiro 2026  
**Versão:** 1.0.0  
**Status:** ✅ Produção | 96M+ registros processados

---

**💡 Dica Final:** Este sistema foi projetado para ser robusto e recuperável. Se algo der errado, verifique logs, use `npm run status` e não hesite em recomeçar com `npm run limpar-controle`. O sistema sempre pode retomar de onde parou! 🚀
