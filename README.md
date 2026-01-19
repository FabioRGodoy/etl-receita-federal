# ETL Receita Federal - Dados Abertos CNPJ

Sistema ETL robusto para processar dumps públicos de CNPJ da Receita Federal do Brasil.

## ✨ Características

- **🔄 Processamento Streaming**: Arquivos processados via streaming, sem carregar na memória
- **💾 Checkpoint System**: Salva progresso a cada 30 segundos, permite retomar de onde parou
- **🛡️ Proteção Anti-Loop**: Previne perda de dados em redeploys acidentais
- **🧹 Sanitização UTF-8**: Remove bytes nulos e caracteres inválidos dos dados
- **📦 FULL LOAD e DELTA**: Suporte para carga inicial completa e incremental
- **🔐 Graceful Shutdown**: Shutdown elegante com salvamento de checkpoint (SIGTERM/SIGINT)
- **📊 Logging Estruturado**: Logs detalhados com timestamps e categorias
- **🚀 Docker Ready**: Totalmente containerizado com Docker/Coolify

## 📋 Requisitos

- Node.js 20+
- PostgreSQL 17+ (testado com 17.7)
- 20GB de espaço livre em disco (`/data/temp`)
- 5GB para logs (`/data/logs`)
- Conexão estável com internet (downloads de ~20GB total)

## 🚀 Quick Start

### Instalação Local

```bash
# Clonar repositório
git clone https://github.com/seu-usuario/etl-receita-federal.git
cd etl-receita-federal

# Instalar dependências
npm install

# Configurar variáveis de ambiente
cp .env.example .env
# Editar .env com suas configurações de banco

# Criar banco de dados e schema
psql -U postgres -c "CREATE DATABASE etl_receita_federal;"
psql -U postgres -d etl_receita_federal -f sql/schema.sql

# Aplicar migrations
node run-migration.js

# Executar FULL LOAD
npm run full-load -- --yes
```

### Deploy com Docker/Coolify

```bash
# 1. Configure no Coolify:
#    - Repository: seu-repo-git
#    - Build Pack: Dockerfile
#    - Porta: 3000 (opcional, para health check)

# 2. Configure Environment Variables no Coolify:
DB_HOST=seu-host-postgres
DB_PORT=5430
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=sua-senha
MODE=manual  # Importante: não executar automaticamente

# 3. Configure Persistent Storage:
/data/temp → 20GB
/data/logs → 5GB

# 4. Deploy
# Container subirá e ficará ativo aguardando comandos

# 5. Executar ETL manualmente (via Execute Command no Coolify):
npm run full-load -- --yes
```

## ⚙️ Configuração

### Variáveis de Ambiente (.env)

```env
# Database Configuration
DB_HOST=145.223.94.201
DB_PORT=5430
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=sua_senha_aqui

# ETL Configuration
BASE_URL=https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/
BATCH_SIZE=1000           # Registros por batch INSERT
DOWNLOAD_TIMEOUT=600000   # 10 minutos
TEMP_DIR=./temp           # Diretório para ZIPs temporários
LOG_DIR=./logs            # Diretório de logs

# Test Mode (opcional)
TESTMODE=false
```

### Configuração Docker/Coolify

**Environment Variables:**
- `MODE=manual` - Container não executa ETL automaticamente
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` - Conexão PostgreSQL
- `BASE_URL` - URL dos dados abertos da Receita Federal

**Persistent Storage:**
- `/data/temp` → 20GB (downloads de arquivos ZIP)
- `/data/logs` → 5GB (logs do ETL)

**Restart Policy:**
- Configure como `no` ou `on-failure` (NUNCA use `always`)

## 💻 Uso

### Comandos Principais

```bash
# FULL LOAD (carga inicial completa - trunca tabelas)
npm run full-load -- --yes

# DELTA (carga incremental - apenas arquivos novos)
npm run delta -- --yes

# Discovery (listar arquivos disponíveis)
npm run discovery

# Verificar estado do ETL
npm run status
# ou
node verificar-estado.js

# Limpar controle (checkpoints e histórico)
node limpar-controle.js

# Limpar arquivos temporários
node limpar-temp.js

# Limpar tabelas de dados
node limpar-estabelecimentos.js
node limpar-socios.js
```

### ⚠️ FULL LOAD - Importante!

**Atenção:**
- FULL LOAD faz **TRUNCATE** em todas as tabelas (municipios, estabelecimentos, socios)
- Processa **~50 milhões de registros** (21 arquivos)
- Demora aproximadamente **7-10 horas**
- **Protegido contra loop infinito:** Não executa se houver FULL LOAD concluído nas últimas 2 horas

```bash
# Exemplo de execução segura
npm run full-load -- --yes

# Se precisar forçar (ignora proteção de 2h)
npm run full-load -- --yes --force
```

### 🔄 Checkpoint e Retomada

O sistema salva checkpoints a cada 30 segundos. Se o processo for interrompido:

```bash
# Basta rodar novamente - retoma automaticamente
npm run full-load -- --yes

# Se quiser começar do zero:
node limpar-controle.js  # Remove checkpoints
node limpar-temp.js      # Remove ZIPs baixados
npm run full-load -- --yes
```

## 📁 Estrutura do Projeto

```
etl-receita-federal/
├── docker/
│   └── entrypoint.sh          # Entrypoint do container Docker
├── logs/                      # Logs da aplicação (persistente)
├── scripts/
│   ├── backup-db.sh           # Backup do banco
│   ├── health-check.sh        # Health check
│   ├── run-delta.sh           # Executar DELTA
│   └── test-resume.sh         # Teste de retomada
├── sql/
│   ├── schema.sql             # Schema principal
│   ├── drop.sql               # Limpar banco
│   ├── migrate-*.sql          # Migrations
├── src/
│   ├── cli/
│   │   ├── full-load.js       # CLI FULL LOAD
│   │   ├── delta.js           # CLI DELTA
│   │   ├── discovery-test.js  # CLI Discovery
│   │   └── test-municipios.js # Testes
│   ├── config/
│   │   ├── constants.js       # Constantes do sistema
│   │   ├── database.js        # Configuração PostgreSQL
│   │   └── logger.js          # Sistema de logs
│   ├── services/
│   │   ├── control.js         # Controle de runs e checkpoints
│   │   ├── discovery.js       # Discovery de arquivos
│   │   ├── downloader.js      # Download com retry
│   │   ├── loader.js          # Carregamento batch no banco
│   │   └── processor.js       # Processamento streaming ZIP/CSV
│   ├── transformers/
│   │   ├── estabelecimento.js # Transformer estabelecimentos
│   │   ├── municipio.js       # Transformer municipios
│   │   └── socio.js           # Transformer socios
│   ├── utils/
│   │   └── sanitize.js        # Sanitização UTF-8
│   └── orchestrator.js        # Orquestrador principal
├── temp/                      # Downloads temporários (persistente)
├── .env                       # Variáveis de ambiente
├── Dockerfile                 # Build do container
├── package.json               # Dependencies
└── README.md                  # Esta documentação
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

## 🗄️ Schema do Banco

### Tabelas Principais

```sql
-- Tabela de Municipios (FK parent)
municipios (
  codigo_municipio INTEGER PRIMARY KEY,
  nome_municipio VARCHAR(200)
)

-- Tabela de Estabelecimentos (~50M registros)
estabelecimentos (
  id SERIAL PRIMARY KEY,
  cnpj_basico VARCHAR(8),
  cnpj_ordem VARCHAR(4),
  cnpj_dv VARCHAR(2),
  cnpj VARCHAR(14),
  codigo_municipio INTEGER REFERENCES municipios,
  -- ... 30 campos total
  UNIQUE(cnpj_basico, cnpj_ordem, cnpj_dv)
)

-- Tabela de Socios
socios (
  id SERIAL PRIMARY KEY,
  cnpj_basico VARCHAR(8) REFERENCES estabelecimentos(cnpj_basico),
  -- ... 11 campos total
  UNIQUE(cnpj_basico, identificador_socio, cpf_cnpj_socio)
)
```

### Tabelas de Controle

```sql
-- Controle de runs (execuções do ETL)
etl_control_runs (
  id SERIAL PRIMARY KEY,
  run_type VARCHAR(10),           -- 'FULL' ou 'DELTA'
  status VARCHAR(20),              -- 'running', 'completed', 'error', 'interrupted'
  total_files INTEGER,
  files_completed INTEGER,
  files_failed INTEGER,
  started_at TIMESTAMP,
  completed_at TIMESTAMP
)

-- Controle de arquivos processados
etl_control_files (
  id SERIAL PRIMARY KEY,
  file_name VARCHAR(255) UNIQUE,
  file_url TEXT,
  file_type VARCHAR(50),           -- 'municipios', 'estabelecimentos', 'socios'
  run_id INTEGER REFERENCES etl_control_runs(id),
  status VARCHAR(20),              -- 'pending', 'processing', 'done', 'error'
  checkpoint JSONB,                -- { linesProcessed, csvFileName, completed }
  records_inserted INTEGER,
  started_at TIMESTAMP,
  completed_at TIMESTAMP
)
```

## 📈 Performance

### Tempos de Execução (Ambiente Produção)

| Operação | Tempo | Observações |
|----------|-------|-------------|
| **FULL LOAD** | 7-10 horas | 21 arquivos, ~50M registros |
| **DELTA** | 1-3 horas | Apenas arquivos novos |
| **Municipios** | 5-10 minutos | ~5.572 registros |
| **Estabelecimentos0** | 1-2 horas | ~5M registros por arquivo |
| **Socios0** | 30-60 minutos | ~2M registros por arquivo |

### Configurações de Performance

```env
BATCH_SIZE=1000           # Registros por batch INSERT
DOWNLOAD_TIMEOUT=600000   # 10 minutos por arquivo
```

### Otimizações Implementadas

- ✅ Streaming de arquivos (zero cópia na memória)
- ✅ Batch INSERT (1000 registros por vez)
- ✅ ON CONFLICT DO NOTHING (evita duplicatas sem erro)
- ✅ Checkpoint system (retoma sem reprocessar)
- ✅ Connection pooling (pg Pool)
- ✅ Graceful shutdown (salva estado antes de sair)

### Melhorias Futuras (V2)

- [ ] COPY FROM STDIN (3-5x mais rápido que INSERT)
- [ ] Drop/recreate indexes durante FULL LOAD
- [ ] Processamento paralelo de arquivos independentes
- [ ] Compressão de logs antigos
- [ ] Métricas e dashboard em tempo real
- [ ] Testes automatizados (unit + integration)

## 🤖 Automação (Cron Jobs)

### Configurar DELTA diário

```bash
# Editar crontab
crontab -e

# Adicionar linha (executa todo dia às 2h da manhã)
0 2 * * * cd /caminho/etl-receita-federal && ./scripts/run-delta.sh >> logs/cron-delta.log 2>&1
```

### Exemplo de crontab completo

Ver arquivo `crontab.example`:

```cron
# DELTA diário às 2h
0 2 * * * cd /app && npm run delta -- --yes >> logs/cron.log 2>&1

# Backup semanal aos domingos às 3h
0 3 * * 0 cd /app && ./scripts/backup-db.sh >> logs/backup.log 2>&1

# Limpeza de logs antigos (> 30 dias)
0 4 * * * find /app/logs -name "*.log" -mtime +30 -delete
```

### Health Check Automático

```bash
# Health check a cada 5 minutos
*/5 * * * * cd /app && ./scripts/health-check.sh >> logs/health.log 2>&1
```

## 📝 Logging

### Estrutura de Logs

```
logs/
├── etl-2026-01-18.log       # Log do dia
├── etl-2026-01-17.log       # Log dia anterior
├── cron-delta.log           # Logs do cron DELTA
└── backup.log               # Logs de backup
```

### Formato de Log

```
[2026-01-18 01:32:15] INFO  | orchestrator    | Iniciando ETL - Tipo: FULL
[2026-01-18 01:32:20] INFO  | discovery       | 21 arquivos encontrados
[2026-01-18 01:32:25] INFO  | downloader      | ⬇️  Baixando: Municipios.zip
[2026-01-18 01:33:00] INFO  | processor       | 📋 Retomando do checkpoint: linha 793,000
[2026-01-18 01:45:00] INFO  | orchestrator    | ✅ Arquivo concluído: 5,572 registros
```

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
```

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
# Verificar se PostgreSQL está acessível
psql -h 145.223.94.201 -p 5430 -U postgres -d postgres

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
DB_HOST=seu-host-postgres
DB_PORT=5430
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=sua-senha-segura
BASE_URL=https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/
TEMP_DIR=/data/temp
LOG_DIR=/data/logs
```

#### 3. Configurar Persistent Storage

| Source Path | Destination | Size |
|-------------|-------------|------|
| `/data/temp` | `/data/temp` | 20GB |
| `/data/logs` | `/data/logs` | 5GB |

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
pg_dump -h host -p 5430 -U postgres -d postgres > backup-$(date +%Y%m%d).sql
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
DB_HOST=145.223.94.201
DB_PORT=5430
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

## 📦 Scripts Úteis

### Backup

```bash
# Backup completo do banco
./scripts/backup-db.sh

# Backup manual
pg_dump -h host -p 5430 -U postgres -d postgres > backup.sql

# Restaurar backup
psql -h host -p 5430 -U postgres -d postgres < backup.sql
```

### Health Check

```bash
# Verificar se ETL está rodando
./scripts/health-check.sh

# Verificar estado do banco
npm run status
```

### Limpeza

```bash
# Limpar controle (checkpoints e runs)
node limpar-controle.js

# Limpar arquivos temporários
node limpar-temp.js

# Limpar dados (cuidado!)
node limpar-estabelecimentos.js
node limpar-socios.js
```

## 🔧 Desenvolvimento

### Setup Local

```bash
# Clone e instale
git clone https://github.com/seu-usuario/etl-receita-federal.git
cd etl-receita-federal
npm install

# Configure banco local
cp .env.example .env
# Edite .env com configurações locais

# Crie banco e schema
createdb etl_receita_federal
psql -d etl_receita_federal -f sql/schema.sql

# Aplique migrations
node run-migration.js
```

### Executar Localmente

```bash
# Modo development (com logs verbosos)
NODE_ENV=development npm run full-load -- --yes

# Testar apenas um tipo de arquivo
npm run test:municipios
npm run test:estabelecimentos  
npm run test:socios
```

### Debug

```bash
# Executar com inspect
node --inspect src/cli/full-load.js

# Logs detalhados
DEBUG=* npm run full-load -- --yes
```

### Estrutura de Código

```javascript
// Padrão de serviço
export async function serviceName(params) {
  try {
    // Lógica aqui
    logger.info('service', 'Mensagem', { dados });
    return result;
  } catch (error) {
    logger.error('service', 'Erro', error.message);
    throw error;
  }
}

// Padrão de transformer
export function transformRecord(row) {
  try {
    // Validação
    if (!row[0]) return null;
    
    // Transformação
    const record = {
      field: safeTrim(row[0]),
      number: parseInteger(row[1])
    };
    
    // Sanitização final
    return sanitizeRecord(record);
  } catch (error) {
    console.error('Erro transform:', error.message);
    return null;
  }
}
```

## 🤝 Contribuindo

### Guidelines

1. Mantenha código simples e direto
2. Use ES modules (import/export)
3. Adicione logs em pontos críticos
4. Valide dados antes de inserir no banco
5. Teste localmente antes de commit
6. Documente mudanças complexas

### Pull Request

```bash
# Crie branch
git checkout -b feature/nova-funcionalidade

# Faça commits descritivos
git commit -m "feat: adicionar retry no download"

# Push e abra PR
git push origin feature/nova-funcionalidade
```

### Checklist de PR

- [ ] Código testado localmente
- [ ] Logs adicionados
- [ ] Variáveis sensíveis removidas
- [ ] README atualizado (se necessário)
- [ ] Migrations aplicadas (se necessário)

## 📚 Recursos

### Documentação Oficial

- [Receita Federal - Dados Abertos CNPJ](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Node.js Streams](https://nodejs.org/api/stream.html)

### Dependências Principais

- `pg` - Cliente PostgreSQL
- `yauzl` - Descompactação de ZIP streaming
- `csv-parse` - Parser de CSV streaming
- `node-fetch` - HTTP client para downloads

## ❓ FAQ

### Por que streaming ao invés de carregar arquivos na memória?

Arquivos da Receita Federal podem ter até 2GB. Streaming permite processar qualquer tamanho sem estourar memória.

### Por que checkpoint a cada 30 segundos?

Balanceamento entre performance (overhead de I/O) e granularidade de retomada. Pode ser ajustado em `src/orchestrator.js`.

### Como funciona o sistema de retomada?

O checkpoint salva:
- Nome do arquivo CSV dentro do ZIP
- Número da última linha processada
- Timestamp

Ao retomar, o processor pula linhas já processadas e continua de onde parou.

### Por que TRUNCATE no FULL LOAD?

FULL LOAD é carga inicial completa. TRUNCATE é mais rápido que DELETE e reseta sequences. Para atualização incremental, use DELTA.

### Como adicionar um novo transformer?

1. Crie arquivo em `src/transformers/novo-tipo.js`
2. Implemente função `transformRecord(row)`
3. Adicione ao `TRANSFORMERS` em `src/orchestrator.js`
4. Configure tipo em `src/config/constants.js`

## 📄 Licença

MIT License - Sinta-se livre para usar, modificar e distribuir.

## 👨‍💻 Autor

Desenvolvido para processar dados abertos da Receita Federal de forma eficiente e resiliente.

---

**⭐ Se este projeto foi útil, considere dar uma estrela no GitHub!**
