# Estrutura do Projeto

## 📂 Organização de Diretórios

```
etl-receita-federal/
│
├── 📁 docker/                      # Configurações Docker
│   └── entrypoint.sh              # Script de inicialização do container
│
├── 📁 logs/                        # Logs da aplicação (persistente)
│   └── etl-YYYY-MM-DD.log         # Logs diários
│
├── 📁 scripts/                     # Scripts auxiliares de produção
│   ├── backup-db.sh               # Backup do banco
│   ├── health-check.sh            # Health check
│   ├── run-delta.sh               # Executar DELTA
│   └── test-resume.sh             # Testar retomada
│
├── 📁 sql/                         # Scripts SQL e migrations
│   ├── schema.sql                 # Schema principal
│   ├── drop.sql                   # Limpar banco
│   ├── setup-banco.sh             # Setup inicial do banco
│   └── migrations/                # Migrations SQL
│       ├── run-migration.js       # Executor de migrations
│       ├── migrate-add-run-id.sql
│       ├── migrate-add-checkpoint.sql
│       ├── migrate-add-updated-at.sql
│       └── migrate-fix-on-conflict.sql
│
├── 📁 src/                         # Código-fonte principal
│   ├── cli/                       # Interfaces de linha de comando
│   │   ├── full-load.js           # CLI FULL LOAD
│   │   ├── delta.js               # CLI DELTA
│   │   ├── discovery-test.js      # CLI Discovery
│   │   └── test-municipios.js     # Teste específico
│   │
│   ├── config/                    # Configurações
│   │   ├── constants.js           # Constantes do sistema
│   │   ├── database.js            # Pool PostgreSQL
│   │   └── logger.js              # Sistema de logs
│   │
│   ├── services/                  # Serviços principais
│   │   ├── control.js             # Controle de runs e checkpoints
│   │   ├── discovery.js           # Discovery de arquivos
│   │   ├── downloader.js          # Download com retry
│   │   ├── loader.js              # Carregamento batch no banco
│   │   └── processor.js           # Processamento streaming ZIP/CSV
│   │
│   ├── transformers/              # Transformadores de dados
│   │   ├── estabelecimento.js     # Transformer estabelecimentos
│   │   ├── municipio.js           # Transformer municipios
│   │   └── socio.js               # Transformer socios
│   │
│   ├── utils/                     # Utilitários internos
│   │   └── sanitize.js            # Sanitização UTF-8
│   │
│   └── orchestrator.js            # Orquestrador principal
│
├── 📁 temp/                        # Downloads temporários (persistente)
│   └── *.zip                      # Arquivos baixados da Receita Federal
│
├── 📁 tests/                       # Scripts de teste
│   ├── check-constraints.js       # Verificar constraints do banco
│   ├── test-conexao.js            # Testar conexão com banco
│   ├── test-discovery.js          # Testar discovery
│   ├── test-estabelecimentos-simples.js
│   ├── test-municipios-simples.js
│   ├── test-municipios-2025-12.js
│   ├── test-socios-simples.js
│   ├── run-test-estabelecimentos.sh
│   ├── run-test-estabelecimentos.bat
│   ├── run-test-socios.sh
│   └── run-test-socios.bat
│
├── 📁 utils/                       # Utilitários de manutenção
│   ├── limpar-controle.js         # Limpar checkpoints e runs
│   ├── limpar-estabelecimentos.js # Truncar tabela estabelecimentos
│   ├── limpar-socios.js           # Truncar tabela socios
│   ├── limpar-temp.js             # Limpar arquivos temporários
│   └── verificar-estado.js        # Verificar estado do ETL
│
├── .dockerignore                  # Ignorar arquivos no Docker build
├── .env                           # Variáveis de ambiente (não commitar!)
├── .gitignore                     # Ignorar arquivos no Git
├── crontab.example                # Exemplo de cron jobs
├── Dockerfile                     # Build do container
├── package.json                   # Dependências Node.js
└── README.md                      # Documentação principal
```

## 🎯 Comandos NPM Atualizados

### Comandos Principais
```bash
npm run full-load          # Executa FULL LOAD
npm run delta              # Executa DELTA
npm run discovery          # Lista arquivos disponíveis
npm run status             # Verifica estado do ETL
```

### Utilitários de Manutenção
```bash
npm run limpar-controle    # Limpa checkpoints e runs
npm run limpar-temp        # Remove arquivos ZIP temporários
npm run migrate            # Executa migrations
```

### Testes
```bash
npm run test:conexao           # Testa conexão com banco
npm run test:discovery         # Testa discovery de arquivos
npm run test:municipios        # Testa processamento municipios
npm run test:estabelecimentos  # Testa processamento estabelecimentos
npm run test:socios            # Testa processamento socios
```

## 📝 Fluxo de Execução

### FULL LOAD
1. **CLI** (`src/cli/full-load.js`) → recebe parâmetros
2. **Orchestrator** (`src/orchestrator.js`) → coordena fluxo
3. **Discovery** (`src/services/discovery.js`) → encontra arquivos
4. **Control** (`src/services/control.js`) → cria run, registra arquivos
5. **Downloader** (`src/services/downloader.js`) → baixa ZIPs
6. **Processor** (`src/services/processor.js`) → extrai e parseia CSV
7. **Transformer** (`src/transformers/*.js`) → transforma dados
8. **Sanitizer** (`src/utils/sanitize.js`) → limpa dados
9. **Loader** (`src/services/loader.js`) → insere no banco

### Checkpoint e Retomada
- **Control Service** salva checkpoint a cada 30s no banco
- Se interrompido, próxima execução retoma do checkpoint
- Checkpoint armazena: arquivo, linha, timestamp

## 🔧 Manutenção

### Limpeza Periódica
```bash
# Limpar logs antigos (> 30 dias)
find logs/ -name "*.log" -mtime +30 -delete

# Limpar arquivos temporários
npm run limpar-temp

# Limpar checkpoints de runs antigos
npm run limpar-controle
```

### Backup
```bash
# Backup completo do banco
./scripts/backup-db.sh

# Ou manual
pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME > backup.sql
```

### Troubleshooting
```bash
# Verificar estado geral
npm run status

# Ver logs recentes
tail -f logs/etl-$(date +%Y-%m-%d).log

# Testar conexão
npm run test:conexao

# Verificar constraints do banco
node tests/check-constraints.js
```

## 📚 Documentação

- **README.md** - Documentação principal completa
- **STRUCTURE.md** - Este arquivo (estrutura do projeto)
- **package.json** - Scripts e dependências
- **crontab.example** - Exemplos de automação

## 🚀 Deploy

Para deploy em produção, consulte a seção "Deploy com Docker/Coolify" no README.md principal.

## 🤝 Contribuindo

Ao adicionar novos arquivos, mantenha a organização:
- Testes → `tests/`
- Utilitários → `utils/`
- Código-fonte → `src/`
- Scripts de produção → `scripts/`
- SQL e migrations → `sql/` e `sql/migrations/`
