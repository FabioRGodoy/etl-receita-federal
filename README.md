# ETL Receita Federal

Sistema ETL para processar dumps públicos de CNPJ da Receita Federal do Brasil.

## Características

- **Processamento Streaming**: Arquivos nunca são carregados completamente em memória
- **Recuperação de Falhas**: Capaz de retomar processamento de onde parou
- **FULL LOAD e DELTA**: Suporte para carga inicial e incremental
- **Simples e Robusto**: Código direto, sem over-engineering

## Requisitos

- Node.js 18+
- PostgreSQL 13+
- 10-15GB de espaço livre em disco

## Instalação

```bash
# Instalar dependências
npm install

# Configurar variáveis de ambiente
cp .env.example .env
# Editar .env com suas configurações

# Criar banco de dados
createdb etl_receita_federal

# Criar schema
psql -d etl_receita_federal -f sql/schema.sql
```

## Configuração

Edite o arquivo `.env`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_receita_federal
DB_USER=postgres
DB_PASSWORD=sua_senha

BASE_URL=https://dadosabertos.rfb.gov.br/CNPJ/
BATCH_SIZE=1000
```

## Uso

### Testar Discovery

```bash
npm run discovery
```

### FULL LOAD (Carga Inicial)

```bash
npm run full-load
```

**Importante**: FULL LOAD pode demorar 10-15 horas. Rode overnight.

### DELTA (Carga Incremental)

```bash
npm run delta
```

## Estrutura do Projeto

```
etl-receita-federal/
├── src/
│   ├── config/         # Configurações (database, logger, constants)
│   ├── services/       # Serviços principais (discovery, downloader, processor, loader, control)
│   ├── transformers/   # Transformadores de dados
│   ├── orchestrator.js # Orquestrador principal
│   └── cli/            # Comandos CLI
├── sql/                # Scripts SQL
├── logs/               # Logs da aplicação
└── temp/               # Downloads temporários
```

## Arquitetura

O sistema é dividido em componentes simples:

1. **Discovery Service**: Descobre arquivos disponíveis na Receita Federal
2. **File Downloader**: Baixa arquivos via streaming
3. **Stream Processor**: Descompacta ZIP e parseia CSV
4. **Data Transformer**: Transforma e valida dados
5. **Database Loader**: Carrega dados no PostgreSQL em batches
6. **ETL Control**: Controla estado e permite retomada
7. **Orchestrator**: Coordena todo o fluxo

## Logs

Logs são gravados em:
- Console (stdout/stderr)
- Arquivo: `logs/etl-YYYY-MM-DD.log`

## Scripts Úteis

### Executar carga DELTA manualmente

```bash
./scripts/run-delta.sh
```

### Fazer backup do banco

```bash
./scripts/backup-db.sh
```

### Health check

```bash
./scripts/health-check.sh
```

## Automação

### Configurar cron job para DELTA diário

Veja `crontab.example` para configuração completa.

```bash
crontab -e
```

Adicionar:
```cron
0 2 * * * cd /caminho/para/etl-receita-federal && ./scripts/run-delta.sh >> logs/cron-delta.log 2>&1
```

## Troubleshooting

### Erro de conexão ao banco

```bash
# Verificar se PostgreSQL está rodando
sudo systemctl status postgresql

# Testar conexão
psql -h localhost -U postgres -d etl_receita_federal
```

### Retomar processamento após falha

Basta rodar o comando novamente (full-load ou delta). O ETL retoma automaticamente de onde parou.

### Verificar arquivos com erro

```bash
psql -d etl_receita_federal -c "
  SELECT file_name, error_message 
  FROM etl_control_files 
  WHERE status = 'error';
"
```

### Reprocessar arquivos com erro

```bash
psql -d etl_receita_federal -c "
  UPDATE etl_control_files 
  SET status = 'pending' 
  WHERE status = 'error';
"

npm run full-load  # ou npm run delta
```

### Limpar banco e recomeçar

```bash
psql -d etl_receita_federal -f sql/drop.sql
npm run full-load -- --yes
```

## Deploy em Produção

Veja `DEPLOY.md` para guia completo de deploy em VPS/servidor.

## Arquivos Importantes

- `sql/schema.sql` - Schema do banco de dados
- `sql/drop.sql` - Limpar e recriar banco
- `scripts/run-delta.sh` - Script para cron job
- `scripts/backup-db.sh` - Backup automático
- `scripts/health-check.sh` - Monitoramento
- `crontab.example` - Exemplo de configuração cron
- `DEPLOY.md` - Guia completo de deploy

## Performance

**V1 (Atual - MVP)**:
- FULL LOAD: ~10-15 horas (batch INSERT)
- DELTA: ~1-3 horas
- Batch size: 1000 registros

**V2 (Futuro - Otimizações)**:
- [ ] COPY FROM STDIN (3-5x mais rápido)
- [ ] Drop/recreate de índices durante FULL LOAD
- [ ] Retry automático com backoff
- [ ] Logs JSON estruturados
- [ ] Métricas e dashboard
- [ ] Testes automatizados

## Suporte

Para problemas ou dúvidas:
1. Verificar logs em `logs/`
2. Executar `./scripts/health-check.sh`
3. Consultar `DEPLOY.md` para troubleshooting

## Licença

MIT
