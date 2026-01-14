# Quick Start - ETL Receita Federal

## ⚡ Início Rápido (5 minutos)

### 1. Instalar Dependências

```bash
npm install
```

### 2. Configurar Banco de Dados

```bash
# Criar banco
createdb etl_receita_federal

# Criar schema
psql -d etl_receita_federal -f sql/schema.sql
```

### 3. Configurar Variáveis

```bash
cp .env.example .env
# Editar .env com suas configurações
```

### 4. Testar

#### Testar Discovery
```bash
npm run discovery
```

#### Testar Pipeline com Municípios
```bash
node src/cli/test-municipios.js https://dadosabertos.rfb.gov.br/CNPJ/Municipios.zip --truncate
```

## 🚀 Comandos Principais

### FULL LOAD (Carga Inicial)
```bash
# Atenção: Pode demorar 10-15 horas!
npm run full-load -- --yes
```

### DELTA (Carga Incremental)
```bash
npm run delta -- --yes
```

### Health Check
```bash
./scripts/health-check.sh
```

## 📁 Estrutura do Projeto

```
etl-receita-federal/
├── src/
│   ├── config/              # Configurações (DB, logger, constantes)
│   ├── services/            # Serviços principais
│   │   ├── discovery.js     # Descobre arquivos na RF
│   │   ├── downloader.js    # Baixa arquivos
│   │   ├── processor.js     # Processa ZIP/CSV
│   │   ├── loader.js        # Carrega no banco
│   │   └── control.js       # Controle de estado
│   ├── transformers/        # Transformadores de dados
│   │   ├── municipio.js
│   │   ├── estabelecimento.js
│   │   └── socio.js
│   ├── orchestrator.js      # Orquestrador principal
│   └── cli/                 # Comandos CLI
├── sql/                     # Scripts SQL
├── scripts/                 # Scripts de automação
├── logs/                    # Logs da aplicação
└── temp/                    # Downloads temporários
```

## 📊 Consultas Úteis

### Ver status de processamento
```sql
SELECT status, COUNT(*) 
FROM etl_control_files 
GROUP BY status;
```

### Ver últimos arquivos processados
```sql
SELECT file_name, status, records_inserted, completed_at 
FROM etl_control_files 
WHERE status = 'done'
ORDER BY completed_at DESC 
LIMIT 10;
```

### Contar registros
```sql
SELECT 
  (SELECT COUNT(*) FROM municipios) as municipios,
  (SELECT COUNT(*) FROM estabelecimentos) as estabelecimentos,
  (SELECT COUNT(*) FROM socios) as socios;
```

### Buscar empresa por CNPJ
```sql
SELECT * FROM estabelecimentos WHERE cnpj = '12345678000100';
```

### Buscar empresas por nome fantasia
```sql
SELECT cnpj, nome_fantasia, uf, codigo_municipio 
FROM estabelecimentos 
WHERE nome_fantasia ILIKE '%nome%'
LIMIT 10;
```

## 🔧 Troubleshooting Rápido

### Erro de conexão ao banco
```bash
sudo systemctl status postgresql
psql -d etl_receita_federal -c "SELECT 1"
```

### Reprocessar arquivos com erro
```bash
psql -d etl_receita_federal -c "UPDATE etl_control_files SET status = 'pending' WHERE status = 'error';"
npm run full-load -- --yes
```

### Limpar tudo e recomeçar
```bash
psql -d etl_receita_federal -f sql/drop.sql
npm run full-load -- --yes
```

## 📚 Documentação Completa

- **README.md**: Visão geral e uso básico
- **DEPLOY.md**: Guia completo de deploy em produção
- **crontab.example**: Configuração de automação

## 🎯 Próximos Passos

1. ✅ Setup completo
2. ✅ Teste com arquivo de Municípios
3. ⏳ Execute FULL LOAD (deixe rodando overnight)
4. ⏳ Configure cron job para DELTA diário
5. ⏳ Configure backup automático
6. ⏳ Implemente monitoramento

## 💡 Dicas

- Use `screen` ou `tmux` para FULL LOAD
- Monitore logs em `logs/etl-YYYY-MM-DD.log`
- Health check mostra estatísticas do sistema
- DELTA só processa arquivos novos (ano/mês)
- Sistema retoma automaticamente após falhas

## 📞 Suporte

Em caso de problemas:
1. Verifique logs: `tail -f logs/etl-*.log`
2. Execute: `./scripts/health-check.sh`
3. Consulte: `DEPLOY.md` seção Troubleshooting
