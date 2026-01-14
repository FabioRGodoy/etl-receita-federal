# Guia de Deploy - ETL Receita Federal

## Pré-requisitos

### VPS / Servidor
- Ubuntu 20.04+ ou similar
- 4GB RAM mínimo (8GB recomendado)
- 50GB espaço em disco livre
- Node.js 18+
- PostgreSQL 13+

## 1. Preparação do Servidor

### Instalar Node.js

```bash
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs
```

### Instalar PostgreSQL

```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### Configurar PostgreSQL

```bash
# Criar usuário e banco
sudo -u postgres psql

-- No console do PostgreSQL:
CREATE DATABASE etl_receita_federal;
CREATE USER etl_user WITH ENCRYPTED PASSWORD 'sua_senha_forte';
GRANT ALL PRIVILEGES ON DATABASE etl_receita_federal TO etl_user;
\q
```

### Ajustar configurações do PostgreSQL (opcional, para performance)

Editar `/etc/postgresql/{version}/main/postgresql.conf`:

```ini
# Para FULL LOAD
shared_buffers = 2GB
work_mem = 50MB
maintenance_work_mem = 512MB
effective_cache_size = 6GB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
```

Reiniciar:
```bash
sudo systemctl restart postgresql
```

## 2. Instalação do ETL

### Clonar ou enviar código

```bash
# Opção 1: Git
cd /opt
sudo git clone <seu-repo> etl-receita-federal
cd etl-receita-federal

# Opção 2: rsync
rsync -avz /local/path/ user@servidor:/opt/etl-receita-federal/
```

### Instalar dependências

```bash
cd /opt/etl-receita-federal
npm install --production
```

### Configurar ambiente

```bash
cp .env.example .env
nano .env
```

Editar com suas configurações:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_receita_federal
DB_USER=etl_user
DB_PASSWORD=sua_senha_forte

BASE_URL=https://dadosabertos.rfb.gov.br/CNPJ/
BATCH_SIZE=1000
DOWNLOAD_TIMEOUT=600000
TEMP_DIR=/opt/etl-receita-federal/temp
LOG_DIR=/opt/etl-receita-federal/logs
```

### Criar schema do banco

```bash
psql -h localhost -U etl_user -d etl_receita_federal -f sql/schema.sql
```

## 3. Primeira Execução (FULL LOAD)

### Usando screen ou tmux

```bash
# Instalar screen
sudo apt install screen

# Criar sessão
screen -S etl-full-load

# Dentro da sessão, executar:
cd /opt/etl-receita-federal
node src/cli/full-load.js --yes

# Desanexar: Ctrl+A, depois D
# Reanexar: screen -r etl-full-load
```

### Monitorar progresso

Em outro terminal:

```bash
# Logs em tempo real
tail -f /opt/etl-receita-federal/logs/etl-$(date +%Y-%m-%d).log

# Verificar quantos arquivos foram processados
psql -U etl_user -d etl_receita_federal -c "
  SELECT status, COUNT(*) 
  FROM etl_control_files 
  GROUP BY status;
"

# Ver últimos arquivos processados
psql -U etl_user -d etl_receita_federal -c "
  SELECT file_name, status, records_inserted, completed_at 
  FROM etl_control_files 
  WHERE status = 'done'
  ORDER BY completed_at DESC 
  LIMIT 10;
"
```

## 4. Configurar Carga DELTA Automática

### Criar script wrapper

```bash
sudo nano /opt/etl-receita-federal/scripts/run-delta.sh
```

Conteúdo:
```bash
#!/bin/bash
set -e

cd /opt/etl-receita-federal

# Executar DELTA
node src/cli/delta.js --yes

# Enviar email em caso de erro (opcional)
if [ $? -ne 0 ]; then
    echo "ETL DELTA falhou em $(date)" | mail -s "ETL Error" seu-email@example.com
fi
```

Dar permissão de execução:
```bash
chmod +x /opt/etl-receita-federal/scripts/run-delta.sh
```

### Configurar cron job

```bash
crontab -e
```

Adicionar:
```cron
# ETL Receita Federal - DELTA diário às 2h da manhã
0 2 * * * /opt/etl-receita-federal/scripts/run-delta.sh >> /opt/etl-receita-federal/logs/cron.log 2>&1

# Limpar logs antigos (manter últimos 30 dias)
0 3 * * 0 find /opt/etl-receita-federal/logs -name "etl-*.log" -mtime +30 -delete
```

## 5. Backup

### Backup automático do PostgreSQL

```bash
sudo nano /opt/etl-receita-federal/scripts/backup-db.sh
```

Conteúdo:
```bash
#!/bin/bash
BACKUP_DIR="/opt/backups/etl-receita-federal"
DATE=$(date +%Y%m%d_%H%M%S)
FILENAME="etl_receita_federal_$DATE.sql.gz"

mkdir -p $BACKUP_DIR

# Backup
pg_dump -U etl_user -d etl_receita_federal | gzip > $BACKUP_DIR/$FILENAME

# Manter últimos 7 backups
ls -t $BACKUP_DIR/*.sql.gz | tail -n +8 | xargs rm -f

echo "Backup concluído: $FILENAME"
```

Adicionar ao cron:
```cron
# Backup diário às 4h da manhã
0 4 * * * /opt/etl-receita-federal/scripts/backup-db.sh
```

## 6. Monitoramento

### Script de health check

```bash
sudo nano /opt/etl-receita-federal/scripts/health-check.sh
```

Conteúdo:
```bash
#!/bin/bash

# Verificar se banco está acessível
psql -U etl_user -d etl_receita_federal -c "SELECT 1" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "ERRO: Banco de dados inacessível"
    exit 1
fi

# Verificar última execução
LAST_RUN=$(psql -U etl_user -d etl_receita_federal -t -c "
  SELECT completed_at 
  FROM etl_control_runs 
  WHERE status = 'completed' 
  ORDER BY completed_at DESC 
  LIMIT 1;
")

echo "Última execução: $LAST_RUN"
echo "Sistema OK"
```

## 7. Troubleshooting

### Logs

```bash
# Ver últimos erros
grep ERROR /opt/etl-receita-federal/logs/etl-$(date +%Y-%m-%d).log

# Ver estatísticas de run
psql -U etl_user -d etl_receita_federal -c "
  SELECT * FROM etl_control_runs ORDER BY id DESC LIMIT 5;
"
```

### Reprocessar arquivos com erro

```bash
# Marcar arquivos com erro como pendentes
psql -U etl_user -d etl_receita_federal -c "
  UPDATE etl_control_files 
  SET status = 'pending' 
  WHERE status = 'error';
"

# Executar novamente
node src/cli/full-load.js --yes
# ou
node src/cli/delta.js --yes
```

### Limpar e recomeçar

```bash
psql -U etl_user -d etl_receita_federal -f sql/drop.sql
node src/cli/full-load.js --yes
```

## 8. Segurança

### Firewall

```bash
sudo ufw allow 22/tcp
sudo ufw allow 5432/tcp  # Apenas se precisar acesso remoto ao PostgreSQL
sudo ufw enable
```

### Acesso ao PostgreSQL

Por padrão, PostgreSQL só aceita conexões locais. Se precisar acesso remoto:

```bash
# Editar pg_hba.conf
sudo nano /etc/postgresql/{version}/main/pg_hba.conf

# Adicionar linha:
# host    etl_receita_federal    etl_user    0.0.0.0/0    md5

# Editar postgresql.conf
sudo nano /etc/postgresql/{version}/main/postgresql.conf

# Alterar:
# listen_addresses = '*'

# Reiniciar
sudo systemctl restart postgresql
```

## 9. Performance

### Índices adicionais (se necessário)

```sql
-- Exemplo: busca por razão social
CREATE INDEX idx_estabelecimento_nome_fantasia 
ON estabelecimentos USING gin(to_tsvector('portuguese', nome_fantasia));

-- Exemplo: busca por CEP
CREATE INDEX idx_estabelecimento_cep ON estabelecimentos(cep);
```

### Vacuum e Analyze

```bash
# Adicionar ao cron
crontab -e
```

```cron
# Vacuum e Analyze semanalmente
0 5 * * 0 psql -U etl_user -d etl_receita_federal -c "VACUUM ANALYZE;"
```

## 10. Atualização do Sistema

```bash
cd /opt/etl-receita-federal
git pull
npm install --production
sudo systemctl restart cron  # se houver mudanças no cron
```
