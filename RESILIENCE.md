# Melhorias de Resiliência do ETL

## ✅ Implementações Concluídas

### 1. **Checkpoint Granular**
- **Arquivo**: `sql/migrate-add-checkpoint.sql`
- **Descrição**: Adiciona coluna `checkpoint` (JSONB) na tabela `etl_control_files`
- **Ação**: Rode `node run-migration.js` antes de iniciar o ETL

### 2. **Funções de Checkpoint** 
- **Arquivo**: `src/services/control.js`
- **Novas funções**:
  - `updateFileCheckpoint(fileId, checkpoint)` - Salva progresso
  - `getFileCheckpoint(fileId)` - Recupera checkpoint
- **Estrutura do checkpoint**:
  ```json
  {
    "linesProcessed": 1500000,
    "csvFileName": "K3241.K03200Y0.ESTABELE",
    "completed": false,
    "lastCheckpoint": "2026-01-17T03:45:12.345Z"
  }
  ```

### 3. **Processor com Resume**
- **Arquivo**: `src/services/processor.js`
- **Melhorias**:
  - Recupera checkpoint ao iniciar processamento
  - Pula linhas já processadas (resume automático)
  - Salva checkpoint a cada 30 segundos
  - Checkpoint final quando completa arquivo
  - Log: `📋 Retomando do checkpoint: linha X`

### 4. **Graceful Shutdown**
- **Arquivo**: `src/orchestrator.js`
- **Handlers adicionados**:
  - `SIGTERM` - Shutdown gracioso (Docker stop)
  - `SIGINT` - Ctrl+C
- **Comportamento**:
  - Salva checkpoint antes de encerrar
  - Aguarda 2s para garantir salvamento
  - Fecha pool de conexões corretamente
  - Status do run: `interrupted` (pode retomar)

### 5. **Preservação de Arquivos**
- **Arquivo**: `src/orchestrator.js` (função `processFile`)
- **Mudanças**:
  - ✅ Verifica se arquivo ZIP já existe antes de baixar
  - ✅ Reutiliza arquivo existente se checkpoint indica incompletude
  - ❌ **NÃO deleta** arquivo em caso de erro
  - ✅ Deleta apenas após sucesso completo
  - Log: `♻️ Reutilizando arquivo existente`

### 6. **Ordenação Correta de Arquivos**
- **Arquivos**: `src/orchestrator.js` + `src/services/control.js`
- **Ordem garantida**:
  1. Municipios (prioridade 1)
  2. Estabelecimentos (prioridade 2)
  3. Socios (prioridade 3)
- **Razão**: Respeitar foreign keys (estabelecimentos → municipios)

## 📋 Novos Scripts Utilitários

### `verificar-estado.js`
```bash
node verificar-estado.js
```
Mostra:
- Runs em andamento
- Arquivos em processamento
- Arquivos pendentes
- Checkpoints salvos
- Arquivos ZIP no diretório temp
- Estatísticas gerais

### `limpar-controle.js`
```bash
node limpar-controle.js
```
Limpa tabelas de controle para reiniciar ETL do zero.

## 🚀 Como Usar

### Primeira execução:
```bash
# 1. Aplicar migração
node run-migration.js

# 2. Iniciar ETL
npm run full-load -- --yes
```

### Após crash/interrupção:
```bash
# Apenas rodar novamente - ele retoma automaticamente
npm run full-load -- --yes

# Verificar estado antes
node verificar-estado.js
```

## 🔍 Logs Importantes

### Checkpoint salvo:
```
[INFO] processor | 💾 Checkpoint salvo: 1.500.000 linhas
```

### Resume automático:
```
[INFO] processor | 📋 Retomando do checkpoint: linha 1.500.000
```

### Reutilizando arquivo:
```
[INFO] orchestrator | ♻️ Reutilizando arquivo existente: Estabelecimentos0.zip
```

### Shutdown gracioso:
```
[WARN] orchestrator | ⚠️  SIGTERM recebido - iniciando shutdown gracioso
[INFO] orchestrator | 💾 Salvando checkpoint do arquivo em processamento...
```

### Mantendo arquivo para retry:
```
[WARN] orchestrator | ⚠️  Mantendo arquivo para retry: Estabelecimentos0.zip
```

## 🐛 Debugging

### Verificar checkpoint no banco:
```sql
SELECT 
  file_name,
  status,
  checkpoint->>'linesProcessed' as lines_processed,
  checkpoint->>'lastCheckpoint' as last_checkpoint,
  checkpoint->>'completed' as completed
FROM etl_control_files
WHERE checkpoint IS NOT NULL
ORDER BY created_at DESC;
```

### Verificar arquivos no temp:
```bash
ls -lh temp/*.zip
du -sh temp/
```

### Ver logs em tempo real:
```bash
tail -f logs/etl-*.log
```

## ⚠️  Importante para Deploy na VPS

### Variáveis de Ambiente (Coolify):
```bash
TEMP_DIR=/data/temp
LOG_DIR=/data/logs
```

### Volumes Persistentes:
```
/data/temp → etl_temp (20 GB)
/data/logs → etl_logs (5 GB)
```

### Health Check:
Desabilitar (é job batch, não web server)

### Restart Policy:
`unless-stopped` ou `on-failure:3`

## 🎯 Cenário de Teste

1. Iniciar ETL
2. Após alguns minutos (durante processamento de Estabelecimentos0.zip)
3. Matar processo: `kill -9 <PID>`
4. Verificar estado: `node verificar-estado.js`
5. Reiniciar: `npm run full-load -- --yes`
6. **Resultado esperado**: Retoma do checkpoint, não re-baixa arquivo

## 📊 Melhorias Implementadas

| Problema                          | Status | Solução                                    |
|-----------------------------------|--------|--------------------------------------------|
| Arquivos perdidos ao restart      | ✅     | Volumes persistentes `/data/temp`          |
| Recomeça do zero após crash       | ✅     | Checkpoint a cada 30s no banco             |
| Cleanup prematuro                 | ✅     | Mantém arquivo até sucesso completo        |
| FK violation (municipios)         | ✅     | Ordenação garantida por tipo               |
| Processo morto sem aviso          | ✅     | Graceful shutdown (SIGTERM/SIGINT)         |
| Não detecta interrupção           | ✅     | Status `interrupted` + checkpoint          |
| Re-download desnecessário         | ✅     | Verifica existência + checkpoint           |

---

**Pronto para produção!** 🚀
