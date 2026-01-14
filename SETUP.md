# Setup Rápido - ETL Receita Federal

## ✅ Status Atual

- ✅ Código completo implementado
- ✅ Conexão com banco PostgreSQL funcionando
- ⏳ Falta criar banco e schema

## 🚀 Setup em 3 Comandos

### Opção 1: Script Automático (Recomendado)

```bash
./setup-banco.sh
```

Vai pedir a senha do PostgreSQL: `mCwErBcgAJyrCaxZyQiiNj9VBt84W8zOociX1jP9MtyB1O6Pie0rt0Cc4b05vhAt`

### Opção 2: Manual

```bash
# 1. Criar banco
createdb -h 145.223.94.201 -p 5431 -U postgres etl_receita_federal

# 2. Criar schema
psql -h 145.223.94.201 -p 5431 -U postgres -d etl_receita_federal -f sql/schema.sql

# 3. Verificar
psql -h 145.223.94.201 -p 5431 -U postgres -d etl_receita_federal -c "\dt"
```

### 3. Atualizar .env

Depois de criar o banco, atualizar o `.env`:

```bash
# Trocar esta linha:
DB_NAME=postgres

# Por esta:
DB_NAME=etl_receita_federal
```

Ou rodar:

```bash
sed -i 's/DB_NAME=postgres/DB_NAME=etl_receita_federal/' .env
```

## 🧪 Testar

```bash
# 1. Testar conexão
node test-conexao.js

# 2. Testar pipeline completo (1 arquivo pequeno)
node test-municipios-simples.js
```

## 📊 Resultado Esperado

Se tudo funcionar, `test-municipios-simples.js` deve mostrar:

```
======================================================================
🧪 TESTE SIMPLES - 1 Arquivo de Municípios
======================================================================

1️⃣  Testando conexão com PostgreSQL...
   ✅ Conectado ao banco

2️⃣  Verificando tabela municipios...
   ✅ Tabela municipios existe

3️⃣  Baixando arquivo...
   ✅ Download concluído

4️⃣  Processando arquivo ZIP...
   ✅ Batch processado: 5570 municípios

5️⃣  Limpando arquivo temporário...
   ✅ Arquivo removido

6️⃣  Verificando dados carregados...
   📊 Total de municípios no banco: 5570

   📋 Amostra (primeiros 5):
      ...

======================================================================
✅ TESTE CONCLUÍDO COM SUCESSO!
======================================================================
```

## 🐛 Troubleshooting

### Senha do PostgreSQL

Se pedir senha, use: `mCwErBcgAJyrCaxZyQiiNj9VBt84W8zOociX1jP9MtyB1O6Pie0rt0Cc4b05vhAt`

### Erro "relation does not exist"

Execute novamente: `psql -h 145.223.94.201 -p 5431 -U postgres -d etl_receita_federal -f sql/schema.sql`

### Banco já existe

Tudo bem! Apenas crie o schema: `psql ... -f sql/schema.sql`

## 📁 Arquivos Criados

- ✅ `test-conexao.js` - Testa conexão com banco
- ✅ `test-municipios-simples.js` - Teste completo com 1 arquivo
- ✅ `setup-banco.sh` - Script de setup automático
- ✅ `TESTES.md` - Guia completo de testes
- ✅ `SETUP.md` - Este arquivo

## 🎯 Ordem Recomendada

1. ✅ `./setup-banco.sh` - Criar banco e schema
2. ✅ Atualizar `DB_NAME` no `.env`
3. ✅ `node test-conexao.js` - Validar
4. ✅ `node test-municipios-simples.js` - Testar pipeline
5. 🚀 `npm run full-load -- --yes` - Quando tudo OK (10-15h)

---

Qualquer dúvida, consulte `TESTES.md` para mais detalhes! 🚀
