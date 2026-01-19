-- Migração para corrigir constraints ON CONFLICT
-- Adiciona constraints UNIQUE corretas para as tabelas

-- 1. ESTABELECIMENTOS
-- O CNPJ completo é formado por: cnpj_basico + cnpj_ordem + cnpj_dv
-- Remover constraint antiga se existir
ALTER TABLE IF EXISTS estabelecimentos DROP CONSTRAINT IF EXISTS estabelecimentos_cnpj_key;

-- Adicionar constraint composta correta
ALTER TABLE estabelecimentos 
ADD CONSTRAINT estabelecimentos_cnpj_composto_key 
UNIQUE (cnpj_basico, cnpj_ordem, cnpj_dv);

-- 2. SOCIOS
-- A chave única para sócios é: cnpj_basico + identificador_socio + cpf_cnpj_socio
ALTER TABLE socios 
ADD CONSTRAINT socios_identificacao_key 
UNIQUE (cnpj_basico, identificador_socio, cpf_cnpj_socio);

-- 3. MUNICIPIOS
-- Codigo do municipio já deve ser único, garantir PRIMARY KEY ou UNIQUE
-- Usar CASCADE pois existe FK em estabelecimentos
ALTER TABLE municipios 
DROP CONSTRAINT IF EXISTS municipios_pkey CASCADE;

ALTER TABLE municipios 
ADD CONSTRAINT municipios_pkey 
PRIMARY KEY (codigo_municipio);

-- Recriar FK em estabelecimentos
ALTER TABLE estabelecimentos
ADD CONSTRAINT fk_municipio 
FOREIGN KEY (codigo_municipio) 
REFERENCES municipios(codigo_municipio);

-- Verificar constraints criadas
SELECT 
    tc.table_name, 
    tc.constraint_name, 
    tc.constraint_type,
    STRING_AGG(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = 'public'
    AND tc.table_name IN ('estabelecimentos', 'socios', 'municipios')
    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
GROUP BY tc.table_name, tc.constraint_name, tc.constraint_type
ORDER BY tc.table_name, tc.constraint_type;
