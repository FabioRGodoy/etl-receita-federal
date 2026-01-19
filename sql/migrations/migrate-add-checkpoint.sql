-- Migração: Adicionar coluna checkpoint para resume capability

BEGIN;

-- Adicionar coluna checkpoint (JSONB para flexibilidade)
ALTER TABLE etl_control_files
ADD COLUMN IF NOT EXISTS checkpoint JSONB DEFAULT NULL;

-- Índice para consultas por checkpoint
CREATE INDEX IF NOT EXISTS idx_etl_files_checkpoint 
ON etl_control_files USING gin(checkpoint);

COMMIT;

-- Verificar resultado
SELECT 
  column_name, 
  data_type, 
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_name = 'etl_control_files'
  AND column_name = 'checkpoint';
