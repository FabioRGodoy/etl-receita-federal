-- ============================================
-- Migração: Adicionar coluna last_modified_date
-- Para suportar DELTA baseado em data de modificação
-- ============================================

BEGIN;

-- Adicionar coluna de data de modificação do arquivo
ALTER TABLE etl_control_files
ADD COLUMN IF NOT EXISTS last_modified_date TIMESTAMP DEFAULT NULL;

-- Comentário explicativo
COMMENT ON COLUMN etl_control_files.last_modified_date IS 
'Data de última modificação do arquivo no servidor da Receita Federal (extraída do HTML)';

-- Índice para consultas de delta
CREATE INDEX IF NOT EXISTS idx_etl_files_last_modified 
ON etl_control_files(file_name, last_modified_date);

COMMIT;

-- Verificar resultado
SELECT 
  column_name, 
  data_type, 
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_name = 'etl_control_files'
  AND column_name = 'last_modified_date';

-- Mostrar contagem de registros sem data
SELECT 
  COUNT(*) as total_registros,
  COUNT(last_modified_date) as com_data,
  COUNT(*) - COUNT(last_modified_date) as sem_data
FROM etl_control_files;
