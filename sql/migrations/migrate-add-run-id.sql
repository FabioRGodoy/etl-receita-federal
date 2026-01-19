-- Migração: Adicionar coluna run_id para associar arquivos aos runs

BEGIN;

-- Adicionar coluna run_id
ALTER TABLE etl_control_files
ADD COLUMN IF NOT EXISTS run_id INTEGER;

-- Adicionar foreign key (se não existir)
DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_etl_files_run'
  ) THEN
    ALTER TABLE etl_control_files
    ADD CONSTRAINT fk_etl_files_run
    FOREIGN KEY (run_id) REFERENCES etl_control_runs(id)
    ON DELETE SET NULL;
  END IF;
END $$;

-- Índice para buscar arquivos por run
CREATE INDEX IF NOT EXISTS idx_etl_files_run_id 
ON etl_control_files(run_id);

COMMIT;

-- Verificar
SELECT 
  column_name, 
  data_type, 
  is_nullable
FROM information_schema.columns
WHERE table_name = 'etl_control_files'
  AND column_name = 'run_id';
