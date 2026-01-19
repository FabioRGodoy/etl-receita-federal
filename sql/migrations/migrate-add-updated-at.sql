-- ============================================
-- Migração: Adicionar coluna updated_at
-- ============================================

-- Adicionar coluna updated_at na tabela etl_control_files (se não existir)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'etl_control_files' 
        AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE etl_control_files 
        ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();
        
        RAISE NOTICE 'Coluna updated_at adicionada com sucesso!';
    ELSE
        RAISE NOTICE 'Coluna updated_at já existe.';
    END IF;
END $$;
