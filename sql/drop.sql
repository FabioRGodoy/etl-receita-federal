-- ============================================
-- ETL Receita Federal - Drop Tables
-- CUIDADO: Este script apaga todos os dados!
-- ============================================

DROP TABLE IF EXISTS socios CASCADE;
DROP TABLE IF EXISTS estabelecimentos CASCADE;
DROP TABLE IF EXISTS municipios CASCADE;
DROP TABLE IF EXISTS etl_control_runs CASCADE;
DROP TABLE IF EXISTS etl_control_files CASCADE;

-- Recriar as tabelas
\i schema.sql
