-- ============================================
-- ETL Receita Federal - Schema V1
-- ============================================

-- Tabela de controle de arquivos processados
CREATE TABLE IF NOT EXISTS etl_control_files (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) UNIQUE NOT NULL,
    file_url TEXT NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    file_year INTEGER,
    file_month INTEGER,
    load_type VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_status ON etl_control_files(status);
CREATE INDEX IF NOT EXISTS idx_etl_type_date ON etl_control_files(file_type, file_year, file_month);

-- Tabela de controle de execuções
CREATE TABLE IF NOT EXISTS etl_control_runs (
    id SERIAL PRIMARY KEY,
    run_type VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL,
    total_files INTEGER,
    files_completed INTEGER DEFAULT 0,
    files_failed INTEGER DEFAULT 0,
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    error_message TEXT
);

-- ============================================
-- Tabela de Municípios (Auxiliar)
-- ============================================
CREATE TABLE IF NOT EXISTS municipios (
    codigo_municipio INTEGER PRIMARY KEY,
    nome_municipio VARCHAR(200) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- Tabela de Estabelecimentos (Principal)
-- ============================================
CREATE TABLE IF NOT EXISTS estabelecimentos (
    id BIGSERIAL PRIMARY KEY,
    cnpj_basico VARCHAR(8) NOT NULL,
    cnpj_ordem VARCHAR(4) NOT NULL,
    cnpj_dv VARCHAR(2) NOT NULL,
    cnpj VARCHAR(14) NOT NULL UNIQUE,
    identificador_matriz_filial INTEGER,
    nome_fantasia VARCHAR(255),
    situacao_cadastral INTEGER,
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral INTEGER,
    nome_cidade_exterior VARCHAR(255),
    codigo_pais INTEGER,
    data_inicio_atividade DATE,
    cnae_fiscal_principal INTEGER,
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro VARCHAR(100),
    logradouro VARCHAR(255),
    numero VARCHAR(50),
    complemento VARCHAR(255),
    bairro VARCHAR(100),
    cep VARCHAR(8),
    uf VARCHAR(2),
    codigo_municipio INTEGER,
    ddd1 VARCHAR(4),
    telefone1 VARCHAR(20),
    ddd2 VARCHAR(4),
    telefone2 VARCHAR(20),
    ddd_fax VARCHAR(4),
    fax VARCHAR(20),
    correio_eletronico VARCHAR(255),
    situacao_especial VARCHAR(255),
    data_situacao_especial DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_municipio FOREIGN KEY (codigo_municipio) 
        REFERENCES municipios(codigo_municipio) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj ON estabelecimentos(cnpj);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj_basico ON estabelecimentos(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_situacao ON estabelecimentos(situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_municipio ON estabelecimentos(codigo_municipio);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_uf ON estabelecimentos(uf);

-- ============================================
-- Tabela de Sócios
-- ============================================
CREATE TABLE IF NOT EXISTS socios (
    id BIGSERIAL PRIMARY KEY,
    cnpj_basico VARCHAR(8) NOT NULL,
    identificador_socio INTEGER,
    nome_socio VARCHAR(255),
    cpf_cnpj_socio VARCHAR(14),
    qualificacao_socio INTEGER,
    data_entrada_sociedade DATE,
    codigo_pais INTEGER,
    cpf_representante_legal VARCHAR(11),
    nome_representante_legal VARCHAR(255),
    qualificacao_representante_legal INTEGER,
    faixa_etaria INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_socio_cnpj_basico ON socios(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_socio_cpf_cnpj ON socios(cpf_cnpj_socio);
CREATE INDEX IF NOT EXISTS idx_socio_nome ON socios(nome_socio);

-- ============================================
-- Comentários das tabelas
-- ============================================
COMMENT ON TABLE etl_control_files IS 'Controle de arquivos processados pelo ETL';
COMMENT ON TABLE etl_control_runs IS 'Controle de execuções do ETL (FULL ou DELTA)';
COMMENT ON TABLE municipios IS 'Tabela auxiliar de municípios';
COMMENT ON TABLE estabelecimentos IS 'Dados de estabelecimentos (CNPJ)';
COMMENT ON TABLE socios IS 'Dados de sócios e representantes legais';
