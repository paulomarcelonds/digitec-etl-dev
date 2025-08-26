-- Criar schema dev se não existir
CREATE SCHEMA IF NOT EXISTS dev;

-- Criar tabela atlas no schema dev
CREATE TABLE dev.atlas (
    tipo VARCHAR(35),
    modelo VARCHAR(50),
    codigo_item_jde VARCHAR(25),
    codigo_material_sap VARCHAR(25),
    numero_serie VARCHAR(65),
    enderecavel_principal VARCHAR(55),
    operacao VARCHAR(50),
    nome_local VARCHAR(50),
    perfil VARCHAR(50),
    codigo_fornecedor_jde VARCHAR(50),
    codigo_fornecedor_sap VARCHAR(50),
    estado VARCHAR(50),
    data_ultima_alteracao TIMESTAMP,
    responsavel VARCHAR(50),
    numero_contrato VARCHAR(50),
    classificacao_material VARCHAR(50),
    empresa_material VARCHAR(50),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criar índices para melhor performance
CREATE INDEX idx_atlas_numero_serie ON dev.atlas(numero_serie);
CREATE INDEX idx_atlas_enderecavel_principal ON dev.atlas(enderecavel_principal);
CREATE INDEX idx_atlas_data_alteracao ON dev.atlas(data_ultima_alteracao);
CREATE INDEX idx_atlas_estado ON dev.atlas(estado);
CREATE INDEX idx_atlas_codigo_item_jde ON dev.atlas(codigo_item_jde);
CREATE INDEX idx_atlas_codigo_material_sap ON dev.atlas(codigo_material_sap);