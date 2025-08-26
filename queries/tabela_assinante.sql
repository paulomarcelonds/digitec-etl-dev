-- Criar schema dev se não existir
CREATE SCHEMA IF NOT EXISTS dev;

-- Criar tabela assinante no schema dev
CREATE TABLE dev.assinante (
    serie_ender_princ VARCHAR(60),
    tipo VARCHAR(25),
    modelo VARCHAR(50),
    material_sap VARCHAR(25),
    item_jde VARCHAR(50),
    estado VARCHAR(50),
    local VARCHAR(50),
    tipo_local VARCHAR(50),
    perfil_local VARCHAR(50),
    contrato_netsms VARCHAR(50),
    tipo_contrato_uno VARCHAR(50),
    operacao VARCHAR(50),
    empresa_material VARCHAR(50),
    tipo_mercadoria VARCHAR(50),
    reusos VARCHAR(25),
    data_alteracao TIMESTAMP,
    responsavel VARCHAR(50),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- Criar índices
CREATE INDEX idx_assinante_serie ON dev.assinante(serie_ender_princ);
CREATE INDEX idx_assinante_data ON dev.assinante(data_alteracao);
CREATE INDEX idx_assinante_local ON dev.assinante(local);