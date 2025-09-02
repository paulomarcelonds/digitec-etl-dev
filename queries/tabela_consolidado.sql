CREATE SCHEMA IF NOT EXISTS dev;

CREATE TABLE dev.consolidado (
    fornecedor           VARCHAR(15),
    sla                  VARCHAR(15),
    status               VARCHAR(15),
    contrato             VARCHAR(15),
    wo                   VARCHAR(15),
    tp_wo                VARCHAR(15),
    tecnico              VARCHAR(15),
    data_atendimento     TIMESTAMP,
    municipio            VARCHAR(15),
    material             VARCHAR(15),
    descricao            VARCHAR(15),
    quantidade_lancada   INT,
    un                   VARCHAR(15),
    ingestion_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
