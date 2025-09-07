CREATE SCHEMA IF NOT EXISTS dev;

CREATE TABLE dev.servico_retirada (
    data      TIMESTAMP,
    contrato  VARCHAR(25),
    cop       VARCHAR(25),
    tecnico   VARCHAR(25),
    servico   VARCHAR(50),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
