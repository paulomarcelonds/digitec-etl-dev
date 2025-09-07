CREATE SCHEMA IF NOT EXISTS dev;

CREATE TABLE dev.atendimento (
    sku             VARCHAR(21) NOT NULL,
    qtd             INT,
    descricao       VARCHAR(25),
    parceira        VARCHAR(50),
    atlas           VARCHAR(50),
    tecnico         VARCHAR(21) NOT NULL,
    data            TIMESTAMP NOT NULL,
    movimento       VARCHAR(21) NOT NULL,
    sap             VARCHAR(21),
    id_tecnico      VARCHAR(21),
    modalidade      VARCHAR(21),
    responsavel     VARCHAR(21),
    cat_material    VARCHAR(21),
    observacao      VARCHAR(21),
    modelo          VARCHAR(50),
    ingestion_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
