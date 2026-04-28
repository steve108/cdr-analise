-- =============================================================================
-- CDR Analise — Schema MySQL
-- =============================================================================
-- Criação das tabelas de controle e dados brutos de CDR.
-- Execute com um usuário que tenha privilégios CREATE TABLE / CREATE VIEW.
-- =============================================================================

CREATE TABLE IF NOT EXISTS cdr_import_batch (
    batch_id        BIGINT        NOT NULL AUTO_INCREMENT,
    zip_filename    VARCHAR(255)  NOT NULL,
    zip_path        VARCHAR(1000) NOT NULL,
    zip_hash        CHAR(64)      NOT NULL COMMENT 'SHA-256 do arquivo ZIP',
    brand           VARCHAR(100)  NOT NULL,
    reference_date  DATE          NOT NULL COMMENT 'Data extraída do nome do ZIP',
    reference_hour  CHAR(4)       NOT NULL COMMENT 'HHMM extraído do nome do ZIP',
    status          ENUM('pending','processing','done','error') NOT NULL DEFAULT 'pending',
    total_files     INT           NULL,
    total_rows      BIGINT        NULL,
    imported_rows   BIGINT        NULL,
    error_message   TEXT          NULL,
    started_at      DATETIME      NULL,
    finished_at     DATETIME      NULL,
    created_at      DATETIME      NOT NULL,
    PRIMARY KEY (batch_id),
    UNIQUE KEY uq_zip_hash (zip_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS cdr_import_file (
    import_file_id     BIGINT        NOT NULL AUTO_INCREMENT,
    batch_id           BIGINT        NOT NULL,
    csv_filename       VARCHAR(255)  NOT NULL,
    service_type       ENUM('DATA','SMS','VOICE') NOT NULL,
    direction          ENUM('MO','MT')            NOT NULL,
    status             ENUM('pending','processing','done','error') NOT NULL DEFAULT 'pending',
    total_rows         BIGINT        NULL,
    imported_rows      BIGINT        NULL,
    last_line_imported BIGINT        NULL COMMENT 'Última linha confirmada no banco — usada para retomar importação',
    error_message      TEXT          NULL,
    started_at         DATETIME      NULL,
    finished_at        DATETIME      NULL,
    created_at         DATETIME      NOT NULL,
    PRIMARY KEY (import_file_id),
    UNIQUE KEY uq_batch_csv (batch_id, csv_filename),
    CONSTRAINT fk_file_batch
        FOREIGN KEY (batch_id) REFERENCES cdr_import_batch (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS cdr_raw (
    cdr_raw_id                          BIGINT         NOT NULL AUTO_INCREMENT,
    batch_id                            BIGINT         NOT NULL,
    import_file_id                      BIGINT         NOT NULL,
    service_type                        ENUM('DATA','SMS','VOICE') NOT NULL,
    direction                           ENUM('MO','MT')            NOT NULL,

    -- Campos originais do CSV
    id_cdr                              VARCHAR(100)   NULL,
    id_brand                            VARCHAR(100)   NULL,
    brand_id                            VARCHAR(100)   NULL,
    id_subscription                     VARCHAR(100)   NULL,
    subscription_id                     VARCHAR(100)   NULL,
    subscription_type                   VARCHAR(100)   NULL,
    msisdn                              VARCHAR(30)    NULL,
    imsi                                VARCHAR(30)    NULL,
    originator                          VARCHAR(100)   NULL,
    originator_hidden                   VARCHAR(10)    NULL,
    destination                         VARCHAR(100)   NULL,
    day                                 VARCHAR(20)    NULL,
    start_date                          VARCHAR(30)    NULL COMMENT 'Armazenado como string raw do CSV (formato esperado: YYYY-MM-DD HH:MM:SS)',
    parse_day                           VARCHAR(20)    NULL,
    parse_date                          VARCHAR(30)    NULL,
    traffic_units                       DECIMAL(20,4)  NULL COMMENT 'DATA=bytes, VOICE=segundos, SMS=unidades',
    traffic_units_rated_session         DECIMAL(20,4)  NULL,
    amount_charged                      DECIMAL(20,6)  NULL,
    concept                             VARCHAR(255)   NULL,
    discount                            DECIMAL(20,6)  NULL,
    package_id                          VARCHAR(100)   NULL,
    package_instance_id                 VARCHAR(100)   NULL,
    rating_pack_ref                     VARCHAR(100)   NULL,
    price_plan_ref                      VARCHAR(100)   NULL,
    price_ref                           VARCHAR(100)   NULL,
    subscription_balance                DECIMAL(20,6)  NULL,
    rating_pack_traffic_limit_status    VARCHAR(50)    NULL,
    rating_pack_amount_limit_status     VARCHAR(50)    NULL,
    traffic_units_sub_session           DECIMAL(20,4)  NULL,
    traffic_units_rated_sub_session     DECIMAL(20,4)  NULL,
    amount_charged_sub_session          DECIMAL(20,6)  NULL,
    balance_sub_session                 DECIMAL(20,6)  NULL,
    subscription_location               VARCHAR(100)   NULL,
    location                            VARCHAR(100)   NULL,
    gprs_station_id                     VARCHAR(100)   NULL,

    -- Controle de importação
    raw_line_number                     BIGINT         NOT NULL COMMENT 'Número da linha no CSV (1-based, sem o header)',
    raw_hash                            CHAR(64)       NOT NULL COMMENT 'SHA-256 do conteúdo da linha — garante idempotência',
    raw_data_json                       JSON           NOT NULL COMMENT 'Linha completa original em JSON para auditoria',
    created_at                          DATETIME       NOT NULL,

    PRIMARY KEY (cdr_raw_id),
    UNIQUE KEY uq_raw_hash (raw_hash),
    KEY idx_batch_id      (batch_id),
    KEY idx_import_file   (import_file_id),
    KEY idx_msisdn        (msisdn),
    KEY idx_service_dir   (service_type, direction),
    KEY idx_start_date    (start_date),
    CONSTRAINT fk_raw_batch
        FOREIGN KEY (batch_id)        REFERENCES cdr_import_batch (batch_id),
    CONSTRAINT fk_raw_file
        FOREIGN KEY (import_file_id)  REFERENCES cdr_import_file  (import_file_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- =============================================================================
-- Views de consolidação mensal
-- Premissa: start_date armazenado como 'YYYY-MM-DD ...' (ISO-like).
-- LEFT(start_date, 7) extrai 'YYYY-MM'.
-- =============================================================================

CREATE OR REPLACE VIEW vw_cdr_monthly_by_line AS
SELECT
    LEFT(start_date, 7)                                                          AS `year_month`,
    msisdn,
    SUM(CASE WHEN service_type = 'DATA'  THEN COALESCE(traffic_units, 0) ELSE 0 END)       AS data_bytes,
    SUM(CASE WHEN service_type = 'DATA'  THEN COALESCE(traffic_units, 0) ELSE 0 END)
        / 1048576                                                                AS data_mb,
    SUM(CASE WHEN service_type = 'DATA'  THEN COALESCE(traffic_units, 0) ELSE 0 END)
        / 1073741824                                                             AS data_gb,
    SUM(CASE WHEN service_type = 'VOICE' THEN COALESCE(traffic_units, 0) ELSE 0 END)       AS voice_seconds,
    SUM(CASE WHEN service_type = 'VOICE' THEN COALESCE(traffic_units, 0) ELSE 0 END) / 60  AS voice_minutes,
    SUM(CASE WHEN service_type = 'SMS'   THEN COALESCE(traffic_units, 0) ELSE 0 END)       AS sms_total,
    SUM(COALESCE(amount_charged, 0))                                             AS amount_charged
FROM cdr_raw
WHERE start_date IS NOT NULL
GROUP BY LEFT(start_date, 7), msisdn;


CREATE OR REPLACE VIEW vw_cdr_monthly_summary AS
SELECT
    LEFT(start_date, 7)                                                          AS `year_month`,
    COUNT(*)                                                                     AS total_lines,
    SUM(CASE WHEN service_type = 'DATA'  THEN COALESCE(traffic_units, 0) ELSE 0 END)       AS data_bytes,
    SUM(CASE WHEN service_type = 'DATA'  THEN COALESCE(traffic_units, 0) ELSE 0 END)
        / 1048576                                                                AS data_mb,
    SUM(CASE WHEN service_type = 'DATA'  THEN COALESCE(traffic_units, 0) ELSE 0 END)
        / 1073741824                                                             AS data_gb,
    SUM(CASE WHEN service_type = 'VOICE' THEN COALESCE(traffic_units, 0) ELSE 0 END)       AS voice_seconds,
    SUM(CASE WHEN service_type = 'VOICE' THEN COALESCE(traffic_units, 0) ELSE 0 END) / 60  AS voice_minutes,
    SUM(CASE WHEN service_type = 'SMS'   THEN COALESCE(traffic_units, 0) ELSE 0 END)       AS sms_total,
    SUM(COALESCE(amount_charged, 0))                                             AS amount_charged
FROM cdr_raw
WHERE start_date IS NOT NULL
GROUP BY LEFT(start_date, 7);
