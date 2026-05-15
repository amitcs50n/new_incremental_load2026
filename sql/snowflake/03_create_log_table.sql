

USE ROLE SYSADMIN;
USE DATABASE PHARMA_DB;

CREATE SCHEMA IF NOT EXISTS METADATA;

USE SCHEMA PHARMA_DB.METADATA;


CREATE OR REPLACE TABLE pipeline_run_log (
    log_id              NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    run_id              VARCHAR(100)  NOT NULL,        -- ADF's @pipeline().RunId, or 'local-<uuid>'
    pipeline_name       VARCHAR(100)  NOT NULL,
    layer               VARCHAR(20)   NOT NULL,        -- 'DATABRICKS' or 'ADF'
    activity_name       VARCHAR(100),
    source_table        VARCHAR(100),
    status              VARCHAR(20)   NOT NULL,        -- STARTED / SUCCEEDED / FAILED / NO_DATA
    start_time          TIMESTAMP_NTZ NOT NULL,
    end_time            TIMESTAMP_NTZ,
    rows_read           NUMBER(38,0),
    rows_inserted       NUMBER(38,0),
    rows_updated        NUMBER(38,0),
    rows_deleted        NUMBER(38,0),
    bytes_processed     NUMBER(38,0),
    error_code          VARCHAR(50),
    error_message       VARCHAR(16777216),
    error_stack_trace   VARCHAR(16777216),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    duration_seconds    NUMBER(38,0) AS (DATEDIFF(second, start_time, end_time))
);
