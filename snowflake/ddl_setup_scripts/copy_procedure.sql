-- COPY INTO RAW.CUSTOMERS_RAW
-- FROM @raw.coretelecoms_stage
-- PATTERN = 'customers/.*/.*[.]parquet$'
-- FILE_FORMAT = (TYPE = PARQUET);

-- COPY INTO RAW.AGENTS_RAW
-- FROM @raw.coretelecoms_stage
-- PATTERN = 'agents/.*/.*[.]parquet$'
-- FILE_FORMAT = (TYPE = PARQUET);

-- COPY INTO RAW.CALL_CENTER_LOGS_RAW
-- FROM @raw.coretelecoms_stage
-- PATTERN = 'call_center_logs/.*/.*[.]parquet$'
-- FILE_FORMAT = (TYPE = PARQUET);

-- COPY INTO RAW.SOCIAL_MEDIA_RAW
-- FROM @raw.coretelecoms_stage
-- PATTERN = 'social_media/.*/.*[.]parquet$'
-- FILE_FORMAT = (TYPE = PARQUET);

-- COPY INTO RAW.WEBSITE_COMPLAINTS_RAW
-- FROM @raw.coretelecoms_stage
-- PATTERN = 'website_complaints/.*/.*[.]parquet$'
-- FILE_FORMAT = (TYPE = PARQUET);

-- The below code creates a stored procedure to automate the above COPY commands

USE DATABASE CORE_TELECOMS;
USE SCHEMA RAW;

-- Grant usage on the procedure specifically
GRANT USAGE ON PROCEDURE  CORE_TELECOMS.RAW.INGEST_TELECOMS_RAW_DATA() TO ROLE ETL_ROLE;

CREATE OR REPLACE PROCEDURE INGEST_TELECOMS_RAW_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- 1. Load Customers
    COPY INTO RAW.CUSTOMERS_RAW
    FROM @raw.coretelecoms_stage
    PATTERN = 'customers/.*/.*[.]parquet$'
    FILE_FORMAT = (TYPE = PARQUET);

    -- 2. Load Agents
    COPY INTO RAW.AGENTS_RAW
    FROM @raw.coretelecoms_stage
    PATTERN = 'agents/.*/.*[.]parquet$'
    FILE_FORMAT = (TYPE = PARQUET);

    -- 3. Load Call Center Logs
    COPY INTO RAW.CALL_CENTER_LOGS_RAW
    FROM @raw.coretelecoms_stage
    PATTERN = 'call_center_logs/.*/.*[.]parquet$'
    FILE_FORMAT = (TYPE = PARQUET);

    -- 4. Load Social Media
    COPY INTO RAW.SOCIAL_MEDIA_RAW
    FROM @raw.coretelecoms_stage
    PATTERN = 'social_media/.*/.*[.]parquet$'
    FILE_FORMAT = (TYPE = PARQUET);

    -- 5. Load Website Complaints
    COPY INTO RAW.WEBSITE_COMPLAINTS_RAW
    FROM @raw.coretelecoms_stage
    PATTERN = 'website_complaints/.*/.*[.]parquet$'
    FILE_FORMAT = (TYPE = PARQUET);

    RETURN 'Success: All raw tables ingested.';
END;
$$;

CALL RAW.INGEST_TELECOMS_RAW_DATA();

SHOW PROCEDURES IN SCHEMA RAW;