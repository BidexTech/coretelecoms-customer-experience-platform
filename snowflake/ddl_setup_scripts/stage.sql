USE DATABASE CORE_TELECOMS;
USE SCHEMA RAW;

CREATE OR REPLACE STAGE coretelecoms_stage
    URL = 's3://coretelecoms-datalake-raw/raw/'
    STORAGE_INTEGRATION = s3_coretelecoms_integration
    FILE_FORMAT = (TYPE = PARQUET);


LIST @coretelecoms_stage;