COPY INTO RAW.CUSTOMERS_RAW
FROM @raw.coretelecoms_stage
PATTERN = 'customers/.*/.*[.]parquet$'
FILE_FORMAT = (TYPE = PARQUET);

COPY INTO RAW.AGENTS_RAW
FROM @raw.coretelecoms_stage
PATTERN = 'agents/.*/.*[.]parquet$'
FILE_FORMAT = (TYPE = PARQUET);

COPY INTO RAW.CALL_CENTER_LOGS_RAW
FROM @raw.coretelecoms_stage
PATTERN = 'call_center_logs/.*/.*[.]parquet$'
FILE_FORMAT = (TYPE = PARQUET);

COPY INTO RAW.SOCIAL_MEDIA_RAW
FROM @raw.coretelecoms_stage
PATTERN = 'social_media/.*/.*[.]parquet$'
FILE_FORMAT = (TYPE = PARQUET);

COPY INTO RAW.WEBSITE_COMPLAINTS_RAW
FROM @raw.coretelecoms_stage
PATTERN = 'website_complaints/.*/.*[.]parquet$'
FILE_FORMAT = (TYPE = PARQUET);

