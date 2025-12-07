CREATE OR REPLACE STORAGE INTEGRATION s3_coretelecoms_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::Your-Account-ID:role/SnowflakeReadS3Role'
    STORAGE_ALLOWED_LOCATIONS = ('*')
    STORAGE_BLOCKED_LOCATIONS = ('s3://coretelecom-terraform-state/');


DESC STORAGE INTEGRATION s3_coretelecoms_integration;

ALTER STORAGE INTEGRATION s3_coretelecoms_integration
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::Your-Account-ID:role/SnowflakeReadS3Role';