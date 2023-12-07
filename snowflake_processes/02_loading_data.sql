CREATE TABLE JOBS.data ( 
    index INTEGER,
    company_name STRING,
    job_title STRING,
    location STRING,
    company_domain STRING,
    job_linkedin_url VARCHAR,
    posted_on TIMESTAMP_TZ,
    job_id STRING,
    city STRING,
    state STRING,
    month STRING
);

-- -- Set up environment variables for AWS credentials and S3 bucket name
-- SET AWS_ACCESS_KEY = SYSTEM$SET_ENV_VARIABLE('AWS_ACCESS_KEY', 'AKIAQE5K2OLW7ZD2RDRI');
-- SET AWS_SECRET_KEY = SYSTEM$SET_ENV_VARIABLE('AWS_SECRET_KEY', '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');
-- SET S3_BUCKET_NAME = SYSTEM$SET_ENV_VARIABLE('S3_BUCKET_NAME', 'final-project-7245');

-- -- Creating an external stage using environment variables
-- CREATE OR REPLACE STAGE stage1
-- URL = 's3://' || :S3_BUCKET_NAME || '/your-path'
-- CREDENTIALS = (AWS_KEY_ID = :AWS_ACCESS_KEY AWS_SECRET_KEY = :AWS_SECRET_KEY);

-- -- Using the stage in a COPY INTO command
-- COPY INTO DATA
-- FROM @stage/Cleaned.csv
-- FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"')
-- ON_ERROR = 'CONTINUE';
