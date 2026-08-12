USE DATABASE bus_censor_db;
USE SCHEMA raw;

COPY INTO raw_bus_sensors
FROM @bus_censor_raw_stage
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT COUNT(*) FROM raw_bus_sensors;