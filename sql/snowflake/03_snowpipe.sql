USE SCHEMA raw;

CREATE OR REPLACE PIPE bus_censor_raw_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO raw_bus_sensors
  FROM @bus_censor_raw_stage
  FILE_FORMAT = (TYPE = PARQUET)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT SYSTEM$PIPE_STATUS('bus_censor_raw_pipe');