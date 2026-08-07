CREATE OR REPLACE STAGE bus_censor_raw_stage
  URL = 's3://mkp-s3-data/bus-censor/raw/'
  STORAGE_INTEGRATION = s3_bus_censor_integration
  FILE_FORMAT = (TYPE = PARQUET);

LIST @bus_censor_raw_stage;