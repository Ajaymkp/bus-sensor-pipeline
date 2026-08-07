USE SCHEMA raw;

CREATE OR REPLACE TABLE raw_bus_sensors (
    sensor_id NUMBER,
    bus_id VARCHAR(20),
    sensor_type VARCHAR(50),
    reading_value NUMBER(10,4),
    unit VARCHAR(20),
    latitude NUMBER(10,6),
    longitude NUMBER(10,6),
    recorded_at TIMESTAMP_NTZ,
    status VARCHAR(20),
    _ingestion_date DATE DEFAULT CURRENT_DATE(),
    _file_name VARCHAR(500),
    _file_row_number NUMBER
);