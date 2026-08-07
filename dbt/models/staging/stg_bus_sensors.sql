with source as (
    select * from {{ source('raw', 'raw_bus_sensors') }}
)

select
    sensor_id,
    bus_id,
    sensor_type,
    reading_value,
    unit,
    latitude,
    longitude,
    recorded_at,
    status,
    _ingestion_date
from source