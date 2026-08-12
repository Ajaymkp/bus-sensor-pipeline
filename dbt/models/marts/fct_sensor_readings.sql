{{ config(materialized='table') }}

select
    date(recorded_at) as reading_date,
    sensor_type,
    count(*) as total_readings,
    round(avg(reading_value), 2) as avg_value,
    min(reading_value) as min_value,
    max(reading_value) as max_value,
    count_if(status = 'WARNING') as warning_count
from {{ ref('stg_bus_sensors') }}
group by 1, 2

