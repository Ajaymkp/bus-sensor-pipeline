with ranked as (
    select
        bus_id,
        sensor_type,
        reading_value,
        unit,
        latitude,
        longitude,
        status,
        recorded_at,
        row_number() over (
            partition by bus_id, sensor_type
            order by recorded_at desc
        ) as rn
    from {{ ref('stg_bus_sensors') }}
)

select
    bus_id,
    sensor_type,
    reading_value,
    unit,
    latitude,
    longitude,
    status,
    recorded_at
from ranked
where rn = 1