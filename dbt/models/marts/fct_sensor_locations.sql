select distinct
    bus_id,
    latitude,
    longitude,
    status,
    recorded_at
from {{ ref('stg_bus_sensors') }}
where latitude between -90 and 90
  and longitude between -180 and 180