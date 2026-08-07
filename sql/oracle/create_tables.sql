-- first "docker oracle start" inside terminal to start the Oracle container
-- then run this script to create the table and insert 1000 fake rows inside terminal 

docker exec -i oracle sqlplus SYSTEM/admin@XE << 'EOF'
-- Create the table
CREATE TABLE bus_sensors (
    sensor_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bus_id VARCHAR2(20) NOT NULL,
    sensor_type VARCHAR2(50),
    reading_value NUMBER(10,4),
    unit VARCHAR2(20),
    latitude NUMBER(10,6),
    longitude NUMBER(10,6),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR2(20) DEFAULT 'ACTIVE'
);

-- Insert 1000 fake rows
INSERT INTO bus_sensors (bus_id, sensor_type, reading_value, unit, latitude, longitude, status)
SELECT 
    'BUS-' || LPAD(ROWNUM, 3, '0'),
    CASE MOD(ROWNUM, 5) 
        WHEN 0 THEN 'SPEED' 
        WHEN 1 THEN 'TEMPERATURE' 
        WHEN 2 THEN 'VIBRATION' 
        WHEN 3 THEN 'FUEL_LEVEL' 
        ELSE 'ENGINE_RPM' 
    END,
    ROUND(DBMS_RANDOM.VALUE(10, 120), 2),
    CASE MOD(ROWNUM, 5) 
        WHEN 0 THEN 'KM/H' 
        WHEN 1 THEN 'CELSIUS' 
        WHEN 2 THEN 'G_FORCE' 
        WHEN 3 THEN 'PERCENT' 
        ELSE 'RPM' 
    END,
    ROUND(DBMS_RANDOM.VALUE(12.90, 13.10), 6),
    ROUND(DBMS_RANDOM.VALUE(77.50, 77.70), 6),
    CASE WHEN DBMS_RANDOM.VALUE(0,1) > 0.9 THEN 'WARNING' ELSE 'ACTIVE' END
FROM dual
CONNECT BY ROWNUM <= 1000;

COMMIT;
EXIT;
EOF