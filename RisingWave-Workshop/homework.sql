-- Stream processing in SQL with RisingWave - Homework

-- Create the materialized view for Question 1

CREATE MATERIALIZED VIEW trip_time_stats AS
SELECT
    tz1.Zone AS pickup_zone,
    tz2.Zone AS dropoff_zone,
    AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))) AS avg_trip_time,
    MIN(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))) AS min_trip_time,
    MAX(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))) AS max_trip_time
FROM
    trip_data td
JOIN
    taxi_zone tz1 ON td.PULocationID = tz1.location_id
JOIN
    taxi_zone tz2 ON td.DOLocationID = tz2.location_id
GROUP BY
    tz1.Zone, tz2.Zone;

-- Read the data from the materialized view for Question 1

SELECT * FROM trip_time_stats ORDER BY avg_trip_time DESC LIMIT 1;


-- Create the materialized view for Question 2

CREATE MATERIALIZED VIEW trip_count_time_stats AS
SELECT
    tz1.Zone AS pickup_zone,
    tz2.Zone AS dropoff_zone,
    AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))) AS avg_trip_time,
    MIN(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))) AS min_trip_time,
    MAX(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))) AS max_trip_time,
    COUNT(*) AS trip_count
FROM
    trip_data td
JOIN
    taxi_zone tz1 ON td.PULocationID = tz1.location_id
JOIN
    taxi_zone tz2 ON td.DOLocationID = tz2.location_id
GROUP BY
    tz1.Zone, tz2.Zone;

-- Read the data from the materialized view for Question 2

SELECT * FROM trip_count_time_stats ORDER BY avg_trip_time DESC LIMIT 1;


-- Create the materialized view for Question 3

CREATE MATERIALIZED VIEW trip_busy_zones_stats AS
SELECT
    tz.Zone AS pickup_zone,
    COUNT(*) AS trip_count
FROM
    trip_data td
JOIN
    taxi_zone tz ON td.PULocationID = tz.location_id

WHERE
    tpep_pickup_datetime >= (SELECT MAX(tpep_pickup_datetime) - INTERVAL '17 hours' FROM trip_data)
GROUP BY
    tz.Zone;

-- Read the data from the materialized view for Question 3

SELECT * FROM trip_busy_zones_stats ORDER BY trip_count DESC LIMIT 3;
