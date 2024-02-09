-- SETUP: Create an external table using the Green Taxi Trip Records Data for 2022.
CREATE OR REPLACE EXTERNAL TABLE `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_external` 
OPTIONS (
  format='PARQUET',
  uris=['gs://hs-dez-taxi-trip-data-bucket/green-taxi/green_tripdata_2022-*.parquet']
);

-- SETUP: Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
CREATE OR REPLACE TABLE `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned`
AS SELECT * FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_external`;

-- Q1: What is count of records for the 2022 Green Taxi Data?
-- query:
SELECT COUNT(1) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_external`;

-- Q2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
-- query:
SELECT COUNT(DISTINCT PULocationID) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_external`;
-- query:
SELECT COUNT(DISTINCT PULocationID) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned`;

-- Q3: How many records have a fare_amount of 0?
-- query:
SELECT COUNT(1) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned` WHERE fare_amount = 0;

-- Q4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
-- query:
CREATE OR REPLACE TABLE `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_partitoned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS SELECT * FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned`;

-- Q5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
-- query:
SELECT DISTINCT(PULocationID) 
FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned` 
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
-- query:
SELECT DISTINCT(PULocationID) 
FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_partitoned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- (Bonus: Not worth points) Q8: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
-- query:
SELECT count(*) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned`;
-- BigQuery estimate it will read 0 Bytes. Estimating the number of records in a BigQuery table does not incur any byte processing costs. This is because BigQuery uses metadata about the table, like its size and schema, to make this estimation. Since it doesn't actually read the data, there are no bytes processed.
