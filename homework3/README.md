## Week 3 Homework
ATTENTION: At the end of the submission form, you will be required to include a link to your GitHub repository or other public code-hosting site. This repository should contain your code for solving the homework. If your solution includes code that is not in file format (such as SQL queries or shell commands), please include these directly in the README file of your repository.

<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>

<b>The external table can be created using the following query:</b></br>
> query:
```
CREATE OR REPLACE EXTERNAL TABLE `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_external` 
OPTIONS (
  format='PARQUET',
  uris=['gs://hs-dez-taxi-trip-data-bucket/green-taxi/green_tripdata_2022-*.parquet']
);
```

Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>

<b>The materialized table can be created using the following query:</b></br>
> query:
```
CREATE OR REPLACE TABLE `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned`
AS SELECT * FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_external`;
```
</p>

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- **840,402**
- 1,936,423
- 253,647

## Answer 1:
> The correct option is 840,402.
> query:
```
SELECT COUNT(1) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_external`;
```
> Note: The number of rows in a BigQuery table can also be read from the Details tab, under the Storage Information section. This information only exists for non-external tables.

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- **0 MB for the External Table and 6.41MB for the Materialized Table**
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

## Answer 2:
> The correct option is (0 MB for the External Table and 6.41MB for the Materialized Table).
> queries:
```
SELECT COUNT(DISTINCT PULocationID) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_external`;
```
```
SELECT COUNT(DISTINCT PULocationID) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned`;
```

## Question 3:
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- **1,622**

## Answer 3:
> The correct option is (1,622).
> query:
```
SELECT COUNT(1) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned` WHERE fare_amount = 0;
```

## Question 4:
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- **Partition by lpep_pickup_datetime  Cluster on PUlocationID**
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

## Answer 4:
> The correct option is (Partition by lpep_pickup_datetime  Cluster on PUlocationID).
> query:
```
CREATE OR REPLACE TABLE `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_partitoned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS SELECT * FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned`;
```

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- **12.82 MB for non-partitioned table and 1.12 MB for the partitioned table**
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

## Answer 5:
> The correct option is (12.82 MB for non-partitioned table and 1.12 MB for the partitioned table).
> queries:
```
SELECT DISTINCT(PULocationID) 
FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned` 
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```
```
SELECT DISTINCT(PULocationID) 
FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_partitoned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```

## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- **GCP Bucket**
- Big Table
- Container Registry

## Answer 6:
> The correct option is (GCP Bucket).
> Note:
> In BigQuery, external tables do not store data in BigQuery itself. Rather, external tables are virtual representations of data stored in external storage systems like Google Cloud Storage, Google Drive, BigTable, or Cloud SQL. 
> For the mentioned table, the data is Parquet files in the GCS bucket.

## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- **False**

## Answer 7: 
> The correct option is (False).
> Note:
> Clustering data in BigQuery isn't always necessary due to factors such as small dataset size, frequent changes, cost considerations, query patterns and data structure. 

## (Bonus: Not worth points) Question 8:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

## Answer 8: 
> query:
```
SELECT count(*) FROM `hs-dez-taxi-trip-data-project.ny_taxi.green_taxi_trips_non_partitioned`;
```
> BigQuery estimate it will read 0 Bytes. Estimating the number of records in a BigQuery table does not incur any byte processing costs. This is because BigQuery uses metadata about the table, like its size and schema, to make this estimation. Since it doesn't actually read the data, there are no bytes processed.