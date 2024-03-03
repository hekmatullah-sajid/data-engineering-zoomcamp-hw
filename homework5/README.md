# Module 5 - Batch Processing using Spark, PySpark, and GCP

## Overview

In this module, we'll explore batch processing techniques using Spark and pyspark in conjunction with Google Cloud Platform (GCP). The primary focus will be on processing FHV 2019-10 data using Spark.

## Scripts

### 1. Spark-HW-Local.ipynb

- Reads CSV FHV data and zone parquet data.
- Utilizes a local cluster for computation.
- Stores results locally.

### 2. Spark-HW-Dataproc.py

- Designed to run on a Dataproc cluster.
- Reads CSV and parquet files from Google Cloud Storage (GCS) and writes output back to GCS.
- Executable using the GCP console "submit job" option with parameters.

#### Running using GCP Console "Submit Job" Option:

1. Go to your GCP console and navigate to Dataproc.
2. Select your cluster or create a new one.
3. Click on "Jobs" tab.
4. Click on "Submit Job" button.
5. Choose "PySpark" as the job type.
6. Enter the location of the Python file (`Spark-HW-Dataproc.py`).
7. Add parameters:
   - `--input_fhv=gs://hs-de-zoomcamp/raw/fhv_tripdata_2019-10.csv`
   - `--input_zones=gs://hs-de-zoomcamp/pq/zones/part-00000-eafd7dbe-7f57-4696-9e10-89d8b95c2dfa-c000.snappy.parquet`
   - `--gcs_output=gs://hs-de-zoomcamp/output`

### 3. Spark-HW-Dataproc-BigQuery.py

- Executes on GCP.
- Reads CSV and parquet files from GCS.
- Writes results to both BigQuery and GCS.
- Executed using the `gcloud dataproc jobs submit pyspark` command.

#### Command:

```

gcloud dataproc jobs submit pyspark \
    --cluster=hs-de-zoomcamp-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar \
    gs://hs-de-zoomcamp/code/Spark-HW-Dataproc-BigQuery.py \
    -- \
        --input_fhv=gs://hs-de-zoomcamp/raw/fhv_tripdata_2019-10.csv \
		--input_zones=gs://hs-de-zoomcamp/pq/zones/part-00000-eafd7dbe-7f57-4696-9e10-89d8b95c2dfa-c000.snappy.parquet \
		--gcs_output=gs://hs-de-zoomcamp/output \
		--bq_output=nyc_fhv_data.fhv_oct_2019

```

## Setup

- Ensure all necessary dependencies are installed.
- For local execution, have Spark installed.
- For Dataproc and GCP execution, make sure you have access and permissions set up accordingly.

## Conclusion

These scripts provide different methods to process FHV data using Spark, demonstrating its flexibility in various environments and setups.
