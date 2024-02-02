import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# JSON user key for authentication with GCP
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/my-gcp-secrets.json"
bucket_name = 'hs-de-zoomcamp-mage-gcs-m2' # GCS bucker name
project_id = 'qwiklabs-gcp-04-e6c6c8f906ce' # GCS project id
table_name = 'nyc_green_taxi_data' # Folder name in bucket where your data will be stored

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):

    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'], # Partitioning data using lpep_pickup_date date column
        filesystem=gcs
    )
    