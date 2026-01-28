import os
import boto3
import time
#from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv()

# Specify data local folder"
#local_folder = os.getenv('LOCAL_FOLDER')

# Specify the name of the S3 bucket
#api_files_bucket_name = os.getenv('API_FILES_BUCKET_NAME')

# Create an Athena client
athena_client = boto3.client('athena', region_name='eu-west-1')
s3_client = boto3.client('s3', region_name='eu-west-1')


def wait_for_query(query_execution_id, poll_interval=5, timeout_seconds=300):
    start_time = time.time()
    terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response["QueryExecution"]["Status"]["State"]
        if status in terminal_states:
            if status != "SUCCEEDED":
                reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                raise RuntimeError(f"Athena query {query_execution_id} {status}: {reason}")
            return
        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Athena query {query_execution_id} timed out.")
        time.sleep(poll_interval)

def clear_s3_prefix(s3_uri):
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got {s3_uri}")
    bucket, _, prefix = s3_uri[5:].partition("/")
    prefix = prefix.rstrip("/")
    list_prefix = f"{prefix}/" if prefix else ""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})

def athena_tables_creation():

# Define your SQL statements
    create_external_table_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS stocks_table (
    Code STRING,
    Title STRING,
    Size STRING,
    Style STRING,
    Country STRING,
    Group STRING,
    Available INT,
    `Price (GBP)` DOUBLE  -- Enclose column name with space in backticks
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LOCATION 's3://hedonism-wines-api-files/'
    TBLPROPERTIES ('skip.header.line.count'='1')
    """

    parquet_location = "s3://hedonism-wines-api-parquet/"
    create_parquet_table_sql = f"""
    CREATE TABLE IF NOT EXISTS stocks_table_parquet
    WITH (
    format = 'PARQUET',
    external_location = '{parquet_location}'
    ) AS
    SELECT * FROM hedonism_wines.stocks_table
    """

    # Execute SQL statements
    response = athena_client.start_query_execution(
        QueryString=create_external_table_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'  # Specify your Athena database
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'  # Specify an S3 location for query results
        }
    )
    wait_for_query(response['QueryExecutionId'])

    clear_s3_prefix(parquet_location)

    # Execute second SQL statement
    response = athena_client.start_query_execution(
        QueryString=create_parquet_table_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'  # Specify your Athena database
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'  # Specify an S3 location for query results
        }
    )
    wait_for_query(response['QueryExecutionId'])

    print("Athena tables and views created successfully.")
