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
    drop_today_view_sql = """
    DROP VIEW IF EXISTS whisky_stocks_table_today
    """

    drop_whisky_view_sql = """
    DROP VIEW IF EXISTS whisky_stocks_table
    """

    drop_stocks_view_sql = """
    DROP VIEW IF EXISTS stocks_table
    """

    drop_stocks_table_sql = """
    DROP TABLE IF EXISTS stocks_table
    """

    drop_raw_table_sql = """
    DROP TABLE IF EXISTS stocks_table_raw
    """
    create_external_table_sql = """
    CREATE EXTERNAL TABLE IF NOT EXISTS stocks_table_raw (
    abv STRING,
    availability STRING,
    code STRING,
    country STRING,
    type STRING,
    url STRING,
    price_gbp STRING,
    price_ex_vat STRING,
    price_incl_vat STRING,
    size STRING,
    style STRING,
    title STRING,
    vintage STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
      'separatorChar' = ',',
      'quoteChar' = '"',
      'escapeChar' = '\\\\'
    )
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
    SELECT
        TRY_CAST(abv AS DOUBLE) AS abv,
        availability,
        code,
        country,
        type,
        url,
        TRY_CAST(price_gbp AS DOUBLE) AS price_gbp,
        TRY_CAST(price_ex_vat AS DOUBLE) AS price_ex_vat,
        TRY_CAST(price_incl_vat AS DOUBLE) AS price_incl_vat,
        size,
        style,
        title,
        vintage,
        DATE_PARSE(regexp_extract("$path", '(\\d{{4}}_\\d{{2}}_\\d{{2}})', 1), '%Y_%m_%d') AS import_date
    FROM hedonism_wines.stocks_table_raw
    """

    drop_parquet_table_sql = """
    DROP TABLE IF EXISTS stocks_table_parquet
    """

    create_stocks_view_sql = """
    CREATE VIEW stocks_table AS
    SELECT
        abv,
        availability,
        code,
        country,
        type,
        url,
        price_gbp,
        price_ex_vat,
        price_incl_vat,
        size,
        style,
        title,
        vintage,
        import_date
    FROM stocks_table_parquet
    """

    create_whisky_view_sql = """
    CREATE VIEW whisky_stocks_table AS
    SELECT
        abv,
        availability,
        code,
        country,
        type,
        url,
        COALESCE(price_gbp, price_incl_vat) AS price_gbp,
        COALESCE(price_ex_vat, 0) AS price_ex_vat,
        COALESCE(price_incl_vat, 0) AS price_incl_vat,
        size,
        style,
        title,
        vintage,
        import_date
    FROM stocks_table_parquet
    WHERE type = 'Whisky'
    """

    create_today_view_sql = """
    CREATE VIEW whisky_stocks_table_today AS
    SELECT
        import_date,
        code,
        title,
        price_gbp,
        url
    FROM whisky_stocks_table
    WHERE import_date = CURRENT_DATE
    """

    # Execute SQL statements
    response = athena_client.start_query_execution(
        QueryString=drop_today_view_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'
        }
    )
    wait_for_query(response['QueryExecutionId'])

    response = athena_client.start_query_execution(
        QueryString=drop_whisky_view_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'
        }
    )
    wait_for_query(response['QueryExecutionId'])

    response = athena_client.start_query_execution(
        QueryString=drop_stocks_view_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'
        }
    )
    wait_for_query(response['QueryExecutionId'])

    response = athena_client.start_query_execution(
        QueryString=drop_stocks_table_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'
        }
    )
    wait_for_query(response['QueryExecutionId'])

    response = athena_client.start_query_execution(
        QueryString=drop_raw_table_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'  # Specify your Athena database
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'  # Specify an S3 location for query results
        }
    )
    wait_for_query(response['QueryExecutionId'])

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

    response = athena_client.start_query_execution(
        QueryString=drop_parquet_table_sql,
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

    response = athena_client.start_query_execution(
        QueryString=create_stocks_view_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'
        }
    )
    wait_for_query(response['QueryExecutionId'])

    response = athena_client.start_query_execution(
        QueryString=create_whisky_view_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'
        }
    )
    wait_for_query(response['QueryExecutionId'])

    response = athena_client.start_query_execution(
        QueryString=create_today_view_sql,
        QueryExecutionContext={
            'Database': 'hedonism_wines'
        },
        ResultConfiguration={
            'OutputLocation': 's3://dario-athena-query-results/'
        }
    )
    wait_for_query(response['QueryExecutionId'])

    print("Athena tables and views created successfully.")
