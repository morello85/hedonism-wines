import os
import boto3
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Specify data local folder"
local_folder = os.getenv('LOCAL_FOLDER')

# Specify the name of the S3 bucket
bucket_name = os.getenv('BUCKET_NAME')

# Create an Athena client
athena_client = boto3.client('athena', region_name='eu-west-1')

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

    create_parquet_table_sql = """
    CREATE TABLE IF NOT EXISTS stocks_table_parquet
    WITH (
    format = 'PARQUET',
    external_location = 's3://hedonism-wines-api-parquet/'
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

    # # Wait for the query execution to complete
    # query_execution_id = response['QueryExecutionId']
    # # Define the terminal states
    # terminal_states = ['SUCCEEDED', 'FAILED', 'CANCELLED']

    # while True:
    #     # Get the query execution status
    #     response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    #     status = response['QueryExecution']['Status']['State']

    #     # Check if the query execution is in a terminal state
    #     if status in terminal_states:
    #         break

    #     # Wait for a few seconds before checking again
    #     time.sleep(5)

    # # Once the query execution is in a terminal state, continue with your logic
    # athena_client.get_waiter('query_execution_completed').wait(QueryExecutionId=query_execution_id)

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

    # Wait for the query execution to complete
    query_execution_id = response['QueryExecutionId']
    #athena_client.get_waiter('query_execution_completed').wait(QueryExecutionId=query_execution_id)

    print("Athena tables and views created successfully.")
