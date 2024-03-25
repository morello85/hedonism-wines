import duckdb
import pandas as pd
from datetime import datetime
import os

def process_data(df):
    
    # Read the database file path from the environment variable
    #db_path = os.environ.get('DB_PATH')

    # Specify the file path for the DuckDB database
    db_path = '/Users/MacUser/hedonism-wines_app/database.db'  # Example path, replace with your desired path

    # Establish a connection to an in-memory DuckDB database
    conn = duckdb.connect(database=db_path, read_only=False)

    # Execute SQL queries to create a table only for whisky records
#    conn.execute("""CREATE OR REPLACE TABLE whisky_stocks_table AS 
 #               SELECT * FROM stocks_table 
  #              WHERE type = 'Whisky'""")

    today_date = datetime.now().strftime('%Y-%m-%d')

    # Execute DELETE query
    delete_query = f"DELETE FROM stocks_table WHERE import_date = '{today_date}'"

    # Execute the query
    conn.execute(delete_query)

    # Commit the changes
    conn.commit()

    # Close connection
    conn.close()

    conn = duckdb.connect(database=db_path, read_only=False)

    # Get the column names from the DataFrame
    columns = list(df.columns)

    # Generate the list of column names for the INSERT INTO statement
    column_names = ", ".join(columns)

    # Generate the list of parameter placeholders (?, ?, ?) for the VALUES clause
    parameter_placeholders = ", ".join(["?" for _ in range(len(columns))])

    # Convert the DataFrame to records list
    records = df.values.tolist()

    # Define the name of your existing table
    table_name = 'stocks_table'

    # Construct the SQL INSERT INTO statement dynamically
    sql_insert = f"INSERT INTO {table_name} ({column_names}) VALUES ({parameter_placeholders})"

    # Execute the INSERT statement
    conn.executemany(sql_insert, records)

    # Commit the transaction (optional, depending on your needs)
    conn.commit()

    # Execute SQL queries to create a table only for whisky records
    conn.execute("""CREATE OR REPLACE TABLE whisky_stocks_table AS 
                    SELECT * FROM stocks_table 
                    WHERE type = 'Whisky'""")

    # Close the connection
    conn.close()