import pandas as pd
from sqlalchemy import create_engine
import duckdb
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Specify the file path for the DuckDB database
db_path = os.getenv('DB_PATH')

# Establish a connection to an in-memory DuckDB database
conn = duckdb.connect(database=db_path, read_only=False)

def query_discounted_items():
    # Execute SQL queries to create a table only for whisky records
    results = conn.execute("""
            WITH current_price AS (
                SELECT code, price_gbp, import_date, title, url
                FROM whisky_stocks_table_today
            ),
            historical_max_price AS (
                SELECT code, MAX(price_gbp) AS max_price
                FROM whisky_stocks_table
                GROUP BY code
            ),
            output AS (
                SELECT  c.import_date AS current_date,
						c.code, 
                        c.title,
                        c.url,
                        c.price_gbp AS current_price,
                        m.max_price AS old_price,
                        m.max_price - c.price_gbp AS discount,
                        round(((m.max_price - c.price_gbp) / m.max_price),4) * 100 AS perc_saving
                FROM current_price c 
                JOIN historical_max_price m ON c.code = m.code
            )
            SELECT * FROM output WHERE discount > 0
    """).fetchdf()

    # Convert the results to a DataFrame
    df = pd.DataFrame(results)
        
    return df


def stocks_and_median_values():

	# Execute SQL queries to create a table only for whisky records
	results = conn.execute("""SELECT COUNT (*) stock_count,
	                          MEDIAN (price_gbp) median_price,
                              SUM (availability) total_availability,
	                          import_date
	                          FROM whisky_stocks_table 
	                          GROUP BY import_date
	                          ORDER BY 3 DESC
	                """).fetchdf()

	# Convert the results to a DataFrame
	df = pd.DataFrame(results)

	# Convert import_date to datetime
	df['import_date'] = pd.to_datetime(df['import_date'])

	# Extract date part
	df['import_date'] = df['import_date'].dt.date
	df['import_date'] = df['import_date'].astype(str).str[:10]

	return df

def price_search ():
	results = conn.execute("""SELECT 
						  import_date, 
						  code,
						  title,
						  price_gbp price_gbp,
						  url						  
                          FROM whisky_stocks_table_today 
                """).fetchdf()

	# Convert the results to a DataFrame
	df = pd.DataFrame(results)

	return df

#conn.close()