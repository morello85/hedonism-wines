import pandas as pd
from sqlalchemy import create_engine
import duckdb
from datetime import datetime
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
			ORDER BY discount DESC
    """).fetchdf()

    # Convert the results to a DataFrame
    df = pd.DataFrame(results)
        
    return df


def stocks_and_median_values():

	# Execute SQL queries to create a table only for whisky records
	results = conn.execute("""SELECT COUNT (*) stock_count,
	                          MEDIAN (CAST(price_gbp AS FLOAT)) median_price,
                              SUM (CAST(availability AS FLOAT)) total_availability,
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

def units_sold():
	results = conn.execute("""
                       WITH todays_items AS (             
                        SELECT code, title, url, price_gbp, availability, import_date 
                        FROM whisky_stocks_table
                        WHERE import_date = CURRENT_DATE()
                       ),
                       yesterdays_items AS (
                        SELECT code, title, url, price_gbp, availability, import_date  
                        FROM whisky_stocks_table
                        WHERE import_date = CURRENT_DATE() -1
                       )
                       SELECT CAST(CURRENT_DATE() AS DATE) AS import_date, a.code, 
						a.title, 
						a.url, 
						a.price_gbp,
						a.today_availability availability,
                        CAST(a.yesterday_availability AS FLOAT) - CAST(a.today_availability AS FLOAT) units_sold
                       FROM 
                       (
                       SELECT 
                       CAST (y.code AS STRING) ||'-'|| CAST (y.availability AS STRING) yesterday_code_availability,
                       CAST (t.code AS STRING) ||'-'|| CAST (t.availability AS STRING) today_code_availability,
                       y.code,
                       y.title,
                       y.url,
                       y.price_gbp,
                       y.availability yesterday_availability,
                       t.availability today_availability
                       FROM yesterdays_items y LEFT OUTER JOIN todays_items t
                       ON y.code = t.code
                       ) a
                       WHERE a.today_code_availability <> yesterday_code_availability
					   AND CAST(a.yesterday_availability AS FLOAT) - CAST(a.today_availability AS FLOAT) > 0
                       ORDER BY price_gbp DESC
                """).fetchdf()

	# Convert the results to a DataFrame
	df = pd.DataFrame(results)
	
    # Get today's date
	today_date_file_name = datetime.now().strftime("_%Y_%m_%d")

    # Define filename with today's date appended
	filename = f"sales{today_date_file_name}.csv"  # Change "data" to your desired filename prefix
        
    # Define the path where you want to save the file
	folder_path = "/Users/MacUser/hedonism-wines_app/sales_data/"  # Change this to your desired folder path
	
    # Export dataframe
	df.to_csv(folder_path + filename, index=False)

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