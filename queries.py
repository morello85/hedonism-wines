import pandas as pd
from sqlalchemy import create_engine
import duckdb


# Specify the file path for the DuckDB database
db_path = '/Users/MacUser/hedonism-wines_app/database.db'  # Example path, replace with your desired path

# Establish a connection to an in-memory DuckDB database
conn = duckdb.connect(database=db_path, read_only=False)

def query_discounted_items():

	# Execute SQL queries to create a table only for whisky records
	results = conn.execute("""SELECT 
	                          code,
	                          title,
	                          url,
	                          max (import_date) todays_date,
	                          min (price_gbp) min_price,
	                          max (price_gbp) max_price,
	                          max (price_gbp) -  min (price_gbp) AS price_diff
	                          FROM whisky_stocks_table
	                          GROUP BY code, title, url
	                          HAVING max (price_gbp) - min (price_gbp) >0
	                          ORDER BY price_diff
	                """).fetchdf()

	# Convert the results to a DataFrame
	df = pd.DataFrame(results)
		
	return df


def stocks_and_median_values():

	# Execute SQL queries to create a table only for whisky records
	results = conn.execute("""SELECT COUNT (*) stock_count,
	                          MEDIAN (price_gbp) median_price,
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
	results = conn.execute("""SELECT MAX (price_gbp) price_gbp,
                          title
                          FROM whisky_stocks_table 
                          GROUP BY title
                          ORDER BY 1 DESC
                """).fetchdf()

	# Convert the results to a DataFrame
	df = pd.DataFrame(results)

	return df


#conn.close()