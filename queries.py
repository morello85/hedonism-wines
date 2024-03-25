import pandas as pd
from sqlalchemy import create_engine
import duckdb


# Specify the file path for the DuckDB database
db_path = '/Users/MacUser/hedonism-wines_app/database.db'  # Example path, replace with your desired path

# Establish a connection to an in-memory DuckDB database
conn = duckdb.connect(database=db_path, read_only=False)

def query_discounted_items():
    # Execute SQL queries to create a table only for whisky records
    results = conn.execute("""
        WITH current_price AS (
            SELECT code, price_gbp, import_date, title, url
            FROM whisky_stocks_table
            WHERE import_date = CURRENT_DATE()
        ),
        minimum_price AS (
            SELECT code, price_gbp, import_date
            FROM (
                SELECT code, 
                       RANK() OVER (PARTITION BY code ORDER BY price_gbp ASC) rank,
                       price_gbp,
                       import_date
                FROM whisky_stocks_table
            ) ranked
            WHERE rank = 1
        ),
        previous_price AS (
            SELECT code, price_gbp, min(import_date) import_date
            FROM (
                SELECT code, 
                       RANK() OVER (PARTITION BY code ORDER BY price_gbp ASC) rank,
                       price_gbp,
                       import_date import_date
                FROM whisky_stocks_table
            ) ranked
            WHERE rank = 2
            GROUP BY code, price_gbp
        ),
        output AS (
            SELECT  c.code, 
                    c.title,
                    c.url,
                    c.price_gbp as current_minimum_price, 
                    c.import_date as current_date,
                    p.price_gbp as previous_price,
                    p.import_date as previous_date,
                    p.price_gbp - m.price_gbp as price_diff,
                    ((p.price_gbp - m.price_gbp)/p.price_gbp)*100 AS perc_saving
            FROM current_price c 
            JOIN minimum_price m ON c.code = m.code AND c.price_gbp = m.price_gbp
            JOIN previous_price p ON c.code = p.code
        )
        SELECT * FROM output WHERE price_diff > 0
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