import pandas as pd
import duckdb
from datetime import datetime
from pathlib import Path
from typing import Optional
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Specify the file path for the DuckDB database
db_path = os.getenv('DB_PATH')
if not db_path:
    raise ValueError(
        "DB_PATH is required for DuckDB storage. Set it to a secure, "
        "non-world-writable location (e.g., /var/lib/hedonism-wines/database.duckdb)."
    )

def query_discounted_items():
    """Query discounted items in the whisky stocks."""
    with duckdb.connect(database=db_path, read_only=False) as conn:
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

    df = pd.DataFrame(results)
    return df


def stocks_and_median_values():
    """Get stock count and median price by import date."""
    with duckdb.connect(database=db_path, read_only=False) as conn:
        results = conn.execute("""
            SELECT COUNT (*) stock_count,
                   MEDIAN (CAST(price_gbp AS FLOAT)) median_price,
                   SUM (CAST(availability AS FLOAT)) total_availability,
                   import_date
            FROM whisky_stocks_table 
            GROUP BY import_date
            ORDER BY 3 DESC
        """).fetchdf()

    df = pd.DataFrame(results)
    df['import_date'] = pd.to_datetime(df['import_date']).dt.date.astype(str).str[:10]
    return df


def stocks_and_median_values_by_code():
    """Get stock and median values by code."""
    with duckdb.connect(database=db_path, read_only=False) as conn:
        results = conn.execute("""
            WITH x AS (
                SELECT
                MEDIAN (CAST(price_gbp AS FLOAT)) median_price,
                SUM (CAST(availability AS FLOAT)) total_availability,
                import_date,
                code
                FROM whisky_stocks_table 
                GROUP BY import_date, code
                ORDER BY 3 DESC),
            y AS (
                SELECT COUNT (DISTINCT median_price) price_changes_count,
                code
                FROM x
                GROUP BY code)
            SELECT x.*, y.price_changes_count
            FROM x INNER JOIN y ON x.code = y.code
        """).fetchdf()

    df = pd.DataFrame(results)
    df['import_date'] = pd.to_datetime(df['import_date']).dt.date.astype(str).str[:10]
    return df


def units_sold(output_folder: Optional[Path] = None):
    """Get the units sold for the current and previous day."""
    with duckdb.connect(database=db_path, read_only=False) as conn:
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

    df = pd.DataFrame(results)
    today_date_file_name = datetime.now().strftime("_%Y_%m_%d")
    if output_folder:
        output_folder.mkdir(parents=True, exist_ok=True)
        filename = f"sales{today_date_file_name}.csv"
        file_path = output_folder / filename
        df.to_csv(file_path, index=False)
    df['import_date'] = pd.to_datetime(df['import_date']).dt.date.astype(str).str[:10]
    return df


def price_search():
    """Search for prices in the whisky stock table."""
    with duckdb.connect(database=db_path, read_only=False) as conn:
        results = conn.execute("""
            SELECT 
                import_date, 
                code,
                title,
                price_gbp price_gbp,
                url
            FROM whisky_stocks_table_today 
        """).fetchdf()

    df = pd.DataFrame(results)
    return df


def main():
    # Your main logic for processing
    discounted_items = query_discounted_items()
    print(discounted_items)
    stocks_data = stocks_and_median_values()
    print(stocks_data)
    # Continue with the rest of the logic


if __name__ == "__main__":
    main()
