import duckdb
import pandas as pd
from datetime import datetime
import os
import glob
import logging
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_csv_files_in_folder(folder_path):
    # Step 1: Read all files into dataframes
    csv_files = glob.glob(f"{folder_path}/*.csv")
    dataframes = []

    for file in csv_files:
        df = pd.read_csv(file)

        # Extract the date part of the file name
        file_name = os.path.basename(file)
        date_str = '_'.join(file_name.split('_')[-3:]).split('.')[0]
        date = pd.to_datetime(date_str, format='%Y_%m_%d')

        # Add a new column 'import_date' with the extracted date
        df['import_date'] = date.strftime('%Y-%m-%d')

        dataframes.append(df)

    # Step 2: Identify all unique columns across all dataframes
    all_columns = set()
    for df in dataframes:
        all_columns.update(df.columns)

    # Step 3: Standardize each dataframe to have all columns
    standardized_dataframes = []
    for df in dataframes:
        for column in all_columns:
            if column not in df.columns:
                df[column] = pd.NA
        standardized_dataframes.append(df[sorted(all_columns)])

    # Step 4: Concatenate all dataframes
    combined_df = pd.concat(standardized_dataframes, ignore_index=True)
    combined_df.rename(columns={'Code':'code',
                           'Title':'title',
                           'Size':'size',
                           'Style':'style',
                           'Country':'country',
                           'Group':'type',
                           'Available':'availability',
                           'Price (GBP)': 'price_gbp',
                           'ABV': 'abv',
                           'Link':'url',
                           'Price (ex-VAT)':'price_ex_vat',
                           'Price (inc VAT)': 'price_incl_vat',
                           'Vintage':'vintage'
                           }, inplace=True)
    
    logger.info("Dataframes combined successfully.")
    return combined_df

#@st.cache_data
def create_or_replace_tables(folder_path):
    start_time = time.time()

    # Read the database file path from the environment variable
    db_path = os.getenv('DB_PATH')

    try:
        # Establish a connection to the DuckDB database
        with duckdb.connect(database=db_path) as conn:
            # Set parallelism if your system supports it
            conn.execute('PRAGMA threads=4;')  # Adjust number of threads based on your system

            csv_glob = os.path.join(folder_path, "*.csv")
            start_time = time.time()
            conn.execute(
                """
                CREATE OR REPLACE TABLE stocks_table AS
                WITH raw AS (
                    SELECT *
                    FROM read_csv_auto(
                        ?,
                        union_by_name=true,
                        filename=true
                    )
                )
                SELECT
                    TRY_CAST("ABV" AS DOUBLE) AS abv,
                    "Available" AS availability,
                    "Code" AS code,
                    "Country" AS country,
                    "Group" AS type,
                    "Link" AS url,
                    TRY_CAST("Price (GBP)" AS DOUBLE) AS price_gbp,
                    TRY_CAST("Price (ex-VAT)" AS DOUBLE) AS price_ex_vat,
                    TRY_CAST("Price (inc VAT)" AS DOUBLE) AS price_incl_vat,
                    "Size" AS size,
                    "Style" AS style,
                    "Title" AS title,
                    "Vintage" AS vintage,
                    TRY_STRPTIME(
                        regexp_extract(filename, '(\\d{4}_\\d{2}_\\d{2})', 1),
                        '%Y_%m_%d'
                    )::DATE AS import_date
                FROM raw
                """,
                [csv_glob],
            )

            end_time = time.time()
            logger.info("Main table recreated taking %s seconds to run.", end_time - start_time)

            conn.execute("DROP VIEW IF EXISTS whisky_stocks_table")
            logger.info("whisky_stocks_table view dropped successfully.")
            # Create or replace the whisky_stocks_table view
            conn.execute("""CREATE OR REPLACE VIEW whisky_stocks_table AS 
                            SELECT 
                            abv,
                            availability,
                            code,
                            country,
                            type,
                            url,
                            COALESCE(price_gbp, price_incl_vat) AS price_gbp,
                            COALESCE(price_ex_vat,0) price_ex_vat,
                            COALESCE(price_incl_vat,0) price_incl_vat,
                            size,
                            style,
                            title,
                            vintage,
                            import_date
                            FROM stocks_table 
                            WHERE type = 'Whisky'""")
            
            conn.execute("DROP VIEW IF EXISTS whisky_stocks_table_today")
            logger.info("whisky_stocks_table_today view dropped successfully.")
            # Create or replace the whisky_stocks_table_today view
            conn.execute("""CREATE OR REPLACE VIEW whisky_stocks_table_today AS 
                            SELECT 
                            import_date, 
                            code,
                            title,
                            price_gbp,
                            url						  
                            FROM whisky_stocks_table 
                            WHERE import_date = CURRENT_DATE()
                            """)
            logger.info("Main views recreated.")
        
        logger.info("Tables created or replaced successfully.")
    except Exception as e:
        logger.exception("Error occurred: %s", e)
    end_time = time.time()
    logger.info("This function took %s seconds to run.", end_time - start_time)
