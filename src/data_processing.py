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
def create_or_replace_tables(df):
    start_time = time.time()

    # Read the database file path from the environment variable
    db_path = os.getenv('DB_PATH')

    try:
        # Establish a connection to the DuckDB database
        with duckdb.connect(database=db_path) as conn:
            # Drop the existing stocks_table if it exists
            conn.execute("DROP TABLE IF EXISTS stocks_table")
            logger.info("Tables dropped successfully.")

            # Create or replace the stocks_table
            chunksize = 1000  # Adjust the chunk size as needed

            dtype = {
            'abv': 'DOUBLE',
            'availability': 'VARCHAR',
            'code': 'VARCHAR',
            'country': 'VARCHAR',
            'type': 'VARCHAR',
            'url': 'VARCHAR',
            'price_gbp': 'DOUBLE',
            'price_ex_vat': 'DOUBLE',
            'price_incl_vat': 'DOUBLE',
            'size': 'VARCHAR',
            'style': 'VARCHAR',
            'title': 'VARCHAR',
            'vintage': 'VARCHAR',
            'import_date': 'DATE'}
            
            # Set parallelism if your system supports it
            conn.execute('PRAGMA threads=4;')  # Adjust number of threads based on your system

            start_time = time.time()

            df.to_sql('stocks_table', con=conn, index=False, if_exists='append', chunksize=chunksize,method='multi', dtype=dtype)

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
