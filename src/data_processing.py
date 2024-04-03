import api
import duckdb
import pandas as pd
from datetime import datetime
import os
import glob
import logging
import warnings

# Suppress all warnings
warnings.filterwarnings("ignore")

folder_path = '/Users/MacUser/hedonism-wines_app/data'


def read_csv_files_in_folder(folder_path):
    # Initialize an empty list to store DataFrames
    dfs = []
    # Use glob to get a list of all CSV files in the folder
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

    for file in csv_files:
        df = pd.read_csv(file)
    # Extract the date from the file name
        file_name = os.path.basename(file)
        date_str = '_'.join(file_name.split('_')[-3:])  # Extract the date part of the file name
        date_str = date_str.split('.')[0]  # Remove the .csv extension
        date = pd.to_datetime(date_str, format='%Y_%m_%d')  # Convert the date string to a datetime object

        # Add a new column 'import_date' with the extracted date
        df['import_date'] = date

        # Format the date in the DataFrame to display as 'YYYY-MM-DD'
        df['import_date'] = df['import_date'].dt.strftime('%Y-%m-%d')

        #print (file,df.columns)
        # Append the DataFrame to the list
        dfs.append(df)

    # Read each CSV file into a DataFrame and concatenate them
    combined_df = pd.concat(dfs, ignore_index=False)
    #print (combined_df.info())
    combined_df.rename(columns={'Code':'code',
                           'Title':'title',
                           'Size':'size',
                           'Style':'style',
                           'Country':'country',
                           'Group':'type',
                           'Available':'availability',
                           'Price (GBP)': 'price_gbp'}, inplace=True)

    # Trim white space from title column
    combined_df['title'] = combined_df['title'].apply(str.strip)
    # Reconstruct url
    combined_df['url'] = 'https://hedonism.co.uk/product/' + combined_df['title'].str.replace(' ', '-').str.lower() + '-whisky'

    return combined_df

def create_or_replace_tables(df):
    db_path = '/Users/MacUser/hedonism-wines_app/database.db'

    try:
        # Establish a connection to the DuckDB database
        with duckdb.connect(database=db_path) as conn:
            # Drop the existing stocks_table if it exists
            conn.execute("DROP TABLE IF EXISTS stocks_table")
            print ("Tables dropped successfully.")

            # Create or replace the stocks_table
            df.to_sql('stocks_table', con=conn, index=False, if_exists='replace')

            # Create or replace the whisky_stocks_table view
            conn.execute("""CREATE OR REPLACE VIEW whisky_stocks_table AS 
                            SELECT * FROM stocks_table 
                            WHERE type = 'Whisky'""")
        
        print("Tables created or replaced successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")


# def process_data(df):
    
#     # Read the database file path from the environment variable
#     #db_path = os.environ.get('DB_PATH')

#     # Specify the file path for the DuckDB database
#     db_path = '/Users/MacUser/hedonism-wines_app/database.db'  # Example path, replace with your desired path

#     # Establish a connection to an in-memory DuckDB database
#     conn = duckdb.connect(database=db_path, read_only=False)

#     today_date = datetime.now().strftime('%Y-%m-%d')  

#     # Execute DELETE queries and commit the changes
#     delete_query_1 = f"DELETE FROM stocks_table WHERE import_date = '{today_date}'"
#     conn.execute(delete_query_1)

#     delete_query_2 = f"DELETE FROM whisky_stocks_table"
#     conn.execute(delete_query_2)

#     # Close connection
#     #conn.close()

#     #conn = duckdb.connect(database=db_path, read_only=False)

#     # Get the column names from the DataFrame
#     columns = list(df.columns)

#     # Generate the list of column names for the INSERT INTO statement
#     column_names = ", ".join(columns)

#     # Generate the list of parameter placeholders (?, ?, ?) for the VALUES clause
#     parameter_placeholders = ", ".join(["?" for _ in range(len(columns))])

#     # Convert the DataFrame to records list
#     records = df.values.tolist()

#     # Define the name of your existing table
#     table_name = 'stocks_table'

#     # Construct the SQL INSERT INTO statement dynamically
#     sql_insert = f"INSERT INTO {table_name} ({column_names}) VALUES ({parameter_placeholders})"

#     # Execute the INSERT statement
#     conn.executemany(sql_insert, records)

#     # Commit the transaction (optional, depending on your needs)
#     conn.commit()

#     # results = conn.execute("""SELECT 
# 		# 				  import_date, 
# 		# 				  code,
# 		# 				  title,
# 		# 				  price_gbp price_gbp,
# 		# 				  url						  
#     #                       FROM stocks_table 
# 		# 				  WHERE import_date = CURRENT_DATE()
#     #                       AND code = 'HED84172'
#     #                       ORDER BY price_gbp DESC
#     #             """).fetchdf()

# 	  # # Convert the results to a DataFrame
#     # df = pd.DataFrame(results)
#     # print (df)

#     # Execute SQL queries to create a table only for whisky records
#     conn.execute("""CREATE OR REPLACE TABLE whisky_stocks_table AS 
#                     SELECT * FROM stocks_table 
#                     WHERE type = 'Whisky'""")
    
#     # Close the connection
#     conn.close()