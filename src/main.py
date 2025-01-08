import api
import data_processing as dp
import s3upload as su
import email_alerting as ea
import athena_queries as aq
import queries as q
import pandas as pd
import os
from dotenv import load_dotenv
import time
import subprocess  # Import subprocess module to run shell commands
import duckdb


# Load environment variables from .env file
load_dotenv()

# Specify data local folders and S3 buckets
local_folder = os.getenv('LOCAL_FOLDER')
local_sales_folder = os.getenv('LOCAL_SALES_FOLDER')
api_files_bucket_name = os.getenv('API_FILES_BUCKET_NAME')
sales_files_bucket_name = os.getenv('SALES_FILES_BUCKET_NAME')
db_path = os.getenv('DB_PATH')


def check_db_lock():
    """Check if the DuckDB database is locked or accessible."""
    try:
        conn = duckdb.connect(database=db_path, read_only=False)  # Open DuckDB database
        conn.execute('SELECT 1')  # Run a simple query to check the database status
        conn.close()
        return True  # No issues, return True
    except Exception as e:
        print(f"Error encountered: {e}")
        return False  # Lock or connection issue


def process_api_data():
    """Fetch data from the API and upload it to S3."""
    url = 'https://hedonism.co.uk/sites/default/files/full-stock-list.csv'
    df = api.fetch_data_from_api(url)
    if df is not None:
        print("API data fetched successfully.")
        su.upload_files_to_s3(local_folder, api_files_bucket_name)
        print("API stocks data uploaded to S3 successfully.")
    else:
        print("Failed to fetch data from API.")
    return df


def process_sales_data():
    """Process sales data and upload to S3."""
    q.units_sold()
    su.upload_files_to_s3(local_sales_folder, sales_files_bucket_name)
    print("Sales data created and loaded to S3 successfully.")


def email_discount_alert():
    """Send email alerts for discounted items."""
    df = q.query_discounted_items()
    df = df[df['current_price'] <= 500].sort_values(by='current_price')

    if ea.is_dataframe_empty(df):
        subject = "No hedonism wine discounts today"
        body = "Sorry, no discounts today:\n\n" + str(df)
        ea.send_email(subject, body)
    else:
        html_table = df.to_html(index=False)
        subject = "Hedonism wine discounts for you today"
        ea.send_email(subject, html_table)


def run_streamlit():
    """Run the Streamlit app after the script completes."""
    time.sleep(5)  # Add a 5-second delay before running Streamlit
    subprocess.run(["streamlit", "run", "data_viz.py"])


def main():
    """Main function to execute the workflow."""
    if check_db_lock():
        process_api_data()
        df = dp.read_csv_files_in_folder(local_folder)
        print("Data read successfully.")

        dp.create_or_replace_tables(df)
        print("Data processed successfully.")

        process_sales_data()
        aq.athena_tables_creation()

        email_discount_alert()
        run_streamlit()
    else:
        print("Database is locked or unreachable. Please try again later.")


if __name__ == "__main__":
    main()
