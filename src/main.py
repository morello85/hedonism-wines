import api
import url_validation as uv
import data_processing as dp
import s3upload as su
import email_alerting as ea
import athena_queries as aq
import queries as q
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Specify data local folders
local_folder = os.getenv('LOCAL_FOLDER')
local_sales_folder = os.getenv('LOCAL_SALES_FOLDER')

# Specify the name of the S3 buckets
api_files_bucket_name = os.getenv('API_FILES_BUCKET_NAME')
sales_files_bucket_name = os.getenv('SALES_FILES_BUCKET_NAME')

def main():
    url = 'https://hedonism.co.uk/sites/default/files/full-stock-list.csv'
    df = api.fetch_data_from_api(url)
    if df is not None:
        print("API data fetched successfully.")
        # Save stocks and sales data to s3
        su.upload_files_to_s3(local_folder, api_files_bucket_name)
        print ("API stocks data uploaded to s3 successfully.")
    else:
        print("Failed to fetch data from API.")
    
    # Process the data
    df = dp.read_csv_files_in_folder(local_folder)
    print ("Data read successfully.")

    dp.create_or_replace_tables(df)
    print("Data processed successfully.")

    # Export sales data
    q.units_sold()
    su.upload_files_to_s3(local_sales_folder,sales_files_bucket_name)
    print ("Sales data created and loaded to s3 successfully.")

    # Create athena tables
    aq.athena_tables_creation()

    # Url validation
    df = q.price_search()
    urls_to_validate = df['url'].tolist()
    uv.validate_urls(urls_to_validate)
    print ("URLs validated.")

    # Alerting by email
    df = q.query_discounted_items()

    if ea.is_dataframe_empty(df):
        subject = "No hedonism wine discounts today"
        body = "Sorry no discounts today:\n\n" + str(df)
        ea.send_email(subject, body)
    else:
        # Convert DataFrame to HTML table
        html_table = df.to_html(index=False)
        subject = "hedonism wine discounts for you today"
        ea.send_email(subject, html_table)
    
if __name__ == "__main__":
    main()