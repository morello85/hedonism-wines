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

# # Load environment variables from .env file
load_dotenv()

# Specify data local folder"
local_folder = os.getenv('LOCAL_FOLDER')

# Specify the name of the S3 bucket
bucket_name = os.getenv('BUCKET_NAME')
# #bucket_name = "hedonism-wines-api-files"

def main():
    url = 'https://hedonism.co.uk/sites/default/files/full-stock-list.csv'
    df = api.fetch_data_from_api(url)
    if df is not None:
        print("Data fetched successfully.")
                # Save data to s3
        su.upload_files_to_s3(local_folder, bucket_name)
        print ("Data uploaded to s3 successfully.")
    else:
        print("Failed to fetch data from API.")
        # Process the data
    
    df = dp.read_csv_files_in_folder(local_folder)
    print ("Data read successfully.")

    dp.create_or_replace_tables(df)
    print("Data processed successfully.")

    # create athena tables
    aq.athena_tables_creation()

    #Url validation
    df = q.price_search()
    urls_to_validate = df['url'].tolist()
    uv.validate_urls(urls_to_validate)
    print ("URLs validated.")

    #Alerting by email
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