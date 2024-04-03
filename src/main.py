import api
import data_processing as dp
import data_viz as dv
import s3upload as su
import email_alerting as ea
import queries as q
import pandas as pd
from tabulate import tabulate
import streamlit as st



# Specify data local folder"
local_folder = "/Users/MacUser/hedonism-wines_app/data"

# Specify the name of the S3 bucket
bucket_name = "hedonism-wines-api-files"

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

    dv.visualise_discounted_items()
    dv.visualise_stocks_and_median_values()
    dv.visualise_price_search()

    # # # Rendering for streamlit app
    # # #st.title("My Streamlit App")
    # # #st.write("This is a Streamlit app!")

    # # Alerting by email
    df = q.query_discounted_items()

    if ea.is_dataframe_empty(df):
        subject = "No hedonism wine discounts today"
        body = "Sorry no discounts today:\n\n" + str(df)
        ea.send_email(subject, body)
    else:
        plain_text_table = tabulate(df, headers='keys', tablefmt='fancy_grid')
        # Convert DataFrame to HTML table
        html_table = df.to_html(index=False)
        subject = "hedonism wine discounts for you today"
        ea.send_email(subject, html_table)
        #body = "Check these out:\n\n" + plain_text_table
        #ea.send_email(subject, body)
    
if __name__ == "__main__":
    main()