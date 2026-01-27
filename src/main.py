import api
import data_processing as dp
import s3upload as su
import email_alerting as ea
import athena_queries as aq
import queries as q
from config import load_settings
import os
import time
import subprocess  # Import subprocess module to run shell commands

settings = load_settings()

def process_api_data():
    """Fetch data from the API and upload it to S3."""
    url = 'https://hedonism.co.uk/full-stock-list.csv'
    #url = 'https://hedonism.co.uk/sites/default/files/full-stock-list.csv'
    df = api.fetch_data_from_api(url, settings.local_folder)
    if df is not None:
        print("API data fetched successfully.")
        su.upload_files_to_s3(settings.local_folder, settings.api_files_bucket_name)
        print("API stocks data uploaded to S3 successfully.")
    else:
        print("Failed to fetch data from API.")
    return df


def process_sales_data():
    """Process sales data and upload to S3."""
    q.units_sold(settings.local_sales_folder)
    su.upload_files_to_s3(settings.local_sales_folder, settings.sales_files_bucket_name)
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
    if os.getenv("SKIP_STREAMLIT", "").lower() in {"1", "true", "yes"}:
        print("Skipping Streamlit launch because SKIP_STREAMLIT is set.")
        return
    time.sleep(5)  # Add a 5-second delay before running Streamlit
    subprocess.run(["streamlit", "run", "data_viz.py"])


def main():
    """Main function to execute the workflow."""
    start_time = time.time()

    process_api_data()
    df = dp.read_csv_files_in_folder(settings.local_folder)
    print("Data read successfully.")
    
    dp.create_or_replace_tables(settings.local_folder)
    print("Data processed successfully.")

    process_sales_data()
    aq.athena_tables_creation()

    email_discount_alert()
    run_streamlit()
    end_time = time.time()
    print(f"Full pipeline completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
