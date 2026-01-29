import os
import subprocess  # Import subprocess module to run shell commands
import time
from datetime import datetime, timezone
from pathlib import Path

import api
import athena_queries as aq
import data_processing as dp
import email_alerting as ea
import queries as q
import s3upload as su
from config import load_settings


def process_api_data(settings):
    """Fetch data from the API and upload it to S3."""
    url = 'https://hedonism.co.uk/full-stock-list.csv'
    #url = 'https://hedonism.co.uk/sites/default/files/full-stock-list.csv'
    df = api.fetch_data_from_api(url, settings.local_folder)
    if df is not None:
        print("API data fetched successfully.")
        if settings.api_files_bucket_name:
            su.upload_files_to_s3(settings.local_folder, settings.api_files_bucket_name)
            print("API stocks data uploaded to S3 successfully.")
        else:
            print("Skipping API S3 upload because API_FILES_BUCKET_NAME is missing.")
    else:
        print("Failed to fetch data from API.")
    return df


def process_sales_data(settings):
    """Process sales data and upload to S3."""
    q.units_sold(settings.local_sales_folder)
    if settings.sales_files_bucket_name:
        su.upload_files_to_s3(settings.local_sales_folder, settings.sales_files_bucket_name)
        print("Sales data created and loaded to S3 successfully.")
    else:
        print("Skipping sales S3 upload because SALES_FILES_BUCKET_NAME is missing.")


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


def write_last_refresh(timestamp_path: Path) -> None:
    """Write the last refresh timestamp to disk."""
    timestamp_path.write_text(
        datetime.now(timezone.utc).isoformat(),
        encoding="utf-8",
    )


def main():
    """Main function to execute the workflow."""
    allow_missing_s3 = os.getenv("SKIP_S3_UPLOADS", "").lower() in {"1", "true", "yes"}
    settings = load_settings(required=True, allow_missing_s3=allow_missing_s3)
    start_time = time.time()

    process_api_data(settings)
    dp.create_or_replace_tables(settings.local_folder, settings.db_path)
    print("Data processed successfully.")

    process_sales_data(settings)
    if settings.api_files_bucket_name:
        aq.athena_tables_creation()
    else:
        print("Skipping Athena table creation because API_FILES_BUCKET_NAME is missing.")

    email_discount_alert()
    end_time = time.time()
    print(f"Full pipeline completed in {end_time - start_time:.2f} seconds.")
    write_last_refresh(Path(__file__).resolve().parent / "last_refresh.txt")
    run_streamlit()


if __name__ == "__main__":
    main()
