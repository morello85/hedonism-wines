from api import fetch_data_from_api
from data_processing import process_data
import data_viz as dv
import s3upload as su


# Specify data local folder"
local_folder = "/Users/MacUser/hedonism-wines_app/data"

# Specify the name of the S3 bucket
bucket_name = "hedonism-wines-api-files"

def main():
    url = 'https://hedonism.co.uk/sites/default/files/full-stock-list.csv'
    df = fetch_data_from_api(url)
    if df is not None:
        print("Data fetched successfully.")

        # Process the data
        process_data(df)
        print("Data processed successfully.")

        # Save data to s3
        su.upload_files_to_s3(local_folder, bucket_name)
        print ("Data uploaded to s3 successfully")

        print(df.head())
    else:
        print("Failed to fetch data from API.")
    dv.visualise_discounted_items()
    dv.visualise_stocks_and_median_values()
    dv.visualise_price_search()
    
if __name__ == "__main__":
    main()