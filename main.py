from api import fetch_data_from_api
from data_processing import process_data
from data_viz import *

def main():
    url = 'https://hedonism.co.uk/sites/default/files/full-stock-list.csv'
    df = fetch_data_from_api(url)
    if df is not None:
        print("Data fetched successfully.")

        # Process the data
        process_data(df)
        print("Data processed successfully.")

        print(df.head())
    else:
        print("Failed to fetch data from API.")
    visualise_discounted_items()
    visualise_stocks_and_median_values()
    visualise_price_search()
    
if __name__ == "__main__":
    main()