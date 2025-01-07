import requests
import pandas as pd
from io import StringIO
from datetime import datetime
import os

def fetch_data_from_api(url):
    # Define headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    # Make request with headers
    response = requests.get(url, headers=headers)

    # Check if request was successful
    if response.status_code == 200:
        # Read CSV from response content
        csv_data = StringIO(response.text)

        # Get today's date
        today_date_file_name = datetime.now().strftime("_%Y_%m_%d")

        # Define filename with today's date appended
        filename = f"full-stock-list{today_date_file_name}.csv"  # Change "data" to your desired filename prefix
        
        # Define the path where you want to save the file
        folder_path = "/Users/MacUser/hedonism-wines_fresh/data/"  # Change this to your desired folder path

        # Check if the file already exists and delete it if it does
        file_path = folder_path + filename
        if os.path.exists(file_path):
            os.remove(file_path)
            print("Existing file deleted.")
        
        df = pd.read_csv(csv_data)

        # Save DataFrame to CSV with the modified filename
        df.to_csv(folder_path + filename, index=False)
        print("File saved successfully.")
        
        return df
    else:
        print("Failed to fetch data:", response.status_code)
        return None