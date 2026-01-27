import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from pathlib import Path

def fetch_data_from_api(url, output_folder: Path):
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
        
        # Ensure output folder exists
        output_folder.mkdir(parents=True, exist_ok=True)
        file_path = output_folder / filename
        
        df = pd.read_csv(csv_data)

        # Save DataFrame to CSV with the modified filename
        df.to_csv(file_path, index=False)
        print("File saved successfully.")
        
        return df
    else:
        print("Failed to fetch data:", response.status_code)
        return None
