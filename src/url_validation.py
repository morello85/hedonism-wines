import requests
import pandas as pd

def validate_urls(urls):
    valid_urls = []
    invalid_urls = []

    for url in urls:
        try:
            response = requests.head(url, allow_redirects=True)
            if response.status_code == 200:
                valid_urls.append(url)
            else:
                invalid_urls.append(url)
        except requests.RequestException:
            invalid_urls.append(url)

    # Create DataFrame for invalid URLs
    invalid_urls_df = pd.DataFrame({'URL': invalid_urls})

    # Export DataFrame to CSV
    invalid_urls_df.to_csv('/Users/MacUser/hedonism-wines_app/invalid_urls.csv', index=False)

    return