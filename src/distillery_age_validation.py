import queries as q
import re
import pandas as pd

def extract_distillery_and_age(title):
    match = re.search(r'(\d{4})|(\d+)\s*(\w+)|(\w+)\s*(\d+)', title)
    if match:
        age = match.group(1) or match.group(2)
        distillery = match.group(3) or match.group(4)
        return pd.Series([distillery.strip(), age.strip()])
    else:
        return pd.Series([None, None])


def main():
    df = q.price_search()
    print(df.head(20))
    df[['distillery', 'year_age']] = df['title'].apply(extract_distillery_and_age)
    return df


if __name__ == "__main__":
    main()
