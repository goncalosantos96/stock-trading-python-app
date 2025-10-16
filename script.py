import requests
import openai
import os
import time
from dotenv import load_dotenv
import csv

# Loads the environment variables from the .env file
load_dotenv()

# Gets the API KEY from the .env file
API_KEY = os.getenv("POLYGON_API_KEY")

# Limit for each request page
LIMIT = 1000

def extract_data_from_api_job():
    # URL to get the ticker from the Polygon API
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_KEY}"

    # List to store the tickers
    tickers = []

    # HTTP GET request to the POLYGON API
    response = requests.get(url)

# Converts the response to a JSON Object
    data = response.json()

# Loop to add each ticker from data to the ticker list
    for ticker in data['results']:
        tickers.append(ticker)


    while 'next_url' in data:

        print(f"Requesting next page: {data['next_url']}")
        # Loop to add each ticker from data to the ticker list
        for ticker in data['results']:
            tickers.append(ticker)

    # HTTP GET request to the Polygon API
        response = requests.get(f"{data['next_url']}&apiKey={API_KEY}")

        # Converts the response to a JSON Object
        data = response.json()

        # time to be idle in order to be able to not exceed requests per minute
        time.sleep(15)


    if not tickers:
        print("No tickers retrieved; skipping CSV export.")
    else:
        example_ticker = tickers[0]
        headers = list(example_ticker.keys())
        output_file = "tickers.csv"
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore", restval="")
            writer.writeheader()
            for row in tickers:
                writer.writerow(row)
        print(f"Wrote {len(tickers)} rows to {output_file}")


if __name__ == '__main__':
    extract_data_from_api_job()