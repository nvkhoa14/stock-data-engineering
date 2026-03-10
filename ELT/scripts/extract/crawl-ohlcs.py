import requests
import json
import datetime
import pathlib as Path


def crawl_ohlcs():
    # Get the OHLCs of yesterday
    date_crawl = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # Define the path to save the JSON data
    data_path = Path.Path(__file__).parent.parent.parent / "data" / "raw" / "ohlcs"
    # API key for authentication
    apiKey = "9FfH_uc8CfaiGo0dUm24weZjZwFwPzNt"

    # Set parameters for the API request
    adjusted = "true"
    include_otc = "true"

    # Construct the API URL with query parameters
    url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_crawl}?adjusted={adjusted}&include_otc={include_otc}&apiKey={apiKey}'

    # Make a GET request to the API
    r = requests.get(url)

    # Parse the response JSON
    data = r.json()
    data = data.get("results", [])

    # Serialize the JSON object to a formatted string
    json_object = json.dumps(data, indent=4)

    # Get yesterday's date formatted as YYYY_MM_DD
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")

    # Define the file path for saving the JSON data
    data_path.mkdir(parents=True, exist_ok=True)
    path = data_path / f"crawl_ohlcs_{date}.json"

    # Write the JSON data to a file
    with open(path, "w") as outfile:
        outfile.write(json_object)

    # Print success message with total OHLCs and file path
    print(f"The process of crawling {len(data)} OHLCs was successful")
    print(f"Saving at {path}")

crawl_ohlcs()