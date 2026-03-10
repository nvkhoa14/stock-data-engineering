import requests
import json
import datetime
import pathlib as Path

def crawl_companies():
    # API token for SEC API
    API_TOKEN = "dd1db170794790c8553fda864aebf62433db1a3a5ddd53f38525d3a83f3673dd"

    # List of stock exchanges to extract data from
    exchanges = ["nasdaq", "nyse"]

    # Initialize an empty list to hold company data
    list_companies = []

    # Iterate over each exchange and fetch company data
    for exchange in exchanges:
        url = f'https://api.sec-api.io/mapping/exchange/{exchange}?token={API_TOKEN}'
        response = requests.get(url)
        data = response.json()
        list_companies.extend(data)
        print(f"Extracted {len(data)} companies from the {exchange.upper()} stock exchange.")

    # Get the current date for filename
    date = datetime.date.today().strftime("%Y_%m_%d")

    # Define the target path for saving the JSON file
    cur_path = Path.Path(__file__).parent.parent.parent
    target_path = cur_path / "data" / "raw" / "companies"
    target_path.mkdir(parents=True, exist_ok=True)
    path = target_path / f"crawl_companies_{date}.json"

    # Serialize the list of companies to JSON
    json_object = json.dumps(list_companies, indent=4)

    # Write the JSON data to a file
    with open(path, "w") as outfile:
        outfile.write(json_object)
    print(f"Data saved to {path}")
    
crawl_companies()