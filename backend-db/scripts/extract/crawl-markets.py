import requests
import json
import datetime
import pathlib as Path

def crawl_markets():
    # Define function and API key for Alpha Vantage API
    FUNCTION = "MARKET_STATUS"
    API_KEY = "JM488A8BNUQXVZRH"

    # Construct the API URL
    url = f'https://www.alphavantage.co/query?function={FUNCTION}&apikey={API_KEY}'
    response = requests.get(url)
    data = response.json()["markets"]

    # Serialize the market data to JSON
    json_object = json.dumps(data, indent=4)

    # Get the current date for filename
    date = datetime.date.today().strftime("%Y_%m_%d")

    # Define the target path for saving the JSON file
    cur_path = Path.Path(__file__).parent.parent.parent
    target_path = cur_path / "data" / "raw" / "markets"
    target_path.mkdir(parents=True, exist_ok=True)
    path = target_path / f"crawl_markets_{date}.json"

    # Write the JSON data to a file
    with open(path, "w") as outfile:
        outfile.write(json_object)

    # Print completion messages
    print(f"Extracted {len(data)} regions and exchanges.")
    print(f"Data saved to {path}")

crawl_markets()