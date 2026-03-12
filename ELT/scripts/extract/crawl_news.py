import requests
import json
import datetime
import pathlib as Path

# Get the current time
# Format string as YYYYMMDDTHHMM
def get_data_by_time_range(time_zone):
    """Get data based on the current time zone range."""
    yesterday = (datetime.date.today() - datetime.timedelta(days=1))
    if time_zone == 1:
        time_from = yesterday.strftime("%Y%m%dT"+"0000")
        time_to = yesterday.strftime("%Y%m%dT"+"2359")
    elif time_zone == 2:
        time_from = yesterday.strftime("%Y%m%dT"+"0000")
        time_to = yesterday.strftime("%Y%m%dT"+"1200")
    else:
        time_from = yesterday.strftime("%Y%m%dT"+"1201")
        time_to = yesterday.strftime("%Y%m%dT"+"2359")
    return time_from, time_to

def crawl_news():
    function = "NEWS_SENTIMENT"
    sort = "LATEST"
    limit = "1000"
    apikey = "JM488A8BNUQXVZRH"

    json_object = []
    total = 0
    for time_zone in [1, 2, 3]:
        # Call the function to get data
        time_from, time_to = get_data_by_time_range(time_zone)
        print(time_from, time_to)

        # Construct the API URL with query parameters
        url = f'https://www.alphavantage.co/query?function={function}&time_from={time_from}&time_to={time_to}&limit={limit}&apikey={apikey}'
        
        # Make a GET request to the API
        r = requests.get(url)
                
        # Parse the response JSON
        data = r.json()["feed"]
                
        # Increment the total count of news items
        total += len(data)
        
        if total == 1000 and time_zone == 1:
            continue
        
        # Append the data to the json_object list
        json_object += data
        
        if total < 1000 and time_zone == 1:
            break
        

    # Serialize the JSON object to a formatted string
    json_object = json.dumps(json_object, indent=4)

    # Get yesterday's date formatted as YYYY_MM_DD
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")

    # Define the file path for saving the JSON data
    path = Path.Path(__file__).parent.parent.parent / "data" / "raw" / "news" / f"news_{date}.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write the JSON data to a file
    with open(path, "w") as outfile:
        outfile.write(json_object)

    # Print success message with total news items and file path
    print(f"The process of crawling {total} news was successful")
    print(f"Saving at {path}")

# crawl_news()