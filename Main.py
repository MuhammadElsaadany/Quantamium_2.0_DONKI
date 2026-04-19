import requests, os

from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent/"API.env")


try:
    recieved_raw_data = requests.get("https://api.nasa.gov/planetary/apod?api_key=" + str(os.environ.get("NASA_API_KEY")) + "&date=2025-4-1")
    if recieved_raw_data.status_code == 200:
        recieved_data = recieved_raw_data.json()
        print(recieved_data)
    elif recieved_raw_data.status_code == 429:
        raise Exception("Error: You've exceeded your rate limit! | status code: 429")
    elif recieved_raw_data.status_code == 404:
        raise Exception("Error: Not found! | status code: 404")
    else:
        raise Exception("one of the other 95316 status codes that I have no idea exists popped up and is: " + str(recieved_raw_data.status_code))
    
except requests.exceptions.ConnectionError:
    raise Exception ("Connection failed")