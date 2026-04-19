import requests, os
from dotenv import load_dotenv

load_dotenv()

api_key = os.environ.get("NASA_API_KEY")
if not api_key:
    raise ValueError("Couldn't fetch NASA_API_KEY")

try:
    response = requests.get("https://api.nasa.gov/planetary/apod?api_key=" + api_key + "&date=2025-4-1")
    
    ratelimit = response.headers.get("X-Ratelimit-Limit", None)
    ratelimit_remaining = response.headers.get("X-Ratelimit-Remaining", None)
    if ratelimit and ratelimit_remaining:
        print("Remaining rate limit: " + str(ratelimit_remaining) + " of " + str(ratelimit))
        
    if response.status_code == 200:
        apod_data = response.json()
        #print(apod_data)

    elif response.status_code == 429:
        raise Exception("Error: You've exceeded your rate limit! | status code: 429")
    
    elif response.status_code == 404:
        raise Exception("Error: Not found! | status code: 404")
    
    elif response.status_code == 503:
        raise Exception("Error: Service Unavailable. | status code: 503")
    
    else:
        raise Exception("one of the other 95316 status codes that I have no idea exists popped up! | status code: " + str(response.status_code))
    
except requests.exceptions.ConnectionError:
    raise Exception ("Connection failed!")