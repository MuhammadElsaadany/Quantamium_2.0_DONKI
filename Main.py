import requests, os, sqlite3
from dotenv import load_dotenv

load_dotenv()

connection = sqlite3.connect("flr.db")
cursor = connection.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS solar_flares (    
    flrID TEXT PRIMARY KEY,
    catalog TEXT NOT NULL,
    beginTime TEXT NOT NULL,
    peakTime TEXT,
    endTime TEXT,
    classType TEXT NOT NULL,
    sourceLocation TEXT,
    note TEXT,
    submissionTime TEXT NOT NULL,
    link TEXT NOT NULL,
    activeRegionNum INTEGER,
    versionId INTEGER NOT NULL,
    instruments TEXT NOT NULL,
    linkedEvents TEXT,
    sentNotifications TEXT)""")


api_key = os.environ.get("NASA_API_KEY")
if not api_key:
    raise ValueError("Couldn't fetch NASA_API_KEY")

try:
    response = requests.get("https://api.nasa.gov/DONKI/FLR?api_key=" + api_key)
    
    ratelimit = response.headers.get("X-Ratelimit-Limit", "Failed to load rate limit")
    ratelimit_remaining = response.headers.get("X-Ratelimit-Remaining", "Failed to load remaining rate limit")
    print("Remaining rate limit: " + str(ratelimit_remaining) + " of " + str(ratelimit))
        
    if response.status_code == 200:
        flr_data = response.json()
        for flare in flr_data:
            cursor.execute("""INSERT OR IGNORE INTO solar_flares (
                            flrID,
                            catalog,
                            beginTime,
                            peakTime,
                            endTime,
                            classType,
                            sourceLocation,
                            note,
                            submissionTime,
                            link,
                            activeRegionNum,
                            versionId,
                            instruments,
                            linkedEvents,
                            sentNotifications) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                            flare["flrID"],
                            flare["catalog"],
                            flare["beginTime"],
                            flare["peakTime"],
                            flare["endTime"],
                            flare["classType"],
                            flare["sourceLocation"],
                            flare["note"],
                            flare["submissionTime"],
                            flare["link"],
                            flare["activeRegionNum"],
                            flare["versionId"],
                            str(flare["instruments"]) if isinstance(flare["instruments"], list) else None,
                            str(flare["linkedEvents"]) if isinstance(flare["linkedEvents"], list) else None,
                            str(flare["sentNotifications"]) if isinstance(flare["sentNotifications"], list) else None))
        connection.commit()

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