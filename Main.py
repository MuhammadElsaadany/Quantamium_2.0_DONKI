import requests, os, sqlite3
from dotenv import load_dotenv

load_dotenv()

api_key = os.environ.get("NASA_API_KEY")
if not api_key:
    raise ValueError("Couldn't fetch NASA_API_KEY")


def fetch_and_parse(db_name, db_table, url, db_insert, db_keys):
    connection = sqlite3.connect(str(db_name))
    cursor = connection.cursor()
    cursor.execute(db_table)

    try:
        response = requests.get(str(url) + api_key)
        
        ratelimit = response.headers.get("X-Ratelimit-Limit", "Failed to load rate limit")
        ratelimit_remaining = response.headers.get("X-Ratelimit-Remaining", "Failed to load remaining rate limit")
        print("Remaining rate limit: " + str(ratelimit_remaining) + " of " + str(ratelimit))
            
        if response.status_code == 200:
            response_data = response.json()
            for i in response_data:
                values = tuple(str(i[key]) if isinstance(i[key], list) else i[key] for key in db_keys)
                cursor.execute(db_insert, values)
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


# fetch_and_parse("maindata.db",
                
#     """CREATE TABLE IF NOT EXISTS solar_flares (    
#     flrID TEXT PRIMARY KEY,
#     catalog TEXT NOT NULL,
#     beginTime TEXT NOT NULL,
#     peakTime TEXT,
#     endTime TEXT,
#     classType TEXT NOT NULL,
#     sourceLocation TEXT,
#     note TEXT,
#     submissionTime TEXT NOT NULL,
#     link TEXT NOT NULL,
#     activeRegionNum INTEGER,
#     versionId INTEGER NOT NULL,
#     instruments TEXT NOT NULL,
#     linkedEvents TEXT,
#     sentNotifications TEXT,
#     alerted INTEGER DEFAULT 0)""",

#     "https://api.nasa.gov/DONKI/FLR?api_key=",

#     """INSERT OR IGNORE INTO solar_flares (
#     flrID,
#     catalog,
#     beginTime,
#     peakTime,
#     endTime,
#     classType,
#     sourceLocation,
#     note,
#     submissionTime,
#     link,
#     activeRegionNum,
#     versionId,
#     instruments,
#     linkedEvents,
#     sentNotifications) 
#     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",

#     ["flrID",
#      "catalog",
#      "beginTime",
#      "peakTime",
#      "endTime",
#      "classType",
#      "sourceLocation",
#      "note",
#      "submissionTime",
#      "link",
#      "activeRegionNum",
#      "versionId",
#      "instruments",
#      "linkedEvents",
#      "sentNotifications"])



# fetch_and_parse("maindata.db",
                
#     """CREATE TABLE IF NOT EXISTS geomagnetic_storms (    
#     gstID TEXT PRIMARY KEY,
#     startTime TEXT NOT NULL,
#     allKpIndex TEXT NOT NULL,
#     link TEXT NOT NULL,
#     linkedEvents TEXT,
#     submissionTime TEXT NOT NULL,
#     versionId INTEGER NOT NULL,
#     sentNotifications TEXT,
#     alerted INTEGER DEFAULT 0)""",

#     "https://api.nasa.gov/DONKI/GST?api_key=",

#     """INSERT OR IGNORE INTO geomagnetic_storms (
#     gstID,
#     startTime,
#     allKpIndex,
#     link,
#     linkedEvents,
#     submissionTime,
#     versionId,
#     sentNotifications) 
#     VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",

#     ["gstID",
#     "startTime",
#     "allKpIndex",
#     "link",
#     "linkedEvents",
#     "submissionTime",
#     "versionId",
#     "sentNotifications"])



# fetch_and_parse("maindata.db",
                
#     """CREATE TABLE IF NOT EXISTS coronal_mass_ejections (    
#     activityID TEXT PRIMARY KEY,
#     catalog TEXT NOT NULL,
#     startTime TEXT NOT NULL,
#     instruments TEXT NOT NULL,
#     sourceLocation TEXT NOT NULL,
#     activeRegionNum INTEGER,
#     note TEXT,
#     submissionTime TEXT NOT NULL,
#     versionId INTEGER NOT NULL,
#     link TEXT NOT NULL,
#     cmeAnalyses TEXT NOT NULL,
#     linkedEvents TEXT,
#     sentNotifications TEXT,
#     alerted INTEGER DEFAULT 0)""",

#     "https://api.nasa.gov/DONKI/CME?api_key=",

#     """INSERT OR IGNORE INTO coronal_mass_ejections (
#     activityID,
#     catalog,
#     startTime,
#     instruments,
#     sourceLocation,
#     activeRegionNum,
#     note,
#     submissionTime,
#     versionId,
#     link,
#     cmeAnalyses,
#     linkedEvents,
#     sentNotifications) 
#     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",

#     ["activityID",
#     "catalog",
#     "startTime",
#     "instruments",
#     "sourceLocation",
#     "activeRegionNum",
#     "note",
#     "submissionTime",
#     "versionId",
#     "link",
#     "cmeAnalyses",
#     "linkedEvents",
#     "sentNotifications"])

    def check_anomalies(db_name, table_name, execute_call, keys, primary_key):
        connection = sqlite3.connect(db_name)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        anomaly = cursor.execute(execute_call).fetchall()
        if anomaly:
            print("---------")
            print("WARNING! SIGNIFICANT ANOMALIES DETECTED!")
            print("---------")
            for row in anomaly:
                for key in keys:
                    print(str(key) + ": " + str(row[key]))
                cursor.execute("UPDATE " + str(table_name) + " SET alerted = 1 WHERE " + str(primary_key) + " = ?", (str(row[primary_key]), ))
                connection.commit()
                print("---------")



    check_anomalies("maindata.db", "solar_flares", "SELECT * FROM solar_flares WHERE (classType LIKE 'X%' OR classType LIKE 'M5%' OR classType LIKE 'M6%' OR classType LIKE 'M7%' OR classType LIKE 'M8%' OR classType LIKE 'M9%') AND alerted = 0", ["flrID", "beginTime", "classType", "sourceLocation", "activeRegionNum"], "flrID")
