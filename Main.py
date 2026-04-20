import requests, os, sqlite3, ast, time
from dotenv import load_dotenv

load_dotenv()


api_key = os.environ.get("NASA_API_KEY")
if not api_key:
    raise ValueError("Couldn't fetch NASA_API_KEY.")



def fetch_and_parse(db_name, table_name, db_create_table, url, db_insert, db_keys):

    try:
        connection = sqlite3.connect(db_name)
    except sqlite3.OperationalError as e1:
        raise sqlite3.OperationalError("Error: Couldn't connect to " + str(db_name) + " with fetch_and_parse: " + str(e1) + ".")
    
    cursor = connection.cursor()
    print("Started parsing: " + str(table_name) + ".")
    cursor.execute(db_create_table)
    attempts_to_reconnect = 0
    seconds = 5

    while attempts_to_reconnect < 5:
        
        try:
            response = requests.get(str(url) + api_key)
            ratelimit_maximum = response.headers.get("X-Ratelimit-Limit", "Failed to load maximum rate limit!")
            ratelimit_remaining = response.headers.get("X-Ratelimit-Remaining", "Failed to load remaining rate limit!")

            if response.status_code == 200:
                response_data = response.json()
                for i in response_data:
                    values = tuple(str(i[key]) if isinstance(i[key], list) else i[key] for key in db_keys)
                    cursor.execute(db_insert, values)
                connection.commit()
                print("Finished parsing: " + str(table_name) + ". | Rate limit: " + str(ratelimit_remaining) + " of " + str(ratelimit_maximum) + "\n")
                break

            elif response.status_code == 429:
                raise Exception("Error: You've exceeded your rate limit: " + str(ratelimit_remaining) + " of " + str(ratelimit_maximum) + " | status code: 429")
            
            elif response.status_code == 404:
                raise Exception("Error: Not found! | status code: 404")
            
            elif response.status_code == 503:
                if attempts_to_reconnect == 4:
                    raise Exception("Error: Service is not available! Failed to reconnect 5 times. | status code: 503")
                else:
                    attempts_to_reconnect += 1
                    print("Error: Service is not available! Reattmpting.. | Attempt number: " + str(attempts_to_reconnect) +  " | status code: 503")
                    time.sleep(seconds * (attempts_to_reconnect))

            elif response.status_code == 403:
                raise Exception("Error: Forbidden or missing API key. | status code: 403")
            
            else:
                raise Exception("Error: One of the other 95316 status codes that I have no idea exists popped up! | status code: " + str(response.status_code))
            
        except requests.exceptions.ConnectionError as e2:
            raise requests.exceptions.ConnectionError("Connection failed: " + str(e2) + ".")




def fetch_nested(db_name, parent_table_name, db_create_table, db_insert,  stringified_key, foreign_key):

    try:
        connection = sqlite3.connect(db_name)
    except sqlite3.OperationalError as e1:
        raise sqlite3.OperationalError("Error: Couldn't connect to " + str(db_name) + " with fetch_nested: " + str(e1) + ".")
    
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    cursor.execute(db_create_table)
    nested = cursor.execute("SELECT " + str(stringified_key) + ", " + str(foreign_key) + " FROM " + str(parent_table_name)).fetchall()

    if nested:
        print("Started fetching nested: " + str(stringified_key) + " in: " + str(parent_table_name) + ".")
        for row in nested:
            if row[str(stringified_key)] is not None:
                try:
                    un_nested = ast.literal_eval(row[str(stringified_key)])
                except ValueError:
                        print("Warning: Skipped one malformed row.")
                        continue
                for reading in un_nested:
                    cursor.execute(db_insert, (row[str(foreign_key)], reading["observedTime"], reading["kpIndex"], reading["source"]))
        connection.commit()
        print("Finished fetching nested: " + str(stringified_key) + " in: " + str(parent_table_name) + "." + "\n")




def check_anomalies(db_name, table_name, execute_call, keys, primary_key, primary_key2=None):

    try:
        connection = sqlite3.connect(db_name)
    except sqlite3.OperationalError as e1:
        raise sqlite3.OperationalError("Error: Couldn't connect to " + str(db_name) + " with check_anomalies: " + str(e1) + ".")
    
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    anomaly = cursor.execute(execute_call).fetchall()

    if anomaly:
        print("---------" + "\n" + "WARNING! SIGNIFICANT ANOMALIES DETECTED!" + "\n" + "---------")
        for row in anomaly:
            print("anomalyTable: " + str(table_name))
            for key in keys:
                print(str(key) + ": " + str(row[key]))
            if primary_key and not primary_key2:
                cursor.execute("UPDATE " + str(table_name) + " SET alerted = 1 WHERE " + str(primary_key) + " = ?", (str(row[primary_key]), ))
            elif primary_key and primary_key2:
                cursor.execute("UPDATE " + str(table_name) + " SET alerted = 1 WHERE " + str(primary_key) + " = ? AND " + str(primary_key2) + " = ?", (str(row[primary_key]), str(row[primary_key2])))
            print("---------")
        connection.commit()
    else:
        print("No other anomalies detected in: " + str(table_name) + ".")




fetch_and_parse(
    "maindata.db",
                
    "solar_flares",
                
    """CREATE TABLE IF NOT EXISTS solar_flares (    
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
    sentNotifications TEXT,
    alerted INTEGER DEFAULT 0)""",

    "https://api.nasa.gov/DONKI/FLR?api_key=",

    """INSERT OR IGNORE INTO solar_flares (
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
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",

    ["flrID",
     "catalog",
     "beginTime",
     "peakTime",
     "endTime",
     "classType",
     "sourceLocation",
     "note",
     "submissionTime",
     "link",
     "activeRegionNum",
     "versionId",
     "instruments",
     "linkedEvents",
     "sentNotifications"])




fetch_and_parse(
    "maindata.db",
                
    "geomagnetic_storms",
                
    """CREATE TABLE IF NOT EXISTS geomagnetic_storms (    
    gstID TEXT PRIMARY KEY,
    startTime TEXT NOT NULL,
    allKpIndex TEXT NOT NULL,
    link TEXT NOT NULL,
    linkedEvents TEXT,
    submissionTime TEXT NOT NULL,
    versionId INTEGER NOT NULL,
    sentNotifications TEXT,
    alerted INTEGER DEFAULT 0)""",

    "https://api.nasa.gov/DONKI/GST?api_key=",

    """INSERT OR IGNORE INTO geomagnetic_storms (
    gstID,
    startTime,
    allKpIndex,
    link,
    linkedEvents,
    submissionTime,
    versionId,
    sentNotifications) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",

    ["gstID",
    "startTime",
    "allKpIndex",
    "link",
    "linkedEvents",
    "submissionTime",
    "versionId",
    "sentNotifications"])




fetch_and_parse(
    "maindata.db",
                
    "coronal_mass_ejections",
                
    """CREATE TABLE IF NOT EXISTS coronal_mass_ejections (    
    activityID TEXT PRIMARY KEY,
    catalog TEXT NOT NULL,
    startTime TEXT NOT NULL,
    instruments TEXT NOT NULL,
    sourceLocation TEXT NOT NULL,
    activeRegionNum INTEGER,
    note TEXT,
    submissionTime TEXT NOT NULL,
    versionId INTEGER NOT NULL,
    link TEXT NOT NULL,
    cmeAnalyses TEXT NOT NULL,
    linkedEvents TEXT,
    sentNotifications TEXT,
    alerted INTEGER DEFAULT 0)""",

    "https://api.nasa.gov/DONKI/CME?api_key=",

    """INSERT OR IGNORE INTO coronal_mass_ejections (
    activityID,
    catalog,
    startTime,
    instruments,
    sourceLocation,
    activeRegionNum,
    note,
    submissionTime,
    versionId,
    link,
    cmeAnalyses,
    linkedEvents,
    sentNotifications) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",

    ["activityID",
    "catalog",
    "startTime",
    "instruments",
    "sourceLocation",
    "activeRegionNum",
    "note",
    "submissionTime",
    "versionId",
    "link",
    "cmeAnalyses",
    "linkedEvents",
    "sentNotifications"])




fetch_nested(
    "maindata.db",
             
    "geomagnetic_storms",
             
    """CREATE TABLE IF NOT EXISTS gst_kp_readings (    
    gstID TEXT NOT NULL,
    observedTime TEXT NOT NULL,
    kpIndex REAL NOT NULL,
    source TEXT NOT NULL,
    alerted INTEGER DEFAULT 0,
    PRIMARY KEY (gstID, observedTime))""",
    
    """INSERT OR IGNORE INTO gst_kp_readings (    
    gstID,
    observedTime,
    kpIndex,
    source)
    VALUES (?, ?, ?, ?)""",
    
    "allKpIndex",
    
    "gstID")




check_anomalies(
    "maindata.db",

    "solar_flares",

    "SELECT * FROM solar_flares WHERE (classType LIKE 'X%' OR classType LIKE 'M5%' OR classType LIKE 'M6%' OR classType LIKE 'M7%' OR classType LIKE 'M8%' OR classType LIKE 'M9%') AND alerted = 0",

    ["flrID", "beginTime", "classType", "sourceLocation", "activeRegionNum"],

    "flrID")




check_anomalies(
    "maindata.db",

    "gst_kp_readings",

    "SELECT * FROM gst_kp_readings WHERE kpIndex > 7 AND alerted = 0",

    ["gstID", "observedTime", "kpIndex", "source"],

    "gstID",

    "observedTime")
