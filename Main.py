import requests, os, sqlite3, ast, time, logging
from dotenv import load_dotenv


load_dotenv()


logging.basicConfig(
    filename="log.txt", #creates log file or appends if exists
    level=logging.INFO, #security level which includes INFO, WARNING and ERROR
    format="%(asctime)s - %(levelname)s - %(message)s") #time - security level - message


logging.info("-----NEW RUN-----")


api_key = os.environ.get("NASA_API_KEY") #reads the API from the .env file
if not api_key:
    logging.error("Couldn't fetch API key")
    raise ValueError("Couldn't fetch API key")




def fetch_and_parse(db_name, table_name, db_create_table, url, db_insert, db_dicts_keys):
    """
    This function connects to a new/existing database and table,
    requests data from API, parses & decodes data from it,
    inserts data into database.
    

    :param db_name: str - database filename for 'sqlite3.connect()'
        for example:
            "maindata.db"
        
            
    :param table_name: str - for logging.
        for example:
            "coronal_mass_ejections"
        
            
    :param db_create_table: str - SQL command that goes into 'cursor.execute()' #always include 'alerted INTEGER DEFAULT 0' when creating tables.
        for example:
            "CREATE TABLE IF NOT EXISTS coronal_mass_ejections (activityID TEXT PRIMARY KEY, catalog TEXT NOT NULL, alerted INTEGER DEFAULT 0)"

            
    :param url: str - the url (excluding queries)
        for example:
            "https://api.nasa.gov/DONKI/CME"
        
            
    :param db_insert: str - SQL command excluding the values, goes into 'cursor.execute()' #we get values from 'db_dict_keys'
        for example:
            "INSERT OR IGNORE INTO coronal_mass_ejections (activityID, catalog) VALUES (?, ?)"

            
    :param db_dicts_keys: str/list of str - used to process values into 'cursor.execute()'
        for example:
            ["activityID", "catalog"]
    """
    try:
        with sqlite3.connect(db_name) as connection: #connects to new/existing db, and disconnects automatically after leaving 'with'
            cursor = connection.cursor()
            logging.info(f"Started parsing: {table_name}")
            cursor.execute(db_create_table) #uses SQL commands
            attempts_to_reconnect = 0

            while attempts_to_reconnect < 5:
                try:
                    response = requests.get(url,  params={'api_key': api_key}) #stores the encoded API call response data
                    ratelimit_maximum = response.headers.get("X-Ratelimit-Limit", "Failed to load maximum rate limit!") #from headers that came within API response..
                    ratelimit_remaining = response.headers.get("X-Ratelimit-Remaining", "Failed to load remaining rate limit!")

                    if response.status_code == 200: #status code 200 means successfully connected to API and got response back
                        response_data = response.json() #decodes the response data
                        for i in response_data:
                            values = tuple(str(i[key]) if isinstance(i[key], list) else i[key] for key in db_dicts_keys) #assigns 'db_insert' values using 'db_dicts_keys'
                            cursor.execute(db_insert, values)
                        logging.info(f"Finished parsing: {table_name} | Rate limit: {ratelimit_remaining} of {ratelimit_maximum}")
                        break

                    elif response.status_code == 403:
                        logging.error("Error: Forbidden or missing API key. | status code: 403")
                        raise Exception("Error: Forbidden or missing API key. | status code: 403")
                    elif response.status_code == 404:
                        logging.error("Error: Not found! | status code: 404")
                        raise Exception("Error: Not found! | status code: 404")
                    elif response.status_code == 429:
                        logging.error(f"Error: You've exceeded your rate limit: {ratelimit_remaining} of {ratelimit_maximum} | status code: 429")
                        raise Exception(f"Error: You've exceeded your rate limit: {ratelimit_remaining} of {ratelimit_maximum} | status code: 429")

                    elif response.status_code == 503:
                        if attempts_to_reconnect == 4:
                            logging.error("Error: Service is not available! | Failed to reconnect 5 times. | status code: 503")
                            raise Exception("Error: Service is not available! | Failed to reconnect 5 times. | status code: 503")
                        else:
                            attempts_to_reconnect += 1
                            logging.warning(f"Error: Service is not available! | status code: 503 | Re-attmpting... ({attempts_to_reconnect})")
                            time.sleep(5 * (attempts_to_reconnect))

                    else:
                        logging.error(f"Error: Unexpected status code: {response.status_code}")
                        raise Exception(f"Error: Unexpected status code: {response.status_code}")
                except requests.exceptions.ConnectionError as e2:
                    logging.error(f"Connection failed: {e2}")
                    raise requests.exceptions.ConnectionError(f"Connection failed: {e2}")

    except sqlite3.OperationalError as e1:
        logging.error(f"Error: Couldn't connect to {db_name} with fetch_and_parse: {e1}")
        raise sqlite3.OperationalError(f"Error: Couldn't connect to {db_name} with fetch_and_parse: {e1}")




def fetch_stringified(db_name, parent_table_name, db_create_table, db_insert, stringified_key, foreign_key, other_db_dicts_keys):
    """
    This function connects to a new/existing database and table,
    fetches specific table name for stringified entries that matches 'stringified_key' and ties it to 'foreign_key',
    de-stringifies these entries and inserts them to other new/exiting table.
    

    :param db_name: str - database filename for 'sqlite3.connect()'
        for example:
            "maindata.db"

            
    :param parent_table_name: str - for fetching within, and logging.
        for example:
            "geomagnetic_storms"
        
            
    :param db_create_table: str - SQL command that goes into 'cursor.execute()' #always include 'alerted INTEGER DEFAULT 0' when creating tables.
        for example:
            "CREATE TABLE IF NOT EXISTS gst_kp_readings (gstID TEXT NOT NULL, observedTime TEXT NOT NULL, kpIndex REAL NOT NULL, alerted INTEGER DEFAULT 0, PRIMARY KEY (gstID, observedTime))"

            
    :param db_insert: str - SQL command excluding the values, goes into 'cursor.execute()' #we get values from 'other_db_dicts_keys'
        for example:
            "INSERT OR IGNORE INTO gst_kp_readings (gstID, observedTime, kpIndex) VALUES (?, ?, ?)"

            
    :param stringified_key: str - the header (key) which you want to de-stringify it's rows data (values).
        for example:
            "allKpIndex" #this is from parent table geomagnetic_storms
        
            
    :param foreign_key: str - parent key that used as a reference from parent table to new table.
        for example:
            "gstID" #pulls gstID from parent table to new table to tie them together
        
            
    :param other_db_dicts_keys: str/list of str - used to process values into 'cursor.execute()' #don't include 'foreign_key' here as you already pass it on it's own param.
        for example:
            ["observedTime", "kpIndex"]
    """
    try:
        with sqlite3.connect(db_name) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            cursor.execute(db_create_table)
            stringified = cursor.execute(f"SELECT {stringified_key}, {foreign_key} FROM {parent_table_name}").fetchall() #fetches values of both the stringified key and the foreign key

            if stringified:
                logging.info(f"Started fetching stringified: {stringified_key} in: {parent_table_name}")
                for row in stringified:
                    if row[str(stringified_key)] is not None and row[str(stringified_key)] != "[]":
                        try:
                            de_stringified = ast.literal_eval(row[str(stringified_key)]) #this is what de-stringifies entries
                        except ValueError:
                                logging.warning("Warning: Skipped one malformed row!")
                                continue
                        for i in de_stringified:
                            values = (row[str(foreign_key)], ) + tuple(str(i[key]) if isinstance(i[key], list) else i[key] for key in other_db_dicts_keys)
                            cursor.execute(db_insert, values) #ties data of stringified_key with foreign_key
                logging.info(f"Finished fetching stringified: {stringified_key} in: {parent_table_name}")

    except sqlite3.OperationalError as e1:
        logging.error(f"Error: Couldn't connect to {db_name} with fetch_stringified: {e1}")
        raise sqlite3.OperationalError(f"Error: Couldn't connect to {db_name} with fetch_stringified: {e1}")




def check_anomalies(db_name, table_name, anomaly_rule, db_dicts_keys, primary_key, primary_key2=None):
    """
    This function connects to a new/existing database,
    if entries matches 'anomaly_rule' it logs them under WARNING type,
    and updates 'alerted' to 1 to prevent appearing in logs again.

    
    :param db_name: str - database filename for 'sqlite3.connect()'
        for example:
            "maindata.db"

            
    :param table_name: str - for logging and updating 'alerted'
        for example:
            "gst_kp_readings"

            
    :param anomaly_rule: str - SQL command that goes into 'cursor.execute().fetchall()' #always include 'AND alerted = 0' within your 'anomaly_rule'
        for example: 
            "SELECT * FROM gst_kp_readings WHERE kpIndex > 7 AND alerted = 0"


    :param db_dicts_keys: str/list of str - headers (keys) you want to log their values in case of anomalies.
        for example:
            ["gstID", "observedTime", "kpIndex", "source"]


    :param primary_key: str - to validate where anomalies came from, and helps locating anomalies 'alerted' value.
        for example:
            "gstID"


    :param primary_key2: str - (optional) to validate where anomalies came from alongside 'primary_key', and helps locating anomalies 'alerted' value.
        for example:
            "observedTime"
    """
    try:
        with sqlite3.connect(db_name) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()
            anomaly = cursor.execute(anomaly_rule).fetchall()

            if anomaly:
                logging.warning("---------")
                logging.warning("WARNING! SIGNIFICANT ANOMALIES DETECTED!")
                logging.warning("---------")
                for row in anomaly:
                    logging.warning(f"anomalyTable: {table_name}") #had to use camelCase here to match the logging style.. I really prefer snake_case
                    for key in db_dicts_keys:
                        logging.warning(f"{key}: {row[key]}")
                    if primary_key and not primary_key2:
                        cursor.execute(f"UPDATE {table_name} SET alerted = 1 WHERE {primary_key} = ?", (str(row[primary_key]), )) #fetches 'alerted' entry for that specific primary key and sets it to 1 (so it doesn't show in log again)
                    elif primary_key and primary_key2:
                        cursor.execute(f"UPDATE {table_name} SET alerted = 1 WHERE {primary_key} = ? AND {primary_key2} = ?", (str(row[primary_key]), str(row[primary_key2]))) #similar to ^ but with two primary keys
                    logging.warning("---------")
            else:
                logging.info(f"No other anomalies detected in: {table_name}")

    except sqlite3.OperationalError as e1:
        logging.error(f"Error: Couldn't connect to {db_name} with check_anomalies: {e1}")
        raise sqlite3.OperationalError(f"Error: Couldn't connect to {db_name} with check_anomalies: {e1}")
    





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

    "https://api.nasa.gov/DONKI/FLR",

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

    "https://api.nasa.gov/DONKI/GST",

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

    "https://api.nasa.gov/DONKI/CME",

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




fetch_stringified(
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
    
    "gstID",
    
    ["observedTime", "kpIndex", "source"])




check_anomalies(
    "maindata.db",

    "solar_flares",

    """SELECT * FROM solar_flares 
    WHERE (classType LIKE 'X%' OR 
    classType LIKE 'M5%' OR 
    classType LIKE 'M6%' OR 
    classType LIKE 'M7%' OR 
    classType LIKE 'M8%' OR 
    classType LIKE 'M9%') 
    AND alerted = 0""",

    ["flrID", "beginTime", "classType", "sourceLocation", "activeRegionNum"],

    "flrID")




check_anomalies(
    "maindata.db",

    "gst_kp_readings",

    "SELECT * FROM gst_kp_readings WHERE kpIndex > 7 AND alerted = 0",

    ["gstID", "observedTime", "kpIndex", "source"],

    "gstID",

    "observedTime")



logging.info("-----END OF RUN-----")