# Quantamium 2.0
A script that fetches data using NASA DONKI API, parses data into clean SQL, allows detecting anomalies and logs them.

-------------------------------------------------------
--------------- Which Data It Collects? ---------------
-------------------------------------------------------
- Currently it fetches 3 NASA DONKI API's: [Solar Flare (FLR)](https://en.wikipedia.org/wiki/Solar_flare), [Coronal Mass Ejection (CME)](https://en.wikipedia.org/wiki/Coronal_mass_ejection) and [Geomagnetic Storm (GST)](https://en.wikipedia.org/wiki/Geomagnetic_storm).
- Parses data and stores each of them under it's own table in a database.
- Can be set to detect when specific readings are alarming.
- Currently checks and warns user when:
\
-- FLR class is X or M5 -> M9
\
-- GST Kpindex exceeds 7

-------------------------------------------------
--------------- How To Set It Up? ---------------
-------------------------------------------------
- git clone https://github.com/MuhammadElsaadany/Quantamium_2.0
- Need Python installed, developed using [Python 3.11.9](https://www.python.org/downloads/release/python-3119/) (Could have issues with dependencies in newer versions)
- Write in the terminal: pip install -r requirements.txt
- Need a .env file (inside the project folder!) that contains NASA_API_KEY=[your api from here](https://api.nasa.gov/)
- Run the script by writing: python Main.py

---------------------------------------------------
--------------- How To Schedule It? ---------------
---------------------------------------------------
- Open Task Scheduler (search it in Windows start menu)
- Create a new Basic Task
- Set the trigger daily or at whatever time you want
- Set the action "Start a program"
- Program: path to the Python executable #NOTE: write this in terminal to find path to Python: where python
- Arguments: path to the script #NOTE: spaces in folders names causes issues, make sure the path has no spaces!
- Start in: the project folder path #NOTE: this is how Python finds your .env, .db and your log files

---------------------------------------------------
--------------- Example Of The Log: ---------------
---------------------------------------------------
<img width="791" height="538" alt="image" src="https://github.com/user-attachments/assets/1416270a-1b81-4bd5-accc-18ac644d87d0" />
