# ULS_Lookup_2025_GOVSHUTDOWN
Most recent version of ULS database exports from FCC 9-29-2025

# 1. Initial DB setup (one time)
## Download Weekly DB exports from FCC
Applications 	340.49 MB 	9/27/2025 a_amat.zip
Licenses 	164.16 MB 	9/28/2025 l_amat.zip

https://data.fcc.gov/download/pub/uls/complete/l_amat.zip

https://data.fcc.gov/download/pub/uls/complete/a_amat.zip
## 2. Import data from downloaded weekly files
# Import both license and application data
python3 uls_importer.py --db uls.db --license-file l_amat.zip --app-file a_amat.zip

# Import separately
python3 uls_importer.py --db uls.db --import-file l_amat.zip
python3 uls_importer.py --db uls.db --import-file a_amat.zip

# Import from directory (will auto-detect and import both types)
python3 uls_importer.py --db uls.db --import-dir /path/to/uls/files



## 3. Apply daily db updates
python3 uls_importer.py --db uls.db --import-file l_am_wed.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file a_am_wed.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file l_am_thu.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file a_am_thu.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file l_am_fri.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file a_am_fri.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file l_am_sat.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file a_am_sat.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file l_am_sun.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file a_am_sun.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file l_am_mon.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file a_am_mon.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file l_am_tue.zip --import-type daily --replace
python3 uls_importer.py --db uls.db --import-file a_am_tue.zip --import-type daily --replace

## 4. Check status
python3 uls_importer.py --db uls.db --status

## 5. Optimize database
python3 uls_importer.py --db uls.db --vacuum --analyze

# WebApp Run 

## 1. Run the web app (after importing data)
python3 uls_webapp.py

## 2. Open browser to http://localhost:5120

## 3. For production with better performance
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5120 uls_webapp:app




