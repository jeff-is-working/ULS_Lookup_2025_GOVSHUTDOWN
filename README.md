# ULS_Lookup_2025_GOVSHUTDOWN
Most recent version of ULS database exports from FCC 9-29-2025


# 1. Initial setup (one time)
python3 uls_importer.py --db uls.db --schema public_access_database_definitions_sql_20250417.txt

# 2. Import your weekly export
python3 uls_importer.py --db uls.db --import-file l_amat.zip --import-type full

# 3. Apply daily updates
python3 uls_importer.py --db uls.db --import-file l_am_thu.zip --import-type daily --replace

python3 uls_importer.py --db uls.db --import-file l_am_fri.zip --import-type daily --replace

python3 uls_importer.py --db uls.db --import-file l_am_sat.zip --import-type daily --replace

python3 uls_importer.py --db uls.db --import-file l_am_sun.zip --import-type daily --replace

python3 uls_importer.py --db uls.db --import-file l_am_mon.zip --import-type daily --replace

python3 uls_importer.py --db uls.db --import-file l_am_tue.zip --import-type daily --replace

python3 uls_importer.py --db uls.db --import-file l_am_wed.zip --import-type daily --replace

# 4. Check status
python3 uls_importer.py --db uls.db --status


# 1. Run the web app (after importing data)
python3 uls_webapp.py

# 2. Open browser to http://localhost:5120

# 3. For production with better performance
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5120 uls_webapp:app


# api.py - Simple REST API for ULS database
from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)
DATABASE = 'uls.db'

def query_db(query, args=(), one=False):
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

@app.route('/api/search')
def search():
    search_type = request.args.get('type', 'callsign')
    value = request.args.get('q', '')
    
    if search_type == 'callsign':
        result = query_db("""
            SELECT h.*, e.entity_name, e.city, e.state
            FROM PUBACC_HD h
            LEFT JOIN PUBACC_EN e ON h.unique_system_identifier = e.unique_system_identifier
            WHERE h.call_sign = ?
        """, [value.upper()], one=True)
    elif search_type == 'name':
        results = query_db("""
            SELECT h.call_sign, h.license_status, e.entity_name, e.city, e.state
            FROM PUBACC_EN e
            JOIN PUBACC_HD h ON e.unique_system_identifier = h.unique_system_identifier
            WHERE e.entity_name LIKE ?
            LIMIT 100
        """, [f'%{value}%'])
        return jsonify([dict(r) for r in results])
    
    return jsonify(dict(result) if result else {'error': 'Not found'})

if __name__ == '__main__':
    app.run(debug=True)



