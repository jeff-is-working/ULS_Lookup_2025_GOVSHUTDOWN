#!/usr/bin/env python3
"""
FCC ULS Database Web Application
Provides search interface and CSV export for ULS license data
"""

from flask import Flask, render_template, request, jsonify, send_file, Response
import sqlite3
import csv
import io
import os
from datetime import datetime
from functools import lru_cache
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['DATABASE'] = os.environ.get('ULS_DATABASE', 'uls.db')
app.config['MAX_RESULTS'] = 1000
app.config['PAGE_SIZE'] = 50

# Database connection
def get_db():
    """Create database connection with optimizations"""
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    # Performance optimizations
    conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
    conn.execute("PRAGMA temp_store = MEMORY")
    return conn

def query_db(query, args=(), one=False):
    """Execute database query"""
    conn = get_db()
    cur = conn.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

@lru_cache(maxsize=128)
def get_states():
    """Get list of states from database"""
    results = query_db("SELECT DISTINCT state FROM PUBACC_EN WHERE state IS NOT NULL ORDER BY state")
    return [r['state'] for r in results]

@lru_cache(maxsize=256)
def get_counties(state=None):
    """Get list of counties, optionally filtered by state"""
    if state:
        results = query_db("""
            SELECT DISTINCT l.location_county as county 
            FROM PUBACC_LO l
            JOIN PUBACC_HD h ON l.call_sign = h.call_sign
            JOIN PUBACC_EN e ON h.unique_system_identifier = e.unique_system_identifier
            WHERE e.state = ? AND l.location_county IS NOT NULL
            ORDER BY l.location_county
        """, [state])
    else:
        results = query_db("""
            SELECT DISTINCT location_county as county 
            FROM PUBACC_LO 
            WHERE location_county IS NOT NULL 
            ORDER BY location_county
        """)
    return [r['county'] for r in results]

@app.route('/')
def index():
    """Render main search page"""
    return render_template('index.html')

@app.route('/api/search', methods=['GET'])
def search():
    """Main search API endpoint"""
    # Get search parameters
    search_type = request.args.get('type', 'callsign')
    query_value = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', app.config['PAGE_SIZE']))
    
    # Validate inputs
    if not query_value and search_type != 'geographic':
        return jsonify({'error': 'Search query required'}), 400
    
    if per_page > 200:
        per_page = 200
    
    offset = (page - 1) * per_page
    
    # Build query based on search type
    base_query = """
        SELECT DISTINCT
            h.unique_system_identifier,
            h.call_sign,
            h.uls_file_number,
            h.radio_service_code,
            h.grant_date,
            h.expired_date,
            h.cancellation_date,
            h.license_status,
            e.entity_name,
            e.first_name,
            e.last_name,
            e.frn,
            e.street_address,
            e.city,
            e.state,
            e.zip_code,
            e.email,
            e.phone
        FROM PUBACC_HD h
        LEFT JOIN PUBACC_EN e ON h.unique_system_identifier = e.unique_system_identifier
    """
    
    where_clause = ""
    params = []
    
    if search_type == 'callsign':
        where_clause = "WHERE h.call_sign = ?"
        params = [query_value.upper()]
        
    elif search_type == 'frn':
        where_clause = "WHERE e.frn = ?"
        params = [query_value]
        
    elif search_type == 'uls_id':
        where_clause = "WHERE h.unique_system_identifier = ?"
        params = [query_value]
        
    elif search_type == 'name':
        # Parse name (could be "first last" or "last, first")
        name_parts = query_value.replace(',', ' ').split()
        if len(name_parts) >= 2:
            where_clause = """
                WHERE (
                    (UPPER(e.first_name) LIKE ? AND UPPER(e.last_name) LIKE ?) OR
                    (UPPER(e.first_name) LIKE ? AND UPPER(e.last_name) LIKE ?) OR
                    UPPER(e.entity_name) LIKE ?
                )
            """
            params = [
                f"%{name_parts[0].upper()}%", f"%{name_parts[1].upper()}%",
                f"%{name_parts[1].upper()}%", f"%{name_parts[0].upper()}%",
                f"%{query_value.upper()}%"
            ]
        else:
            where_clause = """
                WHERE (
                    UPPER(e.first_name) LIKE ? OR 
                    UPPER(e.last_name) LIKE ? OR 
                    UPPER(e.entity_name) LIKE ?
                )
            """
            params = [f"%{query_value.upper()}%"] * 3
            
    elif search_type == 'geographic':
        # Geographic search with multiple filters
        region = request.args.get('region', '')
        state = request.args.get('state', '')
        county = request.args.get('county', '')
        city = request.args.get('city', '')
        
        conditions = []
        
        if state:
            conditions.append("e.state = ?")
            params.append(state.upper())
            
        if county:
            # Join with location table for county
            base_query = """
                SELECT DISTINCT
                    h.unique_system_identifier,
                    h.call_sign,
                    h.uls_file_number,
                    h.radio_service_code,
                    h.grant_date,
                    h.expired_date,
                    h.cancellation_date,
                    h.license_status,
                    e.entity_name,
                    e.first_name,
                    e.last_name,
                    e.frn,
                    e.street_address,
                    e.city,
                    e.state,
                    e.zip_code,
                    e.email,
                    e.phone
                FROM PUBACC_HD h
                LEFT JOIN PUBACC_EN e ON h.unique_system_identifier = e.unique_system_identifier
                LEFT JOIN PUBACC_LO l ON h.call_sign = l.call_sign
            """
            conditions.append("UPPER(l.location_county) LIKE ?")
            params.append(f"%{county.upper()}%")
            
        if city:
            conditions.append("UPPER(e.city) LIKE ?")
            params.append(f"%{city.upper()}%")
            
        if region:
            # Map regions to states (simplified example)
            region_states = {
                'northeast': ['CT', 'ME', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT'],
                'southeast': ['AL', 'AR', 'FL', 'GA', 'KY', 'LA', 'MS', 'NC', 'SC', 'TN', 'VA', 'WV'],
                'midwest': ['IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI'],
                'southwest': ['AZ', 'NM', 'OK', 'TX'],
                'west': ['AK', 'CA', 'CO', 'HI', 'ID', 'MT', 'NV', 'OR', 'UT', 'WA', 'WY']
            }
            
            if region.lower() in region_states:
                states_list = region_states[region.lower()]
                placeholders = ','.join(['?' for _ in states_list])
                conditions.append(f"e.state IN ({placeholders})")
                params.extend(states_list)
        
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        else:
            return jsonify({'error': 'Geographic search requires at least one filter'}), 400
    
    else:
        return jsonify({'error': 'Invalid search type'}), 400
    
    # Add license status filter
    status_filter = request.args.get('status', '')
    if status_filter:
        if where_clause:
            where_clause += " AND h.license_status = ?"
        else:
            where_clause = "WHERE h.license_status = ?"
        params.append(status_filter)
    
    # Get total count
    count_query = f"SELECT COUNT(DISTINCT h.unique_system_identifier) as total FROM PUBACC_HD h LEFT JOIN PUBACC_EN e ON h.unique_system_identifier = e.unique_system_identifier {where_clause}"
    if 'PUBACC_LO' in base_query:
        count_query = f"SELECT COUNT(DISTINCT h.unique_system_identifier) as total FROM PUBACC_HD h LEFT JOIN PUBACC_EN e ON h.unique_system_identifier = e.unique_system_identifier LEFT JOIN PUBACC_LO l ON h.call_sign = l.call_sign {where_clause}"
    
    total_result = query_db(count_query, params, one=True)
    total_count = total_result['total'] if total_result else 0
    
    # Execute main query with pagination
    full_query = f"{base_query} {where_clause} ORDER BY h.call_sign LIMIT ? OFFSET ?"
    params.extend([per_page, offset])
    
    results = query_db(full_query, params)
    
    # Format results
    formatted_results = []
    for row in results:
        formatted_results.append({
            'unique_system_identifier': row['unique_system_identifier'],
            'call_sign': row['call_sign'],
            'uls_file_number': row['uls_file_number'],
            'radio_service_code': row['radio_service_code'],
            'grant_date': row['grant_date'],
            'expired_date': row['expired_date'],
            'license_status': row['license_status'],
            'entity_name': row['entity_name'] or f"{row['first_name'] or ''} {row['last_name'] or ''}".strip(),
            'frn': row['frn'],
            'address': f"{row['street_address'] or ''}, {row['city'] or ''}, {row['state'] or ''} {row['zip_code'] or ''}".strip(', '),
            'email': row['email'],
            'phone': row['phone']
        })
    
    return jsonify({
        'results': formatted_results,
        'total': total_count,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_count + per_page - 1) // per_page
    })

@app.route('/api/export/csv', methods=['POST'])
def export_csv():
    """Export search results to CSV"""
    # Get search parameters from POST body
    data = request.get_json()
    search_type = data.get('type', 'callsign')
    query_value = data.get('q', '')
    
    # Build same query as search but without pagination
    base_query = """
        SELECT 
            h.call_sign,
            h.uls_file_number,
            h.unique_system_identifier,
            h.radio_service_code,
            h.grant_date,
            h.expired_date,
            h.license_status,
            e.entity_name,
            e.first_name,
            e.last_name,
            e.frn,
            e.street_address,
            e.city,
            e.state,
            e.zip_code,
            e.email,
            e.phone
        FROM PUBACC_HD h
        LEFT JOIN PUBACC_EN e ON h.unique_system_identifier = e.unique_system_identifier
    """
    
    # [Similar WHERE clause building as in search() - abbreviated for space]
    where_clause = ""
    params = []
    
    if search_type == 'callsign':
        where_clause = "WHERE h.call_sign = ?"
        params = [query_value.upper()]
    elif search_type == 'frn':
        where_clause = "WHERE e.frn = ?"
        params = [query_value]
    # ... (implement other search types similarly)
    
    # Limit exports to prevent abuse
    full_query = f"{base_query} {where_clause} LIMIT {app.config['MAX_RESULTS']}"
    results = query_db(full_query, params)
    
    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write headers
    writer.writerow([
        'Call Sign', 'ULS File Number', 'System ID', 'Service Code',
        'Grant Date', 'Expiration Date', 'Status', 'Entity Name',
        'First Name', 'Last Name', 'FRN', 'Street Address',
        'City', 'State', 'ZIP', 'Email', 'Phone'
    ])
    
    # Write data
    for row in results:
        writer.writerow([
            row['call_sign'], row['uls_file_number'], row['unique_system_identifier'],
            row['radio_service_code'], row['grant_date'], row['expired_date'],
            row['license_status'], row['entity_name'], row['first_name'],
            row['last_name'], row['frn'], row['street_address'],
            row['city'], row['state'], row['zip_code'], row['email'], row['phone']
        ])
    
    # Create response
    output.seek(0)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename=uls_export_{timestamp}.csv'
        }
    )

@app.route('/api/license/<callsign>')
def get_license_detail(callsign):
    """Get detailed license information"""
    # Get main license data
    license_data = query_db("""
        SELECT h.*, e.*, 
               am.operator_class as amateur_class,
               am.previous_callsign,
               am.vanity_callsign_change
        FROM PUBACC_HD h
        LEFT JOIN PUBACC_EN e ON h.unique_system_identifier = e.unique_system_identifier
        LEFT JOIN PUBACC_AM am ON h.call_sign = am.callsign
        WHERE h.call_sign = ?
    """, [callsign.upper()], one=True)
    
    if not license_data:
        return jsonify({'error': 'License not found'}), 404
    
    # Get location data
    locations = query_db("""
        SELECT * FROM PUBACC_LO 
        WHERE call_sign = ?
        ORDER BY location_number
    """, [callsign.upper()])
    
    # Get frequency data
    frequencies = query_db("""
        SELECT * FROM PUBACC_FR
        WHERE call_sign = ?
        ORDER BY location_number, frequency_assigned
    """, [callsign.upper()])
    
    # Get history
    history = query_db("""
        SELECT * FROM PUBACC_HS
        WHERE callsign = ?
        ORDER BY log_date DESC
        LIMIT 20
    """, [callsign.upper()])
    
    return jsonify({
        'license': dict(license_data),
        'locations': [dict(l) for l in locations],
        'frequencies': [dict(f) for f in frequencies],
        'history': [dict(h) for h in history]
    })

@app.route('/api/stats')
def get_stats():
    """Get database statistics"""
    stats = {}
    
    # Total licenses
    result = query_db("SELECT COUNT(*) as count FROM PUBACC_HD", one=True)
    stats['total_licenses'] = result['count'] if result else 0
    
    # Active licenses
    result = query_db("SELECT COUNT(*) as count FROM PUBACC_HD WHERE license_status = 'A'", one=True)
    stats['active_licenses'] = result['count'] if result else 0
    
    # By service
    services = query_db("""
        SELECT radio_service_code, COUNT(*) as count 
        FROM PUBACC_HD 
        WHERE license_status = 'A'
        GROUP BY radio_service_code 
        ORDER BY count DESC 
        LIMIT 10
    """)
    stats['top_services'] = [dict(s) for s in services]
    
    # Last update
    result = query_db("""
        SELECT MAX(import_date) as last_update 
        FROM import_tracking
    """, one=True)
    stats['last_update'] = result['last_update'] if result and result['last_update'] else 'Unknown'
    
    return jsonify(stats)

@app.route('/api/regions')
def get_regions():
    """Get available regions and states"""
    return jsonify({
        'regions': [
            {'code': 'northeast', 'name': 'Northeast'},
            {'code': 'southeast', 'name': 'Southeast'},
            {'code': 'midwest', 'name': 'Midwest'},
            {'code': 'southwest', 'name': 'Southwest'},
            {'code': 'west', 'name': 'West'}
        ],
        'states': get_states()
    })

# Create templates directory and HTML template
if not os.path.exists('templates'):
    os.makedirs('templates')

# HTML template content (saved as templates/index.html)
HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FCC ULS Database Search</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { background: #2c3e50; color: white; padding: 30px 0; margin: -20px -20px 30px; }
        .header h1 { text-align: center; font-size: 2em; }
        .search-box { background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 30px; }
        .search-type { display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap; }
        .search-type button { padding: 10px 20px; border: 2px solid #3498db; background: white; color: #3498db; border-radius: 5px; cursor: pointer; transition: all 0.3s; }
        .search-type button.active { background: #3498db; color: white; }
        .search-type button:hover { background: #2980b9; color: white; border-color: #2980b9; }
        .search-form { display: flex; gap: 10px; margin-bottom: 20px; }
        .search-form input, .search-form select { flex: 1; padding: 12px; border: 1px solid #ddd; border-radius: 5px; font-size: 16px; }
        .search-form button { padding: 12px 30px; background: #27ae60; color: white; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; }
        .search-form button:hover { background: #229954; }
        .geographic-filters { display: none; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin-bottom: 20px; }
        .geographic-filters.active { display: grid; }
        .results { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .results-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 10px; border-bottom: 2px solid #ecf0f1; }
        .results-count { color: #7f8c8d; }
        .export-btn { padding: 8px 16px; background: #95a5a6; color: white; border: none; border-radius: 5px; cursor: pointer; }
        .export-btn:hover { background: #7f8c8d; }
        table { width: 100%; border-collapse: collapse; }
        th { background: #ecf0f1; padding: 12px; text-align: left; font-weight: 600; }
        td { padding: 12px; border-bottom: 1px solid #ecf0f1; }
        tr:hover { background: #f8f9fa; }
        .pagination { display: flex; justify-content: center; gap: 5px; margin-top: 20px; }
        .pagination button { padding: 8px 12px; border: 1px solid #ddd; background: white; cursor: pointer; border-radius: 3px; }
        .pagination button:hover { background: #ecf0f1; }
        .pagination button.active { background: #3498db; color: white; border-color: #3498db; }
        .pagination button:disabled { opacity: 0.5; cursor: not-allowed; }
        .loading { text-align: center; padding: 40px; color: #7f8c8d; }
        .error { background: #e74c3c; color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        .license-status { padding: 2px 8px; border-radius: 3px; font-size: 12px; font-weight: bold; }
        .status-A { background: #27ae60; color: white; }
        .status-E { background: #e67e22; color: white; }
        .status-C { background: #e74c3c; color: white; }
        @media (max-width: 768px) {
            .search-form { flex-direction: column; }
            table { font-size: 14px; }
            th, td { padding: 8px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>FCC ULS Database Search</h1>
        </div>
        
        <div class="search-box">
            <div class="search-type">
                <button class="active" data-type="callsign">Call Sign</button>
                <button data-type="frn">FRN</button>
                <button data-type="uls_id">ULS ID</button>
                <button data-type="name">Name</button>
                <button data-type="geographic">Geographic</button>
            </div>
            
            <form class="search-form" id="searchForm">
                <input type="text" id="searchInput" placeholder="Enter call sign..." required>
                <button type="submit">Search</button>
            </form>
            
            <div class="geographic-filters" id="geoFilters">
                <select id="regionSelect">
                    <option value="">Select Region...</option>
                    <option value="northeast">Northeast</option>
                    <option value="southeast">Southeast</option>
                    <option value="midwest">Midwest</option>
                    <option value="southwest">Southwest</option>
                    <option value="west">West</option>
                </select>
                <select id="stateSelect">
                    <option value="">Select State...</option>
                </select>
                <input type="text" id="countyInput" placeholder="County...">
                <input type="text" id="cityInput" placeholder="City...">
            </div>
        </div>
        
        <div class="results" id="results" style="display: none;">
            <div class="results-header">
                <div class="results-count" id="resultsCount"></div>
                <button class="export-btn" id="exportBtn">Export to CSV</button>
            </div>
            <div id="resultsTable"></div>
            <div class="pagination" id="pagination"></div>
        </div>
        
        <div class="loading" id="loading" style="display: none;">Loading...</div>
    </div>
    
    <script>
        let currentSearchType = 'callsign';
        let currentSearchParams = {};
        let currentPage = 1;
        
        // Search type buttons
        document.querySelectorAll('.search-type button').forEach(btn => {
            btn.addEventListener('click', function() {
                document.querySelectorAll('.search-type button').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                currentSearchType = this.dataset.type;
                updateSearchUI();
            });
        });
        
        function updateSearchUI() {
            const input = document.getElementById('searchInput');
            const geoFilters = document.getElementById('geoFilters');
            
            if (currentSearchType === 'geographic') {
                input.style.display = 'none';
                input.required = false;
                geoFilters.classList.add('active');
                loadStates();
            } else {
                input.style.display = 'block';
                input.required = true;
                geoFilters.classList.remove('active');
                
                // Update placeholder
                const placeholders = {
                    'callsign': 'Enter call sign...',
                    'frn': 'Enter FRN...',
                    'uls_id': 'Enter ULS System ID...',
                    'name': 'Enter first and/or last name...'
                };
                input.placeholder = placeholders[currentSearchType];
            }
        }
        
        // Load states
        async function loadStates() {
            try {
                const response = await fetch('/api/regions');
                const data = await response.json();
                const select = document.getElementById('stateSelect');
                select.innerHTML = '<option value="">Select State...</option>';
                data.states.forEach(state => {
                    select.innerHTML += `<option value="${state}">${state}</option>`;
                });
            } catch (error) {
                console.error('Error loading states:', error);
            }
        }
        
        // Search form
        document.getElementById('searchForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            currentPage = 1;
            await performSearch();
        });
        
        async function performSearch() {
            const loading = document.getElementById('loading');
            const results = document.getElementById('results');
            
            loading.style.display = 'block';
            results.style.display = 'none';
            
            // Build search parameters
            currentSearchParams = {
                type: currentSearchType,
                page: currentPage,
                per_page: 50
            };
            
            if (currentSearchType === 'geographic') {
                currentSearchParams.region = document.getElementById('regionSelect').value;
                currentSearchParams.state = document.getElementById('stateSelect').value;
                currentSearchParams.county = document.getElementById('countyInput').value;
                currentSearchParams.city = document.getElementById('cityInput').value;
            } else {
                currentSearchParams.q = document.getElementById('searchInput').value;
            }
            
            try {
                const params = new URLSearchParams(currentSearchParams);
                const response = await fetch(`/api/search?${params}`);
                const data = await response.json();
                
                if (response.ok) {
                    displayResults(data);
                } else {
                    alert('Error: ' + (data.error || 'Search failed'));
                }
            } catch (error) {
                alert('Error performing search: ' + error.message);
            } finally {
                loading.style.display = 'none';
            }
        }
        
        function displayResults(data) {
            const results = document.getElementById('results');
            const resultsCount = document.getElementById('resultsCount');
            const resultsTable = document.getElementById('resultsTable');
            const pagination = document.getElementById('pagination');
            
            resultsCount.textContent = `Found ${data.total} results`;
            
            if (data.results.length === 0) {
                resultsTable.innerHTML = '<p>No results found</p>';
                pagination.innerHTML = '';
                results.style.display = 'block';
                return;
            }
            
            // Build table
            let html = '<table><thead><tr>';
            html += '<th>Call Sign</th><th>Name</th><th>Status</th><th>Grant Date</th><th>Expiration</th><th>Location</th>';
            html += '</tr></thead><tbody>';
            
            data.results.forEach(row => {
                const statusClass = `status-${row.license_status || 'U'}`;
                html += '<tr>';
                html += `<td><strong>${row.call_sign || ''}</strong></td>`;
                html += `<td>${row.entity_name || ''}</td>`;
                html += `<td><span class="license-status ${statusClass}">${row.license_status || ''}</span></td>`;
                html += `<td>${row.grant_date || ''}</td>`;
                html += `<td>${row.expired_date || ''}</td>`;
                html += `<td>${row.address || ''}</td>`;
                html += '</tr>';
            });
            
            html += '</tbody></table>';
            resultsTable.innerHTML = html;
            
            // Build pagination
            let pageHtml = '';
            
            if (data.page > 1) {
                pageHtml += '<button onclick="changePage(1)">First</button>';
                pageHtml += `<button onclick="changePage(${data.page - 1})">Previous</button>`;
            }
            
            // Show page numbers
            const maxPages = Math.min(data.total_pages, 10);
            const startPage = Math.max(1, data.page - 5);
            const endPage = Math.min(data.total_pages, startPage + 9);
            
            for (let i = startPage; i <= endPage; i++) {
                const active = i === data.page ? 'active' : '';
                pageHtml += `<button class="${active}" onclick="changePage(${i})">${i}</button>`;
            }
            
            if (data.page < data.total_pages) {
                pageHtml += `<button onclick="changePage(${data.page + 1})">Next</button>`;
                pageHtml += `<button onclick="changePage(${data.total_pages})">Last</button>`;
            }
            
            pagination.innerHTML = pageHtml;
            results.style.display = 'block';
        }
        
        function changePage(page) {
            currentPage = page;
            performSearch();
        }
        
        // Export to CSV
        document.getElementById('exportBtn').addEventListener('click', async () => {
            const params = {...currentSearchParams};
            delete params.page;
            delete params.per_page;
            
            try {
                const response = await fetch('/api/export/csv', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(params)
                });
                
                if (response.ok) {
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `uls_export_${new Date().getTime()}.csv`;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                } else {
                    alert('Export failed');
                }
            } catch (error) {
                alert('Error exporting data: ' + error.message);
            }
        });
    </script>
</body>
</html>'''

# Save the HTML template
with open('templates/index.html', 'w') as f:
    f.write(HTML_TEMPLATE)

if __name__ == '__main__':
    # Check if database exists
    if not os.path.exists(app.config['DATABASE']):
        logger.warning(f"Database not found at {app.config['DATABASE']}")
        logger.warning("Please run the import script first to create the database")
    
    # Run the application
    app.run(debug=True, host='0.0.0.0', port=5120)