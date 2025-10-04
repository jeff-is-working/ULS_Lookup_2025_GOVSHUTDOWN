#!/usr/bin/env python3
"""
FCC ULS Database Import Script for SQLite
Imports FCC ULS public access files into SQLite database
Handles both license data (l_*.zip) and application data (a_*.zip)
Supports complete weekly exports and daily difference files
"""

import sqlite3
import zipfile
import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path
import csv
import io
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ULSImporter:
    def __init__(self, db_path='uls.db'):
        """Initialize the ULS importer with database path"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
        # Mapping of file prefixes to table names
        # These tables are used for both license and application data
        self.table_mapping = {
            'A2': 'PUBACC_A2', 'A3': 'PUBACC_A3', 'AC': 'PUBACC_AC', 
            'AD': 'PUBACC_AD', 'AG': 'PUBACC_AG', 'AH': 'PUBACC_AH', 
            'AM': 'PUBACC_AM', 'AN': 'PUBACC_AN', 'AP': 'PUBACC_AP', 
            'AS': 'PUBACC_AS', 'AT': 'PUBACC_AT', 'BC': 'PUBACC_BC', 
            'BD': 'PUBACC_BD', 'BE': 'PUBACC_BE', 'BF': 'PUBACC_BF', 
            'BL': 'PUBACC_BL', 'BO': 'PUBACC_BO', 'BT': 'PUBACC_BT', 
            'CD': 'PUBACC_CD', 'CF': 'PUBACC_CF', 'CG': 'PUBACC_CG', 
            'CO': 'PUBACC_CO', 'CP': 'PUBACC_CP', 'CS': 'PUBACC_CS', 
            'EC': 'PUBACC_EC', 'EM': 'PUBACC_EM', 'EN': 'PUBACC_EN', 
            'F2': 'PUBACC_F2', 'F3': 'PUBACC_F3', 'F4': 'PUBACC_F4', 
            'F5': 'PUBACC_F5', 'F6': 'PUBACC_F6', 'FA': 'PUBACC_FA', 
            'FC': 'PUBACC_FC', 'FF': 'PUBACC_FF', 'FR': 'PUBACC_FR', 
            'FS': 'PUBACC_FS', 'FT': 'PUBACC_FT', 'HD': 'PUBACC_HD', 
            'HS': 'PUBACC_HS', 'IA': 'PUBACC_IA', 'IF': 'PUBACC_IF',
            'IR': 'PUBACC_IR', 'L2': 'PUBACC_L2', 'L3': 'PUBACC_L3', 
            'L4': 'PUBACC_L4', 'L5': 'PUBACC_L5', 'L6': 'PUBACC_L6', 
            'LA': 'PUBACC_LA', 'LC': 'PUBACC_LC', 'LD': 'PUBACC_LD', 
            'LF': 'PUBACC_LF', 'LH': 'PUBACC_LH', 'LL': 'PUBACC_LL', 
            'LM': 'PUBACC_LM', 'LO': 'PUBACC_LO', 'LS': 'PUBACC_LS', 
            'MC': 'PUBACC_MC', 'ME': 'PUBACC_ME', 'MF': 'PUBACC_MF', 
            'MH': 'PUBACC_MH', 'MI': 'PUBACC_MI', 'MK': 'PUBACC_MK', 
            'MP': 'PUBACC_MP', 'MW': 'PUBACC_MW', 'O2': 'PUBACC_O2', 
            'OP': 'PUBACC_OP', 'P2': 'PUBACC_P2', 'PA': 'PUBACC_PA', 
            'PC': 'PUBACC_PC', 'RA': 'PUBACC_RA', 'RC': 'PUBACC_RC', 
            'RE': 'PUBACC_RE', 'RI': 'PUBACC_RI', 'RZ': 'PUBACC_RZ', 
            'SC': 'PUBACC_SC', 'SE': 'PUBACC_SE', 'SF': 'PUBACC_SF', 
            'SG': 'PUBACC_SG', 'SH': 'PUBACC_SH', 'SI': 'PUBACC_SI', 
            'SR': 'PUBACC_SR', 'ST': 'PUBACC_ST', 'SV': 'PUBACC_SV', 
            'TA': 'PUBACC_TA', 'TL': 'PUBACC_TL', 'TP': 'PUBACC_TP', 
            'UA': 'PUBACC_UA', 'VC': 'PUBACC_VC'
        }
        
    def connect(self):
        """Connect to SQLite database with optimizations"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Enable optimizations
        self.cursor.execute("PRAGMA foreign_keys = ON")
        self.cursor.execute("PRAGMA journal_mode = WAL")
        self.cursor.execute("PRAGMA synchronous = NORMAL")
        self.cursor.execute("PRAGMA cache_size = -128000")  # 128MB cache
        self.cursor.execute("PRAGMA temp_store = MEMORY")
        self.cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap
        
        logger.info(f"Connected to database: {self.db_path}")
        
    def disconnect(self):
        """Disconnect from database"""
        if self.conn:
            # Optimize database before closing
            self.cursor.execute("PRAGMA optimize")
            self.conn.close()
            logger.info("Disconnected from database")
            
    def create_schema(self, sql_file_path):
        """Create database schema from SQL definition file"""
        logger.info(f"Creating schema from: {sql_file_path}")
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
            
        # Convert SQL Server syntax to SQLite
        sql_content = self.convert_to_sqlite(sql_content)
        
        # Split into individual statements
        statements = sql_content.split(';\n')
        
        created_tables = []
        for statement in statements:
            statement = statement.strip()
            if statement and statement.lower().startswith('create table'):
                try:
                    self.cursor.execute(statement)
                    # Extract table name
                    match = re.search(r'create table\s+(\w+)', statement, re.IGNORECASE)
                    if match:
                        created_tables.append(match.group(1))
                    logger.debug(f"Created table from statement: {statement[:50]}...")
                except sqlite3.Error as e:
                    logger.warning(f"Error creating table: {e}")
                    
        logger.info(f"Created {len(created_tables)} tables")
        
        # Create indexes for better performance
        self.create_indexes()
        
        # Create import tracking table
        self.create_import_tracking_table()
        
        # Create metadata table
        self.create_metadata_table()
        
        self.conn.commit()
        logger.info("Schema creation completed")
        
    def convert_to_sqlite(self, sql_content):
        """Convert SQL Server syntax to SQLite syntax"""
        # Remove 'dbo.' prefix
        sql_content = sql_content.replace('dbo.', '')
        
        # Remove 'go' statements
        sql_content = sql_content.replace('\ngo\n', ';\n')
        sql_content = sql_content.replace('\ngo', ';')
        
        # Convert data types
        sql_content = sql_content.replace('numeric(', 'DECIMAL(')
        sql_content = sql_content.replace('money', 'DECIMAL(19,4)')
        sql_content = sql_content.replace('datetime', 'TEXT')
        sql_content = sql_content.replace('tinyint', 'INTEGER')
        sql_content = sql_content.replace('smallint', 'INTEGER')
        sql_content = sql_content.replace('int', 'INTEGER')
        sql_content = sql_content.replace('char(', 'TEXT(')
        sql_content = sql_content.replace('varchar(', 'TEXT(')
        
        return sql_content
        
    def create_indexes(self):
        """Create indexes for better query performance"""
        logger.info("Creating indexes...")
        
        indexes = [
            # Header table indexes
            "CREATE INDEX IF NOT EXISTS idx_hd_call_sign ON PUBACC_HD(call_sign)",
            "CREATE INDEX IF NOT EXISTS idx_hd_uls_file ON PUBACC_HD(uls_file_number)",
            "CREATE INDEX IF NOT EXISTS idx_hd_unique_id ON PUBACC_HD(unique_system_identifier)",
            "CREATE INDEX IF NOT EXISTS idx_hd_status ON PUBACC_HD(license_status)",
            "CREATE INDEX IF NOT EXISTS idx_hd_service ON PUBACC_HD(radio_service_code)",
            "CREATE INDEX IF NOT EXISTS idx_hd_grant_date ON PUBACC_HD(grant_date)",
            
            # Entity table indexes
            "CREATE INDEX IF NOT EXISTS idx_en_unique_id ON PUBACC_EN(unique_system_identifier)",
            "CREATE INDEX IF NOT EXISTS idx_en_licensee_id ON PUBACC_EN(licensee_id)",
            "CREATE INDEX IF NOT EXISTS idx_en_entity_name ON PUBACC_EN(entity_name)",
            "CREATE INDEX IF NOT EXISTS idx_en_frn ON PUBACC_EN(frn)",
            "CREATE INDEX IF NOT EXISTS idx_en_last_name ON PUBACC_EN(last_name)",
            "CREATE INDEX IF NOT EXISTS idx_en_state ON PUBACC_EN(state)",
            "CREATE INDEX IF NOT EXISTS idx_en_city ON PUBACC_EN(city)",
            
            # Location table indexes
            "CREATE INDEX IF NOT EXISTS idx_lo_call_sign ON PUBACC_LO(call_sign)",
            "CREATE INDEX IF NOT EXISTS idx_lo_location ON PUBACC_LO(call_sign, location_number)",
            "CREATE INDEX IF NOT EXISTS idx_lo_state ON PUBACC_LO(location_state)",
            "CREATE INDEX IF NOT EXISTS idx_lo_county ON PUBACC_LO(location_county)",
            
            # Frequency table indexes
            "CREATE INDEX IF NOT EXISTS idx_fr_call_sign ON PUBACC_FR(call_sign)",
            "CREATE INDEX IF NOT EXISTS idx_fr_frequency ON PUBACC_FR(call_sign, location_number, antenna_number)",
            "CREATE INDEX IF NOT EXISTS idx_fr_freq_assigned ON PUBACC_FR(frequency_assigned)",
            
            # Application data indexes
            "CREATE INDEX IF NOT EXISTS idx_ad_uls_file ON PUBACC_AD(uls_file_number)",
            "CREATE INDEX IF NOT EXISTS idx_ad_unique_id ON PUBACC_AD(unique_system_identifier)",
            "CREATE INDEX IF NOT EXISTS idx_ad_receipt_date ON PUBACC_AD(receipt_date)",
            "CREATE INDEX IF NOT EXISTS idx_ad_status ON PUBACC_AD(application_status)",
            
            # History table indexes
            "CREATE INDEX IF NOT EXISTS idx_hs_call_sign ON PUBACC_HS(callsign)",
            "CREATE INDEX IF NOT EXISTS idx_hs_uls_file ON PUBACC_HS(uls_file_number)",
            "CREATE INDEX IF NOT EXISTS idx_hs_date ON PUBACC_HS(log_date)",
            
            # Amateur table indexes
            "CREATE INDEX IF NOT EXISTS idx_am_callsign ON PUBACC_AM(callsign)",
            "CREATE INDEX IF NOT EXISTS idx_am_unique_id ON PUBACC_AM(unique_system_identifier)",
            "CREATE INDEX IF NOT EXISTS idx_am_operator_class ON PUBACC_AM(operator_class)",
        ]
        
        for index in indexes:
            try:
                self.cursor.execute(index)
                logger.debug(f"Created index: {index[:60]}...")
            except sqlite3.Error as e:
                logger.warning(f"Error creating index: {e}")
                
        self.conn.commit()
        logger.info(f"Created {len(indexes)} indexes")
        
    def create_import_tracking_table(self):
        """Create table to track imports for update management"""
        sql = """
        CREATE TABLE IF NOT EXISTS import_tracking (
            import_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            import_type TEXT NOT NULL,
            import_date TEXT NOT NULL,
            records_imported INTEGER DEFAULT 0,
            tables_updated INTEGER DEFAULT 0,
            status TEXT,
            error_message TEXT,
            UNIQUE(file_name, import_date)
        )
        """
        self.cursor.execute(sql)
        
        # Create index on file_name
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_import_file ON import_tracking(file_name)"
        )
        
        self.conn.commit()
        logger.info("Created import tracking table")
        
    def create_metadata_table(self):
        """Create table to store database metadata"""
        sql = """
        CREATE TABLE IF NOT EXISTS db_metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
        """
        self.cursor.execute(sql)
        self.conn.commit()
        logger.info("Created metadata table")
        
    def update_metadata(self, key, value):
        """Update database metadata"""
        self.cursor.execute("""
            INSERT OR REPLACE INTO db_metadata (key, value, updated_at)
            VALUES (?, ?, ?)
        """, (key, value, datetime.now().isoformat()))
        self.conn.commit()
        
    def detect_file_type(self, zip_path):
        """Detect whether this is a license or application file"""
        filename = os.path.basename(zip_path).lower()
        
        if filename.startswith('l_'):
            return 'license'
        elif filename.startswith('a_'):
            return 'application'
        elif 'license' in filename:
            return 'license'
        elif 'application' in filename or 'app' in filename:
            return 'application'
        else:
            # Try to detect from contents
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    # Look for HD.dat (header file)
                    for name in zf.namelist():
                        if name.upper() == 'HD.DAT':
                            # Check first line
                            with zf.open(name) as f:
                                first_line = f.readline().decode('utf-8', errors='ignore')
                                # Applications have different record types
                                if first_line.startswith('HD|'):
                                    return 'license'
            except:
                pass
                
        logger.warning(f"Could not determine file type for {zip_path}, assuming license data")
        return 'license'
        
    def import_zip_file(self, zip_path, import_type='full', replace=False, file_type=None):
        """Import data from a zip file"""
        if not os.path.exists(zip_path):
            logger.error(f"File not found: {zip_path}")
            return False
            
        # Auto-detect file type if not specified
        if file_type is None:
            file_type = self.detect_file_type(zip_path)
            
        logger.info(f"Importing {file_type} data from {zip_path}")
        
        # Check if already imported (unless replace is True)
        file_name = os.path.basename(zip_path)
        if not replace:
            self.cursor.execute(
                "SELECT import_id FROM import_tracking WHERE file_name = ? AND status = 'completed'",
                (file_name,)
            )
            if self.cursor.fetchone():
                logger.info(f"File already imported: {zip_path}")
                return True
                
        total_records = 0
        tables_updated = 0
        error_message = None
        
        try:
            # Begin transaction for better performance
            self.cursor.execute("BEGIN TRANSACTION")
            
            with zipfile.ZipFile(zip_path, 'r') as zf:
                dat_files = [f for f in zf.namelist() if f.upper().endswith('.DAT')]
                logger.info(f"Found {len(dat_files)} DAT files to process")
                
                for file_name_dat in sorted(dat_files):
                    logger.info(f"Processing: {file_name_dat}")
                    
                    # Extract file prefix
                    prefix = os.path.basename(file_name_dat).upper().replace('.DAT', '')
                    table_name = self.table_mapping.get(prefix)
                    
                    if not table_name:
                        logger.warning(f"Unknown file prefix: {prefix}")
                        continue
                        
                    # Read and import data
                    with zf.open(file_name_dat) as dat_file:
                        content = dat_file.read().decode('utf-8', errors='replace')
                        records = self.import_dat_content(
                            content, table_name, import_type, replace
                        )
                        
                        if records > 0:
                            total_records += records
                            tables_updated += 1
                            logger.info(f"  → Imported {records:,} records into {table_name}")
                        else:
                            logger.debug(f"  → No new records for {table_name}")
            
            # Commit transaction
            self.conn.commit()
            
            # Record successful import
            self.cursor.execute("""
                INSERT OR REPLACE INTO import_tracking 
                (file_name, file_type, import_type, import_date, records_imported, 
                 tables_updated, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                os.path.basename(zip_path),
                file_type,
                import_type,
                datetime.now().isoformat(),
                total_records,
                tables_updated,
                'completed',
                None
            ))
            self.conn.commit()
            
            # Update metadata
            self.update_metadata(
                f'last_{file_type}_import', 
                datetime.now().isoformat()
            )
            
            logger.info(f"✓ Successfully imported {total_records:,} records from {tables_updated} tables")
            return True
            
        except Exception as e:
            logger.error(f"Error importing zip file: {e}")
            error_message = str(e)
            
            # Rollback transaction
            try:
                self.conn.rollback()
            except:
                pass
                
            # Record failed import
            try:
                self.cursor.execute("""
                    INSERT OR REPLACE INTO import_tracking 
                    (file_name, file_type, import_type, import_date, records_imported, 
                     tables_updated, status, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    os.path.basename(zip_path),
                    file_type,
                    import_type,
                    datetime.now().isoformat(),
                    total_records,
                    tables_updated,
                    'failed',
                    error_message
                ))
                self.conn.commit()
            except:
                pass
                
            return False
            
    def import_dat_content(self, content, table_name, import_type='full', replace=False):
        """Import content from a DAT file into specified table"""
        lines = content.strip().split('\n')
        if not lines:
            return 0
            
        # Get table columns
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in self.cursor.fetchall()]
        
        if not columns:
            logger.warning(f"Table {table_name} does not exist")
            return 0
            
        records_imported = 0
        batch_size = 1000
        batch = []
        
        # Build insert query
        column_names = ','.join(columns)
        placeholders = ','.join(['?' for _ in columns])
        
        if import_type == 'daily' or replace:
            # Use INSERT OR REPLACE for updates
            sql = f"INSERT OR REPLACE INTO {table_name} ({column_names}) VALUES ({placeholders})"
        else:
            # Use INSERT OR IGNORE for initial load
            sql = f"INSERT OR IGNORE INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        for line in lines:
            if not line.strip():
                continue
                
            # Parse pipe-separated values
            values = line.split('|')
            
            # Pad with None if needed
            while len(values) < len(columns):
                values.append(None)
                
            # Truncate if too many values
            values = values[:len(columns)]
            
            # Clean values - strip whitespace and convert empty strings to None
            values = [v.strip() if v and v.strip() else None for v in values]
            
            batch.append(values)
            
            # Execute batch when it reaches batch_size
            if len(batch) >= batch_size:
                try:
                    self.cursor.executemany(sql, batch)
                    records_imported += self.cursor.rowcount
                except sqlite3.Error as e:
                    logger.debug(f"Error in batch insert: {e}")
                    # Try individual inserts for this batch
                    for val in batch:
                        try:
                            self.cursor.execute(sql, val)
                            records_imported += self.cursor.rowcount
                        except sqlite3.Error:
                            pass
                batch = []
        
        # Execute remaining batch
        if batch:
            try:
                self.cursor.executemany(sql, batch)
                records_imported += self.cursor.rowcount
            except sqlite3.Error as e:
                logger.debug(f"Error in final batch insert: {e}")
                for val in batch:
                    try:
                        self.cursor.execute(sql, val)
                        records_imported += self.cursor.rowcount
                    except sqlite3.Error:
                        pass
                        
        return records_imported
        
    def import_directory(self, directory_path, pattern='*.zip', import_type='full'):
        """Import all matching files from a directory"""
        path = Path(directory_path)
        files = sorted(path.glob(pattern))
        
        logger.info(f"Found {len(files)} files matching pattern: {pattern}")
        
        # Separate license and application files
        license_files = []
        application_files = []
        
        for file_path in files:
            file_type = self.detect_file_type(str(file_path))
            if file_type == 'license':
                license_files.append(file_path)
            else:
                application_files.append(file_path)
                
        logger.info(f"  License files: {len(license_files)}")
        logger.info(f"  Application files: {len(application_files)}")
        
        # Import license files first
        success_count = 0
        for file_path in license_files:
            if self.import_zip_file(str(file_path), import_type=import_type, file_type='license'):
                success_count += 1
                
        # Then import application files
        for file_path in application_files:
            if self.import_zip_file(str(file_path), import_type=import_type, file_type='application'):
                success_count += 1
                
        logger.info(f"Successfully imported {success_count} of {len(files)} files")
        return success_count
        
    def import_both_files(self, license_file, application_file, import_type='full'):
        """Import both license and application files together"""
        logger.info("Importing license and application data together")
        
        success = True
        
        # Import license data first
        if license_file:
            logger.info(f"Step 1/2: Importing license data from {license_file}")
            if not self.import_zip_file(license_file, import_type=import_type, file_type='license'):
                logger.error("Failed to import license data")
                success = False
        
        # Import application data
        if application_file and success:
            logger.info(f"Step 2/2: Importing application data from {application_file}")
            if not self.import_zip_file(application_file, import_type=import_type, file_type='application'):
                logger.error("Failed to import application data")
                success = False
                
        if success:
            logger.info("✓ Successfully imported both license and application data")
        
        return success
        
    def get_import_status(self):
        """Get status of all imports"""
        self.cursor.execute("""
            SELECT file_name, file_type, import_type, import_date, 
                   records_imported, tables_updated, status, error_message
            FROM import_tracking
            ORDER BY import_date DESC
        """)
        return self.cursor.fetchall()
        
    def get_table_counts(self):
        """Get record counts for all tables"""
        counts = {}
        for prefix, table_name in sorted(self.table_mapping.items()):
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = self.cursor.fetchone()[0]
                if count > 0:
                    counts[table_name] = count
            except sqlite3.Error:
                pass
        return counts
        
    def get_summary_stats(self):
        """Get summary statistics about the database"""
        stats = {}
        
        try:
            # Total licenses
            self.cursor.execute("SELECT COUNT(*) FROM PUBACC_HD")
            stats['total_licenses'] = self.cursor.fetchone()[0]
            
            # Active licenses
            self.cursor.execute("SELECT COUNT(*) FROM PUBACC_HD WHERE license_status = 'A'")
            stats['active_licenses'] = self.cursor.fetchone()[0]
            
            # Total applications
            self.cursor.execute("SELECT COUNT(DISTINCT uls_file_number) FROM PUBACC_AD")
            stats['total_applications'] = self.cursor.fetchone()[0]
            
            # Pending applications
            self.cursor.execute("SELECT COUNT(*) FROM PUBACC_AD WHERE application_status = 'P'")
            stats['pending_applications'] = self.cursor.fetchone()[0]
            
            # Amateur licenses
            self.cursor.execute("SELECT COUNT(*) FROM PUBACC_AM")
            stats['amateur_licenses'] = self.cursor.fetchone()[0]
            
        except sqlite3.Error as e:
            logger.warning(f"Error getting summary stats: {e}")
            
        return stats
        
    def vacuum_database(self):
        """Optimize database by running VACUUM"""
        logger.info("Vacuuming database (this may take a while)...")
        try:
            self.cursor.execute("VACUUM")
            logger.info("Database vacuum completed")
        except sqlite3.Error as e:
            logger.error(f"Error vacuuming database: {e}")
            
    def analyze_database(self):
        """Update database statistics for query optimization"""
        logger.info("Analyzing database...")
        try:
            self.cursor.execute("ANALYZE")
            logger.info("Database analysis completed")
        except sqlite3.Error as e:
            logger.error(f"Error analyzing database: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Import FCC ULS license and application data into SQLite database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initial setup with schema
  %(prog)s --db uls.db --schema schema.sql
  
  # Import license data
  %(prog)s --db uls.db --import-file l_amat.zip
  
  # Import application data
  %(prog)s --db uls.db --import-file a_amat.zip
  
  # Import both together
  %(prog)s --db uls.db --license-file l_amat.zip --app-file a_amat.zip
  
  # Import all files from directory
  %(prog)s --db uls.db --import-dir /path/to/uls/files
  
  # Import daily updates
  %(prog)s --db uls.db --import-file l_am_tue.zip --import-type daily --replace
  
  # Show database status
  %(prog)s --db uls.db --status
  
  # Optimize database
  %(prog)s --db uls.db --vacuum --analyze
        """
    )
    
    parser.add_argument('--db', default='uls.db', 
                       help='Path to SQLite database (default: uls.db)')
    parser.add_argument('--schema', 
                       help='Path to SQL schema file (for initial setup)')
    parser.add_argument('--import-file', 
                       help='Path to single zip file to import')
    parser.add_argument('--license-file', 
                       help='Path to license data zip file (l_*.zip)')
    parser.add_argument('--app-file', 
                       help='Path to application data zip file (a_*.zip)')
    parser.add_argument('--import-dir', 
                       help='Path to directory with zip files')
    parser.add_argument('--import-type', choices=['full', 'daily'], default='full',
                       help='Type of import (default: full)')
    parser.add_argument('--replace', action='store_true', 
                       help='Replace existing records (use with daily updates)')
    parser.add_argument('--status', action='store_true', 
                       help='Show import status and database statistics')
    parser.add_argument('--pattern', default='*.zip', 
                       help='File pattern for directory import (default: *.zip)')
    parser.add_argument('--vacuum', action='store_true',
                       help='Vacuum database to reclaim space and optimize')
    parser.add_argument('--analyze', action='store_true',
                       help='Analyze database to update statistics')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize importer
    importer = ULSImporter(args.db)
    importer.connect()
    
    try:
        # Create schema if specified
        if args.schema:
            logger.info("Creating database schema...")
            importer.create_schema(args.schema)
            logger.info("Schema created successfully")
            
        # Import both license and application files
        if args.license_file or args.app_file:
            importer.import_both_files(
                args.license_file,
                args.app_file,
                import_type=args.import_type
            )
            
        # Import single file
        elif args.import_file:
            success = importer.import_zip_file(
                args.import_file, 
                import_type=args.import_type,
                replace=args.replace
            )
            if success:
                logger.info("✓ Import completed successfully")
            else:
                logger.error("✗ Import failed")
                sys.exit(1)
                
        # Import directory
        elif args.import_dir:
            importer.import_directory(
                args.import_dir,
                pattern=args.pattern,
                import_type=args.import_type
            )
            
        # Vacuum database
        if args.vacuum:
            importer.vacuum_database()
            
        # Analyze database
        if args.analyze:
            importer.analyze_database()
            
        # Show status
        if args.status or (not any([args.schema, args.import_file, args.license_file, 
                                     args.app_file, args.import_dir, args.vacuum, args.analyze])):
            print("\n" + "="*70)
            print("DATABASE STATUS")
            print("="*70)
            
            # Summary statistics
            print("\n📊 Summary Statistics:")
            stats = importer.get_summary_stats()
            for key, value in stats.items():
                print(f"  {key.replace('_', ' ').title()}: {value:,}")
            
            # Import history
            print("\n📥 Import History:")
            imports = importer.get_import_status()
            if imports:
                print(f"  {'File':<30} {'Type':<12} {'Date':<20} {'Records':<12} {'Status':<10}")
                print(f"  {'-'*30} {'-'*12} {'-'*20} {'-'*12} {'-'*10}")
                for imp in imports[:10]:  # Show last 10 imports
                    file_name = imp[0][:28] if len(imp[0]) > 28 else imp[0]
                    status = imp[6]
                    status_symbol = "✓" if status == "completed" else "✗"
                    print(f"  {file_name:<30} {imp[1]:<12} {imp[3][:19]:<20} {imp[4]:>10,}  {status_symbol} {status:<8}")
            else:
                print("  No imports found")
                
            # Table counts
            print("\n📋 Table Record Counts:")
            counts = importer.get_table_counts()
            if counts:
                # Group by category
                total = sum(counts.values())
                print(f"  {'Table':<20} {'Records':>15} {'% of Total':>12}")
                print(f"  {'-'*20} {'-'*15} {'-'*12}")
                for table, count in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:20]:
                    pct = (count / total * 100) if total > 0 else 0
                    print(f"  {table:<20} {count:>15,} {pct:>11.1f}%")
                print(f"  {'-'*20} {'-'*15} {'-'*12}")
                print(f"  {'TOTAL':<20} {total:>15,} {100.0:>11.1f}%")
            else:
                print("  No data in database")
                
            # Database file info
            print("\n💾 Database File:")
            if os.path.exists(args.db):
                size_mb = os.path.getsize(args.db) / (1024 * 1024)
                print(f"  Path: {args.db}")
                print(f"  Size: {size_mb:,.2f} MB")
            
            print("\n" + "="*70)
                
    except KeyboardInterrupt:
        logger.info("\nImport interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        importer.disconnect()


if __name__ == "__main__":
    main()