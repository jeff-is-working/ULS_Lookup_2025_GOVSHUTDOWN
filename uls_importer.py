#!/usr/bin/env python3
"""
FCC ULS Database Import Script for SQLite
Imports FCC ULS public access files into SQLite database
Handles both complete weekly exports and daily difference files
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ULSImporter:
    def __init__(self, db_path='uls_database.db'):
        """Initialize the ULS importer with database path"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
        # Mapping of file prefixes to table names
        self.table_mapping = {
            'A2': 'PUBACC_A2', 'AC': 'PUBACC_AC', 'AD': 'PUBACC_AD',
            'AG': 'PUBACC_AG', 'AH': 'PUBACC_AH', 'AM': 'PUBACC_AM',
            'AN': 'PUBACC_AN', 'AP': 'PUBACC_AP', 'AS': 'PUBACC_AS',
            'AT': 'PUBACC_AT', 'BC': 'PUBACC_BC', 'BD': 'PUBACC_BD',
            'BE': 'PUBACC_BE', 'BF': 'PUBACC_BF', 'BL': 'PUBACC_BL',
            'BO': 'PUBACC_BO', 'BT': 'PUBACC_BT', 'CD': 'PUBACC_CD',
            'CF': 'PUBACC_CF', 'CG': 'PUBACC_CG', 'CO': 'PUBACC_CO',
            'CP': 'PUBACC_CP', 'CS': 'PUBACC_CS', 'EM': 'PUBACC_EM',
            'EN': 'PUBACC_EN', 'F2': 'PUBACC_F2', 'F3': 'PUBACC_F3',
            'F4': 'PUBACC_F4', 'F5': 'PUBACC_F5', 'F6': 'PUBACC_F6',
            'FA': 'PUBACC_FA', 'FC': 'PUBACC_FC', 'FF': 'PUBACC_FF',
            'FR': 'PUBACC_FR', 'FS': 'PUBACC_FS', 'FT': 'PUBACC_FT',
            'HD': 'PUBACC_HD', 'HS': 'PUBACC_HS', 'IA': 'PUBACC_IA',
            'IR': 'PUBACC_IR', 'LA': 'PUBACC_LA', 'L2': 'PUBACC_L2',
            'L3': 'PUBACC_L3', 'L4': 'PUBACC_L4', 'L5': 'PUBACC_L5',
            'L6': 'PUBACC_L6', 'LC': 'PUBACC_LC', 'LD': 'PUBACC_LD',
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
            'UA': 'PUBACC_UA', 'VC': 'PUBACC_VC', 'EC': 'PUBACC_EC',
            'IF': 'PUBACC_IF', 'A3': 'PUBACC_A3'
        }
        
    def connect(self):
        """Connect to SQLite database"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        # Enable foreign keys
        self.cursor.execute("PRAGMA foreign_keys = ON")
        logger.info(f"Connected to database: {self.db_path}")
        
    def disconnect(self):
        """Disconnect from database"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
            
    def create_schema(self, sql_file_path):
        """Create database schema from SQL definition file"""
        logger.info(f"Creating schema from: {sql_file_path}")
        
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
            
        # Convert SQL Server syntax to SQLite
        sql_content = self.convert_to_sqlite(sql_content)
        
        # Split into individual statements
        statements = sql_content.split(';\n')
        
        for statement in statements:
            statement = statement.strip()
            if statement and statement.lower().startswith('create table'):
                try:
                    self.cursor.execute(statement)
                    logger.debug(f"Created table from statement: {statement[:50]}...")
                except sqlite3.Error as e:
                    logger.warning(f"Error creating table: {e}")
                    
        # Create indexes for better performance
        self.create_indexes()
        
        # Create import tracking table
        self.create_import_tracking_table()
        
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
            "CREATE INDEX IF NOT EXISTS idx_hd_call_sign ON PUBACC_HD(call_sign)",
            "CREATE INDEX IF NOT EXISTS idx_hd_uls_file ON PUBACC_HD(uls_file_number)",
            "CREATE INDEX IF NOT EXISTS idx_hd_unique_id ON PUBACC_HD(unique_system_identifier)",
            "CREATE INDEX IF NOT EXISTS idx_en_unique_id ON PUBACC_EN(unique_system_identifier)",
            "CREATE INDEX IF NOT EXISTS idx_en_licensee_id ON PUBACC_EN(licensee_id)",
            "CREATE INDEX IF NOT EXISTS idx_en_entity_name ON PUBACC_EN(entity_name)",
            "CREATE INDEX IF NOT EXISTS idx_lo_call_sign ON PUBACC_LO(call_sign)",
            "CREATE INDEX IF NOT EXISTS idx_lo_location ON PUBACC_LO(call_sign, location_number)",
            "CREATE INDEX IF NOT EXISTS idx_fr_call_sign ON PUBACC_FR(call_sign)",
            "CREATE INDEX IF NOT EXISTS idx_fr_frequency ON PUBACC_FR(call_sign, location_number, antenna_number)",
            "CREATE INDEX IF NOT EXISTS idx_hs_call_sign ON PUBACC_HS(callsign)",
            "CREATE INDEX IF NOT EXISTS idx_hs_uls_file ON PUBACC_HS(uls_file_number)",
        ]
        
        for index in indexes:
            try:
                self.cursor.execute(index)
                logger.debug(f"Created index: {index}")
            except sqlite3.Error as e:
                logger.warning(f"Error creating index: {e}")
                
        self.conn.commit()
        
    def create_import_tracking_table(self):
        """Create table to track imports for update management"""
        sql = """
        CREATE TABLE IF NOT EXISTS import_tracking (
            import_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            import_type TEXT NOT NULL,
            import_date TEXT NOT NULL,
            records_imported INTEGER,
            status TEXT,
            UNIQUE(file_name)
        )
        """
        self.cursor.execute(sql)
        self.conn.commit()
        logger.info("Created import tracking table")
        
    def import_zip_file(self, zip_path, import_type='full', replace=False):
        """Import data from a zip file"""
        if not os.path.exists(zip_path):
            logger.error(f"File not found: {zip_path}")
            return False
            
        # Check if already imported
        if not replace:
            self.cursor.execute(
                "SELECT import_id FROM import_tracking WHERE file_name = ?",
                (os.path.basename(zip_path),)
            )
            if self.cursor.fetchone():
                logger.info(f"File already imported: {zip_path}")
                return True
                
        logger.info(f"Importing {import_type} file: {zip_path}")
        total_records = 0
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for file_name in zf.namelist():
                    if file_name.upper().endswith('.DAT'):
                        logger.info(f"Processing: {file_name}")
                        
                        # Extract file prefix
                        prefix = file_name.upper().replace('.DAT', '')
                        table_name = self.table_mapping.get(prefix)
                        
                        if not table_name:
                            logger.warning(f"Unknown file prefix: {prefix}")
                            continue
                            
                        # Read and import data
                        with zf.open(file_name) as dat_file:
                            content = dat_file.read().decode('utf-8', errors='replace')
                            records = self.import_dat_content(
                                content, table_name, import_type, replace
                            )
                            total_records += records
                            logger.info(f"Imported {records} records from {file_name}")
                            
            # Record successful import
            self.cursor.execute("""
                INSERT OR REPLACE INTO import_tracking 
                (file_name, import_type, import_date, records_imported, status)
                VALUES (?, ?, ?, ?, ?)
            """, (
                os.path.basename(zip_path),
                import_type,
                datetime.now().isoformat(),
                total_records,
                'completed'
            ))
            self.conn.commit()
            
            logger.info(f"Successfully imported {total_records} total records")
            return True
            
        except Exception as e:
            logger.error(f"Error importing zip file: {e}")
            self.conn.rollback()
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
            
            # Clean values
            values = [v.strip() if v else None for v in values]
            
            # Build insert query
            if import_type == 'daily' or replace:
                # Use INSERT OR REPLACE for updates
                placeholders = ','.join(['?' for _ in columns])
                column_names = ','.join(columns)
                sql = f"INSERT OR REPLACE INTO {table_name} ({column_names}) VALUES ({placeholders})"
            else:
                # Use INSERT OR IGNORE for initial load
                placeholders = ','.join(['?' for _ in columns])
                column_names = ','.join(columns)
                sql = f"INSERT OR IGNORE INTO {table_name} ({column_names}) VALUES ({placeholders})"
                
            try:
                self.cursor.execute(sql, values)
                records_imported += self.cursor.rowcount
            except sqlite3.Error as e:
                logger.debug(f"Error inserting record: {e}")
                continue
                
        self.conn.commit()
        return records_imported
        
    def import_directory(self, directory_path, pattern='*.zip', import_type='full'):
        """Import all matching files from a directory"""
        path = Path(directory_path)
        files = sorted(path.glob(pattern))
        
        logger.info(f"Found {len(files)} files matching pattern: {pattern}")
        
        for file_path in files:
            self.import_zip_file(str(file_path), import_type=import_type)
            
    def get_import_status(self):
        """Get status of all imports"""
        self.cursor.execute("""
            SELECT file_name, import_type, import_date, records_imported, status
            FROM import_tracking
            ORDER BY import_date DESC
        """)
        return self.cursor.fetchall()
        
    def get_table_counts(self):
        """Get record counts for all tables"""
        counts = {}
        for prefix, table_name in self.table_mapping.items():
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = self.cursor.fetchone()[0]
                if count > 0:
                    counts[table_name] = count
            except sqlite3.Error:
                pass
        return counts


def main():
    parser = argparse.ArgumentParser(description='Import FCC ULS data into SQLite database')
    parser.add_argument('--db', default='uls_database.db', help='Path to SQLite database')
    parser.add_argument('--schema', help='Path to SQL schema file (for initial setup)')
    parser.add_argument('--import-file', help='Path to zip file to import')
    parser.add_argument('--import-dir', help='Path to directory with zip files')
    parser.add_argument('--import-type', choices=['full', 'daily'], default='full',
                       help='Type of import (full or daily update)')
    parser.add_argument('--replace', action='store_true', 
                       help='Replace existing records (for updates)')
    parser.add_argument('--status', action='store_true', 
                       help='Show import status and table counts')
    parser.add_argument('--pattern', default='*.zip', 
                       help='File pattern for directory import')
    
    args = parser.parse_args()
    
    # Initialize importer
    importer = ULSImporter(args.db)
    importer.connect()
    
    try:
        # Create schema if specified
        if args.schema:
            importer.create_schema(args.schema)
            
        # Import single file
        if args.import_file:
            success = importer.import_zip_file(
                args.import_file, 
                import_type=args.import_type,
                replace=args.replace
            )
            if success:
                logger.info("Import completed successfully")
            else:
                logger.error("Import failed")
                sys.exit(1)
                
        # Import directory
        if args.import_dir:
            importer.import_directory(
                args.import_dir,
                pattern=args.pattern,
                import_type=args.import_type
            )
            
        # Show status
        if args.status:
            print("\n=== Import Status ===")
            imports = importer.get_import_status()
            if imports:
                for imp in imports:
                    print(f"File: {imp[0]}")
                    print(f"  Type: {imp[1]}, Date: {imp[2]}")
                    print(f"  Records: {imp[3]}, Status: {imp[4]}")
            else:
                print("No imports found")
                
            print("\n=== Table Record Counts ===")
            counts = importer.get_table_counts()
            for table, count in sorted(counts.items()):
                print(f"{table}: {count:,} records")
                
    finally:
        importer.disconnect()


if __name__ == "__main__":
    main()