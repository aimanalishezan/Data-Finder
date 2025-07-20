#!/usr/bin/env python3
"""
Efficiently process large JSON files and store data in SQLite/PostgreSQL.
"""
import argparse
import json
import os
import sqlite3
import gzip
import ijson
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Iterator, Union, Tuple
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_processing.log')
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self, db_path: str = 'company_data.db', use_sqlite: bool = True):
        """Initialize database connection."""
        self.use_sqlite = use_sqlite
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self._connect()
        self._create_tables()
    
    def _connect(self):
        """Establish database connection."""
        try:
            if self.use_sqlite:
                self.conn = sqlite3.connect(self.db_path)
                self.conn.execute('PRAGMA journal_mode=WAL')
                self.conn.execute('PRAGMA synchronous=NORMAL')
                self.conn.execute('PRAGMA cache_size=-10000')  # 10MB cache
            else:
                import psycopg2
                from psycopg2.extras import execute_batch
                self.conn = psycopg2.connect(
                    dbname='company_finder',
                    user='postgres',
                    password='postgres',
                    host='localhost',
                    port='5432'
                )
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def _create_tables(self):
        """Create necessary tables if they don't exist."""
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                industry TEXT,
                city TEXT,
                company_type TEXT,
                address TEXT,
                registration_date DATE,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            if not self.use_sqlite:
                create_table_sql = create_table_sql.replace(
                    'INTEGER PRIMARY KEY AUTOINCREMENT',
                    'SERIAL PRIMARY KEY'
                ).replace('TEXT', 'VARCHAR(1000)')
            
            self.cursor.execute(create_table_sql)
            self._create_indexes()
            self.conn.commit()
            logger.info("Database tables created/verified")
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            self.conn.rollback()
            raise
    
    def _create_indexes(self):
        """Create indexes for faster queries."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name);",
            "CREATE INDEX IF NOT EXISTS idx_companies_business_id ON companies(business_id);",
            "CREATE INDEX IF NOT EXISTS idx_companies_industry ON companies(industry);",
            "CREATE INDEX IF NOT EXISTS idx_companies_city ON companies(city);",
            "CREATE INDEX IF NOT EXISTS idx_companies_company_type ON companies(company_type);",
            "CREATE INDEX IF NOT EXISTS idx_companies_registration_date ON companies(registration_date);"
        ]
        
        for sql in indexes:
            try:
                self.cursor.execute(sql)
            except Exception as e:
                logger.warning(f"Error creating index: {e}")
    
    def insert_companies_batch(self, companies: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Insert a batch of companies into the database."""
        if not companies:
            return 0, 0
            
        inserted = 0
        errors = 0
        
        for company in companies:
            try:
                # Prepare data
                values = (
                    company.get('business_id'),
                    company.get('name'),
                    company.get('industry'),
                    company.get('city'),
                    company.get('company_type'),
                    company.get('address'),
                    company.get('registration_date'),
                    json.dumps(company.get('metadata', {}))
                )
                
                # Build and execute query
                if self.use_sqlite:
                    query = """
                    INSERT OR IGNORE INTO companies 
                    (business_id, name, industry, city, company_type, address, registration_date, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """
                else:
                    query = """
                    INSERT INTO companies 
                    (business_id, name, industry, city, company_type, address, registration_date, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (business_id) DO NOTHING
                    """
                
                self.cursor.execute(query, values)
                if self.cursor.rowcount > 0:
                    inserted += 1
                    
            except Exception as e:
                errors += 1
                logger.warning(f"Error inserting company: {e}")
        
        if inserted > 0:
            self.conn.commit()
            
        return inserted, errors
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

class JSONProcessor:
    """Processes large JSON files efficiently."""
    
    def __init__(self, file_path: str, batch_size: int = 1000):
        """Initialize JSON processor."""
        self.file_path = Path(file_path)
        self.batch_size = batch_size
        self.is_gzipped = self.file_path.suffix.lower() in ('.gz', '.gzip')
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
    
    def process_file(self) -> Iterator[List[Dict[str, Any]]]:
        """Process JSON file in batches with improved structure detection."""
        batch = []
        processed = 0
        
        # Open file with appropriate handler
        try:
            file_handle = (
                gzip.open(self.file_path, 'rb') 
                if self.is_gzipped 
                else open(self.file_path, 'rb')
            )
            
            # Read first 1KB to detect file structure
            sample = file_handle.read(1024)
            file_handle.seek(0)
            
            # Check if it's a JSON array or newline-delimited JSON
            is_json_array = sample.strip().startswith(b'[')
            is_ndjson = any(b'\n' in line for line in sample.split(b'{')[:3] if line.strip())
            
            logger.info(f"Processing file: {self.file_path}")
            logger.info(f"Detected: {'GZIP compressed ' if self.is_gzipped else ''}{'JSON array' if is_json_array else 'NDJSON' if is_ndjson else 'unknown format'}")
            
            if is_json_array:
                logger.info("Processing as JSON array")
                for batch in self._process_json_array(file_handle):
                    processed += len(batch)
                    yield batch
            else:
                logger.info("Processing as newline-delimited JSON")
                for batch in self._process_ndjson(file_handle):
                    processed += len(batch)
                    yield batch
            
            logger.info(f"Successfully processed {processed} records")
            
        except Exception as e:
            logger.error(f"Error processing file: {e}", exc_info=True)
            raise
            
        finally:
            if 'file_handle' in locals() and file_handle:
                file_handle.close()
    
    def _process_json_array(self, file_handle) -> Iterator[List[Dict[str, Any]]]:
        """Process a JSON array file."""
        batch = []
        
        # Parse the JSON array
        for item in ijson.items(file_handle, 'item'):
            processed = self._process_company(item)
            if processed:
                batch.append(processed)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        
        if batch:
            yield batch
    
                return None
            
            # Log first few records for debugging
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Processing record: {json.dumps(company, default=str)[:500]}...")
            
            # Extract business_id from various possible fields
            business_id = None
            for field in ['business_id', 'id', 'company_id', 'identifier']:
                if field in company and company[field]:
                    business_id = str(company[field]).strip()
                    if business_id:
                        break
            
            if not business_id:
                logger.debug(f"Skipping record with missing business_id: {company}")
                return None
            
            # Extract name from various possible fields
            name = None
            for field in ['name', 'company_name', 'business_name', 'title']:
                if field in company and company[field]:
                    name = str(company[field]).strip()
                    if name:
                        break
            
            if not name:
                logger.debug(f"Skipping record {business_id} with missing name")
                return None
            
            # Process registration date from various formats
            reg_date = None
            date_fields = ['registration_date', 'founding_date', 'established', 'date_created']
            
            for field in date_fields:
                if field in company and company[field]:
                    try:
                        date_val = company[field]
                        if isinstance(date_val, (int, float)):
                            # Handle timestamps
                            reg_date = datetime.fromtimestamp(date_val).date()
                        elif isinstance(date_val, str):
                            # Try common date formats
                            for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%d.%m.%Y', '%Y'):
                                try:
                                    reg_date = datetime.strptime(date_val, fmt).date()
                                    break
                                except ValueError:
                                    continue
                        
                        if reg_date:
                            break
                            
                    except Exception as e:
                        logger.debug(f"Error parsing date {field}={company[field]}: {e}")
            
            # Extract other fields with flexible field names
            def get_field(possible_names, default=None):
                for name in possible_names:
                    if name in company and company[name] not in (None, '', [], {}):
                        return company[name]
                return default
            
            # Create processed company record
            processed = {
                'business_id': business_id,
                'name': name,
                'industry': str(get_field(['industry', 'sector', 'business_type'], '')).strip() or None,
                'city': str(get_field(['city', 'municipality', 'location'], '')).strip() or None,
                'company_type': str(get_field(['company_type', 'type', 'legal_form'], '')).strip() or None,
                'address': str(get_field(['address', 'street_address', 'location'], '')).strip() or None,
                'registration_date': reg_date,
                'metadata': {k: v for k, v in company.items() 
                           if k not in ('business_id', 'id', 'company_id', 'identifier',
                                      'name', 'company_name', 'business_name', 'title',
                                      'industry', 'sector', 'business_type',
                                      'city', 'municipality', 'location',
                                      'company_type', 'type', 'legal_form',
                                      'address', 'street_address',
                                      'registration_date', 'founding_date', 'established', 'date_created')}
            }
            
            return processed
            
        except Exception as e:
            logger.warning(f"Error processing company: {e}")
            return None

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Process large JSON file into database')
    parser.add_argument('file', help='Path to JSON file to process')
    parser.add_argument('--db', default='company_data.db', help='SQLite database path')
    parser.add_argument('--batch-size', type=int, default=1000, 
                       help='Number of records to process in each batch')
    parser.add_argument('--postgres', action='store_true', 
                       help='Use PostgreSQL instead of SQLite')
    
    args = parser.parse_args()
    
    try:
        # Initialize database
        db = DatabaseManager(db_path=args.db, use_sqlite=not args.postgres)
        
        # Process file
        processor = JSONProcessor(args.file, batch_size=args.batch_size)
        total_inserted = 0
        total_errors = 0
        
        # Process in batches
        for batch in tqdm(processor.process_file(), desc="Processing batches"):
            inserted, errors = db.insert_companies_batch(batch)
            total_inserted += inserted
            total_errors += errors
        
        logger.info(f"Processing complete. Inserted: {total_inserted}, Errors: {total_errors}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        if 'db' in locals():
            db.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
