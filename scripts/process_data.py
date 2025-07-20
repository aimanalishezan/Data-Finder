import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
import sqlite3
from typing import Dict, Any, Iterator, Optional

import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm

# Default database configuration
DEFAULT_DB_CONFIG = {
    'dbname': 'company_finder',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

class DatabaseManager:
    """Handles database connections and operations."""
    
    def __init__(self, db_url: Optional[str] = None, use_sqlite: bool = False):
        self.use_sqlite = use_sqlite
        if use_sqlite:
            self.conn = sqlite3.connect('company_data.db')
            self.cursor = self.conn.cursor()
        else:
            if db_url:
                # Parse connection string: postgresql://user:password@host:port/dbname
                from urllib.parse import urlparse
                result = urlparse(db_url)
                db_config = {
                    'dbname': result.path[1:],  # Remove leading '/'
                    'user': result.username,
                    'password': result.password,
                    'host': result.hostname,
                    'port': result.port or '5432'
                }
            else:
                db_config = DEFAULT_DB_CONFIG
                
            self.conn = psycopg2.connect(**db_config)
            self.cursor = self.conn.cursor()
        
        self._create_tables()
    
    def _create_tables(self):
        """Create necessary tables if they don't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            business_id VARCHAR(50) UNIQUE NOT NULL,
            name TEXT NOT NULL,
            industry TEXT,
            city TEXT,
            company_type TEXT,
            address TEXT,
            registration_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        if self.use_sqlite:
            # SQLite doesn't support SERIAL, uses INTEGER PRIMARY KEY AUTOINCREMENT
            create_table_sql = create_table_sql.replace(
                'SERIAL PRIMARY KEY',
                'INTEGER PRIMARY KEY AUTOINCREMENT'
            )
            create_table_sql = create_table_sql.replace(
                'VARCHAR', 'TEXT'
            )
        
        self.cursor.execute(create_table_sql)
        self.conn.commit()
    
    def insert_companies_batch(self, companies: list[Dict[str, Any]]):
        """Insert multiple companies in a batch."""
        if not companies:
            return
            
        columns = companies[0].keys()
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Build the SQL query
        if self.use_sqlite:
            # SQLite uses ? as placeholder
            placeholders = ', '.join(['?'] * len(columns))
            query = f"""
            INSERT OR IGNORE INTO companies ({columns_str}) 
            VALUES ({placeholders})
            """
        else:
            # PostgreSQL uses %s as placeholder
            query = f"""
            INSERT INTO companies ({columns_str}) 
            VALUES ({placeholders})
            ON CONFLICT (business_id) DO NOTHING
            """
        
        # Prepare data for batch insert
        values = [tuple(company[col] for col in columns) for company in companies]
        
        # Execute batch insert
        try:
            if self.use_sqlite:
                self.cursor.executemany(query, values)
            else:
                execute_batch(self.cursor, query, values, page_size=100)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting batch: {e}")
            raise
    
    def close(self):
        """Close the database connection."""
        self.conn.close()


def process_json_file(file_path: str, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
    """
    Read and process a large JSON file line by line or as a whole.
    Yields batches of processed company records.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    batch = []
    
    try:
        # Try to read as JSON Lines (one JSON object per line)
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in tqdm(f, desc="Processing JSON file"):
                try:
                    record = json.loads(line.strip())
                    # Transform the record to match our schema
                    company = {
                        'business_id': record.get('businessId', ''),
                        'name': record.get('name', ''),
                        'industry': record.get('industry', ''),
                        'city': record.get('location', {}).get('city', ''),
                        'company_type': record.get('companyType', ''),
                        'address': record.get('address', ''),
                        'registration_date': record.get('registrationDate', '')
                    }
                    batch.append(company)
                    
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                        
                except json.JSONDecodeError:
                    continue
                    
    except UnicodeDecodeError:
        # If not JSON Lines, try as a single JSON array
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    for record in tqdm(data, desc="Processing JSON array"):
                        company = {
                            'business_id': record.get('businessId', ''),
                            'name': record.get('name', ''),
                            'industry': record.get('industry', ''),
                            'city': record.get('location', {}).get('city', ''),
                            'company_type': record.get('companyType', ''),
                            'address': record.get('address', ''),
                            'registration_date': record.get('registrationDate', '')
                        }
                        batch.append(company)
                        
                        if len(batch) >= batch_size:
                            yield batch
                            batch = []
        except Exception as e:
            print(f"Error processing JSON file: {e}")
            raise
    
    # Yield any remaining records
    if batch:
        yield batch


def main():
    parser = argparse.ArgumentParser(description='Process company data JSON file and load into database.')
    parser.add_argument('--input', type=str, required=True, help='Path to the input JSON file')
    parser.add_argument('--db', type=str, help='Database connection string (postgresql://user:pass@host:port/dbname)')
    parser.add_argument('--sqlite', action='store_true', help='Use SQLite instead of PostgreSQL')
    parser.add_argument('--batch-size', type=int, default=1000, help='Number of records to insert in a batch')
    
    args = parser.parse_args()
    
    print(f"Starting to process file: {args.input}")
    print(f"Using {'SQLite' if args.sqlite else 'PostgreSQL'} database")
    
    try:
        # Initialize database connection
        db = DatabaseManager(args.db, use_sqlite=args.sqlite)
        
        # Process the file and insert into database
        for batch in process_json_file(args.input, args.batch_size):
            db.insert_companies_batch(batch)
            print(f"Inserted {len(batch)} records")
        
        print("Data processing completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'db' in locals():
            db.close()


if __name__ == "__main__":
    main()
