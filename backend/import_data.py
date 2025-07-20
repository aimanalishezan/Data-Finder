#!/usr/bin/env python3
"""
Data Import Script for Company Data Finder
This script imports all company data from the JSON file into the database.
"""

import json
import sqlite3
import os
from datetime import datetime, date
from typing import Dict, Any, List
import sys

def parse_date(date_str):
    """Parse date string in various formats"""
    if not date_str or date_str == "null" or date_str == "":
        return None
    
    # Try different date formats
    date_formats = [
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f"
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(str(date_str), fmt)
            return parsed_date.date()
        except ValueError:
            continue
    
    # If no format works, return None
    print(f"Warning: Could not parse date: {date_str}")
    return None

def clean_text(text):
    """Clean and normalize text data"""
    if text is None or text == "null":
        return None
    return str(text).strip() if str(text).strip() else None

def create_database_schema():
    """Create the database schema if it doesn't exist"""
    conn = sqlite3.connect("company_data.db")
    cursor = conn.cursor()
    
    # Drop existing table to start fresh
    cursor.execute("DROP TABLE IF EXISTS companies")
    
    # Create companies table with all possible fields
    cursor.execute('''
        CREATE TABLE companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT UNIQUE,
            name TEXT NOT NULL,
            industry TEXT,
            city TEXT,
            company_type TEXT,
            address TEXT,
            registration_date DATE,
            postal_code TEXT,
            phone TEXT,
            email TEXT,
            website TEXT,
            employees INTEGER,
            revenue REAL,
            status TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database schema created successfully")

def import_json_data(json_file_path):
    """Import data from JSON file into the database"""
    
    if not os.path.exists(json_file_path):
        print(f"Error: JSON file not found at {json_file_path}")
        return False
    
    print(f"Loading data from: {json_file_path}")
    
    try:
        # Load JSON data
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        print(f"Loaded JSON data successfully")
        
        # Handle different JSON structures
        companies_data = []
        
        if isinstance(data, list):
            companies_data = data
        elif isinstance(data, dict):
            # Try common keys for company data
            possible_keys = ['companies', 'data', 'results', 'items', 'records']
            for key in possible_keys:
                if key in data:
                    companies_data = data[key]
                    break
            
            # If no common key found, assume the dict itself contains company data
            if not companies_data and data:
                companies_data = [data]
        
        if not companies_data:
            print("Error: No company data found in JSON file")
            return False
        
        print(f"Found {len(companies_data)} companies to import")
        
        # Connect to database
        conn = sqlite3.connect("company_data.db")
        cursor = conn.cursor()
        
        imported_count = 0
        skipped_count = 0
        
        for i, company in enumerate(companies_data):
            try:
                # Extract and clean company data
                business_id = clean_text(company.get('business_id') or company.get('id') or company.get('company_id') or f"AUTO_{i+1}")
                name = clean_text(company.get('name') or company.get('company_name') or company.get('title'))
                
                if not name:
                    print(f"Warning: Skipping company {i+1} - no name found")
                    skipped_count += 1
                    continue
                
                # Extract other fields with fallbacks
                industry = clean_text(company.get('industry') or company.get('sector') or company.get('business_type'))
                city = clean_text(company.get('city') or company.get('location') or company.get('municipality'))
                company_type = clean_text(company.get('company_type') or company.get('type') or company.get('legal_form'))
                address = clean_text(company.get('address') or company.get('street_address') or company.get('full_address'))
                postal_code = clean_text(company.get('postal_code') or company.get('zip_code') or company.get('postcode'))
                phone = clean_text(company.get('phone') or company.get('telephone') or company.get('phone_number'))
                email = clean_text(company.get('email') or company.get('email_address'))
                website = clean_text(company.get('website') or company.get('url') or company.get('homepage'))
                status = clean_text(company.get('status') or company.get('company_status'))
                description = clean_text(company.get('description') or company.get('business_description'))
                
                # Parse numeric fields
                employees = None
                if company.get('employees') or company.get('employee_count'):
                    try:
                        employees = int(company.get('employees') or company.get('employee_count'))
                    except (ValueError, TypeError):
                        pass
                
                revenue = None
                if company.get('revenue') or company.get('turnover'):
                    try:
                        revenue = float(company.get('revenue') or company.get('turnover'))
                    except (ValueError, TypeError):
                        pass
                
                # Parse registration date
                registration_date = parse_date(
                    company.get('registration_date') or 
                    company.get('founded') or 
                    company.get('established') or 
                    company.get('incorporation_date')
                )
                
                # Insert into database
                cursor.execute('''
                    INSERT OR REPLACE INTO companies (
                        business_id, name, industry, city, company_type, address,
                        registration_date, postal_code, phone, email, website,
                        employees, revenue, status, description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    business_id, name, industry, city, company_type, address,
                    registration_date, postal_code, phone, email, website,
                    employees, revenue, status, description
                ))
                
                imported_count += 1
                
                # Progress indicator
                if imported_count % 100 == 0:
                    print(f"Imported {imported_count} companies...")
                
            except Exception as e:
                print(f"Warning: Error importing company {i+1}: {e}")
                skipped_count += 1
                continue
        
        # Commit all changes
        conn.commit()
        conn.close()
        
        print(f"\nImport completed successfully!")
        print(f"Imported: {imported_count} companies")
        print(f"Skipped: {skipped_count} companies")
        print(f"Total in database: {imported_count}")
        
        return True
        
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON file - {e}")
        return False
    except Exception as e:
        print(f"Error: Failed to import data - {e}")
        return False

def verify_import():
    """Verify the imported data"""
    conn = sqlite3.connect("company_data.db")
    cursor = conn.cursor()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM companies")
    total_count = cursor.fetchone()[0]
    
    # Get sample data
    cursor.execute("SELECT name, city, industry FROM companies LIMIT 5")
    sample_companies = cursor.fetchall()
    
    # Get industry distribution
    cursor.execute("SELECT industry, COUNT(*) as count FROM companies WHERE industry IS NOT NULL GROUP BY industry ORDER BY count DESC LIMIT 10")
    industries = cursor.fetchall()
    
    conn.close()
    
    print(f"\nDatabase Verification:")
    print(f"Total companies: {total_count}")
    
    print(f"\nSample companies:")
    for company in sample_companies:
        print(f"  - {company[0]} ({company[1]}, {company[2]})")
    
    print(f"\nTop industries:")
    for industry, count in industries:
        print(f"  - {industry}: {count} companies")

def main():
    """Main function"""
    print("Starting Company Data Import...")
    print("=" * 50)
    
    # JSON file path
    json_file_path = "D:\\Jani_Finland\\Data Finder\\data_20250718.json"
    
    # Create database schema
    create_database_schema()
    
    # Import data
    success = import_json_data(json_file_path)
    
    if success:
        # Verify import
        verify_import()
        print(f"\nAll data imported successfully!")
        print(f"Your backend server will now show all the imported companies!")
    else:
        print(f"\nImport failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
