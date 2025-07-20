#!/usr/bin/env python3
"""
Finnish Company Data Import Script
This script imports Finnish company data from the specific JSON structure.
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
    
    return None

def get_company_name(names_list):
    """Extract the primary company name from names list"""
    if not names_list or not isinstance(names_list, list):
        return None
    
    # Look for active names (no endDate or endDate is None)
    active_names = [name for name in names_list if name.get('endDate') is None]
    
    if active_names:
        # Prefer type '1' (primary name)
        primary_names = [name for name in active_names if name.get('type') == '1']
        if primary_names:
            return primary_names[0].get('name')
        else:
            return active_names[0].get('name')
    
    # If no active names, get the most recent one
    if names_list:
        return names_list[0].get('name')
    
    return None

def get_address_info(addresses_list):
    """Extract address information from addresses list"""
    if not addresses_list or not isinstance(addresses_list, list):
        return None, None, None
    
    # Get the first address (usually the primary one)
    address = addresses_list[0]
    
    street = address.get('street', '')
    building_number = address.get('buildingNumber', '')
    post_code = address.get('postCode', '')
    
    # Build full address
    full_address = f"{street} {building_number}".strip() if street else None
    
    # Get city from postOffices
    city = None
    post_offices = address.get('postOffices', [])
    if post_offices and isinstance(post_offices, list):
        city = post_offices[0].get('city')
    
    return full_address, city, post_code

def get_business_line(main_business_line):
    """Extract business line/industry from mainBusinessLine"""
    if not main_business_line or not isinstance(main_business_line, dict):
        return None
    
    descriptions = main_business_line.get('descriptions', [])
    if descriptions and isinstance(descriptions, list):
        # Prefer English description (languageCode '3'), then Finnish ('1')
        for desc in descriptions:
            if desc.get('languageCode') == '3':
                return desc.get('description')
        
        for desc in descriptions:
            if desc.get('languageCode') == '1':
                return desc.get('description')
        
        # If no preferred language, return first description
        if descriptions:
            return descriptions[0].get('description')
    
    return None

def get_company_type(company_forms):
    """Extract company type from companyForms"""
    if not company_forms or not isinstance(company_forms, list):
        return None
    
    # Get active company form (no endDate)
    active_forms = [form for form in company_forms if form.get('endDate') is None]
    
    if active_forms:
        descriptions = active_forms[0].get('descriptions', [])
        if descriptions and isinstance(descriptions, list):
            # Prefer English description
            for desc in descriptions:
                if desc.get('languageCode') == '3':
                    return desc.get('description')
            
            # Fallback to first description
            return descriptions[0].get('description')
    
    return None

def create_database_schema():
    """Create the database schema"""
    conn = sqlite3.connect("company_data.db")
    cursor = conn.cursor()
    
    # Drop existing table to start fresh
    cursor.execute("DROP TABLE IF EXISTS companies")
    
    # Create companies table
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
            website TEXT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database schema created successfully")

def import_finnish_data(json_file_path):
    """Import Finnish company data from JSON file"""
    
    if not os.path.exists(json_file_path):
        print(f"Error: JSON file not found at {json_file_path}")
        return False
    
    print(f"Loading Finnish company data from: {json_file_path}")
    
    try:
        # Load JSON data
        with open(json_file_path, 'r', encoding='utf-8') as file:
            companies_data = json.load(file)
        
        print(f"Loaded {len(companies_data)} companies from JSON file")
        
        # Connect to database
        conn = sqlite3.connect("company_data.db")
        cursor = conn.cursor()
        
        imported_count = 0
        skipped_count = 0
        
        for i, company in enumerate(companies_data):
            try:
                # Extract business ID
                business_id_obj = company.get('businessId', {})
                business_id = business_id_obj.get('value') if isinstance(business_id_obj, dict) else str(business_id_obj)
                
                if not business_id:
                    business_id = f"AUTO_{i+1}"
                
                # Extract company name
                name = get_company_name(company.get('names', []))
                if not name:
                    skipped_count += 1
                    continue
                
                # Extract other information
                industry = get_business_line(company.get('mainBusinessLine'))
                company_type = get_company_type(company.get('companyForms', []))
                
                # Extract address information
                address, city, postal_code = get_address_info(company.get('addresses', []))
                
                # Extract website
                website_obj = company.get('website', {})
                website = website_obj.get('url') if isinstance(website_obj, dict) else None
                
                # Extract registration date
                registration_date = parse_date(company.get('registrationDate'))
                
                # Extract status
                status = company.get('status')
                
                # Insert into database
                cursor.execute('''
                    INSERT OR REPLACE INTO companies (
                        business_id, name, industry, city, company_type, address,
                        registration_date, postal_code, website, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    business_id, name, industry, city, company_type, address,
                    registration_date, postal_code, website, status
                ))
                
                imported_count += 1
                
                # Progress indicator
                if imported_count % 1000 == 0:
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
    cursor.execute("SELECT name, city, industry, company_type FROM companies LIMIT 10")
    sample_companies = cursor.fetchall()
    
    # Get industry distribution
    cursor.execute("""
        SELECT industry, COUNT(*) as count 
        FROM companies 
        WHERE industry IS NOT NULL 
        GROUP BY industry 
        ORDER BY count DESC 
        LIMIT 10
    """)
    industries = cursor.fetchall()
    
    # Get city distribution
    cursor.execute("""
        SELECT city, COUNT(*) as count 
        FROM companies 
        WHERE city IS NOT NULL 
        GROUP BY city 
        ORDER BY count DESC 
        LIMIT 10
    """)
    cities = cursor.fetchall()
    
    conn.close()
    
    print(f"\nDatabase Verification:")
    print(f"Total companies: {total_count}")
    
    print(f"\nSample companies:")
    for company in sample_companies:
        print(f"  - {company[0]} ({company[1]}, {company[2]}, {company[3]})")
    
    print(f"\nTop industries:")
    for industry, count in industries:
        print(f"  - {industry}: {count} companies")
    
    print(f"\nTop cities:")
    for city, count in cities:
        print(f"  - {city}: {count} companies")

def main():
    """Main function"""
    print("Starting Finnish Company Data Import...")
    print("=" * 50)
    
    # JSON file path
    json_file_path = "D:\\Jani_Finland\\Data Finder\\data_20250718.json"
    
    # Create database schema
    create_database_schema()
    
    # Import data
    success = import_finnish_data(json_file_path)
    
    if success:
        # Verify import
        verify_import()
        print(f"\nAll Finnish company data imported successfully!")
        print(f"Your backend server will now show all the imported companies!")
        print(f"Restart your backend server to see the new data.")
    else:
        print(f"\nImport failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
