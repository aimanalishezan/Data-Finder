import sqlite3
import os
from datetime import date, datetime

def init_database():
    """Initialize SQLite database with sample data"""
    db_path = "company_data.db"
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create companies table
    cursor.execute('''
        CREATE TABLE companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            industry TEXT,
            city TEXT,
            company_type TEXT,
            address TEXT,
            registration_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Insert sample data
    sample_companies = [
        ("FI12345678", "Tech Solutions Oy", "Technology", "Helsinki", "Osakeyhtiö", "Mannerheimintie 1, Helsinki", "2020-01-15"),
        ("FI87654321", "Nordic Consulting Ab", "Consulting", "Stockholm", "Aktiebolag", "Kungsgatan 10, Stockholm", "2019-03-22"),
        ("FI11223344", "Green Energy Ltd", "Energy", "Tampere", "Osakeyhtiö", "Hämeenkatu 5, Tampere", "2021-06-10"),
        ("FI55667788", "Food Innovations Oy", "Food & Beverage", "Turku", "Osakeyhtiö", "Aurakatu 3, Turku", "2018-11-30"),
        ("FI99887766", "Digital Marketing Pro", "Marketing", "Oulu", "Osakeyhtiö", "Kirkkokatu 8, Oulu", "2022-02-14"),
        ("FI44556677", "Construction Masters", "Construction", "Espoo", "Osakeyhtiö", "Tapiontori 1, Espoo", "2017-09-05"),
        ("FI33445566", "Healthcare Solutions", "Healthcare", "Vantaa", "Osakeyhtiö", "Tikkurilantie 10, Vantaa", "2020-08-18"),
        ("FI22334455", "Logistics Express", "Logistics", "Lahti", "Osakeyhtiö", "Vesijärvenkatu 2, Lahti", "2019-12-03"),
        ("FI66778899", "Fashion Forward Oy", "Fashion", "Jyväskylä", "Osakeyhtiö", "Kauppakatu 15, Jyväskylä", "2021-04-27"),
        ("FI77889900", "Auto Services Ltd", "Automotive", "Kuopio", "Osakeyhtiö", "Puijonkatu 12, Kuopio", "2018-07-11"),
    ]
    
    for company in sample_companies:
        cursor.execute('''
            INSERT INTO companies (business_id, name, industry, city, company_type, address, registration_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', company)
    
    conn.commit()
    conn.close()
    
    print(f"Database initialized successfully with {len(sample_companies)} sample companies!")
    print(f"Database file: {os.path.abspath(db_path)}")

if __name__ == "__main__":
    init_database()
