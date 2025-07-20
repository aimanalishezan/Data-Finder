from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import List, Optional, Dict, Any
from datetime import date
import os
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
import pandas as pd
from pathlib import Path
import tempfile

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./company_data.db")
USE_SQLITE = DATABASE_URL.startswith("sqlite")

app = FastAPI(title="Company Data API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
def get_db_connection():
    if USE_SQLITE:
        conn = sqlite3.connect("company_data.db")
        conn.row_factory = sqlite3.Row
    else:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn

# Pydantic models
class CompanyBase(BaseModel):
    business_id: str
    name: str
    industry: Optional[str] = None
    city: Optional[str] = None
    company_type: Optional[str] = None
    address: Optional[str] = None
    registration_date: Optional[date] = None

class CompanyCreate(CompanyBase):
    pass

class Company(CompanyBase):
    id: int
    created_at: Optional[date] = None
    updated_at: Optional[date] = None

    class Config:
        from_attributes = True

# API Endpoints
@app.get("/")
async def root():
    return {"message": "Company Data API is running"}

@app.get("/companies", response_model=Dict[str, Any])
async def get_companies(
    skip: int = 0,
    limit: int = 100,
    industry: Optional[str] = None,
    city: Optional[str] = None,
    company_type: Optional[str] = None,
    min_date: Optional[date] = None,
    max_date: Optional[date] = None,
    search: Optional[str] = None
):
    """
    Retrieve companies with optional filtering and pagination.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build the query
    query = "SELECT * FROM companies WHERE 1=1"
    params = []
    
    if industry:
        query += " AND industry ILIKE %s" if not USE_SQLITE else " AND industry LIKE ?"
        params.append(f"%{industry}%")
    
    if city:
        query += " AND city ILIKE %s" if not USE_SQLITE else " AND city LIKE ?"
        params.append(f"%{city}%")
    
    if company_type:
        query += " AND company_type ILIKE %s" if not USE_SQLITE else " AND company_type LIKE ?"
        params.append(f"%{company_type}%")
    
    if min_date:
        query += " AND registration_date >= %s" if not USE_SQLITE else " AND registration_date >= ?"
        params.append(min_date)
    
    if max_date:
        query += " AND registration_date <= %s" if not USE_SQLITE else " AND registration_date <= ?"
        params.append(max_date)
    
    if search:
        search_query = " AND (name ILIKE %s OR business_id ILIKE %s OR address ILIKE %s)" if not USE_SQLITE else " AND (name LIKE ? OR business_id LIKE ? OR address LIKE ?)"
        query += search_query
        search_param = f"%{search}%"
        params.extend([search_param, search_param, search_param])
    
    # Count total records
    count_query = query.replace("SELECT *", "SELECT COUNT(*)")
    cursor.execute(count_query, params)
    total = cursor.fetchone()[0] if USE_SQLITE else cursor.fetchone()['count']
    
    # Add pagination
    query += " ORDER BY id LIMIT %s OFFSET %s" if not USE_SQLITE else " ORDER BY id LIMIT ? OFFSET ?"
    params.extend([limit, skip])
    
    cursor.execute(query, params)
    companies = cursor.fetchall()
    
    # Convert to list of dictionaries
    if USE_SQLITE:
        companies = [dict(row) for row in companies]
    else:
        companies = [dict(row) for row in companies]
    
    conn.close()
    
    return {
        "data": companies,
        "total": total,
        "skip": skip,
        "limit": limit
    }

@app.get("/export")
async def export_companies(
    industry: Optional[str] = None,
    city: Optional[str] = None,
    company_type: Optional[str] = None,
    min_date: Optional[date] = None,
    max_date: Optional[date] = None,
    search: Optional[str] = None
):
    """
    Export companies to Excel with optional filtering.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build the query (same as get_companies but without pagination)
    query = "SELECT * FROM companies WHERE 1=1"
    params = []
    
    if industry:
        query += " AND industry ILIKE %s" if not USE_SQLITE else " AND industry LIKE ?"
        params.append(f"%{industry}%")
    
    if city:
        query += " AND city ILIKE %s" if not USE_SQLITE else " AND city LIKE ?"
        params.append(f"%{city}%")
    
    if company_type:
        query += " AND company_type ILIKE %s" if not USE_SQLITE else " AND company_type LIKE ?"
        params.append(f"%{company_type}%")
    
    if min_date:
        query += " AND registration_date >= %s" if not USE_SQLITE else " AND registration_date >= ?"
        params.append(min_date)
    
    if max_date:
        query += " AND registration_date <= %s" if not USE_SQLITE else " AND registration_date <= ?"
        params.append(max_date)
    
    if search:
        search_query = " AND (name ILIKE %s OR business_id ILIKE %s OR address ILIKE %s)" if not USE_SQLITE else " AND (name LIKE ? OR business_id LIKE ? OR address LIKE ?)"
        query += search_query
        search_param = f"%{search}%"
        params.extend([search_param, search_param, search_param])
    
    query += " ORDER BY id"
    
    cursor.execute(query, params)
    companies = cursor.fetchall()
    
    # Convert to DataFrame
    if USE_SQLITE:
        df = pd.DataFrame([dict(row) for row in companies])
    else:
        df = pd.DataFrame([dict(row) for row in companies])
    
    conn.close()
    
    if df.empty:
        raise HTTPException(status_code=404, detail="No companies found")
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
        df.to_excel(tmp_file.name, index=False)
        tmp_file_path = tmp_file.name
    
    return FileResponse(
        path=tmp_file_path,
        filename="companies_export.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# Vercel handler
handler = app
