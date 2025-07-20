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
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
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
        orm_mode = True

# API Endpoints
@app.get("/")
async def root():
    return {"message": "Company Data API - Use /docs for API documentation"}

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
    
    # Base query
    query = """
    SELECT * FROM companies
    WHERE 1=1
    """
    params = []
    
    # Add filters
    if industry:
        query += " AND LOWER(industry) LIKE LOWER(%s)" if not USE_SQLITE else " AND LOWER(industry) LIKE LOWER(?)"
        params.append(f"%{industry}%")
    
    if city:
        query += " AND LOWER(city) LIKE LOWER(%s)" if not USE_SQLITE else " AND LOWER(city) LIKE LOWER(?)"
        params.append(f"%{city}%")
    
    if company_type:
        query += " AND company_type = %s" if not USE_SQLITE else " AND company_type = ?"
        params.append(company_type)
    
    if min_date:
        query += " AND registration_date >= %s" if not USE_SQLITE else " AND registration_date >= ?"
        params.append(min_date)
    
    if max_date:
        query += " AND registration_date <= %s" if not USE_SQLITE else " AND registration_date <= ?"
        params.append(max_date)
    
    if search:
        search_condition = """
        AND (
            LOWER(name) LIKE LOWER(%s) OR
            LOWER(business_id) LIKE LOWER(%s) OR
            LOWER(address) LIKE LOWER(%s)
        )
        """ if not USE_SQLITE else """
        AND (
            LOWER(name) LIKE LOWER(?) OR
            LOWER(business_id) LIKE LOWER(?) OR
            LOWER(address) LIKE LOWER(?)
        )
        """
        query += search_condition
        params.extend([f"%{search}%"] * 3)
    
    # Add ordering and pagination
    query += " ORDER BY name"
    query += " LIMIT %s OFFSET %s" if not USE_SQLITE else " LIMIT ? OFFSET ?"
    params.extend([limit, skip])
    
    # Execute query
    cursor.execute(query, params)
    
    if USE_SQLITE:
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    else:
        results = cursor.fetchall()
    
    # Get total count for pagination
    count_query = "SELECT COUNT(*) as count FROM (" + query.replace("LIMIT %s OFFSET %s", "").replace("LIMIT ? OFFSET ?", "") + ") as subquery"
    cursor.execute(count_query, params[:-2] if len(params) > 2 else [])
    total_count = cursor.fetchone()['count'] if not USE_SQLITE else cursor.fetchone()[0]
    
    conn.close()
    
    return {
        "data": results,
        "total": total_count,
        "skip": skip,
        "limit": limit
    }

@app.get("/companies/{company_id}")
def get_company_by_id(company_id: int):
    """
    Retrieve a single company by ID.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Query for specific company
        query = "SELECT * FROM companies WHERE id = ?" if USE_SQLITE else "SELECT * FROM companies WHERE id = %s"
        cursor.execute(query, (company_id,))
        
        company = cursor.fetchone()
        
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
        
        # Convert to dictionary
        columns = [desc[0] for desc in cursor.description]
        company_dict = dict(zip(columns, company))
        
        return company_dict
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

@app.get("/export/", response_class=FileResponse)
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
    # Get all matching companies (without pagination)
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT * FROM companies
    WHERE 1=1
    """
    params = []
    
    # Add filters (same as get_companies)
    if industry:
        query += " AND LOWER(industry) LIKE LOWER(%s)" if not USE_SQLITE else " AND LOWER(industry) LIKE LOWER(?)"
        params.append(f"%{industry}%")
    
    if city:
        query += " AND LOWER(city) LIKE LOWER(%s)" if not USE_SQLITE else " AND LOWER(city) LIKE LOWER(?)"
        params.append(f"%{city}%")
    
    if company_type:
        query += " AND company_type = %s" if not USE_SQLITE else " AND company_type = ?"
        params.append(company_type)
    
    if min_date:
        query += " AND registration_date >= %s" if not USE_SQLITE else " AND registration_date >= ?"
        params.append(min_date)
    
    if max_date:
        query += " AND registration_date <= %s" if not USE_SQLITE else " AND registration_date <= ?"
        params.append(max_date)
    
    if search:
        search_condition = """
        AND (
            LOWER(name) LIKE LOWER(%s) OR
            LOWER(business_id) LIKE LOWER(%s) OR
            LOWER(address) LIKE LOWER(%s)
        )
        """ if not USE_SQLITE else """
        AND (
            LOWER(name) LIKE LOWER(?) OR
            LOWER(business_id) LIKE LOWER(?) OR
            LOWER(address) LIKE LOWER(?)
        )
        """
        query += search_condition
        params.extend([f"%{search}%"] * 3)
    
    query += " ORDER BY name"
    
    # Execute query
    cursor.execute(query, params)
    
    if USE_SQLITE:
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    else:
        results = cursor.fetchall()
    
    conn.close()
    
    # Convert to DataFrame and then to Excel
    if not results:
        raise HTTPException(status_code=404, detail="No data found for the given filters")
    
    df = pd.DataFrame(results)
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
        excel_file = tmp.name
        df.to_excel(excel_file, index=False, engine='openpyxl')
    
    # Return the file as a response
    return FileResponse(
        excel_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"company_export_{date.today().isoformat()}.xlsx"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
