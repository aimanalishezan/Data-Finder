#!/usr/bin/env python3
"""
Startup script for the Company Data Finder backend.
This script initializes the database and starts the FastAPI server.
"""

import os
import sys
import subprocess
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are installed"""
    try:
        import fastapi
        import uvicorn
        import sqlite3
        import pandas
        print("✅ All dependencies are available")
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please install dependencies with: pip install -r requirements.txt")
        return False

def initialize_database():
    """Initialize the database with sample data"""
    print("🔄 Initializing database...")
    try:
        from init_db import init_database
        init_database()
        return True
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False

def start_server():
    """Start the FastAPI server"""
    print("🚀 Starting FastAPI server...")
    try:
        import uvicorn
        from main import app
        
        print("📡 Server will be available at: http://localhost:8000")
        print("📖 API documentation at: http://localhost:8000/docs")
        print("🛑 Press Ctrl+C to stop the server")
        
        uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=8000,
            reload=True,
            log_level="info"
        )
    except Exception as e:
        print(f"❌ Server startup failed: {e}")
        return False

def main():
    """Main startup function"""
    print("🏁 Starting Company Data Finder Backend...")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Initialize database
    if not initialize_database():
        sys.exit(1)
    
    # Start server
    start_server()

if __name__ == "__main__":
    main()
