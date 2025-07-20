# Data Processing Scripts

This directory contains scripts for processing large JSON files and importing them into a database.

## Requirements

- Python 3.8+
- Required packages (install with `pip install -r requirements.txt`):
  - ijson
  - tqdm
  - psycopg2-binary (for PostgreSQL support)
  - orjson (for faster JSON processing)
  - chardet (for file encoding detection)

## Scripts

### process_large_json.py

Processes large JSON files and imports them into a SQLite or PostgreSQL database.

#### Features

- Efficiently processes large JSON files using streaming
- Supports both gzipped and uncompressed JSON files
- Handles malformed data gracefully
- Provides progress tracking
- Supports both SQLite and PostgreSQL
- Creates appropriate indexes for better query performance

#### Usage

```bash
# Basic usage with SQLite
python process_large_json.py data.json

# Specify SQLite database path
python process_large_json.py data.json --db my_database.db

# Use PostgreSQL instead of SQLite
python process_large_json.py data.json --postgres

# Adjust batch size (default: 1000)
python process_large_json.py data.json --batch-size 5000

# Process gzipped JSON file
python process_large_json.py data.json.gz
```

#### Database Schema

The script creates a `companies` table with the following structure:

```sql
CREATE TABLE companies (
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
);
```

#### Performance Tips

1. For large files, use a larger batch size (e.g., 5000-10000) for better performance
2. Ensure you have enough disk space for the database (at least 2-3x the size of the JSON file)
3. For PostgreSQL, consider tuning the database configuration for bulk imports
4. The script creates indexes after importing all data for better performance

#### Error Handling

- The script logs errors to `data_processing.log`
- Invalid records are skipped and counted in the error total
- Database transactions are used to ensure data consistency

## Example

```bash
# Process a large JSON file into SQLite
python process_large_json.py /path/to/large_data.json --db /path/to/output.db

# Process the same file with PostgreSQL
export PGPASSWORD=your_password
python process_large_json.py /path/to/large_data.json --postgres
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
