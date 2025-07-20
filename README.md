# Company Data Finder

A system for processing, storing, and querying large company datasets with a web interface for searching and exporting data.

## Features

- Process large JSON files (1.3GB+)
- Store data in PostgreSQL/SQLite database
- Web-based search interface with filters
- Export search results to Excel
- (Optional) XBRL financial data integration
- (Optional) Contact information scraping

## Project Structure

```
.
├── backend/               # Backend API (FastAPI/Python)
│   ├── api/
│   │   └── index.py      # Main API endpoints
│   ├── requirements.txt   # Python dependencies
│   └── company_data.db   # SQLite database
├── frontend/              # Frontend React TypeScript application
│   ├── src/
│   │   ├── components/   # React components
│   │   ├── contexts/     # React contexts (Auth, Theme)
│   │   ├── hooks/        # Custom React hooks
│   │   └── utils/        # Utility functions
│   ├── package.json
│   └── tailwind.config.js
├── .vscode/              # VS Code settings
├── vercel.json           # Vercel deployment config
├── .gitignore
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.8+
- Node.js 16+
- PostgreSQL (or SQLite for development)
- npm or yarn

### Installation

1. Clone the repository
2. Set up backend:
   ```bash
   cd backend
   npm install
   cp .env.example .env
   # Edit .env with your database credentials
   ```
3. Set up frontend:
   ```bash
   cd ../frontend
   npm install
   ```
4. Run the development servers:
   ```bash
   # Backend (from backend directory)
   python start_server.py
   
   # Frontend (from frontend directory)
   npm run dev
   ```

## Deployment

### Vercel Deployment

This project is configured for automatic deployment on Vercel with both frontend and backend:

1. **Push to GitHub**: The project includes proper `.gitignore` and configuration files
2. **Connect to Vercel**: Import your GitHub repository to Vercel
3. **Automatic Deployment**: Vercel will automatically:
   - Build the React frontend
   - Deploy the FastAPI backend as serverless functions
   - Handle routing between frontend and API

### Environment Variables

For production deployment, set these environment variables in Vercel:
- `DATABASE_URL`: Your production database URL (if using PostgreSQL)
- Any other API keys or secrets your application needs

### Local Development vs Production

- **Local**: Uses SQLite database (`company_data.db`)
- **Production**: Can use PostgreSQL or other cloud databases
- **CORS**: Configured to allow all origins (adjust for production security)
   - Backend: `npm run dev` (from /backend)
   - Frontend: `npm start` (from /frontend)

## Data Processing

Place your JSON file in the `data/` directory and run:

```bash
python scripts/process_data.py --input data/company_data.json --db postgresql://user:password@localhost/companydb
```

## License

MIT
