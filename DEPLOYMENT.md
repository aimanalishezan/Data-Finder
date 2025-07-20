# 🚀 Vercel Deployment Guide

This guide will help you deploy your Company Data Finder application to Vercel with both frontend and backend.

## 📁 Project Structure
```
Data Finder/
├── frontend/           # React + TypeScript frontend
├── backend/           # FastAPI backend
├── vercel.json        # Root Vercel configuration
├── package.json       # Root package.json for deployment
└── DEPLOYMENT.md      # This guide
```

## 🔧 Pre-deployment Setup

### 1. Install Dependencies
```bash
# Install frontend dependencies
cd frontend
npm install

# Install backend dependencies (if testing locally)
cd ../backend
pip install -r requirements.txt
```

### 2. Environment Variables
Update your environment variables in the Vercel dashboard:

**Frontend Environment Variables:**
- `VITE_FIREBASE_API_KEY`: Your Firebase API key
- `VITE_FIREBASE_AUTH_DOMAIN`: Your Firebase auth domain
- `VITE_FIREBASE_PROJECT_ID`: Your Firebase project ID
- `VITE_FIREBASE_STORAGE_BUCKET`: Your Firebase storage bucket
- `VITE_FIREBASE_MESSAGING_SENDER_ID`: Your Firebase messaging sender ID
- `VITE_FIREBASE_APP_ID`: Your Firebase app ID
- `VITE_FIREBASE_MEASUREMENT_ID`: Your Firebase measurement ID
- `VITE_API_URL`: Your backend API URL (will be auto-configured for Vercel)

**Backend Environment Variables:**
- `DATABASE_URL`: Your database connection string (PostgreSQL recommended for production)

## 🚀 Deployment Steps

### Option 1: Deploy Everything Together (Recommended)

1. **Connect to Vercel:**
   ```bash
   # Install Vercel CLI
   npm i -g vercel
   
   # Login to Vercel
   vercel login
   
   # Deploy from root directory
   vercel
   ```

2. **Configure Project:**
   - Choose "Link to existing project" or create new
   - Set root directory as the project root
   - Vercel will automatically detect the configuration

### Option 2: Deploy Frontend and Backend Separately

#### Deploy Backend First:
```bash
cd backend
vercel
```

#### Deploy Frontend:
```bash
cd frontend
# Update VITE_API_URL in .env.production with your backend URL
vercel
```

## 🔗 API Integration

Your frontend is already configured to connect to the backend through:
- `/api/companies` - Get companies with filtering and pagination
- `/api/export` - Export companies to Excel

The API endpoints match your FastAPI backend structure.

## 🗄️ Database Setup

For production, you'll need a PostgreSQL database:

1. **Recommended Services:**
   - Neon (free tier available)
   - Supabase (free tier available)
   - Railway (free tier available)
   - PlanetScale (MySQL alternative)

2. **Update DATABASE_URL:**
   ```
   postgresql://username:password@host:port/database
   ```

## 🔒 Security Considerations

1. **Environment Variables:**
   - Never commit `.env` files to git
   - Use Vercel's environment variable dashboard
   - Different variables for development/production

2. **CORS Configuration:**
   - Update CORS origins in production
   - Restrict to your domain only

3. **Database Security:**
   - Use connection pooling
   - Enable SSL connections
   - Regular backups

## 🧪 Testing Your Deployment

1. **Frontend Tests:**
   - Login functionality
   - Company data loading
   - Filtering and search
   - Export functionality

2. **Backend Tests:**
   - API endpoints responding
   - Database connections working
   - CORS headers correct

## 🐛 Troubleshooting

### Common Issues:

1. **Build Failures:**
   - Check Node.js version compatibility
   - Verify all dependencies are installed
   - Check for TypeScript errors

2. **API Connection Issues:**
   - Verify VITE_API_URL is correct
   - Check CORS configuration
   - Ensure backend is deployed and running

3. **Database Connection:**
   - Verify DATABASE_URL format
   - Check database permissions
   - Ensure database is accessible from Vercel

### Debug Commands:
```bash
# Check build locally
npm run build

# Test backend locally
cd backend && python main.py

# Check Vercel logs
vercel logs
```

## 📊 Monitoring

1. **Vercel Analytics:**
   - Enable in Vercel dashboard
   - Monitor performance and usage

2. **Error Tracking:**
   - Consider adding Sentry or similar
   - Monitor API errors and performance

## 🔄 Updates and Maintenance

1. **Automatic Deployments:**
   - Connect GitHub repository
   - Auto-deploy on push to main branch

2. **Environment Management:**
   - Use different environments (dev/staging/prod)
   - Test changes in staging first

---

## 🎉 You're Ready!

Your Company Data Finder is now ready for Vercel deployment with:
- ✅ React TypeScript frontend
- ✅ FastAPI Python backend  
- ✅ Firebase authentication
- ✅ Company data management
- ✅ Excel export functionality
- ✅ Responsive design with Tailwind CSS

Deploy with confidence! 🚀
