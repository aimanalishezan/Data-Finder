/**
 * Application configuration
 * This file contains feature flags and other configurable settings
 */

// Feature flags - these can be toggled based on environment variables
const features = {
  // Enable/disable XBRL integration
  xbrlIntegration: import.meta.env.VITE_ENABLE_XBRL_INTEGRATION === 'true',
  
  // Enable/disable contact scraper
  contactScraper: import.meta.env.VITE_ENABLE_CONTACT_SCRAPER === 'true',
  
  // Enable/disable experimental features
  experimentalFeatures: {
    // Add experimental features here
    darkMode: true,
    advancedFilters: true,
    bulkActions: true,
  },
};

// API configuration
const api = {
  // Base URL for API requests
  baseUrl: import.meta.env.VITE_API_URL || 'http://localhost:8000',
  
  // Default request timeout in milliseconds
  timeout: 30000,
  
  // Default number of items per page
  itemsPerPage: 10,
  
  // Maximum number of items that can be exported at once
  maxExportItems: 10000,
};

// UI configuration
const ui = {
  // Default theme
  theme: 'light', // 'light' | 'dark' | 'system'
  
  // Table configuration
  table: {
    // Number of rows per page options
    rowsPerPageOptions: [10, 25, 50, 100],
    
    // Default sort field and order
    defaultSort: {
      field: 'name',
      order: 'asc' as const,
    },
  },
  
  // Date format (using date-fns format)
  dateFormat: 'MMM d, yyyy',
  
  // Date and time format
  dateTimeFormat: 'MMM d, yyyy h:mm a',
};

// Export configuration object
const config = {
  features,
  api,
  ui,
  
  // App metadata
  app: {
    name: 'Company Finder',
    version: '1.0.0',
    description: 'A web application for searching and managing company data',
  },
  
  // Get the full API URL for a given endpoint
  getApiUrl: (endpoint: string): string => {
    // Remove leading slash if present
    const normalizedEndpoint = endpoint.startsWith('/') ? endpoint.substring(1) : endpoint;
    return `${api.baseUrl}/${normalizedEndpoint}`;
  },
};

export default config;
