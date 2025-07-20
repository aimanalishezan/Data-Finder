# Company Finder Frontend

This is the frontend application for the Company Finder system, built with React, TypeScript, and Tailwind CSS. It provides a user interface for searching, filtering, and exporting company data.

## Features

- User authentication (login/logout)
- Search and filter companies by various criteria
- Sortable and paginated results table
- Export search results to Excel
- Responsive design for all screen sizes
- Modern UI with smooth animations

## Prerequisites

- Node.js 16+ and npm/yarn
- Backend API server (see the main README for setup instructions)

## Getting Started

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd frontend
   ```

2. **Install dependencies**
   ```bash
   npm install
   # or
   yarn install
   ```

3. **Set up environment variables**
   Copy the example environment file and update the values as needed:
   ```bash
   cp .env.example .env.local
   ```
   
   Update the `.env.local` file with your backend API URL and any other required settings.

4. **Start the development server**
   ```bash
   npm run dev
   # or
   yarn dev
   ```
   The application will be available at `http://localhost:5173` (or the port specified in your Vite config).

## Available Scripts

- `npm run dev` - Start the development server
- `npm run build` - Build the application for production
- `npm run preview` - Preview the production build locally
- `npm run lint` - Run ESLint to check for code issues
- `npm run type-check` - Check TypeScript types

## Project Structure

```
src/
├── api/                 # API service functions
├── assets/              # Static assets (images, fonts, etc.)
├── components/          # Reusable UI components
│   ├── CompanyTable.tsx # Company data table component
│   ├── FilterPanel.tsx  # Search filters component
│   ├── Navbar.tsx       # Main navigation bar
│   ├── Pagination.tsx   # Pagination controls
│   └── PrivateRoute.tsx # Protected route component
├── context/             # React context providers
│   └── AuthContext.tsx  # Authentication context
├── pages/               # Page components
│   ├── Dashboard.tsx    # Main dashboard page
│   └── Login.tsx        # Login page
├── types/               # TypeScript type definitions
└── utils/               # Utility functions
```

## Styling

This project uses [Tailwind CSS](https://tailwindcss.com/) for styling. Custom styles can be added in `src/index.css` or by using Tailwind's `@apply` directive in component CSS modules.

## Authentication

The application uses JWT-based authentication. The authentication state is managed by the `AuthContext` and persisted in local storage.

## API Integration

API requests are made using Axios with request/response interceptors for handling authentication and errors. The API client is configured in `src/api/api.ts`.

## Environment Variables

- `VITE_API_URL` - Base URL for the backend API (default: `http://localhost:8000`)
- `VITE_AUTH0_DOMAIN` - Auth0 domain (if using Auth0)
- `VITE_AUTH0_CLIENT_ID` - Auth0 client ID (if using Auth0)
- `VITE_ENABLE_XBRL_INTEGRATION` - Enable XBRL integration (default: `false`)
- `VITE_ENABLE_CONTACT_SCRAPER` - Enable contact scraper (default: `false`)

## Testing

To run tests:

```bash
npm test
# or
yarn test
```

## Building for Production

To create a production build:

```bash
npm run build
```

The build artifacts will be stored in the `dist/` directory.

## Deployment

The application can be deployed to any static hosting service (Vercel, Netlify, GitHub Pages, etc.) or served using a web server like Nginx or Apache.

## License

MIT
