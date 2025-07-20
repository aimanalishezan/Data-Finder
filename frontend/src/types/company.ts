/**
 * Company type definition
 * Represents a company entity in the system
 */
export interface Company {
  /** Unique identifier */
  id: number;
  
  /** Business registration number or identifier */
  business_id: string;
  
  /** Company name */
  name: string;
  
  /** Industry sector */
  industry?: string;
  
  /** City where the company is located */
  city?: string;
  
  /** Type of company (e.g., LLC, Corporation) */
  company_type?: string;
  
  /** Full company address */
  address?: string;
  
  /** Date of company registration (ISO format) */
  registration_date?: string;
  
  /** Company revenue in euros */
  revenue?: number;
  
  /** Date when the record was created (ISO format) */
  created_at?: string;
  
  /** Date when the record was last updated (ISO format) */
  updated_at?: string;
  
  /** Additional metadata (optional) */
  metadata?: Record<string, unknown>;
}

/**
 * Paginated response type for company listings
 */
export interface PaginatedCompanyResponse {
  /** Array of company records */
  data: Company[];
  
  /** Total number of records matching the query */
  total: number;
  
  /** Number of records to skip for pagination */
  skip: number;
  
  /** Maximum number of records per page */
  limit: number;
}

/**
 * Company filter parameters
 */
export interface CompanyFilters {
  /** Search query string */
  search?: string;
  
  /** Filter by industry */
  industry?: string;
  
  /** Filter by city */
  city?: string;
  
  /** Filter by company type */
  company_type?: string;
  
  /** Filter by minimum registration date (YYYY-MM-DD) */
  min_date?: string;
  
  /** Filter by maximum registration date (YYYY-MM-DD) */
  max_date?: string;
  
  /** Number of records to skip (for pagination) */
  skip?: number;
  
  /** Maximum number of records to return (for pagination) */
  limit?: number;
  
  /** Field to sort by */
  sort_by?: keyof Company;
  
  /** Sort direction */
  sort_order?: 'asc' | 'desc';
}

/**
 * Company statistics
 */
export interface CompanyStats {
  /** Total number of companies */
  total_companies: number;
  
  /** Number of companies by industry */
  by_industry: Record<string, number>;
  
  /** Number of companies by city */
  by_city: Record<string, number>;
  
  /** Number of companies by type */
  by_type: Record<string, number>;
  
  /** Number of new companies by year */
  by_year: Record<string, number>;
}
