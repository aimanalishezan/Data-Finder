import { apiRequest } from './api';

export interface Company {
  id: number;
  business_id: string;
  name: string;
  industry?: string;
  city?: string;
  company_type?: string;
  address?: string;
  registration_date?: string;
  revenue?: number;
  postal_code?: string;
  website?: string;
  status?: string;
  created_at?: string;
  updated_at?: string;
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  skip: number;
  limit: number;
}

export interface CompanyFilters {
  search?: string;
  company_name?: string;
  business_id?: string;
  industry?: string;
  location?: string;
  city?: string;
  company_type?: string;
  min_revenue?: string;
  max_revenue?: string;
  min_date?: string;
  max_date?: string;
  skip?: number;
  limit?: number;
}

export const fetchCompanies = async (filters: CompanyFilters = {}) => {
  const params = new URLSearchParams();
  
  // Add filters to query params
  Object.entries(filters).forEach(([key, value]) => {
    if (value !== undefined && value !== '') {
      params.append(key, String(value));
    }
  });
  
  return apiRequest<PaginatedResponse<Company>>('get', `/companies?${params.toString()}`);
};

export const fetchCompanyById = async (id: number) => {
  return apiRequest<Company>('get', `/companies/${id}`);
};

export const exportCompanies = async (filters: CompanyFilters = {}) => {
  const params = new URLSearchParams();
  
  // Add filters to query params
  Object.entries(filters).forEach(([key, value]) => {
    if (value !== undefined && value !== '') {
      params.append(key, String(value));
    }
  });
  
  return apiRequest('get', `/export?${params.toString()}`, null, {
    responseType: 'blob',
  });
};
