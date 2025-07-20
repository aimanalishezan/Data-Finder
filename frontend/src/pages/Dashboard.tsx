import React, { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { MagnifyingGlassIcon, FunnelIcon, DocumentArrowDownIcon } from '@heroicons/react/24/outline';
import CompanyTable from '../components/CompanyTable';
import Pagination from '../components/Pagination';
import FilterPanel from '../components/FilterPanel';
import { exportToExcel } from '../utils/export';
import { fetchCompanies } from '../api/company';
import { useAuth } from '../context/AuthContext';

export default function Dashboard() {
  const [filters, setFilters] = useState({
    search: '',
    company_name: '',
    business_id: '',
    industry: '',
    location: '',
    city: '',
    company_type: '',
    min_revenue: '',
    max_revenue: '',
    min_date: '',
    max_date: '',
  });
  const [currentPage, setCurrentPage] = useState(1);
  const [showFilters, setShowFilters] = useState(false);
  const itemsPerPage = 10;
  const navigate = useNavigate();
  const { logout } = useAuth();

  // Fetch companies with filters and pagination
  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['companies', currentPage, filters],
    queryFn: () =>
      fetchCompanies({
        skip: (currentPage - 1) * itemsPerPage,
        limit: itemsPerPage,
        ...filters,
      }),
    keepPreviousData: true,
  });

  // Reset to first page when filters change
  useEffect(() => {
    setCurrentPage(1);
  }, [filters]);

  const handleFilterChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value } = e.target;
    setFilters((prev) => ({
      ...prev,
      [name]: value,
    }));
  };

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    refetch();
  };

  const handleResetFilters = () => {
    setFilters({
      search: '',
      company_name: '',
      business_id: '',
      industry: '',
      location: '',
      city: '',
      company_type: '',
      min_revenue: '',
      max_revenue: '',
      min_date: '',
      max_date: '',
    });
  };

  const handleExport = async () => {
    try {
      const response = await fetch(
        `${import.meta.env.VITE_API_URL}/export?${new URLSearchParams(
          Object.entries(filters).reduce((acc, [key, value]) => {
            if (value) acc[key] = value;
            return acc;
          }, {} as Record<string, string>)
        )}`,
        {
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${localStorage.getItem('token')}`,
          },
        }
      );

      if (!response.ok) throw new Error('Export failed');

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `companies_export_${new Date().toISOString().split('T')[0]}.xlsx`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      a.remove();
    } catch (error) {
      console.error('Export error:', error);
      alert('Failed to export data. Please try again.');
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-center md:justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Company Directory</h1>
        <div className="mt-4 flex space-x-3 md:mt-0">
          <button
            type="button"
            onClick={handleExport}
            className="inline-flex items-center rounded-md bg-white px-3 py-2 text-sm font-semibold text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 hover:bg-gray-50"
          >
            <DocumentArrowDownIcon className="-ml-0.5 mr-1.5 h-5 w-5" aria-hidden="true" />
            Export to Excel
          </button>
        </div>
      </div>

      <div className="rounded-lg bg-white shadow">
        <div className="border-b border-gray-200 px-4 py-5 sm:px-6">
          <form onSubmit={handleSearch} className="space-y-4">
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between">
              <div className="relative flex-1">
                <div className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3">
                  <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" aria-hidden="true" />
                </div>
                <input
                  type="text"
                  name="search"
                  value={filters.search}
                  onChange={handleFilterChange}
                  className="block w-full rounded-md border-0 py-1.5 pl-10 pr-3 text-gray-900 ring-1 ring-inset ring-gray-300 placeholder:text-gray-400 focus:ring-2 focus:ring-inset focus:ring-indigo-600 sm:text-sm sm:leading-6"
                  placeholder="Search companies..."
                />
              </div>
              <button
                type="button"
                onClick={() => setShowFilters(!showFilters)}
                className="mt-3 inline-flex items-center rounded-md bg-white px-3 py-2 text-sm font-semibold text-gray-900 shadow-sm ring-1 ring-inset ring-gray-300 hover:bg-gray-50 sm:mt-0 sm:ml-3"
              >
                <FunnelIcon className="-ml-0.5 mr-1.5 h-5 w-5" aria-hidden="true" />
                {showFilters ? 'Hide Filters' : 'Show Filters'}
              </button>
            </div>

            {showFilters && (
              <FilterPanel
                filters={filters}
                onFilterChange={handleFilterChange}
                onReset={handleResetFilters}
              />
            )}
          </form>
        </div>

        <div className="overflow-x-auto">
          <CompanyTable
            companies={data?.data || []}
            isLoading={isLoading}
            error={error as Error}
          />
        </div>

        <div className="flex items-center justify-between border-t border-gray-200 bg-white px-4 py-3 sm:px-6">
          <div className="flex flex-1 justify-between sm:hidden">
            <button
              onClick={() => setCurrentPage((prev) => Math.max(prev - 1, 1))}
              disabled={currentPage === 1}
              className="relative inline-flex items-center rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              Previous
            </button>
            <button
              onClick={() => setCurrentPage((prev) => prev + 1)}
              disabled={!data || data.data.length < itemsPerPage}
              className="relative ml-3 inline-flex items-center rounded-md border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              Next
            </button>
          </div>
          <div className="hidden sm:flex sm:flex-1 sm:items-center sm:justify-between">
            <div>
              <p className="text-sm text-gray-700">
                Showing <span className="font-medium">{(currentPage - 1) * itemsPerPage + 1}</span> to{' '}
                <span className="font-medium">
                  {Math.min(currentPage * itemsPerPage, data?.total || 0)}
                </span>{' '}
                of <span className="font-medium">{data?.total || 0}</span> results
              </p>
            </div>
            <Pagination
              currentPage={currentPage}
              totalItems={data?.total || 0}
              itemsPerPage={itemsPerPage}
              onPageChange={setCurrentPage}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
