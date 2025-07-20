import React from 'react';
import { FunnelIcon, XMarkIcon } from '@heroicons/react/24/outline';

interface FilterPanelProps {
  filters: {
    company_name: string;
    business_id: string;
    industry: string;
    location: string;
    city: string;
    company_type: string;
    min_revenue: string;
    max_revenue: string;
    min_date: string;
    max_date: string;
  };
  onFilterChange: (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => void;
  onReset: () => void;
}

export default function FilterPanel({ filters, onFilterChange, onReset }: FilterPanelProps) {
  return (
    <div className="mt-4 rounded-md bg-gray-50 p-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-medium text-gray-900">Filters</h3>
        <button
          type="button"
          onClick={onReset}
          className="text-sm font-medium text-indigo-600 hover:text-indigo-500"
        >
          Reset all
        </button>
      </div>
      
      <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        <div>
          <label htmlFor="company_name" className="block text-sm font-medium text-gray-700">
            Company Name
          </label>
          <input
            type="text"
            name="company_name"
            id="company_name"
            value={filters.company_name}
            onChange={onFilterChange}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            placeholder="e.g. Tech Solutions"
          />
        </div>
        
        <div>
          <label htmlFor="business_id" className="block text-sm font-medium text-gray-700">
            Business ID
          </label>
          <input
            type="text"
            name="business_id"
            id="business_id"
            value={filters.business_id}
            onChange={onFilterChange}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            placeholder="e.g. FI12345678"
          />
        </div>
        
        <div>
          <label htmlFor="industry" className="block text-sm font-medium text-gray-700">
            Industry
          </label>
          <input
            type="text"
            name="industry"
            id="industry"
            value={filters.industry}
            onChange={onFilterChange}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            placeholder="e.g. Technology, Healthcare"
          />
        </div>
        
        <div>
          <label htmlFor="location" className="block text-sm font-medium text-gray-700">
            Location
          </label>
          <input
            type="text"
            name="location"
            id="location"
            value={filters.location}
            onChange={onFilterChange}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            placeholder="e.g. Helsinki, Finland"
          />
        </div>
        
        <div>
          <label htmlFor="city" className="block text-sm font-medium text-gray-700">
            City
          </label>
          <input
            type="text"
            name="city"
            id="city"
            value={filters.city}
            onChange={onFilterChange}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            placeholder="e.g. Helsinki"
          />
        </div>
        
        <div>
          <label htmlFor="company_type" className="block text-sm font-medium text-gray-700">
            Company Type
          </label>
          <select
            id="company_type"
            name="company_type"
            value={filters.company_type}
            onChange={onFilterChange}
            className="mt-1 block w-full rounded-md border-gray-300 py-2 pl-3 pr-10 text-base focus:border-indigo-500 focus:outline-none focus:ring-indigo-500 sm:text-sm"
          >
            <option value="">All Types</option>
            <option value="Corporation">Corporation</option>
            <option value="LLC">LLC</option>
            <option value="Sole Proprietorship">Sole Proprietorship</option>
            <option value="Partnership">Partnership</option>
            <option value="Non-Profit">Non-Profit</option>
          </select>
        </div>
        
        <div>
          <label htmlFor="min_revenue" className="block text-sm font-medium text-gray-700">
            Min Revenue (€)
          </label>
          <input
            type="number"
            name="min_revenue"
            id="min_revenue"
            value={filters.min_revenue}
            onChange={onFilterChange}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            placeholder="e.g. 100000"
            min="0"
            step="1000"
          />
        </div>
        
        <div>
          <label htmlFor="max_revenue" className="block text-sm font-medium text-gray-700">
            Max Revenue (€)
          </label>
          <input
            type="number"
            name="max_revenue"
            id="max_revenue"
            value={filters.max_revenue}
            onChange={onFilterChange}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            placeholder="e.g. 5000000"
            min="0"
            step="1000"
          />
        </div>
        
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label htmlFor="min_date" className="block text-sm font-medium text-gray-700">
              From Date
            </label>
            <input
              type="date"
              name="min_date"
              id="min_date"
              value={filters.min_date}
              onChange={onFilterChange}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            />
          </div>
          
          <div>
            <label htmlFor="max_date" className="block text-sm font-medium text-gray-700">
              To Date
            </label>
            <input
              type="date"
              name="max_date"
              id="max_date"
              value={filters.max_date}
              onChange={onFilterChange}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm"
            />
          </div>
        </div>
      </div>
    </div>
  );
}
