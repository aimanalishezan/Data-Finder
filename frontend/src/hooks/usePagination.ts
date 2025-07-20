import { useState, useMemo } from 'react';
import config from '../config/app';

interface UsePaginationProps {
  /** Total number of items */
  totalItems: number;
  
  /** Initial page number (1-based) */
  initialPage?: number;
  
  /** Number of items per page */
  itemsPerPage?: number;
  
  /** Callback function when page changes */
  onPageChange?: (page: number) => void;
}

interface UsePaginationReturn {
  // Current page number (1-based)
  currentPage: number;
  
  // Number of items per page
  itemsPerPage: number;
  
  // Total number of pages
  totalPages: number;
  
  // Array of page numbers to display in pagination
  pageNumbers: number[];
  
  // Go to a specific page
  goToPage: (page: number) => void;
  
  // Go to the next page
  nextPage: () => void;
  
  // Go to the previous page
  prevPage: () => void;
  
  // Check if there's a next page
  hasNextPage: boolean;
  
  // Check if there's a previous page
  hasPrevPage: boolean;
  
  // Get the start and end index of items for the current page
  getPaginationRange: () => { start: number; end: number };
  
  // Get the current page items from an array
  getPaginatedItems: <T>(items: T[]) => T[];
}

/**
 * Custom hook for handling pagination logic
 */
export const usePagination = ({
  totalItems,
  initialPage = 1,
  itemsPerPage: initialItemsPerPage = config.api.itemsPerPage,
  onPageChange,
}: UsePaginationProps): UsePaginationReturn => {
  const [currentPage, setCurrentPage] = useState(initialPage);
  const [itemsPerPage, setItemsPerPage] = useState(initialItemsPerPage);
  
  // Calculate total pages
  const totalPages = Math.ceil(totalItems / itemsPerPage) || 1;
  
  // Ensure current page is within valid range
  const safePage = Math.min(Math.max(1, currentPage), totalPages);
  if (currentPage !== safePage) {
    setCurrentPage(safePage);
  }
  
  // Calculate the range of page numbers to display
  const pageNumbers = useMemo(() => {
    const maxVisiblePages = 5; // Number of page numbers to show in pagination
    const halfVisiblePages = Math.floor(maxVisiblePages / 2);
    
    let startPage = Math.max(1, safePage - halfVisiblePages);
    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
    
    // Adjust if we're near the start or end
    if (endPage - startPage + 1 < maxVisiblePages) {
      startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }
    
    return Array.from(
      { length: endPage - startPage + 1 },
      (_, i) => startPage + i
    );
  }, [safePage, totalPages]);
  
  // Navigate to a specific page
  const goToPage = (page: number) => {
    const newPage = Math.max(1, Math.min(page, totalPages));
    if (newPage !== currentPage) {
      setCurrentPage(newPage);
      onPageChange?.(newPage);
    }
  };
  
  // Go to next page
  const nextPage = () => {
    if (hasNextPage) {
      const newPage = currentPage + 1;
      setCurrentPage(newPage);
      onPageChange?.(newPage);
    }
  };
  
  // Go to previous page
  const prevPage = () => {
    if (hasPrevPage) {
      const newPage = currentPage - 1;
      setCurrentPage(newPage);
      onPageChange?.(newPage);
    }
  };
  
  // Check if there's a next page
  const hasNextPage = currentPage < totalPages;
  
  // Check if there's a previous page
  const hasPrevPage = currentPage > 1;
  
  // Get the start and end index of items for the current page
  const getPaginationRange = () => {
    const start = (currentPage - 1) * itemsPerPage;
    const end = Math.min(start + itemsPerPage - 1, totalItems - 1);
    return { start, end };
  };
  
  // Get the current page items from an array
  const getPaginatedItems = <T,>(items: T[]): T[] => {
    const { start, end } = getPaginationRange();
    return items.slice(start, end + 1);
  };
  
  return {
    currentPage: safePage,
    itemsPerPage,
    totalPages,
    pageNumbers,
    goToPage,
    nextPage,
    prevPage,
    hasNextPage,
    hasPrevPage,
    getPaginationRange,
    getPaginatedItems,
  };
};

export default usePagination;
