import { write, utils } from 'xlsx';
import { saveAs } from 'file-saver';
import { Company } from '../types/company';

/**
 * Exports an array of companies to an Excel file
 * @param companies Array of company objects to export
 * @param fileName Name of the output file (without extension)
 */
export const exportToExcel = (companies: Company[], fileName: string = 'companies_export') => {
  try {
    // Create a new workbook
    const wb = utils.book_new();
    
    // Convert companies data to worksheet
    const ws = utils.json_to_sheet(companies, {
      header: ['name', 'business_id', 'industry', 'city', 'company_type', 'registration_date', 'address'],
      skipHeader: false,
    });
    
    // Set column widths
    const colWidths = [
      { wch: 30 }, // name
      { wch: 15 }, // business_id
      { wch: 25 }, // industry
      { wch: 20 }, // city
      { wch: 20 }, // company_type
      { wch: 15 }, // registration_date
      { wch: 40 }, // address
    ];
    
    ws['!cols'] = colWidths;
    
    // Add the worksheet to the workbook
    utils.book_append_sheet(wb, ws, 'Companies');
    
    // Generate Excel file and trigger download
    const excelBuffer = write(wb, { bookType: 'xlsx', type: 'array' });
    const data = new Blob([excelBuffer], { type: 'application/octet-stream' });
    
    // Add timestamp to filename
    const timestamp = new Date().toISOString().slice(0, 10);
    const fullFileName = `${fileName}_${timestamp}.xlsx`;
    
    // Save the file
    saveAs(data, fullFileName);
    
    return true;
  } catch (error) {
    console.error('Error exporting to Excel:', error);
    throw new Error('Failed to export data to Excel');
  }
};

/**
 * Formats a date string to a more readable format
 * @param dateString ISO date string
 * @returns Formatted date string (e.g., "Jan 1, 2023")
 */
export const formatDate = (dateString?: string): string => {
  if (!dateString) return '-';
  
  const options: Intl.DateTimeFormatOptions = {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  };
  
  return new Date(dateString).toLocaleDateString(undefined, options);
};

/**
 * Formats a number with thousands separators
 * @param num Number to format
 * @returns Formatted number string (e.g., "1,234,567")
 */
export const formatNumber = (num?: number): string => {
  if (num === undefined || num === null) return '-';
  return num.toLocaleString();
};
