/**
 * Format a date string to a more readable format
 * @param dateString - The date string to format (ISO format or similar)
 * @param format - The format to use (default: 'MMM d, yyyy')
 * @returns Formatted date string
 */
export const formatDate = (
  dateString: string | Date | undefined | null,
  format: string = 'MMM d, yyyy'
): string => {
  if (!dateString) return '-';
  
  const date = typeof dateString === 'string' ? new Date(dateString) : dateString;
  
  // Handle invalid date
  if (isNaN(date.getTime())) return '-';
  
  // Extract date components
  const day = date.getDate();
  const month = date.getMonth();
  const year = date.getFullYear();
  const hours = date.getHours();
  const minutes = date.getMinutes();
  const seconds = date.getSeconds();
  
  // Month names
  const monthNames = [
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
  ];
  
  // Full month names
  const fullMonthNames = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
  ];
  
  // Day names
  const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const fullDayNames = [
    'Sunday', 'Monday', 'Tuesday', 'Wednesday', 
    'Thursday', 'Friday', 'Saturday'
  ];
  
  // Format the date according to the format string
  return format
    .replace(/yyyy/g, year.toString())
    .replace(/yy/g, year.toString().slice(-2))
    .replace(/MMMM/g, fullMonthNames[month])
    .replace(/MMM/g, monthNames[month])
    .replace(/MM/g, (month + 1).toString().padStart(2, '0'))
    .replace(/M/g, (month + 1).toString())
    .replace(/dddd/g, fullDayNames[date.getDay()])
    .replace(/ddd/g, dayNames[date.getDay()])
    .replace(/dd/g, day.toString().padStart(2, '0'))
    .replace(/d/g, day.toString())
    .replace(/HH/g, hours.toString().padStart(2, '0'))
    .replace(/H/g, hours.toString())
    .replace(/hh/g, (hours % 12 || 12).toString().padStart(2, '0'))
    .replace(/h/g, (hours % 12 || 12).toString())
    .replace(/mm/g, minutes.toString().padStart(2, '0'))
    .replace(/m/g, minutes.toString())
    .replace(/ss/g, seconds.toString().padStart(2, '0'))
    .replace(/s/g, seconds.toString())
    .replace(/a/g, hours < 12 ? 'am' : 'pm')
    .replace(/A/g, hours < 12 ? 'AM' : 'PM');
};

/**
 * Calculate the time difference between two dates in a human-readable format
 * @param startDate - The start date
 * @param endDate - The end date (defaults to now)
 * @returns Human-readable time difference (e.g., "2 days ago", "3 months ago")
 */
export const timeAgo = (
  startDate: string | Date,
  endDate: string | Date = new Date()
): string => {
  const start = typeof startDate === 'string' ? new Date(startDate) : startDate;
  const end = typeof endDate === 'string' ? new Date(endDate) : endDate;
  
  // Handle invalid dates
  if (isNaN(start.getTime()) || isNaN(end.getTime())) {
    return 'Invalid date';
  }
  
  const seconds = Math.floor((end.getTime() - start.getTime()) / 1000);
  
  // Less than a minute
  if (seconds < 60) {
    return 'just now';
  }
  
  // Less than an hour
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes} minute${minutes === 1 ? '' : 's'} ago`;
  }
  
  // Less than a day
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours} hour${hours === 1 ? '' : 's'} ago`;
  }
  
  // Less than a month
  const days = Math.floor(hours / 24);
  if (days < 30) {
    return `${days} day${days === 1 ? '' : 's'} ago`;
  }
  
  // Less than a year
  const months = Math.floor(days / 30);
  if (months < 12) {
    return `${months} month${months === 1 ? '' : 's'} ago`;
  }
  
  // Years
  const years = Math.floor(months / 12);
  return `${years} year${years === 1 ? '' : 's'} ago`;
};

/**
 * Check if a date is today
 * @param date - The date to check
 * @returns True if the date is today
 */
export const isToday = (date: Date | string): boolean => {
  const d = typeof date === 'string' ? new Date(date) : date;
  const today = new Date();
  
  return (
    d.getDate() === today.getDate() &&
    d.getMonth() === today.getMonth() &&
    d.getFullYear() === today.getFullYear()
  );
};

/**
 * Check if a date is in the past
 * @param date - The date to check
 * @returns True if the date is in the past
 */
export const isPast = (date: Date | string): boolean => {
  const d = typeof date === 'string' ? new Date(date) : date;
  return d.getTime() < Date.now();
};

/**
 * Check if a date is in the future
 * @param date - The date to check
 * @returns True if the date is in the future
 */
export const isFuture = (date: Date | string): boolean => {
  return !isPast(date);
};

/**
 * Get the start of the day for a given date
 * @param date - The date
 * @returns A new date object set to the start of the day (00:00:00.000)
 */
export const startOfDay = (date: Date = new Date()): Date => {
  const d = new Date(date);
  d.setHours(0, 0, 0, 0);
  return d;
};

/**
 * Get the end of the day for a given date
 * @param date - The date
 * @returns A new date object set to the end of the day (23:59:59.999)
 */
export const endOfDay = (date: Date = new Date()): Date => {
  const d = new Date(date);
  d.setHours(23, 59, 59, 999);
  return d;
};

/**
 * Add days to a date
 * @param date - The date to add days to
 * @param days - The number of days to add (can be negative)
 * @returns A new date object with the days added
 */
export const addDays = (date: Date, days: number): Date => {
  const result = new Date(date);
  result.setDate(result.getDate() + days);
  return result;
};

/**
 * Get the difference in days between two dates
 * @param date1 - The first date
 * @param date2 - The second date
 * @returns The difference in days (positive if date1 is after date2)
 */
export const differenceInDays = (date1: Date, date2: Date = new Date()): number => {
  const diffTime = date1.getTime() - date2.getTime();
  return Math.floor(diffTime / (1000 * 60 * 60 * 24));
};

/**
 * Format a duration in milliseconds to a human-readable string
 * @param ms - The duration in milliseconds
 * @returns A human-readable duration string (e.g., "2h 30m")
 */
export const formatDuration = (ms: number): string => {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  
  if (days > 0) {
    return `${days}d ${hours % 24}h`;
  }
  
  if (hours > 0) {
    return `${hours}h ${minutes % 60}m`;
  }
  
  if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  }
  
  return `${seconds}s`;
};
