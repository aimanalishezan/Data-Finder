/**
 * Formats a number with commas as thousands separators
 * @param num - The number to format
 * @param decimals - Number of decimal places to show (default: 0)
 * @returns Formatted number string
 */
export const formatNumber = (num: number, decimals: number = 0): string => {
  if (isNaN(num)) return '0';
  
  return num.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
};

/**
 * Formats a number as a currency string
 * @param num - The number to format
 * @param currency - The currency code (e.g., 'USD', 'EUR')
 * @param decimals - Number of decimal places to show (default: 2)
 * @returns Formatted currency string
 */
export const formatCurrency = (
  num: number, 
  currency: string = 'USD', 
  decimals: number = 2
): string => {
  if (isNaN(num)) return '$0.00';
  
  return num.toLocaleString(undefined, {
    style: 'currency',
    currency,
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
};

/**
 * Formats a number as a percentage
 * @param num - The number to format (0-1)
 * @param decimals - Number of decimal places to show (default: 0)
 * @returns Formatted percentage string
 */
export const formatPercent = (num: number, decimals: number = 0): string => {
  if (isNaN(num)) return '0%';
  
  return (num * 100).toLocaleString(undefined, {
    style: 'percent',
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
};

/**
 * Clamps a number between a minimum and maximum value
 * @param num - The number to clamp
 * @param min - The minimum value
 * @param max - The maximum value
 * @returns The clamped number
 */
export const clamp = (num: number, min: number, max: number): number => {
  return Math.min(Math.max(num, min), max);
};

/**
 * Rounds a number to a specified number of decimal places
 * @param num - The number to round
 * @param decimals - Number of decimal places (default: 0)
 * @returns The rounded number
 */
export const round = (num: number, decimals: number = 0): number => {
  const factor = Math.pow(10, decimals);
  return Math.round((num + Number.EPSILON) * factor) / factor;
};

/**
 * Floors a number to a specified number of decimal places
 * @param num - The number to floor
 * @param decimals - Number of decimal places (default: 0)
 * @returns The floored number
 */
export const floor = (num: number, decimals: number = 0): number => {
  const factor = Math.pow(10, decimals);
  return Math.floor(num * factor) / factor;
};

/**
 * Ceils a number to a specified number of decimal places
 * @param num - The number to ceil
 * @param decimals - Number of decimal places (default: 0)
 * @returns The ceiled number
 */
export const ceil = (num: number, decimals: number = 0): number => {
  const factor = Math.pow(10, decimals);
  return Math.ceil(num * factor) / factor;
};

/**
 * Generates a random number between min and max (inclusive)
 * @param min - The minimum value
 * @param max - The maximum value
 * @param decimals - Number of decimal places (default: 0)
 * @returns A random number between min and max
 */
export const random = (min: number, max: number, decimals: number = 0): number => {
  const num = Math.random() * (max - min) + min;
  return round(num, decimals);
};

/**
 * Checks if a number is between two values (inclusive)
 * @param num - The number to check
 * @param min - The minimum value
 * @param max - The maximum value
 * @returns True if the number is between min and max (inclusive)
 */
export const isBetween = (num: number, min: number, max: number): boolean => {
  return num >= min && num <= max;
};

/**
 * Converts a number to a human-readable file size string
 * @param bytes - The number of bytes
 * @param decimals - Number of decimal places (default: 2)
 * @returns A human-readable file size string (e.g., '1.23 MB')
 */
export const formatFileSize = (bytes: number, decimals: number = 2): string => {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(decimals))} ${sizes[i]}`;
};

/**
 * Converts a number to a human-readable duration string
 * @param seconds - The number of seconds
 * @returns A human-readable duration string (e.g., '1h 23m 45s')
 */
export const formatDuration = (seconds: number): string => {
  if (seconds < 0) return '0s';
  
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  
  const parts: string[] = [];
  
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0 || hours > 0) parts.push(`${minutes}m`);
  parts.push(`${secs}s`);
  
  return parts.join(' ');
};

/**
 * Pads a number with leading zeros
 * @param num - The number to pad
 * @param length - The total length of the resulting string
 * @returns The padded number as a string
 */
export const padNumber = (num: number, length: number): string => {
  return num.toString().padStart(length, '0');
};

/**
 * Converts a number to a roman numeral
 * @param num - The number to convert (1-3999)
 * @returns The roman numeral string
 */
export const toRoman = (num: number): string => {
  if (num < 1 || num > 3999) return num.toString();
  
  const values = [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1];
  const numerals = [
    'M', 'CM', 'D', 'CD', 'C', 'XC', 'L', 'XL', 'X', 'IX', 'V', 'IV', 'I'
  ];
  
  let result = '';
  
  for (let i = 0; i < values.length; i++) {
    while (num >= values[i]) {
      result += numerals[i];
      num -= values[i];
    }
  }
  
  return result;
};

/**
 * Converts a roman numeral to a number
 * @param str - The roman numeral string
 * @returns The converted number or NaN if invalid
 */
export const fromRoman = (str: string): number => {
  const romanMap: Record<string, number> = {
    I: 1,
    V: 5,
    X: 10,
    L: 50,
    C: 100,
    D: 500,
    M: 1000,
  };
  
  let result = 0;
  let prev = 0;
  
  for (let i = str.length - 1; i >= 0; i--) {
    const current = romanMap[str[i].toUpperCase()];
    
    if (current === undefined) {
      return NaN; // Invalid character
    }
    
    if (current < prev) {
      result -= current;
    } else {
      result += current;
    }
    
    prev = current;
  }
  
  // Verify that the result converts back to the same roman numeral
  if (toRoman(result) !== str.toUpperCase()) {
    return NaN; // Invalid roman numeral
  }
  
  return result;
};

/**
 * Calculates the percentage of a value relative to a total
 * @param value - The value
 * @param total - The total
 * @param decimals - Number of decimal places (default: 2)
 * @returns The percentage as a number between 0 and 100
 */
export const calculatePercentage = (
  value: number, 
  total: number, 
  decimals: number = 2
): number => {
  if (total === 0) return 0;
  return round((value / total) * 100, decimals);
};

/**
 * Calculates the average of an array of numbers
 * @param numbers - The array of numbers
 * @param decimals - Number of decimal places (default: 2)
 * @returns The average or 0 if the array is empty
 */
export const average = (numbers: number[], decimals: number = 2): number => {
  if (numbers.length === 0) return 0;
  const sum = numbers.reduce((a, b) => a + b, 0);
  return round(sum / numbers.length, decimals);
};

/**
 * Calculates the sum of an array of numbers
 * @param numbers - The array of numbers
 * @param decimals - Number of decimal places (default: 2)
 * @returns The sum
 */
export const sum = (numbers: number[], decimals: number = 2): number => {
  return round(numbers.reduce((a, b) => a + b, 0), decimals);
};

/**
 * Calculates the median of an array of numbers
 * @param numbers - The array of numbers
 * @returns The median or 0 if the array is empty
 */
export const median = (numbers: number[]): number => {
  if (numbers.length === 0) return 0;
  
  const sorted = [...numbers].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  
  if (sorted.length % 2 === 0) {
    return (sorted[middle - 1] + sorted[middle]) / 2;
  }
  
  return sorted[middle];
};

/**
 * Calculates the mode of an array of numbers
 * @param numbers - The array of numbers
 * @returns An array of modes (there can be multiple)
 */
export const mode = (numbers: number[]): number[] => {
  if (numbers.length === 0) return [];
  
  const frequencyMap = numbers.reduce<Record<number, number>>((acc, num) => {
    acc[num] = (acc[num] || 0) + 1;
    return acc;
  }, {});
  
  const maxFrequency = Math.max(...Object.values(frequencyMap));
  
  return Object.entries(frequencyMap)
    .filter(([_, freq]) => freq === maxFrequency)
    .map(([num]) => Number(num));
};
