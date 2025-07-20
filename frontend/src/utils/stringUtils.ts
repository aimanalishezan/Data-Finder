/**
 * Capitalizes the first letter of a string
 * @param str - The string to capitalize
 * @returns The string with the first letter capitalized
 */
export const capitalize = (str: string): string => {
  if (!str) return '';
  return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
};

/**
 * Truncates a string to a specified length and adds an ellipsis if needed
 * @param str - The string to truncate
 * @param maxLength - The maximum length of the string
 * @param ellipsis - The ellipsis string to append (default: '...')
 * @returns The truncated string
 */
export const truncate = (str: string, maxLength: number, ellipsis: string = '...'): string => {
  if (!str || str.length <= maxLength) return str;
  return `${str.substring(0, maxLength)}${ellipsis}`;
};

/**
 * Converts a string to kebab-case
 * @param str - The string to convert
 * @returns The kebab-cased string
 */
export const toKebabCase = (str: string): string => {
  return str
    .replace(/([a-z])([A-Z])/g, '$1-$2')
    .replace(/[\s_]+/g, '-')
    .toLowerCase();
};

/**
 * Converts a string to camelCase
 * @param str - The string to convert
 * @returns The camelCased string
 */
export const toCamelCase = (str: string): string => {
  return str
    .replace(/(?:^\w|[A-Z]|\b\w)/g, (word, index) => {
      return index === 0 ? word.toLowerCase() : word.toUpperCase();
    })
    .replace(/[\s-]+/g, '');
};

/**
 * Converts a string to PascalCase
 * @param str - The string to convert
 * @returns The PascalCased string
 */
export const toPascalCase = (str: string): string => {
  return (` ${str}`)
    .toLowerCase()
    .replace(/[^a-zA-Z0-9]+(.)/g, (_, chr) => chr.toUpperCase())
    .trim();
};

/**
 * Converts a string to title case
 * @param str - The string to convert
 * @returns The title-cased string
 */
export const toTitleCase = (str: string): string => {
  return str
    .toLowerCase()
    .split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
};

/**
 * Generates a random string of specified length
 * @param length - The length of the random string
 * @returns A random string
 */
export const randomString = (length: number = 10): string => {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  
  return result;
};

/**
 * Checks if a string is a valid email address
 * @param email - The email address to validate
 * @returns True if the email is valid
 */
export const isValidEmail = (email: string): boolean => {
  const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return re.test(email);
};

/**
 * Checks if a string is a valid URL
 * @param url - The URL to validate
 * @returns True if the URL is valid
 */
export const isValidUrl = (url: string): boolean => {
  try {
    new URL(url);
    return true;
  } catch (e) {
    return false;
  }
};

/**
 * Removes HTML tags from a string
 * @param html - The HTML string
 * @returns The plain text without HTML tags
 */
export const stripHtml = (html: string): string => {
  const tmp = document.createElement('div');
  tmp.innerHTML = html;
  return tmp.textContent || tmp.innerText || '';
};

/**
 * Escapes special characters in a string for use in a regular expression
 * @param str - The string to escape
 * @returns The escaped string
 */
export const escapeRegExp = (str: string): string => {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
};

/**
 * Replaces all occurrences of a substring in a string
 * @param str - The original string
 * @param search - The substring to replace
 * @param replacement - The replacement string
 * @returns The string with all occurrences replaced
 */
export const replaceAll = (str: string, search: string, replacement: string): string => {
  return str.split(search).join(replacement);
};

/**
 * Converts a string to a URL-friendly slug
 * @param str - The string to convert
 * @returns The slugified string
 */
export const slugify = (str: string): string => {
  return str
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, '') // Remove special characters
    .replace(/[\s_-]+/g, '-') // Replace spaces and underscores with a single dash
    .replace(/^-+|-+$/g, ''); // Remove leading/trailing dashes
};

/**
 * Counts the number of words in a string
 * @param str - The string to count words in
 * @returns The word count
 */
export const wordCount = (str: string): number => {
  if (!str.trim()) return 0;
  return str.trim().split(/\s+/).length;
};

/**
 * Counts the number of characters in a string (supports Unicode)
 * @param str - The string to count characters in
 * @returns The character count
 */
export const charCount = (str: string): number => {
  return Array.from(str).length;
};

/**
 * Pads a string with a specified character to reach a specified length
 * @param str - The string to pad
 * @param length - The target length
 * @param char - The padding character (default: ' ')
 * @param left - Whether to pad on the left (true) or right (false)
 * @returns The padded string
 */
export const pad = (str: string, length: number, char: string = ' ', left: boolean = true): string => {
  const padding = char.repeat(Math.max(0, length - str.length));
  return left ? padding + str : str + padding;
};

/**
 * Generates a hash code for a string
 * @param str - The string to hash
 * @returns A 32-bit signed integer hash
 */
export const hashCode = (str: string): number => {
  let hash = 0;
  
  if (str.length === 0) return hash;
  
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  
  return hash;
};

/**
 * Removes diacritics (accents) from a string
 * @param str - The string to normalize
 * @returns The normalized string without diacritics
 */
export const removeDiacritics = (str: string): string => {
  return str.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
};

/**
 * Converts a string to a boolean
 * @param str - The string to convert
 * @param defaultValue - The default value to return if the string is not a valid boolean
 * @returns The boolean value or the default value
 */
export const toBoolean = (str: string | boolean | number | null | undefined, defaultValue: boolean = false): boolean => {
  if (typeof str === 'boolean') return str;
  if (typeof str === 'number') return str !== 0;
  if (!str) return defaultValue;
  
  const normalized = str.toString().toLowerCase().trim();
  
  if (['true', 'yes', '1', 'y', 't'].includes(normalized)) {
    return true;
  }
  
  if (['false', 'no', '0', 'n', 'f', ''].includes(normalized)) {
    return false;
  }
  
  return defaultValue;
};
