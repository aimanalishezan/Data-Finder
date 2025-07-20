/**
 * Validation utilities
 */

/**
 * Validates an email address
 * @param email - The email to validate
 * @returns True if the email is valid
 */
export const isValidEmail = (email: string): boolean => {
  const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return re.test(email);
};

/**
 * Validates a URL
 * @param url - The URL to validate
 * @returns True if the URL is valid
 */
export const isValidUrl = (url: string): boolean => {
  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
};

/**
 * Validates a phone number
 * @param phone - The phone number to validate
 * @returns True if the phone number is valid
 */
export const isValidPhone = (phone: string): boolean => {
  const re = /^[+]?[(]?[0-9]{1,4}[)]?[-\s.]?[0-9]{1,3}[-\s.]?[0-9]{1,4}[-\s.]?[0-9]{1,9}$/;
  return re.test(phone);
};

/**
 * Validates a password
 * @param password - The password to validate
 * @param options - Validation options
 * @returns An object with validation results
 */
export const validatePassword = (
  password: string,
  options: {
    minLength?: number;
    requireUppercase?: boolean;
    requireLowercase?: boolean;
    requireNumbers?: boolean;
    requireSpecialChars?: boolean;
  } = {}
): {
  isValid: boolean;
  errors: string[];
  score: number; // 0-100
} => {
  const {
    minLength = 8,
    requireUppercase = true,
    requireLowercase = true,
    requireNumbers = true,
    requireSpecialChars = true,
  } = options;
  
  const errors: string[] = [];
  let score = 0;
  
  // Check minimum length
  if (password.length < minLength) {
    errors.push(`Password must be at least ${minLength} characters long`);
  } else {
    score += 25;
  }
  
  // Check for uppercase letters
  if (requireUppercase && !/[A-Z]/.test(password)) {
    errors.push('Password must contain at least one uppercase letter');
  } else if (requireUppercase) {
    score += 25;
  }
  
  // Check for lowercase letters
  if (requireLowercase && !/[a-z]/.test(password)) {
    errors.push('Password must contain at least one lowercase letter');
  } else if (requireLowercase) {
    score += 25;
  }
  
  // Check for numbers
  if (requireNumbers && !/\d/.test(password)) {
    errors.push('Password must contain at least one number');
  } else if (requireNumbers) {
    score += 15;
  }
  
  // Check for special characters
  if (requireSpecialChars && !/[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>/?]/.test(password)) {
    errors.push('Password must contain at least one special character');
  } else if (requireSpecialChars) {
    score += 10;
  }
  
  // If no requirements failed, add a small bonus
  if (errors.length === 0) {
    score = 100;
  }
  
  return {
    isValid: errors.length === 0,
    errors,
    score,
  };
};

/**
 * Validates a credit card number using Luhn algorithm
 * @param cardNumber - The credit card number to validate
 * @returns True if the credit card number is valid
 */
export const isValidCreditCard = (cardNumber: string): boolean => {
  // Remove all non-digit characters
  const cleanNumber = cardNumber.replace(/\D/g, '');
  
  // Check if the number is empty or contains non-digits
  if (!/^\d+$/.test(cleanNumber)) {
    return false;
  }
  
  // Check card length based on card type (simplified)
  const cardType = getCreditCardType(cleanNumber);
  if (!cardType) return false;
  
  // Luhn algorithm
  let sum = 0;
  let shouldDouble = false;
  
  for (let i = cleanNumber.length - 1; i >= 0; i--) {
    let digit = parseInt(cleanNumber.charAt(i), 10);
    
    if (shouldDouble) {
      digit *= 2;
      if (digit > 9) {
        digit = (digit % 10) + 1;
      }
    }
    
    sum += digit;
    shouldDouble = !shouldDouble;
  }
  
  return sum % 10 === 0;
};

/**
 * Gets the credit card type based on the card number
 * @param cardNumber - The credit card number
 * @returns The credit card type or null if unknown
 */
export const getCreditCardType = (cardNumber: string): string | null => {
  const cleanNumber = cardNumber.replace(/\D/g, '');
  
  // Visa
  if (/^4/.test(cleanNumber)) {
    return 'visa';
  }
  
  // Mastercard
  if (/^5[1-5]/.test(cleanNumber)) {
    return 'mastercard';
  }
  
  // American Express
  if (/^3[47]/.test(cleanNumber)) {
    return 'amex';
  }
  
  // Discover
  if (/^6(?:011|5)/.test(cleanNumber)) {
    return 'discover';
  }
  
  // JCB
  if (/^35(?:2[89]|[3-8][0-9])/.test(cleanNumber)) {
    return 'jcb';
  }
  
  // Diners Club
  if (/^3(?:0[0-5]|[68][0-9])/.test(cleanNumber)) {
    return 'diners';
  }
  
  return null;
};

/**
 * Validates a date string
 * @param dateString - The date string to validate (YYYY-MM-DD)
 * @returns True if the date is valid
 */
export const isValidDate = (dateString: string): boolean => {
  const re = /^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$/;
  
  if (!re.test(dateString)) {
    return false;
  }
  
  const [year, month, day] = dateString.split('-').map(Number);
  const date = new Date(year, month - 1, day);
  
  return (
    date.getFullYear() === year &&
    date.getMonth() === month - 1 &&
    date.getDate() === day
  );
};

/**
 * Validates a time string
 * @param timeString - The time string to validate (HH:MM or HH:MM:SS)
 * @returns True if the time is valid
 */
export const isValidTime = (timeString: string): boolean => {
  const re = /^([01]\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?$/;
  return re.test(timeString);
};

/**
 * Validates a date-time string
 * @param dateTimeString - The date-time string to validate (ISO 8601)
 * @returns True if the date-time is valid
 */
export const isValidDateTime = (dateTimeString: string): boolean => {
  try {
    const date = new Date(dateTimeString);
    return !isNaN(date.getTime());
  } catch {
    return false;
  }
};

/**
 * Validates a social security number (US)
 * @param ssn - The SSN to validate (with or without hyphens)
 * @returns True if the SSN is valid
 */
export const isValidSSN = (ssn: string): boolean => {
  const cleanSSN = ssn.replace(/[-\s]/g, '');
  
  // Check if it's 9 digits
  if (!/^\d{9}$/.test(cleanSSN)) {
    return false;
  }
  
  // Check for invalid SSNs
  if (/^(000|666|9)\d{2}/.test(cleanSSN)) {
    return false;
  }
  
  if (/^0{3}/.test(cleanSSN)) {
    return false;
  }
  
  if (/^0{4}$/.test(cleanSSN.substring(5))) {
    return false;
  }
  
  return true;
};

/**
 * Validates a ZIP code (US)
 * @param zipCode - The ZIP code to validate (5 or 9 digits)
 * @returns True if the ZIP code is valid
 */
export const isValidZipCode = (zipCode: string): boolean => {
  return /^\d{5}(-\d{4})?$/.test(zipCode);
};

/**
 * Validates a credit card expiration date
 * @param month - The month (1-12)
 * @param year - The full year (e.g., 2025)
 * @returns True if the expiration date is valid
 */
export const isValidCardExpiry = (month: number, year: number): boolean => {
  if (month < 1 || month > 12) {
    return false;
  }
  
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth() + 1; // 1-12
  
  if (year < currentYear) {
    return false;
  }
  
  if (year === currentYear && month < currentMonth) {
    return false;
  }
  
  return true;
};

/**
 * Validates a credit card CVV
 * @param cvv - The CVV to validate
 * @param cardType - Optional card type (visa, mastercard, amex, etc.)
 * @returns True if the CVV is valid
 */
export const isValidCVV = (cvv: string, cardType?: string): boolean => {
  if (!/^\d+$/.test(cvv)) {
    return false;
  }
  
  // American Express CVV is 4 digits, others are usually 3
  if (cardType === 'amex') {
    return cvv.length === 4;
  }
  
  return cvv.length === 3;
};

/**
 * Validates a file
 * @param file - The file to validate
 * @param options - Validation options
 * @returns An object with validation results
 */
export const validateFile = (
  file: File,
  options: {
    allowedTypes?: string[];
    maxSizeMB?: number;
    minSizeKB?: number;
  } = {}
): {
  isValid: boolean;
  errors: string[];
} => {
  const { allowedTypes = [], maxSizeMB = 10, minSizeKB = 0 } = options;
  const errors: string[] = [];
  
  // Check file type
  if (allowedTypes.length > 0 && !allowedTypes.includes(file.type)) {
    errors.push(`File type not allowed. Allowed types: ${allowedTypes.join(', ')}`);
  }
  
  // Check file size (in bytes)
  const maxSizeBytes = maxSizeMB * 1024 * 1024; // Convert MB to bytes
  const minSizeBytes = minSizeKB * 1024; // Convert KB to bytes
  
  if (file.size > maxSizeBytes) {
    errors.push(`File is too large. Maximum size: ${maxSizeMB}MB`);
  }
  
  if (file.size < minSizeBytes) {
    errors.push(`File is too small. Minimum size: ${minSizeKB}KB`);
  }
  
  return {
    isValid: errors.length === 0,
    errors,
  };
};

/**
 * Validates a form field based on validation rules
 * @param value - The field value
 * @param rules - Validation rules
 * @returns An object with validation results
 */
export const validateField = (
  value: any,
  rules: {
    required?: boolean;
    minLength?: number;
    maxLength?: number;
    min?: number;
    max?: number;
    pattern?: RegExp;
    custom?: (value: any) => string | boolean | Promise<string | boolean>;
    message?: string;
  } = {}
): Promise<{
  isValid: boolean;
  message?: string;
}> => {
  const { 
    required = false, 
    minLength, 
    maxLength, 
    min, 
    max, 
    pattern, 
    custom,
    message,
  } = rules;
  
  // Check required field
  if (required && (value === undefined || value === null || value === '')) {
    return Promise.resolve({
      isValid: false,
      message: message || 'This field is required',
    });
  }
  
  // Skip further validation if the field is empty and not required
  if (value === undefined || value === null || value === '') {
    return Promise.resolve({ isValid: true });
  }
  
  // Check min length
  if (minLength !== undefined && String(value).length < minLength) {
    return Promise.resolve({
      isValid: false,
      message: message || `Minimum length is ${minLength} characters`,
    });
  }
  
  // Check max length
  if (maxLength !== undefined && String(value).length > maxLength) {
    return Promise.resolve({
      isValid: false,
      message: message || `Maximum length is ${maxLength} characters`,
    });
  }
  
  // Check min value (for numbers)
  if (min !== undefined && typeof value === 'number' && value < min) {
    return Promise.resolve({
      isValid: false,
      message: message || `Minimum value is ${min}`,
    });
  }
  
  // Check max value (for numbers)
  if (max !== undefined && typeof value === 'number' && value > max) {
    return Promise.resolve({
      isValid: false,
      message: message || `Maximum value is ${max}`,
    });
  }
  
  // Check pattern
  if (pattern && !pattern.test(String(value))) {
    return Promise.resolve({
      isValid: false,
      message: message || 'Invalid format',
    });
  }
  
  // Run custom validation if provided
  if (custom) {
    const result = custom(value);
    
    if (result instanceof Promise) {
      return result.then(res => {
        if (res === true) {
          return { isValid: true };
        } else if (res === false) {
          return { isValid: false, message: message || 'Validation failed' };
        } else {
          return { isValid: false, message: res as string };
        }
      });
    } else {
      if (result === true) {
        return Promise.resolve({ isValid: true });
      } else if (result === false) {
        return Promise.resolve({ 
          isValid: false, 
          message: message || 'Validation failed' 
        });
      } else {
        return Promise.resolve({ 
          isValid: false, 
          message: result as string 
        });
      }
    }
  }
  
  // If all validations pass
  return Promise.resolve({ isValid: true });
};

/**
 * Validates a form with multiple fields
 * @param formData - The form data object
 * @param validationRules - Validation rules for each field
 * @returns A promise that resolves with validation results
 */
export const validateForm = async <T extends Record<string, any>>(
  formData: T,
  validationRules: {
    [K in keyof T]?: Parameters<typeof validateField>[1];
  }
): Promise<{
  isValid: boolean;
  errors: Partial<Record<keyof T, string>>;
}> => {
  const errors: Partial<Record<keyof T, string>> = {};
  let isValid = true;
  
  // Validate each field
  const validations = Object.entries(validationRules).map(
    async ([field, rules]) => {
      const value = formData[field as keyof T];
      const result = await validateField(value, rules as any);
      
      if (!result.isValid) {
        isValid = false;
        errors[field as keyof T] = result.message || 'Invalid field';
      }
    }
  );
  
  // Wait for all validations to complete
  await Promise.all(validations);
  
  return { isValid, errors };
};
