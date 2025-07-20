import { AxiosError } from 'axios';
import { toast } from 'react-hot-toast';

/**
 * Handles API errors and displays appropriate error messages to the user
 * @param error The error object from Axios or a standard Error
 * @param defaultMessage Default error message to display if no specific message is available
 */
export const handleApiError = (error: unknown, defaultMessage = 'An error occurred'): void => {
  console.error('API Error:', error);
  
  // Handle Axios errors
  if (isAxiosError(error)) {
    const { response, request, message } = error;
    
    // Server responded with an error status code (4xx, 5xx)
    if (response) {
      const { status, data } = response;
      
      // Handle specific HTTP status codes
      switch (status) {
        case 400:
          toast.error((data as any)?.message || 'Invalid request. Please check your input.');
          break;
        case 401:
          toast.error('Session expired. Please log in again.');
          // Redirect to login will be handled by the axios interceptor
          break;
        case 403:
          toast.error('You do not have permission to perform this action.');
          break;
        case 404:
          toast.error('The requested resource was not found.');
          break;
        case 429:
          toast.error('Too many requests. Please try again later.');
          break;
        case 500:
          toast.error('Server error. Please try again later.');
          break;
        default:
          toast.error((data as any)?.message || defaultMessage);
      }
    } 
    // Request was made but no response was received
    else if (request) {
      toast.error('No response from server. Please check your connection.');
    } 
    // Something happened in setting up the request
    else {
      toast.error(message || defaultMessage);
    }
  }
  // Handle standard JavaScript errors
  else if (error instanceof Error) {
    toast.error(error.message || defaultMessage);
  }
  // Handle any other type of error
  else {
    toast.error(defaultMessage);
  }
};

/**
 * Type guard to check if an error is an Axios error
 */
const isAxiosError = (error: unknown): error is AxiosError => {
  return (error as AxiosError).isAxiosError === true;
};

/**
 * Creates a function that wraps an async function with error handling
 * @param asyncFunction The async function to wrap
 * @param errorMessage Optional custom error message
 * @returns A function that calls the original function with error handling
 */
export const withErrorHandling = <T extends (...args: any[]) => Promise<any>>(
  asyncFunction: T,
  errorMessage?: string
) => {
  return async (...args: Parameters<T>): Promise<Awaited<ReturnType<T>> | undefined> => {
    try {
      return await asyncFunction(...args);
    } catch (error) {
      handleApiError(error, errorMessage);
      return undefined;
    }
  };
};

/**
 * Handles form submission errors and sets form errors if using react-hook-form
 * @param error The error object from Axios or a standard Error
 * @param setError Function from react-hook-form's useForm
 * @param defaultMessage Default error message to display if no specific message is available
 */
export const handleFormError = (
  error: unknown,
  setError?: (field: string, options: { type: string; message: string }) => void,
  defaultMessage = 'Please check your input and try again.'
): void => {
  console.error('Form Error:', error);
  
  if (isAxiosError(error)) {
    const { response } = error;
    
    // Handle validation errors (422 Unprocessable Entity)
    if (response?.status === 422 && (response.data as any)?.errors && setError) {
      // Set form errors for each field with validation errors
      Object.entries((response.data as any).errors).forEach(([field, messages]) => {
        const message = Array.isArray(messages) ? messages[0] : String(messages);
        setError(field, { type: 'manual', message });
      });
      return;
    }
  }
  
  // Fallback to generic error handling
  handleApiError(error, defaultMessage);
};
