import { useState, useCallback, useRef } from 'react';
import { AxiosRequestConfig, AxiosResponse } from 'axios';
import api, { apiRequest } from '../api/api';
import { handleApiError } from '../utils/errorHandler';

type HttpMethod = 'get' | 'post' | 'put' | 'delete' | 'patch';

interface UseApiOptions<T> {
  /** Initial data value */
  initialData?: T;
  
  /** Callback when the request is successful */
  onSuccess?: (data: T) => void;
  
  /** Callback when the request fails */
  onError?: (error: Error) => void;
  
  /** Callback when the request completes (success or error) */
  onComplete?: () => void;
  
  /** Whether to execute the request immediately */
  immediate?: boolean;
  
  /** Additional Axios request config */
  config?: Omit<AxiosRequestConfig, 'method' | 'url' | 'data'>;
}

interface UseApiReturn<T> {
  /** The response data */
  data: T | null;
  
  /** Error object if the request failed */
  error: Error | null;
  
  /** Whether the request is in progress */
  loading: boolean;
  
  /** Whether the request has been executed */
  executed: boolean;
  
  /** Execute the request */
  execute: (requestData?: any) => Promise<T | null>;
  
  /** Reset the hook state */
  reset: () => void;
  
  /** Manually set the data */
  setData: (data: T | null) => void;
  
  /** Manually set the error */
  setError: (error: Error | null) => void;
}

/**
 * Custom hook for making API requests with loading and error states
 * @param method HTTP method (get, post, put, delete, patch)
 * @param url API endpoint URL
 * @param options Hook options
 */
function useApi<T = any>(
  method: HttpMethod,
  url: string,
  options: UseApiOptions<T> = {}
): UseApiReturn<T> {
  const {
    initialData = null,
    onSuccess,
    onError,
    onComplete,
    immediate = false,
    config = {},
  } = options;
  
  const [data, setData] = useState<T | null>(initialData);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [executed, setExecuted] = useState<boolean>(false);
  const controllerRef = useRef<AbortController | null>(null);
  
  // Create a stable reference to the execute function
  const execute = useCallback(async (requestData?: any): Promise<T | null> => {
    // Cancel any ongoing request
    if (controllerRef.current) {
      controllerRef.current.abort();
    }
    
    // Create a new AbortController for this request
    const controller = new AbortController();
    controllerRef.current = controller;
    
    setLoading(true);
    setError(null);
    
    try {
      const response: AxiosResponse<T> = await api({
        method,
        url,
        data: requestData,
        signal: controller.signal,
        ...config,
      });
      
      setData(response.data);
      setExecuted(true);
      onSuccess?.(response.data);
      
      return response.data;
    } catch (err) {
      // Ignore aborted requests
      if (err.name === 'AbortError') {
        return null;
      }
      
      const error = err as Error;
      setError(error);
      setExecuted(true);
      onError?.(error);
      
      // Use the error handler to show user-friendly messages
      handleApiError(error);
      
      return null;
    } finally {
      // Only reset loading state if this is the most recent request
      if (controllerRef.current === controller) {
        setLoading(false);
        controllerRef.current = null;
      }
      
      onComplete?.();
    }
  }, [method, url, config, onSuccess, onError, onComplete]);
  
  // Execute the request immediately if requested
  const initialRequest = useRef(immediate);
  if (initialRequest.current) {
    initialRequest.current = false;
    execute();
  }
  
  // Reset the hook state
  const reset = useCallback(() => {
    // Cancel any ongoing request
    if (controllerRef.current) {
      controllerRef.current.abort();
      controllerRef.current = null;
    }
    
    setData(initialData);
    setError(null);
    setLoading(false);
    setExecuted(false);
  }, [initialData]);
  
  return {
    data,
    error,
    loading,
    executed,
    execute,
    reset,
    setData,
    setError,
  };
}

/**
 * Convenience hook for GET requests
 */
useApi.get = <T = any>(
  url: string,
  options?: Omit<UseApiOptions<T>, 'immediate'> & { immediate?: boolean }
) => useApi<T>('get', url, options);

/**
 * Convenience hook for POST requests
 */
useApi.post = <T = any>(
  url: string,
  options?: Omit<UseApiOptions<T>, 'immediate'> & { immediate?: boolean }
) => useApi<T>('post', url, options);

/**
 * Convenience hook for PUT requests
 */
useApi.put = <T = any>(
  url: string,
  options?: Omit<UseApiOptions<T>, 'immediate'> & { immediate?: boolean }
) => useApi<T>('put', url, options);

/**
 * Convenience hook for DELETE requests
 */
useApi.delete = <T = any>(
  url: string,
  options?: Omit<UseApiOptions<T>, 'immediate'> & { immediate?: boolean }
) => useApi<T>('delete', url, options);

/**
 * Convenience hook for PATCH requests
 */
useApi.patch = <T = any>(
  url: string,
  options?: Omit<UseApiOptions<T>, 'immediate'> & { immediate?: boolean }
) => useApi<T>('patch', url, options);

export default useApi;
