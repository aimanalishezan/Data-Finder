import axios, { AxiosRequestConfig, AxiosResponse } from 'axios';

// Create an axios instance with default config
const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:8000',
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add a request interceptor to include the auth token
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Add a response interceptor to handle common errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response) {
      // Handle specific HTTP status codes
      if (error.response.status === 401) {
        // Unauthorized - redirect to login
        localStorage.removeItem('token');
        window.location.href = '/login';
      }
      
      // Return a more user-friendly error message
      const message = error.response.data?.message || 'An error occurred';
      return Promise.reject(new Error(message));
    } else if (error.request) {
      // The request was made but no response was received
      return Promise.reject(new Error('No response from server. Please check your connection.'));
    } else {
      // Something happened in setting up the request
      return Promise.reject(error);
    }
  }
);

// Helper function to make API requests
export const apiRequest = async <T>(
  method: 'get' | 'post' | 'put' | 'delete',
  url: string,
  data?: any,
  config?: AxiosRequestConfig
): Promise<T> => {
  try {
    const response: AxiosResponse<T> = await api[method](url, data, config);
    return response.data;
  } catch (error) {
    console.error(`API ${method.toUpperCase()} ${url} failed:`, error);
    throw error;
  }
};

export default api;
