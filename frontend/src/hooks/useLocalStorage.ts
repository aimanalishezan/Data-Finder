import { useState, useEffect, useCallback } from 'react';

type SetValue<T> = (value: T | ((val: T) => T)) => void;

/**
 * Custom hook for handling localStorage with React state
 * @param key - The key to use in localStorage
 * @param initialValue - The initial value to use if no value exists in localStorage
 * @returns [storedValue, setValue] - The stored value and a function to update it
 */
function useLocalStorage<T>(
  key: string,
  initialValue: T
): [T, SetValue<T>] {
  // State to store our value
  // Pass initial state function to useState so logic is only executed once
  const [storedValue, setStoredValue] = useState<T>(() => {
    if (typeof window === 'undefined') {
      return initialValue;
    }

    try {
      // Get from local storage by key
      const item = window.localStorage.getItem(key);
      // Parse stored json or if none return initialValue
      return item ? JSON.parse(item) : initialValue;
    } catch (error) {
      // If error also return initialValue
      console.error(`Error reading localStorage key "${key}":`, error);
      return initialValue;
    }
  });

  // Return a wrapped version of useState's setter function that persists the new value to localStorage
  const setValue: SetValue<T> = useCallback((value) => {
    try {
      // Allow value to be a function so we have same API as useState
      const valueToStore = value instanceof Function ? value(storedValue) : value;
      
      // Save state
      setStoredValue(valueToStore);
      
      // Save to local storage
      if (typeof window !== 'undefined') {
        window.localStorage.setItem(key, JSON.stringify(valueToStore));
      }
    } catch (error) {
      console.error(`Error setting localStorage key "${key}":`, error);
    }
  }, [key, storedValue]);

  // Sync state between browser tabs
  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }

    const handleStorageChange = (event: StorageEvent) => {
      if (event.key === key && event.newValue !== null) {
        try {
          const newValue = JSON.parse(event.newValue);
          if (newValue !== storedValue) {
            setStoredValue(newValue);
          }
        } catch (error) {
          console.error(`Error parsing localStorage value for key "${key}":`, error);
        }
      }
    };

    // Listen for changes to this key in other tabs/windows
    window.addEventListener('storage', handleStorageChange);
    
    // Clean up
    return () => {
      window.removeEventListener('storage', handleStorageChange);
    };
  }, [key, storedValue]);

  return [storedValue, setValue];
}

export default useLocalStorage;
