/**
 * Storage types
 */
type StorageType = 'local' | 'session';

/**
 * Storage item with expiration
 */
interface StorageItem<T> {
  value: T;
  expiresAt?: number; // Unix timestamp in milliseconds
}

/**
 * Gets the storage object based on type
 * @param type - The storage type ('local' or 'session')
 * @returns The storage object
 */
const getStorage = (type: StorageType): Storage => {
  return type === 'local' ? window.localStorage : window.sessionStorage;
};

/**
 * Checks if storage is available
 * @param type - The storage type ('local' or 'session')
 * @returns True if storage is available
 */
export const isStorageAvailable = (type: StorageType): boolean => {
  try {
    const storage = getStorage(type);
    const testKey = '__test__';
    
    storage.setItem(testKey, testKey);
    storage.removeItem(testKey);
    
    return true;
  } catch (e) {
    return false;
  }
};

/**
 * Gets an item from storage
 * @param key - The storage key
 * @param type - The storage type ('local' or 'session')
 * @param defaultValue - The default value to return if the item doesn't exist or is expired
 * @returns The stored value or the default value
 */
export const getItem = <T>(
  key: string,
  type: StorageType = 'local',
  defaultValue: T | null = null
): T | null => {
  try {
    const storage = getStorage(type);
    const item = storage.getItem(key);
    
    if (!item) return defaultValue;
    
    const parsedItem: StorageItem<T> = JSON.parse(item);
    
    // Check if the item has expired
    if (parsedItem.expiresAt && parsedItem.expiresAt < Date.now()) {
      // Remove expired item
      storage.removeItem(key);
      return defaultValue;
    }
    
    return parsedItem.value;
  } catch (error) {
    console.error(`Error getting item from ${type}Storage:`, error);
    return defaultValue;
  }
};

/**
 * Sets an item in storage
 * @param key - The storage key
 * @param value - The value to store
 * @param options - Storage options
 * @param options.type - The storage type ('local' or 'session')
 * @param options.ttl - Time to live in milliseconds
 * @returns True if the operation was successful
 */
export const setItem = <T>(
  key: string,
  value: T,
  options: {
    type?: StorageType;
    ttl?: number; // Time to live in milliseconds
  } = {}
): boolean => {
  try {
    const { type = 'local', ttl } = options;
    const storage = getStorage(type);
    
    const item: StorageItem<T> = {
      value,
      ...(ttl && { expiresAt: Date.now() + ttl }),
    };
    
    storage.setItem(key, JSON.stringify(item));
    return true;
  } catch (error) {
    console.error(`Error setting item in ${options.type || 'local'}Storage:`, error);
    return false;
  }
};

/**
 * Removes an item from storage
 * @param key - The storage key
 * @param type - The storage type ('local' or 'session')
 * @returns True if the operation was successful
 */
export const removeItem = (key: string, type: StorageType = 'local'): boolean => {
  try {
    const storage = getStorage(type);
    storage.removeItem(key);
    return true;
  } catch (error) {
    console.error(`Error removing item from ${type}Storage:`, error);
    return false;
  }
};

/**
 * Clears all items from storage
 * @param type - The storage type ('local' or 'session')
 * @returns True if the operation was successful
 */
export const clear = (type: StorageType = 'local'): boolean => {
  try {
    const storage = getStorage(type);
    storage.clear();
    return true;
  } catch (error) {
    console.error(`Error clearing ${type}Storage:`, error);
    return false;
  }
};

/**
 * Gets all keys from storage
 * @param type - The storage type ('local' or 'session')
 * @returns An array of keys
 */
export const getKeys = (type: StorageType = 'local'): string[] => {
  try {
    const storage = getStorage(type);
    return Object.keys(storage);
  } catch (error) {
    console.error(`Error getting keys from ${type}Storage:`, error);
    return [];
  }
};

/**
 * Gets all items from storage
 * @param type - The storage type ('local' or 'session')
 * @returns An object with all key-value pairs
 */
export const getAllItems = <T>(
  type: StorageType = 'local'
): Record<string, T> => {
  try {
    const storage = getStorage(type);
    const result: Record<string, T> = {};
    
    for (let i = 0; i < storage.length; i++) {
      const key = storage.key(i);
      if (key) {
        const value = getItem<T>(key, type);
        if (value !== null) {
          result[key] = value;
        }
      }
    }
    
    return result;
  } catch (error) {
    console.error(`Error getting all items from ${type}Storage:`, error);
    return {};
  }
};

/**
 * Clears expired items from storage
 * @param type - The storage type ('local' or 'session')
 * @returns The number of items removed
 */
export const clearExpired = (type: StorageType = 'local'): number => {
  try {
    const storage = getStorage(type);
    let count = 0;
    
    for (let i = 0; i < storage.length; i++) {
      const key = storage.key(i);
      if (key) {
        const item = storage.getItem(key);
        if (item) {
          try {
            const parsedItem = JSON.parse(item) as StorageItem<unknown>;
            if (parsedItem.expiresAt && parsedItem.expiresAt < Date.now()) {
              storage.removeItem(key);
              count++;
            }
          } catch (e) {
            // If the item is not in the expected format, skip it
            continue;
          }
        }
      }
    }
    
    return count;
  } catch (error) {
    console.error(`Error clearing expired items from ${type}Storage:`, error);
    return 0;
  }
};

/**
 * Storage wrapper with a simpler API
 */
export const storage = {
  /**
   * Gets an item from local storage
   */
  get: <T>(key: string, defaultValue: T | null = null): T | null => 
    getItem(key, 'local', defaultValue),
  
  /**
   * Sets an item in local storage
   */
  set: <T>(
    key: string, 
    value: T, 
    ttl?: number
  ): boolean => setItem(key, value, { type: 'local', ttl }),
  
  /**
   * Removes an item from local storage
   */
  remove: (key: string): boolean => removeItem(key, 'local'),
  
  /**
   * Clears all items from local storage
   */
  clear: (): boolean => clear('local'),
  
  /**
   * Gets all keys from local storage
   */
  keys: (): string[] => getKeys('local'),
  
  /**
   * Gets all items from local storage
   */
  getAll: <T>(): Record<string, T> => getAllItems<T>('local'),
  
  /**
   * Clears expired items from local storage
   */
  clearExpired: (): number => clearExpired('local'),
};

/**
 * Session storage wrapper with a simpler API
 */
export const session = {
  /**
   * Gets an item from session storage
   */
  get: <T>(key: string, defaultValue: T | null = null): T | null => 
    getItem(key, 'session', defaultValue),
  
  /**
   * Sets an item in session storage
   */
  set: <T>(
    key: string, 
    value: T, 
    ttl?: number
  ): boolean => setItem(key, value, { type: 'session', ttl }),
  
  /**
   * Removes an item from session storage
   */
  remove: (key: string): boolean => removeItem(key, 'session'),
  
  /**
   * Clears all items from session storage
   */
  clear: (): boolean => clear('session'),
  
  /**
   * Gets all keys from session storage
   */
  keys: (): string[] => getKeys('session'),
  
  /**
   * Gets all items from session storage
   */
  getAll: <T>(): Record<string, T> => getAllItems<T>('session'),
  
  /**
   * Clears expired items from session storage
   */
  clearExpired: (): number => clearExpired('session'),
};
