/**
 * Deeply clones an object or array
 * @param obj - The object to clone
 * @returns A deep clone of the object
 */
export const deepClone = <T>(obj: T): T => {
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }
  
  if (obj instanceof Date) {
    return new Date(obj.getTime()) as unknown as T;
  }
  
  if (Array.isArray(obj)) {
    return obj.map(item => deepClone(item)) as unknown as T;
  }
  
  if (obj instanceof Map) {
    const result = new Map();
    obj.forEach((value, key) => {
      result.set(key, deepClone(value));
    });
    return result as unknown as T;
  }
  
  if (obj instanceof Set) {
    const result = new Set();
    obj.forEach(value => {
      result.add(deepClone(value));
    });
    return result as unknown as T;
  }
  
  const result: Record<string, any> = {};
  
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      result[key] = deepClone((obj as any)[key]);
    }
  }
  
  return result as T;
};

/**
 * Deeply merges multiple objects
 * @param target - The target object
 * @param sources - The source objects to merge
 * @returns A new merged object
 */
export const deepMerge = <T extends Record<string, any>>(target: T, ...sources: Partial<T>[]): T => {
  if (!sources.length) return deepClone(target);
  
  const result = deepClone(target);
  
  for (const source of sources) {
    if (!source || typeof source !== 'object') continue;
    
    for (const key in source) {
      if (!Object.prototype.hasOwnProperty.call(source, key)) continue;
      
      const targetValue = result[key];
      const sourceValue = source[key];
      
      if (Array.isArray(targetValue) && Array.isArray(sourceValue)) {
        result[key] = [...targetValue, ...sourceValue] as any;
      } else if (
        targetValue && 
        sourceValue && 
        typeof targetValue === 'object' && 
        typeof sourceValue === 'object' &&
        !(targetValue as any instanceof Date) &&
        !(sourceValue as any instanceof Date)
      ) {
        result[key] = deepMerge(targetValue, sourceValue) as any;
      } else {
        result[key] = sourceValue as any;
      }
    }
  }
  
  return result;
};

/**
 * Checks if two values are deeply equal
 * @param a - The first value
 * @param b - The second value
 * @returns True if the values are deeply equal
 */
export const deepEqual = (a: any, b: any): boolean => {
  // Primitive types and functions
  if (a === b) return true;
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') {
    return a !== a && b !== b; // Handles NaN
  }
  
  // Dates
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime();
  }
  
  // Arrays
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i])) return false;
    }
    
    return true;
  }
  
  // Maps
  if (a instanceof Map && b instanceof Map) {
    if (a.size !== b.size) return false;
    
    for (const [key, value] of a) {
      if (!b.has(key) || !deepEqual(value, b.get(key))) return false;
    }
    
    return true;
  }
  
  // Sets
  if (a instanceof Set && b instanceof Set) {
    if (a.size !== b.size) return false;
    
    const aArray = Array.from(a);
    const bArray = Array.from(b);
    
    for (let i = 0; i < aArray.length; i++) {
      if (!deepEqual(aArray[i], bArray[i])) return false;
    }
    
    return true;
  }
  
  // Objects
  const aKeys = Object.keys(a);
  const bKeys = Object.keys(b);
  
  if (aKeys.length !== bKeys.length) return false;
  
  for (const key of aKeys) {
    if (!bKeys.includes(key) || !deepEqual(a[key], b[key])) {
      return false;
    }
  }
  
  return true;
};

/**
 * Gets a nested property from an object using a dot-notation path
 * @param obj - The object to get the property from
 * @param path - The dot-notation path to the property
 * @param defaultValue - The default value to return if the property doesn't exist
 * @returns The value of the property or the default value
 */
export const get = <T>(
  obj: Record<string, any> | undefined | null,
  path: string,
  defaultValue?: T
): T | undefined => {
  if (!obj || typeof obj !== 'object') return defaultValue;
  
  const keys = path.split('.');
  let result: any = obj;
  
  for (const key of keys) {
    if (result === null || result === undefined) {
      return defaultValue;
    }
    
    result = result[key];
    
    if (result === undefined) {
      return defaultValue;
    }
  }
  
  return result === undefined ? defaultValue : result;
};

/**
 * Sets a nested property on an object using a dot-notation path
 * @param obj - The object to set the property on
 * @param path - The dot-notation path to the property
 * @param value - The value to set
 * @returns A new object with the updated property
 */
export const set = <T extends Record<string, any>>(
  obj: T,
  path: string,
  value: any
): T => {
  if (!path) return { ...obj, ...value };
  
  const keys = path.split('.');
  const newObj = Array.isArray(obj) ? [...obj] : { ...obj };
  
  let current: any = newObj;
  
  for (let i = 0; i < keys.length - 1; i++) {
    const key = keys[i];
    
    if (current[key] === undefined || current[key] === null) {
      current[key] = {};
    } else if (Array.isArray(current[key])) {
      current[key] = [...current[key]];
    } else if (typeof current[key] === 'object') {
      current[key] = { ...current[key] };
    } else {
      current[key] = {};
    }
    
    current = current[key];
  }
  
  current[keys[keys.length - 1]] = value;
  return newObj as T;
};

/**
 * Omits specified properties from an object
 * @param obj - The source object
 * @param keys - The keys to omit
 * @returns A new object without the specified keys
 */
export const omit = <T extends Record<string, any>, K extends keyof T>(
  obj: T,
  keys: K[]
): Omit<T, K> => {
  const result = { ...obj };
  
  for (const key of keys) {
    delete result[key];
  }
  
  return result;
};

/**
 * Picks specified properties from an object
 * @param obj - The source object
 * @param keys - The keys to pick
 * @returns A new object with only the specified keys
 */
export const pick = <T extends Record<string, any>, K extends keyof T>(
  obj: T,
  keys: K[]
): Pick<T, K> => {
  const result = {} as Pick<T, K>;
  
  for (const key of keys) {
    if (key in obj) {
      result[key] = obj[key];
    }
  }
  
  return result;
};

/**
 * Inverts the keys and values of an object
 * @param obj - The object to invert
 * @returns A new object with inverted keys and values
 */
export const invert = <T extends Record<string, any>>(obj: T): Record<string, string> => {
  const result: Record<string, string> = {};
  
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      result[String(obj[key])] = key;
    }
  }
  
  return result;
};

/**
 * Maps over the values of an object
 * @param obj - The source object
 * @param fn - The mapping function
 * @returns A new object with mapped values
 */
export const mapValues = <
  T extends Record<string, any>,
  R
>(
  obj: T,
  fn: (value: T[keyof T], key: keyof T, obj: T) => R
): Record<keyof T, R> => {
  const result = {} as Record<keyof T, R>;
  
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      result[key] = fn(obj[key], key, obj);
    }
  }
  
  return result;
};

/**
 * Maps over the keys of an object
 * @param obj - The source object
 * @param fn - The mapping function
 * @returns A new object with mapped keys
 */
export const mapKeys = <
  T extends Record<string, any>,
  K extends string | number | symbol
>(
  obj: T,
  fn: (value: T[keyof T], key: keyof T, obj: T) => K
): Record<K, T[keyof T]> => {
  const result = {} as Record<K, T[keyof T]>;
  
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      const newKey = fn(obj[key], key, obj);
      result[newKey] = obj[key];
    }
  }
  
  return result;
};

/**
 * Filters an object's entries based on a predicate function
 * @param obj - The source object
 * @param predicate - The filter function
 * @returns A new object with filtered entries
 */
export const filter = <T extends Record<string, any>>(
  obj: T,
  predicate: (value: T[keyof T], key: keyof T, obj: T) => boolean
): Partial<T> => {
  const result: Partial<T> = {};
  
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key) && predicate(obj[key], key, obj)) {
      result[key] = obj[key];
    }
  }
  
  return result;
};

/**
 * Creates an object from an array of key-value pairs
 * @param entries - The key-value pairs
 * @returns A new object
 */
export const fromEntries = <K extends string | number | symbol, V>(
  entries: [K, V][]
): Record<K, V> => {
  return entries.reduce((acc, [key, value]) => {
    acc[key] = value;
    return acc;
  }, {} as Record<K, V>);
};

/**
 * Converts an object to an array of key-value pairs
 * @param obj - The source object
 * @returns An array of key-value pairs
 */
export const entries = <T extends Record<string, any>>(
  obj: T
): [keyof T, T[keyof T]][] => {
  return Object.entries(obj) as [keyof T, T[keyof T]][];
};

/**
 * Checks if an object is empty
 * @param obj - The object to check
 * @returns True if the object has no own enumerable properties
 */
export const isEmpty = (obj: Record<string, any>): boolean => {
  if (!obj) return true;
  return Object.keys(obj).length === 0;
};

/**
 * Gets the size of an object (number of own enumerable properties)
 * @param obj - The object to check
 * @returns The number of own enumerable properties
 */
export const size = (obj: Record<string, any>): number => {
  if (!obj) return 0;
  return Object.keys(obj).length;
};

/**
 * Creates a new object with the same keys but all values set to a default value
 * @param obj - The source object
 * @param defaultValue - The default value
 * @returns A new object with default values
 */
export const mapToDefault = <T extends Record<string, any>, V>(
  obj: T,
  defaultValue: V
): Record<keyof T, V> => {
  const result = {} as Record<keyof T, V>;
  
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      result[key] = defaultValue;
    }
  }
  
  return result;
};
