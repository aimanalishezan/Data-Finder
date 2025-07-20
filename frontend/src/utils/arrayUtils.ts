/**
 * Removes duplicate values from an array
 * @param array - The array to deduplicate
 * @returns A new array with only unique values
 */
export const unique = <T>(array: T[]): T[] => {
  return [...new Set(array)];
};

/**
 * Groups an array of objects by a specific key
 * @param array - The array to group
 * @param key - The key to group by (can be a function or a string key)
 * @returns An object with keys as group names and values as arrays of items
 */
export const groupBy = <T>(
  array: T[], 
  key: string | ((item: T) => string)
): Record<string, T[]> => {
  return array.reduce<Record<string, T[]>>((acc, item) => {
    const groupKey = typeof key === 'function' 
      ? key(item) 
      : String((item as any)[key]);
    
    if (!acc[groupKey]) {
      acc[groupKey] = [];
    }
    
    acc[groupKey].push(item);
    return acc;
  }, {});
};

/**
 * Sorts an array of objects by a specific key
 * @param array - The array to sort
 * @param key - The key to sort by (can be a function or a string key)
 * @param order - The sort order ('asc' or 'desc')
 * @returns A new sorted array
 */
export const sortBy = <T>(
  array: T[], 
  key: string | ((item: T) => any),
  order: 'asc' | 'desc' = 'asc'
): T[] => {
  return [...array].sort((a, b) => {
    const aValue = typeof key === 'function' ? key(a) : (a as any)[key];
    const bValue = typeof key === 'function' ? key(b) : (b as any)[key];
    
    if (aValue < bValue) return order === 'asc' ? -1 : 1;
    if (aValue > bValue) return order === 'asc' ? 1 : -1;
    return 0;
  });
};

/**
 * Flattens a nested array
 * @param array - The array to flatten
 * @returns A new flattened array
 */
export const flatten = <T>(array: any[]): T[] => {
  return array.reduce((acc, item) => 
    acc.concat(Array.isArray(item) ? flatten(item) : item), 
    [] as T[]
  );
};

/**
 * Creates an array of numbers from start to end (inclusive)
 * @param start - The start number
 * @param end - The end number
 * @param step - The step between numbers (default: 1)
 * @returns An array of numbers
 */
export const range = (start: number, end: number, step: number = 1): number[] => {
  const result: number[] = [];
  
  if (step === 0) {
    throw new Error('Step cannot be zero');
  }
  
  if (start < end && step > 0) {
    for (let i = start; i <= end; i += step) {
      result.push(i);
    }
  } else if (start > end && step < 0) {
    for (let i = start; i >= end; i += step) {
      result.push(i);
    }
  }
  
  return result;
};

/**
 * Chunks an array into smaller arrays of a specified size
 * @param array - The array to chunk
 * @param size - The size of each chunk
 * @returns An array of chunks
 */
export const chunk = <T>(array: T[], size: number): T[][] => {
  const result: T[][] = [];
  
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  
  return result;
};

/**
 * Shuffles an array using the Fisher-Yates algorithm
 * @param array - The array to shuffle
 * @returns A new shuffled array
 */
export const shuffle = <T>(array: T[]): T[] => {
  const result = [...array];
  
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  
  return result;
};

/**
 * Gets the intersection of multiple arrays
 * @param arrays - The arrays to intersect
 * @returns A new array with values that are present in all arrays
 */
export const intersection = <T>(...arrays: T[][]): T[] => {
  if (arrays.length === 0) return [];
  if (arrays.length === 1) return [...new Set(arrays[0])];
  
  return arrays.reduce((a, b) => {
    const setB = new Set(b);
    return a.filter(x => setB.has(x));
  }, [...new Set(arrays[0])]);
};

/**
 * Gets the difference between two arrays
 * @param array1 - The first array
 * @param array2 - The second array
 * @returns A new array with values that are in array1 but not in array2
 */
export const difference = <T>(array1: T[], array2: T[]): T[] => {
  const set2 = new Set(array2);
  return array1.filter(x => !set2.has(x));
};

/**
 * Gets the union of multiple arrays
 * @param arrays - The arrays to union
 * @returns A new array with all unique values from all arrays
 */
export const union = <T>(...arrays: T[][]): T[] => {
  return [...new Set(flatten(arrays) as T[])];
};

/**
 * Creates an array of key-value pairs from an object
 * @param obj - The object to convert
 * @returns An array of [key, value] pairs
 */
export const entries = <K extends string | number | symbol, V>(
  obj: Record<K, V>
): [K, V][] => {
  return Object.entries(obj) as [K, V][];
};

/**
 * Creates an object from an array of key-value pairs
 * @param entries - The key-value pairs
 * @returns An object with the key-value pairs
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
 * Maps over an object's entries and returns a new object
 * @param obj - The object to map over
 * @param fn - The mapping function
 * @returns A new object with mapped values
 */
export const mapValues = <K extends string | number | symbol, V, R>(
  obj: Record<K, V>,
  fn: (value: V, key: K, obj: Record<K, V>) => R
): Record<K, R> => {
  return fromEntries(
    Object.entries(obj).map(([key, value]) => [
      key as K,
      fn(value as V, key as K, obj)
    ]) as [K, R][]
  );
};

/**
 * Maps over an object's keys and returns a new object
 * @param obj - The object to map over
 * @param fn - The mapping function
 * @returns A new object with mapped keys
 */
export const mapKeys = <K extends string | number | symbol, V, R extends string | number | symbol>(
  obj: Record<K, V>,
  fn: (key: K, value: V, obj: Record<K, V>) => R
): Record<R, V> => {
  return fromEntries(
    Object.entries(obj).map(([key, value]) => [
      fn(key as K, value as V, obj),
      value
    ]) as [R, V][]
  );
};

/**
 * Picks specific properties from an object
 * @param obj - The source object
 * @param keys - The keys to pick
 * @returns A new object with only the picked properties
 */
export const pick = <T extends object, K extends keyof T>(
  obj: T,
  keys: K[]
): Pick<T, K> => {
  return keys.reduce((acc, key) => {
    if (key in obj) {
      acc[key] = obj[key];
    }
    return acc;
  }, {} as Pick<T, K>);
};

/**
 * Omits specific properties from an object
 * @param obj - The source object
 * @param keys - The keys to omit
 * @returns A new object without the omitted properties
 */
export const omit = <T extends object, K extends keyof T>(
  obj: T,
  keys: K[]
): Omit<T, K> => {
  const result = { ...obj };
  keys.forEach(key => {
    delete result[key];
  });
  return result;
};

/**
 * Gets the last element of an array
 * @param array - The array
 * @returns The last element or undefined if the array is empty
 */
export const last = <T>(array: T[]): T | undefined => {
  return array[array.length - 1];
};

/**
 * Gets the first element of an array
 * @param array - The array
 * @returns The first element or undefined if the array is empty
 */
export const first = <T>(array: T[]): T | undefined => {
  return array[0];
};

/**
 * Creates an array of a specified length filled with a value
 * @param length - The length of the array
 * @param value - The value to fill the array with
 * @returns A new array filled with the specified value
 */
export const fill = <T>(length: number, value: T): T[] => {
  return Array(length).fill(value);
};

/**
 * Creates an array of a specified length filled with the results of calling a function
 * @param length - The length of the array
 * @param fn - The function to call for each element
 * @returns A new array with the results of calling the function
 */
export const times = <T>(length: number, fn: (index: number) => T): T[] => {
  return Array.from({ length }, (_, i) => fn(i));
};
