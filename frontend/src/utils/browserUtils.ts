/**
 * Browser detection and feature utilities
 */

// Browser types
type Browser = 'Chrome' | 'Firefox' | 'Safari' | 'Edge' | 'Opera' | 'IE' | 'Unknown';
type OS = 'Windows' | 'MacOS' | 'iOS' | 'Android' | 'Linux' | 'Unknown';
type DeviceType = 'Mobile' | 'Tablet' | 'Desktop';

interface BrowserInfo {
  browser: Browser;
  version: string;
  os: OS;
  osVersion: string;
  deviceType: DeviceType;
  isMobile: boolean;
  isTablet: boolean;
  isDesktop: boolean;
  isTouchDevice: boolean;
}

/**
 * Detects browser information
 * @returns Browser information object
 */
export const detectBrowser = (): BrowserInfo => {
  const userAgent = navigator.userAgent;
  let browser: Browser = 'Unknown';
  let version = '';
  let os: OS = 'Unknown';
  let osVersion = '';
  let deviceType: DeviceType = 'Desktop';
  
  // Detect browser
  if (userAgent.indexOf('Firefox') > -1) {
    browser = 'Firefox';
    version = userAgent.match(/Firefox\/(\d+\.?\d*)/)?.[1] || '';
  } else if (userAgent.indexOf('SamsungBrowser') > -1) {
    browser = 'Chrome'; // Samsung browser is Chrome-based
    version = userAgent.match(/SamsungBrowser\/(\d+\.?\d*)/)?.[1] || '';
  } else if (userAgent.indexOf('Opera') > -1 || userAgent.indexOf('OPR') > -1) {
    browser = 'Opera';
    version = userAgent.match(/(?:Opera|OPR)\/(\d+\.?\d*)/)?.[1] || '';
  } else if (userAgent.indexOf('Trident') > -1) {
    browser = 'IE';
    const match = userAgent.match(/rv:(\d+\.?\d*)/);
    version = match ? match[1] : '';
  } else if (userAgent.indexOf('Edge') > -1) {
    browser = 'Edge';
    version = userAgent.match(/Edge\/(\d+\.?\d*)/)?.[1] || '';
  } else if (userAgent.indexOf('Chrome') > -1) {
    browser = 'Chrome';
    version = userAgent.match(/Chrome\/(\d+\.?\d*)/)?.[1] || '';
  } else if (userAgent.indexOf('Safari') > -1) {
    browser = 'Safari';
    version = userAgent.match(/Version\/(\d+\.?\d*)/)?.[1] || '';
  }
  
  // Detect OS
  if (userAgent.indexOf('Windows') > -1) {
    os = 'Windows';
    const match = userAgent.match(/Windows NT (\d+\.?\d*)/);
    osVersion = match ? match[1] : '';
  } else if (userAgent.indexOf('Mac') > -1) {
    os = 'MacOS';
    const match = userAgent.match(/Mac OS X (\d+[._]\d+)/);
    osVersion = match ? match[1].replace('_', '.') : '';
  } else if (userAgent.indexOf('iPhone') > -1 || userAgent.indexOf('iPad') > -1 || userAgent.indexOf('iPod') > -1) {
    os = 'iOS';
    const match = userAgent.match(/OS (\d+_\d+)/);
    osVersion = match ? match[1].replace('_', '.') : '';
  } else if (userAgent.indexOf('Android') > -1) {
    os = 'Android';
    const match = userAgent.match(/Android (\d+\.?\d*)/);
    osVersion = match ? match[1] : '';
  } else if (userAgent.indexOf('Linux') > -1) {
    os = 'Linux';
  }
  
  // Detect device type
  const isMobile = /Mobi|Android|iPhone|iPad|iPod|Windows Phone/i.test(userAgent);
  const isTablet = /iPad|Android(?!.*Mobile)|Tablet|Silk/i.test(userAgent);
  
  if (isTablet) {
    deviceType = 'Tablet';
  } else if (isMobile) {
    deviceType = 'Mobile';
  }
  
  // Check for touch support
  const isTouchDevice = 'ontouchstart' in window || 
    (navigator as any).maxTouchPoints > 0 || 
    (navigator as any).msMaxTouchPoints > 0;
  
  return {
    browser,
    version,
    os,
    osVersion,
    deviceType,
    isMobile,
    isTablet,
    isDesktop: !isMobile && !isTablet,
    isTouchDevice,
  };
};

/**
 * Checks if the browser is online
 * @returns True if the browser is online
 */
export const isOnline = (): boolean => {
  return navigator.onLine;
};

/**
 * Gets the current scroll position
 * @returns The scroll position { x, y }
 */
export const getScrollPosition = (): { x: number; y: number } => ({
  x: window.pageXOffset,
  y: window.pageYOffset,
});

/**
 * Gets the viewport dimensions
 * @returns The viewport dimensions { width, height }
 */
export const getViewportSize = (): { width: number; height: number } => ({
  width: window.innerWidth,
  height: window.innerHeight,
});

/**
 * Checks if an element is in the viewport
 * @param element - The DOM element to check
 * @param offset - Optional offset in pixels
 * @returns True if the element is in the viewport
 */
export const isInViewport = (element: HTMLElement, offset: number = 0): boolean => {
  if (!element) return false;
  
  const rect = element.getBoundingClientRect();
  const viewport = getViewportSize();
  
  return (
    rect.top >= -offset &&
    rect.left >= -offset &&
    rect.bottom <= viewport.height + offset &&
    rect.right <= viewport.width + offset
  );
};

/**
 * Scrolls to an element with smooth animation
 * @param element - The DOM element or selector to scroll to
 * @param options - Scroll options
 */
export const scrollToElement = (
  element: HTMLElement | string,
  options: {
    offset?: number;
    behavior?: 'auto' | 'smooth';
    block?: 'start' | 'center' | 'end' | 'nearest';
    inline?: 'start' | 'center' | 'end' | 'nearest';
  } = {}
): void => {
  const {
    offset = 0,
    behavior = 'smooth',
    block = 'start',
    inline = 'nearest',
  } = options;
  
  let target: HTMLElement | null = null;
  
  if (typeof element === 'string') {
    target = document.querySelector(element);
  } else {
    target = element;
  }
  
  if (!target) return;
  
  const elementPosition = target.getBoundingClientRect().top + window.pageYOffset;
  const offsetPosition = elementPosition + offset;
  
  window.scrollTo({
    top: offsetPosition,
    behavior,
  });
  
  // Fallback for browsers that don't support scroll options
  if (behavior === 'smooth' && !('scrollBehavior' in document.documentElement.style)) {
    target.scrollIntoView({
      block,
      inline,
      behavior: 'smooth',
    });
  }
};

/**
 * Copies text to the clipboard
 * @param text - The text to copy
 * @returns A promise that resolves when the text is copied
 */
export const copyToClipboard = async (text: string): Promise<boolean> => {
  try {
    if (navigator.clipboard) {
      await navigator.clipboard.writeText(text);
      return true;
    } else {
      // Fallback for older browsers
      const textarea = document.createElement('textarea');
      textarea.value = text;
      textarea.style.position = 'fixed';
      document.body.appendChild(textarea);
      textarea.select();
      
      const success = document.execCommand('copy');
      document.body.removeChild(textarea);
      
      return success;
    }
  } catch (error) {
    console.error('Failed to copy text:', error);
    return false;
  }
};

/**
 * Downloads a file
 * @param data - The file data (URL, Blob, or text)
 * @param filename - The filename
 * @param type - The MIME type
 */
export const downloadFile = (
  data: string | Blob,
  filename: string,
  type: string = 'application/octet-stream'
): void => {
  let blob: Blob;
  
  if (typeof data === 'string') {
    if (data.startsWith('data:')) {
      // Data URL
      const link = document.createElement('a');
      link.href = data;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      return;
    } else {
      // Plain text
      blob = new Blob([data], { type });
    }
  } else {
    blob = data;
  }
  
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  
  // Clean up
  setTimeout(() => {
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }, 0);
};

/**
 * Opens a file selection dialog
 * @param options - File input options
 * @returns A promise that resolves with the selected files
 */
export const selectFiles = (options: {
  accept?: string;
  multiple?: boolean;
  capture?: 'user' | 'environment';
} = {}): Promise<FileList> => {
  return new Promise((resolve) => {
    const input = document.createElement('input');
    input.type = 'file';
    
    if (options.accept) input.accept = options.accept;
    if (options.multiple) input.multiple = true;
    if (options.capture) input.capture = options.capture;
    
    input.onchange = () => {
      if (input.files && input.files.length > 0) {
        resolve(input.files!);
      }
    };
    
    input.click();
  });
};

/**
 * Reads a file as text
 * @param file - The file to read
 * @returns A promise that resolves with the file content
 */
export const readFileAsText = (file: File): Promise<string> => {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    
    reader.onload = (event) => {
      resolve(event.target?.result as string);
    };
    
    reader.onerror = (error) => {
      reject(error);
    };
    
    reader.readAsText(file);
  });
};

/**
 * Reads a file as a data URL
 * @param file - The file to read
 * @returns A promise that resolves with the data URL
 */
export const readFileAsDataURL = (file: File): Promise<string> => {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    
    reader.onload = (event) => {
      resolve(event.target?.result as string);
    };
    
    reader.onerror = (error) => {
      reject(error);
    };
    
    reader.readAsDataURL(file);
  });
};

/**
 * Checks if a CSS feature is supported
 * @param property - The CSS property to check
 * @param value - The CSS value to check
 * @returns True if the feature is supported
 */
export const isCssSupported = (property: string, value?: string): boolean => {
  if (typeof window === 'undefined') return false;
  
  const element = document.createElement('div');
  const style = element.style as any;
  
  if (value === undefined) {
    return property in style;
  }
  
  style[property] = value;
  return !!style[property];
};

/**
 * Adds a CSS class to the document root
 * @param className - The class name to add
 */
export const addRootClass = (className: string): void => {
  document.documentElement.classList.add(className);
};

/**
 * Removes a CSS class from the document root
 * @param className - The class name to remove
 */
export const removeRootClass = (className: string): void => {
  document.documentElement.classList.remove(className);
};

/**
 * Toggles a CSS class on the document root
 * @param className - The class name to toggle
 * @param force - Force add or remove the class
 */
export const toggleRootClass = (className: string, force?: boolean): void => {
  document.documentElement.classList.toggle(className, force);
};
