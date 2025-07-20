import React, { createContext, useContext, useEffect, useState, useMemo } from 'react';

type ThemeMode = 'light' | 'dark' | 'system';

interface ThemeContextType {
  /** Current theme mode */
  theme: ThemeMode;
  
  /** Whether the current theme is dark */
  isDark: boolean;
  
  /** Set the theme mode */
  setTheme: (theme: ThemeMode) => void;
  
  /** Toggle between light and dark themes */
  toggleTheme: () => void;
}

// Create context with a default value
const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

// Default theme
const DEFAULT_THEME: ThemeMode = 'system';

// Theme storage key
const THEME_STORAGE_KEY = 'app_theme';

/**
 * Theme provider component
 */
export const ThemeProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [theme, setThemeState] = useState<ThemeMode>(DEFAULT_THEME);
  const [isMounted, setIsMounted] = useState(false);
  
  // Get the system color scheme preference
  const systemPrefersDark = useMediaQuery('(prefers-color-scheme: dark)');
  
  // Determine if the current theme is dark
  const isDark = useMemo(() => {
    if (theme === 'system') {
      return systemPrefersDark;
    }
    return theme === 'dark';
  }, [theme, systemPrefersDark]);
  
  // Set the theme class on the document element
  useEffect(() => {
    if (isDark) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [isDark]);
  
  // Load saved theme from localStorage on mount
  useEffect(() => {
    try {
      const savedTheme = localStorage.getItem(THEME_STORAGE_KEY) as ThemeMode | null;
      if (savedTheme && ['light', 'dark', 'system'].includes(savedTheme)) {
        setThemeState(savedTheme);
      }
    } catch (error) {
      console.error('Failed to load theme from localStorage:', error);
    }
    
    setIsMounted(true);
  }, []);
  
  // Set theme and save to localStorage
  const setTheme = (newTheme: ThemeMode) => {
    try {
      setThemeState(newTheme);
      localStorage.setItem(THEME_STORAGE_KEY, newTheme);
    } catch (error) {
      console.error('Failed to save theme to localStorage:', error);
    }
  };
  
  // Toggle between light and dark themes
  const toggleTheme = () => {
    setTheme(theme === 'dark' ? 'light' : 'dark');
  };
  
  // Don't render the app until we've loaded the theme from localStorage
  if (!isMounted) {
    return null;
  }
  
  const value = {
    theme,
    isDark,
    setTheme,
    toggleTheme,
  };
  
  return (
    <ThemeContext.Provider value={value}>
      <div className={`min-h-screen transition-colors duration-200 ${isDark ? 'dark bg-gray-900 text-white' : 'bg-white text-gray-900'}`}>
        {children}
      </div>
    </ThemeContext.Provider>
  );
};

/**
 * Custom hook to use the theme context
 */
export const useTheme = (): ThemeContextType => {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
};

/**
 * Custom hook to listen for system color scheme changes
 */
function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState(false);
  
  useEffect(() => {
    const mediaQuery = window.matchMedia(query);
    
    // Set the initial value
    setMatches(mediaQuery.matches);
    
    // Create event listener
    const handler = (event: MediaQueryListEvent) => setMatches(event.matches);
    
    // Add listener for future changes
    mediaQuery.addEventListener('change', handler);
    
    // Clean up
    return () => mediaQuery.removeEventListener('change', handler);
  }, [query]);
  
  return matches;
}

export default ThemeContext;
