import { useState, useCallback, ChangeEvent, FormEvent } from 'react';
import { handleFormError } from '../utils/errorHandler';

interface FormErrors<T> {
  [key: string]: string;
}

type FormTouched<T> = {
  [K in keyof T]?: boolean;
}

interface UseFormProps<T> {
  /** Initial form values */
  initialValues: T;
  
  /** Validation function that returns an object with error messages */
  validate?: (values: T) => Partial<Record<keyof T, string>>;
  
  /** Form submission handler */
  onSubmit: (values: T) => Promise<void> | void;
  
  /** Callback when form is successfully submitted */
  onSuccess?: () => void;
}

interface UseFormReturn<T> {
  // Form values
  values: T;
  
  // Form errors
  errors: FormErrors<T>;
  
  // Touched fields
  touched: FormTouched<T>;
  
  // Form submission state
  isSubmitting: boolean;
  
  // Form validation state
  isValid: boolean;
  
  // Handle input change
  handleChange: (e: ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => void;
  
  // Handle input blur
  handleBlur: (e: React.FocusEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => void;
  
  // Handle form submission
  handleSubmit: (e: FormEvent<HTMLFormElement>) => Promise<void>;
  
  // Manually set field value
  setFieldValue: <K extends keyof T>(field: K, value: T[K]) => void;
  
  // Manually set field error
  setFieldError: (field: keyof T, message: string) => void;
  
  // Manually set field touched
  setFieldTouched: (field: keyof T, isTouched?: boolean) => void;
  
  // Reset form to initial values
  resetForm: () => void;
}

/**
 * Custom hook for managing form state and validation
 */
export const useForm = <T extends Record<string, any>>({
  initialValues,
  validate,
  onSubmit,
  onSuccess,
}: UseFormProps<T>): UseFormReturn<T> => {
  const [values, setValues] = useState<T>(initialValues);
  const [errors, setErrors] = useState<FormErrors<T>>({});
  const [touched, setTouched] = useState<FormTouched<T>>({});
  const [isSubmitting, setIsSubmitting] = useState(false);
  
  // Validate form
  const validateForm = useCallback((valuesToValidate: T): boolean => {
    if (!validate) return true;
    
    const validationErrors = validate(valuesToValidate);
    const hasErrors = Object.keys(validationErrors).length > 0;
    
    setErrors(validationErrors as FormErrors<T>);
    return !hasErrors;
  }, [validate]);
  
  // Handle input change
  const handleChange = useCallback((e: ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => {
    const { name, value, type } = e.target;
    
    // Handle different input types
    let finalValue: any = value;
    
    if (type === 'number') {
      finalValue = value === '' ? '' : Number(value);
    } else if (type === 'checkbox') {
      finalValue = (e.target as HTMLInputElement).checked;
    }
    
    // Update the field value
    const newValues = {
      ...values,
      [name]: finalValue,
    };
    
    setValues(newValues);
    
    // Validate field if it's been touched
    if (touched[name as keyof T]) {
      validateForm(newValues);
    }
  }, [values, touched, validateForm]);
  
  // Handle input blur
  const handleBlur = useCallback((e: React.FocusEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => {
    const { name } = e.target;
    
    // Mark field as touched
    if (!touched[name as keyof T]) {
      setTouched(prev => ({
        ...prev,
        [name]: true,
      }));
    }
    
    // Validate field
    if (validate) {
      const validationErrors = validate(values);
      setErrors(prev => ({
        ...prev,
        [name]: validationErrors[name as keyof T] || '',
      }));
    }
  }, [touched, validate, values]);
  
  // Handle form submission
  const handleSubmit = useCallback(async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    
    // Mark all fields as touched
    const allTouched = Object.keys(values).reduce((acc, key) => ({
      ...acc,
      [key]: true,
    }), {} as FormTouched<T>);
    
    setTouched(allTouched);
    
    // Validate form
    const isValid = validateForm(values);
    
    if (!isValid) {
      return;
    }
    
    // Submit form
    try {
      setIsSubmitting(true);
      await onSubmit(values);
      onSuccess?.();
    } catch (error) {
      handleFormError(error, (field, options) => {
        setErrors(prev => ({
          ...prev,
          [field]: options.message,
        }));
      });
    } finally {
      setIsSubmitting(false);
    }
  }, [onSubmit, onSuccess, validateForm, values]);
  
  // Set field value manually
  const setFieldValue = useCallback(<K extends keyof T>(field: K, value: T[K]) => {
    setValues(prev => ({
      ...prev,
      [field]: value,
    }));
    
    // Validate field if it's been touched
    if (touched[field as keyof T] && validate) {
      const newValues = {
        ...values,
        [field]: value,
      };
      
      const validationErrors = validate(newValues);
      setErrors(prev => ({
        ...prev,
        [field]: validationErrors[field as keyof T] || '',
      }));
    }
  }, [touched, validate, values]);
  
  // Set field error manually
  const setFieldError = useCallback((field: keyof T, message: string) => {
    setErrors(prev => ({
      ...prev,
      [field]: message,
    }));
  }, []);
  
  // Set field touched manually
  const setFieldTouched = useCallback((field: keyof T, isTouched = true) => {
    setTouched(prev => ({
      ...prev,
      [field]: isTouched,
    }));
  }, []);
  
  // Reset form to initial values
  const resetForm = useCallback(() => {
    setValues(initialValues);
    setErrors({});
    setTouched({});
  }, [initialValues]);
  
  // Check if form is valid
  const isValid = useCallback(() => {
    if (!validate) return true;
    return Object.keys(validate(values) || {}).length === 0;
  }, [validate, values]);
  
  return {
    values,
    errors,
    touched,
    isSubmitting,
    isValid: isValid(),
    handleChange,
    handleBlur,
    handleSubmit,
    setFieldValue,
    setFieldError,
    setFieldTouched,
    resetForm,
  };
};

export default useForm;
