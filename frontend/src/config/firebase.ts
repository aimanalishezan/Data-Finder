/// <reference types="vite/client" />
// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";
import { getAuth, GoogleAuthProvider } from "firebase/auth";

// Your web app's Firebase configuration
// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
  apiKey: import.meta.env.VITE_FIREBASE_API_KEY || "AIzaSyCyr3YmfnX6AUQ7isuEQ84nSUFMqwYCJhc",
  authDomain: import.meta.env.VITE_FIREBASE_AUTH_DOMAIN || "companydatafetcher.firebaseapp.com",
  projectId: import.meta.env.VITE_FIREBASE_PROJECT_ID || "companydatafetcher",
  storageBucket: import.meta.env.VITE_FIREBASE_STORAGE_BUCKET || "companydatafetcher.firebasestorage.app",
  messagingSenderId: import.meta.env.VITE_FIREBASE_MESSAGING_SENDER_ID || "954681837320",
  appId: import.meta.env.VITE_FIREBASE_APP_ID || "1:954681837320:web:ec885b225f649f30168ebd",
  measurementId: import.meta.env.VITE_FIREBASE_MEASUREMENT_ID || "G-PW4WC3VXFN"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);

// Initialize Firebase Authentication and get a reference to the service
export const auth = getAuth(app);

// Initialize Google Auth Provider
export const googleProvider = new GoogleAuthProvider();

// Initialize Analytics (optional)
export const analytics = getAnalytics(app);

export default app;
