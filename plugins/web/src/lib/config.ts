const raw = import.meta.env.VITE_BASE_PATH || "/trex";
export const BASE_PATH = raw.endsWith("/") ? raw.slice(0, -1) : raw;

// Vite's `base` config sets import.meta.env.BASE_URL automatically
export const UI_BASE_PATH = import.meta.env.BASE_URL.replace(/\/$/, "");
