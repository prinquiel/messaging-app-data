/** @type {import('tailwindcss').Config} */
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["'Inter'", "ui-sans-serif", "system-ui"],
      },
      colors: {
        brand: {
          50: "#eef2ff",
          100: "#e0e7ff",
          200: "#c7d2fe",
          500: "#6366f1",
          600: "#4f46e5",
          700: "#4338ca",
        },
        night: "#0f172a",
        slate: {
          950: "#020617",
        },
      },
      boxShadow: {
        glass: "0 25px 80px rgba(15, 23, 42, 0.45)",
      },
      backdropBlur: {
        xs: "2px",
      },
      animation: {
        "fade-in-up": "fadeInUp 0.35s ease forwards",
        "scale-in": "scaleIn 0.3s ease forwards",
      },
      keyframes: {
        fadeInUp: {
          "0%": { opacity: 0, transform: "translateY(8px)" },
          "100%": { opacity: 1, transform: "translateY(0)" },
        },
        scaleIn: {
          "0%": { opacity: 0, transform: "scale(0.95)" },
          "100%": { opacity: 1, transform: "scale(1)" },
        },
      },
    },
  },
  plugins: [],
}


