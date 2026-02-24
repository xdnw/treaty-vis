import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--bg)",
        panel: "var(--panel)",
        text: "var(--text)",
        muted: "var(--muted)",
        accent: "var(--accent)",
        success: "var(--success)",
        danger: "var(--danger)",
        warning: "var(--warning)"
      },
      // fontFamily: {
      //   display: ["\"Space Grotesk\"", "sans-serif"],
      //   body: ["\"IBM Plex Sans\"", "sans-serif"]
      // },
      boxShadow: {
        panel: "0 10px 40px rgba(9, 16, 31, 0.15)"
      }
    }
  },
  plugins: []
} satisfies Config;
