// The application loads only same-origin code and data: the API and the
// tile server are reached through this server's own /api and /tiles
// rewrites, so `connect-src 'self'` covers both. MapLibre builds its
// workers from blob URLs and decodes tiles into blob-backed images, which
// is why worker-src and img-src admit `blob:`.
//
// `'unsafe-inline'` for scripts is Next's inline bootstrap and flight
// payload; removing it needs a per-request nonce, which is a deliberate
// follow-on rather than something to switch on untested. `'unsafe-eval'`
// is development-only — the production bundle never needs it.
const isDevelopment = process.env.NODE_ENV === "development";

const contentSecurityPolicy = [
  "default-src 'self'",
  `script-src 'self' 'unsafe-inline'${isDevelopment ? " 'unsafe-eval'" : ""}`,
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' data: blob:",
  "font-src 'self' data:",
  "worker-src 'self' blob:",
  "connect-src 'self'",
  "object-src 'none'",
  "base-uri 'self'",
  "form-action 'self'",
  "frame-ancestors 'self'",
].join("; ");

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  distDir: process.env.NODE_ENV === 'development' ? '.next-dev' : '.next',
  async headers() {
    return [
      {
        source: "/:path*",
        headers: [
          { key: "X-Content-Type-Options", value: "nosniff" },
          { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
          { key: "Permissions-Policy", value: "camera=(), microphone=(), geolocation=()" },
          { key: "X-Frame-Options", value: "SAMEORIGIN" },
          { key: "Content-Security-Policy", value: contentSecurityPolicy },
          { key: "Cross-Origin-Opener-Policy", value: "same-origin" },
        ],
      },
    ];
  },
  async rewrites() {
    const apiOrigin =
      process.env.API_ORIGIN || process.env.NEXT_PUBLIC_API_ORIGIN || "http://localhost:8000";
    const tilesOrigin =
      process.env.TILES_ORIGIN || process.env.NEXT_PUBLIC_TILES_ORIGIN || "http://localhost:3000";

    return [
      {
        source: "/api/:path*",
        destination: `${apiOrigin}/api/:path*`,
      },
      {
        source: "/tiles/:path*",
        destination: `${tilesOrigin}/:path*`,
      },
    ];
  },
};

export default nextConfig;
