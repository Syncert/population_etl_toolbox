// The Content-Security-Policy is not set here. It carries a per-request nonce
// so that `script-src` can forbid inline scripts without forbidding Next's
// own bootstrap, and a per-request value has to be produced per request --
// see `middleware.ts`, which owns the policy. The static headers below apply
// to every response and do not vary.
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
