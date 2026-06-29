/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  distDir: process.env.NODE_ENV === 'development' ? '.next-dev' : '.next',
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
