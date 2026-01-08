import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'd2i3hjw4ptoo7c.cloudfront.net',
        pathname: '/nus/dataset_91qIDxbbk340bcKdW/**',
      },
    ],
  },
};

export default nextConfig;
