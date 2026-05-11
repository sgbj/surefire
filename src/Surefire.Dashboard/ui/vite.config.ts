import path from "path";
import {readFileSync} from "fs";
import {defineConfig} from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

/** Read VersionPrefix + VersionSuffix from the repo's Directory.Build.props
 *  so the UI footer always matches the canonical NuGet package version. */
function readDotnetVersion(): string {
  try {
    const xml = readFileSync(
      path.resolve(__dirname, "../../../Directory.Build.props"),
      "utf8",
    );
    const prefix = xml.match(/<VersionPrefix>([^<]+)<\/VersionPrefix>/)?.[1]?.trim();
    const suffix = xml.match(/<VersionSuffix>([^<]+)<\/VersionSuffix>/)?.[1]?.trim();
    if (!prefix) return "dev";
    return suffix ? `${prefix}-${suffix}` : prefix;
  } catch {
    return "dev";
  }
}

export default defineConfig(({command}) => ({
  plugins: [react(), tailwindcss()],
  base: command === "build" ? "./" : "/surefire/",
  define: {
    __APP_VERSION__: JSON.stringify(readDotnetVersion()),
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/surefire/api": "http://localhost:5000",
    },
  },
}));
