// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightImageZoom from "starlight-image-zoom";
import starlightLlmsTxt from "starlight-llms-txt";

export default defineConfig({
  site: "https://batary.dev",
  base: "/surefire",
  integrations: [
    starlight({
      title: "Surefire",
      logo: { src: "./src/assets/surefire.svg" },
      description:
        "Distributed job scheduling for .NET with a minimal API style",
      plugins: [starlightImageZoom(), starlightLlmsTxt()],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/sgbj/surefire",
        },
      ],
      customCss: ["./src/styles/custom.css"],
      expressiveCode: {
        styleOverrides: { borderRadius: "0.625rem" },
      },
      sidebar: [
        {
          label: "Getting started",
          items: [
            { label: "Installation", slug: "getting-started/installation" },
            { label: "Configuration", slug: "getting-started/configuration" },
          ],
        },
        {
          label: "Concepts",
          items: [
            { label: "Architecture", slug: "concepts/architecture" },
            { label: "Job lifecycle", slug: "concepts/job-lifecycle" },
            { label: "Queues", slug: "concepts/queues" },
          ],
        },
        {
          label: "Guides",
          items: [
            { label: "Jobs", slug: "guides/jobs" },
            { label: "Scheduling", slug: "guides/scheduling" },
            { label: "Triggering and running", slug: "guides/triggering" },
            { label: "Streaming", slug: "guides/streaming" },
            { label: "Rate limiting", slug: "guides/rate-limiting" },
            { label: "Cancellation", slug: "guides/cancellation" },
            { label: "Filters", slug: "guides/filters" },
            { label: "Dashboard", slug: "guides/dashboard" },
            { label: "Metrics and tracing", slug: "guides/metrics" },
          ],
        },
        {
          label: "Storage providers",
          items: [
            { label: "PostgreSQL", slug: "storage/postgresql" },
            { label: "SQL Server", slug: "storage/sqlserver" },
            { label: "SQLite", slug: "storage/sqlite" },
            { label: "Redis", slug: "storage/redis" },
          ],
        },
        { label: "Roadmap", slug: "roadmap" },
      ],
    }),
  ],
});
