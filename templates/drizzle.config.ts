import { defineConfig } from "drizzle-kit";
import { PROCESSED_SCHEMA } from "./schema.ts";

export default defineConfig({
  dialect: "postgresql",
  schema: "./schema.ts",
  out: "./drizzle",
  schemaFilter: [PROCESSED_SCHEMA],
  dbCredentials: {
    url: process.env.DATABASE_URL ?? "postgresql://localhost:5432/indexer",
  },
});
