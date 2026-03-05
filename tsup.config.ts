import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    cli: "src/cli.ts",
  },
  format: ["esm"],
  dts: {
    resolve: true,
    entry: { index: "src/index.ts" },
  },
  clean: true,
  sourcemap: true,
  splitting: false,
  treeshake: true,
});
