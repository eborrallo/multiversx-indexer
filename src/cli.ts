#!/usr/bin/env bun
import { cpSync, existsSync, mkdirSync, readFileSync, watch, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import type { IndexerConfig } from "./config";
import { startIndexer } from "./runtime/pipeline";

const args = process.argv.slice(2);
const command = args[0];
const configPath = resolve(process.cwd(), args[1] ?? "indexer.config.ts");

function getTemplatesDir(): string {
  const pkgDir = dirname(import.meta.dirname); // parent of src/ = package root
  return join(pkgDir, "templates");
}

async function runInit(targetDir?: string): Promise<void> {
  const cwd = targetDir ? resolve(process.cwd(), targetDir) : process.cwd();
  const templatesDir = getTemplatesDir();

  if (!existsSync(templatesDir)) {
    console.error("Templates directory not found. Is multiverse-indexer installed correctly?");
    process.exit(1);
  }

  const files = [
    "indexer.config.ts",
    "schema.ts",
    "drizzle.config.ts",
    "docker-compose.yml",
    ".env.example",
    "handlers/example-event.ts",
  ];

  console.log("Creating multiverse-indexer project...\n");

  for (const file of files) {
    const src = join(templatesDir, file);
    const dest = join(cwd, file);

    if (!existsSync(src)) {
      console.warn(`  Skip ${file} (template not found)`);
      continue;
    }

    const destDir = dirname(dest);
    if (!existsSync(destDir)) {
      mkdirSync(destDir, { recursive: true });
    }

    if (existsSync(dest)) {
      console.log(`  Skip ${file} (already exists)`);
    } else {
      cpSync(src, dest);
      console.log(`  Created ${file}`);
    }
  }

  // Add scripts to package.json if it exists
  const pkgPath = join(cwd, "package.json");
  if (existsSync(pkgPath)) {
    try {
      const pkg = JSON.parse(readFileSync(pkgPath, "utf-8")) as Record<string, unknown>;
      const scripts = (pkg.scripts as Record<string, string>) ?? {};
      let updated = false;

      if (!scripts.start) {
        scripts.start = "multiverse-indexer start ./indexer.config.ts";
        updated = true;
      }
      if (!scripts.dev) {
        scripts.dev = "multiverse-indexer dev ./indexer.config.ts";
        updated = true;
      }
      if (!scripts.studio) {
        scripts.studio = "multiverse-indexer studio ./indexer.config.ts";
        updated = true;
      }

      if (updated) {
        pkg.scripts = scripts;
        writeFileSync(pkgPath, `${JSON.stringify(pkg, null, 2)}\n`);
        console.log("\n  Updated package.json with indexer scripts");
      }
    } catch {
      // ignore
    }
  }

  console.log("\nDone! Next steps:");
  console.log("  1. Copy .env.example to .env and set KEPLER_API_KEY");
  console.log("  2. (Optional) Start Postgres: docker compose up -d");
  console.log("  3. Edit indexer.config.ts with your contract address and events");
  console.log("  4. Customize schema.ts and handlers/ for your use case");
  console.log("  5. Run: bun run dev");
}

async function loadConfig(path: string): Promise<IndexerConfig> {
  const mod = await import(path);
  const config = mod.default ?? mod.config;
  if (!config) {
    throw new Error(`No default or named "config" export found in ${path}`);
  }
  return config as IndexerConfig;
}

async function runStart(
  config: IndexerConfig,
  options?: { isDev?: boolean },
): Promise<{ close: () => Promise<void> }> {
  return startIndexer(config, options);
}

async function main() {
  if (!command) {
    console.log("Usage: multiverse-indexer <command> [options]");
    console.log("");
    console.log("Commands:");
    console.log("  init [dir]  Scaffold a new indexer project (default: current directory)");
    console.log(
      "  start [config]  Clean processed schema → migrate → reindex → backfill → realtime",
    );
    console.log(
      "  dev [config]   Same as start, with auto-restart on config/schema/handler changes",
    );
    console.log("  studio [config]  Open Drizzle Studio for the project database");
    console.log("");
    console.log("  config-path  Default: ./indexer.config.ts");
    process.exit(0);
  }

  if (command === "init") {
    await runInit(args[1]);
    return;
  }

  if (command === "studio") {
    const { spawnSync } = await import("node:child_process");
    const res = spawnSync("bunx", ["drizzle-kit", "studio"], {
      cwd: process.cwd(),
      stdio: "inherit",
    });
    process.exit((res.status ?? res.signal) ? 1 : 0);
  }

  if (command !== "start" && command !== "dev") {
    console.error(`Unknown command: ${command}`);
    process.exit(1);
  }

  let config: IndexerConfig;
  try {
    config = await loadConfig(configPath);
  } catch (e) {
    console.error(`Failed to load config from ${configPath}:`, e);
    process.exit(1);
  }

  let indexer: { close: () => Promise<void> } | null = null;

  const shutdown = () => {
    if (indexer) void indexer.close().then(() => process.exit(0));
    else process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  const run = async () => {
    if (indexer) {
      await indexer.close();
      indexer = null;
    }
    try {
      config = await loadConfig(configPath);
      indexer = await runStart(config, { isDev: command === "dev" });
    } catch (e) {
      console.error("Error:", e);
      if (command === "start") process.exit(1);
    }
  };

  if (command === "dev") {
    await run();
    const configDir = dirname(configPath);
    const toWatch = [configPath, join(configDir, "schema.ts"), join(configDir, "handlers")];
    for (const p of toWatch) {
      try {
        watch(p, { recursive: p.endsWith("handlers") }, (_event, filename) => {
          if (!filename || filename.endsWith(".ts") || filename.endsWith(".tsx")) {
            console.log("Change detected, restarting...");
            void run();
          }
        });
      } catch {
        // path may not exist (e.g. handlers/ missing)
      }
    }
    return;
  }

  await run();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
