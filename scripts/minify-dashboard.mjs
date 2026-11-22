#!/usr/bin/env node
// Writes a minified copy of the dashboard for static hosting. Only the inline
// <style> and <script> blocks are minified (with esbuild); the markup, and so
// every label, role and ARIA attribute, is copied exactly as written. The
// source file stays readable; the Go server sends it gzip-compressed instead.
//
//   node scripts/minify-dashboard.mjs internal/api/ui/index.html dist/netlify/index.html
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";

const [src, out] = process.argv.slice(2);
if (!src || !out) {
  console.error("usage: minify-dashboard.mjs SRC OUT");
  process.exit(2);
}

const esbuild = (code, loader) =>
  execFileSync("npx", ["--yes", "esbuild@0.25", "--minify", `--loader=${loader}`, "--log-level=warning"], {
    input: code, encoding: "utf8", maxBuffer: 64 << 20,
  }).trim();

const html = readFileSync(src, "utf8");
let blocks = 0;
const min = html
  .replace(/<style>([\s\S]*?)<\/style>/g, (_, css) => { blocks++; return `<style>${esbuild(css, "css")}</style>`; })
  .replace(/<script>([\s\S]*?)<\/script>/g, (_, js) => { blocks++; return `<script>${esbuild(js, "js")}</script>`; });
if (blocks !== 2) {
  console.error(`minify-dashboard: expected one <style> and one <script> block, found ${blocks}`);
  process.exit(1);
}
writeFileSync(out, min);
console.log(`minify-dashboard: ${html.length} -> ${min.length} bytes`);
