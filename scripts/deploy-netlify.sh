#!/usr/bin/env bash
# Deploy the Vault dashboard to Netlify. Netlify serves the static page and
# proxies every other path (the JSON API) to a Vault API that is publicly
# reachable at BACKEND_URL: a cloudflared tunnel to a local stack, or the VM
# from scripts/deploy-gce.sh.
#
#   ./scripts/deploy-netlify.sh https://example.trycloudflare.com
#
# Requires a Netlify login (npx netlify-cli login). Deploys to NETLIFY_SITE
# (name or ID) if set, else to the linked site; with neither, the first run
# asks which site to create or link and later runs reuse it.
set -euo pipefail

BACKEND_URL=${1:-${BACKEND_URL:-}}
if [ -z "$BACKEND_URL" ]; then
  echo "usage: $0 BACKEND_URL" >&2
  exit 2
fi
BACKEND_URL=${BACKEND_URL%/}
case "$BACKEND_URL" in
  http://localhost* | http://127.* | https://localhost* | https://127.*)
    echo "deploy-netlify: $BACKEND_URL is only reachable from this machine, not from Netlify" >&2
    exit 2 ;;
esac

cd "$(dirname "$0")/.."
if ! curl -fsS "$BACKEND_URL/health" >/dev/null; then
  echo "deploy-netlify: no Vault API answering at $BACKEND_URL/health" >&2
  exit 1
fi

OUT=dist/netlify
rm -rf "$OUT"
mkdir -p "$OUT"
node scripts/minify-dashboard.mjs internal/api/ui/index.html "$OUT/index.html"
# index.html shadows "/", everything else goes to the API.
printf '/*  %s/:splat  200\n' "$BACKEND_URL" > "$OUT/_redirects"
# Netlify compresses responses itself (brotli/gzip). Browsers may reuse the
# page for 5 minutes and revalidate in the background after that.
cat > "$OUT/_headers" <<'HEADERS'
/
  Cache-Control: public, max-age=300, stale-while-revalidate=86400
/index.html
  Cache-Control: public, max-age=300, stale-while-revalidate=86400
HEADERS

npx --yes netlify-cli deploy --prod --dir "$OUT" ${NETLIFY_SITE:+--site "$NETLIFY_SITE"}
