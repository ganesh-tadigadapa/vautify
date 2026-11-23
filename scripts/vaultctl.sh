#!/usr/bin/env bash
# vaultctl — keep the public Vault site alive without anyone babysitting it.
#
# The public site is a Netlify page that proxies the API to a cloudflared
# "quick tunnel" pointing at the docker stack on this Mac. Quick tunnels get a
# NEW random URL every time cloudflared restarts (reboot, Wi-Fi drop, sleep),
# so Netlify has to be redeployed with the new URL. This script does that.
#
#   vaultctl status     show every layer: Docker, stack, tunnel, public site
#   vaultctl fix        repair whatever is broken (safe to run any time)
#   vaultctl check      what the watchdog runs every 2 min: exits fast if healthy
#   vaultctl publish    rebuild the dashboard from the repo and deploy it
#   vaultctl rebuild    rebuild + restart the backend containers from the repo
#   vaultctl install    install the background watchdog (launchd), run once
#   vaultctl uninstall  remove the background watchdog
#   vaultctl logs       follow the watchdog log
#
# Settings live in ~/.vaultify/config (created by `install`).
set -uo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

HOME_DIR="$HOME/.vaultify"
CONFIG="$HOME_DIR/config"
SITE_DIR="$HOME_DIR/site"
LOG_DIR="$HOME_DIR/logs"
LOG="$LOG_DIR/watchdog.log"
TUNNEL_LOG="$LOG_DIR/tunnel.log"
STATE_URL="$HOME_DIR/deployed_url"
LOCK="$HOME_DIR/lock"
LA="$HOME/Library/LaunchAgents"
UIDN=$(id -u)

# Defaults; ~/.vaultify/config overrides them.
SITE_NAME=vault-store-865
SITE_ID=66a02707-9c48-4e15-993a-fee5dcee8dca
PORT=8096
PROJECT=vault3
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." 2>/dev/null && pwd)"
[ -f "$CONFIG" ] && . "$CONFIG"

mkdir -p "$HOME_DIR" "$LOG_DIR" "$SITE_DIR"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG" >&2; }
rotate_log() { [ -f "$LOG" ] && [ "$(stat -f%z "$LOG")" -gt 1048576 ] && mv "$LOG" "$LOG.1"; }

lock() {
  if ! mkdir "$LOCK" 2>/dev/null; then
    # Stale lock from a crashed run (older than 15 min)?
    if [ -n "$(find "$LOCK" -maxdepth 0 -mmin +15 2>/dev/null)" ]; then
      rmdir "$LOCK" 2>/dev/null; mkdir "$LOCK" 2>/dev/null || exit 0
    else
      echo "another vaultctl run is in progress" >&2; exit 0
    fi
  fi
  trap 'rmdir "$LOCK" 2>/dev/null' EXIT
}

# Run "$@" but kill it after $1 seconds. Docker Desktop sometimes wedges and
# every docker command then hangs forever; this keeps the watchdog moving.
tmo() {
  local s=$1; shift
  "$@" & local p=$!
  ( sleep "$s"; kill -9 "$p" 2>/dev/null ) & local w=$!
  wait "$p"; local rc=$?
  kill "$w" 2>/dev/null; wait "$w" 2>/dev/null
  return $rc
}

# ---------- probes ----------
# Any HTTP answer from any of these counts; some networks block 1.1.1.1 or
# dns.google outright, and one blocked host must not look like "offline".
online() {
  local u
  for u in https://www.cloudflare.com https://api.netlify.com https://1.1.1.1; do
    [ "$(curl -sS -m 6 -o /dev/null -w '%{http_code}' "$u" 2>/dev/null)" != "000" ] && return 0
  done
  return 1
}
docker_up()    { tmo 15 docker info >/dev/null 2>&1; }
local_ok()     { curl -fsS -m 4 "http://localhost:$PORT/health" 2>/dev/null | grep -q '"status": *"ok"'; }
public_ok()    { curl -fsS -m 15 "https://$SITE_NAME.netlify.app/health" 2>/dev/null | grep -q '"status": *"ok"'; }
tunnel_url()   { grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' "$TUNNEL_LOG" 2>/dev/null | tail -1; }

# Reach the tunnel through public DNS (1.1.1.1): the Mac's own resolver often
# caches "no such host" for a brand-new trycloudflare name for minutes.
# Some networks (office/campus Wi-Fi) block plain DNS to outside resolvers, so
# try, in order: the system resolver, DNS-over-HTTPS (port 443), then dig with
# a short timeout. Relying on dig @1.1.1.1 alone made every tunnel look dead.
resolve_ip() {
  local host=$1 ip
  ip=$(curl -fsS -m 5 -H 'accept: application/dns-json' \
        "https://1.1.1.1/dns-query?name=$host&type=A" 2>/dev/null \
        | grep -oE '"data":"[0-9.]+"' | head -1 | grep -oE '[0-9.]+')
  [ -n "$ip" ] || ip=$(curl -fsS -m 5 "https://dns.google/resolve?name=$host&type=A" 2>/dev/null \
        | grep -oE '"data": *"[0-9.]+"' | head -1 | grep -oE '[0-9]+\.[0-9.]+')
  [ -n "$ip" ] || ip=$(dig +short +time=2 +tries=1 @1.1.1.1 "$host" A 2>/dev/null | grep -E '^[0-9.]+$' | head -1)
  echo "$ip"
}

tunnel_ok() {
  local url=$1 host ip
  [ -n "$url" ] || return 1
  host=${url#https://}
  curl -fsS -m 10 "$url/health" 2>/dev/null | grep -q '"status": *"ok"' && return 0
  ip=$(resolve_ip "$host")
  [ -n "$ip" ] || return 1
  curl -fsS -m 10 --resolve "$host:443:$ip" "$url/health" 2>/dev/null | grep -q '"status": *"ok"'
}

# ---------- repair steps ----------
wait_docker() {
  for _ in $(seq 1 60); do docker_up && { log "Docker is up"; return 0; }; sleep 3; done
  return 1
}

force_restart_docker() {
  log "Docker is frozen -> force-quitting and restarting Docker Desktop"
  pkill -9 -f '^docker ' 2>/dev/null
  pkill -9 -f '/Applications/Docker.app/Contents/MacOS/' 2>/dev/null
  sleep 5
  open -a Docker
}

ensure_docker() {
  docker_up && return 0
  if pgrep -f '/Applications/Docker.app/Contents/MacOS/com.docker.backend' >/dev/null; then
    # Running but not answering: maybe still starting, else frozen.
    log "Docker is running but not answering; waiting"
    wait_docker && return 0
    force_restart_docker
  else
    log "Docker is not running -> starting Docker Desktop"
    open -a Docker
  fi
  wait_docker && return 0
  force_restart_docker
  wait_docker && return 0
  log "ERROR: Docker will not start. Open Docker Desktop and look for an error."; return 1
}

ensure_stack() {
  local_ok && return 0
  ensure_docker || return 1
  local_ok && return 0   # restart policies bring containers back with Docker
  sleep 20; local_ok && return 0
  local ids
  ids=$(tmo 20 docker ps -aq --filter "label=com.docker.compose.project=$PROJECT")
  if [ -z "$ids" ]; then
    log "ERROR: no '$PROJECT' containers exist. Run: vaultctl rebuild"; return 1
  fi
  log "Vault API not answering on :$PORT -> starting '$PROJECT' containers"
  # shellcheck disable=SC2086
  tmo 120 docker start $ids >/dev/null 2>&1
  for _ in $(seq 1 60); do local_ok && { log "Vault API is healthy"; return 0; }; sleep 2; done
  log "ERROR: Vault API still unhealthy. See: docker logs ${PROJECT}-vault-api-1"; return 1
}

restart_tunnel() {
  log "Restarting cloudflared tunnel (new URL incoming)"
  : > "$TUNNEL_LOG"
  launchctl kickstart -k "gui/$UIDN/com.vaultify.tunnel" 2>/dev/null \
    || { log "ERROR: tunnel agent not installed. Run: vaultctl install"; return 1; }
}

# Prints the working tunnel URL on stdout.
ensure_tunnel() {
  local url attempt
  url=$(tunnel_url)
  if tunnel_ok "$url"; then echo "$url"; return 0; fi
  # A fresh tunnel can need ~30s before it routes; give the current one a chance.
  if [ -n "$url" ]; then
    for _ in $(seq 1 6); do sleep 5; tunnel_ok "$url" && { echo "$url"; return 0; }; done
  fi
  for attempt in 1 2 3; do
    restart_tunnel || return 1
    for _ in $(seq 1 24); do
      sleep 5
      url=$(tunnel_url)
      tunnel_ok "$url" && { log "Tunnel is up: $url"; echo "$url"; return 0; }
    done
    log "Tunnel attempt $attempt did not come up"
  done
  log "ERROR: could not get a working tunnel"; return 1
}

deploy_site() {
  local url=$1
  if [ ! -f "$SITE_DIR/index.html" ]; then
    log "ERROR: no built dashboard in $SITE_DIR. Run: vaultctl publish"; return 1
  fi
  printf '/*  %s/:splat  200\n' "$url" > "$SITE_DIR/_redirects"
  [ -f "$SITE_DIR/_headers" ] || cat > "$SITE_DIR/_headers" <<'H'
/
  Cache-Control: public, max-age=300, stale-while-revalidate=86400
/index.html
  Cache-Control: public, max-age=300, stale-while-revalidate=86400
H
  log "Deploying $SITE_NAME -> $url"
  # Run from the site folder with --no-build: launchd starts jobs in "/",
  # where netlify-cli would try (and fail) to run a build.
  # On a flaky network the Netlify API sometimes answers "Project not found";
  # a retry a few seconds later goes through, so try a few times.
  local try
  for try in 1 2 3 4; do
    if (cd "$SITE_DIR" && npx --yes netlify-cli deploy --prod --no-build --dir . \
         --site "$SITE_ID" --message "vaultctl: tunnel $url") >>"$LOG" 2>&1; then
      echo "$url" > "$STATE_URL"
      log "Deployed"
      return 0
    fi
    log "Netlify deploy attempt $try failed; retrying"
    sleep $((try * 10))
  done
  log "ERROR: Netlify deploy failed (logged out? run: npx netlify-cli login)"; return 1
}

heal() {
  local force=${1:-}
  online || { log "No internet connection; nothing to do"; return 0; }
  ensure_stack  || return 1   # only touches Docker if the API is down
  local url
  url=$(ensure_tunnel) || return 1
  if [ -n "$force" ] || [ "$url" != "$(cat "$STATE_URL" 2>/dev/null)" ] || ! public_ok; then
    deploy_site "$url" || return 1
  fi
  for _ in $(seq 1 12); do public_ok && { log "OK: https://$SITE_NAME.netlify.app is healthy"; return 0; }; sleep 5; done
  log "WARNING: deployed, but public /health is not OK yet. Re-run: vaultctl fix"; return 1
}

# ---------- commands ----------
cmd_status() {
  local url
  url=$(tunnel_url)
  printf '%-14s %s\n' "Internet"  "$(online && echo OK || echo DOWN)"
  printf '%-14s %s\n' "Docker"    "$(docker_up && echo OK || echo 'NOT RUNNING / NOT ANSWERING')"
  printf '%-14s %s\n' "Vault API"   "$(local_ok && echo "OK (localhost:$PORT)" || echo DOWN)"
  printf '%-14s %s\n' "Tunnel"    "$(tunnel_ok "$url" && echo "OK ($url)" || echo "DOWN (${url:-none})")"
  printf '%-14s %s\n' "Netlify"   "$(cat "$STATE_URL" 2>/dev/null || echo 'never deployed by vaultctl')"
  printf '%-14s %s\n' "Public site" "$(public_ok && echo "OK  https://$SITE_NAME.netlify.app" || echo "DOWN https://$SITE_NAME.netlify.app")"
  printf '%-14s %s\n' "Watchdog"  "$(launchctl print "gui/$UIDN/com.vaultify.watchdog" >/dev/null 2>&1 && echo installed || echo 'NOT installed (vaultctl install)')"
}

cmd_publish() {
  [ -f "$REPO/internal/api/ui/index.html" ] || { echo "repo not found at $REPO" >&2; exit 1; }
  node "$REPO/scripts/minify-dashboard.mjs" "$REPO/internal/api/ui/index.html" "$SITE_DIR/index.html" || exit 1
  lock
  local url
  url=$(tunnel_url)
  tunnel_ok "$url" || { heal force; exit $?; }
  deploy_site "$url" && public_ok && log "OK: https://$SITE_NAME.netlify.app is healthy"
}

cmd_rebuild() {
  [ -f "$REPO/docker-compose.yml" ] || { echo "repo not found at $REPO" >&2; exit 1; }
  ensure_docker || exit 1
  (cd "$REPO" && VAULT_API_PORT=$PORT docker compose -p "$PROJECT" \
      -f docker-compose.yml -f docker-compose.selfheal.yml up --build -d)
  for _ in $(seq 1 60); do local_ok && { log "Backend rebuilt and healthy"; exit 0; }; sleep 2; done
  log "ERROR: backend unhealthy after rebuild"; exit 1
}

plist() { # label, program-args (one per line), extra xml
  local label=$1 args=$2 extra=$3 a xml=""
  while IFS= read -r a; do xml+="<string>$a</string>"; done <<<"$args"
  cat > "$LA/$label.plist" <<P
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>$label</string>
<key>ProgramArguments</key><array>$xml</array>
<key>EnvironmentVariables</key><dict><key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string></dict>
$extra
</dict></plist>
P
  # bootout is asynchronous; bootstrap fails with "5: I/O error" until it's done.
  launchctl bootout "gui/$UIDN/$label" 2>/dev/null
  for _ in $(seq 1 20); do launchctl print "gui/$UIDN/$label" >/dev/null 2>&1 || break; sleep 0.5; done
  for _ in 1 2 3 4 5; do
    launchctl bootstrap "gui/$UIDN" "$LA/$label.plist" 2>/dev/null && return 0
    sleep 1
  done
  echo "WARNING: could not load $label" >&2
}

cmd_install() {
  mkdir -p "$HOME_DIR/bin" "$LA"
  # Run from ~/.vaultify, not ~/Desktop: macOS blocks background jobs from
  # reading the Desktop folder.
  cp "${BASH_SOURCE[0]}" "$HOME_DIR/bin/vaultctl"; chmod +x "$HOME_DIR/bin/vaultctl"
  [ -f "$CONFIG" ] || cat > "$CONFIG" <<C
SITE_NAME=$SITE_NAME
SITE_ID=$SITE_ID
PORT=$PORT
PROJECT=$PROJECT
REPO=$REPO
C
  node "$REPO/scripts/minify-dashboard.mjs" "$REPO/internal/api/ui/index.html" "$SITE_DIR/index.html"

  plist com.vaultify.tunnel "$HOME_DIR/bin/vaultctl
tunnel-run" "<key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>ThrottleInterval</key><integer>10</integer>"
  plist com.vaultify.watchdog "$HOME_DIR/bin/vaultctl
check" "<key>RunAtLoad</key><true/><key>StartInterval</key><integer>120</integer>
<key>StandardErrorPath</key><string>$LOG_DIR/launchd.err</string>"
  # Keep the Mac awake while on the charger (does nothing on battery).
  plist com.vaultify.awake "/usr/bin/caffeinate
-s" "<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>"

  ln -sf "$HOME_DIR/bin/vaultctl" /opt/homebrew/bin/vaultctl 2>/dev/null || true
  echo "Installed. Background watchdog checks the site every 2 minutes."
  echo "Repairing now..."
  lock; heal force
}

cmd_uninstall() {
  for l in com.vaultify.watchdog com.vaultify.tunnel com.vaultify.awake; do
    launchctl bootout "gui/$UIDN/$l" 2>/dev/null; rm -f "$LA/$l.plist"
  done
  rm -f /opt/homebrew/bin/vaultctl
  echo "Watchdog removed (containers and ~/.vaultify left in place)."
}

case "${1:-status}" in
  status)    cmd_status ;;
  check)     rotate_log; lock; if public_ok && tunnel_ok "$(tunnel_url)"; then exit 0; fi
             log "Site unhealthy -> healing"; heal ;;
  fix)       lock; heal force; cmd_status ;;
  publish)   cmd_publish ;;
  rebuild)   cmd_rebuild ;;
  install)   cmd_install ;;
  uninstall) cmd_uninstall ;;
  logs)      tail -n 50 -f "$LOG" ;;
  tunnel-run) : > "$TUNNEL_LOG"
             exec cloudflared tunnel --no-autoupdate --protocol http2 --url "http://localhost:$PORT" >>"$TUNNEL_LOG" 2>&1 ;;
  *) sed -n '2,20p' "$0"; exit 2 ;;
esac
