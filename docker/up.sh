#!/usr/bin/env bash
# usage: docker/up.sh [--no-ui] [public-url]. Starts the stack with the UI, or without it; run it again to upgrade or to change the public URL.
set -euo pipefail
export LC_ALL=C
umask 077
cd "$(dirname "$0")/.."
env=docker/.env
compose=(docker compose -f docker/compose.yaml)
applied=docker/.applied-url

if [ -t 1 ]; then g=$'\033[32m' r=$'\033[31m' y=$'\033[33m' b=$'\033[1m' d=$'\033[2m' n=$'\033[0m'; else g='' r='' y='' b='' d='' n=''; fi
ok() { echo "  ${g}✓${n} $*"; }
warn() { echo "  ${y}!${n} $*"; }
die() { echo "  ${r}✗${n} $*" >&2; exit 1; }
get() { awk -v k="$1" 'index($0, k "=") == 1 { v = substr($0, length(k) + 2) } END { print v }' "$env" | tr -d " \"'"; }
put() {
  awk -v k="$1" -v v="$2" 'index($0, k "=") == 1 { $0 = k "=" v; f = 1 } { print } END { if (!f) print k "=" v }' "$env" >"$env.new"
  cat "$env.new" >"$env" && rm "$env.new"
}

ui=1
[ "${1:-}" != --no-ui ] || { ui= && shift; }
[ -z "$ui" ] || compose+=(--profile ui)
arg=${1:-}

echo "${b}human-specialist${n}"
command -v docker >/dev/null || die "Docker is not installed: https://docs.docker.com/get-docker/"
v=$(docker compose version --short 2>/dev/null) || die "Docker Compose v2 is missing: https://docs.docker.com/compose/install/"
docker info >/dev/null 2>&1 || die "cannot reach the Docker daemon: start Docker, or give your user access to it"
ok "Docker is running, Compose $v"

[ -f "$env" ] || { cp docker/.env.example "$env" && ok "created $env"; }
if [ -n "$arg" ]; then
  [[ $arg =~ ^https?://[^/?#[:space:]]+ ]] || die "'$arg' is not a public URL: docker/up.sh https://<tunnel or domain>"
  put PUBLIC_BASE_URL "${arg%/}"
fi
url=$(get PUBLIC_BASE_URL)
case "$url" in
  "" | *//hs.example.com*) die "give the public URL Plivo calls back: docker/up.sh https://<tunnel or domain>" ;;
  http://* | https://*) ok "PUBLIC_BASE_URL $url" ;;
  *) die "PUBLIC_BASE_URL must start with https:// (it is $url)" ;;
esac
for k in HS_OPERATOR_KEY HS_SECRET_KEY; do
  v=$(get "$k")
  if [ -z "$v" ]; then
    put "$k" "$(head -c 32 /dev/urandom | base64)" && ok "generated $k"
  elif [ ${#v} -lt 32 ]; then
    die "$k is ${#v} bytes and needs at least 32: empty it in $env and run again to generate one"
  fi
done

quiet=$("${compose[@]}" up --help 2>/dev/null | grep -o -- --quiet-build || true)
echo "  ${d}building and starting; the first run takes a few minutes${n}"
if ! "${compose[@]}" up -d --build --wait $quiet; then
  for s in migrate hs; do
    echo "  ${b}$s log${n}"
    "${compose[@]}" logs --no-log-prefix --tail 6 "$s" | sed 's/^/    /'
  done
  die "the stack did not come up; the log lines above say why"
fi
ok "database migrated, service healthy"
plivo=$(get PLIVO_API_URL) && plivo=${plivo:-https://api.plivo.com}
"${compose[@]}" exec -T hs python -c 'import sys, urllib.error as e, urllib.request as u
try: u.urlopen(sys.argv[1], timeout=5)
except e.HTTPError: pass' "$plivo" >/dev/null 2>&1 || die "the service cannot reach Plivo at $plivo: restart Docker, or check your network and firewall"
ok "the service reaches Plivo"

port=$(get PORT) ui_port=$(get UI_PORT) front=${ui_port:-6005}
[ -n "$ui" ] || front=${port:-6004}
if curl -fsS --max-time 10 "$url/status" >/dev/null 2>&1; then
  ok "$url answers"
else
  warn "$url/status does not answer from here, so Plivo cannot reach the service: start the tunnel to port $front"
fi
[ ! -f "$applied" ] || [ "$(cat "$applied")" = "$url" ] || warn "the public URL changed: under Connect, Plivo Account, enter the operator key and click Re-apply Plivo URLs"
echo "$url" >"$applied"
open=$url
if [ -z "$ui" ]; then open="none, the stack runs without the UI"
else case "$url" in *.trycloudflare.com*) open=http://localhost:$front && warn "a quick tunnel cannot carry the desk's live events, so open the UI on this machine" ;; esac; fi
proj=${COMPOSE_PROJECT_NAME:-$(get COMPOSE_PROJECT_NAME)}
[ -z "$proj" ] || compose=(docker compose -p "$proj" "${compose[@]:2}")
cat <<EOF

  ${b}Open${n}      $open
  ${b}Plivo${n}     $url   ${d}(calls and messages come back here)${n}
  ${b}API${n}       http://localhost:${port:-6004}/api/v1
  ${b}Operator${n}  grep HS_OPERATOR_KEY $env | cut -d= -f2-   ${d}paste it under Register an organization${n}

  Logs  ${compose[*]} logs -f hs
  Stop  ${compose[*]} down
EOF
