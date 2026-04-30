#!/usr/bin/env bash
# One-command startup for DataLake AI v2 (React + FastAPI)
# Usage: make ui  OR  bash scripts/start_ui.sh

set -e

FRONTEND_DIR="services/ai-agent-v2/frontend"
BACKEND_DIR="services/ai-agent"
BACKEND_PORT=8502
FRONTEND_PORT=3001

# ── Colors ────────────────────────────────────────────────────────────────────
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
RESET='\033[0m'

log()  { echo -e "${CYAN}▷${RESET}  $1"; }
ok()   { echo -e "${GREEN}✓${RESET}  $1"; }
warn() { echo -e "${YELLOW}!${RESET}  $1"; }
err()  { echo -e "${RED}✗${RESET}  $1"; }

echo ""
echo -e "${BOLD}  Codincity — DataLake AI v2${RESET}"
echo -e "  ${CYAN}Starting up…${RESET}"
echo ""

# ── Sanity checks ─────────────────────────────────────────────────────────────
if ! command -v node &>/dev/null; then
  err "Node.js not found. Install it: https://nodejs.org"
  exit 1
fi
if ! command -v npm &>/dev/null; then
  err "npm not found. Install Node.js: https://nodejs.org"
  exit 1
fi
if ! command -v python3 &>/dev/null && ! command -v python &>/dev/null; then
  err "Python not found."
  exit 1
fi

PYTHON=$(command -v python3 || command -v python)

# ── Install frontend deps if needed ──────────────────────────────────────────
if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
  log "Installing frontend dependencies (first run — takes ~30s)…"
  npm install --prefix "$FRONTEND_DIR" --silent
  ok "Frontend dependencies installed"
else
  ok "Frontend dependencies ready"
fi

# ── Install backend deps if needed ───────────────────────────────────────────
if ! $PYTHON -c "import fastapi, uvicorn" &>/dev/null 2>&1; then
  log "Installing FastAPI + Uvicorn…"
  $PYTHON -m pip install fastapi "uvicorn[standard]" python-multipart --quiet
  ok "FastAPI installed"
else
  ok "FastAPI ready"
fi

# ── Install AI agent deps if needed ──────────────────────────────────────────
if ! $PYTHON -c "import langchain_core, langgraph, langchain_groq" &>/dev/null 2>&1; then
  log "Installing AI agent dependencies (langgraph, langchain, groq)…"
  $PYTHON -m pip install -r services/ai-agent/requirements.txt --quiet
  ok "AI agent dependencies installed"
else
  ok "AI agent dependencies ready"
fi

# ── Kill any leftover processes on our ports ──────────────────────────────────
kill_port() {
  local pid
  pid=$(lsof -ti tcp:$1 2>/dev/null || true)
  [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
}
kill_port $BACKEND_PORT
kill_port $FRONTEND_PORT

# ── Get local network IP (WiFi/eth only — excludes Docker bridges 172.x) ──────
LOCAL_IP=$(ip addr show 2>/dev/null \
           | grep 'inet ' \
           | grep -v '127\.' \
           | grep -v '172\.' \
           | awk '{print $2}' | cut -d/ -f1 | head -1)
[ -z "$LOCAL_IP" ] && LOCAL_IP="localhost"

# ── Start FastAPI backend ─────────────────────────────────────────────────────
log "Starting FastAPI backend on :$BACKEND_PORT…"
cd "$BACKEND_DIR"
$PYTHON server.py &>/tmp/datalake_api.log &
BACKEND_PID=$!
cd - > /dev/null

# Wait up to 8s for backend to be ready
for i in $(seq 1 16); do
  if curl -sf "http://localhost:$BACKEND_PORT/api/health" &>/dev/null; then
    ok "Backend ready  →  http://localhost:$BACKEND_PORT"
    break
  fi
  sleep 0.5
  if [ "$i" -eq 16 ]; then
    warn "Backend slow to start — frontend will retry. Check /tmp/datalake_api.log if needed"
  fi
done

# ── Cleanup on exit ───────────────────────────────────────────────────────────
cleanup() {
  echo ""
  log "Shutting down…"
  kill $BACKEND_PID 2>/dev/null || true
  kill_port $FRONTEND_PORT
  ok "Stopped. Goodbye."
  exit 0
}
trap cleanup INT TERM

# ── Print access URLs ─────────────────────────────────────────────────────────
echo ""
echo -e "  ${BOLD}Access URLs${RESET}"
echo -e "  ${GREEN}PC / browser   →${RESET}  http://localhost:$FRONTEND_PORT"
echo -e "  ${GREEN}Phone / tablet →${RESET}  http://${LOCAL_IP}:$FRONTEND_PORT"
echo -e "  ${CYAN}API docs       →${RESET}  http://localhost:$BACKEND_PORT/api/docs"
echo ""
echo -e "  ${YELLOW}Ctrl+C to stop${RESET}"
echo ""

# ── Start Vite frontend (foreground — keeps terminal alive) ───────────────────
npm run dev --prefix "$FRONTEND_DIR"

# Foreground process ended — clean up backend
cleanup
