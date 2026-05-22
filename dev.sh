#!/usr/bin/env bash
# StreamGuard local dev launcher.
#
# Starts the backend (FastAPI on :8000, owns the batch scheduler
# and a long-lived Spark session) and the frontend dev server (Vite
# on :5173), each wrapped in `caffeinate` so macOS will not suspend
# either process while you have the dashboard open. The backend is
# additionally wrapped in a `while true` loop so that if its Spark
# session dies (e.g. after a sleep/wake cycle), it auto-restarts
# within 5 seconds without manual intervention.
#
# The recording agent and the Spark streaming job are NOT started
# here - they live in their own terminals so their logs stay
# readable, and they should be launched under `caffeinate` too.
#
# Ctrl+C cleans both children up.

set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"

cleanup() {
  echo
  echo "stopping backend and frontend..."
  [[ -n "${BACKEND_PID:-}" ]] && kill "$BACKEND_PID" 2>/dev/null
  [[ -n "${FRONTEND_PID:-}" ]] && kill "$FRONTEND_PID" 2>/dev/null
  wait 2>/dev/null
  echo "done"
}
trap cleanup INT TERM

cd "$ROOT"

echo "[backend]  http://localhost:8000   (FastAPI, auto-restart on death)"
caffeinate -imsu bash -c '
  while true; do
    uv run uvicorn streamguard.api:app --reload
    echo "[$(date)] backend exited, restarting in 5s..."
    sleep 5
  done
' &
BACKEND_PID=$!

echo "[frontend] http://localhost:5173   (Vite)"
( cd "$ROOT/frontend" && caffeinate -imsu npm run dev ) &
FRONTEND_PID=$!

echo
echo "PIDs: backend=$BACKEND_PID  frontend=$FRONTEND_PID"
echo "Ctrl+C to stop both."
echo

wait
