#!/usr/bin/env bash
# StreamGuard - tmux startup.
#
# Starts the four long-running processes (agent, streaming, backend,
# frontend) inside a single detached tmux session named "streamguard",
# each in its own window so logs stay separated.
#
# The Mac is allowed to sleep and lock normally — no `caffeinate`
# wrappers. Every process is wrapped in a `while true` restart loop
# so a hard crash recovers within 5 seconds. The agent additionally
# subscribes to macOS NSWorkspaceDidWakeNotification and re-creates
# its pynput listeners on wake (pynput's event tap is killed by the
# kernel during sleep); the loop is a safety net for harder failures.
#
# Usage:
#   ./startup-tmux.sh                       # start everything detached
#   tmux attach -t streamguard              # look at the live logs
#   tmux kill-session -t streamguard        # stop everything
#
# Inside tmux:
#   Ctrl+B then 0/1/2/3 - cycle windows
#   Ctrl+B then D       - detach without stopping

set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is not installed. Install with: brew install tmux"
  exit 1
fi

# Make sure Kafka is up before any consumer starts. Docker handles the
# "already running" case quietly, so it's safe to call every time.
docker compose up -d >/dev/null || {
  echo "Failed to start Kafka via docker compose. Is Docker Desktop running?"
  exit 1
}

# Wait for the broker to actually accept connections before launching
# the agent. On cold boot the container starts in milliseconds but
# the broker needs a few seconds to listen on 9092; without this gate
# the agent would silently buffer into a dead socket.
echo "waiting for Kafka on localhost:9092..."
deadline=$(( $(date +%s) + 60 ))
until nc -z localhost 9092 2>/dev/null; do
  if (( $(date +%s) > deadline )); then
    echo "Kafka did not become reachable on localhost:9092 within 60s."
    exit 1
  fi
  sleep 1
done
echo "Kafka is reachable."

# Kill any previous session so this script is idempotent.
tmux kill-session -t streamguard 2>/dev/null

tmux new-session -d -s streamguard -n agent \
  "bash -c 'while true; do uv run python -m streamguard.agent --sink kafka; echo \"[\$(date)] agent exited, restarting in 5s...\"; sleep 5; done'"

tmux new-window -t streamguard -n streaming \
  "bash -c 'while true; do uv run python -m streamguard.streaming_job; echo \"[\$(date)] streaming exited, restarting in 5s...\"; sleep 5; done'"

tmux new-window -t streamguard -n backend \
  "bash -c 'while true; do uv run uvicorn streamguard.api:app --reload; echo \"[\$(date)] backend exited, restarting in 5s...\"; sleep 5; done'"

tmux new-window -t streamguard -n frontend \
  "cd $ROOT/frontend && npm run dev"

echo
echo "StreamGuard started in detached tmux session 'streamguard'."
echo "  Attach: tmux attach -t streamguard"
echo "  Stop:   tmux kill-session -t streamguard"
echo "  Open:   http://localhost:5173"
