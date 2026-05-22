#!/usr/bin/env bash
# StreamGuard - tmux startup.
#
# Starts the four long-running processes (agent, streaming, backend,
# frontend) inside a single detached tmux session named "streamguard",
# each in its own window so logs stay separated. Every process is
# wrapped in `caffeinate -imsu` to keep the system awake on AC power
# without forcing the display on.
#
# Streaming and backend are wrapped in a `while true` loop so they
# self-restart after a sleep/wake cycle or any other crash. The agent
# is not - pynput's macOS event tap can go silent without the process
# exiting, so a restart loop wouldn't catch it. If the dashboard's
# live dot goes red while you're typing, attach to the session and
# Ctrl+C / re-run the agent window manually.
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

# Kill any previous session so this script is idempotent.
tmux kill-session -t streamguard 2>/dev/null

tmux new-session -d -s streamguard -n agent \
  "caffeinate -imsu uv run python -m streamguard.agent --sink kafka"

tmux new-window -t streamguard -n streaming \
  "caffeinate -imsu bash -c 'while true; do uv run python -m streamguard.streaming_job; echo \"[\$(date)] streaming exited, restarting in 5s...\"; sleep 5; done'"

tmux new-window -t streamguard -n backend \
  "caffeinate -imsu bash -c 'while true; do uv run uvicorn streamguard.api:app --reload; echo \"[\$(date)] backend exited, restarting in 5s...\"; sleep 5; done'"

tmux new-window -t streamguard -n frontend \
  "cd $ROOT/frontend && caffeinate -imsu npm run dev"

echo
echo "StreamGuard started in detached tmux session 'streamguard'."
echo "  Attach: tmux attach -t streamguard"
echo "  Stop:   tmux kill-session -t streamguard"
echo "  Open:   http://localhost:5173"
