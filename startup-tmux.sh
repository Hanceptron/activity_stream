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

# Make sure events.raw exists before the streaming job subscribes to
# it. Kafka's auto.create.topics.enable triggers on produce but not on
# the Spark KafkaSource admin lookup, so without this step a fresh
# Kafka container puts the streaming job into a crash loop on
# UnknownTopicOrPartitionException until the agent produces its first
# event. --if-not-exists makes this idempotent for the common case
# (Kafka was just restarted with the topic intact).
docker exec streamguard-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events.raw \
  --partitions 1 --replication-factor 1 >/dev/null

# If the streaming checkpoint claims a Kafka offset higher than what
# the broker actually has, the next streaming startup throws
# "Some data may have been lost" and crashes forever. This happens
# whenever Kafka data was wiped (docker compose down recreates the
# container without a persistent volume). Detect and wipe the stale
# checkpoint so the next streaming startup begins fresh from
# startingOffsets=latest. We leave output/metrics + output/events
# parquet alone - those are durable derived data and the dot already
# ignores anything older than its freshness threshold.
ckpt_off=$(cat output/checkpoint/metrics/offsets/$(ls output/checkpoint/metrics/offsets/ 2>/dev/null | grep -E '^[0-9]+$' | sort -n | tail -1) 2>/dev/null \
  | grep '"events.raw"' | sed -E 's/.*"0":([0-9]+).*/\1/')
kafka_hw=$(docker exec streamguard-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic events.raw 2>/dev/null \
  | awk -F: '{print $3}')
if [[ -n "$ckpt_off" && -n "$kafka_hw" && "$ckpt_off" -gt "$kafka_hw" ]]; then
  echo "checkpoint offset $ckpt_off > kafka high-water $kafka_hw; wiping output/checkpoint"
  rm -rf output/checkpoint output/metrics/_spark_metadata output/events/_spark_metadata
fi

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
