# KeySpark - Startup

Four processes need to run for the full pipeline: Kafka (Docker), the
recording agent, the Spark streaming job, and the dashboard (FastAPI +
React). The API owns the batch scheduler internally - no separate batch
terminal needed.

## Prerequisites (one-time)

- Docker Desktop running.
- Java 17 or 21 on `PATH` (`java -version` should print something).
- macOS Accessibility AND Input Monitoring granted to whichever terminal
  app you launch the agent from. Quit and re-launch the terminal after
  granting either permission for the first time.
- `brew install tmux` if you want the recommended background-running mode.

## Sleep & screen lock

KeySpark is sleep-friendly: the Mac is allowed to sleep and the
screen is allowed to lock as usual (no `caffeinate` wrappers anywhere).

- Walk away → screen locks per `System Settings → Lock Screen`.
- Lid closed or system sleep → fine; the recorder picks up on wake.
- On wake, the agent subscribes to macOS `NSWorkspaceDidWakeNotification`
  and re-creates its pynput listeners automatically. Spark's RPC dies
  on sleep, so the streaming job and backend both run inside a
  `while true` restart loop that recovers within ~5 s.
- If something hard-crashes anyway, each process (including the
  agent) is wrapped in the same restart loop in `startup-tmux.sh`.

The dashboard's live dot is the at-a-glance smoke test: it should be
green again within ~30 s of unlocking.

## Option A: tmux session (recommended for long recordings)

`tmux` lets you start everything once, close every terminal window, and
keep the processes running. Re-attach any time to check logs.

### Start

```sh
./startup-tmux.sh
```

The script waits for Kafka to accept connections on `localhost:9092`
before starting any of the four tmux windows, so the agent never races
the broker on cold boot.

Open <http://localhost:5173> in a browser.

### Inspect / attach

```sh
tmux attach -t streamguard
```

Inside tmux:

- `Ctrl+B` then `0` / `1` / `2` / `3` cycles through windows
  (`agent`, `streaming`, `backend`, `frontend`).
- `Ctrl+B` then `D` detaches without stopping anything.

### Stop

```sh
tmux kill-session -t streamguard
```

## Option B: foreground terminals

If you'd rather have a separate terminal window per process and watch
the logs directly, run four terminals manually:

### Terminal 1 - Kafka

```sh
docker compose up -d
```

(Container detaches; this terminal is free.)

### Terminal 2 - Recording agent (auto-restart loop)

```sh
while true; do
  uv run python -m streamguard.agent --sink kafka
  echo "[$(date)] agent exited, restarting in 5s..."
  sleep 5
done
```

Prints a "listeners restarted after wake" line each time the Mac wakes
from sleep; otherwise quiet.

### Terminal 3 - Spark streaming job (auto-restart loop)

```sh
while true; do
  uv run python -m streamguard.streaming_job
  echo "[$(date)] streaming job exited, restarting in 5s..."
  sleep 5
done
```

### Terminal 4 - Backend + frontend

```sh
./dev.sh
```

Starts uvicorn (:8000, with its own auto-restart loop) and Vite (:5173).

Open <http://localhost:5173>.

## What's running

| Component | Port | Notes |
|---|---|---|
| Kafka broker | 9092 | Docker container (auto-restarts via compose) |
| Recording agent | - | uv / pynput; subscribes to macOS wake events |
| Spark streaming job | - | writes `output/metrics/`, `output/events/` |
| FastAPI backend | 8000 | also owns the in-process batch loop |
| React dev server | 5173 | the dashboard |

## Verifying it's all alive

```sh
ps -ef | grep -E 'streamguard|uvicorn|vite' | grep -v grep
docker ps --filter name=streamguard-kafka
curl -s http://localhost:8000/api/batch_status

# Newest metric window age - should be <120s while you're typing
curl -s http://localhost:8000/api/metrics | python3 -c '
import json, sys
from datetime import datetime, timezone
d = json.load(sys.stdin)
if d:
    latest = d[-1]["window_start"]
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(latest).replace(tzinfo=timezone.utc)).total_seconds()
    print(f"newest window: {latest} UTC, age {age:.0f}s, live? {age < 120}")
else:
    print("no recent windows")
'
```

The dashboard's live dot (green/red) is the easiest day-to-day smoke
test: green = full chain is flowing.

## Recovery

### Dashboard goes "offline" while you're typing

```sh
ps -ef | grep -E 'streamguard.streaming_job|streamguard.agent' | grep -v grep
```

All three of agent, streaming job, and backend live inside a restart
loop, so a missing process should respawn within ~5 s on its own. If
the dot stays red:

- **No process listed at all**: the loop itself was killed. Re-run
  `./startup-tmux.sh` (it is idempotent — kills the prior session
  first) or relaunch the affected terminal.
- **Process listed but no fresh data**: tail the relevant tmux pane
  (`tmux attach -t streamguard`, then `Ctrl+B 0/1/2/3`) for an error
  message. Wake handling now lives inside the agent, so a stuck event
  tap is no longer the expected culprit; check Kafka first.

### Spark Kafka NPE on streaming-job startup

Known Spark 4.x bug when restoring from an existing checkpoint. The loop
keeps crashing on the same NPE until you wipe the stale state:

```sh
pkill -f streamguard.streaming_job
rm -rf output/checkpoint
rm -rf output/metrics/_spark_metadata
rm -rf output/events/_spark_metadata
```

The loop restarts cleanly within 5s. Parquet data in `output/metrics/`,
`output/events/`, `output/sessions/`, `output/baseline/`, and
`output/heatmaps/` is preserved.

### Rhythm-metric removal migration

The `flight_time_std` and `long_pause_count` columns were retired. On
the first restart after pulling that change, wipe the now-incompatible
streaming state so Spark rebuilds the parquet with the new schema:

```sh
pkill -f streamguard.streaming_job
rm -rf output/checkpoint/metrics output/metrics output/baseline
```

The streaming job recreates `output/metrics/` on its next loop; the
batch scheduler will refill `output/baseline/` on its next tick.

### Force a fresh batch run

The API runs the batch every 5 minutes automatically. If you want one
right now:

```sh
JAVA_HOME=$(/usr/libexec/java_home) uv run python -m streamguard.batch_job
```

(Standalone invocation; competes with the API's in-process scheduler,
but both write with `mode("overwrite")`, so the last writer wins.)

### Fresh start - wipe everything and begin a new recording

```sh
tmux kill-session -t streamguard 2>/dev/null
pkill -f streamguard
docker compose down            # optional - also stops Kafka
rm -rf output/
```

Then start over from "Option A" or "Option B".

## Shutdown

tmux:

```sh
tmux kill-session -t streamguard
```

Foreground terminals: `Ctrl+C` in each. Order doesn't matter.

`docker compose down` to also stop the Kafka container.

Parquet data in `output/` persists between runs.
