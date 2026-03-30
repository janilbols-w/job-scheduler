# Quickstart Scripts

This folder contains a small runnable environment for local scheduler experiments.

## 0) One-Command Quickstart

Run the full flow (start API, wait for readiness, seed devices, launch mock workers, collect telemetry from core trace APIs, auto-stop):

```bash
python3 examples/quickstart/simulation.py --simulation-duration 60
```

Compatibility wrapper (same flow):

```bash
./examples/quickstart/run_quickstart.sh
```

Optional environment variables:

- `QUICKSTART_PYTHON_BIN` (default auto-detect: `python` then `python3`)
- `SCHEDULER_HOST` (default `127.0.0.1`)
- `SCHEDULER_PORT` (default `8000`)
- `SCHEDULER_URL` (default `http://$SCHEDULER_HOST:$SCHEDULER_PORT`)
- `MOCK_MEAN_WAKEUP_SECONDS` (default `8.0`)
- `MOCK_ACTION_SUCCESS_RATE` (default `0.8`)
- `SIMULATION_DURATION` (default `60`)
- `SIMULATION_OUTPUT_DIR` (optional)
- `DISABLE_CORE_TRACE` (default `0`; set to `1` to disable scheduler/resource/workload trace events)

## Simulation Outputs

`simulation.py` writes results into `examples/quickstart/simulation_outputs/<timestamp>/` by default (or `--output-dir`).

Saved data files:

- `events.jsonl`: merged trace events from scheduler/resource/workload via `GET /trace/events`
- `resource_events.jsonl`: resource-only trace events from `/trace/events` response
- `workload_events.jsonl`: workload-only trace events from `/trace/events` response
- `api_client_events.jsonl`: local `AdminClient` HTTP trace events generated during simulation
- `workload_samples.jsonl`: periodic snapshots over time
- `workload_timeline.csv`: tabular timeline export
- `simulation_summary.json`: run configuration and aggregate counts

Trace control endpoints used by simulation:

- `POST /trace/config` to enable or disable core trace emission
- `GET /trace/events?clear=1` to fetch and drain buffered trace events

Generated graphs:

- `job_events_timeline.png`
- `scheduler_events_timeline.png`
- `workload_timeline.png`
- `resource_view.png`

## 1) Start Scheduler API Server

```bash
python3 examples/quickstart/start_api_server.py --host 127.0.0.1 --port 8000
```

## 2) Seed Test Environment

This script:

- adds 4 devices split across `host-0` and `host-1`
- creates a few jobs via `AdminClient` to initialize workload state
- reads devices and seed jobs from [examples/quickstart/seed_config.json](examples/quickstart/seed_config.json)

```bash
python3 examples/quickstart/seed_test_env.py --scheduler-url http://127.0.0.1:8000
```

Use a custom config file:

```bash
python3 examples/quickstart/seed_test_env.py \
  --scheduler-url http://127.0.0.1:8000 \
  --config /path/to/seed_config.json
```

To launch mock-job processes directly from seed data:

```bash
python3 examples/quickstart/seed_test_env.py \
  --scheduler-url http://127.0.0.1:8000 \
  --launch-mock-jobs
```

## 3) Run Mock Job Workers

`mock_job.py` exposes worker APIs:

- `POST /wake_up`
- `POST /sleep`

Each endpoint returns success or failure randomly according to `--action-success-rate`.

Each mock worker owns a `JobClient` and periodically calls scheduler `POST /wake_up` with Poisson-style timing (exponential inter-arrival) controlled by `--mean-wakeup-seconds`.

When launched by `seed_test_env.py --launch-mock-jobs`, each seed job can also define `init_delay_seconds` in [examples/quickstart/seed_config.json](examples/quickstart/seed_config.json) to stagger startup wake behavior.

Each seed job can also configure randomized delay for successful worker responses:

- `success_delay_min_seconds` (default `0.0`)
- `success_delay_max_seconds` (default `0.0`)

When a mock worker returns success from `POST /wake_up` or `POST /sleep`, it will delay the response by a random value uniformly sampled from `[success_delay_min_seconds, success_delay_max_seconds]`.

Example terminal A:

```bash
python3 examples/quickstart/mock_job.py \
  --scheduler-url http://127.0.0.1:8000 \
  --job-id mock-job-a \
  --listen-port 9100 \
  --device-uuids d-0
```

Example terminal B:

```bash
python3 examples/quickstart/mock_job.py \
  --scheduler-url http://127.0.0.1:8000 \
  --job-id mock-job-b \
  --listen-port 9101 \
  --device-uuids d-1,d-2 \
  --mean-wakeup-seconds 5 \
  --action-success-rate 0.7
```

## Notes

- Keep the scheduler API server running before starting seed or mock scripts.
- For custom device payloads, pass `--devices-json` with a JSON list of devices.
- `run_quickstart.sh` handles process lifecycle and cleanup automatically on Ctrl+C.
