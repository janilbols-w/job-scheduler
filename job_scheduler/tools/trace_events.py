import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def append_timed_event(
    events: list[dict[str, Any]],
    started_at_monotonic: float,
    event_type: str,
    **payload: Any,
) -> dict[str, Any]:
    event = {
        "t": round(time.monotonic() - started_at_monotonic, 4),
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event_type,
    }
    event.update(payload)
    events.append(event)
    return event


def summarize_trace(
    started_at: str,
    scheduler_url: str,
    config_path: str,
    sample_interval_seconds: float,
    simulation_duration_seconds: float,
    events: list[dict[str, Any]],
    samples: list[dict[str, Any]],
    output_dir: Path,
    duration_seconds: float,
) -> dict[str, Any]:
    return {
        "started_at": started_at,
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(duration_seconds, 4),
        "scheduler_url": scheduler_url,
        "config": str(Path(config_path).resolve()),
        "sample_interval_seconds": sample_interval_seconds,
        "simulation_duration_seconds": simulation_duration_seconds,
        "event_count": len(events),
        "sample_count": len(samples),
        "output_dir": str(output_dir.resolve()),
    }


def save_trace(
    output_dir: Path,
    events: list[dict[str, Any]],
    samples: list[dict[str, Any]],
    summary: dict[str, Any],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    events_path = output_dir / "events.jsonl"
    with events_path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=True) + "\n")

    samples_path = output_dir / "workload_samples.jsonl"
    with samples_path.open("w", encoding="utf-8") as f:
        for sample in samples:
            f.write(json.dumps(sample, ensure_ascii=True) + "\n")

    csv_path = output_dir / "workload_timeline.csv"
    all_devices = sorted(
        {
            uuid
            for sample in samples
            for uuid in sample.get("device_usage_mb", {}).keys()
        }
    )
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        header = ["t", "ts", "job_count", "total_requested_memory_mb", *all_devices]
        writer.writerow(header)
        for sample in samples:
            row = [
                sample["t"],
                sample["ts"],
                sample["job_count"],
                sample["total_requested_memory_mb"],
            ]
            for uuid in all_devices:
                row.append(sample.get("device_usage_mb", {}).get(uuid, 0))
            writer.writerow(row)

    summary_path = output_dir / "simulation_summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    return {
        "events": events_path,
        "samples": samples_path,
        "timeline_csv": csv_path,
        "summary": summary_path,
    }


def save_component_trace_logs(
    output_dir: Path,
    component_events: dict[str, list[dict[str, Any]]],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    saved: dict[str, Path] = {}
    for component, events in component_events.items():
        path = output_dir / f"{component}_events.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event, ensure_ascii=True) + "\n")
        saved[component] = path
    return saved


def generate_trace_graph(
    output_dir: Path,
    events: list[dict[str, Any]],
    samples: list[dict[str, Any]],
    config: dict[str, Any],
) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    paths: list[Path] = []

    def _event_name(event: dict[str, Any]) -> str:
        return str(event.get("event", ""))

    # 1) Job events timeline
    job_events = [
        e
        for e in events
        if _event_name(e)
        in {
            "job_added",
            "job_removed",
            "wake_response",
            "wake_failed",
            "workload_job_added",
            "workload_job_removed",
            "workload_job_rejected",
            "scheduler_wake_result",
        }
    ]
    job_ids = sorted(
        {
            str(e.get("job_id"))
            for e in job_events
            if e.get("job_id") is not None
        }
    )
    if job_ids:
        y_map = {job_id: idx for idx, job_id in enumerate(job_ids)}
        fig, ax = plt.subplots(figsize=(11, 5))
        style = {
            "job_added": ("o", "tab:green"),
            "job_removed": ("x", "tab:red"),
            "wake_response": (".", "tab:blue"),
            "wake_failed": ("^", "tab:orange"),
            "workload_job_added": ("o", "tab:green"),
            "workload_job_removed": ("x", "tab:red"),
            "workload_job_rejected": ("^", "tab:orange"),
            "scheduler_wake_result": (".", "tab:blue"),
        }
        for event_name, (marker, color) in style.items():
            xs = [
                e["t"]
                for e in job_events
                if _event_name(e) == event_name and e.get("job_id") in y_map
            ]
            ys = [
                y_map[str(e["job_id"])]
                for e in job_events
                if _event_name(e) == event_name and e.get("job_id") in y_map
            ]
            if xs:
                ax.scatter(
                    xs,
                    ys,
                    label=event_name,
                    marker=marker,
                    color=color,
                    alpha=0.8,
                )
        ax.set_yticks(list(range(len(job_ids))))
        ax.set_yticklabels(job_ids)
        ax.set_xlabel("time (s)")
        ax.set_ylabel("job_id")
        ax.set_title("Job Events Timeline")
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)
        path = output_dir / "job_events_timeline.png"
        fig.tight_layout()
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)

    # 2) Scheduler events timeline
    wake_events = [
        e
        for e in events
        if _event_name(e) in {"wake_response", "scheduler_wake_result", "scheduler_enqueue"}
    ]
    if wake_events:
        wake_events = sorted(wake_events, key=lambda e: e["t"])
        xs: list[float] = []
        scheduled: list[int] = []
        ignored: list[int] = []
        queue_sizes: list[int] = []
        s_count = 0
        i_count = 0
        for e in wake_events:
            name = _event_name(e)
            response = e.get("response", {}) if isinstance(e.get("response"), dict) else {}
            status = response.get("status")

            if name == "wake_response":
                if status == "scheduled":
                    s_count += 1
                elif status == "ignored":
                    i_count += 1
                queue_size = int(response.get("queue_size", 0))
            elif name == "scheduler_enqueue":
                enqueued = bool(e.get("enqueued"))
                if enqueued:
                    s_count += 1
                else:
                    i_count += 1
                queue_size = int(e.get("queue_size", 0))
            else:
                success = bool(e.get("success"))
                if success:
                    s_count += 1
                else:
                    i_count += 1
                queue_size = 0

            xs.append(float(e["t"]))
            scheduled.append(s_count)
            ignored.append(i_count)
            queue_sizes.append(queue_size)

        fig, ax1 = plt.subplots(figsize=(11, 5))
        ax1.plot(xs, scheduled, label="scheduled (cumulative)", color="tab:green")
        ax1.plot(xs, ignored, label="ignored (cumulative)", color="tab:red")
        ax1.set_xlabel("time (s)")
        ax1.set_ylabel("count")
        ax1.grid(True, alpha=0.3)

        ax2 = ax1.twinx()
        ax2.plot(xs, queue_sizes, label="queue_size", color="tab:blue", alpha=0.5)
        ax2.set_ylabel("queue size")

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
        ax1.set_title("Scheduler Events Timeline")
        path = output_dir / "scheduler_events_timeline.png"
        fig.tight_layout()
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)

    # 3) Workload timeline
    if samples:
        xs = [float(s["t"]) for s in samples]
        job_counts = [int(s["job_count"]) for s in samples]
        total_mem = [int(s["total_requested_memory_mb"]) for s in samples]
        fig, ax1 = plt.subplots(figsize=(11, 5))
        ax1.plot(xs, job_counts, color="tab:purple", label="job_count")
        ax1.set_xlabel("time (s)")
        ax1.set_ylabel("job count")
        ax1.grid(True, alpha=0.3)

        ax2 = ax1.twinx()
        ax2.plot(xs, total_mem, color="tab:cyan", label="total_requested_memory_mb")
        ax2.set_ylabel("memory (MB)")

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
        ax1.set_title("Workload Timeline")
        path = output_dir / "workload_timeline.png"
        fig.tight_layout()
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)

    # 4) Resource view (capacity vs usage)
    if samples:
        final_usage = samples[-1].get("device_usage_mb", {})
        device_caps = {
            str(device["uuid"]): int(device["memory_size_mb"])
            for device in config.get("devices", [])
        }
        uuids = sorted(device_caps.keys())
        capacities = [device_caps[u] for u in uuids]
        usage = [int(final_usage.get(u, 0)) for u in uuids]
        fig, ax = plt.subplots(figsize=(11, 5))
        x = list(range(len(uuids)))
        width = 0.4
        ax.bar(
            [i - width / 2 for i in x],
            capacities,
            width=width,
            label="capacity_mb",
            color="tab:gray",
        )
        ax.bar(
            [i + width / 2 for i in x],
            usage,
            width=width,
            label="requested_mb",
            color="tab:blue",
        )
        ax.set_xticks(x)
        ax.set_xticklabels(uuids)
        ax.set_ylabel("memory (MB)")
        ax.set_title("Resource View (Final Snapshot)")
        ax.legend(loc="upper right")
        ax.grid(True, axis="y", alpha=0.3)
        path = output_dir / "resource_view.png"
        fig.tight_layout()
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)

    # 5) Per-device memory stack by jobs over time
    if samples:
        xs = [float(sample.get("t", 0.0)) for sample in samples]

        # Build snapshot maps first, then materialize fixed-length series.
        snapshot_device_job_memory: list[dict[str, dict[str, float]]] = []
        device_job_ids: dict[str, set[str]] = {}

        for sample in samples:
            jobs = sample.get("workload_jobs")
            if not isinstance(jobs, list):
                workload = sample.get("workload", {})
                jobs = workload.get("jobs", []) if isinstance(workload, dict) else []

            snapshot_map: dict[str, dict[str, float]] = {}
            if isinstance(jobs, list):
                for job in jobs:
                    if not isinstance(job, dict):
                        continue
                    job_id = str(job.get("job_id", ""))
                    if not job_id:
                        continue
                    gpu_memory_mb = job.get("gpu_memory_mb", {})
                    if not isinstance(gpu_memory_mb, dict):
                        continue
                    for uuid, value in gpu_memory_mb.items():
                        device_uuid = str(uuid)
                        try:
                            memory = float(value)
                        except (TypeError, ValueError):
                            continue
                        snapshot_map.setdefault(device_uuid, {})[job_id] = memory
                        device_job_ids.setdefault(device_uuid, set()).add(job_id)

            snapshot_device_job_memory.append(snapshot_map)

        device_job_series: dict[str, dict[str, list[float]]] = {}
        for device_uuid, job_ids_set in device_job_ids.items():
            per_device: dict[str, list[float]] = {}
            for job_id in sorted(job_ids_set):
                series: list[float] = []
                for snapshot_map in snapshot_device_job_memory:
                    value = snapshot_map.get(device_uuid, {}).get(job_id, 0.0)
                    series.append(float(value))
                per_device[job_id] = series
            device_job_series[device_uuid] = per_device

        non_empty_devices = [
            device_uuid
            for device_uuid, per_device in sorted(device_job_series.items())
            if any(any(value > 0 for value in series) for series in per_device.values())
        ]

        if non_empty_devices and xs:
            fig, axes = plt.subplots(
                nrows=len(non_empty_devices),
                ncols=1,
                figsize=(12, max(4, 3 * len(non_empty_devices))),
                sharex=True,
            )
            if len(non_empty_devices) == 1:
                axes = [axes]

            for idx, device_uuid in enumerate(non_empty_devices):
                ax = axes[idx]
                per_device = device_job_series[device_uuid]
                job_ids = sorted(per_device.keys())
                y_series = [per_device[job_id] for job_id in job_ids]
                ax.stackplot(xs, *y_series, labels=job_ids, alpha=0.85)
                ax.set_ylabel(f"{device_uuid} MB")
                ax.grid(True, axis="y", alpha=0.3)
                if idx == 0:
                    ax.set_title("Per-Device Memory Usage Stacked By Jobs")
                if len(job_ids) <= 8:
                    ax.legend(loc="upper right", fontsize=8)

            axes[-1].set_xlabel("time (s)")
            path = output_dir / "device_memory_stack_by_jobs.png"
            fig.tight_layout()
            fig.savefig(path, dpi=160)
            plt.close(fig)
            paths.append(path)

    return paths
