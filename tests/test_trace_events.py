import json
import sys
import types

from job_scheduler.tools.trace_events import generate_trace_graph, save_component_trace_logs


def test_save_component_trace_logs_writes_one_jsonl_per_component(tmp_path):
    component_events = {
        "resource": [
            {"component": "resource", "event": "resource_device_added", "uuid": "d-0"},
        ],
        "workload": [
            {"component": "workload", "event": "workload_job_added", "job_id": "job-1"},
        ],
        "api_client": [
            {"component": "api_client", "event": "api_request", "path": "/resource", "ok": True},
        ],
    }

    saved = save_component_trace_logs(output_dir=tmp_path, component_events=component_events)

    assert set(saved.keys()) == {"resource", "workload", "api_client"}
    assert (tmp_path / "resource_events.jsonl").exists()
    assert (tmp_path / "workload_events.jsonl").exists()
    assert (tmp_path / "api_client_events.jsonl").exists()

    resource_lines = (tmp_path / "resource_events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    workload_lines = (tmp_path / "workload_events.jsonl").read_text(encoding="utf-8").strip().splitlines()
    client_lines = (tmp_path / "api_client_events.jsonl").read_text(encoding="utf-8").strip().splitlines()

    assert json.loads(resource_lines[0])["event"] == "resource_device_added"
    assert json.loads(workload_lines[0])["event"] == "workload_job_added"
    assert json.loads(client_lines[0])["event"] == "api_request"


def test_generate_trace_graph_writes_device_memory_stack_plot(tmp_path, monkeypatch):
    class FakeAxes:
        def scatter(self, *args, **kwargs):
            _ = (args, kwargs)

        def set_yticks(self, *args, **kwargs):
            _ = (args, kwargs)

        def set_yticklabels(self, *args, **kwargs):
            _ = (args, kwargs)

        def set_xlabel(self, *args, **kwargs):
            _ = (args, kwargs)

        def set_ylabel(self, *args, **kwargs):
            _ = (args, kwargs)

        def set_title(self, *args, **kwargs):
            _ = (args, kwargs)

        def legend(self, *args, **kwargs):
            _ = (args, kwargs)

        def grid(self, *args, **kwargs):
            _ = (args, kwargs)

        def plot(self, *args, **kwargs):
            _ = (args, kwargs)

        def twinx(self):
            return FakeAxes()

        def get_legend_handles_labels(self):
            return ([], [])

        def bar(self, *args, **kwargs):
            _ = (args, kwargs)

        def set_xticks(self, *args, **kwargs):
            _ = (args, kwargs)

        def set_xticklabels(self, *args, **kwargs):
            _ = (args, kwargs)

        def stackplot(self, *args, **kwargs):
            _ = (args, kwargs)

    class FakeFigure:
        def tight_layout(self):
            return None

        def savefig(self, path, dpi=160):
            _ = dpi
            with open(path, "wb") as f:
                f.write(b"fake-image")

    class FakePyplot(types.ModuleType):
        def __init__(self):
            super().__init__("matplotlib.pyplot")

        def subplots(self, nrows=1, ncols=1, figsize=None, sharex=False):
            _ = (ncols, figsize, sharex)
            fig = FakeFigure()
            if nrows == 1:
                return fig, FakeAxes()
            return fig, [FakeAxes() for _ in range(nrows)]

        def close(self, *args, **kwargs):
            _ = (args, kwargs)

    fake_matplotlib = types.ModuleType("matplotlib")
    fake_matplotlib.use = lambda backend: backend
    fake_pyplot = FakePyplot()

    monkeypatch.setitem(sys.modules, "matplotlib", fake_matplotlib)
    monkeypatch.setitem(sys.modules, "matplotlib.pyplot", fake_pyplot)

    samples = [
        {
            "t": 0.0,
            "job_count": 1,
            "total_requested_memory_mb": 4000,
            "device_usage_mb": {"d-0": 4000},
            "workload_jobs": [
                {"job_id": "job-a", "gpu_memory_mb": {"d-0": 4000}},
            ],
        },
        {
            "t": 1.0,
            "job_count": 2,
            "total_requested_memory_mb": 13000,
            "device_usage_mb": {"d-0": 13000},
            "workload_jobs": [
                {"job_id": "job-a", "gpu_memory_mb": {"d-0": 5000}},
                {"job_id": "job-b", "gpu_memory_mb": {"d-0": 8000}},
            ],
        },
        {
            "t": 2.0,
            "job_count": 1,
            "total_requested_memory_mb": 8000,
            "device_usage_mb": {"d-0": 8000},
            "workload_jobs": [
                {"job_id": "job-b", "gpu_memory_mb": {"d-0": 8000}},
            ],
        },
    ]
    config = {"devices": [{"uuid": "d-0", "memory_size_mb": 16000}]}

    paths = generate_trace_graph(output_dir=tmp_path, events=[], samples=samples, config=config)

    stack_path = tmp_path / "device_memory_stack_by_jobs.png"
    assert stack_path.exists()
    assert stack_path in paths
