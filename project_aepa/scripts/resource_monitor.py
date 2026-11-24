"""
Background resource monitor for local LLM runs on Apple Silicon.

Samples every second (default) so short peaks are not missed:
- GPU utilization and GPU memory (via ioreg, no sudo needed)
- System memory used / available, swap, macOS memory pressure level
- vLLM queue metrics from /metrics (if available)

Process RSS is not tracked: on Apple Silicon, model weights and KV cache live in
Metal buffers that RSS does not count. GPU memory is the meaningful figure.

Sampling still cannot catch every instantaneous spike, so treat the reported
max as a lower bound of the true peak.

Usage:
    monitor = ResourceMonitor(vllm_metrics_url="http://localhost:8000/metrics")
    monitor.start()
    ...
    monitor.stop()
    monitor.print_summary()
    monitor.save("samples.csv")
"""

import os
import re
import subprocess
import threading
import time

import pandas as pd
import psutil
import requests

GPU_UTIL_RE = re.compile(r'"Device Utilization %"=(\d+)')
GPU_MEM_RE = re.compile(r'"In use system memory"=(\d+)')
VLLM_METRIC_RE = re.compile(
    r"^vllm:(num_requests_running|num_requests_waiting)\{.*\}\s+([\d.]+)", re.M
)

# kern.memorystatus_vm_pressure_level: 1 = normal, 2 = warning, 4 = critical
PRESSURE_LABELS = {1: "normal", 2: "warning", 4: "critical"}

GB = 1024**3


def read_gpu() -> tuple[float | None, float | None]:
    try:
        out = subprocess.run(
            ["ioreg", "-r", "-d", "1", "-w", "0", "-c", "IOAccelerator"],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except Exception:
        return None, None
    util = GPU_UTIL_RE.search(out)
    mem = GPU_MEM_RE.search(out)
    return (
        float(util.group(1)) if util else None,
        int(mem.group(1)) / GB if mem else None,
    )


def read_memory_pressure() -> int | None:
    try:
        out = subprocess.run(
            ["sysctl", "-n", "kern.memorystatus_vm_pressure_level"],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        return int(out)
    except Exception:
        return None


def read_vllm_queue(url: str | None) -> dict:
    if not url:
        return {}
    try:
        text = requests.get(url, timeout=2).text
    except Exception:
        return {}
    metrics = {}
    for name, value in VLLM_METRIC_RE.findall(text):
        metrics[name] = metrics.get(name, 0) + float(value)
    return metrics


def start_memory_guard(limit_gb: float, interval: float = 1.0) -> threading.Thread:
    """
    Kill this process if its resident memory exceeds limit_gb.

    macOS does not enforce RLIMIT_AS, so a watchdog thread is the practical safeguard.
    """
    process = psutil.Process()

    def watch():
        while True:
            rss = process.memory_info().rss / GB
            if rss > limit_gb:
                print(
                    f"\n❌ Memory guard: process uses {rss:.1f} GB > limit {limit_gb:g} GB. Aborting.",
                    flush=True,
                )
                os._exit(1)
            time.sleep(interval)

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    return thread


class ResourceMonitor:
    def __init__(self, interval: float = 1.0, vllm_metrics_url: str | None = None):
        self.interval = interval
        self.vllm_metrics_url = vllm_metrics_url
        self.samples: list[dict] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _sample(self) -> dict:
        gpu_util, gpu_mem = read_gpu()
        vm, swap = psutil.virtual_memory(), psutil.swap_memory()
        return {
            "time": time.time(),
            "gpu_util_pct": gpu_util,
            "gpu_mem_gb": gpu_mem,
            "ram_used_gb": (vm.total - vm.available) / GB,
            "ram_available_gb": vm.available / GB,
            "swap_used_gb": swap.used / GB,
            "pressure_level": read_memory_pressure(),
            **read_vllm_queue(self.vllm_metrics_url),
        }

    def _run(self):
        while not self._stop.is_set():
            self.samples.append(self._sample())
            self._stop.wait(self.interval)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=10)

    def latest(self) -> str:
        if not self.samples:
            return ""
        s = self.samples[-1]
        parts = []
        if s.get("gpu_util_pct") is not None:
            parts.append(f"GPU {s['gpu_util_pct']:.0f}%")
        parts.append(f"free {s['ram_available_gb']:.1f}G")
        if s.get("pressure_level") in (2, 4):
            parts.append(f"pressure {PRESSURE_LABELS[s['pressure_level']]}")
        if "num_requests_waiting" in s:
            parts.append(f"wait {s['num_requests_waiting']:.0f}")
        return " ".join(parts)

    def print_summary(self):
        if not self.samples:
            print("No resource samples collected")
            return
        df = pd.DataFrame(self.samples)
        t0 = df["time"].iloc[0]

        print(
            f"\nResources ({len(df)} samples every {self.interval:g}s; max is a lower bound of the true peak):"
        )
        print(f"  {'':18s} {'mean':>8s} {'p95':>8s} {'max':>8s}  max at")
        for col, label in [
            ("gpu_util_pct", "GPU util %"),
            ("gpu_mem_gb", "GPU mem GB"),
            ("ram_used_gb", "RAM used GB"),
            ("swap_used_gb", "Swap GB"),
            ("num_requests_running", "vLLM running"),
            ("num_requests_waiting", "vLLM waiting"),
        ]:
            if col not in df or not df[col].notna().any():
                continue
            s = df[col].dropna()
            at = df.loc[s.idxmax(), "time"] - t0
            print(
                f"  {label:18s} {s.mean():8.1f} {s.quantile(0.95):8.1f} {s.max():8.1f}  {at:6.0f}s"
            )

        min_free = df["ram_available_gb"].min()
        print(f"  {'RAM available min':18s} {min_free:8.1f} GB")

        swap_growth = df["swap_used_gb"].iloc[-1] - df["swap_used_gb"].iloc[0]
        print(f"  {'Swap growth':18s} {swap_growth:+8.2f} GB")

        if df["pressure_level"].notna().any():
            counts = df["pressure_level"].map(PRESSURE_LABELS).value_counts()
            print(
                f"  {'Memory pressure':18s} "
                + ", ".join(f"{k} {v}" for k, v in counts.items())
            )

        if swap_growth > 0.5 or (df["pressure_level"] == 4).any():
            print(
                "  ⚠️  Memory is tight (swap grew or pressure hit critical): lower concurrency"
            )
        elif (df["pressure_level"] == 2).any():
            print("  ⚠️  Memory pressure reached warning: little headroom left")
        if "num_requests_waiting" in df and df["num_requests_waiting"].mean() > 1:
            print("  ⚠️  Requests are queuing: higher concurrency will not help")

    def save(self, path):
        pd.DataFrame(self.samples).to_csv(path, index=False)
