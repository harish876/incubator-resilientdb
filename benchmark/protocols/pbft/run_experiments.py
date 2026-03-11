#!/usr/bin/env python3
"""
ecs251_run_experiments.py

Runs PBFT consensus_server selftest for multiple transport variants and logs metrics into a CSV.

Current variants enabled:
- tcp
- rdma
- basic_ring

fetch_add is intentionally left out for now.

Easy to extend later:
- Add new variants by adding entries in VARIANTS.
- Add new collectors by adding a function and wiring it into run_one().

USAGE EXAMPLES:

1) Build once, then run 5 trials of all current variants:
   python3 run_experiments.py --ip 128.110.216.215 --base-port 26000 --num-requests 1000 --trials 5 --build

2) Only run tcp + rdma:
   python3 run_experiments.py --ip 128.110.216.215 --base-port 26000 --variants tcp rdma --trials 3 --build

3) Add strace and perf (often requires sudo):
   sudo python3 ecs251_run_experiments.py --ip 128.110.216.215 --base-port 26000 --num-requests 1000 --trials 3 --build --enable-strace --enable-perf

Notes:
- Selftest creates 4 replicas in-process and sends 2 requests (SET+GET).
  This is bring-up/correctness + micro latency printing, not a throughput benchmark.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

# -----------------------------
# Hardcoded fixed paths
# -----------------------------
REPO_PATH = "/users/Pranav14/incubator-resilientdb"
RESULTS_CSV_PATH = "/users/Pranav14/incubator-resilientdb/benchmark/protocols/pbft/results_csv/main_dataset_20req.csv"

# Bazel target + built binary for consensus_server
DEFAULT_BAZEL_TARGET = "//benchmark/protocols/pbft:consensus_server"
DEFAULT_BUILT_BIN_REL = "bazel-bin/benchmark/protocols/pbft/consensus_server"

# -----------------------------
# Variants enabled for now
# -----------------------------
VARIANTS: Dict[str, str] = {
    "tcp":        "--selftest tcp {ip} {base_port} {num_requests}",
    "rdma":       "--selftest rdma {ip} {base_port} {num_requests}",
    "basic_ring": "--selftest basic_ring {ip} {base_port} {num_requests}",
}

# Future placeholder if/when fetch_add gets integrated cleanly:
# "rdma_fetchadd": "--selftest rdma_fetchadd {ip} {base_port}",

LAT_RE = re.compile(r"req client latency:([0-9.eE+-]+)")
SELFTEST_PASS_RE = re.compile(r"Self-test passed mode=([a-zA-Z0-9_]+)")
RESULT_RE = re.compile(
    r"RESULT "
    r"mode=(?P<mode>\S+) "
    r"num_requests=(?P<num_requests>\d+) "
    r"total_time_s=(?P<total_time_s>[0-9.eE+-]+) "
    r"throughput_rps=(?P<throughput_rps>[0-9.eE+-]+) "
    r"avg_us=(?P<avg_us>[0-9.eE+-]+) "
    r"p50_us=(?P<p50_us>[0-9.eE+-]+) "
    r"p95_us=(?P<p95_us>[0-9.eE+-]+) "
    r"p99_us=(?P<p99_us>[0-9.eE+-]+) "
    r"min_us=(?P<min_us>[0-9.eE+-]+) "
    r"max_us=(?P<max_us>[0-9.eE+-]+) "
    r"status=(?P<status>\S+)"
)


@dataclass
class RunMetrics:
    timestamp_iso: str
    variant: str
    ip: str
    base_port: int
    num_requests: int
    trial: int
    exit_code: int
    wall_time_s: float

    # Old parsed fields
    client_latency_s: Optional[float]
    selftest_mode_reported: Optional[str]

    # New parsed RESULT fields
    result_mode: Optional[str]
    result_num_requests: Optional[int]
    result_total_time_s: Optional[float]
    result_throughput_rps: Optional[float]
    result_avg_us: Optional[float]
    result_p50_us: Optional[float]
    result_p95_us: Optional[float]
    result_p99_us: Optional[float]
    result_min_us: Optional[float]
    result_max_us: Optional[float]
    result_status: Optional[str]

    stdout_tail: str
    stderr_tail: str
    strace_summary: str
    perf_stat: str


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def run_cmd(
    cmd: List[str],
    cwd: str,
    timeout_s: Optional[int],
    env: Optional[dict] = None,
) -> Tuple[int, str, str, float]:
    start = time.perf_counter()
    p = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        out, err = p.communicate(timeout=timeout_s)
        rc = p.returncode
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
        rc = 124
        err = (err or "") + "\n[TIMEOUT] command exceeded timeout\n"
    wall = time.perf_counter() - start
    return rc, out or "", err or "", wall


def tail(s: str, max_lines: int = 30) -> str:
    lines = s.splitlines()
    if len(lines) <= max_lines:
        return s.strip()
    return "\n".join(lines[-max_lines:]).strip()


def parse_client_latency(output: str) -> Optional[float]:
    m = LAT_RE.search(output)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def parse_selftest_mode(output: str) -> Optional[str]:
    m = SELFTEST_PASS_RE.search(output)
    return m.group(1) if m else None


def build_target(repo: str, bazel_target: str) -> None:
    cmd = ["bazel", "build", bazel_target]
    rc, out, err, wall = run_cmd(cmd, cwd=repo, timeout_s=None)
    if rc != 0:
        print("[ERROR] Bazel build failed.")
        print(err)
        sys.exit(rc)
    print(f"[OK] Build succeeded in {wall:.2f}s")


def make_program_path(repo: str, program_path: str) -> str:
    if os.path.isabs(program_path):
        return program_path
    return os.path.join(repo, program_path)


def make_variant_args(variant: str, ip: str, base_port: int, num_requests: int) -> List[str]:
    if variant not in VARIANTS:
        raise ValueError(f"Unknown variant '{variant}'. Known: {sorted(VARIANTS.keys())}")
    tmpl = VARIANTS[variant]
    arg_str = tmpl.format(ip=ip, base_port=base_port, num_requests=num_requests)
    return shlex.split(arg_str)


def collect_strace(
    repo: str,
    program: str,
    prog_args: List[str],
    timeout_s: Optional[int],
) -> Tuple[int, str, str, float]:
    cmd = ["strace", "-f", "-c", program] + prog_args
    return run_cmd(cmd, cwd=repo, timeout_s=timeout_s)


def collect_perf_stat(
    repo: str,
    program: str,
    prog_args: List[str],
    timeout_s: Optional[int],
) -> Tuple[int, str, str, float]:
    cmd = [
        "perf",
        "stat",
        "-e",
        "cycles,instructions,context-switches,cpu-migrations,page-faults,cache-references,cache-misses",
        program,
    ] + prog_args
    return run_cmd(cmd, cwd=repo, timeout_s=timeout_s)


def run_one(
    repo: str,
    program: str,
    variant: str,
    ip: str,
    base_port: int,
    num_requests: int,
    trial: int,
    timeout_s: Optional[int],
    enable_strace: bool,
    enable_perf: bool,
) -> RunMetrics:
    prog_args = make_variant_args(variant, ip, base_port, num_requests)

    rc, out, err, wall = run_cmd([program] + prog_args, cwd=repo, timeout_s=timeout_s)

    combined = out + "\n" + err
    lat = parse_client_latency(combined)
    mode_reported = parse_selftest_mode(combined)
    result_metrics = parse_result_metrics(combined)

    strace_summary = ""
    perf_stat = ""

    if enable_strace:
        _, out_s, err_s, _ = collect_strace(repo, program, prog_args, timeout_s)
        strace_summary = tail(out_s + "\n" + err_s, max_lines=200)

    if enable_perf:
        _, out_p, err_p, _ = collect_perf_stat(repo, program, prog_args, timeout_s)
        perf_stat = tail(out_p + "\n" + err_p, max_lines=200)

    return RunMetrics(
        timestamp_iso=dt.datetime.now().isoformat(timespec="seconds"),
        variant=variant,
        ip=ip,
        base_port=base_port,
        trial=trial,
        exit_code=rc,
        wall_time_s=wall,
        client_latency_s=lat,
        selftest_mode_reported=mode_reported,
        result_mode=result_metrics["result_mode"],
        result_num_requests=result_metrics["result_num_requests"],
        result_total_time_s=result_metrics["result_total_time_s"],
        result_throughput_rps=result_metrics["result_throughput_rps"],
        result_avg_us=result_metrics["result_avg_us"],
        result_p50_us=result_metrics["result_p50_us"],
        result_p95_us=result_metrics["result_p95_us"],
        result_p99_us=result_metrics["result_p99_us"],
        result_min_us=result_metrics["result_min_us"],
        result_max_us=result_metrics["result_max_us"],
        result_status=result_metrics["result_status"],
        stdout_tail=tail(out, max_lines=40),
        stderr_tail=tail(err, max_lines=40),
        strace_summary=strace_summary,
        perf_stat=perf_stat,
        num_requests=num_requests,
    )


def append_csv(path: str, rows: List[RunMetrics]) -> None:
    ensure_parent_dir(path)
    file_exists = os.path.exists(path)

    dict_rows = [asdict(r) for r in rows]
    fieldnames = list(dict_rows[0].keys()) if dict_rows else []

    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        for r in dict_rows:
            w.writerow(r)

def parse_result_metrics(output: str) -> Dict[str, Optional[object]]:
    m = RESULT_RE.search(output)
    if not m:
        return {
            "result_mode": None,
            "result_num_requests": None,
            "result_total_time_s": None,
            "result_throughput_rps": None,
            "result_avg_us": None,
            "result_p50_us": None,
            "result_p95_us": None,
            "result_p99_us": None,
            "result_min_us": None,
            "result_max_us": None,
            "result_status": None,
        }

    return {
        "result_mode": m.group("mode"),
        "result_num_requests": int(m.group("num_requests")),
        "result_total_time_s": float(m.group("total_time_s")),
        "result_throughput_rps": float(m.group("throughput_rps")),
        "result_avg_us": float(m.group("avg_us")),
        "result_p50_us": float(m.group("p50_us")),
        "result_p95_us": float(m.group("p95_us")),
        "result_p99_us": float(m.group("p99_us")),
        "result_min_us": float(m.group("min_us")),
        "result_max_us": float(m.group("max_us")),
        "result_status": m.group("status"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run PBFT selftest experiments (TCP/RDMA/basic_ring) and log metrics to CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog="Tip: Bind to an IP that exists on this host (use `hostname -I`). perf/strace may require sudo.",
    )

    parser.add_argument(
        "--bazel-target",
        default=DEFAULT_BAZEL_TARGET,
        help="Bazel target to build (optional)",
    )
    parser.add_argument(
        "--program",
        default=DEFAULT_BUILT_BIN_REL,
        help="Program path to run (absolute or repo-relative). Default is bazel-bin output for consensus_server.",
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="Run `bazel build <target>` once before experiments.",
    )

    parser.add_argument(
        "--ip",
        required=True,
        help="Bind IP used by selftest replicas (must exist on this machine)",
    )
    parser.add_argument(
        "--base-port",
        type=int,
        default=26000,
        help="Base port for selftest (replicas use base+1..base+4)",
    )
    parser.add_argument(
        "--variants",
        nargs="+",
        default=["tcp", "rdma", "basic_ring"],
        help=f"Variants to run. Known: {sorted(VARIANTS.keys())}",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=3,
        help="How many trials per variant",
    )
    parser.add_argument(
        "--timeout-s",
        type=int,
        default=120,
        help="Timeout per run (seconds)",
    )

    parser.add_argument(
        "--enable-strace",
        action="store_true",
        help="Collect `strace -f -c` summary for each run (slow)",
    )
    parser.add_argument(
        "--enable-perf",
        action="store_true",
        help="Collect `perf stat` counters for each run (may require sudo)",
    )

    parser.add_argument(
        "--num-requests",
        type=int,
        default=1000,
        help="Number of measured GET requests to run in selftest",
    )

    args = parser.parse_args()

    repo = os.path.abspath(REPO_PATH)
    if not os.path.isdir(repo):
        print(f"[ERROR] Hardcoded REPO_PATH not found: {repo}")
        print("Edit REPO_PATH at the top of this script to match your machine.")
        return 2

    if args.build:
        build_target(repo, args.bazel_target)

    program = make_program_path(repo, args.program)
    if not os.path.exists(program):
        print(f"[ERROR] Program not found: {program}")
        print("Did you run with --build? Or is --program pointing to the right bazel-bin path?")
        return 2

    all_rows: List[RunMetrics] = []
    print(f"[INFO] Repo: {repo}")
    print(f"[INFO] Writing results to: {RESULTS_CSV_PATH}")

    for variant in args.variants:
        print(f"\n[INFO] Variant: {variant}")
        for t in range(1, args.trials + 1):
            print(f"[INFO]  Trial {t}/{args.trials} ...")
            row = run_one(
                repo=repo,
                program=program,
                variant=variant,
                ip=args.ip,
                base_port=args.base_port,
                num_requests=args.num_requests,
                trial=t,
                timeout_s=args.timeout_s,
                enable_strace=args.enable_strace,
                enable_perf=args.enable_perf,
            )
            all_rows.append(row)
            print(
                f"[INFO]    exit={row.exit_code} "
                f"wall={row.wall_time_s:.4f}s "
                f"mode={row.result_mode or row.selftest_mode_reported or 'NA'} "
                f"reqs={row.result_num_requests if row.result_num_requests is not None else args.num_requests} "
                f"avg_us={row.result_avg_us if row.result_avg_us is not None else 'NA'} "
                f"p95_us={row.result_p95_us if row.result_p95_us is not None else 'NA'} "
                f"throughput_rps={row.result_throughput_rps if row.result_throughput_rps is not None else 'NA'} "
                f"status={row.result_status or 'NA'}"
            )

    if all_rows:
        append_csv(RESULTS_CSV_PATH, all_rows)
        print(f"\n[OK] Appended {len(all_rows)} rows to {RESULTS_CSV_PATH}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())