#!/usr/bin/env python3
"""
plot_experiment_results.py

Reads the CSVs produced by ecs251_run_experiments.py and generates presentation-ready plots.

Supported inputs:
- main dataset CSV: latency + throughput metrics from the RESULT line
- strace dataset CSV: strace_summary column collected with --enable-strace

Graphs generated (if data is available):
1. avg_latency_us.png
2. p95_latency_us.png
3. p99_latency_us.png
4. throughput_rps.png
5. latency_percentiles_grouped.png
6. total_syscalls.png              (from strace CSV)
7. syscall_time_s.png              (from strace CSV)
8. key_syscalls_grouped.png        (from strace CSV)

Usage examples:
  python3 plot_experiment_results.py \
    --main-csv /path/to/main_dataset.csv \
    --strace-csv /path/to/strace_dataset.csv \
    --outdir /path/to/plots

  python3 plot_experiment_results.py \
    --main-csv /path/to/main_dataset.csv \
    --outdir /path/to/plots

Notes:
- The script expects columns like result_avg_us, result_p95_us, result_p99_us,
  result_throughput_rps, variant, and strace_summary.
- It is robust to missing columns; unsupported plots are skipped.
- Error bars are standard deviation across trials.
"""

from __future__ import annotations

import argparse
import ast
import os
import re
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import pandas as pd


DEFAULT_OUTDIR = "/users/Pranav14/incubator-resilientdb/benchmark/protocols/pbft/results_csv"
VARIANT_ORDER = ["rdma", "basic_ring"]
KEY_SYSCALLS = [
    "sendto",
    "recvfrom",
    "read",
    "write",
    "epoll_wait",
    "poll",
    "ppoll",
    "select",
    "futex",
    "connect",
    "accept",
    "accept4",
    "recvmsg",
    "sendmsg",
]


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def ordered_variants(values: List[str]) -> List[str]:
    seen = set(values)
    ordered = [v for v in VARIANT_ORDER if v in seen]
    leftovers = sorted(v for v in seen if v not in VARIANT_ORDER)
    return ordered + leftovers


def bar_with_error(
    df: pd.DataFrame,
    value_col: str,
    ylabel: str,
    title: str,
    outpath: str,
) -> None:
    if value_col not in df.columns:
        print(f"[SKIP] Missing column: {value_col}")
        return

    agg = (
        df.groupby("variant", dropna=False)[value_col]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    if agg.empty:
        print(f"[SKIP] No data for {value_col}")
        return

    order = ordered_variants(agg["variant"].astype(str).tolist())
    agg["variant"] = pd.Categorical(agg["variant"], categories=order, ordered=True)
    agg = agg.sort_values("variant")
    agg["std"] = agg["std"].fillna(0)

    plt.figure(figsize=(10, 5))
    #plt.bar(agg["variant"].astype(str), agg["mean"], yerr=agg["std"], capsize=4)
    bars = plt.bar(
        agg["variant"].astype(str),
        agg["mean"],
        width=0.5,
        yerr=agg["std"],
        capsize=4
    )
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2,
            height,
            f"{height/1000:.1f} ms",
            ha='center',
            va='bottom'
        )
    plt.ylabel(ylabel)
    plt.title(title)
    plt.ylim(min(agg["mean"]) * 0.9, max(agg["mean"]) * 1.05)
    plt.yscale("log")
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()
    print(f"[OK] Wrote {outpath}")


def grouped_latency_plot(df: pd.DataFrame, outpath: str) -> None:
    needed = ["result_p50_us", "result_p95_us", "result_p99_us"]
    if not all(c in df.columns for c in needed):
        print("[SKIP] Missing one of result_p50_us/result_p95_us/result_p99_us")
        return

    agg = (
        df.groupby("variant", dropna=False)[needed]
        .mean()
        .reset_index()
    )
    if agg.empty:
        print("[SKIP] No latency percentile data")
        return

    order = ordered_variants(agg["variant"].astype(str).tolist())
    agg["variant"] = pd.Categorical(agg["variant"], categories=order, ordered=True)
    agg = agg.sort_values("variant")

    x = list(range(len(agg)))
    width = 0.25

    plt.figure(figsize=(9, 5))
    plt.bar([i - width for i in x], agg["result_p50_us"], width=width, label="P50")
    plt.bar(x, agg["result_p95_us"], width=width, label="P95")
    plt.bar([i + width for i in x], agg["result_p99_us"], width=width, label="P99")
    plt.xticks(x, agg["variant"].astype(str).tolist())
    plt.ylabel("Latency (us)")
    plt.title("Latency Percentiles by Variant")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()
    print(f"[OK] Wrote {outpath}")


TOTAL_RE = re.compile(
    r"^\s*100\.00\s+(?P<seconds>[0-9.]+)\s+\S+\s+(?P<calls>\d+)\s+(?:\d+\s+)?total\s*$"
)
LINE_RE = re.compile(
    r"^\s*[0-9.]+\s+(?P<seconds>[0-9.]+)\s+\S+\s+(?P<calls>\d+)\s+(?:(?P<errors>\d+)\s+)?(?P<syscall>[A-Za-z0-9_]+)\s*$"
)


def _clean_text_blob(val: object) -> str:
    if val is None:
        return ""
    s = str(val)
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        try:
            s = ast.literal_eval(s)
        except Exception:
            pass
    return str(s)


def parse_strace_summary(summary: object) -> Dict[str, object]:
    text = _clean_text_blob(summary)
    total_calls: Optional[int] = None
    total_seconds: Optional[float] = None
    syscall_calls: Dict[str, int] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("% time") or line.startswith("------"):
            continue

        m_total = TOTAL_RE.match(line)
        if m_total:
            total_seconds = float(m_total.group("seconds"))
            total_calls = int(m_total.group("calls"))
            continue

        m_line = LINE_RE.match(line)
        if m_line:
            syscall = m_line.group("syscall")
            calls = int(m_line.group("calls"))
            syscall_calls[syscall] = syscall_calls.get(syscall, 0) + calls

    return {
        "total_syscalls": total_calls,
        "total_syscall_time_s": total_seconds,
        "syscall_calls": syscall_calls,
    }


def add_strace_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "strace_summary" not in df.columns:
        return df

    parsed = df["strace_summary"].apply(parse_strace_summary)
    df = df.copy()
    df["total_syscalls"] = parsed.apply(lambda x: x["total_syscalls"])
    df["total_syscall_time_s"] = parsed.apply(lambda x: x["total_syscall_time_s"])
    df["syscall_calls_map"] = parsed.apply(lambda x: x["syscall_calls"])
    return df


def plot_total_syscalls(df: pd.DataFrame, outpath: str) -> None:
    if "total_syscalls" not in df.columns:
        print("[SKIP] Missing total_syscalls")
        return
    tmp = df.dropna(subset=["total_syscalls"])
    if tmp.empty:
        print("[SKIP] No total_syscalls data")
        return
    bar_with_error(
        tmp,
        "total_syscalls",
        "Total syscalls",
        "Total Syscalls by Variant",
        outpath,
    )


def plot_syscall_time(df: pd.DataFrame, outpath: str) -> None:
    if "total_syscall_time_s" not in df.columns:
        print("[SKIP] Missing total_syscall_time_s")
        return
    tmp = df.dropna(subset=["total_syscall_time_s"])
    if tmp.empty:
        print("[SKIP] No total_syscall_time_s data")
        return
    bar_with_error(
        tmp,
        "total_syscall_time_s",
        "Seconds",
        "Total Time Spent in Syscalls by Variant",
        outpath,
    )


def plot_key_syscalls(df: pd.DataFrame, outpath: str) -> None:
    if "syscall_calls_map" not in df.columns:
        print("[SKIP] Missing syscall_calls_map")
        return

    rows = []
    for _, row in df.iterrows():
        variant = row.get("variant")
        m = row.get("syscall_calls_map")
        if not isinstance(m, dict):
            continue
        entry = {"variant": variant}
        for sc in KEY_SYSCALLS:
            entry[sc] = m.get(sc, 0)
        rows.append(entry)

    if not rows:
        print("[SKIP] No parsed per-syscall data")
        return

    tmp = pd.DataFrame(rows)
    agg = tmp.groupby("variant", dropna=False)[KEY_SYSCALLS].mean().reset_index()
    order = ordered_variants(agg["variant"].astype(str).tolist())
    agg["variant"] = pd.Categorical(agg["variant"], categories=order, ordered=True)
    agg = agg.sort_values("variant")

    present = [c for c in KEY_SYSCALLS if agg[c].sum() > 0]
    if not present:
        print("[SKIP] No key syscall counts found")
        return

    x = list(range(len(agg)))
    width = 0.8 / len(present)

    plt.figure(figsize=(max(10, len(agg) * 1.5), 6))
    for idx, sc in enumerate(present):
        offsets = [i - 0.4 + width / 2 + idx * width for i in x]
        plt.bar(offsets, agg[sc], width=width, label=sc)

    plt.xticks(x, agg["variant"].astype(str).tolist())
    plt.ylabel("Average syscall count")
    plt.title("Key Syscall Counts by Variant")
    plt.legend(fontsize=8)
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()
    print(f"[OK] Wrote {outpath}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Plot ECS251 experiment results from main and strace CSV files."
    )
    parser.add_argument("--main-csv", help="Path to the main dataset CSV")
    parser.add_argument("--strace-csv", help="Path to the strace dataset CSV")
    parser.add_argument(
        "--outdir",
        default=DEFAULT_OUTDIR,
        help="Directory to save plots",
    )
    parser.add_argument(
        "--variants",
        nargs="+",
        help="Subset of variants to plot (e.g. rdma basic_ring tcp)"
    )
    args = parser.parse_args()

    if not args.main_csv and not args.strace_csv:
        print("[ERROR] Provide at least one of --main-csv or --strace-csv")
        return 2

    ensure_dir(args.outdir)

    if args.main_csv:
        main_df = pd.read_csv(args.main_csv)
        if args.variants:
            main_df = main_df[main_df["variant"].isin(args.variants)]
        print(f"[INFO] Loaded main CSV: {args.main_csv} ({len(main_df)} rows)")

        bar_with_error(
            main_df,
            "result_avg_us",
            "Latency (us)",
            "Average Latency by Variant",
            os.path.join(args.outdir, "avg_latency_us.png"),
        )
        bar_with_error(
            main_df,
            "result_p95_us",
            "Latency (us)",
            "P95 Latency by Variant",
            os.path.join(args.outdir, "p95_latency_us.png"),
        )
        bar_with_error(
            main_df,
            "result_p99_us",
            "Latency (us)",
            "P99 Latency by Variant",
            os.path.join(args.outdir, "p99_latency_us.png"),
        )
        bar_with_error(
            main_df,
            "result_throughput_rps",
            "Requests/sec",
            "Throughput by Variant",
            os.path.join(args.outdir, "throughput_rps.png"),
        )
        grouped_latency_plot(
            main_df,
            os.path.join(args.outdir, "latency_percentiles_grouped.png"),
        )

    if args.strace_csv:
        strace_df = pd.read_csv(args.strace_csv)
        if args.variants:
            strace_df = strace_df[strace_df["variant"].isin(args.variants)]
        strace_df = add_strace_columns(strace_df)
        print(f"[INFO] Loaded strace CSV: {args.strace_csv} ({len(strace_df)} rows)")

        plot_total_syscalls(
            strace_df,
            os.path.join(args.outdir, "total_syscalls.png"),
        )
        plot_syscall_time(
            strace_df,
            os.path.join(args.outdir, "syscall_time_s.png"),
        )
        plot_key_syscalls(
            strace_df,
            os.path.join(args.outdir, "key_syscalls_grouped.png"),
        )

    print(f"[DONE] Plots saved under: {args.outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())