#!/usr/bin/env python3
"""
plot_focus_graphs.py

Focused plots for:
1. Key syscall comparison from strace CSV
2. RDMA vs basic_ring comparison from main dataset CSV

Recommended usage:
  python3 plot_focus_graphs.py \
    --main-csv merged_main.csv \
    --strace-csv merged_strace.csv \
    --num-requests 20 \
    --outdir focus_plots
"""

from __future__ import annotations

import argparse
import ast
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import pandas as pd


KEY_SYSCALLS = [
    "sendto",
    "recvfrom",
    "read",
    "write",
    "epoll_wait",
    "poll",
    "ppoll",
    "select",
    "recvmsg",
    "sendmsg",
]
VARIANT_ORDER = ["tcp", "rdma", "basic_ring"]
FOCUS_VARIANTS = ["rdma", "basic_ring"]

COLORS = {
    "tcp": "#4C78A8",
    "rdma": "#F58518",
    "basic_ring": "#54A24B",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot focused syscall and RDMA-vs-basic_ring graphs.")
    parser.add_argument("--main-csv", help="Main experiment CSV")
    parser.add_argument("--strace-csv", help="Strace experiment CSV")
    parser.add_argument("--num-requests", type=int, required=True, help="Fixed num_requests to plot")
    parser.add_argument("--outdir", default="focus_plots", help="Output directory")
    return parser.parse_args()


def apply_style() -> None:
    plt.rcParams.update({
        "figure.figsize": (8, 5),
        "figure.dpi": 140,
        "axes.grid": True,
        "grid.alpha": 0.25,
        "grid.linestyle": "--",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.titlesize": 13,
        "axes.labelsize": 11,
        "legend.fontsize": 10,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
    })


def normalize_num_requests(df: pd.DataFrame) -> pd.DataFrame:
    if "result_num_requests" in df.columns:
        df["plot_num_requests"] = pd.to_numeric(df["result_num_requests"], errors="coerce")
    elif "num_requests" in df.columns:
        df["plot_num_requests"] = pd.to_numeric(df["num_requests"], errors="coerce")
    else:
        raise ValueError("Missing result_num_requests / num_requests column")
    return df


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
    syscall_calls: Dict[str, int] = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("% time") or line.startswith("------"):
            continue
        m_line = LINE_RE.match(line)
        if m_line:
            syscall = m_line.group("syscall")
            calls = int(m_line.group("calls"))
            syscall_calls[syscall] = syscall_calls.get(syscall, 0) + calls

    return {"syscall_calls": syscall_calls}


def plot_key_syscalls(strace_df: pd.DataFrame, num_requests: int, outdir: Path) -> None:
    df = strace_df.copy()
    df = normalize_num_requests(df)

    if "exit_code" in df.columns:
        df = df[df["exit_code"] == 0].copy()
    if "result_status" in df.columns:
        df = df[df["result_status"] == "OK"].copy()

    df = df[df["plot_num_requests"] == num_requests].copy()
    df = df[df["variant"].isin(VARIANT_ORDER)].copy()

    if "strace_summary" not in df.columns or df.empty:
        print("[SKIP] No usable strace data")
        return

    rows = []
    for _, row in df.iterrows():
        parsed = parse_strace_summary(row["strace_summary"])
        entry = {"variant": row["variant"]}
        for sc in KEY_SYSCALLS:
            entry[sc] = parsed["syscall_calls"].get(sc, 0)
        rows.append(entry)

    if not rows:
        print("[SKIP] No parsed syscall rows")
        return

    sdf = pd.DataFrame(rows)
    agg = sdf.groupby("variant", as_index=False)[KEY_SYSCALLS].mean()

    present = [sc for sc in KEY_SYSCALLS if agg[sc].sum() > 0]
    if not present:
        print("[SKIP] No key syscalls present")
        return

    agg["variant"] = pd.Categorical(agg["variant"], categories=VARIANT_ORDER, ordered=True)
    agg = agg.sort_values("variant")

    x = list(range(len(agg)))
    width = 0.8 / len(present)

    fig, ax = plt.subplots(figsize=(max(10, len(agg) * 1.8), 5.5))
    for idx, sc in enumerate(present):
        offsets = [i - 0.4 + width / 2 + idx * width for i in x]
        ax.bar(offsets, agg[sc], width=width, label=sc)

    ax.set_xticks(x)
    ax.set_xticklabels(agg["variant"].astype(str).tolist())
    ax.set_ylabel("Average syscall count")
    ax.set_title(f"Key Syscalls at num_requests={num_requests} (×10³)")
    ax.legend(frameon=False, fontsize=8)
    fig.tight_layout()
    fig.savefig(outdir / f"key_syscalls_numreq_{num_requests}.png", bbox_inches="tight")
    plt.close(fig)


def summarize_main(main_df: pd.DataFrame, num_requests: int) -> pd.DataFrame:
    df = main_df.copy()
    df = normalize_num_requests(df)

    if "exit_code" in df.columns:
        df = df[df["exit_code"] == 0].copy()
    if "result_status" in df.columns:
        df = df[df["result_status"] == "OK"].copy()

    df = df[df["plot_num_requests"] == num_requests].copy()
    df = df[df["variant"].isin(FOCUS_VARIANTS)].copy()

    needed = [
        "result_avg_us",
        "result_p95_us",
        "result_throughput_rps",
    ]
    for c in needed:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    summary = (
        df.groupby("variant", as_index=False)
        .agg(
            avg_us_mean=("result_avg_us", "mean"),
            avg_us_std=("result_avg_us", "std"),
            p95_us_mean=("result_p95_us", "mean"),
            p95_us_std=("result_p95_us", "std"),
            throughput_mean=("result_throughput_rps", "mean"),
            throughput_std=("result_throughput_rps", "std"),
            count=("result_avg_us", "count"),
        )
    )
    summary["variant"] = pd.Categorical(summary["variant"], categories=FOCUS_VARIANTS, ordered=True)
    summary = summary.sort_values("variant")
    return summary


def plot_focus_bar(summary: pd.DataFrame, mean_col: str, std_col: str, ylabel: str, title: str, outpath: Path) -> None:
    if summary.empty:
        print(f"[SKIP] No data for {title}")
        return

    fig, ax = plt.subplots(figsize=(6.5, 4.8))
    x = range(len(summary))
    bars = ax.bar(
        list(x),
        summary[mean_col],
        yerr=summary[std_col].fillna(0),
        capsize=4,
        color=[COLORS.get(v, "#777777") for v in summary["variant"].astype(str)],
        width=0.5,
    )
    ax.set_xticks(list(x))
    ax.set_xticklabels(summary["variant"].astype(str).tolist())
    ax.set_ylabel(ylabel)
    ax.set_title(title)

    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            f"{height:.1f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    fig.tight_layout()
    fig.savefig(outpath, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    apply_style()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if args.strace_csv:
        strace_df = pd.read_csv(args.strace_csv)
        plot_key_syscalls(strace_df, args.num_requests, outdir)

    if args.main_csv:
        main_df = pd.read_csv(args.main_csv)
        summary = summarize_main(main_df, args.num_requests)

        plot_focus_bar(
            summary,
            mean_col="avg_us_mean",
            std_col="avg_us_std",
            ylabel="Average latency (us)",
            title=f"RDMA vs Basic Ring: Avg Latency at num_requests={args.num_requests}",
            outpath=outdir / f"rdma_vs_basic_avg_latency_numreq_{args.num_requests}.png",
        )

        plot_focus_bar(
            summary,
            mean_col="throughput_mean",
            std_col="throughput_std",
            ylabel="Throughput (requests/sec)",
            title=f"RDMA vs Basic Ring: Throughput at num_requests={args.num_requests}",
            outpath=outdir / f"rdma_vs_basic_throughput_numreq_{args.num_requests}.png",
        )

        plot_focus_bar(
            summary,
            mean_col="p95_us_mean",
            std_col="p95_us_std",
            ylabel="P95 latency (us)",
            title=f"RDMA vs Basic Ring: P95 Latency at num_requests={args.num_requests}",
            outpath=outdir / f"rdma_vs_basic_p95_latency_numreq_{args.num_requests}.png",
        )

    print(f"[OK] Wrote focused plots to: {outdir}")


if __name__ == "__main__":
    main()