"""
visualize.py
NFL Metric Stability Analysis — Visualization Script
Produces three figures covering Questions 1, 2, and 3.

Requirements:
    pip install pandas matplotlib numpy

Usage:
    python visualize.py

Expects these CSVs in the same directory:
    stability_results.csv
    predictive_results.csv
    opponent_adjust_results.csv

Output files:
    fig1_stability_curves.png
    fig2_quadrant_chart.png
    fig3_opponent_adjustment.png
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ============================================================
# LOAD DATA
# ============================================================

stab = pd.read_csv("stability_results.csv")
pred = pd.read_csv("predictive_results.csv")
adj  = pd.read_csv("opponent_adjust_results.csv")

# Remove any duplicate header rows (artifact of HDFS getmerge)
stab = stab[stab["metric"] != "metric"].copy()
pred = pred[pred["metric"] != "metric"].copy()
adj  = adj[adj["metric"]  != "metric"].copy()

# Cast numerics
for col in ["n_games", "correlation", "ci_lower", "ci_upper"]:
    stab[col] = pd.to_numeric(stab[col])
for col in ["n_games", "r_wins", "ci_lower_wins", "ci_upper_wins",
            "r_point_diff", "ci_lower_pd", "ci_upper_pd"]:
    pred[col] = pd.to_numeric(pred[col])
for col in ["n_games", "r_raw", "ci_lower_raw", "ci_upper_raw",
            "r_adj", "ci_lower_adj", "ci_upper_adj", "diff"]:
    adj[col] = pd.to_numeric(adj[col])

# ============================================================
# SHARED STYLE
# ============================================================

METRIC_COLORS = {
    "off_pass_epa":  "#378ADD",
    "off_rush_epa":  "#D85A30",
    "off_success":   "#7F77DD",
    "def_pass_epa":  "#1D9E75",
    "def_rush_epa":  "#BA7517",
    "def_success":   "#D4537E",
    "turnover_rate": "#888780",
}

METRIC_LABELS = {
    "off_pass_epa":  "Off passing EPA",
    "off_rush_epa":  "Off rushing EPA",
    "off_success":   "Off success rate",
    "def_pass_epa":  "Def passing EPA",
    "def_rush_epa":  "Def rushing EPA",
    "def_success":   "Def success rate",
    "turnover_rate": "Turnover rate",
}

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "grid.linestyle": "--",
})

# ============================================================
# FIGURE 1 — STABILITY CURVES (Question 1)
# ============================================================

fig1, ax = plt.subplots(figsize=(11, 6))

metrics_q1 = stab["metric"].unique()

for metric in metrics_q1:
    df = stab[stab["metric"] == metric].sort_values("n_games")
    color = METRIC_COLORS.get(metric, "#333333")
    ls = "--" if metric == "turnover_rate" else "-"
    ax.plot(df["n_games"], df["correlation"], color=color, linewidth=2.2,
            linestyle=ls, label=METRIC_LABELS.get(metric, metric), zorder=3)
    ax.fill_between(df["n_games"], df["ci_lower"], df["ci_upper"],
                    color=color, alpha=0.10, zorder=2)

# N=6 reference line
ax.axvline(x=6, color="#888780", linewidth=1.2, linestyle=":", zorder=1)
ax.text(6.15, 0.02, "N=6\n(⅓ season)", fontsize=9, color="#888780", va="bottom")

ax.set_xlabel("Games observed (N)", fontsize=12)
ax.set_ylabel("Split-half correlation (r)", fontsize=12)
ax.set_title("NFL metric stability: how quickly does each stat become reliable?",
             fontsize=13, fontweight="bold", pad=12)
ax.set_xlim(1, 8)
ax.set_ylim(0, 0.75)
ax.set_xticks(range(1, 9))
ax.legend(loc="upper right", fontsize=9.5, framealpha=0.9)

fig1.tight_layout()
fig1.savefig("fig1_stability_curves.png", dpi=180, bbox_inches="tight")
print("Saved fig1_stability_curves.png")

# ============================================================
# FIGURE 2 — QUADRANT CHART (Question 2)
# Combines Q1 stability at N=6 with Q2 predictive r_point_diff at N=6
# ============================================================

# Stability at N=6
stab_n6 = stab[stab["n_games"] == 6][["metric", "correlation"]].rename(
    columns={"correlation": "stability_r6"})

# Predictive r_point_diff at N=6 — use absolute value so defense plots correctly
# Exclude point_diff_baseline — plotted separately as a reference line
pred_n6 = pred[(pred["n_games"] == 6) & (pred["metric"] != "point_diff_baseline")][["metric", "r_point_diff"]].copy()
pred_n6["r_point_diff"] = pred_n6["r_point_diff"].abs()

quad = stab_n6.merge(pred_n6, on="metric")

# Baseline: early point differential predicting late point differential at N=6
baseline_r = pred[(pred["metric"] == "point_diff_baseline") & (pred["n_games"] == 6)]["r_point_diff"].values[0]

fig2, ax = plt.subplots(figsize=(9, 7))

# Quadrant shading
mid_x = quad["stability_r6"].median()
mid_y = quad["r_point_diff"].median()

ax.axhline(y=mid_y, color="#cccccc", linewidth=1, zorder=1)
ax.axvline(x=mid_x, color="#cccccc", linewidth=1, zorder=1)

ax.fill_between([mid_x, 0.75], mid_y, 0.75, color="#EAF3DE", alpha=0.4, zorder=0)
ax.fill_between([0,     mid_x], mid_y, 0.75, color="#FAEEDA", alpha=0.4, zorder=0)
ax.fill_between([mid_x, 0.75], 0,     mid_y, color="#FAEEDA", alpha=0.4, zorder=0)
ax.fill_between([0,     mid_x], 0,     mid_y, color="#FCEBEB", alpha=0.4, zorder=0)

# Quadrant labels
ax.text(0.68, 0.72, "reliable\n& predictive", fontsize=8.5, color="#3B6D11",
        ha="right", va="top", style="italic")
ax.text(0.02, 0.72, "predictive but\nunstable", fontsize=8.5, color="#854F0B",
        ha="left", va="top", style="italic")
ax.text(0.68, 0.02, "stable but\nnot predictive", fontsize=8.5, color="#854F0B",
        ha="right", va="bottom", style="italic")
ax.text(0.02, 0.02, "unreliable\n& weak", fontsize=8.5, color="#A32D2D",
        ha="left", va="bottom", style="italic")

# Plot each metric
for _, row in quad.iterrows():
    color = METRIC_COLORS.get(row["metric"], "#333333")
    ax.scatter(row["stability_r6"], row["r_point_diff"],
               color=color, s=120, zorder=4, edgecolors="white", linewidth=0.8)
    label = METRIC_LABELS.get(row["metric"], row["metric"])
    # offset labels to avoid overlap
    offsets = {
        "off_pass_epa": (0.01, 0.015),
        "off_rush_epa": (0.01, -0.02),
        "off_success":  (-0.01, 0.015),
        "def_pass_epa": (0.01, 0.015),
        "def_rush_epa": (0.01, -0.02),
        "def_success":  (-0.005, -0.022),
    }
    dx, dy = offsets.get(row["metric"], (0.01, 0.01))
    ax.annotate(label, (row["stability_r6"], row["r_point_diff"]),
                xytext=(row["stability_r6"] + dx, row["r_point_diff"] + dy),
                fontsize=9, color=color, fontweight="500")

# Point differential baseline reference line
ax.axhline(y=baseline_r, color="#333333", linewidth=1.2, linestyle=":",
           zorder=2, label=f"Point diff baseline (r={baseline_r:.2f})")
ax.text(0.72, baseline_r + 0.012, f"Point diff\nbaseline", fontsize=8.5,
        color="#333333", ha="right", va="bottom", style="italic")

ax.set_xlabel("Stability at N=6 games (split-half r)", fontsize=12)
ax.set_ylabel("Predictive power (r with remaining point differential)", fontsize=12)
ax.set_title("Which early-season metrics are both reliable and predictive?",
             fontsize=13, fontweight="bold", pad=12)
ax.set_xlim(0, 0.75)
ax.set_ylim(0, 0.75)

fig2.tight_layout()
fig2.savefig("fig2_quadrant_chart.png", dpi=180, bbox_inches="tight")
print("Saved fig2_quadrant_chart.png")

# ============================================================
# FIGURE 3 — OPPONENT ADJUSTMENT (Question 3)
# 2x3 grid, one panel per metric, raw vs adjusted overlaid
# ============================================================

metrics_q3 = ["off_pass_epa", "off_rush_epa", "off_success",
              "def_pass_epa", "def_rush_epa", "def_success"]

fig3, axes = plt.subplots(2, 3, figsize=(13, 8), sharey=True)
axes = axes.flatten()

for i, metric in enumerate(metrics_q3):
    ax = axes[i]
    df = adj[adj["metric"] == metric].sort_values("n_games")
    color = METRIC_COLORS[metric]

    ax.plot(df["n_games"], df["r_raw"], color=color, linewidth=2.2,
            label="Raw", zorder=3)
    ax.plot(df["n_games"], df["r_adj"], color=color, linewidth=2.2,
            linestyle="--", alpha=0.65, label="Opponent-adjusted", zorder=3)

    # CI bands for raw and adjusted
    ax.fill_between(df["n_games"], df["ci_lower_raw"], df["ci_upper_raw"],
                    color=color, alpha=0.10, zorder=2)
    ax.fill_between(df["n_games"], df["ci_lower_adj"], df["ci_upper_adj"],
                    color=color, alpha=0.07, zorder=2)

    ax.fill_between(df["n_games"], df["r_raw"], df["r_adj"],
                    where=(df["r_adj"] > df["r_raw"]),
                    color="#1D9E75", alpha=0.15, label="Adj helps")
    ax.fill_between(df["n_games"], df["r_raw"], df["r_adj"],
                    where=(df["r_adj"] <= df["r_raw"]),
                    color="#D85A30", alpha=0.15, label="Adj hurts")

    ax.axvline(x=6, color="#888780", linewidth=1, linestyle=":", zorder=1)
    ax.set_title(METRIC_LABELS[metric], fontsize=10.5,
                 fontweight="bold", color=color)
    ax.set_xlim(1, 8)
    ax.set_ylim(0, 0.75)
    ax.set_xticks([1, 2, 3, 4, 5, 6, 7, 8])
    if i >= 3:
        ax.set_xlabel("Games (N)", fontsize=9)
    if i % 3 == 0:
        ax.set_ylabel("Correlation (r)", fontsize=9)

# Shared legend
raw_patch = mpatches.Patch(color="#555555", label="Raw (unadjusted)")
adj_patch = mpatches.Patch(color="#555555", linestyle="--",
                            label="Opponent-adjusted", alpha=0.65)
help_patch = mpatches.Patch(color="#1D9E75", alpha=0.4, label="Adjustment helps (+)")
hurt_patch = mpatches.Patch(color="#D85A30", alpha=0.4, label="Adjustment hurts (−)")

from matplotlib.lines import Line2D
legend_handles = [
    Line2D([0], [0], color="#555555", linewidth=2, label="Raw"),
    Line2D([0], [0], color="#555555", linewidth=2, linestyle="--",
           alpha=0.65, label="Opponent-adjusted"),
    mpatches.Patch(color="#1D9E75", alpha=0.4, label="Adjustment helps (+)"),
    mpatches.Patch(color="#D85A30", alpha=0.4, label="Adjustment hurts (−)"),
]
fig3.legend(handles=legend_handles, loc="lower center", ncol=4,
            fontsize=9.5, framealpha=0.9, bbox_to_anchor=(0.5, -0.02))

fig3.suptitle("Does opponent adjustment improve metric stability?",
              fontsize=13, fontweight="bold", y=1.01)
fig3.tight_layout()
fig3.savefig("fig3_opponent_adjustment.png", dpi=180, bbox_inches="tight")
print("Saved fig3_opponent_adjustment.png")

print("\nAll figures saved.")
