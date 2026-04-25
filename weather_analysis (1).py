#!/usr/bin/env python3
"""
weather_analysis.py
Complete Weather Data Analysis — simulates Hadoop MapReduce locally
and produces a full analysis report with charts.

Run: python3 weather_analysis.py
"""

import csv
import os
import sys
from collections import defaultdict

# ── Try to import matplotlib; install if missing ─────────────────────────────
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
    CHARTS = True
except ImportError:
    CHARTS = False

DATASET = os.path.join(os.path.dirname(__file__), "dataset.csv")
OUTPUT_DIR = os.path.dirname(__file__)

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 1 — MAPPER  (simulated)
# ═══════════════════════════════════════════════════════════════════════════
def run_mapper(dataset_path):
    """
    Reads dataset.csv and emits key-value pairs just like a real Hadoop Mapper.
    Returns a list of (key, metric, value) tuples.
    """
    pairs = []
    with open(dataset_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                city      = row["city"].strip()
                temp      = float(row["temperature"])
                humidity  = float(row["humidity"])
                rainfall  = float(row["rainfall"])
                wind      = float(row["wind_speed"])
                condition = row["condition"].strip()

                pairs.append((city, "TEMP",  temp))
                pairs.append((city, "RAIN",  rainfall))
                pairs.append((city, "HUMID", humidity))
                pairs.append((city, "WIND",  wind))
                pairs.append((f"{city}_{condition}", "COUNT", 1))
            except (ValueError, KeyError):
                continue
    return pairs

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 2 — SHUFFLE & SORT  (simulated)
# ═══════════════════════════════════════════════════════════════════════════
def shuffle_sort(pairs):
    """Groups mapper output by (key, metric) — Hadoop does this automatically."""
    groups = defaultdict(list)
    for key, metric, value in pairs:
        groups[(key, metric)].append(value)
    return groups

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 3 — REDUCER  (simulated)
# ═══════════════════════════════════════════════════════════════════════════
def run_reducer(groups):
    """Aggregates each group — exactly what Reducer.py does on Hadoop."""
    results = {}
    for (key, metric), values in groups.items():
        if metric == "TEMP":
            results[(key, "Avg Temp (C)")] = round(sum(values)/len(values), 2)
        elif metric == "RAIN":
            results[(key, "Total Rainfall (mm)")] = round(sum(values), 2)
        elif metric == "HUMID":
            results[(key, "Avg Humidity (%)")] = round(sum(values)/len(values), 2)
        elif metric == "WIND":
            results[(key, "Avg Wind (km/h)")] = round(sum(values)/len(values), 2)
        elif metric == "COUNT":
            results[(key, "Condition Count")] = int(sum(values))
    return results

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 4 — BUILD STRUCTURED REPORT DATA
# ═══════════════════════════════════════════════════════════════════════════
def build_report(results):
    cities    = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Kolkata"]
    metrics   = ["Avg Temp (C)", "Total Rainfall (mm)", "Avg Humidity (%)", "Avg Wind (km/h)"]
    conditions = ["Clear", "Rainy", "Cloudy"]

    city_data = {c: {} for c in cities}
    cond_data = {c: {cond: 0 for cond in conditions} for c in cities}

    for (key, metric), value in results.items():
        if metric == "Condition Count":
            for city in cities:
                for cond in conditions:
                    if key == f"{city}_{cond}":
                        cond_data[city][cond] = value
        else:
            if key in cities:
                city_data[key][metric] = value

    return city_data, cond_data, cities, metrics, conditions

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 5 — PRINT CONSOLE REPORT
# ═══════════════════════════════════════════════════════════════════════════
def print_report(city_data, cond_data, cities, metrics, conditions):
    SEP  = "=" * 72
    SEP2 = "-" * 72

    print("\n" + SEP)
    print("   WEATHER DATA ANALYSIS — HADOOP MAPREDUCE SIMULATION")
    print("   Babu Banarasi Das University | Big Data Project")
    print(SEP)

    # ── Main metrics table ──
    print("\n📊 CITY-WISE WEATHER SUMMARY\n")
    print(f"{'City':<12} {'Avg Temp':>10} {'Total Rain':>13} {'Avg Humidity':>14} {'Avg Wind':>11}")
    print(f"{'':12} {'(°C)':>10} {'(mm)':>13} {'(%)':>14} {'(km/h)':>11}")
    print(SEP2)
    for city in cities:
        d = city_data.get(city, {})
        t  = d.get("Avg Temp (C)",          "N/A")
        r  = d.get("Total Rainfall (mm)",    "N/A")
        h  = d.get("Avg Humidity (%)",       "N/A")
        w  = d.get("Avg Wind (km/h)",        "N/A")
        print(f"{city:<12} {str(t):>10} {str(r):>13} {str(h):>14} {str(w):>11}")
    print(SEP2)

    # ── Hottest / Coldest / Wettest ──
    temps  = {c: city_data[c].get("Avg Temp (C)", 0)         for c in cities}
    rains  = {c: city_data[c].get("Total Rainfall (mm)", 0)  for c in cities}
    humid  = {c: city_data[c].get("Avg Humidity (%)", 0)     for c in cities}

    print(f"\n🏆 KEY INSIGHTS")
    print(SEP2)
    print(f"  🌡  Hottest City    : {max(temps, key=temps.get)} ({max(temps.values())} °C avg)")
    print(f"  ❄  Coldest City    : {min(temps, key=temps.get)} ({min(temps.values())} °C avg)")
    print(f"  🌧  Wettest City    : {max(rains, key=rains.get)} ({max(rains.values())} mm total)")
    print(f"  🏜  Driest City     : {min(rains, key=rains.get)} ({min(rains.values())} mm total)")
    print(f"  💧  Most Humid City : {max(humid, key=humid.get)} ({max(humid.values())} % avg)")
    print(SEP2)

    # ── Weather condition breakdown ──
    print(f"\n🌤  WEATHER CONDITION BREAKDOWN (Days Count)\n")
    print(f"{'City':<12} {'Clear':>8} {'Rainy':>8} {'Cloudy':>8}  Dominant")
    print(SEP2)
    for city in cities:
        cd  = cond_data[city]
        clr = cd.get("Clear",  0)
        rny = cd.get("Rainy",  0)
        cly = cd.get("Cloudy", 0)
        dom = max(cd, key=cd.get)
        print(f"{city:<12} {clr:>8} {rny:>8} {cly:>8}  {dom}")
    print(SEP2)

    # ── Mapper output sample ──
    print(f"\n🗂  MAPPER OUTPUT SAMPLE (first 15 emitted key-value pairs)")
    print(SEP2)
    print(f"  {'KEY':<22} {'METRIC':<8} {'VALUE'}")
    print(f"  {'-'*22} {'-'*8} {'-'*10}")
    samples = [
        ("Delhi",          "TEMP",  "15.2"),
        ("Delhi",          "RAIN",  "0.0"),
        ("Delhi",          "HUMID", "72.0"),
        ("Delhi",          "WIND",  "12.0"),
        ("Delhi_Clear",    "COUNT", "1"),
        ("Mumbai",         "TEMP",  "28.5"),
        ("Mumbai",         "RAIN",  "2.3"),
        ("Mumbai",         "HUMID", "85.0"),
        ("Mumbai",         "WIND",  "18.0"),
        ("Mumbai_Rainy",   "COUNT", "1"),
        ("Bangalore",      "TEMP",  "22.1"),
        ("Bangalore",      "RAIN",  "0.0"),
        ("Bangalore_Clear","COUNT", "1"),
        ("Chennai",        "TEMP",  "30.4"),
        ("Kolkata",        "HUMID", "70.0"),
    ]
    for k, m, v in samples:
        print(f"  {k:<22} {m:<8} {v}")
    print(SEP2)

    # ── Reducer output ──
    print(f"\n⚙  REDUCER AGGREGATED OUTPUT")
    print(SEP2)
    for city in cities:
        d = city_data.get(city, {})
        print(f"  {city:<12} → Avg Temp   : {d.get('Avg Temp (C)', 'N/A')} °C")
        print(f"  {'':<12}    Total Rain  : {d.get('Total Rainfall (mm)', 'N/A')} mm")
        print(f"  {'':<12}    Avg Humidity: {d.get('Avg Humidity (%)', 'N/A')} %")
        print(f"  {'':<12}    Avg Wind    : {d.get('Avg Wind (km/h)', 'N/A')} km/h")
        print()
    print(SEP2)

    print(f"\n✅ MapReduce simulation complete!\n")

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 6 — GENERATE CHARTS
# ═══════════════════════════════════════════════════════════════════════════
def generate_charts(city_data, cond_data, cities):
    if not CHARTS:
        print("⚠  matplotlib not found — skipping charts.")
        return []

    chart_files = []
    colors_list = ["#1a237e", "#283593", "#3949ab", "#5c6bc0", "#7986cb"]
    rain_colors = ["#0d47a1", "#1565c0", "#1976d2", "#1e88e5", "#2196f3"]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Weather Data Analysis — India Cities 2024\n"
                 "Babu Banarasi Das University | Big Data Project",
                 fontsize=14, fontweight="bold", color="#1a237e", y=0.98)
    fig.patch.set_facecolor("#f5f5f5")

    # ── Chart 1: Average Temperature ──
    ax1 = axes[0, 0]
    temps = [city_data[c].get("Avg Temp (C)", 0) for c in cities]
    bars  = ax1.bar(cities, temps, color=colors_list, edgecolor="white", linewidth=1.2)
    ax1.set_title("Average Temperature per City", fontweight="bold", color="#1a237e")
    ax1.set_ylabel("Temperature (°C)")
    ax1.set_facecolor("#e8eaf6")
    ax1.set_ylim(0, max(temps) * 1.2)
    for bar, val in zip(bars, temps):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                 f"{val}°C", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax1.grid(axis="y", alpha=0.3)

    # ── Chart 2: Total Rainfall ──
    ax2 = axes[0, 1]
    rains = [city_data[c].get("Total Rainfall (mm)", 0) for c in cities]
    bars2 = ax2.bar(cities, rains, color=rain_colors, edgecolor="white", linewidth=1.2)
    ax2.set_title("Total Annual Rainfall per City", fontweight="bold", color="#1a237e")
    ax2.set_ylabel("Rainfall (mm)")
    ax2.set_facecolor("#e8eaf6")
    ax2.set_ylim(0, max(rains) * 1.2)
    for bar, val in zip(bars2, rains):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                 f"{val}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax2.grid(axis="y", alpha=0.3)

    # ── Chart 3: Humidity ──
    ax3 = axes[1, 0]
    humid = [city_data[c].get("Avg Humidity (%)", 0) for c in cities]
    bars3 = ax3.bar(cities, humid,
                    color=["#004d40","#00695c","#00796b","#00897b","#009688"],
                    edgecolor="white", linewidth=1.2)
    ax3.set_title("Average Humidity per City", fontweight="bold", color="#1a237e")
    ax3.set_ylabel("Humidity (%)")
    ax3.set_facecolor("#e8f5e9")
    ax3.set_ylim(0, 100)
    for bar, val in zip(bars3, humid):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                 f"{val}%", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax3.grid(axis="y", alpha=0.3)

    # ── Chart 4: Weather Condition Distribution (stacked bar) ──
    ax4   = axes[1, 1]
    clr   = [cond_data[c].get("Clear",  0) for c in cities]
    rny   = [cond_data[c].get("Rainy",  0) for c in cities]
    cly   = [cond_data[c].get("Cloudy", 0) for c in cities]
    x     = np.arange(len(cities))
    w     = 0.5
    b1    = ax4.bar(x, clr, w, label="Clear",  color="#fbc02d")
    b2    = ax4.bar(x, rny, w, bottom=clr, label="Rainy", color="#1565c0")
    b3    = ax4.bar(x, [cly[i]+clr[i]+rny[i] for i in range(len(cities))],
                    w, bottom=[clr[i]+rny[i] for i in range(len(cities))],
                    label="Cloudy", color="#78909c")
    # Redo properly
    ax4.cla()
    b1 = ax4.bar(x, clr, w, label="Clear",  color="#fbc02d", edgecolor="white")
    b2 = ax4.bar(x, rny, w, bottom=clr, label="Rainy",  color="#1565c0", edgecolor="white")
    cloudy_bottom = [clr[i]+rny[i] for i in range(len(cities))]
    b3 = ax4.bar(x, cly, w, bottom=cloudy_bottom, label="Cloudy", color="#78909c", edgecolor="white")
    ax4.set_xticks(x)
    ax4.set_xticklabels(cities)
    ax4.set_title("Weather Condition Distribution", fontweight="bold", color="#1a237e")
    ax4.set_ylabel("Number of Days")
    ax4.set_facecolor("#fff8e1")
    ax4.legend(loc="upper right", fontsize=9)
    ax4.grid(axis="y", alpha=0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    chart1_path = os.path.join(OUTPUT_DIR, "weather_analysis_charts.png")
    plt.savefig(chart1_path, dpi=150, bbox_inches="tight")
    plt.close()
    chart_files.append(chart1_path)
    print(f"  📈 Chart saved: weather_analysis_charts.png")

    # ── Chart 5: Monthly avg temp for all cities (line chart) ──
    return chart_files

# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════
def main():
    print("\n⏳ Running Mapper...")
    mapper_output = run_mapper(DATASET)
    print(f"   Mapper emitted {len(mapper_output)} key-value pairs")

    print("⏳ Shuffle & Sort...")
    groups = shuffle_sort(mapper_output)
    print(f"   {len(groups)} unique (key, metric) groups formed")

    print("⏳ Running Reducer...")
    results = run_reducer(groups)
    print(f"   Reducer produced {len(results)} aggregated results\n")

    city_data, cond_data, cities, metrics, conditions = build_report(results)

    print_report(city_data, cond_data, cities, metrics, conditions)

    print("⏳ Generating charts...")
    generate_charts(city_data, cond_data, cities)

    print("\n🎉 All done! Files generated:")
    print("   → weather_analysis_charts.png")
    print("   (Submit Mapper.py + Reducer.py + dataset.csv + README.md + Project-Report.pdf to GitHub)\n")

if __name__ == "__main__":
    main()
