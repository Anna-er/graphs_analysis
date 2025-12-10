import os
import re
import glob
import subprocess
import argparse
import shutil
import numpy as np
import matplotlib.pyplot as plt
import json
import sys
from datetime import datetime

GUNROCK_BIN = "../Gunrock/build/bin/boruvka"
GIRAPH_JAR = "../ApacheGiraph/target/boruvka-giraph-1.0.0.jar"
GIRAPH_MTX_TO_EDGE_CLASS = "org.example.mst.tools.MtxToEdgeList"
TEMP_DIR = "/tmp/bench_mst_temp"
RESULTS_FILE = "benchmark_results.json"

REGEX_GUNROCK_TIME = r"Handmade MST GPU time:\s+(\d+\.?\d*)\s+ms"
REGEX_GIRAPH_SUPERSTEP = r"Superstep \d+ BoruvkaMSTComputation \(ms\)=(\d+\.?\d*)"


def setup():
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)


def parse_gunrock_time(output):
    match = re.search(REGEX_GUNROCK_TIME, output)
    return float(match.group(1)) if match else None


def parse_giraph_time(output):
    matches = re.findall(REGEX_GIRAPH_SUPERSTEP, output)
    if matches:
        return float(sum(float(t) for t in matches))
    return None


def run_gunrock(mtx_path, runs):
    times = []
    print(f"  [Gunrock] Running {os.path.basename(mtx_path)}...")
    for i in range(runs):
        try:
            res = subprocess.run(
                [GUNROCK_BIN, mtx_path], capture_output=True, text=True, check=True
            )
            out = (res.stdout or "") + "\n" + (res.stderr or "")
            t = parse_gunrock_time(out)
            if t is not None:
                times.append(t)
            else:
                print(f"    [Warning] Could not parse Gunrock time on run {i + 1}")
        except subprocess.CalledProcessError as e:
            print(f"    [Error] Gunrock failed on run {i + 1}:")
            print(e.stderr or e.stdout)
    return times


def run_giraph(mtx_path, runs, core_config):
    times = []
    filename = os.path.basename(mtx_path)
    edgelist_path = os.path.join(TEMP_DIR, filename + ".edgelist")

    num_cores = core_config["count"]
    cpu_mask = core_config["mask"]

    print(
        f"  [Giraph]  Converting & Running {filename} on {num_cores} cores ({cpu_mask})..."
    )

    try:
        subprocess.run(
            [
                "java",
                "-cp",
                GIRAPH_JAR,
                GIRAPH_MTX_TO_EDGE_CLASS,
                "--input",
                mtx_path,
                "--output",
                edgelist_path,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"    [Error] MTX Conversion failed:")
        print(e.stderr or e.stdout)
        return []

    for i in range(runs):
        out_dir = os.path.join(TEMP_DIR, f"giraph_out_{filename}_{i}")
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)

        cmd = []

        if shutil.which("taskset") and cpu_mask:
            cmd.extend(["taskset", "-c", cpu_mask])

        cmd.extend(
            [
                "java",
                f"-XX:ActiveProcessorCount={num_cores}",
                "-Xmx16g",
                "-XX:+UseG1GC",
                "-XX:MaxGCPauseMillis=200",
                "-jar",
                GIRAPH_JAR,
                "--input",
                edgelist_path,
                "--output",
                out_dir,
                "--threads",
                str(num_cores),
            ]
        )

        try:
            res = subprocess.run(cmd, capture_output=True, text=True, check=True)
            full_log = (res.stdout or "") + "\n" + (res.stderr or "")
            t = parse_giraph_time(full_log)
            if t is not None:
                times.append(t)
            else:
                print(f"    [Warning] Could not parse times from Giraph run {i + 1}")
                print("----- Giraph stdout/stderr (truncated) -----")
                print((full_log[:1000] + "...") if len(full_log) > 1000 else full_log)
                print("--------------------------------------------")
        except subprocess.CalledProcessError as e:
            print(f"    [Error] Giraph failed on run {i + 1}:")
            print(f"    Command: {' '.join(cmd)}")
            print("---------------------------------------------------")
            print(e.stdout or "")
            print(e.stderr or "")
            print("---------------------------------------------------")
        except FileNotFoundError as e:
            print(f"    [Error] Failed to launch process: {e}")
            return times

    return times


def save_results_to_json(results, runs, graphs_folder, core_config):
    output_data = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "runs_per_graph": runs,
            "graphs_folder": graphs_folder,
            "core_config": core_config,
            "gunrock_binary": GUNROCK_BIN,
            "giraph_jar": GIRAPH_JAR,
        },
        "results": {},
    }

    for graph_name, data in results.items():
        gr_times = data.get("gunrock", [])
        gi_times = data.get("giraph", [])

        output_data["results"][graph_name] = {
            "gunrock": {
                "raw_times": gr_times,
                "mean": float(np.mean(gr_times)) if gr_times else 0.0,
                "std": float(np.std(gr_times)) if gr_times else 0.0,
                "runs_successful": len(gr_times),
            },
            "giraph": {
                "raw_times": gi_times,
                "mean": float(np.mean(gi_times)) if gi_times else 0.0,
                "std": float(np.std(gi_times)) if gi_times else 0.0,
                "runs_successful": len(gi_times),
            },
        }

    with open(RESULTS_FILE, "w") as f:
        json.dump(output_data, f, indent=2)

    print(f"\nResults saved to {RESULTS_FILE}")


def plot(results, core_count):
    graphs = sorted(results.keys())
    if not graphs:
        print("No results to plot.")
        return

    gunrock_means, gunrock_errs = [], []
    giraph_means, giraph_errs = [], []

    for g in graphs:
        gr = results[g].get("gunrock", [])
        if gr:
            gunrock_means.append(np.mean(gr))
            gunrock_errs.append(1.96 * np.std(gr) / np.sqrt(len(gr)))
        else:
            gunrock_means.append(0)
            gunrock_errs.append(0)

        gi = results[g].get("giraph", [])
        if gi:
            giraph_means.append(np.mean(gi))
            giraph_errs.append(1.96 * np.std(gi) / np.sqrt(len(gi)))
        else:
            giraph_means.append(0)
            giraph_errs.append(0)

    x = np.arange(len(graphs))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 7))
    bars1 = ax.bar(
        x - width / 2,
        gunrock_means,
        width,
        yerr=gunrock_errs,
        capsize=5,
        label="Gunrock (GPU)",
        color="skyblue",
        edgecolor="blue",
    )

    giraph_label = f"Apache Giraph (CPU - {core_count} threads)"
    bars2 = ax.bar(
        x + width / 2,
        giraph_means,
        width,
        yerr=giraph_errs,
        capsize=5,
        label=giraph_label,
        color="lightgreen",
        edgecolor="green",
    )

    ax.set_ylabel("Algorithm Time (ms)")
    ax.set_title("MST Algorithm Performance: Gunrock vs Giraph")
    ax.set_xticks(x)
    ax.set_xticklabels(graphs, rotation=45, ha="right")
    ax.legend()
    ax.set_yscale("log")

    def label_bars(rects):
        for rect in rects:
            h = rect.get_height()
            if h > 0:
                ax.annotate(
                    f"{h:.1f}",
                    (rect.get_x() + rect.get_width() / 2, h),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )

    label_bars(bars1)
    label_bars(bars2)

    plt.tight_layout()
    plt.savefig("benchmark_results.png")
    print("\nPlot saved to benchmark_results.png")


def parse_core_args(core_arg):
    if not core_arg:
        count = os.cpu_count() or 4
        return {"count": count, "mask": ",".join(str(i) for i in range(count))}

    if "," in core_arg:
        parts = core_arg.split(",")
        parts = [p.strip() for p in parts if p.strip()]
        return {"count": len(parts), "mask": ",".join(parts)}
    else:
        try:
            count = int(core_arg)
            return {"count": count, "mask": ",".join(str(i) for i in range(count))}
        except ValueError:
            print(f"Invalid core argument: {core_arg}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--graphs", default="./graphs_mtx", help="Folder with .mtx files"
    )
    parser.add_argument("--runs", type=int, default=5, help="Runs per graph")
    parser.add_argument(
        "--cores",
        default=None,
        help="Cores to use. E.g. '6' (for first 6) or '0,1,3,6,8,10' (specific IDs)",
    )
    args = parser.parse_args()

    setup()

    core_config = parse_core_args(args.cores)
    print(
        f"Benchmark Configuration: Using {core_config['count']} cores (Mask: {core_config['mask']})"
    )

    mtx_files = glob.glob(os.path.join(args.graphs, "*.[mM][tT][xX]"))

    if not mtx_files:
        print(
            f"No .mtx files found in {args.graphs}! Did you run convert_gr_to_mtx.py?"
        )
        return

    results = {}
    print(f"Found {len(mtx_files)} graphs to benchmark.")

    for mtx in sorted(mtx_files):
        name = os.path.basename(mtx)
        print(f"\n--- Benchmarking {name} ---")
        results[name] = {
            "gunrock": run_gunrock(mtx, args.runs),
            "giraph": run_giraph(mtx, args.runs, core_config),
        }

    save_results_to_json(results, args.runs, args.graphs, core_config)
    plot(results, core_config["count"])


if __name__ == "__main__":
    main()
