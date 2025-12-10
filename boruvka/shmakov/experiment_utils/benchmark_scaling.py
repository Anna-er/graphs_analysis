import os
import re
import glob
import subprocess
import shutil
import sys
import matplotlib.pyplot as plt

GIRAPH_JAR = "../ApacheGiraph/target/boruvka-giraph-1.0.0.jar"
GIRAPH_MTX_TO_EDGE_CLASS = "org.example.mst.tools.MtxToEdgeList"
TEMP_DIR = "/tmp/bench_scaling_specific"

PERF_CORES = [0, 1, 3, 6, 8, 10]
MAX_THREADS = len(PERF_CORES)  # Should be 6

TARGET_PATTERNS = ["*CAL*.mtx", "*NE*.mtx", "*NW*.mtx"]

REGEX_GIRAPH_SUPERSTEP = r"Superstep \d+ BoruvkaMSTComputation \(ms\)=(\d+)"


def setup():
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)


def parse_giraph_time(output):
    matches = re.findall(REGEX_GIRAPH_SUPERSTEP, output)
    if matches:
        return float(sum(int(t) for t in matches))
    return None


def convert_graph(mtx_path):
    filename = os.path.basename(mtx_path)
    edgelist_path = os.path.join(TEMP_DIR, filename + ".edgelist")

    if os.path.exists(edgelist_path):
        return edgelist_path

    print(f"  [Setup] Converting {filename} to EdgeList...")
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
        )
        return edgelist_path
    except subprocess.CalledProcessError as e:
        print(f"    [Error] Conversion failed: {e}")
        return None


def run_giraph(edgelist_path, num_threads):
    out_dir = os.path.join(
        TEMP_DIR, f"out_{os.path.basename(edgelist_path)}_{num_threads}"
    )
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)

    current_cores = PERF_CORES[:num_threads]
    cpu_mask = ",".join(str(c) for c in current_cores)

    cmd = []

    if shutil.which("taskset"):
        cmd.extend(["taskset", "-c", cpu_mask])

    cmd.extend(
        [
            "java",
            f"-XX:ActiveProcessorCount={num_threads}",
            "-Xmx16g",
            "-XX:+UseG1GC",
            "-jar",
            GIRAPH_JAR,
            "--input",
            edgelist_path,
            "--output",
            out_dir,
            "--threads",
            str(num_threads),
        ]
    )

    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        full_log = res.stdout + "\n" + res.stderr
        return parse_giraph_time(full_log)
    except subprocess.CalledProcessError as e:
        print(f"\n    [Error] Failed with {num_threads} threads.")
        return None


def plot_combined_speedup(all_results):
    plt.figure(figsize=(10, 7))

    plt.plot(
        range(1, MAX_THREADS + 1),
        range(1, MAX_THREADS + 1),
        "k--",
        alpha=0.4,
        label="Ideal Linear Scaling",
    )

    markers = ["o", "s", "^", "D"]

    for idx, (graph_name, data) in enumerate(all_results.items()):
        threads_x = sorted(data.keys())
        times = [data[t] for t in threads_x]

        baseline = data.get(1)
        if not baseline:
            print(f"Skipping plot for {graph_name} (missing 1-thread baseline)")
            continue

        speedup_y = [baseline / t if t > 0 else 0 for t in times]

        marker = markers[idx % len(markers)]
        plt.plot(
            threads_x,
            speedup_y,
            marker=marker,
            markersize=6,
            linewidth=2,
            label=graph_name,
        )

    plt.title(f"Giraph MST Speedup on Performance Cores ({PERF_CORES})")
    plt.xlabel("Number of Threads (Performance Cores)")
    plt.ylabel("Speedup Factor (vs 1 Thread)")
    plt.xticks(range(1, MAX_THREADS + 1))
    plt.ylim(bottom=0)
    plt.grid(True, which="both", linestyle="--", alpha=0.5)
    plt.legend()
    plt.tight_layout()

    out_file = "perf_core_scaling_results.png"
    plt.savefig(out_file)
    print(f"\n[Success] Plot saved to {out_file}")


def main():
    setup()

    found_files = []
    for pattern in TARGET_PATTERNS:
        path = os.path.join("./graphs_mtx", pattern)
        matches = glob.glob(path)
        if matches:
            found_files.extend(matches)
        else:
            print(f"Warning: No files found for pattern '{pattern}'")

    if not found_files:
        print("Error: No graphs found. Check ./graphs_mtx path.")
        return

    found_files = sorted(list(set(found_files)))

    all_results = {}

    print(f"Testing scaling on Performance Cores: {PERF_CORES}")

    for mtx_path in found_files:
        graph_name = os.path.basename(mtx_path)
        print(f"\n--- Benchmarking {graph_name} ---")

        edgelist_path = convert_graph(mtx_path)
        if not edgelist_path:
            continue

        graph_results = {}

        for t in range(1, MAX_THREADS + 1):
            sys.stdout.write(
                f"\r    Running with {t} threads (Cores: {PERF_CORES[:t]})... "
            )
            sys.stdout.flush()

            time_ms = run_giraph(edgelist_path, t)

            if time_ms is not None:
                graph_results[t] = time_ms
                sys.stdout.write(f"Done ({time_ms:.1f} ms)")
            else:
                sys.stdout.write("Failed")
            sys.stdout.flush()

        print("")  # Newline
        all_results[graph_name] = graph_results

    if all_results:
        plot_combined_speedup(all_results)
    else:
        print("No results to plot.")


if __name__ == "__main__":
    main()
