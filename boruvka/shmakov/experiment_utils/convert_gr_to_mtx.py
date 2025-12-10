import os
import glob
import subprocess
import argparse

GIRAPH_JAR = "../ApacheGiraph/target/boruvka-giraph-1.0.0.jar"
CONVERTER_CLASS = "org.example.mst.tools.GrToMtx"


def main():
    parser = argparse.ArgumentParser(description="Convert .gr files to .mtx")
    parser.add_argument("--input", default="./graphs_raw", help="Folder with .gr files")
    parser.add_argument(
        "--output", default="./graphs_mtx", help="Folder to save .mtx files"
    )
    args = parser.parse_args()

    if not os.path.exists(args.output):
        os.makedirs(args.output)

    gr_files = glob.glob(os.path.join(args.input, "*.gr"))
    if not gr_files:
        print(f"No .gr files found in {args.input}")
        return

    print(f"Found {len(gr_files)} files. Converting...")

    for gr_file in gr_files:
        base_name = os.path.splitext(os.path.basename(gr_file))[0]
        mtx_path = os.path.join(args.output, base_name + ".mtx")

        print(f"Converting {base_name}.gr -> {base_name}.mtx")

        cmd = [
            "java",
            "-cp",
            GIRAPH_JAR,
            CONVERTER_CLASS,
            "--input",
            gr_file,
            "--output",
            mtx_path,
        ]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error converting {gr_file}: {e}")

    print("Conversion complete.")


if __name__ == "__main__":
    main()
