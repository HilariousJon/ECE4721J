import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def read_metrics(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df = df.sort_values(by="num")
    return df


def plot_file_size(df: pd.DataFrame, out_dir: str):
    df_avg = df.groupby("num").first()
    plt.figure()
    plt.plot(df_avg.index, df_avg["size"] / 1024**2, marker="o")
    plt.xscale("log")
    plt.xlabel("Number of Students")
    plt.ylabel("File Size (MB)")
    plt.title("File Size vs Number of Students")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "file_size.jpg"))
    plt.close()


def plot_single_time(df: pd.DataFrame, out_dir: str):
    df_single = df[df["status"] == 1]
    plt.figure()
    plt.plot(df_single["num"], df_single["time"], marker="o", label="Single Node")
    plt.xscale("log")
    plt.xlabel("Number of Students")
    plt.ylabel("Time (s)")
    plt.title("Execution Time vs Number of Students (Single)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "single_time.png"))
    plt.close()


def plot_compare(df: pd.DataFrame, out_dir: str):
    df_single = df[df["status"] == 1]
    df_cluster = df[df["status"] == 2]
    df_mapreduce = df[df["status"] == 3]

    plt.figure()
    plt.plot(df_single["num"], df_single["time"], marker="o", label="Single Node")
    plt.plot(df_cluster["num"], df_cluster["time"], marker="x", label="Cluster")
    plt.plot(df_mapreduce["num"], df_mapreduce["time"], marker="s", label="MapReduce")
    plt.xscale("log")
    plt.xlabel("Number of Students")
    plt.ylabel("Time (s)")
    plt.title("Execution Time Comparison (Single vs Cluster)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "comparison.jpg"))
    plt.close()


def main():
    base_dir = os.path.abspath(os.path.dirname(__file__))
    csv_path = os.path.join(base_dir, "../log/metrics.csv")
    out_dir = os.path.join(base_dir, "../img")
    os.makedirs(out_dir, exist_ok=True)

    df = read_metrics(csv_path)
    plot_file_size(df, out_dir)
    plot_single_time(df, out_dir)
    plot_compare(df, out_dir)

    print(f"Plots saved in: {out_dir}")


if __name__ == "__main__":
    main()
