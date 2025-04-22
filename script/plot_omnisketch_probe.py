#!/usr/bin/env python

import json
import argparse
import matplotlib.pyplot as plt
import numpy as np
from benchmark_result_extractor import extract_items_per_second


def load_benchmark_data(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data


def plot_benchmark_results(results):
    y_vals_1 = results[:len(results)//2]
    y_vals_2 = results[len(results)//2:]

    x_vals = [128, 256, 512, 1024, 2048, 4096]
    plt.plot(x_vals, y_vals_1, label="Red-black tree")
    plt.plot(x_vals, y_vals_2, label="Vector")

    plt.xlabel("MinHash Sketch Size")
    plt.ylabel("Throughput [probes/second]")
    plt.legend()
    plt.yscale("log")
    plt.savefig("omni_sketch_probe.pdf")
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot OmniSketch Probe")
    parser.add_argument("json_file_path", type=str, help="Path to the Google Benchmark JSON output file")
    args = parser.parse_args()

    data = load_benchmark_data(args.json_file_path)
    results = extract_items_per_second(data)
    print(results)
    plot_benchmark_results(results)
