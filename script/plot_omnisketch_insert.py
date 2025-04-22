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
    x_vals = np.arange(10000, 10000 * len(results) + 1, 10000)
    plt.plot(x_vals, results)
    plt.xlabel("# records inserted")
    plt.ylabel("Throughput [items/second]")
    plt.savefig("omni_sketch_insert.pdf")
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot OmniSketch Insert")
    parser.add_argument("json_file_path", type=str, help="Path to the Google Benchmark JSON output file")
    args = parser.parse_args()

    data = load_benchmark_data(args.json_file_path)
    results = extract_items_per_second(data)
    print(results)
    plot_benchmark_results(results)
