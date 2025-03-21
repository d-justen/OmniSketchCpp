#!/usr/bin/env python

import json
import argparse
import matplotlib.pyplot as plt


def load_benchmark_data(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data


def extract_benchmark_results(data):
    benchmarks = data.get("benchmarks", [])
    results = dict()
    for b in benchmarks:
        if "name" in b and b["run_type"] == "iteration":
            bench_id = b["name"].split("/")[1]
            att_count = b["name"].split("/")[2]
            mhs_size = b["name"].split("/")[3]
            if "Join" in bench_id:
                mhs_size = b["name"].split("/")[4]
            q_err = b["QError"]
            if bench_id not in results:
                results[bench_id] = dict()
            if mhs_size not in results[bench_id]:
                results[bench_id][mhs_size] = dict()
            if att_count not in results[bench_id][mhs_size]:
                results[bench_id][mhs_size][att_count] = []
            results[bench_id][mhs_size][att_count].append(q_err)

    return results


def plot_benchmark_results(bench_id, results):
    fig, axs = plt.subplots(nrows=1, ncols=len(results), figsize=(14, 4))
    plt.suptitle(f"{bench_id}, w=256, d=3")
    for index, (key, value) in enumerate(results.items()):
        axs[index].violinplot(list(results[key].values()), showmeans=False, showmedians=True)
        axs[index].set_title(f"B = {key}")

        axs[index].set_yscale("log")
        if "Join" in bench_id:
            axs[index].set_xlabel("Join Sample Size (2^n)")
            axs[index].set_xticks([x + 1 for x in range(len(results[key]))], labels=[f"{x + 2}" for x in range(len(results[key]))])
        else:
            axs[index].set_xlabel("Attribute Domain (2^n)")
            axs[index].set_xticks([x + 1 for x in range(len(results[key]))], labels=[f"{x + 3}" for x in range(len(results[key]))])
        axs[index].set_ylabel("Error (dotted line = no error)")
        axs[index].axhline(y=1, color='grey', linestyle='dotted')
        axs[index].set_ylim([0.1, 10])

    plt.savefig(f"{bench_id}.pdf")
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot ProbeErrors")
    parser.add_argument("json_file_path", type=str, help="Path to the Google Benchmark JSON output file")
    args = parser.parse_args()

    data = load_benchmark_data(args.json_file_path)
    results = extract_benchmark_results(data)
    print(results)
    for bench_id in results:
        plot_benchmark_results(bench_id, results[bench_id])

