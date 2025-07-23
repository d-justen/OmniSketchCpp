import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import sys
from collections import defaultdict

def read_and_process_csv(file_path):
    df = pd.read_csv(file_path)
    initial_count = len(df)

    grouped = df.groupby('number_of_joins')['real_time_ms'].apply(list)
    return grouped

def collect_data(csv_files):
    all_data = defaultdict(lambda: [])
    labels = []

    for file in csv_files:
        label = os.path.basename(file)
        labels.append(label)
        grouped = read_and_process_csv(file)

        for joins in grouped.index:
            all_data[joins].append(grouped[joins])

    return all_data, labels

def plot_boxplot(all_data, labels):
    joins_sorted = sorted(all_data.keys())
    num_files = len(labels)

    fig, ax = plt.subplots(figsize=(14, 6))
    box_data = []
    positions = []

    width = 0.8  # Total width of a group
    box_width = width / num_files

    for i, joins in enumerate(joins_sorted):
        for j in range(num_files):
            qerrs = all_data[joins][j] if j < len(all_data[joins]) else []
            box_data.append(qerrs)
            # Compute position so that each group has multiple boxes
            positions.append(i - width/2 + j * box_width + box_width/2)

    ax.boxplot(box_data, positions=positions, widths=box_width, patch_artist=True, zorder=2)

    # Set x-axis labels at the center of each group
    ax.set_xticks(range(len(joins_sorted)))
    ax.set_xticklabels(joins_sorted)
    ax.set_xlabel('Number of Joins')
    ax.set_ylabel('Time (ms)')
    ax.set_yscale('log')
    ax.set_title('CardEst latency by Number of Joins (per CSV)')

    # Add legend
    legend_patches = [plt.Line2D([0], [0], color='C{}'.format(i), lw=4) for i in range(num_files)]
    for i, patch in enumerate(ax.artists):
        patch.set_facecolor('C{}'.format(i % num_files))
    ax.legend(legend_patches, labels, title="CSV Files", loc='upper right')

    plt.tight_layout()
    plt.savefig("ssb_skew_latencies.pdf")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python plot_latencies.py <csv1> <csv2> ...")
        sys.exit(1)

    csv_files = sys.argv[1:]
    all_data, labels = collect_data(csv_files)
    plot_boxplot(all_data, labels)
