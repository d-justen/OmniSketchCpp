import duckdb
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys

from matplotlib.lines import lineStyles

# ========================
# CONFIGURATION
# ========================
DB_PATH = sys.argv[1]

INPUT_FILES = [
    "Primary_Sketches_Only.csv",
    "Secondary_Sketches.csv",  # Add more CSVs here if needed
]
QERROR_PLOT_PATH = "../data/paper_results/qerror_boxplot_grouped.pdf"
DURATION_PLOT_PATH = "../data/paper_results/duration_boxplot_grouped.pdf"
PALETTE = ["#009BDE", "#00CD6C", "#E38B3D"]  # Fixed colors for boxes
#PALETTE = ["#009BDE", "#E38B3D"] # Fixed colors for boxes


# ========================
# LOAD & PROCESS DATA
# ========================
con = duckdb.connect(DB_PATH)

def clean_label(filename):
    """Remove path & extension, replace underscores with spaces."""
    base = os.path.basename(filename)
    name, _ = os.path.splitext(base)
    return name.replace("_", " ")

records = []  # Will hold all processed rows

def get_top_join_estimate(sql):
    """Get cardinality estimate of the top-most join from DuckDB query plan JSON."""
    plan_json_str = con.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()[1]
    plan = json.loads(plan_json_str)[0]

    def find_top_join(node):
        # Recursively search for the first join node (highest in the plan)
        if "name" in node and "JOIN" in node["name"] or "SCAN" in node["name"]:
            if "extra_info" in node and "Estimated Cardinality" in node["extra_info"]:
                return int(node["extra_info"]["Estimated Cardinality"])
        if "children" in node:
            for child in node["children"]:
                found = find_top_join(child)
                if found is not None:
                    return found
        return None

    return find_top_join(plan["children"][0]) if "children" in plan else None

count = 1
for csv_file in INPUT_FILES:
    df = pd.read_csv(csv_file)
    for idx, row in df.iterrows():
        num_relations = row["relations"]
        sql = row["sql"]
        est = row["card_est"]
        duration_ns = row["duration_ns"]

        try:
            actual = con.execute(sql).fetchone()[0]
        except Exception as e:
            print(f"Error executing query from {csv_file} at row {idx}: {e}")
            continue

        qerror = est / actual  # Per definition, underestimations < 1, overestimations > 1
        duration_us = duration_ns / 1000  # convert to microseconds

        try:
            join_est = get_top_join_estimate(sql)
            qerror_join = join_est / actual if join_est is not None else None
        except Exception as e:
            print(f"Error getting join estimate from {csv_file} at row {idx}: {e}")
            qerror_join = None

        records.append({
            "relations": num_relations,
            "qerror": qerror,
            "duration_us": duration_us,
            "source_file": clean_label(csv_file)
        })

        if count == len(INPUT_FILES):
            if qerror_join is not None:
                records.append({
                    "relations": num_relations,
                    "qerror": qerror_join,
                    "duration_us": duration_us,
                    "source_file": "DuckDB"
                })
    count += 1

# Create dataframe with all results
results_df = pd.DataFrame(records)

# ========================
# PLOTTING FUNCTION
# ========================
def plot_grouped_boxplot(df, value_col, ylabel, title, filename, log_scale=True):
    plt.figure(figsize=(6, 3))
    sns.boxplot(
        data=df,
        x="relations",
        y=value_col,
        hue="source_file",
        palette=PALETTE,
        saturation=1
    )
    plt.xlabel("Number of Relations")
    plt.ylabel(ylabel)
    # plt.title(title)
    if log_scale:
        plt.yscale("log")
    plt.grid(True, which="both", linestyle="--", linewidth=0.5)
    if value_col == "qerror":
        plt.axhline(y=1, linestyle="-", color='#D62E37')
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    print(f"Saved: {filename}")

# ========================
# PLOTS
# ========================
plot_grouped_boxplot(
    results_df,
    value_col="qerror",
    ylabel="Q-Error",
    title="Q-Error by Number of Relations",
    filename=QERROR_PLOT_PATH
)

plot_grouped_boxplot(
    results_df,
    value_col="duration_us",
    ylabel="Duration (µs)",
    title="Estimation Time by Number of Relations",
    filename=DURATION_PLOT_PATH
)

