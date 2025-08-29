#!/usr/bin/env python3
import csv
import json
import time
import duckdb
import sys

REPEATS = 10
THREAD_COUNT = 8

def measure_query_time(con, query, repeats=REPEATS):
    """Run query multiple times, measure execution time, return average in seconds."""
    # Warmup
    con.execute(query)
    _ = con.fetchall()

    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        con.execute(query)
        _ = con.fetchall()  # consume results
        end = time.perf_counter()
        times.append(end - start)
        print(f"{end - start}s")
    return sum(times) / len(times)

def extract_join_card_sum(plan_node):
    """Recursively sum join cardinalities from DuckDB's JSON plan."""
    total = 0
    if isinstance(plan_node, dict):
        if plan_node.get("operator_type", "").lower().endswith("join"):
            if "operator_cardinality" in plan_node:
                try:
                    total += int(plan_node["operator_cardinality"])
                except Exception:
                    pass
        # Recurse into children
        for child in plan_node.get("children", []):
            total += extract_join_card_sum(child)
    elif isinstance(plan_node, list):
        for item in plan_node:
            total += extract_join_card_sum(item)
    return total

def get_join_card_sum(con, query):
    """Run EXPLAIN ANALYZE (FORMAT JSON) and sum join cardinalities."""
    con.execute("SET enable_profiling = 'query_tree'")
    print(con.execute(f"EXPLAIN ANALYZE {query}").fetchone()[1])
    con.execute("SET enable_profiling = 'json'")
    res = con.execute(f"EXPLAIN ANALYZE {query}").fetchone()[1]
    con.execute("SET enable_profiling = 'no_output'")
    con.execute("PRAGMA disable_profiling")
    if res is None:
        return 0
    try:
        plan_json = json.loads(res)
    except json.JSONDecodeError:
        return 0
    return extract_join_card_sum(plan_json)

def process_csv(path_in: str, path_out: str, database_file: str):
    con = duckdb.connect(database=database_file)
    con.execute(f"SET threads = {THREAD_COUNT}")
    with open(path_in, newline='') as f_in, open(path_out, 'w', newline='') as f_out:
        reader = csv.DictReader(f_in, delimiter='|')
        fieldnames = ['join_order', 'original_avg_time', 'original_sum_join_card',
                      'fixed_avg_time', 'fixed_sum_join_card']
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter='|')
        writer.writeheader()

        for row in reader:
            join_order = row['join_order']
            latency = float(row['latency']) / 1000.0
            original_query = row['query']
            fixed_query = row['fixed_query']

            # Original query performance and join cardinalities
            con.execute("SET disabled_optimizers = ''")
            print(fixed_query)
            print("DuckDB original optimizer:")
            orig_time = measure_query_time(con, fixed_query, REPEATS)
            orig_cardsum = get_join_card_sum(con, fixed_query)

            # Fixed join order
            con.execute("SET disabled_optimizers = 'join_order,build_side_probe_side'")
            print("OmniSketch:")
            fix_time = measure_query_time(con, fixed_query, REPEATS)
            fix_cardsum = get_join_card_sum(con, fixed_query)

            writer.writerow({
                'join_order': join_order,
                'original_avg_time': f"{orig_time:.6f}",
                'original_sum_join_card': orig_cardsum,
                'fixed_avg_time': f"{fix_time + latency:.6f}",
                'fixed_sum_join_card': fix_cardsum
            })

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} input.csv output.csv database_file.duckdb", file=sys.stderr)
        sys.exit(1)
    process_csv(sys.argv[1], sys.argv[2], sys.argv[3])
