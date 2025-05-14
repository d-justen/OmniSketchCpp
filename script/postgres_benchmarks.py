#!/usr/bin/env python

import argparse
import psycopg2
import glob
import json
from duckdb_benchmarks import read_table_info, read_queries, TableInfo, BenchmarkQuery

def execute_queries(table_infos: list[TableInfo], queries: dict[str, list[BenchmarkQuery]]):
    con = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
    cur = con.cursor()

    for table_info in table_infos:
        cur.execute(table_info.gen_drop_stmt())
        print(table_info.gen_create_stmt())
        cur.execute(table_info.gen_create_stmt().replace("uinteger", "integer"))
        with open(table_info.data_path, "r") as f:
            cur.copy_from(f, table_info.table_name, sep=",", columns=table_info.column_names)
        cur.execute(f"ANALYZE {table_info.table_name}")

    results = {}
    for file_name in queries:
        results[file_name] = []
        for query in queries[file_name]:
            print(query.gen_stmt())
            cur.execute(f"EXPLAIN (FORMAT JSON) {query.gen_stmt()}")
            plan = cur.fetchone()[0][0]["Plan"]
            multiplier = 1
            while "Join" not in plan["Node Type"] and "Scan" not in plan["Node Type"]:
                if plan["Node Type"] == "Gather":
                    multiplier = plan["Workers Planned"] + 1
                plan = plan["Plans"][0]
            card_est = plan["Plan Rows"] * multiplier
            q_err = card_est / query.cardinality
            print(f"Card: {query.cardinality}, Est: {card_est}")
            results[file_name].append(q_err)

    cur.close()
    con.close()
    return results


def execute_benchmark(definition_file, query_file):
    table_infos = read_table_info(definition_file)

    query_files = glob.glob(query_file)
    query_files.sort()
    queries = {}
    for file in query_files:
        queries[file] = read_queries(file)

    return execute_queries(table_infos, queries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark Postgres Q-Errors")
    parser.add_argument("def_file", type=str, help="Path to the table definition file")
    parser.add_argument("query_file_like", type=str, help="Path to the query files")
    parser.add_argument("output_path", type=str, help="Path to output file")
    args = parser.parse_args()

    results = execute_benchmark(args.def_file, args.query_file_like)
    with open(args.output_path, "w") as file:
        json.dump(results, file)
