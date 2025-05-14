#!/usr/bin/env python

import argparse
import duckdb
import glob
import json

class TableInfo:
    def __init__(self, data_path, table_name, column_names, types):
        self.data_path = data_path
        self.table_name = table_name
        self.column_names = column_names
        self.types = types
        print(data_path)
        print(table_name)
        print(column_names)
        print(types)

    def gen_create_stmt(self):
        column_stmts = ""
        for idx in range(len(self.column_names)):
            column_stmts += f"{self.column_names[idx]} {self.types[idx]},\n"
        column_stmts = column_stmts[:-2]
        return f"CREATE TABLE {self.table_name} ({column_stmts})"

    def gen_drop_stmt(self):
        return f"DROP TABLE IF EXISTS {self.table_name}"

    def gen_copy_stmt(self):
        return f"COPY {self.table_name} FROM '{self.data_path}'"


class BenchmarkQuery:
    def __init__(self, table_names, predicates, cardinality):
        self.table_names = table_names
        self.predicates = predicates
        self.cardinality = cardinality

    def gen_stmt(self):
        return f"SELECT count(*) FROM {", ".join(self.table_names)} WHERE {"\nAND ".join(self.predicates)}"


def read_table_info(path):
    with open(path, "r") as file:
        table_infos = []
        for ln_number, line in enumerate(file, start=1):
            line = line.strip()
            if line:
                components = line.split("|")
                data_path = components[0].split(",")[0]
                table_name = components[0].split(",")[-1]
                column_names = components[1].split(",")
                types = [t.replace("uint", "uinteger") for t in components[2].split(",")]
                table_infos.append(TableInfo(data_path, table_name, column_names, types))
        return table_infos


def read_queries(path):
    with open(path, "r") as file:
        queries = []
        for ln_number, line in enumerate(file, start=1):
            line = line.strip()
            if line:
                components = line.split("|")
                table_names = set()
                predicates = []

                if len(components[0]) > 0:
                    join_conds = components[0].split(",")
                    for join in join_conds:
                        lhs = join.split("=")[0]
                        rhs = join.split("=")[1]
                        table_names.add(lhs.split(".")[0])
                        table_names.add(rhs.split(".")[0])
                        predicates.append(f"{lhs} = {rhs}")

                if len(components[1]) > 0:
                    predicate_components = components[1].split(",")
                    for idx in range(len(predicate_components) // 3):
                        start_idx = idx * 3
                        lhs = predicate_components[start_idx]
                        operand = predicate_components[start_idx + 1]
                        rhs = predicate_components[start_idx + 2]

                        table_names.add(lhs.split(".")[0])

                        if operand == "E":
                            if rhs.split(";")[0].isnumeric():
                                rhs = f"({", ".join(rhs.split(";"))})"
                            else:
                                rhs = f"({", ".join(f"'{val}'" for val in rhs.split(";"))})"
                            operand = "IN"
                        else:
                            if not rhs.isnumeric():
                                rhs = f"'{rhs}'"

                        predicates.append(f"{lhs} {operand} {rhs}")

                cardinality = int(components[2])
                queries.append(BenchmarkQuery(list(table_names), predicates, cardinality))
        return queries


def execute_queries(table_infos: list[TableInfo], queries: dict[str, list[BenchmarkQuery]]):
    con = duckdb.connect()

    for table_info in table_infos:
        con.execute(table_info.gen_create_stmt())
        print(table_info.gen_copy_stmt())
        con.execute(table_info.gen_copy_stmt())

    con.execute("SET profiling_mode = 'detailed'")
    con.execute("SET enable_profiling = 'json'")
    results = {}
    for file_name in queries:
        results[file_name] = []
        for query in queries[file_name]:
            print(query.gen_stmt())
            plan = json.loads(con.execute(f"EXPLAIN ANALYZE {query.gen_stmt()}").fetchall()[0][1])
            node = plan
            while "extra_info" not in node or 'Estimated Cardinality' not in node["extra_info"]:
                node = node["children"][0]
            q_err = float(node["extra_info"]["Estimated Cardinality"]) / query.cardinality
            print(f"Card: {query.cardinality}, Est: {node["extra_info"]["Estimated Cardinality"]}, DuckDB says: {node["operator_cardinality"]}")
            results[file_name].append(q_err)

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
    parser = argparse.ArgumentParser(description="Benchmark DuckDBQErrors")
    parser.add_argument("def_file", type=str, help="Path to the table definition file")
    parser.add_argument("query_file_like", type=str, help="Path to the query files")
    parser.add_argument("output_path", type=str, help="Path to output file")
    args = parser.parse_args()

    results = execute_benchmark(args.def_file, args.query_file_like)
    with open(args.output_path, "w") as file:
        json.dump(results, file)
