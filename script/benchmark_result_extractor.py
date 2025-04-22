#!/usr/bin/env python

import json


def extract_q_errors(data):
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


def extract_items_per_second(data):
    benchmarks = data.get("benchmarks", [])
    results = []
    for b in benchmarks:
        if "name" in b and b["run_type"] == "iteration":
            results.append(b["items_per_second"])

    return results
