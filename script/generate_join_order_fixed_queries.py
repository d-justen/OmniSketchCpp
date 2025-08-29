#!/usr/bin/env python3
"""
Generate SQL with a fixed join order from a pipe-separated CSV.

Input CSV columns:
  - join_order | latency | query

The script builds INNER JOINs that follow the exact binary join tree
described by join_order (e.g., (a,((b,c),d))). All equality join predicates
(table.col = table.col) are moved from WHERE into the appropriate ON clauses
so DuckDB (with join reordering disabled) will respect the join sequence.

Usage:
  python generate_join_order_fixed_queries.py input.csv > output.csv

The output CSV repeats the original columns and appends 'fixed_query'.
"""

import csv
import re
import sys
from typing import List, Tuple, Set, Union

# ---------- small helpers ----------

def strip_outer_quotes(s: str) -> str:
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

def remove_trailing_semicolon(s: str) -> str:
    return re.sub(r';\s*$', '', s.strip())

# ---------- join_order parsing ----------

Node = Union[str, Tuple['Node','Node']]

def tokenize_join_order(s: str) -> List[str]:
    # tokens: "(", ")", ",", NAME
    pattern = re.compile(r'\s*([(),]|[A-Za-z_][A-Za-z0-9_]*)\s*')
    pos = 0
    tokens = []
    while pos < len(s):
        m = pattern.match(s, pos)
        if not m:
            raise ValueError(f"Unexpected token in join_order at pos {pos}: {s[pos:pos+20]}")
        token = m.group(1)
        tokens.append(token)
        pos = m.end()
    return tokens

def parse_join_order_expr(tokens: List[str]) -> Node:
    def parse(idx: int) -> Tuple[Node, int]:
        if idx >= len(tokens):
            raise ValueError("Unexpected end of tokens while parsing join_order")
        tok = tokens[idx]
        if tok == '(':
            left, idx2 = parse(idx+1)
            if tokens[idx2] != ',':
                raise ValueError(f"Expected ',' at position {idx2}, got {tokens[idx2]}")
            right, idx3 = parse(idx2+1)
            if tokens[idx3] != ')':
                raise ValueError(f"Expected ')' at position {idx3}, got {tokens[idx3]}")
            return (left, right), idx3+1
        elif re.match(r'[A-Za-z_]\w*$', tok):
            return tok, idx+1
        else:
            raise ValueError(f"Unexpected token '{tok}' while parsing join_order")
    node, idxf = parse(0)
    if idxf != len(tokens):
        raise ValueError(f"Unconsumed tokens in join_order starting at {idxf}: {tokens[idxf:]}")
    return node

def parse_join_order(s: str) -> Node:
    tokens = tokenize_join_order(s.strip())
    return parse_join_order_expr(tokens)

# ---------- SQL extraction & condition handling ----------

def extract_select_and_where(query: str) -> Tuple[str, str]:
    q = remove_trailing_semicolon(strip_outer_quotes(query))
    m = re.search(r'^\s*SELECT\s+(.*?)\s+FROM\b(.*)$', q, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        raise ValueError("Could not parse SELECT ... FROM in query")
    select_list = m.group(1).strip()
    rest = m.group(2).strip()
    mw = re.search(r'\bWHERE\b(.*)$', rest, flags=re.IGNORECASE | re.DOTALL)
    where = mw.group(1).strip() if mw else ""
    return select_list, where

def split_conditions(where: str) -> List[str]:
    if not where:
        return []
    s = where.strip()
    out = []
    buf = []
    depth = 0
    in_single = False
    in_double = False
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "'" and not in_double:
            in_single = not in_single
            buf.append(ch); i += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch); i += 1; continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1; buf.append(ch); i += 1; continue
            if ch == ')':
                depth -= 1; buf.append(ch); i += 1; continue
            if depth == 0:
                # split on bare AND (word boundaries), case-insensitive
                if (s[i:i+3].lower() == 'and' and
                        (i == 0 or not s[i-1].isalnum()) and
                        (i+3 == len(s) or not s[i+3].isalnum())):
                    out.append(''.join(buf).strip())
                    buf = []
                    i += 3
                    while i < len(s) and s[i].isspace():
                        i += 1
                    continue
        buf.append(ch); i += 1
    last = ''.join(buf).strip()
    if last:
        out.append(last)
    return out

def classify_conditions(conds: List[str]) -> Tuple[List[str], List[str]]:
    join_conds = []
    filters = []
    for c in conds:
        if re.search(r'\b[A-Za-z_]\w*\.[A-Za-z_]\w*\s*=\s*[A-Za-z_]\w*\.[A-Za-z_]\w*\b', c):
            join_conds.append(c.strip())
        else:
            filters.append(c.strip())
    return join_conds, filters

def tables_in_fragment(fragment: str) -> Set[str]:
    return set(re.findall(r'\b([A-Za-z_]\w*)\.[A-Za-z_]\w*\b', fragment))

def build_join_sql(node: Node, join_conds: List[str]) -> Tuple[str, Set[str], Set[int]]:
    """
    Recursively returns (sql, tables_set, used_join_idx_set)
    """
    if isinstance(node, str):
        return node, {node}, set()
    left, right = node
    lsql, ltabs, lused = build_join_sql(left, join_conds)
    rsql, rtabs, rused = build_join_sql(right, join_conds)
    used = set(lused) | set(rused)

    # join predicates that connect the two subtrees
    connectors = []
    for idx, cond in enumerate(join_conds):
        if idx in used:
            continue
        tabs = tables_in_fragment(cond)
        if tabs & ltabs and tabs & rtabs:
            connectors.append((idx, cond))

    if not connectors:
        raise ValueError(f"No join condition found between subtrees {ltabs} and {rtabs}")

    on_clause = ' AND '.join(cond for _, cond in connectors)
    used.update(idx for idx, _ in connectors)
    sql = f"({lsql} JOIN {rsql} ON {on_clause})"
    return sql, ltabs | rtabs, used

def generate_fixed_query(join_order_str: str, original_query: str) -> str:
    join_tree = parse_join_order(join_order_str)
    select_list, where = extract_select_and_where(original_query)
    conds = split_conditions(where) if where else []
    join_conds, filter_conds = classify_conditions(conds)
    join_sql, _, used = build_join_sql(join_tree, join_conds)

    # Any equality conditions not used in ON (should be none) stay in WHERE defensively
    leftover_join_conds = [jc for i, jc in enumerate(join_conds) if i not in used]
    final_filters = filter_conds + leftover_join_conds
    where_clause = f" WHERE {' AND '.join(final_filters)}" if final_filters else ""
    return f"SELECT {select_list} FROM {join_sql}{where_clause};"

# ---------- CSV I/O ----------

def process_csv(path_in: str, out_stream):
    with open(path_in, newline='') as f:
        reader = csv.DictReader(f, delimiter='|')
        fieldnames = reader.fieldnames + ['fixed_query'] if reader.fieldnames else ['join_order','latency','query','fixed_query']
        writer = csv.DictWriter(out_stream, fieldnames=fieldnames, delimiter='|', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in reader:
            try:
                fixed = generate_fixed_query(row['join_order'], row['query'])
            except Exception as e:
                raise RuntimeError(f"Failed on join_order={row.get('join_order')} query={row.get('query')} -> {e}") from e
            row['fixed_query'] = fixed
            writer.writerow(row)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python generate_join_order_fixed_queries.py input.csv > output.csv", file=sys.stderr)
        sys.exit(1)
    process_csv(sys.argv[1], sys.stdout)
