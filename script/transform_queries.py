import re
import duckdb
import argparse

def get_table_aliases(sql):
    from_clause = re.search(r'FROM\s+(.*?)(WHERE|$)', sql, re.IGNORECASE | re.DOTALL)
    if not from_clause:
        return {}
    tables = from_clause.group(1).strip().rstrip(',').split(',')
    alias_map = {}
    for table in tables:
        parts = table.strip().split()
        if len(parts) == 1:
            alias_map[parts[0]] = parts[0]
        elif len(parts) == 3 and parts[1].upper() == "AS":
            alias_map[parts[2]] = parts[0]
        elif len(parts) == 2:
            alias_map[parts[1]] = parts[0]
    return alias_map

def replace_aliases(expression, alias_map):
    def repl(match):
        alias, col = match.group(1), match.group(2)
        table = alias_map.get(alias, alias)
        return f"{table}.{col}"
    return re.sub(r'\b(\w+)\.(\w+)\b', repl, expression)

def is_column_reference(expr):
    return bool(re.match(r'^\w+\.\w+$', expr.strip()))

def parse_conditions(sql, alias_map):
    where_clause = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE | re.DOTALL)
    if not where_clause:
        return [], []

    conditions = where_clause.group(1).strip()
    conditions = re.sub(r'\s+', ' ', conditions)
    parts = [c.strip().rstrip(';') for c in re.split(r'\bAND\b', conditions, flags=re.IGNORECASE)]

    joins = []
    predicates = []

    for cond in parts:
        norm = replace_aliases(cond, alias_map)

        # Join condition: both sides are columns
        join_match = re.match(r'^(\w+\.\w+)\s*=\s*(\w+\.\w+)$', norm)
        if join_match and all(is_column_reference(s) for s in join_match.groups()):
            joins.append(norm.replace(" ", ""))
        else:
            pred = parse_predicate(norm)
            if pred:
                predicates.append(pred)

    return joins, predicates

def parse_predicate(expr):
    # BETWEEN
    match = re.match(r'(\w+\.\w+)\s+BETWEEN\s+([^\s]+)\s+AND\s+([^\s]+)', expr, re.IGNORECASE)
    if match:
        col, v1, v2 = match.groups()
        return f"{col},BETWEEN,{strip_quotes(v1)};{strip_quotes(v2)}"

    # IN
    match = re.match(r'(\w+\.\w+)\s+IN\s*\((.*?)\)', expr, re.IGNORECASE)
    if match:
        col, values = match.groups()
        values = ';'.join(strip_quotes(v) for v in values.split(','))
        return f"{col},IN,{values}"

    # LIKE / NOT LIKE
    match = re.match(r'(\w+\.\w+)\s+(NOT\s+)?LIKE\s+(.+)', expr, re.IGNORECASE)
    if match:
        col, not_part, value = match.groups()
        op = (not_part or '').strip() + 'LIKE'
        return f"{col},{op.strip()},{strip_quotes(value)}"

    # Simple operators (=, !=, <>, <, >, <=, >=)
    match = re.match(r'(\w+\.\w+)\s*(=|<>|!=|<|>|<=|>=)\s*(.+)', expr)
    if match:
        col, op, val = match.groups()
        if is_column_reference(val):  # it's a join mistakenly captured
            return None
        return f"{col},{op},{strip_quotes(val)}"

    return None

def strip_quotes(value):
    return value.strip().strip('"').strip("'")

def compute_cardinality(sql, db_path):
    count_query = re.sub(r'SELECT .*?FROM', 'SELECT COUNT(*) FROM', sql, flags=re.IGNORECASE | re.DOTALL)
    count_query = re.sub(r'GROUP BY .*', '', count_query, flags=re.IGNORECASE)
    con = duckdb.connect(db_path)
    result = con.execute(count_query).fetchone()
    con.close()
    return result[0]

def process_query(sql_text, db_path):
    alias_map = get_table_aliases(sql_text)
    joins, predicates = parse_conditions(sql_text, alias_map)
    join_str = ','.join(sorted(joins))
    pred_str = ','.join(predicates)
    cardinality = compute_cardinality(sql_text, db_path)
    return f"{join_str}|{pred_str}|{cardinality}"

def main():
    parser = argparse.ArgumentParser(description='Transform SQL query into pseudo-CSV format with DuckDB cardinality.')
    parser.add_argument('--sql', required=True, help='Path to SQL file')
    parser.add_argument('--db', required=True, help='Path to DuckDB database file')
    parser.add_argument('--out', required=True, help='Output file to append the result')

    args = parser.parse_args()

    result = ""
    with open(args.sql, 'r') as f:
       for line in f:
          result += process_query(line, args.db) + "\n"

    with open(args.out, 'a') as f:
        f.write(result)

    print("Result written to:", args.out)
    print(result)

if __name__ == '__main__':
    main()
