import json
import csv

def parse_google_benchmark_json_to_csv(input_json_path, output_csv_path):
    with open(input_json_path, 'r') as f:
        data = json.load(f)

    benchmarks = data.get("benchmarks", [])

    # Define the CSV headers
    fieldnames = ["number_of_joins", "real_time_ms", "Card", "Est", "QErr", "MemoryUsageMB"]

    with open(output_csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for benchmark in benchmarks:
            try:
                name_parts = benchmark["name"].split('/')
                number_of_joins = int(name_parts[2])

                real_time_ms = benchmark["real_time"] / 1e6  # Convert ns to ms

                row = {
                    "number_of_joins": number_of_joins,
                    "real_time_ms": round(real_time_ms, 5),  # Round for readability
                    "Card": benchmark["Card"],
                    "Est": benchmark["Est"],
                    "QErr": benchmark["QErr"],
                    "MemoryUsageMB": benchmark["MemoryUsageMB"]
                }

                writer.writerow(row)
            except (KeyError, IndexError, ValueError) as e:
                print(f"Skipping entry due to error: {e}\nBenchmark entry: {benchmark}")

# Example usage:
# parse_google_benchmark_json_to_csv("input.json", "output.csv")
parse_google_benchmark_json_to_csv("build/standard_32.json", "build/standard_32.csv")
parse_google_benchmark_json_to_csv("build/standard_64.json", "build/standard_64.csv")
parse_google_benchmark_json_to_csv("build/standard_128.json", "build/standard_128.csv")
parse_google_benchmark_json_to_csv("build/standard_256.json", "build/standard_256.csv")
parse_google_benchmark_json_to_csv("build/standard_512.json", "build/standard_512.csv")
parse_google_benchmark_json_to_csv("build/standard_1024.json", "build/standard_1024.csv")
parse_google_benchmark_json_to_csv("build/secondary_32.json", "build/secondary_32.csv")
parse_google_benchmark_json_to_csv("build/secondary_64.json", "build/secondary_64.csv")
parse_google_benchmark_json_to_csv("build/secondary_128.json", "build/secondary_128.csv")
parse_google_benchmark_json_to_csv("build/secondary_256.json", "build/secondary_256.csv")
parse_google_benchmark_json_to_csv("build/secondary_512.json", "build/secondary_512.csv")
parse_google_benchmark_json_to_csv("build/secondary_1024.json", "build/secondary_1024.csv")
