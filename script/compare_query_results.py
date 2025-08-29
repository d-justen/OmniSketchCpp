import csv
import sys

# File path (update this to your actual CSV file path)
file_path = sys.argv[1]

# Initialize accumulators
orig_avg_sum = 0.0
fixed_avg_sum = 0.0

max_improvement = 0.0
max_regression = 0.0
avg_time_changes = []

orig_card_sum = 0.0
fixed_card_sum = 0.0

card_improvement = 0.0
card_regression = 0.0
card_changes = []

with open(file_path, "r") as f:
    reader = csv.DictReader(f, delimiter="|")
    for row in reader:
        # Convert values
        o_avg = float(row["original_avg_time"])
        f_avg = float(row["fixed_avg_time"])
        o_card = float(row["original_sum_join_card"])
        f_card = float(row["fixed_sum_join_card"])

        # Accumulate sums
        orig_avg_sum += o_avg
        fixed_avg_sum += f_avg
        orig_card_sum += o_card
        fixed_card_sum += f_card

        # Time improvement/regression
        if f_avg < o_avg:
            improvement = o_avg / f_avg
            max_improvement = max(max_improvement, improvement)
        elif f_avg > o_avg:
            regression = f_avg / o_avg
            max_regression = max(max_regression, regression)

        avg_time_changes.append((f_avg - o_avg) / o_avg)

        # Card improvement/regression
        if f_card < o_card and f_card != 0:
            improvement = o_card / f_card
            card_improvement = max(card_improvement, improvement)
        elif f_card > o_card and o_card != 0:
            regression = f_card / o_card
            card_regression = max(card_regression, regression)

        if o_card != 0:
            card_changes.append((f_card - o_card) / o_card)

# Compute averages
avg_time_change = sum(avg_time_changes) / len(avg_time_changes) if avg_time_changes else 0
avg_card_change = sum(card_changes) / len(card_changes) if card_changes else 0

# Print results
print("=== Average Time Stats ===")
print(f"Sum original_avg_time: {orig_avg_sum}")
print(f"Sum fixed_avg_time: {fixed_avg_sum}")
print(f"Max improvement (x): {max_improvement}")
print(f"Max regression (x): {max_regression}")
print(f"Average relative change: {avg_time_change * 100:.2f}%")

print("\n=== Join Card Stats ===")
print(f"Sum original_sum_join_card: {orig_card_sum}")
print(f"Sum fixed_sum_join_card: {fixed_card_sum}")
print(f"Max improvement (x): {card_improvement}")
print(f"Max regression (x): {card_regression}")
print(f"Average relative change: {avg_card_change * 100:.2f}%")
