import csv
import os

import matplotlib.pyplot as plt


INPUT_FILE = (
    "results/phase2_results.csv"
)


rates = []
average_latencies = []
p99_latencies = []


with open(
    INPUT_FILE,
    "r",
    encoding="utf-8"
) as file:

    reader = csv.DictReader(file)

    for row in reader:

        rates.append(
            int(row["rate"])
        )

        average_latencies.append(
            float(row["average_ms"])
        )

        p99_latencies.append(
            float(row["p99_ms"])
        )


os.makedirs(
    "results",
    exist_ok=True
)


# Average Latency Graph

plt.figure()

plt.plot(
    rates,
    average_latencies,
    marker="o"
)

plt.xlabel(
    "Request rate (requests/s)"
)

plt.ylabel(
    "Average execution latency (ms)"
)

plt.title(
    "Phase II: Average Execution Latency"
)

plt.grid(True)

plt.tight_layout()

plt.savefig(
    "results/average_latency.png",
    dpi=300
)

plt.close()


# P99 Latency Graph
plt.figure()

plt.plot(
    rates,
    p99_latencies,
    marker="o"
)

plt.xlabel(
    "Request rate (requests/s)"
)

plt.ylabel(
    "99th-percentile execution latency (ms)"
)

plt.title(
    "Phase II: 99th-Percentile Latency"
)

plt.grid(True)

plt.tight_layout()

plt.savefig(
    "results/p99_latency.png",
    dpi=300
)

plt.close()


print(
    "Created:"
)

print(
    "results/average_latency.png"
)

print(
    "results/p99_latency.png"
)