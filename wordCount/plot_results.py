import csv
import os

import matplotlib.pyplot as plt


EXPECTED_RATES = [
    800,
    900,
    1000,
    1100,
    1200
]


PHASE2_FILE = (
    "results/phase2_results.csv"
)


LEAST_CONNECTIONS_FILE = (

    "results/"
    "phase3_least_connections.csv"

)


LEAST_RESPONSE_FILE = (

    "results/"
    "phase3_least_response_time.csv"

)

# Read CSV

def read_results(path):

    data = {}


    with open(
        path,
        "r",
        encoding="utf-8"
    ) as file:


        reader = csv.DictReader(
            file
        )


        for row in reader:


            rate = int(
                row["rate"]
            )


            data[rate] = {

                "average_ms":
                    float(
                        row["average_ms"]
                    ),

                "p99_ms":
                    float(
                        row["p99_ms"]
                    )
            }


    return data


# Verify same rates

def require_rates(
    name,
    data
):


    missing = [

        rate

        for rate
        in EXPECTED_RATES

        if rate not in data

    ]


    if missing:

        raise RuntimeError(

            f"{name} is missing "
            f"rates {missing}. "

            f"Use results for "
            f"{EXPECTED_RATES}."

        )


# Load results

phase2 = read_results(
    PHASE2_FILE
)


least_connections = read_results(
    LEAST_CONNECTIONS_FILE
)


least_response = read_results(
    LEAST_RESPONSE_FILE
)


require_rates(
    "Phase II results",
    phase2
)


require_rates(
    "Least Connections results",
    least_connections
)


require_rates(
    "Least Response Time results",
    least_response
)


os.makedirs(
    "results",
    exist_ok=True
)


# Compare avg latency

plt.figure()


plt.plot(

    EXPECTED_RATES,

    [
        phase2[r]["average_ms"]
        for r in EXPECTED_RATES
    ],

    marker="o",

    label="Phase II: 1 server"

)


plt.plot(

    EXPECTED_RATES,

    [
        least_connections[r][
            "average_ms"
        ]

        for r in EXPECTED_RATES
    ],

    marker="o",

    label=(
        "Phase III: "
        "Least Connections"
    )

)


plt.plot(

    EXPECTED_RATES,

    [
        least_response[r][
            "average_ms"
        ]

        for r in EXPECTED_RATES
    ],

    marker="o",

    label=(
        "Phase III: "
        "Least Response Time"
    )

)


plt.xlabel(
    "Request rate (requests/s)"
)


plt.ylabel(
    "Average execution latency (ms)"
)


plt.title(

    "Phase II vs Phase III: "
    "Average Execution Latency"

)


plt.grid(True)

plt.legend()

plt.tight_layout()


plt.savefig(

    "results/"
    "phase3_average_comparison.png",

    dpi=300

)


plt.close()


# P99 comparison
plt.figure()


plt.plot(

    EXPECTED_RATES,

    [
        phase2[r]["p99_ms"]
        for r in EXPECTED_RATES
    ],

    marker="o",

    label="Phase II: 1 server"

)


plt.plot(

    EXPECTED_RATES,

    [
        least_connections[r][
            "p99_ms"
        ]

        for r in EXPECTED_RATES
    ],

    marker="o",

    label=(
        "Phase III: "
        "Least Connections"
    )

)


plt.plot(

    EXPECTED_RATES,

    [
        least_response[r][
            "p99_ms"
        ]

        for r in EXPECTED_RATES
    ],

    marker="o",

    label=(
        "Phase III: "
        "Least Response Time"
    )

)


plt.xlabel(
    "Request rate (requests/s)"
)


plt.ylabel(

    "99th-percentile "
    "execution latency (ms)"

)


plt.title(

    "Phase II vs Phase III: "
    "99th-Percentile Latency"

)


plt.grid(True)

plt.legend()

plt.tight_layout()


plt.savefig(

    "results/"
    "phase3_p99_comparison.png",

    dpi=300

)


plt.close()


print("Created:")


print(

    "results/"
    "phase3_average_comparison.png"

)


print(

    "results/"
    "phase3_p99_comparison.png"

)