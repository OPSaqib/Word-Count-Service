import csv
import math
import os
import statistics
import threading
import time

# Multiple requests concurrently with threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis
import rpyc

# Constants for server connection
SERVER_HOST = "server"
SERVER_PORT = 18861

TEXT_REFERENCE = "sample2.txt"

# Request rates (requests per second)

REQUEST_RATES = [880, 890, 900, 910, 920]

#REQUEST_RATES = [50, 60, 70, 80, 90, 100]

# Run each workload for 10 seconds
DURATION_SECONDS = 10

# Maximum number of concurrent threads to use for sending requests
MAX_WORKERS = 100


# Keywords to check
KEYWORDS = [
    "the",
    "and",
    "time",
    "it",
    "world",
    "pleasure"
]

# Thread local storage for RPyC connections
thread_local = threading.local()

# Create a connection for the current thread
def get_connection():

    # If current thread does not have connection, create a new connection
    if not hasattr(thread_local, "connection"):

        thread_local.connection = rpyc.connect(SERVER_HOST, SERVER_PORT)

    return thread_local.connection # Return the connection for the current thread

# Send one request to the server and return the latency and count
def send_one_request(keyword):

    # Get the connection for the current thread
    connection = get_connection()

    # Measure the time taken to get the count from the server
    start = time.perf_counter()

    # Call exposed method on server count_word(keyword, text_reference)
    count = connection.root.count_word(keyword, TEXT_REFERENCE)

    # Measure the time taken to get the count from the server
    end = time.perf_counter()

    # Calculate latency in milliseconds
    latency_ms = (end - start) * 1000

    # Return the latency and count
    return latency_ms, count

# Calculate the 99th percentile of a list of values
def percentile_99(values):

    # Sort the values
    sorted_values = sorted(values)

    # Calculate the index of the 99th percentile
    index = math.ceil(0.99 * len(sorted_values)) - 1

    # Return the value at the 99th percentile index
    return sorted_values[index]

# Clear the word count cache in Redis (for each experiment)
def clear_word_count_cache():

    # Create a Redis client to connect to the Redis server
    client = redis.Redis(host="redis", port=6379, decode_responses=True)

    # Get all keys that match the pattern "wc:*" (word count cache keys)
    keys = list(client.scan_iter("wc:*"))

    # If there are any keys, delete them from the Redis cache
    if keys:
        client.delete(*keys)

# Run an experiment with a given request rate (requests per second)
def run_experiment(rate):

    print()
    print("=" * 60)
    print(f"Starting experiment: " f"{rate} requests/second")
    print("=" * 60)

    # Clear the word count cache in Redis before starting the experiment
    clear_word_count_cache()

    # Calculate the total number of requests to send during the experiment
    number_of_requests = (rate * DURATION_SECONDS)

    latencies = []
    errors = 0

    futures = [] # Unfinished request to keep track of completion of requests

    # To help calculate when requests should be send to achieve desired rate of requests per second
    start_time = time.perf_counter()

    # Use a thread pool executor to send requests concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        # Schedule requests to be sent at the specified rate
        for request_number in range(number_of_requests):

            # Desired send time for this request
            target_time = (start_time + request_number / rate)

            # Calculate how long to sleep until the target time for this request
            sleep_time = (target_time - time.perf_counter())

            if sleep_time > 0:
                time.sleep(sleep_time)

            # Select a keyword to send with this request from the list
            keyword = KEYWORDS[request_number % len(KEYWORDS)]

            # Give one worker the job of running send_one_request(keyword)
            future = executor.submit(send_one_request, keyword)

            futures.append(future)

        # Wait for all requests to complete and collect the results
        for future in as_completed(futures):

            # Get result of request if no error, otherwise count the error and print it
            try:

                # Get the latency and save it to the list of latencies 
                latency_ms, _ = future.result()

                latencies.append(latency_ms)

            except Exception as error:

                errors += 1

                print("Request failed:", error)

    if not latencies:
        raise RuntimeError("No successful requests")

    # Calculate average latency and 99th percentile latency
    average_latency = statistics.mean(latencies)

    p99_latency = percentile_99(latencies)

    # Print the results of the experiment
    print(f"Requests: " f"{number_of_requests}")

    print(f"Successful: " f"{len(latencies)}")

    print(f"Errors: " f"{errors}")

    print(f"Average latency: " f"{average_latency:.3f} ms")

    print(f"P99 latency: " f"{p99_latency:.3f} ms")

    return {
        "rate": rate,
        "requests": number_of_requests,
        "successful": len(latencies),
        "errors": errors,
        "average_ms": average_latency,
        "p99_ms": p99_latency
    }


def main():

    # Create a directory to store the results if it doesn't exist
    os.makedirs("results", exist_ok=True)

    results = []

    # Run experiments for each request rate and collect the results
    for rate in REQUEST_RATES:

        result = run_experiment(rate)

        results.append(result)

        # Short rest before next workload
        time.sleep(3)

    # Save the results to a CSV file
    output_file = ("results/phase2_results.csv")

    with open(output_file, "w", newline="", encoding="utf-8") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=["rate", "requests", "successful", "errors", "average_ms", "p99_ms"]
        )

        writer.writeheader()

        writer.writerows(results)

    print()
    print(f"Results saved to " f"{output_file}")


if __name__ == "__main__":
    main()