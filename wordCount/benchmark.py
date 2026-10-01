import csv
import math
import os
import random
import re
import statistics
import threading
import time

import redis
import rpyc

# Constants for server connection
SERVER_HOST = "server"
SERVER_PORT = 18861

TEXT_REFERENCE = "sample2.txt"

# Text file used to investigate 
LOCAL_TEXT_PATH = "texts/sample2.txt"

# Request rates (requests per second)
REQUEST_RATES = [800, 900, 1000, 1100, 1200]

DURATION_SECONDS = 10

# Even for slow rates, want minimum number of requests 
MIN_REQUESTS = 1000

# Number of RPyC connections between client and server
NUM_CONNECTIONS = 100

# Max wait time for all responses 
RESPONSE_TIMEOUT_SECONDS = 60

# "cold": every request uses a different keyword -> every request is a cache miss
# "warm": cache is pre-filled, only a few keywords -> every request is a cache hit
CACHE_MODE = "cold"

WARM_KEYWORDS = ["the", "and", "time", "it", "world", "pleasure"]

# 99th percentile latency   
def percentile_99(values):
    sorted_values = sorted(values)
    index = math.ceil(0.99 * len(sorted_values)) - 1
    return sorted_values[index]

# Clear Redis cache before each different experiment
def clear_cache():
    client = redis.Redis(host="redis", port=6379, decode_responses=True)
    keys = list(client.scan_iter("wc:*"))
    if keys:
        client.delete(*keys)
    client.delete("hot_keywords")


def load_word_pool():
    if not os.path.isfile(LOCAL_TEXT_PATH): # No text file found, return empty list
        return []

    # Read file, covert to lowercase, extract words, shuffle them according to seed 42
    with open(LOCAL_TEXT_PATH, "r", encoding="utf-8", errors="ignore") as file:
        text = file.read().lower()
    words = sorted(set(re.findall(r"[a-z]+", text))) # set removes duplicates, sorted does alphabetical ordering
    random.Random(42).shuffle(words)
    return words

# All words in text file
WORD_POOL = load_word_pool()

# Generate a list of keywords for the experiment (according to total requests)
def make_keywords(n):

    # If warm then return warm keywords repeatedly until n is reached
    if CACHE_MODE == "warm":
        return [WARM_KEYWORDS[i % len(WARM_KEYWORDS)] for i in range(n)]

    # If cold then return all words in text file until n is reached, then add "nokeyword" to fill up to n
    keywords = WORD_POOL[:n]
    keywords += [f"nokeyword{i}" for i in range(len(keywords), n)]
    return keywords

# Prewarm cache by sending requests for warm keywords beforehand
def prewarm_cache():
    
    connection = rpyc.connect(SERVER_HOST, SERVER_PORT)
    for keyword in WARM_KEYWORDS:
        connection.root.count_word(keyword, TEXT_REFERENCE)
    connection.close()

# Create multiple RPyC connections to server and return them
def open_connections(n):
    connections = [] # List to hold RPyC connections
    async_calls = [] # List to hold async calls to server for count_word method

    for _ in range(n):
        connection = rpyc.connect(SERVER_HOST, SERVER_PORT) # Connect to server
        
        connection.ping() # Ping server

        # Create async call for count_word method
        async_call = rpyc.async_(connection.root.count_word)

        # Start a thread to serve all requests for this connection
        threading.Thread(target=connection.serve_all, daemon=True).start()

        # Add connection and async_call to lists
        connections.append(connection)
        async_calls.append(async_call)

    return connections, async_calls

# Run a single experiment with the request rate and async calls
def run_experiment(rate, async_calls):
    print()
    print("=" * 60)
    print(f"Starting experiment: {rate} requests/second ({CACHE_MODE} cache)")
    print("=" * 60)

    clear_cache() # Clear Redis cache before each experiment

    # Prewarm cache if CACHE_MODE is warm
    if CACHE_MODE == "warm":
        prewarm_cache()

    # Make keywords for number of requests calculated
    number_of_requests = max(rate * DURATION_SECONDS, MIN_REQUESTS)
    keywords = make_keywords(number_of_requests)

    # Threads to handle replies that finish at the same time to prevent modifying writing to arrays simultaneously
    lock = threading.Lock()
    all_done = threading.Event() # OFF: not all replies have arrived, ON: all replies have arrived
    latencies = []
    error_messages = []
    completed = 0

    # Callback function to handle responses from server
    def make_callback(sent_time):

        # Once response arrived execute this function
        def on_response(result):
            nonlocal completed
            received_time = time.perf_counter()

            with lock:
                try:
                    _ = result.value  # Response (error or count)
                    latencies.append((received_time - sent_time) * 1000) # Calculate latency in ms

                except Exception as error:
                    error_messages.append(str(error))

                completed += 1
                if completed == number_of_requests:
                    all_done.set()

        return on_response

    # Load generator
    max_lag_ms = 0.0
    start_time = time.perf_counter()

    # Loop to send requests at the specified rate
    for request_number in range(number_of_requests):

        # Calculate the target time for the next request based on the rate and the start time
        target_time = start_time + request_number / rate

        # Calculate how long to sleep until the target time is reached
        sleep_time = target_time - time.perf_counter()

        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            # Client cannot keep up with requests being sent
            max_lag_ms = max(max_lag_ms, -sleep_time * 1000)

        # Select async call (from the 1-n connections made)
        async_call = async_calls[request_number % len(async_calls)]

        # SENT: the timestamp is taken immediately before the request is written out
        sent_time = time.perf_counter()
        result = async_call(keywords[request_number], TEXT_REFERENCE) # SEND request to server
        result.add_callback(make_callback(sent_time)) # Run when callback arrives, response from server

    # Calculate end time for sending requests and elapsed time
    send_elapsed = time.perf_counter() - start_time

    # Wait for outstanding replies (nothing is sent anymore, this only collects)
    finished = all_done.wait(timeout=RESPONSE_TIMEOUT_SECONDS)

    # Perform analysis of results below
    with lock:
        successful = len(latencies)
        errors = len(error_messages)
        lost = number_of_requests - completed
        latencies_snapshot = list(latencies)

    if not finished:
        print(f"WARNING: {lost} replies did not arrive within "
              f"{RESPONSE_TIMEOUT_SECONDS} s and are not in the statistics.")

    if not latencies_snapshot:
        raise RuntimeError("No successful requests")

    # Perform statistics
    achieved_rate = number_of_requests / send_elapsed
    average_latency = statistics.mean(latencies_snapshot)
    p99_latency = percentile_99(latencies_snapshot)

    print(f"Requests: {number_of_requests}")
    print(f"Successful: {successful}")
    print(f"Errors: {errors}   No reply: {lost}")
    if error_messages:
        print("First error:", error_messages[0])
    print(f"Achieved send rate: {achieved_rate:.1f} req/s (target {rate})")
    print(f"Max send lag: {max_lag_ms:.1f} ms")
    print(f"Average latency: {average_latency:.3f} ms")
    print(f"P99 latency: {p99_latency:.3f} ms")

    if achieved_rate < 0.95 * rate:
        print("WARNING: client could not sustain the target rate; "
              "this point is limited by the load generator, not the server.")

    return {
        "rate": rate,
        "requests": number_of_requests,
        "successful": successful,
        "errors": errors + lost,
        "average_ms": average_latency,
        "p99_ms": p99_latency,
        "achieved_rate": achieved_rate,
        "max_send_lag_ms": max_lag_ms,
        "cache_mode": CACHE_MODE,
    }


def main():
    os.makedirs("results", exist_ok=True)

    # Open multiple connections to the server and get async calls for count_word method
    connections, async_calls = open_connections(NUM_CONNECTIONS)

    results = []

    # Run experiments for each request rate and collect results then close connections
    try:
        for rate in REQUEST_RATES:
            results.append(run_experiment(rate, async_calls))
            time.sleep(3)
    finally:
        for connection in connections:
            try:
                connection.close()
            except Exception:
                pass

    # Write to csv and save results
    output_file = "results/phase2_results.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["rate", "requests", "successful", "errors",
                        "average_ms", "p99_ms", "achieved_rate",
                        "max_send_lag_ms", "cache_mode"],
        )
        writer.writeheader()
        writer.writerows(results)

    print()
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    main()