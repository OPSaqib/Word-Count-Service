
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

# Connect to the load balancer by default, but allow override via environment variable
SERVER_HOST = os.getenv("SERVER_HOST", "load-balancer")
SERVER_PORT = 18861

# Load balancing algorithm to use (least_connections or least_response_time)
LB_ALGORITHM = os.getenv("LB_ALGORITHM", "least_connections").strip().lower()

TEXT_REFERENCE = "sample2.txt"

# Text file used to investigate
LOCAL_TEXT_PATH = "texts/sample2.txt"

# Request rates (requests per second)
REQUEST_RATES = [800, 900, 1000, 1100, 1200, 1500, 2000, 2500, 3000]

DURATION_SECONDS = 10

# Even for slow rates, want minimum number of requests
MIN_REQUESTS = 1000

# Number of RPyC connections between client and server
NUM_CONNECTIONS = 100

# Replace one of the 100 connections every 0.05 seconds (about 20 new connections/second)
# New connections are created in a background thread so the request sender is not delayed
ROTATION_INTERVAL_SECONDS = 0.05

# Max wait time for all responses
RESPONSE_TIMEOUT_SECONDS = 30

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

# Load all words from the text file into a list, shuffle them, and return the list
def load_word_pool():
    if not os.path.isfile(LOCAL_TEXT_PATH): # No text file found, return empty list
        return []

    # Read file, convert to lowercase, extract words, shuffle them according to seed 42
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
    try:
        for keyword in WARM_KEYWORDS:
            connection.root.count_word(keyword, TEXT_REFERENCE)
    finally:
        connection.close()


def connection_serve(slot):
    try:
        slot['connection'].serve_all()
    except Exception as e:
        print('Exception occurred when serving: {e}')

        # AsyncResults are never resolved. We need to update the pending completed results
        with pool_lock:
            pending = slot['pending']
            slot['pending'] = 0

        with lock:
            completed += pending


# Create one RPyC connection and its asynchronous call
def open_connection():
    connection = rpyc.connect(SERVER_HOST, SERVER_PORT) # Connect to load balancer
    try:
        connection.ping() # Ping server

        # Create async call for count_word method
        async_call = rpyc.async_(connection.root.count_word)

        slot = {"connection": connection, "async_call": async_call, "pending": 0}

        # Start a thread to serve all requests for this connection
        threading.Thread(target=connection_serve, args=(slot,), daemon=True).start()
        return slot
    except Exception:
        connection.close()
        raise

# Create multiple RPyC connections to server and return them
def open_connections(n):
    connections = [] # List to hold RPyC connections and async calls
    try:
        for _ in range(n):
            connections.append(open_connection())
    except Exception:
        close_connections(connections)
        raise RuntimeError('Initial connection could not be made')
    return connections

# Close all RPyC connections
def close_connections(connections):
    for slot in connections:
        try:
            slot["connection"].close()
        except Exception:
            pass

completed = 0 

# Run a single experiment with the request rate and async calls
def run_experiment(rate):
    print()
    print("=" * 70)
    print(f"Starting Phase III experiment: {rate} requests/second")
    print(f"Load-balancing algorithm: {LB_ALGORITHM}")
    print(f"Cache mode: {CACHE_MODE}")
    print("=" * 70)

    clear_cache() # Clear Redis cache before each experiment

    # Prewarm cache if CACHE_MODE is warm
    if CACHE_MODE == "warm":
        prewarm_cache()

    # Make keywords for number of requests calculated
    number_of_requests = max(rate * DURATION_SECONDS, MIN_REQUESTS)
    keywords = make_keywords(number_of_requests)

    # Open fresh connections to server for this experiment
    connections = open_connections(NUM_CONNECTIONS)
    retired_connections = [] # Old connections stay alive until their outstanding requests finish
    pool_lock = threading.Lock() # Protects the current connection slots and pending counters
    stop_rotation = threading.Event()
    rotation_count = 0
    rotation_errors = 0

    # Threads to handle replies that finish at the same time to prevent modifying writing to arrays simultaneously
    lock = threading.Lock()
    all_done = threading.Event() # OFF: not all replies have arrived, ON: all replies have arrived
    latencies = []
    error_messages = []

    global completed
    completed = 0

    # Callback function to handle responses from server
    def make_callback(sent_time, slot):
        # Once response arrived execute this function
        def on_response(result):
            global completed
            received_time = time.perf_counter()
            try:
                _ = result.value # Response (error or count)
                latency_ms = (received_time - sent_time) * 1000
                error = None
            except Exception as exception:
                latency_ms = None
                error = str(exception)

            # The old connection can now be closed if it was replaced by a new one
            with pool_lock:
                slot["pending"] -= 1

            with lock:
                if error is None:
                    latencies.append(latency_ms) # Calculate latency in ms
                else:
                    error_messages.append(error)
                completed += 1
                if completed == number_of_requests:
                    all_done.set()
        return on_response

    # Replace connections gradually while requests are running
    # The request-sending loop never waits for a new connection to be established
    def rotate_connections():
        nonlocal rotation_count, rotation_errors
        next_slot = 0

        while not stop_rotation.wait(ROTATION_INTERVAL_SECONDS):
            try:
                new_slot = open_connection() # Load balancer makes a new routing decision
            except Exception as error:
                rotation_errors += 1
                print(f"ROTATION ERROR | {error}")
                continue

            # Atomically replace one of the 100 available connections
            # Any requests already sent using the old connection can still finish
            with pool_lock:
                old_slot = connections[next_slot]
                connections[next_slot] = new_slot
                retired_connections.append(old_slot)
                next_slot = (next_slot + 1) % NUM_CONNECTIONS
                rotation_count += 1

                # Only close old connections when they have no outstanding requests
                ready_to_close = [slot for slot in retired_connections if slot["pending"] == 0]
                retired_connections[:] = [slot for slot in retired_connections if slot["pending"] > 0]

            # Connection cleanup also runs in the background, not in the request sender
            close_connections(ready_to_close)

    # Load generator
    max_lag_ms = 0.0
    start_time = time.perf_counter()
    rotation_thread = threading.Thread(target=rotate_connections, daemon=True)
    rotation_thread.start()

    try:
        # Loop to send requests at the specified rate
        for request_number in range(number_of_requests):
            # Calculate the target time for the next request based on the rate and the start time
            target_time = start_time + request_number / rate

            # Calculate how long to sleep until the target time is reached
            sleep_time = target_time - time.perf_counter()
            if sleep_time > 0:
                time.sleep(sleep_time)

            # Select async call (from the 100 currently available connections)
            # The lock ensures rotation cannot remove this connection before we mark its request as pending
            with pool_lock:
                slot = connections[request_number % NUM_CONNECTIONS]
                slot["pending"] += 1

            # SENT: the timestamp is taken immediately before the request is written out
            sent_time = time.perf_counter()

            # Measure any actual delay relative to the scheduled send time
            max_lag_ms = max(max_lag_ms, max(0.0, (sent_time - target_time) * 1000))
            try:
                result = slot["async_call"](keywords[request_number], TEXT_REFERENCE) # SEND request to server
                result.add_callback(make_callback(sent_time, slot)) # Run when callback arrives, response from server
            except Exception as error:
                with pool_lock:
                    slot["pending"] -= 1
                with lock:
                    error_messages.append(str(error))
                    completed += 1
                    if completed == number_of_requests:
                        all_done.set()

        # Calculate end time for sending requests and elapsed time
        send_elapsed = time.perf_counter() - start_time

    finally:
        # Stop creating new connections immediately after all requests have been sent
        stop_rotation.set()
        rotation_thread.join()

    time.sleep(1)

    #print([c['pending'] for c in connections])
    #print(completed)
    #print(number_of_requests)

    # Wait for outstanding replies (nothing is sent anymore, this only collects)
    finished = all_done.wait(timeout=RESPONSE_TIMEOUT_SECONDS)

    # Perform analysis of results below
    with lock:
        successful = len(latencies)
        errors = len(error_messages)
        lost = number_of_requests - completed
        latencies_snapshot = list(latencies)

    # Close all remaining current and old connections after replies have been collected
    with pool_lock:
        remaining_connections = list(connections) + list(retired_connections)
    #close_connections(remaining_connections)

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
    print(f"Connections rotated: {rotation_count}   Rotation errors: {rotation_errors}")
    print(f"Average latency: {average_latency:.3f} ms")
    print(f"P99 latency: {p99_latency:.3f} ms")

    if achieved_rate < 0.95 * rate:
        print("WARNING: client could not sustain the target rate; "
              "this point is limited by the load generator, not the server.")

    result_row = {
        "algorithm": LB_ALGORITHM,
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
    return result_row


def main():
    if LB_ALGORITHM not in {"least_connections", "least_response_time"}:
        raise ValueError("LB_ALGORITHM must be 'least_connections' or 'least_response_time'")

    os.makedirs("results", exist_ok=True)
    results = []

    # Run experiments for each request rate and collect results then close connections
    for rate in REQUEST_RATES:
        results.append(run_experiment(rate))
        time.sleep(3)

    # Write to CSV and save results
    output_file = f"results/phase3_{LB_ALGORITHM}.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "algorithm",
            "rate",
            "requests",
            "successful",
            "errors",
            "average_ms",
            "p99_ms",
            "achieved_rate",
            "max_send_lag_ms",
            "cache_mode"
        ])
        writer.writeheader()
        writer.writerows(results)

    print()
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
