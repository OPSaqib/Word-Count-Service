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

SERVER_HOST = "server"
SERVER_PORT = 18861

TEXT_REFERENCE = "sample2.txt"

# Optional local copy of the text (inside the client container) used to draw
# real words as keywords. If it does not exist, synthetic keywords are used.
LOCAL_TEXT_PATH = "texts/sample2.txt"

# Request rates (requests per second)
REQUEST_RATES = [500, 600, 700, 800, 900, 1000]

DURATION_SECONDS = 10

# Every rate sends at least this many requests, so the p99 is meaningful
MIN_REQUESTS = 1000

# Number of client connections (simulated clients). Requests are spread over them
# round-robin. Sending never waits for a connection to be free: each request is
# written to its socket immediately and the reply is handled whenever it arrives.
NUM_CONNECTIONS = 100

# Give up waiting for outstanding replies after this long once sending is finished
RESPONSE_TIMEOUT_SECONDS = 60

# "cold": every request uses a different keyword -> every request is a cache miss
# "warm": cache is pre-filled, only a few keywords -> every request is a cache hit
CACHE_MODE = "cold"

WARM_KEYWORDS = ["the", "and", "time", "it", "world", "pleasure"]


def percentile_99(values):
    sorted_values = sorted(values)
    index = math.ceil(0.99 * len(sorted_values)) - 1
    return sorted_values[index]


def clear_cache():
    client = redis.Redis(host="redis", port=6379, decode_responses=True)
    keys = list(client.scan_iter("wc:*"))
    if keys:
        client.delete(*keys)
    client.delete("hot_keywords")


def load_word_pool():
    if not os.path.isfile(LOCAL_TEXT_PATH):
        return []
    with open(LOCAL_TEXT_PATH, "r", encoding="utf-8", errors="ignore") as file:
        text = file.read().lower()
    words = sorted(set(re.findall(r"[a-z]+", text)))
    random.Random(42).shuffle(words)
    return words


WORD_POOL = load_word_pool()


def make_keywords(n):
    if CACHE_MODE == "warm":
        return [WARM_KEYWORDS[i % len(WARM_KEYWORDS)] for i in range(n)]

    # cold: n unique keywords. Real words first, synthetic ones if the pool runs out.
    # A synthetic keyword costs the same as a real one (the whole file is still scanned).
    keywords = WORD_POOL[:n]
    keywords += [f"nokeyword{i}" for i in range(len(keywords), n)]
    return keywords


def prewarm_cache():
    # Fill the cache with a plain synchronous connection (untimed)
    connection = rpyc.connect(SERVER_HOST, SERVER_PORT)
    for keyword in WARM_KEYWORDS:
        connection.root.count_word(keyword, TEXT_REFERENCE)
    connection.close()


def open_connections(n):
    connections = []
    async_calls = []

    for _ in range(n):
        connection = rpyc.connect(SERVER_HOST, SERVER_PORT)

        # Untimed first-touch work: handshake and fetching the remote method proxy
        connection.ping()
        async_call = rpyc.async_(connection.root.count_word)

        # This thread only receives replies for this connection and fires callbacks.
        # It does not limit sending in any way.
        threading.Thread(target=connection.serve_all, daemon=True).start()

        connections.append(connection)
        async_calls.append(async_call)

    return connections, async_calls


def run_experiment(rate, async_calls):
    print()
    print("=" * 60)
    print(f"Starting experiment: {rate} requests/second ({CACHE_MODE} cache)")
    print("=" * 60)

    clear_cache()

    if CACHE_MODE == "warm":
        prewarm_cache()

    number_of_requests = max(rate * DURATION_SECONDS, MIN_REQUESTS)
    keywords = make_keywords(number_of_requests)

    lock = threading.Lock()
    all_done = threading.Event()
    latencies = []
    error_messages = []
    completed = 0

    def make_callback(sent_time):
        # Called when the reply for one request arrives
        def on_response(result):
            nonlocal completed
            received_time = time.perf_counter()

            with lock:
                try:
                    _ = result.value  # raises if the server returned an error
                    latencies.append((received_time - sent_time) * 1000)
                except Exception as error:
                    error_messages.append(str(error))

                completed += 1
                if completed == number_of_requests:
                    all_done.set()

        return on_response

    max_lag_ms = 0.0
    start_time = time.perf_counter()

    for request_number in range(number_of_requests):
        target_time = start_time + request_number / rate
        sleep_time = target_time - time.perf_counter()

        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            # The generator is behind schedule: the client itself is a bottleneck
            max_lag_ms = max(max_lag_ms, -sleep_time * 1000)

        async_call = async_calls[request_number % len(async_calls)]

        # SENT: the timestamp is taken immediately before the request is written out
        sent_time = time.perf_counter()
        result = async_call(keywords[request_number], TEXT_REFERENCE)
        result.add_callback(make_callback(sent_time))

    send_elapsed = time.perf_counter() - start_time

    # Wait for outstanding replies (nothing is sent anymore, this only collects)
    finished = all_done.wait(timeout=RESPONSE_TIMEOUT_SECONDS)

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

    connections, async_calls = open_connections(NUM_CONNECTIONS)

    results = []

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