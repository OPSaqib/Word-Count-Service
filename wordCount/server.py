import datetime
import os
import re
import redis
import rpyc
import time
from rpyc.utils.server import ThreadedServer


# Directory where text files are stored in the container
TEXT_DIRECTORY = "/app/texts"

# Get server1, server2 or server3 from environment variable, default to "server"
SERVER_NAME = os.getenv("SERVER_NAME", "server")

# Log requests if LOG_REQUESTS environment variable is set to "true"
LOG_REQUESTS = (os.getenv("LOG_REQUESTS", "false").lower() == "true")


# Retrieve which server must fail, in what way, and when. Defaults to server1 shutting down
FAILURE_TYPE = os.getenv("FAILURE_TYPE", "crash")
FAILURE_SERVER = os.getenv("FAILURE_SERVER", "server1");
FAILURE_DELAY_MS = int(os.getenv("FAILURE_DELAY_MS", "3000"))

server_start = None


def log(str):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S.%f')}] {str}")

def simulate_failure():
    global server_start

    if SERVER_NAME != FAILURE_SERVER:
        return

    # We set the server_start on the first request, so we have time to start the benchmark separately
    if server_start is None:
        server_start = time.perf_counter()
        return
   
    # We only want to fail after the specified delay
    if (time.perf_counter() - server_start) < FAILURE_DELAY_MS / 1000:
        return

    match FAILURE_TYPE:
        case "crash":
            log("Simulating crash")
            os._exit(1)
        case "deadlock":
            log("Simulating deadlock")
            time.sleep(99999999)
        case _:
            raise ArgumentException(f'Unknown failure type: "{FAILURE_TYPE}". Must be one of: "crash", "deadlock".')

# Connect to Redis and retry until successful
def connect_to_redis():
    while True:
        try:
            # Connect to Redis server
            client = redis.Redis(host="redis", port=6379, decode_responses=True)

            # Test connection
            client.ping()

            log("Connected to Redis")

            return client

        # If error occurs, try again
        except redis.exceptions.ConnectionError:

            log("Redis not ready, retrying")

            time.sleep(1)

# Save the Redis client
redis_client = connect_to_redis()

# Count whole-word occurrences in text
def count_word_in_text(keyword, text):

    # Regex pattern to match whole words, pattern is \bkeyword\b
    pattern = (r"\b" + re.escape(keyword) + r"\b")

    # Find every matching occurance of "pattern" in text ignoring case
    matches = re.findall(pattern, text, flags=re.IGNORECASE)

    return len(matches)

# Class to handle RPC requests for word counting
class WordCountService(rpyc.Service):

    # Expose the count_word method to be callable by clients
    def exposed_count_word(self, keyword, text_reference):
        # Simulate potential failure 
        simulate_failure()

        # Get filepath to be read from texts directory
        filename = os.path.basename(text_reference)

        filepath = os.path.join(TEXT_DIRECTORY, filename)

        if not os.path.isfile(filepath):

            raise FileNotFoundError(f"Text file " f"'{filename}' does not exist.")

        # Key,value pair for REDIS cache, format: wc:<filename>:<keyword> : VAL
        cache_key = (f"wc:" f"{filename}:" f"{keyword.casefold()}")

        # Increase the count of the keyword in the sorted set "hot_keywords" in Redis
        redis_client.zincrby("hot_keywords", 1, keyword.casefold())

        ####Check REDIS cache for result####

        # Check if result in Redis cache if so return
        cached_result = redis_client.get(cache_key)

        if cached_result is not None:

            if LOG_REQUESTS:

                log(f"HANDLED | " f"server={SERVER_NAME} | " f"keyword={keyword} | " f"cache=HIT | " f"count={cached_result}")

            return int(cached_result)

        ####If not in cache, read file and count occurrences####

        # Open text file and get it ready to read, ignore encoding errors
        with open(filepath, "r", encoding="utf-8", errors="ignore") as file:
            text = file.read()

        # Count occurrences of the keyword in the text
        count = count_word_in_text(keyword, text)

        ####Save result to REDIS cache####

        redis_client.set(cache_key, count)

        if LOG_REQUESTS:
            log(f"HANDLED | " f"server={SERVER_NAME} | " f"keyword={keyword} | " f"cache=MISS | " f"count={count}")

        return count

# Start the RPC server
if __name__ == "__main__":

    log(f"Starting Word Count Server: " f"{SERVER_NAME}")

    log("Listening on port 18861")

    # Create a RPyC network on WordCountService
    server = ThreadedServer(WordCountService, hostname="0.0.0.0", port=18861)
    
    server.start()
