import os
import re
import time

import redis
import rpyc
from rpyc.utils.server import ThreadedServer

# Directory where text files are stored in the container
TEXT_DIRECTORY = "/app/texts"

# Connect to Redis and retry until successful
def connect_to_redis():
    while True:
        try: 
            # Connect to Redis server
            client = redis.Redis(host="redis", port=6379, decode_responses=True)

            # Test connection
            client.ping()

            print("Connected to Redis")
            return client

        # If error occurs, try again
        except redis.exceptions.ConnectionError: 
            print("Redis not ready, retrying")
            time.sleep(1)

# Save the Redis client
redis_client = connect_to_redis()


# Count whole-word occurrences in text
def count_word_in_text(keyword, text):

    # Regex pattern to match whole words, pattern is \bkeyword\b
    pattern = r"\b" + re.escape(keyword) + r"\b"

    # Find every matching occurance of "pattern" in text ignoring case
    matches = re.findall(pattern, text, flags=re.IGNORECASE)

    return len(matches)

# Class to handle RPC requests for word counting
class WordCountService(rpyc.Service):

    # Expose the count_word method to be callable by clients
    def exposed_count_word(self, keyword, text_reference):

        # Get filepath to be read from texts directory
        filename = os.path.basename(text_reference)

        filepath = os.path.join(TEXT_DIRECTORY,filename)

        if not os.path.isfile(filepath):
            raise FileNotFoundError(f"Text file '{filename}' does not exist.")

        # Key,value pair for REDIS cache, format: wc:<filename>:<keyword> : VAL
        cache_key = f"wc:{filename}:{keyword.casefold()}"

        # Increase the count of the keyword in the sorted set "hot_keywords" in Redis
        redis_client.zincrby("hot_keywords", 1, keyword.casefold())

        ####Check REDIS cache for result####

        # Check if result in Redis cache if so return
        cached_result = redis_client.get(cache_key)

        if cached_result is not None:

            print(f"CACHE HIT | " f"keyword={keyword} | " f"file={filename} | " f"count={cached_result}")

            return int(cached_result)

        ####If not in cache, read file and count occurrences####

        print(f"CACHE MISS | " f"keyword={keyword} | " f"file={filename}")

        # Open text file and get it ready to read, ignore encoding errors
        with open(filepath, "r", encoding="utf-8", errors="ignore") as file:
            text = file.read()

        # Count occurrences of the keyword in the text
        count = count_word_in_text(keyword, text)

        ####Save result to REDIS cache####

        redis_client.set(cache_key, count)

        print(f"CALCULATED | " f"keyword={keyword} | " f"file={filename} | " f"count={count}")

        return count


if __name__ == "__main__":

    print("Starting Word Count Server")
    print("Listening on port 18861")

    # Create threader RPC server, 
    # listen for connections on all network interfaces inside this container on port 18861
    server = ThreadedServer(WordCountService, hostname="0.0.0.0", port=18861)

    server.start()