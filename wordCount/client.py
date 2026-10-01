import sys
import time
from benchmark import clear_cache
import rpyc

# Constants for server connection
SERVER_HOST = "server"
SERVER_PORT = 18861

def main():

    # We write command "python client.py keyword text.txt" so should be 3 arguments in sys.argv
    if len(sys.argv) != 3:
        print("Usage: python client.py ""<keyword> <text-file>")

        print("Example: ""python client.py hello sample.txt")

        return

    keyword = sys.argv[1]
    text_reference = sys.argv[2]

    print("Connecting to server")

    # Create a connection to the server using RPyC
    connection = rpyc.connect(SERVER_HOST, SERVER_PORT)

    print(f"Requesting count of '{keyword}' " f"in '{text_reference}'")

    # Measure the time taken to get the count from the server
    start = time.perf_counter()

    # Call exposed method on server count_word(keyword, text_reference)
    count = connection.root.count_word(keyword, text_reference)

    # Measure the time taken to get the count from the server
    end = time.perf_counter()

    # Calculate latency in milliseconds
    latency_ms = (end - start) * 1000

    #print()
    print(f"Keyword: {keyword}")
    print(f"Text: {text_reference}")
    print(f"Occurrences: {count}")
    print(f"Execution latency: {latency_ms:.3f} ms")

    connection.close()


if __name__ == "__main__":
    main()