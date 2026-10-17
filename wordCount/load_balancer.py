import asyncio
import datetime
import docker
import os
import random
import rpyc
import threading
import time

# Load Balancer Configuration
# TEST GNG
LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 18861

# Select load balancing algorithm from environment variable, default to least_connections
ALGORITHM = os.getenv("LB_ALGORITHM", "least_connections").strip().lower()

# Backend server configuration
# active --> number of active connections to this backend
# response_ms --> average response time in milliseconds
BACKENDS = [
    {
        "name": "server1",
        "host": "server1",
        "port": 18861,
        "active": 0,
        "response_ms": None,
        "healthy": True
    },

    {
        "name": "server2",
        "host": "server2",
        "port": 18861,
        "active": 0,
        "response_ms": None,
        "healthy": True
    },

    {
        "name": "server3",
        "host": "server3",
        "port": 18861,
        "active": 0,
        "response_ms": None,
        "healthy": True
    }
]

# A helper class that creates a thread-safe lock that can be used in asyncio routines as well
# Note: this does not work well if the critical section contains await expressions, it will potentially lead to deadlocks
class HybridThreadLock:
    def __init__(self):
        self.lock = threading.Lock()

    async def __aenter__(self):
        # Run lock acquisition on another thread, so that it does not block the asyncio loop
        await asyncio.to_thread(self.lock.acquire)

    async def __aexit__(self, type, value, traceback):
        # Lock release does not block so we run it directly.
        self.lock.release()

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, type, value, traceback):
        self.lock.release()

# Thread-safe lock to protect multiple clients trying to access the same server at the same time
state_lock = HybridThreadLock()

# Rotate between servers equally good (round robin)
tie_counter = 0

def log(str):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S.%f')}] {str}")

# Helper function to format response time for logging
def format_response(value):
    if value is None:
        return "unknown"

    return f"{value:.3f} ms"


# Performs a health check using an rpyc call
def check_health(backend):
    try:
        # Open a connection to the backend
        connection = rpyc.connect(backend['host'], backend['port'])

        # Check is the backend server is working normally
        connection.ping()
        
        # Set the health back to True
        with state_lock:
            if not backend['healthy']:
                log(f"{backend['name']} is healthy")
                backend['healthy'] = True
                backend['response_ms'] = None
    except Exception as e:
        # If an exception occurs, the backend must not be healthy
        with state_lock:
            log(f"{backend['name']} is unhealthy")
            backend['healthy'] = False


# Choose the next backend server based on current state and the selected load balancing algorithm
async def choose_backend():

    global tie_counter

    # Firstly lock the state to prevent race conditions when multiple clients are trying to choose a backend at the same time
    async with state_lock:
        # Only consider healthy backends
        healthy_backends = [backend for backend in BACKENDS if backend['healthy']]

        if ALGORITHM == "least_connections":

            # Find the minimum number of active connections among all backends
            minimum = min(backend["active"] for backend in healthy_backends)

            # Select all backends that have the minimum number of active connections
            candidates = [backend for backend in healthy_backends if backend["active"] == minimum]

        elif ALGORITHM == "least_response_time":

            # Find backends that have not yet measured response time
            unmeasured = [backend for backend in healthy_backends if backend["response_ms"] is None]

            # If there are unmeasured backends, select the one with the least active connections
            if unmeasured:

                minimum_active = min(backend["active"] for backend in unmeasured)

                # Select all unmeasured backends that have the minimum number of active connections
                candidates = [backend for backend in unmeasured if backend["active"] == minimum_active]

            # Otherwise no unmeasured backends, select the backend with the least response time and least active connections
            else:

                # Find the minimum response time among all backends
                minimum_response = min(backend["response_ms"] for backend in healthy_backends)

                # 5% threshold to allow for some variance in response times
                threshold = (minimum_response* 1.05)

                # Select all backends that are within the threshold of the minimum response time
                near_fastest = [backend for backend in healthy_backends if backend["response_ms"] <= threshold]

                # From the fastest backends, find the minimum number of active connections
                minimum_active = min(backend["active"] for backend in near_fastest)

                # Select all backends that are within the threshold of the minimum response time and have the minimum number of active connections
                candidates = [backend for backend in near_fastest if backend["active"] == minimum_active]

        else:
            raise ValueError("LB_ALGORITHM must be " "'least_connections' or " "'least_response_time'")

        # If there are multiple candidates, use round-robin selection to choose one
        backend = candidates[tie_counter % len(candidates)]

        tie_counter += 1

        backend["active"] += 1

        return backend

# Release the backend server after the request is completed
async def release_backend(backend):
    async with state_lock:

        backend["active"] = max(0, backend["active"] - 1)

# Update the average response time for the backend server using an exponential moving average
async def update_response_time(backend, sample_ms):

    async with state_lock:

        old = backend["response_ms"]

        if old is None:

            backend["response_ms"] = sample_ms

        else:

            # Use exponential moving average to update the response time
            backend["response_ms"] = (0.8 * old + 0.2 * sample_ms)

# Handle the client connection, route the request to the selected backend, and manage the data transfer between client and backend
# client_reader: Read byte stream from the client
# client_writer: Write byte stream to the client
async def handle_client(client_reader, client_writer):
    
    # Unique ID for tracking connections in the logs
    uid = random.randint(10**5, 10**6-1)

    peer = client_writer.get_extra_info("peername")

    # We attempt to choose a backend at most len(BACKENDS) times, if it does not succeed then it is unlikely that any backend is healthy.
    for _ in range(len(BACKENDS)):

        # Choose the backend server based on the selected load balancing algorithm
        backend = await choose_backend()

        log(f"ROUTE | id={uid} | " f"algorithm={ALGORITHM} | " f"client={peer} | " f"backend={backend['name']} | " f"active={backend['active']} | " f"response=" f"{format_response(backend['response_ms'])}")

        # Establish a connection to the selected backend server
        # We have: Client <-> Load Balancer <-> Backend Server
        try:
            (
                backend_reader,
                backend_writer
            ) = await asyncio.open_connection(backend["host"], backend["port"])
            
            # The connection is succesful, we can continue without trying again
            break

        except Exception as error:
            log(f"BACKEND CONNECT ERROR | id={uid} | " f"backend={backend['name']} | " f"error={error}")
            
            # Update health
            async with state_lock:
                backend['healthy'] = False
                backend['response_ms'] = None
            
            await release_backend(backend)

            # We try again
            continue

    # If the loop is not broken out of, then all attempts have failed and we must close the client connection
    else:
            client_writer.close()

            await client_writer.wait_closed()

            return

    # Estimate the backend response time
    waiting_since = None

    # Continuosly read data from the client and send it to the backend server
    async def client_to_server():

        nonlocal waiting_since

        # Forever loop to read data from the client and send it to the backend server
        while True:

            # Read up to 65536 bytes from the client
            data = await client_reader.read(65536)

            if not data:
                break

            if waiting_since is None:

                # Start measuring the time since we sent the request to the backend server
                waiting_since = (time.perf_counter())

            # Write the data to the backend server
            backend_writer.write(data)

            # Drain the write buffer to ensure that all data is sent to the backend server
            await backend_writer.drain()

    # Continuosly read data from the backend server and send it to the client
    async def server_to_client():

        nonlocal waiting_since

        try:
            while True:

                # Read up to 65536 bytes from the backend server
                data = await backend_reader.read(65536)

                if not data:
                    break

                if waiting_since is not None:

                    # Calculate the time taken for the backend server to respond and convert it to milliseconds
                    sample_ms = (time.perf_counter() - waiting_since) * 1000

                    waiting_since = None

                    # Update the average response time for the backend server
                    await update_response_time(backend, sample_ms)

                # Write the data to the client
                client_writer.write(data)

                # Drain the write buffer to ensure that all data is sent to the client
                await client_writer.drain()
        except asyncio.CancelledError:
            # Send an explicit RST to prevent timeout hangs on the client's side
            client_writer.transport.abort()

            # We must not swallow the CancelledException according to the asyncio docs
            raise

    # Create two tasks to handle the data transfer between the client and the backend server concurrently
    task_up = asyncio.create_task(client_to_server())

    task_down = asyncio.create_task(server_to_client())

    # Wait for either of the tasks to complete (i.e., either the client or the backend server closes the connection)
    done, pending = await asyncio.wait([task_up, task_down], return_when=(asyncio.FIRST_COMPLETED))
    
    # Track if the connection is closed by the client or the server
    did_client_disconnect = task_up in done

    for task in pending:
        task.cancel()

    # Wait for the pending tasks to be cancelled and handle any exceptions that may occur during cancellation
    pending_results = await asyncio.gather(*pending, return_exceptions=True)

    done_results = await asyncio.gather(*done, return_exceptions=True)

    # Close the connections to the backend server and the client
    backend_writer.close()
    client_writer.close()

    # Wait for the connections to be fully closed and handle any exceptions that may occur during closure
    await asyncio.gather(backend_writer.wait_closed(), client_writer.wait_closed(), return_exceptions=True)

    # Release the backend server after the request is completed
    await release_backend(backend)

    log(f"CLOSE | id={uid} | closed by: {'client' if did_client_disconnect else 'server'} | " f"backend={backend['name']} | " f"active={backend['active']} | " f"response=" f"{format_response(backend['response_ms'])}")

# The doctor performs health checks
def doctor(backend):
    while True:
        # Check and update health status of the backend
        check_health(backend)
    
        # restart if necessary
        if not backend['healthy']:
            restart_backend(backend)

        # Sleep for 1 second before checking again
        time.sleep(1.0)

def restart_backend(backend):
    try:
        client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
        client.containers.get(f"wordcount-{backend['host']}").restart(timeout=0)
    except Exception as e:
        log(f"Could not restart service {backend['name']}: {e}")

async def main():
    if ALGORITHM not in {"least_connections", "least_response_time"}:

        raise ValueError("LB_ALGORITHM must be " "'least_connections' or " "'least_response_time'")

    # Create health-checking threads for each backend to avoid async rpyc callback issues
    for backend in BACKENDS:
        thread = threading.Thread(target=doctor, args=(backend,))
        thread.start()

    # Create actual TCP server that listens for incoming client connections and routes them to the appropriate backend server based on the selected load balancing algorithm
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)

    log("Starting Word Count Load Balancer")

    log(f"Algorithm: {ALGORITHM}")

    log(f"Listening on port " f"{LISTEN_PORT}")

    log("Backends: " "server1, server2, server3")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":

    asyncio.run(
        main()
    )
