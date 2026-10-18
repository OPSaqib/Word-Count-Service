import asyncio
import datetime
import docker
import os
import queue
import random
import rpyc
import threading
import time

from rpyc.utils.server import ThreadedServer

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
        "healthy": True,
        "connection": None,
        "pool": None
    },

    {
        "name": "server2",
        "host": "server2",
        "port": 18861,
        "active": 0,
        "response_ms": None,
        "healthy": True,
        "connection": None,
        "pool": None
    },

    {
        "name": "server3",
        "host": "server3",
        "port": 18861,
        "active": 0,
        "response_ms": None,
        "healthy": True,
        "connection": None,
        "pool": None
    }
]

"""
TODO:
    - Make connection pooler per backend
    - Health check on new connection.ping
    - 
"""

class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.received = 0
        self.completed = 0
        self.in_flight = 0
        self.queue_estimate = 0

metrics = Metrics()


class ConnectionPool:
    def __init__(self, host, port, n_connections: int = 5):
        self.n_connections = n_connections
        self.host = host
        self.port = port
        self.connections = queue.Qeueu()
    
    def cleanup_connections(self):
        pass

    def connect(self):
        # TODO: handle connection error
        for n in self.n_connections:
            connection = rpyc.connect(self.host, self.port)
            self.connections.put(connection)

    def call(self, keyword, text):
        # TODO: handle error in conneciton and update health accordingly
        conn = self.connections.get(timeout=10)

        result = conn.root.count_word(keyword, text)

        self.connections.put(conn)

        return result

# A helper class that creates a thread-safe lock that can be used in asyncio routines as well
# Note: this does not work well if the critical section contains await expressions, it will potentially lead to deadlocks
class HybridThreadLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.duration = 0

    async def __aenter__(self):
        # Run lock acquisition on another thread, so that it does not block the asyncio loop
        await asyncio.to_thread(self.lock.acquire)

    async def __aexit__(self, type, value, traceback):
        # Lock release does not block so we run it directly.
        self.lock.release()

    def __enter__(self):
        start = time.perf_counter()
        self.lock.acquire()
        self.duration = time.perf_counter() - start

    def __exit__(self, type, value, traceback):
        local_duration = self.duration

        self.lock.release()

        if local_duration > 0.1:
            log(f"Lock acquire took {local_duration:.3f}s")

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
        connection = backend['connection']

        if connection is None:
            open_connection(backend)
            connection = backend['connection']

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
        set_unhealthy(backend, str(e))


# Choose the next backend server based on current state and the selected load balancing algorithm
def choose_backend():

    global tie_counter

    # Firstly lock the state to prevent race conditions when multiple clients are trying to choose a backend at the same time
    with state_lock:
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
def release_backend(backend):
    with state_lock:

        backend["active"] = max(0, backend["active"] - 1)

# Update the average response time for the backend server using an exponential moving average
def update_response_time(backend, sample_ms):
    with state_lock:

        old = backend["response_ms"]

        if old is None:

            backend["response_ms"] = sample_ms

        else:

            # Use exponential moving average to update the response time
            backend["response_ms"] = (0.8 * old + 0.2 * sample_ms)

class Proxy(rpyc.Service):

    # Proxy method
    def exposed_count_word(self, keyword, text):
        with metrics.lock:
            metrics.received += 1
            metrics.in_flight += 1

        uid = random.randint(10**5, 10**6-1)

        # We try at most len(BACKENDS)
        for _ in range(len(BACKENDS)):
            
            bstart = time.perf_counter()
            # Choose backend
            backend = choose_backend()

            bduration = time.perf_counter() - bstart
            
            if bduration > 0.1:
                log(f"took {bduration:.3f} to choose a backend")

            # perform request
            try: 
                # For tracking requests in the logs
                #log(f"ROUTE {uid} to {backend['name']}")

                start = time.perf_counter()
                
                # Each connection runs on a separate thread, so we can run this as a blocking call. All requests
                # are handled sequentially per connection in any case, even if they are request async on the client side.
                result = backend['connection'].root.count_word(keyword, text)

                release_backend(backend)

                # log(f'{uid} got result: {result}')

                response_ms = time.perf_counter() - start

                #log(f"FINISH {uid} in {response_ms:.3f} s")

                update_response_time(backend, response_ms)

                with metrics.lock:
                    metrics.completed += 1
                    metrics.in_flight -= 1
                    metrics.queue_estimate = metrics.received - metrics.completed
                
                return result

            except Exception as e:
                # log(f"Error while trying to make request to {backend['name']} {e}")
                # Mark backend as unhealthy and clean up connection
                set_unhealthy(backend, str(e))

                # Try other backend
                continue
        else:
            log(f"Could not respond to {uid}, no healthy backend available")

            with metrics.lock:
                metrics.completed += 1
                metrics.in_flight -= 1
                metrics.queue_estimate = metrics.received - metrics.completed

def serve_all_wrapper(backend):
    try:
        backend['connection'].serve_all()

        log(f"Serve thread for {backend['name']} stopped")
    except Exception as e:
        log(f"Serve thread for {backend['name']} exception: {e}")

def set_unhealthy(backend, reason='No reason given'):
    # If multiple requests fail, we don't want to acquire the lock for each failed attempt
    # Hence we check first if the backend is already set to unhealthy
    if not backend['healthy']:
        return

    log(f"{backend['name']} is unhealthy: {reason}")
    with state_lock:
        if backend['healthy']:
            backend['healthy'] = False
            backend['response_ms'] = None
            backend['connection'] = None


# The doctor performs health checks
def doctor(backend):
    while True:
        log(f"Queue estimate: {metrics.queue_estimate}")
        # Check and update health status of the backend
        check_health(backend)
    
        # restart if necessary
        if not backend['healthy']:
            #restart_backend(backend)
            check_health(backend)
        
        # Wait until next check
        time.sleep(1)


def open_connection(backend):
    connection = rpyc.connect(backend['host'], backend['port'], config={'sync_request_timeout': 5})
    #thread = threading.Thread(target=serve_all_wrapper, args=(backend,), daemon=False)
    #thread.start()

    with state_lock:
        backend['connection'] = connection

def restart_backend(backend):
    try:
        client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
        client.containers.get(f"wordcount-{backend['host']}").restart(timeout=0)

        open_connection(backend)

    except Exception as e:
        log(f"Could not restart service {backend['name']}: {e}")
        return

def main():
    if ALGORITHM not in {"least_connections", "least_response_time"}:
        raise ValueError("LB_ALGORITHM must be " "'least_connections' or " "'least_response_time'")

    # Create health-checking threads for each backend to avoid async rpyc callback issues
    for backend in BACKENDS:
        # Ensure it is running with a connection set up.
        #restart_backend(backend)
        open_connection(backend)

        # Run the health-checking on a separate thread to prevent blocking
        thread = threading.Thread(target=doctor, args=(backend,), daemon=False)
        thread.start()

    log("Starting Word Count Load Balancer")

    log(f"Algorithm: {ALGORITHM}")

    log(f"Listening on port " f"{LISTEN_PORT}")

    log("Backends: " "server1, server2, server3")

    server = ThreadedServer(Proxy, hostname="0.0.0.0", port=18861)

    server.start()

if __name__ == "__main__":
    main()

