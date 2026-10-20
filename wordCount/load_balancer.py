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
        "healthy": False,
        "pool": None
    },

    {
        "name": "server2",
        "host": "server2",
        "port": 18861,
        "active": 0,
        "response_ms": None,
        "healthy": False,
        "pool": None
    },

    {
        "name": "server3",
        "host": "server3",
        "port": 18861,
        "active": 0,
        "response_ms": None,
        "healthy": False,
        "pool": None
    }
]

class ConnectionPool:
    def __init__(self, host, port, n_connections: int = 5):
        self.n_connections = n_connections
        self.host = host
        self.port = port
        self.connections = queue.Queue()
    
    def cleanup_connections(self):
        while not self.connections.empty():
            conn = self.connections.get()
            conn.close()

    def connect(self):
        for n in range(self.n_connections):
            # Create connection
            connection = rpyc.connect(self.host, self.port, config={'close_catchall': True})

            # Ensure it works
            connection.ping()

            # Add it to the connection queue
            self.connections.put(connection)

    def call(self, keyword, text):
        conn = self.connections.get(timeout=10)

        result = conn.root.count_word(keyword, text)

        self.connections.put(conn)

        return result

# Thread-safe lock to protect multiple clients trying to access the same server at the same time
state_lock = threading.Lock()

# Rotate between servers equally good (round robin)
tie_counter = 0

def log(str):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S.%f')}] {str}")


# Performs a health check using an rpyc call
def check_health(backend):
    healthy = False
    try:
        # Open a connection to the backend
        connection = rpyc.connect(backend['host'], backend['port'])

        # Check is the backend server is working normally
        connection.ping()
    
        # The backend is healthy
        healthy = True
    except Exception as e:
        # If an exception occurs, the backend must not be healthy
        healthy = False
    finally:
        log(f"{backend['name']} {'passed' if healthy else 'did not pass' } the health-check")
        
        # If the healthy value matches the current marked value, we simply return
        with state_lock:
            if backend['healthy'] == healthy:
                return

        # Otherwise we need to update it
        if healthy:
            server_set_healthy(backend)
        else:
            server_set_unhealthy(backend)


# This function should only be called by check_health, running in the doctor threads
def server_set_healthy(backend):
    # We ensure healthy is false so we can be sure that no one is using its connection pool.
    with state_lock:
        if backend['healthy']:
            backend['healthy'] = False
    
    # Create a fresh connection pool
    pool = create_pool(backend)
    
    # If the pool connections could not be succesfully made, the backend is not yet healthy
    if pool == None:
        log(f"{backend['name']} passed the health check, but connections won't open.")
        return

    # We update the pool to a fresh pool and reset all statistics
    with state_lock:
        old_pool = backend['pool']
        backend['pool'] = pool
        backend['response_ms'] = None
        backend['healthy'] = True

    log(f"{backend['name']} is marked as healthy")
    
    # To be sure that we don't take up any resources, we close all old connections.
    # We specify close_catchall = True in the config of the connections, so this cannot fail
    if old_pool:
        old_pool.cleanup_connections()

# This function may be called from anywhere
def server_set_unhealthy(backend):
    did_change = False

    with state_lock:
        if backend['healthy']:
            backend['healthy'] = False
            did_change = True
    
    if did_change:
        log(f"{backend['name']} is marked as unhealthy")

def create_pool(backend):
    pool = ConnectionPool(backend['host'], backend['port'], 5)

    try:
        pool.connect()

        return pool
    except Exception as e:
        log(f"Exception during pool connect: {e}")
        return None

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
                result = backend['pool'].call(keyword, text)

                release_backend(backend)

                # log(f'{uid} got result: {result}')

                response_ms = time.perf_counter() - start

                # log(f"FINISH {uid} in {response_ms:.3f} s")

                update_response_time(backend, response_ms)
                
                return result

            except Exception as e:
                log(f"Error while trying to make request to {backend['name']} {e}")

                # Mark backend as unhealthy and clean up connection
                server_set_unhealthy(backend)

                # Try another backend
                continue
        
        # This runs only when the loop has finished, meaning all attempts have failed
        log(f"Could not respond to {uid}, no healthy backend available")

        # We need to raise an exception so that rpyc can communicate to the client that the request has not been fulfilled
        raise RuntimeError('No server available to fulfill the request')

def serve_all_wrapper(backend):
    try:
        backend['connection'].serve_all()

        log(f"Serve thread for {backend['name']} stopped")
    except Exception as e:
        log(f"Serve thread for {backend['name']} exception: {e}")


# The doctor performs health checks
def doctor(backend):
    while True:
        # Check and update health status of the backend
        check_health(backend)
    
        # restart if necessary
        if not backend['healthy']:
            restart_backend(backend)
            check_health(backend)
        
        # Wait until next check
        time.sleep(1)


def restart_backend(backend):
    try:
        client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
        client.containers.get(f"wordcount-{backend['host']}").restart(timeout=0)

    except Exception as e:
        log(f"Could not restart service {backend['name']}: {e}")
        return

def init_doctor_threads():
    for backend in BACKENDS:
        # Run the health-checking on a separate thread to prevent blocking
        thread = threading.Thread(target=doctor, args=(backend,), daemon=False)
        thread.start()


def main():
    if ALGORITHM not in {"least_connections", "least_response_time"}:
        raise ValueError("LB_ALGORITHM must be " "'least_connections' or " "'least_response_time'")

    init_doctor_threads()

    log("Starting Word Count Load Balancer")

    log(f"Algorithm: {ALGORITHM}")

    log(f"Listening on port " f"{LISTEN_PORT}")

    log("Backends: " "server1, server2, server3")

    server = ThreadedServer(Proxy, hostname="0.0.0.0", port=18861)

    server.start()

if __name__ == "__main__":
    main()

