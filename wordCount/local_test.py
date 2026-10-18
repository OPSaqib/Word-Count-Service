import threading
import rpyc
import time


def connection_serve(connection):
    try:
        connection.serve_all()
        print('serve_all just stopped')
        time.sleep(10)
    except Exception as e:
        print(f"Exception on serve thread: {e}")

def make_callback(i):
    def callback(result):
        print(f'Callback for request {i}')

        try:
            print(f"Result: {result.value}")
        except Exception as e:
            print(f"Exception when accessing result: {e}")

    return callback

def main():
    connection = rpyc.connect('localhost', 18861)

    async_call = rpyc.async_(connection.root.count_word)

    thread = threading.Thread(target=connection_serve, args=(connection,))
    thread.start()

    for i in range(5):
        print(f'Making request req {i}')
        try:
            result = async_call('the', 'sample2.txt')
            result.add_callback(make_callback(i))

            time.sleep(1)
        except Exception as e:
            print(f'Exception during request inititaion: {e}')
            break


main()
