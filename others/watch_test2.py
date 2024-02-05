import threading
import time
import threading

lock = threading.Lock()

def process_data(data):

    # Acquire the lock to ensure thread safety
    lock.acquire()

    try:
        # Perform some processing on the data
        processed_data = data * 2

        # Sleep for 5 seconds
        time.sleep(1)

        # Print the processed data
        print(f"Processed data: {processed_data}")
    finally:
        # Release the lock
        lock.release()




if __name__ == "__main__":
    # Create a list to store the threads
    threads = []

    # Define the number of times to call the function
    num_calls = 5

    # Create and start the threads
    for _ in range(num_calls):
        thread = threading.Thread(target=process_data, args=("example data",))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()
