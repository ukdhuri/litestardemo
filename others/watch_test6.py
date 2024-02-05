import time
from icecream import ic

import threading
import random
import time
def print_primes(n):
    time.sleep(2)  # Sleep for 2 seconds
    sieve = [True] * (n + 1)
    p = 2
    while p * p <= n:
        if sieve[p]:
            for i in range(p * p, n + 1, p):
                sieve[i] = False
        p += 1

    for p in range(2, n + 1):
        if sieve[p]:
            ic(p)

# Join all threads
def remove_finished_threads():
    while True:
        time.sleep(1)
        for t in globaltheads:
            if not t.is_alive():
                ic(f'removing thread... {t}')
                globaltheads.remove(t)


globaltheads = []
t = threading.Thread(target=remove_finished_threads)
t.daemon = True  # set the thread as a daemon
t.start()

def call_print_primes():
    n = random.randint(1, 1000)
    t = threading.Thread(target=print_primes, args=(n,))
    globaltheads.append(t)
    t.start()

for _ in range(25):
    call_print_primes()

ic(len(globaltheads))
time.sleep(10)
ic(len(globaltheads))




for t in globaltheads:
    ic(f'Joining thread... {t}')
    t.join()