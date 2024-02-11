import time
import threading
import asyncio
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from icecream import ic

class FileEventHandler(FileSystemEventHandler):
    def __init__(self):
        self.lock = threading.Lock()

    async def on_created(self, event):
        if not event.is_directory:
            self.lock.acquire()
            try:
                await asyncio.sleep(5)  # Simulating the lock for 5 seconds
                with open(event.src_path, 'r') as file:
                    content = file.read()
                    #ic("New file arrived:", event.src_path)
                    #ic("File content:", content)
            finally:
                self.lock.release()

if __name__ == "__main__":
    folder_path = "/home/deck/wt"
    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, folder_path, recursive=False)
    observer.start()

    try:
        loop = asyncio.get_event_loop()
        loop.run_forever()
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
