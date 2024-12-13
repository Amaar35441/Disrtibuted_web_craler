import queue
import threading

class URLQueue:
    def __init__(self, max_size=100):
        self.queue = queue.Queue(maxsize=max_size)
        self.lock = threading.Lock()

    def add_url(self, url):
        """
        Add URL to the queue
        """
        with self.lock:
            if not self.queue.full():
                self.queue.put(url)

    def get_url(self):
        """
        Get URL from the queue
        """
        with self.lock:
            if not self.queue.empty():
                return self.queue.get()
        return None

    def task_done(self):
        """
        Mark task as completed
        """
        self.queue.task_done()
