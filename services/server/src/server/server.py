from .client_handler import ClientHandler
import socket
import threading
import signal

from protocol import Protocol
from lottery import Lottery


class Server:
    def __init__(
        self, server_host: str, server_port: int, storage_path: str, quorum_min: int
    ) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.lottery = Lottery(storage_path)
        self.quorum_min = quorum_min

        self.storage_lock = threading.Lock()

        self.quorum_condition = threading.Condition()
        self.finished_agencies = set()
        self.draw_done = [False]
        
        self.running_event = threading.Event()
        self.running_event.set()
        
        self.server_socket = None
        self.threads = []

        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        if not self.running:
            return
        
        self.running_event.clear()

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

        with self.quorum_condition:
            self.quorum_condition.notify_all()

    def run(self):
        action = "accept-connection"

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_host, self.server_port))
        self.server_socket.listen()

        while self.running:
            try:
                client_socket, _ = self.server_socket.accept()

                client = ClientHandler(
                    Protocol(client_socket),
                    self.lottery,
                    self.quorum_condition,
                    self.finished_agencies,
                    self.draw_done,
                    self.storage_lock,
                    self.quorum_min,
                    self.running_event
                )
                    
            except OSError:
                break

            self.threads = [t for t in self.threads if t.is_alive()]

            client.start()
            self.threads.append(client)

        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=1.0)


    @property
    def running(self):
        return self.running_event.is_set()