import socket
import threading
import signal
import sys

import logger
import protocol
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
        self.draw_done = False
        
        self.running = True
        self.server_socket = None
        self.threads = []

        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        signal.signal(signal.SIGINT, self.__handle_shutdown)

    def __handle_shutdown(self, signum, frame):
        if not self.running:
            return
        
        self.running = False

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

        with self.quorum_condition:
            self.quorum_condition.notify_all()

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets_amount = 0
        agency_id = None

        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.running:
                kind, agency_id_recv, bets = protocol.recv_batch_or_finished(client_socket)

                if kind is None or not self.running:
                    return

                if kind == "batch":
                    agency_id = agency_id_recv
                    try:
                        with self.storage_lock:
                            self.lottery.store_bets(bets)
                        protocol.send_batch_ack(client_socket, True)
                        bets_amount += len(bets)
                    except Exception:
                        protocol.send_batch_ack(client_socket, False)
                    continue

                if kind == "finished":
                    agency_id = agency_id_recv
                    break

            if not self.running:
                return

            if not self._wait_for_quorum(agency_id):
                return

            with self.storage_lock:
                winners = [
                    bet
                    for bet in self.lottery.load_bets()
                    if bet.agency_id == agency_id and self.lottery.has_won(bet)
                ]
            
            if self.running:
                protocol.send_winners(client_socket, winners)
                logger.info(
                    action,
                    logger.LogResult.success,
                    "bets-amount", bets_amount,
                    "winners-amount", len(winners),
                )

        except Exception as e:
            if self.running:
                logger.error(action, logger.LogResult.fail, "bets-amount", bets_amount)
                raise e
        finally:
            try:
                client_socket.close()
            except Exception:
                pass

    def _wait_for_quorum(self, agency_id):
        with self.quorum_condition:
            if agency_id is not None:
                self.finished_agencies.add(agency_id)

            if len(self.finished_agencies) >= self.quorum_min:
                self.draw_done = True
                self.quorum_condition.notify_all()
            else:
                self.quorum_condition.wait_for(lambda: self.draw_done or not self.running)

            return self.running and self.draw_done

    def run(self):
        action = "accept-connection"

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_host, self.server_port))
        self.server_socket.listen()

        while self.running:
            try:
                client_socket, _ = self.server_socket.accept()
            except OSError:
                break

            self.threads = [t for t in self.threads if t.is_alive()]

            thread = threading.Thread(target=self._handle_client, args=(client_socket,))
            thread.daemon = True
            thread.start()
            self.threads.append(thread)

        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=1.0)