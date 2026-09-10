import socket
import threading

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

    def _handle_client(self, client_socket):
        action = "handle-client"
        bets_amount = 0
        agency_id = None

        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                kind, agency_id_recv, bets = protocol.recv_batch_or_finished(client_socket)

                if kind is None:
                    logger.info(
                        action, logger.LogResult.success, "bets-amount", bets_amount
                    )
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

            self._wait_for_quorum(agency_id)

            with self.storage_lock:
                winners = [
                    bet.document
                    for bet in self.lottery.load_bets()
                    if bet.agency_id == agency_id and self.lottery.has_won(bet)
                ]
            protocol.send_winners(client_socket, winners)

            logger.info(
                action,
                logger.LogResult.success,
                "bets-amount", bets_amount,
                "winners-amount", len(winners),
            )

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "bets-amount", bets_amount
            )
            raise e
        finally:
            client_socket.close()

    def _wait_for_quorum(self, agency_id):
        with self.quorum_condition:
            self.finished_agencies.add(agency_id)

            if self.draw_done:
                return

            if len(self.finished_agencies) >= self.quorum_min:
                self.draw_done = True
                self.quorum_condition.notify_all()
            else:
                self.quorum_condition.wait_for(lambda: self.draw_done)

    def run(self):
        action = "accept-connection"
        threads = []
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                thread = threading.Thread(target=self._handle_client, args=(client_socket,))
                thread.start()
                threads.append(thread)