from threading import BrokenBarrierError
import logger
import threading

class ClientHandler(threading.Thread):
    def __init__( 
        self,
        client_protocol,
        lottery,
        quorum_condition,
        finished_agencies,
        draw_done,
        storage_lock,
        quorum_min,
        running_event
    ):
        super().__init__()
        self.protocol = client_protocol
        self.lottery = lottery
        self.quorum_condition = quorum_condition
        self.finished_agencies = finished_agencies
        self.draw_done = draw_done
        self.storage_lock = storage_lock
        self.quorum_min = quorum_min
        self.running_event = running_event

    def run(self):
        action = "handle-client"
        bets_amount = 0
        agency_id = None

        try:
            logger.info(action, logger.LogResult.in_progress)
            while self.running:
                kind, agency_id_recv, bets = self.protocol.recv_batch_or_finished()

                if kind is None or not self.running:
                    return

                if kind == "batch":
                    agency_id = agency_id_recv
                    try:
                        with self.storage_lock:
                            self.lottery.store_bets(bets)
                        self.protocol.send_batch_ack(True)
                        bets_amount += len(bets)
                    except Exception:
                        self.protocol.send_batch_ack(False)
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
                self.protocol.send_winners(winners)
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
                self.protocol.close()
            except Exception:
                pass


    def _wait_for_quorum(self, agency_id):
        with self.quorum_condition:
            if agency_id is not None:
                self.finished_agencies.add(agency_id)

            if len(self.finished_agencies) >= self.quorum_min:
                self.draw_done[0] = True
                self.quorum_condition.notify_all()
            else:
                self.quorum_condition.wait_for(
                    lambda: self.draw_done[0] or not self.running
                )

            return self.running and self.draw_done[0]
    
    @property # decorador para metodo como atributo
    def running(self):
        return self.running_event.is_set()