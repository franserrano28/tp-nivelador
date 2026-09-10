import socket
import struct

from safe_socket import safe_socket
from lottery import Bet

_LENGTH_PREFIX_SIZE = 4

MSG_BATCH = 0x04
MSG_FINISHED = 0x02
MSG_WINNERS = 0x03
MSG_BATCH_ACK = 0x05

class Protocol:
    def __init__(self, client_socket: socket.socket):
        self.socket = client_socket

    def recv_batch_or_finished(self):
        msg_type, payload = self._recv_message()

        if msg_type is None:
            return None, None, None

        if msg_type == MSG_BATCH:
            lines = payload.decode("utf-8").split("\n")
            agency_id = int(lines[0])
            bets = []
            for record in lines[1:]:
                first_name, last_name, document, birthdate, number = record.split("|")
                bets.append(
                    Bet(agency_id, first_name, last_name, int(document), birthdate, int(number))
                )
            return "batch", agency_id, bets

        if msg_type == MSG_FINISHED:
            agency_id = int(payload.decode("utf-8"))
            return "finished", agency_id, None

        raise ValueError(f"Protocol: unexpected message type {msg_type}")


    def send_batch_ack(self, success: bool) -> None:
        payload = bytes([1 if success else 0])
        self._send_message(MSG_BATCH_ACK, payload)


    def send_winners(self, winners):
        lines = [
            f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
            for bet in winners
        ]
        payload = "\n".join(lines).encode("utf-8") if lines else b""
        self._send_message(MSG_WINNERS, payload)

    def close(self):
        self.socket.close()

    def _send_message(self, msg_type, payload):
        body = bytes([msg_type]) + payload
        length_prefix = struct.pack(">I", len(body))
        frame = length_prefix + body
        safe_socket.send_all(self.socket, frame)


    def _recv_message(self):
        length_prefix = safe_socket.recv_all(self.socket, _LENGTH_PREFIX_SIZE)
        if len(length_prefix) < _LENGTH_PREFIX_SIZE:
            return None, None

        (length,) = struct.unpack(">I", length_prefix)

        body = safe_socket.recv_all(self.socket, length)
        if not body:
            return None, None

        return body[0], body[1:]