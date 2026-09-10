import socket

def recv_all(socket, size):
    buffer = bytearray()
    while len(buffer) < size:
        chunk = socket.recv(size - len(buffer))
        if not chunk:
            if not buffer:
                return b""
            raise ConnectionError("Safe Socket: connection closed before receiving all bytes")
        buffer.extend(chunk)
    return buffer

def send_all(socket: socket.socket, data):
    total_sent = 0
    view = memoryview(data)

    while total_sent < len(data):
        sent = socket.send(view[total_sent:]) # con view ahorro memoria en vez de crear slices en cada iteracion
        if sent is None:
            raise ConnectionError("Safe Socket: connection closed before sending all bytes")
        total_sent += sent