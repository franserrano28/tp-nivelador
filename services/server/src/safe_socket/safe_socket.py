import socket

def recv_all(socket: socket.socket, size):
    buffer = b""
    while len(buffer) < size:
        chunk = socket.recv(size - len(buffer))
        if not chunk:
            break
        buffer += chunk
     
    return buffer

def send_all(socket: socket.socket, data):
    total_sent = 0
    while total_sent < len(data):
        sent = socket.send(data[total_sent:])
        if sent == 0:
            raise ConnectionError
        total_sent += sent