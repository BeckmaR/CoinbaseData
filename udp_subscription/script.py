import socket
import re
server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.bind(('', 12000))

pattern = re.compile(r"trade_id=([0-9]+)")

while True:
    data, server = server_socket.recvfrom(1024)
    msg = data.decode('utf-8')
    match = pattern.search(msg)
    if match is not None:
        trade_id = match.group(1)
        print(trade_id)