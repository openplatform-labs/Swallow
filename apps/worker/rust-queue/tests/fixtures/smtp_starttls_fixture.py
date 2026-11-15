import socket, ssl, sys
cert, key, output = sys.argv[1:4]
connection_marker = sys.argv[4] if len(sys.argv) > 4 else None
listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen(1)
listener.settimeout(10)
print(listener.getsockname()[1], flush=True)
conn, _ = listener.accept()
if connection_marker:
    open(connection_marker, "w").close()
conn.settimeout(10)
reader = conn.makefile('rb')
conn.sendall(b'220 localhost ESMTP\r\n')
assert reader.readline().upper().startswith(b'EHLO')
conn.sendall(b'250-localhost\r\n250 STARTTLS\r\n')
assert reader.readline().upper().startswith(b'STARTTLS')
conn.sendall(b'220 Ready\r\n')
reader.close()
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(cert, key)
conn = context.wrap_socket(conn, server_side=True)
reader = conn.makefile('rb')
assert reader.readline().upper().startswith(b'EHLO')
conn.sendall(b'250-localhost\r\n250 AUTH PLAIN\r\n')
assert reader.readline().upper().startswith(b'AUTH PLAIN')
conn.sendall(b'235 Authentication successful\r\n')
assert reader.readline().upper().startswith(b'MAIL FROM')
conn.sendall(b'250 OK\r\n')
assert reader.readline().upper().startswith(b'RCPT TO')
conn.sendall(b'250 OK\r\n')
assert reader.readline().upper().startswith(b'DATA')
conn.sendall(b'354 Continue\r\n')
message = bytearray()
while True:
    line = reader.readline()
    assert line
    if line == b'.\r\n':
        break
    message.extend(line)
with open(output, 'wb') as file:
    file.write(message)
conn.sendall(b'250 accepted\r\n')
reader.close()
conn.close()
listener.close()
