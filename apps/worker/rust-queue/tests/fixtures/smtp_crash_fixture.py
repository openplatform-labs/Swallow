import socket, ssl, sys
from pathlib import Path
cert, key, directory = sys.argv[1:4]
plain = len(sys.argv) > 4 and sys.argv[4] == "plain"
root = Path(directory)
listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen(4)
listener.settimeout(30)
print(listener.getsockname()[1], flush=True)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(cert, key)
for count in (1, 2):
    conn, _ = listener.accept()
    conn.settimeout(30)
    reader = conn.makefile('rb')
    conn.sendall(b'220 localhost ESMTP\r\n')
    assert reader.readline().upper().startswith(b'EHLO')
    if plain:
        conn.sendall(b'250 localhost\r\n')
    else:
        conn.sendall(b'250-localhost\r\n250 STARTTLS\r\n')
        assert reader.readline().upper().startswith(b'STARTTLS')
        conn.sendall(b'220 Ready\r\n')
        reader.close()
        conn = context.wrap_socket(conn, server_side=True)
        reader = conn.makefile('rb')
        assert reader.readline().upper().startswith(b'EHLO')
        conn.sendall(b'250 localhost\r\n')
    assert reader.readline().upper().startswith(b'MAIL FROM')
    conn.sendall(b'250 OK\r\n')
    assert reader.readline().upper().startswith(b'RCPT TO')
    conn.sendall(b'250 OK\r\n')
    assert reader.readline().upper().startswith(b'DATA')
    conn.sendall(b'354 Continue\r\n')
    body = bytearray()
    while True:
        line = reader.readline()
        assert line
        if line == b'.\r\n': break
        body.extend(line)
    (root / f'message-{count}').write_bytes(body)
    if count == 1:
        # Wait for the worker to be killed after the relay has received DATA.
        assert not reader.readline()
    else:
        conn.sendall(b'250 accepted\r\n')
    reader.close()
    conn.close()
listener.settimeout(3)
try:
    extra, _ = listener.accept()
    extra.close()
    (root / 'extra-connection').touch()
except socket.timeout:
    pass
listener.close()
