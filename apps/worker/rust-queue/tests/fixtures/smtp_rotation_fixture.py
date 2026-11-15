import base64, pathlib, socket, ssl, sys
cert, key, root = sys.argv[1:4]
listener = socket.socket(); listener.bind(('127.0.0.1', 0)); listener.listen(4); listener.settimeout(30)
print(listener.getsockname()[1], flush=True)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert, key)
for index, expected in enumerate(('old-fixture', 'new-fixture'), 1):
    conn, _ = listener.accept(); conn.settimeout(15); reader = conn.makefile('rb')
    conn.sendall(b'220 localhost ESMTP\r\n'); assert reader.readline().upper().startswith(b'EHLO'); conn.sendall(b'250-localhost\r\n250 STARTTLS\r\n')
    assert reader.readline().upper().startswith(b'STARTTLS'); conn.sendall(b'220 Ready\r\n'); reader.close()
    conn = context.wrap_socket(conn, server_side=True); reader = conn.makefile('rb')
    assert reader.readline().upper().startswith(b'EHLO'); conn.sendall(b'250-localhost\r\n250 AUTH PLAIN\r\n')
    auth = reader.readline().split(); assert len(auth) == 3 and auth[:2] == [b'AUTH', b'PLAIN']
    assert base64.b64decode(auth[2]) == b'\0fixture-user\0' + expected.encode()
    conn.sendall(b'235 Authentication successful\r\n'); assert reader.readline().upper().startswith(b'MAIL FROM'); conn.sendall(b'250 OK\r\n')
    assert reader.readline().upper().startswith(b'RCPT TO'); conn.sendall(b'250 OK\r\n'); assert reader.readline().upper().startswith(b'DATA'); conn.sendall(b'354 Continue\r\n')
    while True:
        line = reader.readline(); assert line
        if line == b'.\r\n': break
    conn.sendall(b'250 accepted\r\n'); pathlib.Path(root, 'accepted-'+str(index)).touch(); reader.close(); conn.close()
listener.close()
