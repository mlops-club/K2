"""A minimal RFC 6455 WebSocket client (stdlib only) for the lab's scenario
clients. Enough for the daemon's `/cli/sessions/bytes`: text and binary
messages, ping/pong, close. No TLS, no extensions."""

import base64
import hashlib
import os
import socket
import struct

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class WsError(Exception):
    pass


class WebSocket:
    def __init__(self, host, port, path, timeout):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        req = ("GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
               "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n") % (path, host, port, key)
        self.sock.sendall(req.encode())
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise WsError("connection closed during handshake: %r" % head[:300])
            head += chunk
        head, self._buf = head.split(b"\r\n\r\n", 1)
        lines = head.decode("latin-1").split("\r\n")
        if " 101 " not in lines[0] + " ":
            raise WsError("handshake refused: %s %r" % (lines[0], self._buf[:300]))
        want = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
        accept = [l.split(":", 1)[1].strip() for l in lines[1:] if l.lower().startswith("sec-websocket-accept:")]
        if accept != [want]:
            raise WsError("bad Sec-WebSocket-Accept: %r" % accept)

    def settimeout(self, t):
        self.sock.settimeout(t)

    def _read(self, n):
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise WsError("connection closed")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _send(self, opcode, payload=b""):
        mask = os.urandom(4)
        n = len(payload)
        if n < 126:
            hdr = struct.pack("!BB", 0x80 | opcode, 0x80 | n)
        elif n < 65536:
            hdr = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, n)
        else:
            hdr = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, n)
        body = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(hdr + mask + body)

    def send_text(self, text):
        self._send(0x1, text.encode("utf-8"))

    def recv(self):
        """Next data message as ("text", str) or ("binary", bytes), or
        ("close", None). Answers pings on the way."""
        parts, kind = [], None
        while True:
            b0, b1 = self._read(2)
            fin, opcode, n = b0 & 0x80, b0 & 0x0F, b1 & 0x7F
            if n == 126:
                n = struct.unpack("!H", self._read(2))[0]
            elif n == 127:
                n = struct.unpack("!Q", self._read(8))[0]
            mask = self._read(4) if b1 & 0x80 else None
            data = self._read(n)
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            if opcode == 0x9:
                self._send(0xA, data)
                continue
            if opcode == 0xA:
                continue
            if opcode == 0x8:
                return "close", None
            if opcode in (0x1, 0x2):
                kind = "text" if opcode == 0x1 else "binary"
            parts.append(data)
            if fin:
                payload = b"".join(parts)
                return kind, payload.decode("utf-8") if kind == "text" else payload

    def close(self):
        try:
            self._send(0x8, struct.pack("!H", 1000))
        except OSError:
            pass
        self.sock.close()
