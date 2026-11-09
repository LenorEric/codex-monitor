"""Read-only WebSocket subscriptions on the dashboard's existing HTTP listener."""

import json
import shutil
import select
import time

from wsproto import ConnectionType, WSConnection
from wsproto.events import AcceptConnection, CloseConnection, Ping, Pong, TextMessage
from wsproto.utilities import LocalProtocolError, RemoteProtocolError

from monitor_delivery import encoded


def send_bulk(handler, delivery):
    compressed = "gzip" in (handler.headers.get("Accept-Encoding") or "").lower()
    if hasattr(delivery, "snapshot_file"):
        with delivery.snapshot_file(compressed) as (file, size):
            handler.send_response(200)
            handler.send_header("Content-Type", "application/json; charset=utf-8")
            handler.send_header("Cache-Control", "no-store")
            handler.send_header("Vary", "Accept-Encoding")
            if compressed:
                handler.send_header("Content-Encoding", "gzip")
            handler.send_header("Content-Length", str(size))
            handler.end_headers()
            shutil.copyfileobj(file, handler.wfile, length=64 * 1024)
        return True
    body = delivery.bulk(compressed)
    if body is None:
        return False
    handler.send_response(200)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Vary", "Accept-Encoding")
    if compressed:
        handler.send_header("Content-Encoding", "gzip")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)
    return True


def serve_live(handler, delivery, running=lambda: True):
    protocol = WSConnection(ConnectionType.SERVER)
    protocol.initiate_upgrade_connection([(key.encode(), value.encode()) for key, value in handler.headers.items()], handler.path)
    list(protocol.events())
    handler.connection.sendall(protocol.send(AcceptConnection()))
    handler.close_connection = True
    handler.connection.settimeout(5)
    epoch, cursors, fragments = None, {}, ""
    last_ping = last_pong = time.monotonic()

    def send(value):
        body = encoded(value).decode()
        if len(body.encode()) > 4 * 1024 * 1024:
            raise ValueError("Live backlog exceeded; reconnect for a bulk snapshot")
        handler.connection.sendall(protocol.send(TextMessage(data=body)))

    try:
        while running():
            now = time.monotonic()
            if now - last_pong > 60:
                break
            if now - last_ping >= 20:
                handler.connection.sendall(protocol.send(Ping(payload=b"monitor")))
                last_ping = now
            if select.select([handler.connection], [], [], .2)[0]:
                data = handler.connection.recv(65536)
                if not data:
                    break
                protocol.receive_data(data)
                for event in protocol.events():
                    if isinstance(event, CloseConnection):
                        handler.connection.sendall(protocol.send(event.response()))
                        return
                    if isinstance(event, Ping):
                        handler.connection.sendall(protocol.send(event.response()))
                    if isinstance(event, Pong):
                        last_pong = now
                    if isinstance(event, TextMessage):
                        fragments += event.data
                        if len(fragments) > 65536:
                            raise ValueError("Subscription too large")
                        if event.message_finished:
                            request, fragments = json.loads(fragments), ""
                            if request.get("type") != "subscribe" or not isinstance(request.get("cursors"), dict):
                                raise ValueError("Expected a subscription")
                            epoch, cursors = request.get("epoch"), request["cursors"]
                            if epoch is None and set(cursors) == {"state"}:
                                # Status-bar-only subscribers never download history.
                                epoch = delivery.head[0]
                                cursors = {"state": delivery.head[1]["state"]}
                                send({"type": "state", "epoch": epoch, "cursors": cursors, "data": delivery.state or {}})
            if epoch is not None:
                packets = delivery.replay(epoch, cursors)
                if packets is None:
                    send({"type": "refresh"})
                    epoch = None
                else:
                    for packet in packets:
                        send(packet)
                        cursors[packet["stream"]] = packet["seq"]
    except (OSError, ValueError, LocalProtocolError, RemoteProtocolError):
        pass
    finally:
        try:
            handler.connection.sendall(protocol.send(CloseConnection(code=1001, reason="Reconnect to resume")))
        except Exception:
            pass
