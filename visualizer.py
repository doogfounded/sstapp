import http.server
import json
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

PORT = 8000
SERVER_TCP_PORT = 8080
C_SOURCE = "epoll_visual_server.c"
C_BINARY = "./epoll_visual_server"

# Client queues for SSE broadcasting
sse_clients = []
sse_lock = threading.Lock()

# State tracking for API actions
managed_clients = {}
managed_clients_lock = threading.Lock()

c_process = None


def compile_and_start_c_server():
    global c_process
    print(f"[Visualizer] Compiling {C_SOURCE}...")
    res = subprocess.run(["gcc", "-Wall", "-Wextra", C_SOURCE, "-o", C_BINARY])
    if res.returncode != 0:
        print("[Visualizer] Compilation failed!")
        sys.exit(1)

    print(f"[Visualizer] Starting C server executable {C_BINARY}...")
    c_process = subprocess.Popen(
        [C_BINARY],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    def read_stdout():
        while True:
            line = c_process.stdout.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            if line.startswith("{") and line.endswith("}"):
                broadcast_event(line)
            else:
                print(f"[C Output] {line}")

    def read_stderr():
        for line in c_process.stderr:
            print(f"[C Error] {line.strip()}", file=sys.stderr)

    threading.Thread(target=read_stdout, daemon=True).start()
    threading.Thread(target=read_stderr, daemon=True).start()


def broadcast_event(json_str):
    with sse_lock:
        to_remove = []
        for q in sse_clients:
            try:
                q.put_nowait(json_str)
            except queue.Full:
                to_remove.append(q)
        for q in to_remove:
            sse_clients.remove(q)


class TelemetryHTTPHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Silence routine HTTP access logs to keep stdout clean
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/events":
            self.handle_sse()
        elif path == "/api/spawn_client":
            self.handle_spawn_client()
        elif path == "/api/send_data":
            self.handle_send_data(parsed.query)
        elif path == "/api/choke_client":
            self.handle_choke_client(parsed.query)
        elif path == "/api/close_clients":
            self.handle_close_clients()
        elif path == "/" or path == "/index.html":
            self.path = "/index.html"
            return super().do_GET()
        else:
            return super().do_GET()

    def handle_sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        q = queue.Queue(maxsize=100)
        with sse_lock:
            sse_clients.append(q)

        # Send initial connection event
        try:
            self.wfile.write(b"data: {\"type\":\"connected\"}\n\n")
            self.wfile.flush()
        except Exception:
            return

        try:
            while True:
                msg = q.get(timeout=30)
                sse_data = f"data: {msg}\n\n".encode("utf-8")
                self.wfile.write(sse_data)
                self.wfile.flush()
        except (queue.Empty, BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with sse_lock:
                if q in sse_clients:
                    sse_clients.remove(q)

    def handle_spawn_client(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", SERVER_TCP_PORT))
            client_id = f"c_{sock.fileno()}_{time.time()}"
            with managed_clients_lock:
                managed_clients[client_id] = {
                    "sock": sock,
                    "choked": False,
                    "reader_thread": None,
                }

            # Thread to handle receiving echo responses so socket doesn't choke unless requested
            def run_reader():
                while True:
                    with managed_clients_lock:
                        info = managed_clients.get(client_id)
                        if not info or info["choked"]:
                            time.sleep(0.1)
                            continue
                    try:
                        sock.settimeout(0.5)
                        data = sock.recv(4096)
                        if not data:
                            break
                    except socket.timeout:
                        continue
                    except Exception:
                        break

            t = threading.Thread(target=run_reader, daemon=True)
            t.start()
            managed_clients[client_id]["reader_thread"] = t

            self._json_response(
                {"status": "ok", "client_id": client_id, "fd": sock.fileno()}
            )
        except Exception as e:
            self._json_response({"status": "error", "message": str(e)}, status=500)

    def handle_send_data(self, query_str):
        qs = urllib.parse.parse_qs(query_str)
        size = int(qs.get("size", [512])[0])
        cid = qs.get("client_id", [None])[0]

        payload = b"X" * size
        sent_count = 0

        with managed_clients_lock:
            clients_to_send = (
                [managed_clients[cid]]
                if cid in managed_clients
                else list(managed_clients.values())
            )

        for cinfo in clients_to_send:
            try:
                cinfo["sock"].sendall(payload)
                sent_count += 1
            except Exception:
                pass

        self._json_response({"status": "ok", "bytes_sent": size, "targets": sent_count})

    def handle_choke_client(self, query_str):
        qs = urllib.parse.parse_qs(query_str)
        cid = qs.get("client_id", [None])[0]
        choke_state = qs.get("choke", ["true"])[0] == "true"

        with managed_clients_lock:
            clients = (
                [managed_clients[cid]]
                if cid in managed_clients
                else list(managed_clients.values())
            )
            for cinfo in clients:
                cinfo["choked"] = choke_state
                if choke_state:
                    # Set kernel SO_RCVBUF very small to maximize backpressure
                    try:
                        cinfo["sock"].setsockopt(
                            socket.SOL_SOCKET, socket.SO_RCVBUF, 1024
                        )
                    except Exception:
                        pass

        self._json_response({"status": "ok", "choked": choke_state})

    def handle_close_clients(self):
        with managed_clients_lock:
            for cinfo in managed_clients.values():
                try:
                    cinfo["sock"].close()
                except Exception:
                    pass
            managed_clients.clear()
        self._json_response({"status": "ok"})

    def _json_response(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def run():
    compile_and_start_c_server()
    time.sleep(0.5)

    server_address = ("", PORT)
    httpd = http.server.HTTPServer(server_address, TelemetryHTTPHandler)
    print(f"\n=======================================================")
    print(f"🚀 Epoll Real-Time Telemetry Dashboard Server Running!")
    print(f"👉 Open in Browser: http://localhost:{PORT}")
    print(f"=======================================================\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down visualizer...")
        if c_process:
            c_process.terminate()


if __name__ == "__main__":
    run()
