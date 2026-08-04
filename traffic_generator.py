#!/usr/bin/env python3
import argparse
import socket
import sys
import time

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8080


def run_normal_scenario(num_clients=3, duration=10):
    print(
        f"[Traffic Generator] Running NORMAL Echo scenario ({num_clients} clients, {duration}s)..."
    )
    sockets = []
    for i in range(num_clients):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_HOST, SERVER_PORT))
        sockets.append(s)
        print(f" -> Connected client {i+1} (fd {s.fileno()})")
        time.sleep(0.2)

    start_time = time.time()
    payload = b"Hello from epoll traffic generator! " * 4

    while time.time() - start_time < duration:
        for idx, s in enumerate(sockets):
            s.sendall(payload)
            data = s.recv(4096)
            print(f" -> Client {idx+1} (fd {s.fileno()}) sent & received {len(data)} bytes")
            time.sleep(0.5)

    for s in sockets:
        s.close()
    print("[Traffic Generator] Normal scenario complete!")


def run_backpressure_scenario(num_clients=2):
    print("\n" + "=" * 60)
    print("[Traffic Generator] 💥 Running BACKPRESSURE Scenario")
    print(
        "Goal: Fill kernel socket buffer so server write() receives EAGAIN"
    )
    print("      and arms EPOLLOUT in epoll_ctl!")
    print("=" * 60 + "\n")

    sockets = []
    for i in range(num_clients):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Shrink receive buffer on client side to freeze TCP window quickly
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        s.connect((SERVER_HOST, SERVER_PORT))
        sockets.append(s)
        print(f" -> Connected client {i+1} (fd {s.fileno()}) with choked RCVBUF")

    # Send a massive flood without reading responses on client side
    large_payload = b"B" * 65536
    print("\n[+] Flooding server with 64 KB per client WITHOUT reading replies...")

    for idx, s in enumerate(sockets):
        try:
            s.sendall(large_payload)
            print(f" -> Sent 64 KB payload to socket {idx+1}")
        except Exception as e:
            print(f" -> Send error: {e}")

    print("\n[!] Check your browser visualizer now!")
    print("    You should see RED 'EPOLLOUT ARMED' badges and buffer gauges filled!")
    print("    Holding backpressure state for 8 seconds...\n")

    for remaining in range(8, 0, -1):
        print(f" -> Draining buffer in {remaining}s...", end="\r")
        time.sleep(1)

    print("\n\n[+] Now reading replies on clients to unchoke the TCP window...")
    for idx, s in enumerate(sockets):
        s.settimeout(1.0)
        total_recvd = 0
        while True:
            try:
                data = s.recv(8192)
                if not data:
                    break
                total_recvd += len(data)
            except socket.timeout:
                break
        print(f" -> Client {idx+1} read {total_recvd} bytes. Server write buffer drained!")

    for s in sockets:
        s.close()
    print("[Traffic Generator] Backpressure scenario complete!")


def run_churn_scenario(count=20):
    print(f"[Traffic Generator] Running RAPID CHURN scenario ({count} connections)...")
    for i in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_HOST, SERVER_PORT))
        s.sendall(b"PING")
        s.close()
        time.sleep(0.05)
    print("[Traffic Generator] Rapid churn scenario complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Epoll Server Traffic Generator")
    parser.add_argument(
        "--scenario",
        choices=["normal", "backpressure", "churn"],
        default="normal",
        help="Scenario to run",
    )
    parser.add_argument("--clients", type=int, default=2, help="Number of clients")
    args = parser.parse_args()

    if args.scenario == "normal":
        run_normal_scenario(num_clients=args.clients)
    elif args.scenario == "backpressure":
        run_backpressure_scenario(num_clients=args.clients)
    elif args.scenario == "churn":
        run_churn_scenario(count=20)
