import argparse
import json
import socket
import sys


def read_json_line(sock_file):
    line = sock_file.readline()
    if not line:
        raise RuntimeError("server closed connection before response")
    return json.loads(line.decode("utf-8"))


def expect_ok(response, expected_id):
    if response.get("id") != expected_id:
        raise RuntimeError(f"unexpected id: {response!r}")
    if not response.get("ok"):
        raise RuntimeError(f"request failed: {response!r}")


def send_request(sock_file, payload):
    encoded = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
    sock_file.write(encoded)
    sock_file.flush()
    response = read_json_line(sock_file)
    expect_ok(response, payload["id"])
    return response


def main():
    parser = argparse.ArgumentParser(description="Smoke-test SQLprocessor TCP server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=15432)
    args = parser.parse_args()

    with socket.create_connection((args.host, args.port), timeout=10) as sock:
        sock.settimeout(10)
        with sock.makefile("rwb", buffering=0) as sock_file:
            send_request(sock_file, {"id": "ping-1", "op": "ping"})
            send_request(
                sock_file,
                {
                    "id": "delete-1",
                    "op": "sql",
                    "sql": "DELETE FROM case_basic_users WHERE id = 900001;",
                },
            )
            send_request(
                sock_file,
                {
                    "id": "insert-1",
                    "op": "sql",
                    "sql": "INSERT INTO case_basic_users VALUES (900001, 'tcp-user@test.com', '010-9999', 'pw9999', 'TcpUser');",
                },
            )
            response = send_request(
                sock_file,
                {
                    "id": "select-1",
                    "op": "sql",
                    "sql": "SELECT * FROM case_basic_users WHERE id = 900001;",
                },
            )
            if response.get("row_count") != 1:
                raise RuntimeError(f"expected row_count=1: {response!r}")
            print("tcp smoke ok")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"tcp smoke failed: {exc}", file=sys.stderr)
        sys.exit(1)
