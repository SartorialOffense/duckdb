#!/usr/bin/env python3
"""
HTTP server with Range request support and configurable per-request latency.
Used to demonstrate the performance impact of persistent parquet metadata caching.

Usage:
    python3 slow_http_server.py [port] [latency_ms]

    port:       TCP port to listen on (default: 9798)
    latency_ms: milliseconds to sleep before each response (default: 75)
"""

import http.server
import os
import sys
import time


class RangeHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    latency_seconds = 0.075

    def do_GET(self):
        time.sleep(self.latency_seconds)

        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            self.send_error(404)
            return

        file_size = os.path.getsize(path)
        range_header = self.headers.get('Range')

        if range_header and range_header.startswith('bytes='):
            # Parse range
            range_spec = range_header[6:]
            start_str, end_str = range_spec.split('-', 1)
            start = int(start_str) if start_str else 0
            end = int(end_str) if end_str else file_size - 1
            end = min(end, file_size - 1)
            length = end - start + 1

            self.send_response(206)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Length', str(length))
            self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()

            with open(path, 'rb') as f:
                f.seek(start)
                self.wfile.write(f.read(length))
        else:
            # Full file
            self.send_response(200)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Length', str(file_size))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()

            with open(path, 'rb') as f:
                self.wfile.write(f.read())

    def do_HEAD(self):
        time.sleep(self.latency_seconds)

        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            self.send_error(404)
            return

        file_size = os.path.getsize(path)
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Length', str(file_size))
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()

    def log_message(self, format, *args):
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9798
    latency_ms = int(sys.argv[2]) if len(sys.argv) > 2 else 75

    RangeHTTPRequestHandler.latency_seconds = latency_ms / 1000.0

    repo_root = os.path.join(os.path.dirname(__file__), '..', '..', '..')
    os.chdir(repo_root)

    server = http.server.HTTPServer(('127.0.0.1', port), RangeHTTPRequestHandler)
    print(f"Range HTTP server on port {port}, latency={latency_ms}ms", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == '__main__':
    main()
