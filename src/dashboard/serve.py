#!/usr/bin/env python3
import os, http.server, socketserver
os.chdir(os.path.dirname(os.path.abspath(__file__)))
PORT = 8080
with socketserver.TCPServer(("", PORT), http.server.SimpleHTTPRequestHandler) as httpd:
    print(f"Serving at http://localhost:{PORT}")
    httpd.serve_forever()
