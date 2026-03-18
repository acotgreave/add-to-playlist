"""Serve the static docs/ folder for local preview."""
import os
import http.server
import socketserver

os.chdir(os.path.join(os.path.dirname(__file__), 'docs'))
PORT = 5050
with socketserver.TCPServer(("", PORT), http.server.SimpleHTTPRequestHandler) as httpd:
    print(f"Serving docs/ at http://localhost:{PORT}")
    httpd.serve_forever()
