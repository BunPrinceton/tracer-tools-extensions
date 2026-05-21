"""serve_local_precomputed_ben — serve a local precomputed folder so NG can read it.

Why: NG requires HTTP(S) + CORS to load a precomputed source. Python's built-in
http.server is HTTP-only AND sends no CORS headers, so this wraps it with the
CORS headers NG needs. Browsers treat http://localhost as a secure context, so
even hosted HTTPS NG (spelunker.cave-explorer.org, neuroglancer-demo.appspot.com)
can fetch from this server without mixed-content warnings.

Example:
  python serve_local_precomputed_ben.py "C:\\path\\to\\workdir\\image"
  # serves on http://localhost:9000

  # In NG, add a new segmentation layer with source:
  #   precomputed://http://localhost:9000
  # (or the legacy "<url>|neuroglancer-precomputed:" form printed at startup)

Stop the server with Ctrl+C in the terminal where it's running.
"""
import os
import sys
import argparse
import socketserver
from http.server import SimpleHTTPRequestHandler


def _make_handler(serve_dir):
    class CORSHandler(SimpleHTTPRequestHandler):
        # Python 3.7+: pass `directory` so the handler serves from serve_dir
        # without us having to cd into it.
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=serve_dir, **kwargs)

        def translate_path(self, path):
            # NG requests mesh fragments as `<segid>:0` and `<segid>:0:1`,
            # but Windows can't write `:` in filenames, so json_to_volume_ben
            # writes them as `<segid>___0` / `<segid>___0___1` locally and
            # bucket_upload_folder_ben translates them back on upload. For local
            # serving we have to do the same translation on the fly. Also handle
            # the URL-encoded form `%3A` just in case some clients percent-encode.
            translated_path = path.replace("%3A", "___").replace(":", "___")
            return super().translate_path(translated_path)

        def end_headers(self):
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "*")
            self.send_header("Cache-Control", "no-store")
            super().end_headers()

        def do_OPTIONS(self):
            self.send_response(204)
            self.end_headers()

        def log_message(self, fmt, *args):
            # Quiet the per-request stderr noise; print one line on misses only.
            if "404" in (fmt % args):
                sys.stderr.write(f"  miss: {self.path}\n")

    return CORSHandler


def _cli():
    p = argparse.ArgumentParser(description="serve a precomputed folder locally for NG")
    p.add_argument("path", help="path to the folder to serve (the one containing 'info', e.g. workdir/image)")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--bind", default="127.0.0.1", help="bind address; default 127.0.0.1 (localhost only)")
    args = p.parse_args()

    serve_dir = os.path.abspath(args.path)
    if not os.path.isdir(serve_dir):
        print(f"ERROR: not a directory: {serve_dir}")
        sys.exit(1)
    info_path = os.path.join(serve_dir, "info")
    if not os.path.isfile(info_path):
        print(f"WARN: no `info` file at {info_path}. NG won't recognize this as precomputed.")
        print(f"      (Did you mean to point at the `image/` subdirectory?)")

    base = f"http://{args.bind if args.bind != '0.0.0.0' else 'localhost'}:{args.port}"
    print(f"serving {serve_dir} at {base}")
    print()
    print(f"NG source URL (precomputed:// form):")
    print(f"  precomputed://{base}")
    print()
    print(f"NG source URL (legacy bar form, what state_to_ng_layer_ben.py prints):")
    print(f"  {base}|neuroglancer-precomputed:")
    print()
    print(f"To use in NG:")
    print(f"  1. Open your usual NG instance (spelunker, ng-app, etc.)")
    print(f"  2. Add a new layer, set source to the URL above")
    print(f"  3. Add segid 1 (or whatever segid you wrote) to its visible segments")
    print()
    print(f"Press Ctrl+C to stop.")

    handler = _make_handler(serve_dir)
    with socketserver.ThreadingTCPServer((args.bind, args.port), handler) as httpd:
        httpd.allow_reuse_address = True
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nshutting down.")


if __name__ == "__main__":
    _cli()
