"""Measure scoped dashboard manifests and chunks using read-only HTTP requests."""

import argparse
import gzip
import json
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8765", help="Running monitor to read; defaults to localhost.")
    args = parser.parse_args()

    def read(path, etag=None):
        started = time.perf_counter()
        request = urllib.request.Request(args.url.rstrip("/") + path, headers={"Accept-Encoding": "gzip", **({"If-None-Match": etag} if etag else {})})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                data = response.read()
                return json.loads(gzip.decompress(data) if response.headers.get("Content-Encoding") == "gzip" else data), response.headers.get("ETag"), len(data), (time.perf_counter() - started) * 1000
        except urllib.error.HTTPError as exc:
            with exc:
                if exc.code != 304:
                    raise
            return None, etag, 0, (time.perf_counter() - started) * 1000

    for view in ("local", "merged"):
        for selected in ("5h", "All"):
            path = "/api/dashboard?" + urllib.parse.urlencode({"view": view, "range": selected})
            manifest, etag, manifest_bytes, manifest_ms = read(path)
            started = time.perf_counter()
            chunks = [read("/api/dashboard/chunk?" + urllib.parse.urlencode({"id": digest})) for digest in manifest["chunks"]]
            chunk_ms = (time.perf_counter() - started) * 1000
            cached, _, _, cached_ms = read(path, etag)
            print(json.dumps({"view": view, "range": selected, "manifestMs": round(manifest_ms, 3), "chunkMs": round(chunk_ms, 3),
                "chunks": len(chunks), "manifestBytes": manifest_bytes, "chunkBytes": sum(part[2] for part in chunks),
                "conditionalManifestMs": round(cached_ms, 3), "notModified": cached is None,
                "exactSessionCount": sum(row["sessionCount"] for row in manifest["tokenSessions"])}), flush=True)


if __name__ == "__main__":
    main()
