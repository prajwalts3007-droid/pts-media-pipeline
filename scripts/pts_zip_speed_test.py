"""Bounded GitHub Actions source-to-bucket probe; never print signed URLs."""
import ipaddress
import os
import re
import socket
import sys
import time
from urllib.parse import urljoin, urlsplit

import requests
from huggingface_hub import HfApi


LIMIT = 64 * (1 << 20)
BUCKET = "Ptsts1998/pts-streamed-files"
RUN = os.environ["GITHUB_RUN_ID"]
API = HfApi(token=os.environ["PTS_PROBE_HF_TOKEN"])


def public_https(url):
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("unsafe source host")
    for result in socket.getaddrinfo(parsed.hostname, parsed.port or 443):
        if not ipaddress.ip_address(result[4][0]).is_global:
            raise ValueError("non-public source address")


def ranged(url):
    headers = {"Range": f"bytes=0-{LIMIT - 1}", "Accept-Encoding": "identity"}
    session = requests.Session()
    for _ in range(5):
        public_https(url)
        response = session.get(url, headers=headers, stream=True, allow_redirects=False,
                               timeout=(15, 30))
        if response.status_code not in (301, 302, 303, 307, 308):
            break
        redirect = response.headers.get("Location")
        response.close()
        if not redirect:
            raise ValueError("source redirect had no target")
        url = urljoin(url, redirect)
    else:
        raise ValueError("too many source redirects")
    try:
        if response.status_code != 206:
            raise ValueError("source did not return a byte range")
        content_range = re.fullmatch(r"bytes 0-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
        if not content_range or int(content_range[1]) != LIMIT - 1:
            raise ValueError("unexpected source byte range")
        if response.headers.get("Content-Encoding", "identity") != "identity":
            raise ValueError("source encoded byte range")
        data = bytearray()
        started = time.monotonic()
        while len(data) < LIMIT:
            if time.monotonic() - started > 180:
                raise ValueError("download exceeded three-minute probe budget")
            chunk = response.raw.read(min(1 << 20, LIMIT - len(data)))
            if not chunk:
                raise ValueError("source ended before the bounded sample")
            data.extend(chunk)
        return bytes(data), time.monotonic() - started
    finally:
        response.close()
        session.close()


def probe(label):
    url = os.environ["PTS_PROBE_URL_" + label]
    prefix = f"transfers/zip-speed-probe-{RUN}/"
    path = prefix + label.lower() + ".sample"
    uploaded = False
    try:
        data, seconds = ranged(url)
        download_speed = len(data) / (1 << 20) / max(seconds, 0.001)
        print(f"{label} source: {len(data) >> 20} MiB in {seconds:.1f}s ({download_speed:.2f} MiB/s)", flush=True)
        start = time.monotonic()
        API.batch_bucket_files(BUCKET, add=[(data, path)])
        uploaded = True
        upload_seconds = time.monotonic() - start
        print(f"{label} bucket: {len(data) >> 20} MiB in {upload_seconds:.1f}s "
              f"({len(data) / (1 << 20) / max(upload_seconds, 0.001):.2f} MiB/s)", flush=True)
        # The sample stays on GitHub's runner and in the temporary bucket,
        # never on the VPS or in a GitHub artifact.
    finally:
        try:
            API.batch_bucket_files(BUCKET, delete=[path])
            print(f"{label} sample deletion: completed", flush=True)
        except Exception:
            if uploaded:
                print(f"{label} sample deletion: needs operator cleanup", flush=True)


def main():
    failures = 0
    for label in ("R2", "GCS"):
        try:
            probe(label)
        except Exception as exc:
            # requests and Hub SDK exceptions can contain signed URLs or tokens.
            # Print only our own fixed category; never stringify exceptions.
            failures += 1
            print(f"{label} probe failed ({type(exc).__name__}); credentials and URLs hidden", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
