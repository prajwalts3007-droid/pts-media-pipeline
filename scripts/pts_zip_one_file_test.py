"""Disposable full-member range download to HF bucket benchmark.

Source credentials exist only in Actions secrets; never log signed URLs.
"""
import hashlib
import os
import sys
import tempfile
import time
import zipfile

import requests
from huggingface_hub import HfApi

from zip_bucket import BUCKET_ID, CHUNK, RangedZip, inspect_zip


def main():
    source = None
    api = HfApi(token=os.environ["PTS_PROBE_HF_TOKEN"])
    prefix = "transfers/zip-one-file-test-" + os.environ["GITHUB_RUN_ID"] + "/"
    path = prefix + "first-entry.sample"
    uploaded = False
    try:
        source = RangedZip(os.environ["PTS_PROBE_GCS_URL"])
        with zipfile.ZipFile(source) as archive:
            entries, _ = inspect_zip(archive)
            first = entries[0]
            if first.file_size > 8 * (1 << 30):
                raise ValueError("first file exceeds runner scratch limit")
            source.cache = b""
            source.cache_start = -1
            source.streaming = True
            with tempfile.TemporaryDirectory(prefix="pts-one-file-") as scratch:
                local = scratch + "/entry"
                start = time.monotonic()
                checksum = hashlib.sha256()
                copied = 0
                with archive.open(first) as entry, open(local, "wb") as target:
                    while True:
                        chunk = entry.read(CHUNK)
                        if not chunk:
                            break
                        copied += len(chunk)
                        target.write(chunk)
                        checksum.update(chunk)
                        if copied // (512 << 20) != (copied - len(chunk)) // (512 << 20):
                            print("Extracted MiB:", copied >> 20, flush=True)
                if copied != first.file_size:
                    raise ValueError("extracted size differs from ZIP index")
                seconds = time.monotonic() - start
                print("Extracted MiB:", copied >> 20, "seconds:", round(seconds, 1),
                      "MiB/s:", round(copied / (1 << 20) / seconds, 2), flush=True)
                source._close_stream()
                start = time.monotonic()
                api.batch_bucket_files(BUCKET_ID, add=[(local, path)])
                uploaded = True
                seconds = time.monotonic() - start
                print("Bucket upload MiB:", copied >> 20, "seconds:", round(seconds, 1),
                      "MiB/s:", round(copied / (1 << 20) / seconds, 2), flush=True)
                # Public range-read verifies the same bytes were published.
                with requests.get("https://huggingface.co/buckets/" + BUCKET_ID + "/resolve/" + path,
                                  headers={"Range": "bytes=0-1023"}, stream=True,
                                  timeout=(15, 60)) as response:
                    if response.status_code != 206:
                        raise ValueError("published object does not serve ranges")
                    with open(local, "rb") as check:
                        if response.raw.read(1024) != check.read(1024):
                            raise ValueError("published content does not match source")
                print("Public byte-range integrity: passed", flush=True)
    finally:
        if source is not None:
            source.close()
        # Also try deleting when upload raised after writing the object.
        try:
            api.batch_bucket_files(BUCKET_ID, delete=[path])
            print("Temporary bucket object deletion: requested", flush=True)
        except Exception:
            print("Temporary bucket object deletion: operator cleanup required", flush=True)
            if uploaded:
                raise


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Network exceptions can include a signed URL. Only emit the type.
        print("One-file probe failed:", type(exc).__name__, flush=True)
        sys.exit(1)
