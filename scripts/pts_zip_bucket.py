"""GitHub Actions ZIP-to-bucket worker for PTS relay jobs.

Downloads a remote ZIP via HTTP byte ranges, extracts one member at a time,
uploads each member directly to the HF Storage Bucket, and reports progress
to the VPS. Media bytes never pass through the VPS.

Environment (set by the workflow from dispatch inputs and secrets):
  JOB_ID          PTS relay job id (rl + 16 hex chars)
  ATTEMPT         Attempt number
  CALLBACK        VPS worker API base URL (ends with /)
  WORKER_TOKEN    Short-lived bearer token for the VPS callback

The source URL and HF bucket token are fetched from the VPS callback using
the worker token, exactly like the Space worker does.
"""
import io
import ipaddress
import os
import re
import socket
import sys
import tempfile
import time
import zipfile
from urllib.parse import quote, urljoin, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ZIP_LIMIT = 100 * (1 << 30)
EXPANDED_LIMIT = 100 * (1 << 30)
ENTRY_LIMIT = 8 * (1 << 30)
FILE_LIMIT = 500
INDEX_LIMIT = 8 * (1 << 20)
CHUNK = 4 * (1 << 20)
BUCKET_ID = "Ptsts1998/pts-streamed-files"
MAX_SECONDS = 5 * 60 * 60 + 50 * 60  # 5h50m: under GitHub's 6-hour limit


class SourceError(ValueError):
    pass


def public_host(url):
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
        raise SourceError("The ZIP must have a public HTTPS link")
    for answer in socket.getaddrinfo(parts.hostname, parts.port or 443):
        if not ipaddress.ip_address(answer[4][0]).is_global:
            raise SourceError("The ZIP source must resolve to a public address")


class RangedZip(io.RawIOBase):
    """Seekable HTTP Range reader that never accepts a whole-object fallback."""

    def __init__(self, url, session=None, limit=ZIP_LIMIT):
        self.url = url
        self.session = session or requests.Session()
        if session is None:
            self.session.mount("https://", HTTPAdapter(max_retries=Retry(
                total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=("GET",), respect_retry_after_header=True)))
        self.limit = limit
        self.pos = 0
        self.cache_start = -1
        self.cache = b""
        self.size = 0
        self.streaming = False
        self._stream = None
        self.headers = {}
        self._request(0, 0, find_size=True)

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, offset, whence=os.SEEK_SET):
        origin = (0, self.pos, self.size)[whence]
        target = origin + offset
        if target < 0 or target > self.size:
            raise SourceError("ZIP reader sought outside the source")
        if target != self.pos:
            self._close_stream()
        self.pos = target
        return target

    def _close_stream(self):
        if getattr(self, "_stream", None) is not None:
            self._stream.close()
            self._stream = None

    def close(self):
        self._close_stream()
        if hasattr(self, "session"):
            self.session.close()
        super().close()

    def _open_range(self, start, end, find_size=False):
        url = self.url
        for _ in range(6):
            public_host(url)
            response = self.session.get(url, headers={"Range": f"bytes={start}-{end}", "Accept-Encoding": "identity"},
                                        stream=True, allow_redirects=False, timeout=(30, 30))
            if response.status_code not in (301, 302, 303, 307, 308):
                break
            location = response.headers.get("Location")
            response.close()
            if not location:
                raise SourceError("The source redirected without a location")
            url = urljoin(url, location)
        else:
            raise SourceError("The source redirected too many times")
        try:
            if response.status_code >= 400:
                raise SourceError(f"The ZIP source returned HTTP {response.status_code}; the signed link may have expired")
            if response.status_code != 206:
                raise SourceError("The ZIP source must support HTTP byte ranges (206)")
            if find_size:
                self.headers = response.headers
                self.url = url
            match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
            if not match or (int(match[1]), int(match[2])) != (start, end):
                raise SourceError("The ZIP source returned an invalid byte range")
            total = int(match[3])
            if total < 1 or total > self.limit or end >= total or (self.size and total != self.size):
                raise SourceError("The ZIP source changed size or exceeded the safety cap")
            if response.headers.get("Content-Encoding", "identity") != "identity":
                raise SourceError("The ZIP source must not encode HTTP range responses")
            length = response.headers.get("Content-Length")
            if length and int(length) != end - start + 1:
                raise SourceError("The ZIP source returned the wrong range length")
            self.size = total
            return response
        except Exception:
            response.close()
            raise

    def _request(self, start, end, find_size=False):
        response = self._open_range(start, end, find_size=find_size)
        try:
            result = response.raw.read(end - start + 1)
            if len(result) != end - start + 1:
                raise SourceError("The ZIP source returned an incomplete byte range")
            return result
        finally:
            response.close()

    def _read_stream(self, n):
        out = bytearray()
        failures = 0
        while len(out) < n:
            if self._stream is None:
                self._stream = self._open_range(self.pos, self.size - 1)
            try:
                data = self._stream.raw.read(n - len(out))
            except (OSError, requests.RequestException):
                data = b""
            if not data:
                self._close_stream()
                failures += 1
                if failures >= 3:
                    raise SourceError("The ZIP source interrupted the streaming connection")
                continue
            failures = 0
            out.extend(data)
            self.pos += len(data)
        return bytes(out)

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size - self.pos
        if n > INDEX_LIMIT:
            raise SourceError("ZIP reader requested more than 8 MiB at offset %d" % self.pos)
        n = min(n, self.size - self.pos)
        if n <= 0:
            return b""
        if self.streaming:
            return self._read_stream(n)
        out = bytearray()
        while len(out) < n:
            in_cache = self.cache_start <= self.pos < self.cache_start + len(self.cache)
            if not in_cache:
                start = self.pos
                end = min(self.size - 1, start + max(CHUNK, n - len(out)) - 1)
                self.cache = self._request(start, end)
                self.cache_start = start
            available = min(n - len(out), self.cache_start + len(self.cache) - self.pos)
            out.extend(self.cache[self.pos - self.cache_start:self.pos - self.cache_start + available])
            self.pos += available
        return bytes(out)


def safe_path(info):
    name = info.filename
    parts = name[:-1] if info.is_dir() else name
    if (not parts or "\\" in name or "\x00" in name or name.startswith("/") or
            any(part in ("", ".", "..") for part in parts.split("/")) or
            any(ord(c) < 32 or ord(c) == 127 for c in name)):
        raise SourceError("The ZIP contains an unsafe filename")
    if (info.external_attr >> 16) & 0o170000 == 0o120000:
        raise SourceError("The ZIP contains a symbolic link")
    if info.flag_bits & 1:
        raise SourceError("Encrypted ZIP entries are not supported")
    if info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
        raise SourceError("The ZIP uses an unsupported compression method")
    if info.file_size > ENTRY_LIMIT:
        raise SourceError("An extracted file exceeds the 8 GiB limit")
    if len(name.encode("utf-8")) > 1000:
        raise SourceError("A ZIP filename is too long")
    return name


def inspect_zip(archive):
    entries = archive.infolist()
    if not entries or len(entries) > FILE_LIMIT:
        raise SourceError("The ZIP contains too many files")
    if sum(len(info.filename.encode("utf-8")) + 46 for info in entries) > INDEX_LIMIT:
        raise SourceError("The ZIP index exceeds the metadata limit")
    paths, total, files = set(), 0, []
    for info in entries:
        path = safe_path(info)
        if path in paths:
            raise SourceError("The ZIP has duplicate filenames")
        paths.add(path)
        if info.is_dir():
            continue
        total += info.file_size
        if total > EXPANDED_LIMIT:
            raise SourceError("The extracted ZIP exceeds the 100 GiB safety cap")
        files.append(info)
    if not files:
        raise SourceError("The ZIP contains no files")
    if "links.txt" in paths:
        raise SourceError("The ZIP contains a reserved links.txt filename")
    return files, total


def bucket_url(path):
    return "https://huggingface.co/buckets/" + BUCKET_ID + "/resolve/" + quote(path, safe="/")


class ProgressReporter:
    """Reports progress to the VPS callback, checking for cancellation."""

    def __init__(self, callback, job_id, token):
        self.api = callback + job_id
        self.auth = {"Authorization": "Bearer " + token}
        self.cancelled = False
        self._last_report = 0.0

    def report(self, done, total, message, stage="downloading"):
        now = time.monotonic()
        if now - self._last_report < 2 and done < total:
            return
        self._last_report = now
        try:
            resp = requests.post(self.api + "/progress", json={
                "stage": stage,
                "filename": "",
                "progress": done * 100 / total if total else 0,
                "bytes_done": done,
                "size_bytes": total,
                "speed_bps": 0,
                "eta_seconds": 0,
                "message": message,
            }, headers=self.auth, timeout=(10, 20))
            if resp.ok and resp.json().get("cancel"):
                self.cancelled = True
        except requests.RequestException:
            pass  # VPS watchdog handles missed callbacks

    def check_cancel(self):
        if self.cancelled:
            raise SourceError("Cancelled")


def publish_zip(url, job_id, attempt, hf_token, reporter, limit=ZIP_LIMIT):
    """Extract ZIP members one at a time and upload to the HF bucket."""
    from huggingface_hub import HfApi
    api = HfApi(token=hf_token)
    prefix = f"transfers/{job_id}/{attempt}/"
    uploaded = []
    source = None
    try:
        source = RangedZip(url, limit=limit)
        with zipfile.ZipFile(source) as archive:
            if source.size - archive.start_dir > INDEX_LIMIT + 65557:
                raise SourceError("The ZIP index exceeds the metadata limit")
            entries, total = inspect_zip(archive)
            source.cache = b""
            source.cache_start = -1
            source.streaming = True
            done = 0
            links = []
            for index, info in enumerate(entries, 1):
                reporter.check_cancel()
                path = prefix + safe_path(info)
                with tempfile.TemporaryDirectory(prefix="pts-zip-") as scratch:
                    local = os.path.join(scratch, "entry")
                    with archive.open(info) as entry, open(local, "wb") as target:
                        copied = 0
                        while True:
                            reporter.check_cancel()
                            data = entry.read(CHUNK)
                            if not data:
                                break
                            copied += len(data)
                            if copied > info.file_size:
                                raise SourceError("The ZIP entry exceeded its declared size")
                            target.write(data)
                            reporter.report(done + copied, total,
                                            f"Extracting file {index}/{len(entries)}")
                        if copied != info.file_size:
                            raise SourceError("The ZIP entry ended before its declared size")
                    reporter.check_cancel()
                    if hasattr(source, "_close_stream"):
                        source._close_stream()
                    reporter.report(0, 0, f"Uploading file {index}/{len(entries)} ({copied} bytes)",
                                    stage="transferring")
                    api.batch_bucket_files(BUCKET_ID, add=[(local, path)])
                    uploaded.append(path)
                done += info.file_size
                links.append(bucket_url(path))
                reporter.report(done, total, f"Uploaded file {index}/{len(entries)}")
            manifest = prefix + "links.txt"
            reporter.check_cancel()
            api.batch_bucket_files(BUCKET_ID, add=[(("\n".join(links) + "\n").encode("utf-8"), manifest)])
            uploaded.append(manifest)
            reporter.check_cancel()
            return bucket_url(manifest), len(links), prefix
    except Exception:
        try:
            if uploaded:
                api.batch_bucket_files(BUCKET_ID, delete=uploaded)
        except Exception:
            pass
        raise
    finally:
        if source is not None:
            source.close()


def report_failure():
    """Called by the workflow failure step to notify the VPS."""
    job_id = os.environ.get("JOB_ID", "")
    callback = os.environ.get("CALLBACK", "")
    token = os.environ.get("WORKER_TOKEN", "")
    if not job_id or not callback or not token:
        return
    try:
        requests.post(callback + job_id + "/finish", json={
            "stage": "failed",
            "error": "The GitHub Actions runner could not complete the ZIP transfer",
        }, headers={"Authorization": "Bearer " + token}, timeout=(15, 30))
    except requests.RequestException:
        pass


def main():
    if "--report-failure" in sys.argv:
        report_failure()
        return 0

    job_id = os.environ.get("JOB_ID", "")
    attempt = int(os.environ.get("ATTEMPT", "1"))
    callback = os.environ.get("CALLBACK", "")
    worker_token = os.environ.get("WORKER_TOKEN", "")

    if not re.fullmatch(r"rl[a-z0-9]{16}", job_id):
        print("ERROR: invalid JOB_ID", flush=True)
        return 1
    if not callback or not worker_token:
        print("ERROR: missing CALLBACK or WORKER_TOKEN", flush=True)
        return 1

    deadline = time.monotonic() + MAX_SECONDS
    reporter = ProgressReporter(callback, job_id, worker_token)

    try:
        # Fetch job configuration from the VPS (same protocol as the Space worker)
        resp = requests.get(callback + job_id, headers={"Authorization": "Bearer " + worker_token},
                            timeout=(15, 30))
        resp.raise_for_status()
        config = resp.json()
        if config.get("cancelled"):
            print("Job already cancelled", flush=True)
            return 0
        source_url = config["url"]
        hf_token = config["bucket_token"]
        limit = min(max(1, int(config.get("max_bytes") or ZIP_LIMIT)), ZIP_LIMIT)
        max_seconds = min(max(60, int(config.get("max_seconds") or MAX_SECONDS)), MAX_SECONDS)
        deadline = time.monotonic() + max_seconds

        reporter.report(0, 0, "Reading ZIP index")
        manifest, file_count, _ = publish_zip(source_url, job_id, attempt, hf_token,
                                              reporter, limit=limit)

        if time.monotonic() > deadline:
            raise SourceError("Exceeded the runner time limit")

        requests.post(callback + job_id + "/finish", json={
            "stage": "done",
            "message": "Uploaded to the public bucket",
            "bucket_manifest": manifest,
            "bucket_count": file_count,
        }, headers={"Authorization": "Bearer " + worker_token}, timeout=(15, 30))
        print(f"SUCCESS: {file_count} files uploaded, manifest published", flush=True)
        return 0

    except Exception as exc:
        message = str(exc) if isinstance(exc, (ValueError, SourceError)) else \
            "The runner could not complete the ZIP transfer"
        print(f"FAILURE: {message}", flush=True)
        try:
            requests.post(callback + job_id + "/finish", json={
                "stage": "cancelled" if reporter.cancelled else "failed",
                "error": message[:250],
            }, headers={"Authorization": "Bearer " + worker_token}, timeout=(15, 30))
        except requests.RequestException:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())