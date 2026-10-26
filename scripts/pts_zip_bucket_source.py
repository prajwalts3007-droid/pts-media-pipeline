"""Bounded, one-entry-at-a-time extraction of a range-capable remote ZIP.

The ZIP and its contents must never be staged on the VPS. The Space reads only
the ZIP index plus one entry at a time; all redirects must stay on public hosts.
"""
import io
import ipaddress
import os
import re
import socket
import tempfile
import zipfile
from urllib.parse import quote, urljoin, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import HTTPError as RangeHTTPError
from urllib3.util.retry import Retry


ZIP_LIMIT = 100 * (1 << 30)
EXPANDED_LIMIT = 100 * (1 << 30)
ENTRY_LIMIT = 8 * (1 << 30)  # Four parallel jobs stay under the Space's scratch ceiling.
FILE_LIMIT = 500
INDEX_LIMIT = 8 * (1 << 20)
CHUNK = 4 * (1 << 20)
BUCKET_ID = "Ptsts1998/pts-streamed-files"


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
            except (OSError, RangeHTTPError, requests.RequestException):
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
            # Python's zipfile can call read() without a length while reading
            # the end-of-central-directory. Never turn that into a full ZIP
            # download: it is safe only within the bounded tail/index.
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
        raise SourceError("An extracted file exceeds the 8 GiB Space scratch limit")
    # Preserve subfolders, but reject duplicate paths and keep each directory
    # comfortably inside Hugging Face's 10,000-entry folder recommendation.
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


def publish_zip(url, job_id, attempt, token, progress, cancelled, api=None, limit=ZIP_LIMIT, on_source=None):
    """Publish each verified entry and return its public text-manifest URL.

    A failed job removes only its own attempt prefix. Caller persists the
    job/attempt for retry-safe expiry and purge on the VPS.
    """
    if not re.fullmatch(r"rl[a-z0-9]{16}", job_id) or not 1 <= attempt <= 10000:
        raise SourceError("Invalid transfer job identity")
    from huggingface_hub import HfApi
    api = api or HfApi(token=token)
    prefix = f"transfers/{job_id}/{attempt}/"
    uploaded = []
    source = None
    try:
        source = RangedZip(url, limit=limit)
        if on_source is not None:
            on_source(source.headers, source.url)
        # zipfile reads only the end-of-directory and the central directory.
        with zipfile.ZipFile(source) as archive:
            if source.size - archive.start_dir > INDEX_LIMIT + 65557:
                raise SourceError("The ZIP index exceeds the metadata limit")
            entries, total = inspect_zip(archive)
            # Use a persistent range response for each sequential member read.
            # A seek to the next member closes the prior response. This avoids
            # thousands of independent R2 range requests and buffers only one
            # bounded output chunk (never the entire archive).
            source.cache = b""
            source.cache_start = -1
            source.streaming = True
            done = 0
            links = []
            for index, info in enumerate(entries, 1):
                if cancelled():
                    raise SourceError("Cancelled")
                path = prefix + safe_path(info)
                # Do not write extracted files outside the private Space.
                with tempfile.TemporaryDirectory(prefix="pts-zip-") as scratch:
                    local = os.path.join(scratch, "entry")
                    with archive.open(info) as entry, open(local, "wb") as target:
                        copied = 0
                        while True:
                            if cancelled():
                                raise SourceError("Cancelled")
                            data = entry.read(CHUNK)
                            if not data:
                                break
                            copied += len(data)
                            if copied > info.file_size:
                                raise SourceError("The ZIP entry exceeded its declared size")
                            target.write(data)
                            progress(done + copied, total, f"Extracting file {index}/{len(entries)}")
                        if copied != info.file_size:
                            raise SourceError("The ZIP entry ended before its declared size")
                    if cancelled():
                        raise SourceError("Cancelled")
                    if hasattr(source, "_close_stream"):
                        source._close_stream()
                    progress(0, 0, f"Uploading file {index}/{len(entries)} (size {copied} bytes)")
                    api.batch_bucket_files(BUCKET_ID, add=[(local, path)])
                    uploaded.append(path)
                done += info.file_size
                links.append(bucket_url(path))
                progress(done, total, f"Uploaded file {index}/{len(entries)}")
            manifest = prefix + "links.txt"
            if cancelled():
                raise SourceError("Cancelled")
            api.batch_bucket_files(BUCKET_ID, add=[(("\n".join(links) + "\n").encode("utf-8"), manifest)])
            uploaded.append(manifest)
            if cancelled():
                raise SourceError("Cancelled")
            return bucket_url(manifest), len(links), prefix
    except Exception:
        try:
            if hasattr(api, "list_bucket_tree"):
                uploaded = [item.path for item in api.list_bucket_tree(BUCKET_ID, prefix=prefix)
                            if item.type == "file" and item.path.startswith(prefix)]
            if uploaded:
                api.batch_bucket_files(BUCKET_ID, delete=uploaded)
        except Exception:
            pass  # VPS expiry/purge retries orphan cleanup for this job.
        raise
    finally:
        if source is not None:
            source.close()
