"""Relay an entire ZIP to Pixeldrain through bounded RAM, never runner disk.

The source must honor verified HTTPS byte ranges. The initial four-byte probe
establishes the source size before the raw PUT, allowing a fixed Content-Length
instead of an unbounded chunked upload. No ZIP or member is written locally.
"""
import re
import requests
from urllib.parse import quote

from zip_bucket import RangedZip, SourceError


CHUNK = 1 << 20
ID = re.compile(r"[A-Za-z0-9]{4,32}\Z")
API = "https://pixeldrain.com/api/file/"


class SourceBody:
    def __init__(self, response, size, progress, check):
        self.response = response
        self.size = size
        self.progress = progress
        self.check = check
        self.sent = 0

    def __len__(self):
        return self.size

    def __iter__(self):
        while self.sent < self.size:
            self.check()
            try:
                chunk = self.response.raw.read(min(CHUNK, self.size - self.sent))
            except Exception:
                raise SourceError("ZIP source interrupted the streaming connection") from None
            if not chunk:
                raise SourceError("ZIP source ended before its declared size")
            self.sent += len(chunk)
            self.progress(self.sent, self.size, "Uploading ZIP to Pixeldrain")
            yield chunk


def relay_zip(url, filename, key, progress, check, limit, source_factory=RangedZip, session=requests):
    if not key or not 1 <= limit <= 100 << 30:
        raise SourceError("Invalid ZIP relay configuration")
    source = source_factory(url, limit=limit)
    try:
        if source._request(0, 3)[:2] != b"PK":
            raise SourceError("The source is not a ZIP archive")
        check()
        # Source URLs often use opaque IDs; keep an intelligible .zip filename.
        filename = filename if filename.lower().endswith(".zip") else filename + ".zip"
        if not filename or len(filename.encode("utf-8")) > 255 or "/" in filename or "\\" in filename:
            raise SourceError("Invalid ZIP filename")
        with source._open_range(0, source.size - 1) as response:
            body = SourceBody(response, source.size, progress, check)
            try:
                result = session.put(API + quote(filename, safe=""), data=body,
                                     headers={"Content-Length": str(source.size),
                                              "Content-Type": "application/zip"},
                                     auth=("", key), timeout=(30, 180))
            except requests.Timeout:
                raise SourceError("Pixeldrain ZIP upload timed out") from None
            except requests.RequestException:
                raise SourceError("Pixeldrain ZIP upload connection failed") from None
            if result.status_code != 201:
                raise SourceError(f"Pixeldrain rejected the ZIP (HTTP {result.status_code})")
            if body.sent != source.size:
                raise SourceError("ZIP upload was not fully streamed")
            try:
                file_id = result.json()["id"]
            except (ValueError, KeyError, TypeError):
                raise SourceError("Pixeldrain returned an invalid ZIP result") from None
            if not isinstance(file_id, str) or not ID.fullmatch(file_id):
                raise SourceError("Pixeldrain returned an invalid ZIP file ID")
            return file_id
    finally:
        source.close()
