"""Extract a remote ZIP one member at a time into Pixeldrain, never via VPS.

The manifest has `ZIP member path<TAB>direct URL` rows, so an episode name
remains unambiguous even though Pixeldrain URLs only contain opaque file IDs.
"""
import os
import re
import tempfile
import zipfile
from urllib.parse import quote

import requests

from zip_bucket import (CHUNK, INDEX_LIMIT, RangedZip, SourceError, inspect_zip,
                        safe_path)

API = "https://pixeldrain.com/api/file"
FILE_ID = re.compile(r"[A-Za-z0-9]{4,32}\Z")


def upload_file(key, name, path=None, content=None, session=None):
    """Use Pixeldrain's raw PUT API; never log the request or response body."""
    session = session or requests
    if path is not None:
        payload = open(path, "rb")
    else:
        payload = content
    try:
        try:
            response = session.put(API + "/" + quote(name, safe=""), data=payload,
                                   auth=("", key), timeout=(30, 180))
        except requests.Timeout:
            raise SourceError("Pixeldrain upload timed out") from None
        except requests.RequestException:
            raise SourceError("Pixeldrain upload connection failed") from None
        if response.status_code != 201:
            raise SourceError(f"Pixeldrain rejected upload (HTTP {response.status_code})")
        try:
            file_id = response.json()["id"]
        except (ValueError, KeyError, TypeError):
            raise SourceError("Pixeldrain returned an invalid upload result") from None
        if not isinstance(file_id, str) or not FILE_ID.fullmatch(file_id):
            raise SourceError("Pixeldrain returned an invalid file ID")
        return file_id
    finally:
        if path is not None:
            payload.close()


def delete_files(key, file_ids, session=None):
    session = session or requests
    for file_id in file_ids:
        if FILE_ID.fullmatch(file_id):
            try:
                session.delete(API + "/" + file_id, auth=("", key), timeout=(15, 30))
            except requests.RequestException:
                pass  # The owner can remove orphans from their account.


def publish_zip(url, job_id, attempt, key, progress, check, limit, session=None):
    if not re.fullmatch(r"rl[a-z0-9]{16}", job_id) or not 1 <= attempt <= 10000 or not key:
        raise SourceError("Invalid transfer job identity")
    source = None
    uploaded = []
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
                check()
                name = safe_path(info)
                # Preserve original folder information in links.txt, but only
                # send a plain filename to Pixeldrain's PUT endpoint.
                filename = name.rsplit("/", 1)[-1]
                with tempfile.TemporaryDirectory(prefix="pts-zip-") as scratch:
                    local = os.path.join(scratch, "entry")
                    with archive.open(info) as entry, open(local, "wb") as target:
                        copied = 0
                        while True:
                            check()
                            chunk = entry.read(CHUNK)
                            if not chunk:
                                break
                            copied += len(chunk)
                            if copied > info.file_size:
                                raise SourceError("The ZIP entry exceeded its declared size")
                            target.write(chunk)
                            progress(done + copied, total, f"Extracting file {index}/{len(entries)}")
                        if copied != info.file_size:
                            raise SourceError("The ZIP entry ended before its declared size")
                    check()
                    source._close_stream()
                    progress(0, 0, f"Uploading file {index}/{len(entries)} (size {copied} bytes)")
                    file_id = upload_file(key, filename, path=local, session=session)
                    uploaded.append(file_id)
                links.append(name + "\t" + API + "/" + file_id)
                done += info.file_size
                progress(done, total, f"Uploaded file {index}/{len(entries)}")
            check()
            manifest = upload_file(key, f"links-{job_id}-{attempt}.txt",
                                   content=("\n".join(links) + "\n").encode("utf-8"), session=session)
            uploaded.append(manifest)
            check()
            return API + "/" + manifest + "?download", len(links)
    except Exception:
        delete_files(key, uploaded, session=session)
        raise
    finally:
        if source is not None:
            source.close()
