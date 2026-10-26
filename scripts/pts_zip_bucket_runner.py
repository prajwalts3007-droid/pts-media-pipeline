"""Runner control plane for ZIP-to-bucket jobs; signed URLs never enter logs.

The source and bucket token are obtained through the short-lived job callback.
All archive bytes travel directly between the source, runner, and bucket.
"""
import os
import re
import sys
import threading
import time

import requests

from zip_bucket import SourceError, publish_zip


class Reporter:
    def __init__(self, callback, job_id, token, deadline):
        self.url = callback + job_id
        self.headers = {"Authorization": "Bearer " + token}
        self.deadline = deadline
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.state = {"bytes_done": 0, "size_bytes": 0, "speed_bps": 0,
                      "stage": "downloading", "message": "Reading ZIP index"}
        self.cancel = False
        self.last_success = time.monotonic()
        self.previous_bytes = 0
        self.previous_time = time.monotonic()

    def call(self, method, suffix="", body=None):
        response = requests.request(method, self.url + suffix, json=body, headers=self.headers,
                                    timeout=(15, 30))
        response.raise_for_status()
        return response.json()

    def update(self, done, total, message):
        with self.lock:
            if total:
                self.state["bytes_done"] = done
                self.state["size_bytes"] = total
            self.state["stage"] = "transferring" if message.startswith(("Uploading", "Uploaded")) else "downloading"
            self.state["message"] = message + ("; upload speed unavailable" if message.startswith("Uploading") else "")
        self.check()

    def check(self):
        if self.cancel or time.monotonic() >= self.deadline:
            raise SourceError("Cancelled or time limit reached")
        if time.monotonic() - self.last_success > 240:
            raise SourceError("The control-plane callback stopped responding")

    def loop(self):
        while not self.stop.wait(4):
            now = time.monotonic()
            with self.lock:
                snapshot = dict(self.state)
            elapsed = max(now - self.previous_time, 0.001)
            delta = max(0, snapshot["bytes_done"] - self.previous_bytes)
            snapshot["speed_bps"] = int(delta / elapsed) if snapshot["stage"] == "downloading" else 0
            self.previous_bytes, self.previous_time = snapshot["bytes_done"], now
            total, done = snapshot["size_bytes"], snapshot["bytes_done"]
            snapshot["progress"] = done * 100 / total if total else 0
            snapshot["eta_seconds"] = int((total - done) / snapshot["speed_bps"]) if snapshot["speed_bps"] else 0
            snapshot["filename"] = ""
            try:
                answer = self.call("POST", "/progress", snapshot)
                self.last_success = time.monotonic()
                if answer.get("cancel"):
                    self.cancel = True
            except requests.RequestException:
                pass  # The foreground loop exits after four minutes without a callback.


def main():
    job_id = os.environ["PTS_JOB_ID"]
    attempt = int(os.environ["PTS_ATTEMPT"])
    callback = os.environ["PTS_CALLBACK"]
    token = os.environ.get("PTS_JOB_TOKEN", "")
    if (not re.fullmatch(r"rl[a-z0-9]{16}", job_id) or not 1 <= attempt <= 10000 or
            not re.fullmatch(r"https://[A-Za-z0-9.-]+/api/v1/relay/worker/", callback) or
            len(token) != 64):
        raise SourceError("Invalid ZIP job dispatch configuration")
    print("ZIP runner credential accepted", flush=True)
    reporter = Reporter(callback, job_id, token, time.monotonic() + 5.5 * 3600)
    try:
        config = reporter.call("GET")
        if config.get("cancelled"):
            return
        if (config.get("destination") != "bucket" or config.get("attempt") != attempt or
                not config.get("bucket_token") or not config.get("url")):
            raise SourceError("The callback returned a different bucket job")
        limit = min(int(config.get("max_bytes") or 0), 100 << 30)
        if limit < 1:
            raise SourceError("The transfer safety cap is invalid")
        reporter.deadline = time.monotonic() + min(5.5 * 3600, int(config.get("max_seconds") or 0))
        thread = threading.Thread(target=reporter.loop, daemon=True)
        thread.start()
        manifest, count, _ = publish_zip(config["url"], job_id, attempt,
                                         config["bucket_token"], reporter.update,
                                         reporter.check, limit=limit)
        reporter.check()
        reporter.stop.set()
        thread.join(timeout=6)
        reporter.call("POST", "/finish", {"stage": "done", "bucket_manifest": manifest,
                                           "bucket_count": count, "message": "Uploaded to the public bucket"})
        print("Bucket transfer complete; files:", count, flush=True)
    except Exception as exc:
        reporter.stop.set()
        # Only our own fixed errors are safe: requests/HF SDK exceptions may
        # contain signed URLs, auth headers, or bucket tokens.
        message = str(exc) if isinstance(exc, SourceError) else "The ZIP runner could not complete the transfer"
        try:
            reporter.call("POST", "/finish", {"stage": "cancelled" if reporter.cancel else "failed",
                                              "error": message[:250]})
        except requests.RequestException:
            pass
        print("ZIP transfer failed:", message, flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    try:
        main()
    except (KeyError, ValueError, SourceError):
        print("ZIP runner configuration rejected", flush=True)
        sys.exit(1)
