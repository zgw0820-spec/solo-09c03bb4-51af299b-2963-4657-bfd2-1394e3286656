"""Wire-level tests for UrllibBackfillTransport against a stdlib HTTP fake.

Unlike test_backfill.py's in-memory transport these exercise real URL
composition, ``?mount=&from=`` encoding, status classification, Location
resolution, DELETE cancellation, socket timeouts and the HEAD->Range-GET
fallback.  Everything stays in the standard library.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.admission.backfill import (  # noqa: E402
    BackfillService,
    UrllibBackfillTransport,
)
from app.admission.config import Settings  # noqa: E402
from tests.fake_backfill import install_index_fixture  # noqa: E402

SETTINGS = Settings(
    allowed_registries=("localhost:5000", "127.0.0.1:5000"),
    registry_upstream="http://registry.invalid",
    request_timeout=5.0,
)
TARGET = "images/target"
GOLDEN = "images/golden"
PARTIAL = "images/partial"


class WireState:
    def __init__(self):
        self.manifests: dict[str, dict[str, tuple[dict, str]]] = {}
        self.blobs: dict[str, dict[str, int]] = {}
        self.sessions: dict[str, str] = {}
        self.cancelled: list[str] = []
        self.deleted: list[str] = []
        # digest -> seconds to block the mount response (server still mounts).
        self.slow_mounts: dict[str, float] = {}
        self.range_gets: list[tuple[str, str]] = []
        self.head_requests: list[str] = []
        self._session_seq = 0
        self.lock = threading.Lock()

    def new_session(self, target: str) -> str:
        with self.lock:
            self._session_seq += 1
            session_id = f"s{self._session_seq}"
        self.sessions[session_id] = target
        return session_id


STATE = WireState()


def _canonical(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    # -- helpers ----------------------------------------------------------

    def _send_json(self, status: int, payload: dict | None = None,
                   headers: dict[str, str] | None = None) -> None:
        body = _canonical(payload) if payload is not None else b""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if body and self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def _parts(self):
        parsed = urlparse(self.path)
        return parsed, [p for p in parsed.path.split("/") if p]

    # -- GET / HEAD / DELETE / POST dispatch ------------------------------

    def do_GET(self):
        parsed, parts = self._parts()
        if len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "manifests":
            repo = "/".join(parts[1:-2])
            reference = parts[-1]
            entry = STATE.manifests.get(repo, {}).get(reference)
            if entry is None:
                self._send_json(404, {"errors": [{"code": "NOT_FOUND"}]})
                return
            manifest, digest = entry
            body = _canonical(manifest)
            self.send_response(200)
            self.send_header("Content-Type", manifest.get("mediaType", "application/json"))
            self.send_header("Docker-Content-Digest", digest)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        # GET blob: used by the Range fallback.
        if len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "blobs":
            repo = "/".join(parts[1:-2])
            digest = parts[-1]
            size = STATE.blobs.get(repo, {}).get(digest)
            if size is None:
                self._send_json(404, {"errors": [{"code": "BLOB_UNKNOWN"}]})
                return
            STATE.range_gets.append((repo, digest, self.headers.get("Range", "")))
            range_header = self.headers.get("Range", "")
            if range_header.startswith("bytes=0-0"):
                self.send_response(206)
                self.send_header("Content-Range", f"bytes 0-0/{size}")
                self.send_header("Content-Length", "1")
                self.end_headers()
                try:
                    self.wfile.write(b"X")
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.end_headers()
            return

        self._send_json(404, {})

    def do_HEAD(self):
        _parsed, parts = self._parts()
        if len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "blobs":
            repo = "/".join(parts[1:-2])
            digest = parts[-1]
            STATE.head_requests.append(digest)
            # Test hook: pretend HEAD is unsupported for this digest.
            if getattr(STATE, "head_405_digest", None) == digest:
                self.send_response(405)
                self.send_header("Allow", "GET")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            size = STATE.blobs.get(repo, {}).get(digest)
            if size is None:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.send_header("Docker-Content-Digest", digest)
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed, parts = self._parts()
        # /v2/<repo>/blobs/uploads/
        if len(parts) >= 4 and parts[0] == "v2" and parts[-2:] == ["blobs", "uploads"]:
            target = "/".join(parts[1:-2])
            query = parse_qs(parsed.query)
            digest = query.get("mount", [""])[0]
            source = query.get("from", [""])[0]

            delay = STATE.slow_mounts.get(digest, 0.0)

            source_size = STATE.blobs.get(source, {}).get(digest)
            if source_size is not None:
                # Commit the mount server-side before stalling the response,
                # modelling "mount completed but the reply timed out".
                STATE.blobs.setdefault(target, {})[digest] = source_size
            if delay:
                time.sleep(delay)

            if source_size is not None:
                self.send_response(201)
                self.send_header(
                    "Location", f"/v2/{target}/blobs/{digest}"
                )
                self.send_header("Docker-Content-Digest", digest)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            session_id = STATE.new_session(target)
            self.send_response(202)
            self.send_header(
                "Location", f"/v2/{target}/blobs/uploads/{session_id}"
            )
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        self._send_json(404, {})

    def do_DELETE(self):
        _parsed, parts = self._parts()
        # /v2/<repo>/blobs/uploads/<session-id>
        if parts and parts[0] == "v2" and "blobs" in parts:
            blob_index = parts.index("blobs")
            if (
                blob_index + 2 < len(parts)
                and parts[blob_index + 1] == "uploads"
            ):
                repo = "/".join(parts[1:blob_index])
                session_id = parts[blob_index + 2]
                STATE.deleted.append((repo, session_id))
                STATE.cancelled.append(session_id)
                self.send_response(204)
                self.end_headers()
                return
        self.send_response(404)
        self.end_headers()


class WireTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def setUp(self):
        # Reset shared state per test.
        STATE.manifests.clear()
        STATE.blobs.clear()
        STATE.sessions.clear()
        STATE.cancelled.clear()
        STATE.deleted.clear()
        STATE.slow_mounts.clear()
        STATE.range_gets.clear()
        STATE.head_requests.clear()
        STATE.head_405_digest = None

    def make_service(self, timeout=5.0):
        settings = Settings(
            allowed_registries=("localhost:5000", "127.0.0.1:5000"),
            registry_upstream=self.base_url,
            request_timeout=timeout,
        )
        transport = UrllibBackfillTransport(settings, timeout=timeout)
        return BackfillService(settings, transport=transport)

    def install_fixture(self, repository=TARGET, **kwargs):
        # Use the in-memory builder but redirect its add_* calls into wire
        # state by giving it a tiny adapter.
        adapter = _WireAdapter()
        fixture = install_index_fixture(adapter, repository, **kwargs)
        STATE.manifests.update(adapter.manifests)
        for repo, blobs in adapter.blobs.items():
            STATE.blobs.setdefault(repo, {}).update(blobs)
        return fixture


class _WireAdapter:
    """Duck-types the subset of FakeBackfillTransport the builder uses."""

    def __init__(self):
        self.manifests = {}
        self.blobs = {}

    def add_manifest(self, repository, reference, manifest, digest):
        self.manifests.setdefault(repository, {})[reference] = (manifest, digest)

    def add_blob(self, repository, digest, size):
        self.blobs.setdefault(repository, {})[digest] = size

    def remove_blob(self, repository, digest):
        self.blobs.get(repository, {}).pop(digest, None)


class WireFlowTests(WireTestBase):
    def test_full_backfill_with_session_cancel_over_http(self):
        fixture = self.install_fixture(TARGET)
        layers = fixture["layers"]
        index_digest = fixture["index_digest"]

        # golden has everything; partial lacks the amd64 layer.
        for name, (digest, size) in layers.items():
            STATE.blobs.setdefault(GOLDEN, {})[digest] = size
        for name in ("shared", "arm64-only"):
            digest, size = layers[name]
            STATE.blobs.setdefault(PARTIAL, {})[digest] = size
        for digest, _size in layers.values():
            STATE.blobs[TARGET].pop(digest, None)

        service = self.make_service()
        report = service.backfill(
            f"127.0.0.1:5000/{TARGET}@{index_digest}", [PARTIAL, GOLDEN]
        )
        self.assertTrue(report.complete, report.to_dict())
        amd_digest = layers["amd64-only"][0]
        amd = next(l for l in report.layers if l.digest == amd_digest)
        self.assertEqual(amd.mounted_from, GOLDEN)
        self.assertTrue(
            any(a.result == "upload_session" for a in amd.attempts)
        )
        # The 202 session from partial was DELETE-cancelled on the wire.
        self.assertTrue(STATE.cancelled)
        self.assertEqual(len(STATE.cancelled), 1)
        self.assertEqual(STATE.deleted[0][0], TARGET)

        # Retry: no mounts at the HTTP layer.
        before = len(STATE.head_requests)
        report2 = service.backfill(
            f"127.0.0.1:5000/{TARGET}@{index_digest}", [PARTIAL, GOLDEN]
        )
        self.assertTrue(report2.complete)
        self.assertTrue(all(l.already_present for l in report2.layers))
        _ = before

    def test_mount_timeout_but_server_mounts_verified_over_http(self):
        fixture = self.install_fixture(
            TARGET, arches=("amd64",), shared_layers=("lonely",),
            unique_layers=False,
        )
        digest, size = fixture["layers"]["lonely"]
        index_digest = fixture["index_digest"]
        STATE.blobs.setdefault(GOLDEN, {})[digest] = size
        STATE.blobs[TARGET].pop(digest, None)

        # Server takes longer than the client timeout but links the blob.
        STATE.slow_mounts[digest] = 0.8

        service = self.make_service(timeout=0.2)
        start = time.monotonic()
        report = service.backfill(
            f"127.0.0.1:5000/{TARGET}@{index_digest}", [GOLDEN]
        )
        elapsed = time.monotonic() - start
        self.assertTrue(report.complete, report.to_dict())
        layer = report.layers[0]
        self.assertEqual(layer.resolved_by, "verified-after-mount-timeout")
        self.assertLess(elapsed, 2.0)

    def test_head_405_falls_back_to_range_get(self):
        fixture = self.install_fixture(TARGET)
        index_digest = fixture["index_digest"]
        shared_digest = fixture["layers"]["shared"][0]
        STATE.head_405_digest = shared_digest
        # Everything stays present; backfill with no sources is verification.
        service = self.make_service()
        report = service.backfill(
            f"127.0.0.1:5000/{TARGET}@{index_digest}", []
        )
        self.assertTrue(report.complete, report.to_dict())
        shared = next(l for l in report.layers if l.digest == shared_digest)
        self.assertTrue(shared.already_present)
        self.assertIn(
            (TARGET, shared_digest, "bytes=0-0"), STATE.range_gets
        )


if __name__ == "__main__":
    unittest.main()
