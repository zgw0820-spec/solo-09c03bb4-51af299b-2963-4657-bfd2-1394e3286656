"""Offline tests for the OCI layer backfill state machine and keyed locks."""

from __future__ import annotations

import os
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.admission.backfill import (  # noqa: E402
    BackfillRequestError,
    BackfillService,
    KeyedLayerLocks,
    LAYER_FAILED,
    LAYER_PRESENT,
    MOUNT_ERROR,
    MOUNT_TIMEOUT,
    MountResult,
)
from app.admission.config import Settings  # noqa: E402
from app.admission.images import parse_image_reference  # noqa: E402
from tests.fake_backfill import (  # noqa: E402
    FakeBackfillTransport,
    install_index_fixture,
)

SETTINGS = Settings(
    allowed_registries=("localhost:5000", "registry:5000"),
    registry_upstream="http://registry.invalid",
    default_platforms=("linux/amd64", "linux/arm64"),
    request_timeout=1.0,
)
TARGET = "images/target"
GOLDEN = "images/golden"
PARTIAL = "images/partial"
EMPTY_SRC = "images/empty"


def target_ref(index_digest: str) -> str:
    return f"localhost:5000/{TARGET}@{index_digest}"


class BackfillTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.transport = FakeBackfillTransport()
        self.service = BackfillService(SETTINGS, transport=self.transport)
        self.fixture = install_index_fixture(self.transport, TARGET)
        self.index_digest = self.fixture["index_digest"]
        self.layers = self.fixture["layers"]  # name -> (digest, size)

    def seed_source(self, repository: str, *names: str) -> None:
        for name in names:
            digest, size = self.layers[name]
            self.transport.add_blob(repository, digest, size)

    def remove_all_target_layers(self) -> None:
        for digest, _size in self.layers.values():
            self.transport.remove_blob(TARGET, digest)

    def run_backfill(self, sources):
        return self.service.backfill(target_ref(self.index_digest), sources)

    def layer_report(self, report, name):
        digest = self.layers[name][0]
        return next(layer for layer in report.layers if layer.digest == digest)


class RequestValidationTests(BackfillTestBase):
    def test_tag_reference_rejected(self):
        with self.assertRaises(BackfillRequestError) as ctx:
            self.service.backfill("localhost:5000/images/target:v1", [GOLDEN])
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("摘要", str(ctx.exception))

    def test_empty_target_rejected(self):
        with self.assertRaises(BackfillRequestError) as ctx:
            self.service.backfill("   ", [])
        self.assertEqual(ctx.exception.status_code, 422)

    def test_unparseable_target_rejected(self):
        with self.assertRaises(BackfillRequestError) as ctx:
            self.service.backfill("localhost:5000/img@md5:zzz", [])
        self.assertEqual(ctx.exception.status_code, 400)

    def test_non_local_registry_rejected(self):
        with self.assertRaises(BackfillRequestError) as ctx:
            self.service.backfill(
                f"docker.io/library/img@{self.index_digest}", []
            )
        self.assertEqual(ctx.exception.status_code, 403)

    def test_source_on_other_registry_rejected(self):
        with self.assertRaises(BackfillRequestError) as ctx:
            self.run_backfill(["ghcr.io/team/images"])
        self.assertEqual(ctx.exception.status_code, 400)

    def test_local_registry_alias_source_accepted(self):
        self.seed_source(GOLDEN, "shared", "amd64-only", "arm64-only")
        self.remove_all_target_layers()
        report = self.run_backfill([f"registry:5000/{GOLDEN}"])
        self.assertTrue(report.complete)
        self.assertEqual(report.sources, [GOLDEN])

    def test_same_repository_source_ignored_with_warning(self):
        report = self.run_backfill([TARGET, GOLDEN, GOLDEN])
        # target layers are all present in setUp, so nothing is mounted.
        self.assertTrue(report.complete)
        self.assertEqual(report.sources, [GOLDEN])
        self.assertTrue(any("与目标仓库相同" in w for w in report.warnings))

    def test_sources_must_be_list(self):
        with self.assertRaises(BackfillRequestError) as ctx:
            self.service.backfill(target_ref(self.index_digest), "nope")  # type: ignore[arg-type]
        self.assertEqual(ctx.exception.status_code, 422)


class PreconditionTests(BackfillTestBase):
    def test_missing_index_returns_404(self):
        with self.assertRaises(BackfillRequestError) as ctx:
            self.service.backfill(
                "localhost:5000/images/target@" + "sha256:" + "a" * 64, []
            )
        self.assertEqual(ctx.exception.status_code, 404)

    def test_index_digest_header_mismatch_returns_502(self):
        bad_digest = "sha256:" + "f" * 64
        self.transport.add_manifest(
            TARGET, bad_digest,
            self.fixture["index"], "sha256:" + "0" * 64,
        )
        with self.assertRaises(BackfillRequestError) as ctx:
            self.service.backfill(
                f"localhost:5000/{TARGET}@{bad_digest}", []
            )
        self.assertEqual(ctx.exception.status_code, 502)
        self.assertIn("摘要不一致", str(ctx.exception))

    def test_non_index_digest_returns_409(self):
        single = {"schemaVersion": 2, "mediaType":
                  "application/vnd.oci.image.manifest.v1+json",
                  "config": {}, "layers": []}
        digest = "sha256:" + "1" * 64
        self.transport.add_manifest(TARGET, digest, single, digest)
        with self.assertRaises(BackfillRequestError) as ctx:
            self.service.backfill(
                f"localhost:5000/{TARGET}@{digest}", []
            )
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("不是 OCI 索引", str(ctx.exception))

    def test_missing_child_manifest_returns_409(self):
        index = self.fixture["index"]
        child_digest = index["manifests"][0]["digest"]
        del self.transport.manifests[TARGET][child_digest]
        with self.assertRaises(BackfillRequestError) as ctx:
            self.run_backfill([])
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("直属子清单", str(ctx.exception))
        self.assertIn(child_digest, str(ctx.exception))

    def test_nested_index_child_rejected(self):
        nested = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [],
        }
        nested_digest = "sha256:" + "2" * 64
        self.transport.add_manifest(TARGET, nested_digest, nested, nested_digest)
        # Replace the first descriptor's digest in the stored index.
        index_digest = self.index_digest
        stored_index, _ = self.transport.manifests[TARGET][index_digest]
        stored_index["manifests"][0]["digest"] = nested_digest
        with self.assertRaises(BackfillRequestError) as ctx:
            self.run_backfill([])
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("嵌套索引", str(ctx.exception))

    def test_missing_config_returns_409(self):
        arch = "amd64"
        config_digest, _ = self.fixture["configs"][arch]
        self.transport.remove_blob(TARGET, config_digest)
        with self.assertRaises(BackfillRequestError) as ctx:
            self.run_backfill([])
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("配置", str(ctx.exception))

    def test_config_size_mismatch_returns_409(self):
        config_digest, real_size = self.fixture["configs"]["amd64"]
        self.transport.blobs[TARGET][config_digest] = real_size + 999
        with self.assertRaises(BackfillRequestError) as ctx:
            self.run_backfill([])
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("配置", str(ctx.exception))


class HappyPathAndDedupeTests(BackfillTestBase):
    def test_shared_layer_merged_and_all_mounted_from_first_source(self):
        self.seed_source(GOLDEN, "shared", "amd64-only", "arm64-only")
        self.remove_all_target_layers()

        report = self.run_backfill([GOLDEN])

        self.assertTrue(report.complete)
        payload = report.to_dict()
        # shared + two arch-specific layers, despite two child manifests.
        self.assertEqual(payload["summary"]["layers"], 3)
        self.assertEqual(payload["summary"]["mounted"], 3)
        self.assertEqual(payload["summary"]["failed"], 0)

        mounted = [call for call in self.transport.mount_calls
                   if call[0] == TARGET]
        self.assertEqual(len(mounted), 3)
        digests = sorted(call[1] for call in mounted)
        self.assertEqual(len(set(digests)), 3)
        # Every mount asked the golden source.
        self.assertTrue(all(call[2] == GOLDEN for call in mounted))
        # No upload sessions were cancelled on the direct success path.
        self.assertEqual(self.transport.delete_calls, [])
        # Target blobs are now all present with manifest sizes.
        for digest, size in self.layers.values():
            self.assertEqual(self.transport.blobs[TARGET][digest], size)
        # Report exposes failures list empty.
        self.assertEqual(payload["failures"], [])

    def test_no_blob_content_methods_exist_on_service_flow(self):
        # The service only HEADs blobs and POSTs mounts / DELETEs sessions;
        # the transport protocol deliberately has no blob upload/download.
        self.seed_source(GOLDEN, "shared", "amd64-only", "arm64-only")
        self.remove_all_target_layers()
        self.run_backfill([GOLDEN])
        for method in ("get_blob", "upload_blob", "download_blob", "put_manifest"):
            self.assertFalse(hasattr(self.transport, method))

    def test_already_present_layer_is_skipped_without_mount(self):
        self.seed_source(GOLDEN, "amd64-only", "arm64-only")
        # Keep "shared" in target (setUp put everything there) but remove
        # only the arch-specific layers.
        for name in ("amd64-only", "arm64-only"):
            self.transport.remove_blob(TARGET, self.layers[name][0])

        report = self.run_backfill([GOLDEN])
        self.assertTrue(report.complete)

        shared = self.layer_report(report, "shared")
        self.assertEqual(shared.status, LAYER_PRESENT)
        self.assertTrue(shared.already_present)
        self.assertFalse(shared.mount_attempted)
        self.assertIsNone(shared.mounted_from)

        mounted_digests = {call[1] for call in self.transport.mount_calls}
        self.assertNotIn(self.layers["shared"][0], mounted_digests)
        self.assertIn(self.layers["amd64-only"][0], mounted_digests)

    def test_retry_after_success_skips_everything(self):
        self.seed_source(GOLDEN, "shared", "amd64-only", "arm64-only")
        self.remove_all_target_layers()

        first = self.run_backfill([GOLDEN])
        self.assertTrue(first.complete)
        mounts_after_first = len(self.transport.mount_calls)
        self.assertGreater(mounts_after_first, 0)

        self.transport.reset_call_log()
        second = self.run_backfill([GOLDEN])
        self.assertTrue(second.complete)
        self.assertEqual(self.transport.mount_calls, [])
        self.assertEqual(self.transport.delete_calls, [])
        self.assertTrue(
            all(layer.already_present for layer in second.layers)
        )


class Only404MountsTests(BackfillTestBase):
    def test_present_but_wrong_size_is_failed_without_mount(self):
        digest, size = self.layers["shared"]
        self.transport.blobs[TARGET][digest] = size + 7
        report = self.run_backfill([GOLDEN])
        shared = self.layer_report(report, "shared")
        self.assertEqual(shared.status, LAYER_FAILED)
        self.assertTrue(any("大小" in r for r in shared.reasons))
        self.assertFalse(
            any(call[1] == digest for call in self.transport.mount_calls)
        )
        self.assertFalse(report.complete)
        failure = report.to_dict()["failures"][0]
        self.assertEqual(failure["digest"], digest)
        self.assertTrue(failure["reasons"])

    def test_head_transport_error_does_not_mount(self):
        digest, _ = self.layers["shared"]
        self.transport.head_errors[(TARGET, digest)] = "HEAD 请求超时: boom"
        report = self.run_backfill([GOLDEN])
        shared = self.layer_report(report, "shared")
        self.assertEqual(shared.status, LAYER_FAILED)
        self.assertTrue(any("boom" in r for r in shared.reasons))
        self.assertFalse(
            any(call[1] == digest for call in self.transport.mount_calls)
        )

    def test_transient_preliminary_head_error_rechecked_under_lock(self):
        # Models a HEAD racing another caller's link write: the first HEAD
        # fails with 500, but by the time the per-key lock is held the blob is
        # verifiable, so no mount is attempted and the layer is reported good.
        from app.admission.backfill import HeadBlobResult

        digest, size = self.layers["shared"]
        self.remove_all_target_layers()
        calls = {"count": 0}
        original_head = self.transport.head_blob

        def flaky_head(repository, d):
            if d == digest:
                calls["count"] += 1
                if calls["count"] == 1:
                    return HeadBlobResult(
                        state="error", detail="HEAD 目标层返回 HTTP 500"
                    )
            return original_head(repository, d)

        self.transport.head_blob = flaky_head
        self.transport.add_blob(TARGET, digest, size)

        report = self.run_backfill([])
        shared = self.layer_report(report, "shared")
        self.assertEqual(shared.status, LAYER_PRESENT)
        self.assertEqual(shared.resolved_by, "concurrent-request")
        self.assertFalse(
            any(call[1] == digest for call in self.transport.mount_calls)
        )
        self.assertGreaterEqual(calls["count"], 2)


class SessionFallbackTests(BackfillTestBase):
    def test_upload_session_is_cancelled_then_next_source_wins(self):
        # partial has shared + arm64 layer but NOT the amd64 layer.
        self.seed_source(PARTIAL, "shared", "arm64-only")
        self.seed_source(GOLDEN, "shared", "amd64-only", "arm64-only")
        self.remove_all_target_layers()

        report = self.run_backfill([PARTIAL, GOLDEN])
        self.assertTrue(report.complete, report.to_dict())

        amd = self.layer_report(report, "amd64-only")
        self.assertEqual(amd.mounted_from, GOLDEN)
        results = [(a.source, a.result) for a in amd.attempts]
        self.assertIn((PARTIAL, "upload_session"), results)
        self.assertIn((GOLDEN, "mounted"), results)

        # A session was created for the missing-in-partial amd64 layer and
        # actually cancelled through DELETE.
        session_attempt = next(
            a for a in amd.attempts if a.result == "upload_session"
        )
        self.assertTrue(session_attempt.session_cancelled)
        self.assertTrue(self.transport.delete_calls)
        self.assertEqual(
            len(self.transport.deleted_sessions),
            len(self.transport.delete_calls),
        )

        # shared/arm came straight from partial.
        self.assertEqual(self.layer_report(report, "shared").mounted_from, PARTIAL)
        self.assertEqual(
            self.layer_report(report, "arm64-only").mounted_from, PARTIAL
        )

    def test_failed_session_cancel_is_recorded_but_continues(self):
        self.seed_source(GOLDEN, "amd64-only")
        self.remove_all_target_layers()
        digest = self.layers["amd64-only"][0]

        def handler(transport, target, d, source):
            if source == PARTIAL and d == digest:
                return MountResult(
                    outcome="upload_session",
                    status_code=202,
                    location="http://registry.invalid/cancel-fails",
                )
            return None

        self.transport.mount_handler = handler
        report = self.run_backfill([PARTIAL, GOLDEN])
        amd = self.layer_report(report, "amd64-only")
        self.assertEqual(amd.status, LAYER_PRESENT)
        session_attempt = amd.attempts[0]
        self.assertFalse(session_attempt.session_cancelled)
        self.assertIn("500", session_attempt.detail)

    def test_mount_error_falls_through_to_next_source(self):
        self.seed_source(GOLDEN, "shared", "amd64-only", "arm64-only")
        self.remove_all_target_layers()

        def handler(transport, target, digest, source):
            if source == "images/broken-src":
                return MountResult(outcome=MOUNT_ERROR, status_code=500,
                                  detail="挂载请求返回 HTTP 500")
            return None

        self.transport.mount_handler = handler
        report = self.run_backfill(["images/broken-src", GOLDEN])
        self.assertTrue(report.complete)
        for layer in report.layers:
            self.assertEqual(layer.attempts[0].result, MOUNT_ERROR)


class TimeoutVerificationTests(BackfillTestBase):
    def test_timeout_but_server_completed_mount_is_verified_good(self):
        digest, size = self.layers["shared"]
        self.remove_all_target_layers()

        def handler(transport, target, d, source):
            if d == digest:
                # Registry finished the mount server-side even though the
                # client gave up waiting for the response.
                transport.add_blob(target, d, size)
                return MountResult(outcome=MOUNT_TIMEOUT,
                                   detail="挂载请求超时: read timed out")
            return None

        self.transport.mount_handler = handler
        report = self.run_backfill([GOLDEN])
        shared = self.layer_report(report, "shared")
        self.assertEqual(shared.status, LAYER_PRESENT)
        self.assertEqual(shared.resolved_by, "verified-after-mount-timeout")
        self.assertEqual(shared.mounted_from, GOLDEN)
        self.assertTrue(
            any(a.result == MOUNT_TIMEOUT for a in shared.attempts)
        )

    def test_timeout_still_missing_tries_next_source(self):
        self.seed_source(GOLDEN, "shared", "amd64-only", "arm64-only")
        self.remove_all_target_layers()
        shared_digest = self.layers["shared"][0]

        def handler(transport, target, digest, source):
            if source == "images/slow" and digest == shared_digest:
                return MountResult(outcome=MOUNT_TIMEOUT,
                                   detail="挂载请求超时")
            return None

        self.transport.mount_handler = handler
        report = self.run_backfill(["images/slow", GOLDEN])
        self.assertTrue(report.complete)
        shared = self.layer_report(report, "shared")
        sources_tried = [a.source for a in shared.attempts]
        self.assertEqual(sources_tried, ["images/slow", GOLDEN])
        self.assertEqual(shared.mounted_from, GOLDEN)


class IncompleteReportTests(BackfillTestBase):
    def test_all_sources_without_blob_produce_failure_list(self):
        self.remove_all_target_layers()
        report = self.run_backfill([EMPTY_SRC])
        self.assertFalse(report.complete)
        payload = report.to_dict()
        self.assertEqual(payload["summary"]["failed"], 3)
        self.assertEqual(len(payload["failures"]), 3)
        for failure in payload["failures"]:
            self.assertTrue(failure["digest"].startswith("sha256:"))
            self.assertIsInstance(failure["reasons"], list)
            self.assertTrue(failure["reasons"])
        # Every missing-source mount became a cancelled session.
        self.assertEqual(len(self.transport.delete_calls), 3)

    def test_empty_sources_reports_reason(self):
        self.remove_all_target_layers()
        report = self.run_backfill([])
        self.assertFalse(report.complete)
        self.assertTrue(
            any("未提供任何候选源" in " ".join(layer.reasons)
                for layer in report.layers)
        )

    def test_wrong_size_after_mount_fails_verification(self):
        digest, size = self.layers["shared"]
        self.remove_all_target_layers()
        self.seed_source(GOLDEN, "shared")

        def handler(transport, target, d, source):
            if d == digest:
                # "Mount" links a corrupted/truncated blob with wrong size.
                transport.add_blob(target, d, size - 3)
                return MountResult(outcome="mounted", status_code=201)
            return None

        self.transport.mount_handler = handler
        report = self.run_backfill([GOLDEN])
        shared = self.layer_report(report, "shared")
        self.assertEqual(shared.status, LAYER_FAILED)
        self.assertTrue(any("大小与清单不符" in r for r in shared.reasons))

    def test_descriptor_without_size_cannot_be_verified(self):
        # Inject a child whose layer descriptor omits size.
        child = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": self.fixture["configs"]["amd64"][0],
                "size": self.fixture["configs"]["amd64"][1],
            },
            "layers": [
                {"mediaType": "application/vnd.oci.image.layer.v1.tar",
                 "digest": "sha256:" + "9" * 64}
            ],
        }
        child_digest = "sha256:" + "8" * 64
        self.transport.add_manifest(TARGET, child_digest, child, child_digest)
        index = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [{
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": child_digest,
                "size": 1,
                "platform": {"os": "linux", "architecture": "amd64"},
            }],
        }
        index_digest = "sha256:" + "7" * 64
        self.transport.add_manifest(TARGET, index_digest, index, index_digest)
        report = self.service.backfill(
            f"localhost:5000/{TARGET}@{index_digest}", []
        )
        self.assertFalse(report.complete)
        self.assertIn("缺少合法 size", report.layers[0].reasons[0])


class ConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.transport = FakeBackfillTransport()
        self.service = BackfillService(SETTINGS, transport=self.transport)
        self.ref = parse_image_reference(
            "localhost:5000/images/target@" + "sha256:" + "a" * 64
        )

    def _missing_layer(self, digest: str, size: int = 42) -> tuple[str, int]:
        # No blob in target -> initial HEAD is 404.
        return digest, size

    def test_same_layer_calls_are_serialized(self):
        digest = "sha256:" + "1" * 64
        size = 128
        self.transport.add_blob(GOLDEN, digest, size)

        active = 0
        max_active = 0
        handler_calls = 0
        state_lock = threading.Lock()
        entered = threading.Event()

        def handler(transport, target, d, source):
            nonlocal active, max_active, handler_calls
            with state_lock:
                active += 1
                handler_calls += 1
                max_active = max(max_active, active)
            entered.set()
            time.sleep(0.25)
            # Default emulation would also mount; do it explicitly.
            transport.add_blob(target, d, size)
            with state_lock:
                active -= 1
            return MountResult(outcome="mounted", status_code=201)

        self.transport.mount_handler = handler

        def worker():
            return self.service._repair_layer(
                self.ref, digest, size, [GOLDEN]
            )

        start = time.monotonic()
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(worker)
            self.assertTrue(entered.wait(1.0))
            f2 = pool.submit(worker)
            r1 = f1.result(timeout=5)
            r2 = f2.result(timeout=5)
        elapsed = time.monotonic() - start

        self.assertEqual(r1.status, LAYER_PRESENT)
        self.assertEqual(r2.status, LAYER_PRESENT)
        self.assertEqual(max_active, 1, "same target layer must be exclusive")
        # The second caller found the blob after the first holder finished
        # and never invoked the mount handler itself.
        self.assertEqual(handler_calls, 1)
        self.assertGreaterEqual(elapsed, 0.2)
        # Exactly one request needed to mount; the other observed the blob
        # after acquiring the lock and reported the concurrent resolution.
        outcomes = {r.resolved_by for r in (r1, r2)}
        self.assertIn("cross-repository-mount", outcomes)
        self.assertIn("concurrent-request", outcomes)

    def test_different_layers_run_in_parallel(self):
        d1 = "sha256:" + "2" * 64
        d2 = "sha256:" + "3" * 64
        for d in (d1, d2):
            self.transport.add_blob(GOLDEN, d, 10)

        both_entered = threading.Event()
        state_lock = threading.Lock()
        seen: set[str] = set()

        def handler(transport, target, digest, source):
            with state_lock:
                seen.add(digest)
                if len(seen) == 2:
                    both_entered.set()
            # Block until both mount calls are in flight simultaneously.
            self.assertTrue(both_entered.wait(2.0))
            transport.add_blob(target, digest, 10)
            return MountResult(outcome="mounted", status_code=201)

        self.transport.mount_handler = handler

        def worker(digest):
            return self.service._repair_layer(self.ref, digest, 10, [GOLDEN])

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(worker, d1)
            f2 = pool.submit(worker, d2)
            r1 = f1.result(timeout=5)
            r2 = f2.result(timeout=5)

        self.assertEqual(r1.status, LAYER_PRESENT)
        self.assertEqual(r2.status, LAYER_PRESENT)
        self.assertTrue(both_entered.is_set(),
                        "different layers must not block each other")

    def test_same_digest_different_repositories_use_different_locks(self):
        digest = "sha256:" + "4" * 64
        other_ref = parse_image_reference(
            "localhost:5000/images/other@" + "sha256:" + "b" * 64
        )
        self.transport.add_blob(GOLDEN, digest, 10)
        self.transport.add_blob(GOLDEN, digest, 10)

        both_entered = threading.Event()
        state_lock = threading.Lock()
        repos_seen: set[str] = set()

        def handler(transport, target, d, source):
            with state_lock:
                repos_seen.add(target)
                if len(repos_seen) == 2:
                    both_entered.set()
            self.assertTrue(both_entered.wait(2.0))
            transport.add_blob(target, d, 10)
            return MountResult(outcome="mounted", status_code=201)

        self.transport.mount_handler = handler
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(
                self.service._repair_layer, self.ref, digest, 10, [GOLDEN]
            )
            f2 = pool.submit(
                self.service._repair_layer, other_ref, digest, 10, [GOLDEN]
            )
            self.assertEqual(f1.result(timeout=5).status, LAYER_PRESENT)
            self.assertEqual(f2.result(timeout=5).status, LAYER_PRESENT)
        self.assertTrue(both_entered.is_set())

    def test_failed_first_attempt_does_not_cache_failure(self):
        digest = "sha256:" + "5" * 64
        size = 77
        self.transport.add_blob(GOLDEN, digest, size)

        bad = self.service._repair_layer(
            self.ref, digest, size, [EMPTY_SRC]
        )
        self.assertEqual(bad.status, LAYER_FAILED)
        self.assertNotIn(digest, self.transport.blobs.get(TARGET, {}))

        # A later request with its own healthy source must still succeed:
        # no negative cache exists.
        good = self.service._repair_layer(
            self.ref, digest, size, [GOLDEN]
        )
        self.assertEqual(good.status, LAYER_PRESENT)
        self.assertEqual(good.mounted_from, GOLDEN)

    def test_waiter_after_failed_holder_tries_its_own_sources(self):
        """Concurrent version of the no-negative-cache requirement."""
        digest = "sha256:" + "6" * 64
        size = 88
        self.transport.add_blob(GOLDEN, digest, size)

        holder_in_mount = threading.Event()
        release_holder = threading.Event()
        holder_done = threading.Event()

        def handler(transport, target, d, source):
            if source == EMPTY_SRC:
                holder_in_mount.set()
                release_holder.wait(2.0)
                return MountResult(
                    outcome="upload_session", status_code=202,
                    location="http://registry.invalid/session-x",
                )
            transport.add_blob(target, d, size)
            return MountResult(outcome="mounted", status_code=201)

        self.transport.mount_handler = handler

        def failing_holder():
            try:
                return self.service._repair_layer(
                    self.ref, digest, size, [EMPTY_SRC]
                )
            finally:
                holder_done.set()

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(failing_holder)
            self.assertTrue(holder_in_mount.wait(1.0))
            # Holder still inside its critical section; queue the waiter.
            f2 = pool.submit(
                self.service._repair_layer, self.ref, digest, size, [GOLDEN]
            )
            time.sleep(0.15)
            release_holder.set()
            r1 = f1.result(timeout=5)
            r2 = f2.result(timeout=5)

        self.assertEqual(r1.status, LAYER_FAILED)
        self.assertEqual(r2.status, LAYER_PRESENT)
        self.assertEqual(r2.mounted_from, GOLDEN)
        # Waiter must have performed its own mount after the holder failed.
        self.assertIn(
            (TARGET, digest, GOLDEN), self.transport.mount_calls
        )


class KeyedLocksMapTests(unittest.TestCase):
    def test_distinct_keys_get_distinct_locks(self):
        locks = KeyedLayerLocks()
        with locks._guard:
            a = locks._locks.setdefault(("r", "repo", "d1"), threading.Lock())
            b = locks._locks.setdefault(("r", "repo", "d2"), threading.Lock())
        self.assertIsNot(a, b)


if __name__ == "__main__":
    unittest.main()
