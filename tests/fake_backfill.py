"""Scripted in-memory transport for backfill unit tests.

Mirrors the observable Distribution v2 semantics the backfill service relies
on: per-repository blob presence, cross-repository mounts that either link a
blob (201) or open an upload session (202), and cancellable sessions.

Tests can install a low-level ``mount_handler`` to script timeouts, server
side delayed mounts and concurrency barriers.
"""

from __future__ import annotations

import itertools
import os
import sys
import threading
from dataclasses import dataclass, field
from typing import Any, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.admission.backfill import (  # noqa: E402
    CancelResult,
    HeadBlobResult,
    MOUNT_MOUNTED,
    MOUNT_SESSION,
    MountResult,
)
from app.admission.registry import ManifestNotFound  # noqa: E402


@dataclass
class FakeBackfillTransport:
    # repo -> reference -> (manifest dict, digest to report)
    manifests: dict[str, dict[str, tuple[dict[str, Any], str]]] = field(
        default_factory=dict
    )
    # repo -> digest -> size (presence == key in the dict)
    blobs: dict[str, dict[str, int]] = field(default_factory=dict)

    # Optional scripted HEAD failures: (repo, digest) -> detail string.
    head_errors: dict[tuple[str, str], str] = field(default_factory=dict)

    # If set, invoked for every mount; returning None falls back to the
    # default emulation.  Signature (target, digest, source) -> MountResult|None
    mount_handler: Callable[..., MountResult | None] | None = None

    # Observability.
    head_calls: list[tuple[str, str]] = field(default_factory=list)
    mount_calls: list[tuple[str, str, str]] = field(default_factory=list)
    delete_calls: list[str] = field(default_factory=list)
    deleted_sessions: set[str] = field(default_factory=set)

    _session_counter: Any = field(
        default_factory=itertools.count, repr=False, compare=False
    )
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    # -- helpers used by tests/fixtures -----------------------------------

    def add_manifest(
        self, repository: str, reference: str, manifest: dict, digest: str
    ) -> None:
        self.manifests.setdefault(repository, {})[reference] = (manifest, digest)

    def add_blob(self, repository: str, digest: str, size: int) -> None:
        self.blobs.setdefault(repository, {})[digest] = size

    def remove_blob(self, repository: str, digest: str) -> None:
        self.blobs.get(repository, {}).pop(digest, None)

    def reset_call_log(self) -> None:
        self.head_calls.clear()
        self.mount_calls.clear()
        self.delete_calls.clear()

    # -- BackfillTransport protocol ---------------------------------------

    def get_manifest(
        self, repository: str, reference: str
    ) -> tuple[dict[str, Any], str]:
        entry = self.manifests.get(repository, {}).get(reference)
        if entry is None:
            raise ManifestNotFound(f"清单不存在: {repository}:{reference}")
        return entry

    def head_blob(self, repository: str, digest: str) -> HeadBlobResult:
        self.head_calls.append((repository, digest))
        error = self.head_errors.get((repository, digest))
        if error is not None:
            return HeadBlobResult(state="error", detail=error)
        size = self.blobs.get(repository, {}).get(digest)
        if size is None:
            return HeadBlobResult(state="missing", detail="目标返回 HTTP 404")
        return HeadBlobResult(state="present", size=size, digest=digest)

    def post_mount(
        self, target_repository: str, digest: str, source_repository: str
    ) -> MountResult:
        self.mount_calls.append(
            (target_repository, digest, source_repository)
        )
        if self.mount_handler is not None:
            scripted = self.mount_handler(
                self, target_repository, digest, source_repository
            )
            if scripted is not None:
                return scripted

        # Default Distribution-like emulation: a source that links the blob
        # makes the mount succeed and links it into the target; otherwise an
        # upload session (202) is started.
        source_size = self.blobs.get(source_repository, {}).get(digest)
        if source_size is not None:
            with self._lock:
                self.blobs.setdefault(target_repository, {})[digest] = source_size
            return MountResult(
                outcome=MOUNT_MOUNTED,
                status_code=201,
                location=f"/v2/{target_repository}/blobs/{digest}",
                digest=digest,
            )

        session_id = str(next(self._session_counter))
        location = (
            f"http://registry.invalid/v2/{target_repository}"
            f"/blobs/uploads/session-{session_id}"
        )
        return MountResult(
            outcome=MOUNT_SESSION, status_code=202, location=location
        )

    def delete_session(self, location: str) -> CancelResult:
        self.delete_calls.append(location)
        if location and location.endswith("cancel-fails"):
            return CancelResult(cancelled=False, status_code=500,
                                detail="取消上传会话返回 HTTP 500")
        self.deleted_sessions.add(location)
        return CancelResult(cancelled=True, status_code=204)


# ---------------------------------------------------------------------------
# Deterministic OCI index fixture builder
# ---------------------------------------------------------------------------

OCI_INDEX = "application/vnd.oci.image.index.v1+json"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar"


def digest_of(data: bytes) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(data).hexdigest()


def canonical(payload: dict[str, Any]) -> bytes:
    import json

    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()


def make_layer_bytes(name: str) -> bytes:
    # Deterministic, content-addressed distinct bytes per logical layer.
    return (f"layer-blob::{name}::").encode("utf-8") + name.encode() * 16


def install_index_fixture(
    transport: "FakeBackfillTransport",
    repository: str,
    *,
    arches: tuple[str, ...] = ("amd64", "arm64"),
    shared_layers: tuple[str, ...] = ("shared",),
    unique_layers: bool = True,
    broken_config_for: str | None = None,
) -> dict[str, Any]:
    """Install a multi-arch index with shared + per-arch layers.

    Everything (manifests, configs, layers) starts present in *repository*.
    Returns the index digest and ``layers`` name -> (digest, size).
    """
    layer_info: dict[str, tuple[str, int]] = {}

    def layer_descriptor(name: str) -> dict[str, Any]:
        if name not in layer_info:
            data = make_layer_bytes(name)
            layer_info[name] = (digest_of(data), len(data))
        digest, size = layer_info[name]
        return {"mediaType": OCI_LAYER, "digest": digest, "size": size}

    descriptors: list[dict[str, Any]] = []
    configs: dict[str, tuple[str, int]] = {}
    for arch in arches:
        layer_descriptors = [layer_descriptor(n) for n in shared_layers]
        if unique_layers:
            layer_descriptors.append(layer_descriptor(f"{arch}-only"))

        config_blob = canonical({"architecture": arch, "os": "linux"})
        config_digest = digest_of(config_blob)
        configs[arch] = (config_digest, len(config_blob))

        child = {
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST,
            "config": {
                "mediaType": OCI_CONFIG,
                "digest": config_digest,
                "size": len(config_blob),
            },
            "layers": layer_descriptors,
        }
        child_digest = digest_of(canonical(child))
        transport.add_manifest(repository, child_digest, child, child_digest)
        if broken_config_for != arch:
            transport.add_blob(repository, config_digest, len(config_blob))
        for descriptor in layer_descriptors:
            transport.add_blob(
                repository, descriptor["digest"], descriptor["size"]
            )
        descriptors.append(
            {
                "mediaType": OCI_MANIFEST,
                "digest": child_digest,
                "size": len(canonical(child)),
                "platform": {"os": "linux", "architecture": arch},
            }
        )

    index = {
        "schemaVersion": 2,
        "mediaType": OCI_INDEX,
        "manifests": descriptors,
    }
    index_digest = digest_of(canonical(index))
    transport.add_manifest(repository, index_digest, index, index_digest)
    return {
        "index_digest": index_digest,
        "layers": layer_info,
        "configs": configs,
        "index": index,
    }
