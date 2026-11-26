#!/usr/bin/env python3
"""Seed the local Docker registry with OCI multi-arch test fixtures.

Pure standard library: talks the Distribution v2 API over HTTP with
urllib.  Run after `docker compose up`::

    python3 scripts/seed_registry.py seed
    python3 scripts/seed_registry.py retag-demo   # move demo:v1 to a new index
    python3 scripts/seed_registry.py reset-demo   # move demo:v1 back

Fixtures (all under localhost:5000):

* apps/demo:v1 / apps/demo:v2 - full indexes with linux/amd64 + linux/arm64
  children (v1 and v2 contain different manifests so the index digests
  differ; used to demonstrate tag re-pointing).
* apps/arm-only:v1 - index containing only the arm64 child; a default
  admission (both platforms) is denied for the missing amd64 child.
* apps/broken:v1 - index listing both platforms, but the amd64 child
  manifest is deleted afterwards, so fetching it by digest returns 404.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:5000").rstrip("/")

OCI_INDEX = "application/vnd.oci.image.index.v1+json"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar"
OCTET_STREAM = "application/octet-stream"


def digest_of(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def request(method: str, url: str, data: bytes | None = None,
            headers: dict[str, str] | None = None) -> "urllib.response.addinfourl":
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=headers or {})
    try:
        return urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        raise RuntimeError(
            f"{method} {url} -> HTTP {exc.code}: {body}"
        ) from exc


def canonical(payload: dict) -> bytes:
    # Stable encoding; the registry hashes the exact received bytes.
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()


class RepoPusher:
    def __init__(self, name: str):
        self.name = name
        self._blob_cache: dict[str, bytes] = {}

    def upload_blob(self, data: bytes) -> dict:
        digest = digest_of(data)
        if digest in self._blob_cache:
            return {"mediaType": OCTET_STREAM, "digest": digest, "size": len(data)}
        resp = request("POST", f"{REGISTRY_URL}/v2/{self.name}/blobs/uploads/")
        location = resp.headers["Location"]
        upload_url = urllib.parse.urljoin(REGISTRY_URL, location)
        sep = "&" if "?" in upload_url else "?"
        put_url = f"{upload_url}{sep}digest={digest}"
        request("PUT", put_url, data=data,
                headers={"Content-Type": OCTET_STREAM})
        self._blob_cache[digest] = data
        return {"mediaType": OCTET_STREAM, "digest": digest, "size": len(data)}

    def put_manifest(self, reference: str, media_type: str,
                     payload: dict) -> str:
        body = canonical(payload)
        digest = digest_of(body)
        request(
            "PUT",
            f"{REGISTRY_URL}/v2/{self.name}/manifests/{reference}",
            data=body,
            headers={"Content-Type": media_type},
        )
        return digest

    def delete_manifest(self, digest: str) -> None:
        request("DELETE",
                f"{REGISTRY_URL}/v2/{self.name}/manifests/{digest}")


def empty_layer_bytes() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w"):
        pass
    return buf.getvalue()


def build_child(pusher: RepoPusher, arch: str, layer: bytes) -> tuple[dict, str]:
    config = canonical({
        "architecture": arch,
        "os": "linux",
        "config": {},
        "rootfs": {"type": "layers", "diff_ids": [digest_of(layer)]},
    })
    config_desc = pusher.upload_blob(config)
    config_desc["mediaType"] = OCI_CONFIG
    layer_desc = pusher.upload_blob(layer)
    layer_desc["mediaType"] = OCI_LAYER
    manifest = {
        "schemaVersion": 2,
        "mediaType": OCI_MANIFEST,
        "config": config_desc,
        "layers": [layer_desc],
    }
    digest = pusher.put_manifest(
        manifest_digest_tag(manifest), OCI_MANIFEST, manifest
    )
    descriptor = {
        "mediaType": OCI_MANIFEST,
        "digest": digest,
        "size": len(canonical(manifest)),
        "platform": {"os": "linux", "architecture": arch},
    }
    return descriptor, digest


def manifest_digest_tag(manifest: dict) -> str:
    # Children are pushed by digest first (required so the index can
    # reference them); digest is recomputed identically on the second call.
    return digest_of(canonical(manifest))


def push_index(repo: str, architectures: list[str], layer: bytes,
               delete_arch: str | None = None) -> tuple[str, dict[str, str]]:
    pusher = RepoPusher(repo)
    descriptors = []
    child_digests: dict[str, str] = {}
    for arch in architectures:
        descriptor, child_digest = build_child(pusher, arch, layer)
        descriptors.append(descriptor)
        child_digests[arch] = child_digest

    index = {
        "schemaVersion": 2,
        "mediaType": OCI_INDEX,
        "manifests": descriptors,
    }
    index_digest = pusher.put_manifest(
        digest_of(canonical(index)), OCI_INDEX, index
    )
    if delete_arch is not None:
        # Simulate a child manifest that the index promises but can no longer
        # be fetched by digest (registry delete is enabled).  Callers that
        # also want a tag pointing at this index must create the tag *first*:
        # registry 2.8 re-validates referenced children on manifest PUT.
        pusher.delete_manifest(child_digests[delete_arch])
    return index_digest, child_digests


def tag_manifest(repo: str, tag: str, index_digest: str) -> None:
    resp = request("GET",
                   f"{REGISTRY_URL}/v2/{repo}/manifests/{index_digest}",
                   headers={"Accept": OCI_INDEX})
    payload = json.loads(resp.read())
    request(
        "PUT",
        f"{REGISTRY_URL}/v2/{repo}/manifests/{tag}",
        data=canonical(payload),
        headers={"Content-Type": OCI_INDEX},
    )


def cmd_seed(_args: argparse.Namespace) -> None:
    layer = empty_layer_bytes()
    summary = []

    # Two distinct indexes for apps/demo so re-pointing a tag changes digest.
    digest_v1, _ = push_index("apps/demo", ["amd64", "arm64"], layer)
    # A different layer blob makes v2's children (and thus the index) differ.
    layer_v2 = layer + b"\x00"
    digest_v2, _ = push_index("apps/demo", ["amd64", "arm64"], layer_v2)
    tag_manifest("apps/demo", "v1", digest_v1)
    tag_manifest("apps/demo", "v2", digest_v2)
    summary.append(("apps/demo:v1", digest_v1))
    summary.append(("apps/demo:v2", digest_v2))

    digest_arm, _ = push_index("apps/arm-only", ["arm64"], layer)
    tag_manifest("apps/arm-only", "v1", digest_arm)
    summary.append(("apps/arm-only:v1", digest_arm))

    # Broken-child fixture: create the tag while every child exists, then
    # delete the amd64 child manifest by digest so the index still resolves
    # but that child returns 404.
    digest_broken, broken_children = push_index(
        "apps/broken", ["amd64", "arm64"], layer
    )
    tag_manifest("apps/broken", "v1", digest_broken)
    RepoPusher("apps/broken").delete_manifest(broken_children["amd64"])
    summary.append(("apps/broken:v1", digest_broken))

    print("seeded fixtures at", REGISTRY_URL)
    for ref, digest in summary:
        print(f"  {ref:24s} -> {digest}")


def cmd_retag(_args: argparse.Namespace) -> None:
    # Point demo:v1 at the v2 index (simulates a tag re-point).
    resp = request("GET",
                   f"{REGISTRY_URL}/v2/apps/demo/manifests/v2",
                   headers={"Accept": OCI_INDEX})
    digest_v2 = resp.headers["Docker-Content-Digest"]
    tag_manifest("apps/demo", "v1", digest_v2)
    print(f"apps/demo:v1 re-pointed to {digest_v2}")


def cmd_reset(args: argparse.Namespace) -> None:
    # Re-seeding is idempotent (identical bytes keep identical digests) and
    # restores the demo:v1 tag to its original multi-arch index.
    cmd_seed(args)


# ---------------------------------------------------------------------------
# Backfill fixture: shared layer across platforms + one per-arch layer
# ---------------------------------------------------------------------------


def deterministic_tar_layer(filename: str, content: bytes) -> bytes:
    """A byte-for-byte deterministic uncompressed tar layer."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.USTAR_FORMAT) as tf:
        info = tarfile.TarInfo(name=filename)
        info.size = len(content)
        info.mtime = 0
        info.uid = 0
        info.gid = 0
        info.uname = ""
        info.gname = ""
        tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def push_backfill_index(
    repo: str, layer_bytes: dict[str, bytes]
) -> tuple[str, dict[str, str]]:
    """Push an amd64+arm64 index whose children share the 'common' layer."""
    pusher = RepoPusher(repo)
    descriptors = []
    child_digests: dict[str, str] = {}
    for arch in ("amd64", "arm64"):
        ordered_names = ["common", f"{arch}-only"]
        layer_descriptors = []
        diff_ids = []
        for name in ordered_names:
            data = layer_bytes[name]
            descriptor = pusher.upload_blob(data)
            descriptor["mediaType"] = OCI_LAYER
            layer_descriptors.append(descriptor)
            diff_ids.append(digest_of(data))

        config = canonical(
            {
                "architecture": arch,
                "os": "linux",
                "config": {},
                "rootfs": {"type": "layers", "diff_ids": diff_ids},
            }
        )
        config_descriptor = pusher.upload_blob(config)
        config_descriptor["mediaType"] = OCI_CONFIG

        child = {
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST,
            "config": config_descriptor,
            "layers": layer_descriptors,
        }
        child_digest = pusher.put_manifest(
            digest_of(canonical(child)), OCI_MANIFEST, child
        )
        child_digests[arch] = child_digest
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
    index_digest = pusher.put_manifest(
        digest_of(canonical(index)), OCI_INDEX, index
    )
    return index_digest, child_digests


def cmd_seed_backfill(_args: argparse.Namespace) -> None:
    layer_bytes = {
        "common": deterministic_tar_layer(
            "common.txt", b"shared-layer-across-platforms-v1\n"
        ),
        "amd64-only": deterministic_tar_layer("arch.txt", b"amd64-layer\n"),
        "arm64-only": deterministic_tar_layer("arch.txt", b"arm64-layer\n"),
    }

    golden_digest, _ = push_backfill_index("images/golden", layer_bytes)
    target_digest, _ = push_backfill_index("images/target", layer_bytes)
    tag_manifest("images/golden", "v1", golden_digest)
    tag_manifest("images/target", "v1", target_digest)

    # A candidate source that only has two of the three layers: mounting the
    # amd64-only layer from it yields an upload session (202), which the
    # backfill API must cancel before trying images/golden.
    partial = RepoPusher("images/partial")
    for name in ("common", "arm64-only"):
        partial.upload_blob(layer_bytes[name])

    if golden_digest != target_digest:
        # Same canonical bytes must produce the same index digest.
        print(
            f"warning: golden {golden_digest} != target {target_digest}",
            file=sys.stderr,
        )

    print("seeded backfill fixtures at", REGISTRY_URL)
    print(f"  images/target:v1 / @{target_digest}")
    print(f"  images/golden:v1 / @{golden_digest} (all 3 layers)")
    print("  images/partial        (common + arm64 layer only)")
    print()
    print("next:")
    print("  python3 scripts/seed_registry.py break-layers")
    print(
        "  python3 scripts/backfill.py "
        f"--target localhost:5000/images/target@{target_digest} \\"
    )
    print("      --source images/partial --source images/golden")


def head_blob_status(repo: str, digest: str) -> int:
    request_obj = urllib.request.Request(
        f"{REGISTRY_URL}/v2/{repo}/blobs/{digest}", method="HEAD"
    )
    try:
        with urllib.request.urlopen(request_obj, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as exc:
        return exc.code


def collect_layer_digests(repo: str, index_digest: str) -> list[str]:
    _resp, _headers, index_body = _request_bytes(
        "GET",
        f"{REGISTRY_URL}/v2/{repo}/manifests/{index_digest}",
        headers={"Accept": OCI_INDEX},
    )
    index = json.loads(index_body)
    digests: set[str] = set()
    for descriptor in index.get("manifests", []):
        child_digest = descriptor["digest"]
        _r, _h, child_body = _request_bytes(
            "GET",
            f"{REGISTRY_URL}/v2/{repo}/manifests/{child_digest}",
            headers={"Accept": OCI_MANIFEST},
        )
        child = json.loads(child_body)
        for layer in child.get("layers", []):
            digests.add(layer["digest"])
    return sorted(digests)


def _request_bytes(method, url, data=None, headers=None):
    req = urllib.request.Request(
        url, data=data, method=method, headers=headers or {}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"{method} {url} -> HTTP {exc.code}: {body}") from exc


def cmd_break_layers(args: argparse.Namespace) -> None:
    """Remove target repository layer links to simulate 404 layers.

    Cross-repository mounts repair exactly this damage without moving layer
    content.  With ``--fs-root`` files are removed directly (local registry
    binary / bind mount); otherwise ``docker compose exec`` is used against
    the registry service.
    """
    repo = args.repo
    index_digest = args.index_digest
    if not index_digest:
        resp = request(
            "GET",
            f"{REGISTRY_URL}/v2/{repo}/manifests/v1",
            headers={"Accept": OCI_INDEX},
        )
        index_digest = resp.headers["Docker-Content-Digest"]

    digests = collect_layer_digests(repo, index_digest)
    v2_root = args.container_root
    link_paths = [
        f"{v2_root}/repositories/{repo}/_layers/sha256/"
        f"{digest.split(':', 1)[1]}/link"
        for digest in digests
    ]

    if args.fs_root:
        host_paths = [
            os.path.join(
                args.fs_root,
                "repositories",
                repo,
                "_layers",
                "sha256",
                digest.split(":", 1)[1],
                "link",
            )
            for digest in digests
        ]
        for path in host_paths:
            try:
                os.remove(path)
                print("removed", path)
            except FileNotFoundError:
                print("already absent", path)
    else:
        compose_file = (
            args.compose_file
            or Path(__file__).resolve().parent.parent / "docker-compose.yml"
        )
        command = [
            "docker", "compose", "-f", str(compose_file),
            "exec", "-T", "-u", "0", args.service,
            "rm", "-f", *link_paths,
        ]
        print("+", " ".join(command))
        subprocess.run(command, check=True)

    print(f"\nlayer HEAD status in {repo} after link removal:")
    for digest in digests:
        status = head_blob_status(repo, digest)
        print(f"  HTTP {status}  {digest}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("seed")
    sub.add_parser("retag-demo")
    sub.add_parser("reset-demo")
    sub.add_parser("seed-backfill")

    break_parser = sub.add_parser(
        "break-layers",
        help="remove target repo layer links to simulate layer 404s",
    )
    break_parser.add_argument("--repo", default="images/target")
    break_parser.add_argument("--index-digest", default=None)
    break_parser.add_argument(
        "--fs-root",
        default=None,
        help="local .../docker/registry/v2 directory for direct removal "
             "(standalone registry binary / bind mount)",
    )
    break_parser.add_argument(
        "--container-root",
        default="/var/lib/registry/docker/registry/v2",
        help="in-container v2 storage root",
    )
    break_parser.add_argument("--service", default="registry")
    break_parser.add_argument("--compose-file", default=None)

    args = parser.parse_args()

    handlers = {
        "seed": cmd_seed,
        "retag-demo": cmd_retag,
        "reset-demo": cmd_reset,
        "seed-backfill": cmd_seed_backfill,
        "break-layers": cmd_break_layers,
    }
    try:
        handlers[args.command](args)
    except Exception as exc:  # noqa: BLE001 - CLI surface
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
