"""Cross-repository blob backfill for OCI indexes whose layer links are broken.

Scenario
--------
A node can still fetch the image *index* (and the direct child manifests and
config blobs are present in the target repository), but pulling a *layer*
returns HTTP 404.  Given the target index digest reference and an ordered list
of candidate source repositories on the same local registry, this module:

1. fetches the target index by digest and every direct child manifest;
2. verifies config blobs are already present in the target repository;
3. merges layer descriptors shared by several platforms by digest;
4. HEAD-checks every unique layer in the target repository and only for
   layers whose HEAD is exactly 404 tries a cross-repository mount
   (``POST /v2/<target>/blobs/uploads/?mount=<digest>&from=<source>``);
5. never downloads or uploads layer content and never changes tags/manifests;
6. cancels (DELETE) any upload session (HTTP 202) returned by a source and
   continues with the next source;
7. always re-verifies the target by digest + manifest-declared size, even
   after a mount timeout;
8. serializes concurrent callers in this process only for the same
   ``(registry, target repository, layer digest)`` key.  There is no shared
   success/failure cache: once the key lock is held the target is re-checked
   and, if still missing, the current request tries its *own* sources.

The module imports only the standard library at import time; the HTTP
transport uses :mod:`urllib` so the whole state machine is unit-testable
without third-party packages.
"""

from __future__ import annotations

import json
import socket
import threading
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Protocol

from .config import Settings
from .images import ImageReference, ImageReferenceError, parse_image_reference
from .registry import (
    ACCEPT_HEADER,
    INDEX_MEDIA_TYPES,
    MANIFEST_MEDIA_TYPES,
    ManifestNotFound,
    RegistryError,
)

SHA256_PREFIX = "sha256:"

# Final per-layer states.
LAYER_PRESENT = "present"
LAYER_FAILED = "failed"

# Machine-readable outcomes of a single source mount attempt.
MOUNT_MOUNTED = "mounted"
MOUNT_SESSION = "upload_session"
MOUNT_TIMEOUT = "timeout"
MOUNT_ERROR = "error"


class BackfillRequestError(Exception):
    """A caller/registry-state error that should become a 4xx response."""

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


# ---------------------------------------------------------------------------
# Transport result objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HeadBlobResult:
    state: str  # "present" | "missing" | "error"
    size: int | None = None
    digest: str | None = None
    detail: str = ""

    @property
    def present(self) -> bool:
        return self.state == "present"

    @property
    def missing(self) -> bool:
        return self.state == "missing"


@dataclass(frozen=True)
class MountResult:
    outcome: str  # MOUNT_MOUNTED | MOUNT_SESSION | MOUNT_TIMEOUT | MOUNT_ERROR
    status_code: int | None = None
    location: str | None = None
    digest: str | None = None
    detail: str = ""


@dataclass(frozen=True)
class CancelResult:
    cancelled: bool
    status_code: int | None = None
    detail: str = ""


class BackfillTransport(Protocol):
    """Registry operations required by :class:`BackfillService`.

    Manifest reads raise :class:`ManifestNotFound` / :class:`RegistryError`;
    blob operations never raise for ordinary registry responses, they return
    result objects so per-layer failures can be collected.
    """

    def get_manifest(
        self, repository: str, reference: str
    ) -> tuple[dict[str, Any], str]: ...

    def head_blob(self, repository: str, digest: str) -> HeadBlobResult: ...

    def post_mount(
        self, target_repository: str, digest: str, source_repository: str
    ) -> MountResult: ...

    def delete_session(self, location: str) -> CancelResult: ...


# ---------------------------------------------------------------------------
# Keyed in-process locks
# ---------------------------------------------------------------------------


class KeyedLayerLocks:
    """One mutex per ``(registry, repository, digest)`` key.

    Different keys never block each other.  The map itself is guarded by a
    small meta-lock while fetching/creating the per-key lock.
    """

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._locks: dict[tuple[str, str, str], threading.Lock] = {}

    @contextmanager
    def acquire(self, key: tuple[str, str, str]) -> Iterator[None]:
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
        lock.acquire()
        try:
            yield
        finally:
            lock.release()


# ---------------------------------------------------------------------------
# urllib-based production transport (stdlib only)
# ---------------------------------------------------------------------------


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        return isinstance(reason, (TimeoutError, socket.timeout))
    return False


def _describe_url_error(exc: BaseException) -> str:
    if isinstance(exc, urllib.error.URLError):
        return f"{type(exc.reason).__name__}: {exc.reason}"
    return f"{type(exc).__name__}: {exc}"


def _optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class UrllibBackfillTransport:
    """Talks Distribution v2 over plain HTTP using :mod:`urllib`."""

    def __init__(self, settings: Settings, timeout: float | None = None):
        self._base = settings.registry_upstream.rstrip("/")
        self._timeout = settings.request_timeout if timeout is None else timeout

    # -- URL helpers -------------------------------------------------------

    def _manifest_url(self, repository: str, reference: str) -> str:
        return f"{self._base}/v2/{repository}/manifests/{reference}"

    def _blob_url(self, repository: str, digest: str) -> str:
        return f"{self._base}/v2/{repository}/blobs/{digest}"

    def _mount_url(
        self, target_repository: str, digest: str, source_repository: str
    ) -> str:
        query = urllib.parse.urlencode(
            {"mount": digest, "from": source_repository}
        )
        return f"{self._base}/v2/{target_repository}/blobs/uploads/?{query}"

    def _upstream_url_from_location(self, location: str) -> str:
        """Resolve a mount/session Location against the configured upstream.

        Registries commonly return an absolute URL whose host:port matches the
        request Host header; inside Compose that is already
        ``registry:5000``.  When the caller reached the registry through a
        different authority we rewrite the Location's authority to the
        configured upstream while preserving path and query.
        """
        parsed = urllib.parse.urlparse(location)
        if parsed.scheme and parsed.netloc and parsed.path.startswith("/v2/"):
            base = urllib.parse.urlparse(self._base)
            return urllib.parse.urlunparse(
                (
                    base.scheme,
                    base.netloc,
                    parsed.path,
                    "",
                    parsed.query,
                    "",
                )
            )
        return urllib.parse.urljoin(self._base + "/", location)

    # -- manifest ----------------------------------------------------------

    def get_manifest(
        self, repository: str, reference: str
    ) -> tuple[dict[str, Any], str]:
        request = urllib.request.Request(
            self._manifest_url(repository, reference),
            headers={"Accept": ACCEPT_HEADER},
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as resp:
                body = resp.read()
                digest = resp.headers.get("Docker-Content-Digest", "")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise ManifestNotFound(
                    f"清单不存在: {repository}:{reference}"
                ) from exc
            raise RegistryError(
                f"仓库返回 HTTP {exc.code}: {repository}:{reference}"
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise RegistryError(f"访问仓库失败: {_describe_url_error(exc)}") from exc

        if not digest.startswith(SHA256_PREFIX):
            raise RegistryError("仓库响应缺少合法的 Docker-Content-Digest 摘要头")
        try:
            payload = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise RegistryError("仓库返回的清单不是合法 JSON") from exc
        if not isinstance(payload, dict):
            raise RegistryError("仓库返回的清单结构非法")
        return payload, digest

    # -- blobs -------------------------------------------------------------

    def head_blob(self, repository: str, digest: str) -> HeadBlobResult:
        result = self._head_once(repository, digest)
        if result.state == "error" and (
            "HTTP 405" in result.detail or "HTTP 501" in result.detail
        ):
            return self._metadata_via_range_get(repository, digest)
        return result

    def _head_once(self, repository: str, digest: str) -> HeadBlobResult:
        request = urllib.request.Request(
            self._blob_url(repository, digest), method="HEAD"
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as resp:
                return HeadBlobResult(
                    state="present",
                    size=_optional_int(resp.headers.get("Content-Length")),
                    digest=resp.headers.get("Docker-Content-Digest"),
                )
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return HeadBlobResult(state="missing", detail="目标返回 HTTP 404")
            return HeadBlobResult(
                state="error", detail=f"HEAD 目标层返回 HTTP {exc.code}"
            )
        except (urllib.error.URLError, OSError) as exc:
            if _is_timeout(exc):
                detail = f"HEAD 请求超时: {_describe_url_error(exc)}"
            else:
                detail = f"HEAD 请求失败: {_describe_url_error(exc)}"
            return HeadBlobResult(state="error", detail=detail)

    def _metadata_via_range_get(
        self, repository: str, digest: str
    ) -> HeadBlobResult:
        """Fallback for registries that reject HEAD: ask for one byte.

        The response stream is closed before its body is read, so layer
        content is never downloaded.  ``Content-Range`` carries the total
        size (``bytes 0-0/12345``); a plain 200 exposes it via
        ``Content-Length``.
        """
        request = urllib.request.Request(
            self._blob_url(repository, digest),
            headers={"Range": "bytes=0-0"},
            method="GET",
        )
        response = None
        try:
            response = urllib.request.urlopen(request, timeout=self._timeout)
            status = response.status
            content_range = response.headers.get("Content-Range", "")
            total_size: int | None = None
            if "/" in content_range:
                total_size = _optional_int(content_range.rsplit("/", 1)[1])
            if total_size is None:
                total_size = _optional_int(
                    response.headers.get("Content-Length")
                )
            response.close()  # do not read the (potential) layer body
            return HeadBlobResult(
                state="present" if 200 <= status < 300 else "error",
                size=total_size,
                digest=response.headers.get("Docker-Content-Digest"),
                detail="" if 200 <= status < 300 else f"Range GET HTTP {status}",
            )
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return HeadBlobResult(state="missing", detail="目标返回 HTTP 404")
            return HeadBlobResult(
                state="error", detail=f"Range GET 返回 HTTP {exc.code}"
            )
        except (urllib.error.URLError, OSError) as exc:
            return HeadBlobResult(
                state="error",
                detail=f"Range GET 请求失败: {_describe_url_error(exc)}",
            )
        finally:
            if response is not None:
                response.close()

    # -- cross-repository mount -------------------------------------------

    def post_mount(
        self, target_repository: str, digest: str, source_repository: str
    ) -> MountResult:
        request = urllib.request.Request(
            self._mount_url(target_repository, digest, source_repository),
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as resp:
                status = resp.status
                location = resp.headers.get("Location")
                mounted_digest = resp.headers.get("Docker-Content-Digest")
        except urllib.error.HTTPError as exc:
            # A 202 is a success-class response and therefore not raised;
            # 4xx/5xx mount failures are surfaced as error outcomes.
            return MountResult(
                outcome=MOUNT_ERROR,
                status_code=exc.code,
                detail=f"挂载请求返回 HTTP {exc.code}",
            )
        except (urllib.error.URLError, OSError) as exc:
            if _is_timeout(exc):
                return MountResult(
                    outcome=MOUNT_TIMEOUT,
                    detail=f"挂载请求超时: {_describe_url_error(exc)}",
                )
            return MountResult(
                outcome=MOUNT_ERROR,
                detail=f"挂载请求失败: {_describe_url_error(exc)}",
            )

        if status in (200, 201):
            return MountResult(
                outcome=MOUNT_MOUNTED,
                status_code=status,
                location=location,
                digest=mounted_digest,
            )
        if status == 202:
            return MountResult(
                outcome=MOUNT_SESSION,
                status_code=status,
                location=location,
                detail="源仓库无法直接挂载，仓库返回了上传会话",
            )
        return MountResult(
            outcome=MOUNT_ERROR,
            status_code=status,
            detail=f"挂载请求返回意外状态 HTTP {status}",
        )

    # -- upload session cancellation --------------------------------------

    def delete_session(self, location: str) -> CancelResult:
        if not location:
            return CancelResult(
                cancelled=False, detail="202 响应缺少 Location，无法取消上传会话"
            )
        url = self._upstream_url_from_location(location)
        request = urllib.request.Request(url, method="DELETE")
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as resp:
                status = resp.status
            if 200 <= status < 300:
                return CancelResult(cancelled=True, status_code=status)
            return CancelResult(
                cancelled=False,
                status_code=status,
                detail=f"取消上传会话返回 HTTP {status}",
            )
        except urllib.error.HTTPError as exc:
            # 404 means the session is already gone, which is the state we
            # wanted; accept it as cancelled.
            if exc.code == 404:
                return CancelResult(cancelled=True, status_code=404)
            return CancelResult(
                cancelled=False,
                status_code=exc.code,
                detail=f"取消上传会话返回 HTTP {exc.code}",
            )
        except (urllib.error.URLError, OSError) as exc:
            return CancelResult(
                cancelled=False,
                detail=f"取消上传会话失败: {_describe_url_error(exc)}",
            )


# ---------------------------------------------------------------------------
# Report objects
# ---------------------------------------------------------------------------


@dataclass
class AttemptRecord:
    source: str
    result: str
    detail: str = ""
    session_cancelled: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"source": self.source, "result": self.result}
        if self.detail:
            data["detail"] = self.detail
        if self.session_cancelled is not None:
            data["sessionCancelled"] = self.session_cancelled
        return data


@dataclass
class LayerReport:
    digest: str
    size: int | None
    status: str = LAYER_FAILED
    already_present: bool = False
    mount_attempted: bool = False
    mounted_from: str | None = None
    resolved_by: str | None = None
    attempts: list[AttemptRecord] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "digest": self.digest,
            "size": self.size,
            "status": self.status,
            "alreadyPresent": self.already_present,
            "mountAttempted": self.mount_attempted,
            "mountedFrom": self.mounted_from,
            "resolvedBy": self.resolved_by,
            "attempts": [attempt.to_dict() for attempt in self.attempts],
            "reasons": list(self.reasons),
        }


@dataclass
class BackfillReport:
    target: str
    registry: str
    repository: str
    index_digest: str
    sources: list[str]
    layers: list[LayerReport]
    warnings: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return all(layer.status == LAYER_PRESENT for layer in self.layers)

    def _count(self, predicate) -> int:
        return sum(1 for layer in self.layers if predicate(layer))

    def to_dict(self) -> dict[str, Any]:
        present = self._count(lambda layer: layer.status == LAYER_PRESENT)
        mounted = self._count(lambda layer: layer.mounted_from is not None)
        already = self._count(lambda layer: layer.already_present)
        failed_layers = [
            {
                "digest": layer.digest,
                "size": layer.size,
                "reasons": layer.reasons,
            }
            for layer in self.layers
            if layer.status == LAYER_FAILED
        ]
        return {
            "target": self.target,
            "registry": self.registry,
            "repository": self.repository,
            "indexDigest": self.index_digest,
            "sources": list(self.sources),
            "complete": self.complete,
            "summary": {
                "layers": len(self.layers),
                "present": present,
                "alreadyPresent": already,
                "mounted": mounted,
                "failed": len(failed_layers),
            },
            "warnings": list(self.warnings),
            "layers": [layer.to_dict() for layer in self.layers],
            "failures": failed_layers,
        }


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class BackfillService:
    """Orchestrates planning, keyed locking, mounting and verification."""

    def __init__(
        self,
        settings: Settings,
        transport: BackfillTransport | None = None,
        locks: KeyedLayerLocks | None = None,
    ):
        self._settings = settings
        self._transport: BackfillTransport = transport or UrllibBackfillTransport(
            settings
        )
        self._locks = locks or KeyedLayerLocks()

    # -- request validation -----------------------------------------------

    def backfill(
        self, raw_target: str, raw_sources: list[str] | tuple[str, ...] | None
    ) -> BackfillReport:
        ref, sources, warnings = self._validate_request(raw_target, raw_sources)
        layers = self._load_plan(ref)

        layer_reports: list[LayerReport] = []
        # Stable digest order makes lock acquisition deterministic for
        # requests whose layer sets overlap (one lock is held at a time).
        for digest in sorted(layers):
            expected_size = layers[digest]
            layer_reports.append(
                self._repair_layer(ref, digest, expected_size, sources)
            )

        return BackfillReport(
            target=str(ref),
            registry=ref.registry,
            repository=ref.repository,
            index_digest=ref.digest or "",
            sources=list(sources),
            layers=layer_reports,
            warnings=warnings,
        )

    def _validate_request(
        self,
        raw_target: str | None,
        raw_sources: list[str] | tuple[str, ...] | None,
    ) -> tuple[ImageReference, list[str], list[str]]:
        if not isinstance(raw_target, str) or not raw_target.strip():
            raise BackfillRequestError("缺少目标 OCI 索引摘要引用 target", 422)
        try:
            ref = parse_image_reference(raw_target)
        except ImageReferenceError as exc:
            raise BackfillRequestError(
                f"目标引用无法解析: {exc} (收到 {raw_target!r})", 400
            ) from exc

        if not ref.digest:
            raise BackfillRequestError(
                "target 必须是带摘要的 OCI 索引引用 "
                f"(形如 {ref.registry}/{ref.repository}@sha256:<索引摘要>)，"
                "不接受标签，以免回补到变动中的清单",
                400,
            )
        if ref.registry not in self._settings.allowed_registries:
            raise BackfillRequestError(
                f"目标仓库的 Registry {ref.registry!r} 不在受支持的本地仓库 "
                f"{sorted(self._settings.allowed_registries)} 内",
                403,
            )

        if raw_sources is None:
            raw_sources = []
        if not isinstance(raw_sources, (list, tuple)):
            raise BackfillRequestError("sources 必须是候选源仓库名数组", 422)

        warnings: list[str] = []
        sources: list[str] = []
        seen: set[str] = set()
        for raw_source in raw_sources:
            if not isinstance(raw_source, str) or not raw_source.strip():
                raise BackfillRequestError(
                    f"候选源仓库名必须是非空字符串，收到 {raw_source!r}", 400
                )
            repository = self._normalize_source_repository(raw_source, ref)
            if repository == ref.repository:
                warnings.append(
                    f"候选源 {repository!r} 与目标仓库相同，已忽略："
                    "目标返回 404 的层不可能从目标自身挂载"
                )
                continue
            if repository not in seen:
                seen.add(repository)
                sources.append(repository)

        return ref, sources, warnings

    def _normalize_source_repository(
        self, raw_source: str, target: ImageReference
    ) -> str:
        source = raw_source.strip()
        try:
            parsed = parse_image_reference(source)
        except ImageReferenceError as exc:
            raise BackfillRequestError(
                f"候选源仓库名 {source!r} 无法解析: {exc}", 400
            ) from exc

        # A plain name ("apps/golden") parses to the default registry and is
        # used verbatim as a repository path.  A host-prefixed source must
        # point at one of the configured local registry aliases, otherwise it
        # is not "the same local Registry".
        if parsed.registry != "docker.io":
            if parsed.registry not in self._settings.allowed_registries:
                raise BackfillRequestError(
                    f"候选源 {source!r} 指向 {parsed.registry!r}，"
                    "回补只接受与目标同一台本地 Registry 上的仓库",
                    400,
                )
        return parsed.repository

    # -- plan: index + direct children + configs --------------------------

    def _load_plan(self, ref: ImageReference) -> dict[str, int | None]:
        """Return digest -> expected size for every unique layer.

        Raises :class:`BackfillRequestError` with status 404/409/502 when the
        documented preconditions are not met.
        """
        try:
            index, returned_digest = self._transport.get_manifest(
                ref.repository, ref.digest or ""
            )
        except ManifestNotFound as exc:
            raise BackfillRequestError(
                f"目标仓库 {ref.repository!r} 中找不到索引 {ref.digest}：{exc}",
                404,
            ) from exc
        except RegistryError as exc:
            raise BackfillRequestError(str(exc), 502) from exc

        if returned_digest != ref.digest:
            raise BackfillRequestError(
                f"目标索引摘要不一致: 请求 {ref.digest}, "
                f"仓库响应 {returned_digest}",
                502,
            )

        media_type = index.get("mediaType")
        descriptors = index.get("manifests")
        if media_type not in INDEX_MEDIA_TYPES or not isinstance(
            descriptors, list
        ):
            raise BackfillRequestError(
                f"{ref.digest} 指向的清单({media_type!r})不是 OCI 索引或 "
                "Docker manifest list，无法按平台收集直属子清单",
                409,
            )

        problems: list[str] = []
        # digest -> size declared by the owning manifest descriptor.
        layers: dict[str, int | None] = {}

        for position, descriptor in enumerate(descriptors):
            child_digest = self._descriptor_digest(
                descriptor, problems, f"索引第 {position} 个子清单描述符"
            )
            if child_digest is None:
                continue

            try:
                child, child_returned_digest = self._transport.get_manifest(
                    ref.repository, child_digest
                )
            except ManifestNotFound:
                problems.append(
                    f"直属子清单 {child_digest} 不在目标仓库（404）"
                )
                continue
            except RegistryError as exc:
                problems.append(f"读取直属子清单 {child_digest} 失败: {exc}")
                continue

            if child_returned_digest != child_digest:
                problems.append(
                    f"直属子清单摘要不一致: 索引记录 {child_digest}, "
                    f"仓库响应 {child_returned_digest}"
                )
            child_type = child.get("mediaType")
            if child_type not in MANIFEST_MEDIA_TYPES or not isinstance(
                child, dict
            ):
                problems.append(
                    f"直属子清单 {child_digest} 的类型 {child_type!r} 不受支持"
                    "（不接受嵌套索引；回补只展开索引的直属子清单）"
                )
                continue

            self._check_config(ref.repository, child_digest, child, problems)
            self._collect_layers(child_digest, child, layers, problems)

        if problems:
            raise BackfillRequestError(
                "目标仓库不满足回补前置条件（索引、直属子清单和配置须已在目标"
                "仓库）: " + "；".join(problems),
                409,
            )
        return layers

    @staticmethod
    def _descriptor_digest(
        descriptor: Any, problems: list[str], label: str
    ) -> str | None:
        if not isinstance(descriptor, dict):
            problems.append(f"{label} 不是对象")
            return None
        digest = descriptor.get("digest")
        if not isinstance(digest, str) or not digest.startswith(SHA256_PREFIX):
            problems.append(f"{label} 缺少合法 sha256 摘要")
            return None
        return digest

    def _check_config(
        self,
        repository: str,
        child_digest: str,
        child: dict[str, Any],
        problems: list[str],
    ) -> None:
        config = child.get("config")
        if not isinstance(config, dict):
            problems.append(f"子清单 {child_digest} 缺少 config 描述符")
            return
        config_digest = self._descriptor_digest(
            config, problems, f"子清单 {child_digest} 的 config 描述符"
        )
        if config_digest is None:
            return
        expected_size = _optional_int(config.get("size"))
        head = self._transport.head_blob(repository, config_digest)
        if not head.present:
            reason = "目标返回 404" if head.missing else head.detail
            problems.append(
                f"配置 {config_digest} 不在目标仓库（{reason}）；"
                "配置必须预先存在，回补 API 不挂载配置"
            )
            return
        if (
            expected_size is not None
            and head.size is not None
            and head.size != expected_size
        ):
            problems.append(
                f"配置 {config_digest} 大小与子清单不符: "
                f"仓库 {head.size}，清单 {expected_size}"
            )

    def _collect_layers(
        self,
        child_digest: str,
        child: dict[str, Any],
        layers: dict[str, int | None],
        problems: list[str],
    ) -> None:
        layer_descriptors = child.get("layers")
        if not isinstance(layer_descriptors, list):
            problems.append(f"子清单 {child_digest} 的 layers 字段不是数组")
            return
        for index, descriptor in enumerate(layer_descriptors):
            label = f"子清单 {child_digest} 的第 {index} 个层描述符"
            digest = self._descriptor_digest(descriptor, problems, label)
            if digest is None:
                continue
            size = _optional_int(
                descriptor.get("size") if isinstance(descriptor, dict) else None
            )
            if isinstance(descriptor, dict) and descriptor.get("size") is not None and size is None:
                problems.append(f"{label} 的 size 不是合法整数")
                continue
            if digest in layers:
                if layers[digest] != size:
                    problems.append(
                        f"层 {digest} 在不同平台子清单中的 size 不一致: "
                        f"{layers[digest]} vs {size}"
                    )
                # Same digest = same content: merge into one repair target.
                continue
            layers[digest] = size

    # -- per-layer repair --------------------------------------------------

    @staticmethod
    def _size_matches(head: HeadBlobResult, expected_size: int | None) -> bool:
        if expected_size is None:
            # Descriptor without size cannot be size-checked; callers mark
            # such layers failed before reaching this point.
            return True
        return head.size is not None and head.size == expected_size

    def _verify(
        self, repository: str, digest: str, expected_size: int | None
    ) -> tuple[bool, str | None, HeadBlobResult]:
        head = self._transport.head_blob(repository, digest)
        if not head.present:
            if head.missing:
                return False, "回查目标: 仍返回 404", head
            return False, f"回查目标失败: {head.detail}", head
        if not self._size_matches(head, expected_size):
            return (
                False,
                f"层大小与清单不符: 目标仓库为 {head.size} 字节, "
                f"清单记录 {expected_size} 字节",
                head,
            )
        return True, None, head

    def _repair_layer(
        self,
        ref: ImageReference,
        digest: str,
        expected_size: int | None,
        sources: list[str],
    ) -> LayerReport:
        report = LayerReport(digest=digest, size=expected_size)
        repository = ref.repository
        lock_key = (ref.registry, repository, digest)

        if expected_size is None:
            report.reasons.append(
                "清单层描述符缺少合法 size，无法按“大小与清单相符”核对"
            )
            return report

        # Fast path without taking the key lock: anything already healthy is
        # skipped and never triggers a mount.
        initial = self._transport.head_blob(repository, digest)
        if initial.present and self._size_matches(initial, expected_size):
            report.status = LAYER_PRESENT
            report.already_present = True
            report.resolved_by = "already-present"
            return report

        # Any other preliminary result (404, wrong size, or a transient
        # HEAD error that can occur while another caller is mounting the same
        # layer) is resolved authoritatively under the per-key lock.
        preliminary_note: str | None = None
        if initial.present:
            preliminary_note = self._size_mismatch_text(
                initial.size, expected_size
            )
        elif initial.state == "error":
            preliminary_note = initial.detail

        # Serialize only against the same target repository + layer.
        with self._locks.acquire(lock_key):
            # Re-check after acquiring: the previous holder may have fixed
            # this layer, in which case we just verify and skip.  Holding the
            # lock also keeps our HEAD from racing another mount's link write
            # inside the registry.
            ok, reason, head = self._verify(repository, digest, expected_size)
            if ok:
                report.status = LAYER_PRESENT
                report.resolved_by = "concurrent-request"
                if preliminary_note is not None:
                    report.reasons.append(
                        f"初次探测目标层异常或不确定（{preliminary_note}），"
                        "等待同层锁后回查已可取且大小相符，本请求跳过挂载"
                    )
                else:
                    report.reasons.append(
                        "等待同层锁后回查，层已由前一笔并发请求补好，本请求跳过挂载"
                    )
                return report
            if head.present:
                # Present but wrong size.
                report.reasons.append(reason or "目标层大小与清单不符")
                if preliminary_note and preliminary_note not in (reason or ""):
                    report.reasons.append(f"初次探测: {preliminary_note}")
                return report
            if head.state == "error":
                report.reasons.append(reason or "回查目标层失败")
                if preliminary_note and preliminary_note not in (reason or ""):
                    report.reasons.append(f"加锁前探测异常: {preliminary_note}")
                return report

            # Still 404 under the lock.  There is deliberately no remembered
            # failure: this request now tries its own candidate sources.
            report.mount_attempted = True
            mounted_source: str | None = None
            recovered_after_timeout_source: str | None = None

            for source in sources:
                mount = self._transport.post_mount(repository, digest, source)
                attempt = AttemptRecord(
                    source=source,
                    result=mount.outcome,
                    detail=mount.detail,
                )

                if mount.outcome == MOUNT_SESSION:
                    cancel = self._transport.delete_session(
                        mount.location or ""
                    )
                    attempt.session_cancelled = cancel.cancelled
                    if not cancel.cancelled and cancel.detail:
                        attempt.detail = (
                            f"{mount.detail}; {cancel.detail}"
                        ).strip("; ")
                    report.attempts.append(attempt)
                    continue

                if mount.outcome == MOUNT_ERROR:
                    report.attempts.append(attempt)
                    continue

                if mount.outcome == MOUNT_TIMEOUT:
                    report.attempts.append(attempt)
                    # Defining requirement: even a mount timeout must be
                    # followed by a target re-check.  The registry may have
                    # completed the mount before giving up on the response.
                    ok, timeout_reason, _ = self._verify(
                        repository, digest, expected_size
                    )
                    if ok:
                        recovered_after_timeout_source = source
                        break
                    report.reasons.append(
                        f"源 {source!r} 挂载超时后{timeout_reason or '仍未通过核对'}"
                    )
                    continue

                # MOUNT_MOUNTED
                report.attempts.append(attempt)
                mounted_source = source
                break

            # Authoritative final verification for every path.
            ok, reason, final_head = self._verify(
                repository, digest, expected_size
            )
            if ok:
                report.status = LAYER_PRESENT
                if recovered_after_timeout_source is not None:
                    report.mounted_from = recovered_after_timeout_source
                    report.resolved_by = "verified-after-mount-timeout"
                elif mounted_source is not None:
                    report.mounted_from = mounted_source
                    report.resolved_by = "cross-repository-mount"
                else:
                    # No source reported success but the layer is now
                    # verifiable (e.g. another process/operator healed it).
                    report.resolved_by = "target-recheck"
                return report

            report.status = LAYER_FAILED
            report.reasons.append(
                reason or "所有候选源均尝试后，目标层仍未通过摘要/大小核对"
            )
            if not sources:
                report.reasons.append("未提供任何候选源仓库 (sources 为空)")
            return report

    @staticmethod
    def _size_mismatch_text(actual: int | None, expected: int) -> str:
        return f"大小与清单不符: 目标仓库为 {actual} 字节, 清单记录 {expected} 字节"
