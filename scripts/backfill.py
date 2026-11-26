#!/usr/bin/env python3
"""Call POST /backfill on the webhook and print the repair report.

Pure standard library.

Examples:
    python3 scripts/backfill.py \\
        --target localhost:5000/images/target@sha256:abc... \\
        --source images/partial --source images/golden

    # Build the target reference from repository + index digest:
    python3 scripts/backfill.py --repository localhost:5000/images/target \\
        --index-digest sha256:abc... --source images/golden

Exit codes: 0 all layers verified; 1 backfill ran but layers remain broken;
2 request/transport error.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://localhost:8080/backfill")


def build_target(args: argparse.Namespace) -> str:
    if args.target:
        return args.target
    if not args.repository or not args.index_digest:
        raise SystemExit(
            "error: pass --target REF or both --repository and --index-digest"
        )
    repository = args.repository
    if "@" in repository:
        repository = repository.split("@", 1)[0]
    return f"{repository}@{args.index_digest}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--target", help="目标 OCI 索引摘要引用 repo@sha256:...")
    parser.add_argument("--repository", help="目标镜像名(可含 host:port)")
    parser.add_argument("--index-digest", help="目标索引摘要 sha256:...")
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        dest="sources",
        help="候选源仓库名，可重复；按顺序尝试",
    )
    parser.add_argument("--url", default=WEBHOOK_URL, help="/backfill 端点 URL")
    parser.add_argument(
        "--raw", action="store_true", help="只打印原始 JSON 响应"
    )
    args = parser.parse_args()

    target = build_target(args)
    payload = json.dumps({"target": target, "sources": args.sources}).encode()
    request = urllib.request.Request(
        args.url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            report = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        print(f"backfill request failed: HTTP {exc.code}: {body}", file=sys.stderr)
        return 2
    except urllib.error.URLError as exc:
        print(f"backfill request failed: {exc}", file=sys.stderr)
        return 2

    if args.raw:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report.get("complete") else 1

    print(f"target   : {report.get('target')}")
    summary = report.get("summary") or {}
    print(
        "summary  : {layers} layers, {alreadyPresent} already present, "
        "{mounted} mounted, {failed} failed".format(**summary)
    )
    for warning in report.get("warnings") or []:
        print(f"warning  : {warning}")
    for layer in report.get("layers") or []:
        mark = "OK " if layer.get("status") == "present" else "ERR"
        source = layer.get("mountedFrom") or layer.get("resolvedBy") or ""
        print(f"  [{mark}] {layer.get('digest')} ({layer.get('size')} B) {source}")
        for attempt in layer.get("attempts") or []:
            extra = f" - {attempt['detail']}" if attempt.get("detail") else ""
            cancel = ""
            if attempt.get("sessionCancelled") is True:
                cancel = " [session cancelled]"
            elif attempt.get("sessionCancelled") is False:
                cancel = " [session cancel FAILED]"
            print(
                f"        try {attempt.get('source')}: "
                f"{attempt.get('result')}{cancel}{extra}"
            )
        for reason in layer.get("reasons") or []:
            print(f"        reason: {reason}")

    if report.get("complete"):
        print("result   : complete (all layers reachable by digest with manifest size)")
        return 0
    print("result   : INCOMPLETE - layers still failing verification", file=sys.stderr)
    for failure in report.get("failures") or []:
        print(f"  - {failure.get('digest')}: {'; '.join(failure.get('reasons') or [])}",
              file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
