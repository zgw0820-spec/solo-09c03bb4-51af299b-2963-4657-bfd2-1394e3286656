# 层 404 与回补 API 样例

配合 `POST /backfill` 使用。下列真实响应由本机 Distribution registry 2.8.3
+ 本服务实际运行产生；摘要来自 `scripts/seed_registry.py seed-backfill`
生成的固定夹具（不同机器上重跑摘要一致，以脚本打印为准）。

## 0. 准备缺层现场（Docker Compose）

```bash
docker compose up -d --build

# 生成多架构夹具：
#   images/target  目标：索引 + 两个子清单 + 配置 + 3 个层（common 被两平台共用）
#   images/golden  完整候选源（3 个层都在）
#   images/partial 只有 common 和 arm64 层（缺 amd64 层，用于演示 202 回退）
python3 scripts/seed_registry.py seed-backfill

# 删除目标仓库的层链接（只删 _layers/.../link，不动全局 blob 内容），
# 节点仍能取索引/子清单，但拉层 404。
python3 scripts/seed_registry.py break-layers
# 该命令默认通过 `docker compose exec -T registry rm -f ...` 删除；
# 非 compose 的本机 registry 二进制可加：
#   --fs-root /path/to/docker/registry/v2
```

记下方索引摘要（下例均为该值）：

```
sha256:be7d09a204d1c2eded6087c96cb3c3ea0db66a050ad88614c5c39678491ffab1
```

## 1. 缺层的 API 样例（Registry Distribution API）

按摘要 HEAD/GET 层，目标返回 `404`：

```bash
D=sha256:50817642918953e79c6a9758657feda644f3c4646e73da9c34da186f6804c8b6
curl -sS -I "http://localhost:5000/v2/images/target/blobs/$D"
```

```http
HTTP/1.1 404 Not Found
Content-Type: application/json; charset=utf-8
Docker-Distribution-Api-Version: registry/2.0
```

```bash
curl -sS "http://localhost:5000/v2/images/target/blobs/$D"
```

```json
{"errors":[{"code":"BLOB_UNKNOWN","message":"blob unknown to registry","detail":"sha256:50817642918953e79c6a9758657feda644f3c4646e73da9c34da186f6804c8b6"}]}
```

上面的错误体也存为 [`backfill-missing-layer-404.json`](backfill-missing-layer-404.json)。

与此同时索引仍可按摘要取到（这正是“索引能取、拉层 404”的现场）：

```bash
curl -sS -H 'Accept: application/vnd.oci.image.index.v1+json' \
  "http://localhost:5000/v2/images/target/manifests/sha256:be7d09a204d1c2eded6087c96cb3c3ea0db66a050ad88614c5c39678491ffab1"
```

## 2. 回补请求

`POST /backfill`（另有版本化别名 `POST /api/v1/backfill`），请求体见
[`backfill-request.json`](backfill-request.json)：

```json
{
  "target": "localhost:5000/images/target@sha256:be7d09a204d1c2eded6087c96cb3c3ea0db66a050ad88614c5c39678491ffab1",
  "sources": ["images/partial", "images/golden"]
}
```

```bash
curl -sS -X POST http://localhost:8080/backfill \
  -H 'Content-Type: application/json' \
  -d @samples/backfill-request.json
```

也可以用标准库 CLI（输出更易读，未补齐时退出码为 1）：

```bash
python3 scripts/backfill.py \
  --target localhost:5000/images/target@sha256:be7d09a204d1c2eded6087c96cb3c3ea0db66a050ad88614c5c39678491ffab1 \
  --source images/partial --source images/golden
```

服务端只做这些 Registry 调用（不下载/上传层内容，不改标签和清单）：

```
HEAD   /v2/images/target/blobs/sha256:...                 # 仅对 404 的层继续
POST   /v2/images/target/blobs/uploads/?mount=...&from=images/partial
       # 201 -> 挂载成功；202 -> 返回上传会话：
DELETE <202 的 Location>                                 # 取消会话
POST   ...?mount=...&from=images/golden                  # 试下一源
HEAD   /v2/images/target/blobs/sha256:...                 # 按摘要+大小复核
```

## 3. 回补成功响应（含 202 取消后换源）

完整真实响应：[`backfill-response-repaired.json`](backfill-response-repaired.json)。
摘要：

```json
{
  "complete": true,
  "summary": {"layers": 3, "present": 3, "alreadyPresent": 0, "mounted": 3, "failed": 0},
  "failures": []
}
```

`amd64-only` 层在 `images/partial` 中不存在，Registry 返回上传会话，API
取消后改试 `images/golden` 成功（摘录）：

```json
{
  "digest": "sha256:62c9b3e6236430a3e24c12dc62205184592f9751abd482f7e30b82a97a77e2b3",
  "size": 10240,
  "status": "present",
  "mountedFrom": "images/golden",
  "resolvedBy": "cross-repository-mount",
  "attempts": [
    {"source": "images/partial", "result": "upload_session",
     "detail": "源仓库无法直接挂载，仓库返回了上传会话", "sessionCancelled": true},
    {"source": "images/golden", "result": "mounted"}
  ]
}
```

`common` 层同时出现在 amd64/arm64 两个子清单中，按摘要合并，只回补一次，
因此 2 个子清单 × 2 个层 = 4 个层描述符被合并为 3 个唯一层。

## 4. 重试：已补好的层直接跳过

再次 POST 同样的请求，所有层 `alreadyPresent=true`，没有任何挂载调用：

完整响应：[`backfill-response-retry-skipped.json`](backfill-response-retry-skipped.json)

```json
{
  "complete": true,
  "summary": {"layers": 3, "present": 3, "alreadyPresent": 3, "mounted": 0, "failed": 0}
}
```

## 5. 没有补齐：列出未通过核对的层及原因

若候选源都不含这些层（下例只给 `images/arm-only`），每个源对每个缺层返回
202 会话，API 逐一取消，最终按摘要回查仍是 404，于是 `complete=false` 并在
`failures` 中列出每个层和原因。完整真实响应：
[`backfill-response-incomplete.json`](backfill-response-incomplete.json)。

```json
{
  "complete": false,
  "summary": {"layers": 3, "present": 0, "alreadyPresent": 0, "mounted": 0, "failed": 3},
  "failures": [
    {
      "digest": "sha256:50817642918953e79c6a9758657feda644f3c4646e73da9c34da186f6804c8b6",
      "size": 10240,
      "reasons": ["回查目标: 仍返回 404"]
    }
  ]
}
```

这不是 HTTP 层错误：请求被正常处理，响应仍为 `200`，用 `complete`/
`failures` 表达部分失败，便于调用方读取逐层原因。请求/前置条件错误才用
4xx + `{"detail": "..."}`。

## 6. 挂载超时但实际已成功：仍算补好

挂载请求超时不直接判失败：API 一定回查目标，按摘要可取且大小与清单相符即
记为补好，`resolvedBy="verified-after-mount-timeout"`。下例由测试用的
“慢挂载” HTTP fake registry 产生（真实 registry 同机挂载是元数据操作，
通常远快于超时，难以自然构造超时），生产传输走的是同一个
`UrllibBackfillTransport`：

完整响应：[`backfill-response-timeout-recovered.json`](backfill-response-timeout-recovered.json)

```json
{
  "complete": true,
  "layers": [{
    "status": "present",
    "mountedFrom": "images/golden",
    "resolvedBy": "verified-after-mount-timeout",
    "attempts": [{"source": "images/golden", "result": "timeout",
                  "detail": "挂载请求超时: TimeoutError: timed out"}]
  }]
}
```

## 7. 错误码

| HTTP | 场景 |
| --- | --- |
| 400 | `target` 不是合法引用 / 带的是标签而不是摘要；候选源指向其他 Registry |
| 403 | 目标 Registry 不在 `ALLOWED_REGISTRIES` |
| 404 | 目标仓库按摘要取不到索引 |
| 409 | 前置条件不满足：直属子清单或配置缺失、摘要不符、出现嵌套索引等 |
| 422 | 请求体缺 `target` 或 `sources` 类型不对 |
| 502 | 读索引/子清单时 Registry 通信或摘要头异常 |

字段名也接受 `target_reference` / `source_repositories` 等常见别名。

## 8. 并发语义（单进程）

- 锁键为 `(registry, 目标仓库, 层摘要)`：只有同一目标仓库的同一层会相互
  等待；不同层、不同目标仓库各用各的锁，可并行回补。
- 锁内会再次 HEAD 核对：前一笔补成功，后一笔直接跳过；前一笔没补成，
  **不缓存失败结论**，后一笔持锁后重新确认 404，并用**自己的 sources**
  逐个尝试。
- 对同一目标仓库的 3 个缺层并发 8 个真实 HTTP 请求实测：8/8 `complete`，
  每个层恰好发生 1 次跨仓挂载，其余请求为 `concurrent-request` 或
  `already-present`。
