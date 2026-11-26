# image-digest-admission

Kubernetes mutating admission webhook (FastAPI) that rewrites mutable image
**tags** into OCI **index digests** on Deployment create/update, plus a
`POST /backfill` API that repairs image **layers** returning 404 in a local
Registry via same-registry cross-repository mounts.

## 两个能力

1. **`POST /mutate`（原有，保留不变）**：准入阶段把标签替换成索引摘要。
2. **`POST /backfill`（新增）**：节点仍能取到索引、但拉层 404 时，按目标
   OCI 索引摘要 + 候选源仓库名，把目标缺失的层用跨仓挂载补回目标仓库。

## 为什么需要它

镜像标签是可变的：标签被重新指向另一个索引后，同一轮滚动发布中先拉起和
后拉起的副本可能拉到不同版本。本 webhook 在准入阶段把 Pod 模板里的标签
引用一次性替换成索引摘要（`repo@sha256:...`），让一次发布里所有副本都锚定
同一份多架构索引，标签之后如何改指都不影响本次发布。

## 行为约定

- 只接收 `admission.k8s.io/v1` 的 `AdmissionReview`，只处理
  `apps/deployments` 的 `CREATE` / `UPDATE`；其他请求直接放行。
- 只处理本地仓库（默认 `localhost:5000`、`registry:5000`，由
  `ALLOWED_REGISTRIES` 配置）。引用其他 registry 的镜像直接拒绝。
- 容器与初始化容器中**带标签**的镜像被替换为 OCI 索引摘要；已经带
  `@sha256:` 摘要的引用保持原样，且不会触发任何仓库请求。
- 同一个 `(registry, repository, tag)` 在一次准入调用中只解析一次
  （请求内缓存）；缓存不跨请求，下一次带标签提交会重新查询仓库，因此
  标签改指会在新的一次准入中被观察到。
- 平台选择：
  - Pod 模板 `nodeSelector` 含 `kubernetes.io/arch` 时，只核对
    `linux/<该架构>`；
  - 否则同时核对 `linux/amd64` 与 `linux/arm64`。
- 所有要求的平台必须能在**同一个索引**中找到描述符，并且每个子清单都能
  按索引里记录的摘要从仓库取到（摘要不一致也算失败）。
- 任一镜像核对失败：拒绝整次请求，响应中给出镜像和中文原因，不返回补丁。

## 目录结构

```
app/
  main.py                 FastAPI 入口：POST /mutate、POST /backfill、GET /healthz
  admission/
    config.py             环境变量配置
    images.py             镜像引用解析
    registry.py           OCI Distribution API 客户端 + 平台核对
    policy.py             架构选择、请求内缓存、JSON Patch 生成
    backfill.py           层 404 回补：索引展开/按摘要合并层/跨仓挂载/锁与复核
scripts/
  seed_registry.py        纯标准库：推送 OCI 夹具、seed-backfill、break-layers
  send_admission.py       纯标准库：发送 AdmissionReview 并解码补丁
  backfill.py             纯标准库：调用 POST /backfill 并打印逐层结果
samples/                  AdmissionReview 样例 + 回补请求/真实响应/API 说明
tests/                    82 个测试（核心仅依赖标准库；FastAPI 接线测试缺依赖时跳过）
docker-compose.yml        webhook + 本地 registry
Dockerfile                webhook 镜像
```

## 用 Docker Compose 启动

```bash
docker compose up -d --build

# 1. 推送测试夹具（多架构索引 / 缺平台索引 / 子清单被删除的索引）
python3 scripts/seed_registry.py seed

# 2. 健康检查
curl -s http://localhost:8080/healthz
```

### 场景一：正常改写，标签改指后重新解析

```bash
# 首次创建：demo:v1 被改写为索引摘要（init 容器和业务容器同一标签只查一次）
python3 scripts/send_admission.py samples/deployment-create.json
# allowed=true，patch 中两条 replace 均指向同一个 sha256 索引摘要

# 模拟标签改指：把 demo:v1 重新指向 v2 的索引
python3 scripts/seed_registry.py retag-demo

# 再次提交同样的标签引用：新一次准入重新查仓库，补丁里的摘要随之变化
python3 scripts/send_admission.py samples/deployment-update.json

# 恢复现场
python3 scripts/seed_registry.py reset-demo
```

### 场景二：缺少所需子清单（两种失败都给镜像和原因，不带补丁）

```bash
# 2a. 索引里只有 arm64，默认要同时核对 amd64+arm64 -> 拒绝
python3 scripts/send_admission.py samples/deployment-missing-platform.json
# allowed=false: ...索引...中缺少平台 linux/amd64 的子清单描述符

# 同样的镜像加上 kubernetes.io/arch=arm64 选择器 -> 只查 arm64，放行
python3 scripts/send_admission.py samples/deployment-arch-selector.json

# 2b. 索引声明了两个平台，但 amd64 子清单已按摘要删除（404）-> 拒绝
python3 scripts/send_admission.py samples/deployment-broken-child.json
# allowed=false: 镜像 'localhost:5000/apps/broken:v1' 核对失败: 清单不存在...

# 非本地仓库 -> 拒绝
python3 scripts/send_admission.py samples/deployment-non-local-registry.json
```

### 响应示例

成功（补丁为 base64 编码的 JSONPatch）：

```json
{
  "apiVersion": "admission.k8s.io/v1",
  "kind": "AdmissionReview",
  "response": {
    "allowed": true,
    "uid": "req-create-demo",
    "patchType": "JSONPatch",
    "patch": "W3sib3AiOiJyZXBsYWNlIiwicGF0aCI6Ii9zcGVjL3RlbXBsYXRlL3NwZWMvaW5pdENvbnRhaW5lcnMvMC9pbWFnZSIsInZhbHVlIjoibG9jYWxob3N0OjUwMDAvYXBwcy9kZW1vQHNoYTI1NjouLi4ifV0="
  }
}
```

拒绝：

```json
{
  "apiVersion": "admission.k8s.io/v1",
  "kind": "AdmissionReview",
  "response": {
    "allowed": false,
    "uid": "req-missing-platform",
    "status": {"code": 403, "message": "镜像 'localhost:5000/apps/arm-only:v1' 核对失败: 索引 sha256:... 中缺少平台 linux/amd64 的子清单描述符"}
  }
}
```

## 层 404 回补 API：`POST /backfill`

### 它解决什么

索引和子清单还在、节点也能解析出摘要，但某些层 blob 在目标仓库返回 404
（例如仓库 `_layers` 链接丢失）。跨仓挂载是 Registry 元数据操作：让目标
仓库重新链接到同一台 Registry 上候选源仓库已有的层 blob，**不需要上传或
下载层内容**。

### 请求

```json
{
  "target": "localhost:5000/images/target@sha256:<索引摘要>",
  "sources": ["images/partial", "images/golden"]
}
```

- `target` 必须是**带 sha256 摘要的 OCI 索引引用**（不接受标签，避免回补
  到变动中的清单），Registry 必须在 `ALLOWED_REGISTRIES` 内。
- `sources` 是**同一台本地 Registry** 上的候选源仓库名，按顺序尝试；也
  接受带 `localhost:5000/`、`registry:5000/` 前缀的写法和
  `target_reference` / `source_repositories` 字段别名。传空数组则只核对、
  不挂载。

### 行为约定（与需求逐条对应）

1. **前置条件**：按摘要取目标索引；逐个取 `manifests` 中的**直属子清单**
   （不接受嵌套索引）；并 HEAD 核对每个子清单的 **config** 已在目标仓库。
   任一不满足返回 `409`（索引本身缺失为 `404`），信息聚合列出。
2. **按摘要合并层**：收集所有直属子清单的 `layers`，相同 digest（含跨平台
   共用层）只回补一次；同 digest 但清单记录的 size 不一致会记为问题。
3. **只对明确 404 的层挂载**：先 HEAD 目标层。`200` 且 size 与清单一致 →
   保留并跳过；`200` 但 size 不符或 HEAD 出错 → 加同层锁后再权威核对一次
   （避免与并发挂载的链接写竞争），仍异常则记为该层失败，**绝不盲目挂载**。
4. **跨仓挂载**：`POST /v2/<target>/blobs/uploads/?mount=<digest>&from=<source>`。
   - `201` → 挂载完成，进入复核；
   - `202` + 上传会话 `Location` → **`DELETE` 取消该会话**，再试下一源；
   - 其他错误 / 超时 → 记录并试下一源。
5. **超时也必须复核**：挂载请求超时不直接判失败，立即 HEAD/Range-GET 回查
   目标；按摘要可取且大小与清单相符就算补好
   （`resolvedBy="verified-after-mount-timeout"`）。
6. **大小核对才算补好**：任何路径最后都以
   `HEAD /v2/<repo>/blobs/<digest>` 的 `200` + `Content-Length == 清单 size`
   为准；HEAD 不被允许（405/501）时回退到立即关闭响应体的 `Range: bytes=0-0`
   GET，不读取层内容。
7. **不改标签/清单，不搬内容**：整个流程只有 manifest 读、blob HEAD、
   mount POST、session DELETE；没有任何 manifest/tag PUT/DELETE，也没有
   blob 内容的 GET（除立即关闭的 1 字节 Range 探测）或上传。
8. **单进程并发**：锁键为 `(registry, 目标仓库, 层 digest)`，不同层/不同
   目标仓库互不阻塞；持锁后再次核对，前一笔补成功则后一笔跳过，**不缓存
   失败**——前一笔没补成，后一笔仍会用自己的 `sources` 重新尝试。
9. **结果报告**：HTTP `200` 始终返回结构化报告，`complete=false` 表示仍有
   层未通过核对，`failures[]` 列出每个问题层的 digest/size/reasons。
   请求或前置条件错误才用 4xx + `{"detail": ...}`。

响应结构（节选）：

```json
{
  "target": "localhost:5000/images/target@sha256:...",
  "complete": true,
  "summary": {"layers": 3, "present": 3, "alreadyPresent": 0, "mounted": 3, "failed": 0},
  "layers": [
    {"digest": "sha256:...", "size": 10240, "status": "present",
     "alreadyPresent": false, "mountedFrom": "images/golden",
     "resolvedBy": "cross-repository-mount",
     "attempts": [{"source": "images/partial", "result": "upload_session",
                   "sessionCancelled": true},
                  {"source": "images/golden", "result": "mounted"}],
     "reasons": []}
  ],
  "failures": []
}
```

`resolvedBy` 取值：`already-present` / `concurrent-request` /
`cross-repository-mount` / `verified-after-mount-timeout` / `target-recheck`。

### Compose 实测流程（含缺层与回补样例）

完整 curl、真实请求/响应 JSON 与错误码表见
[`samples/backfill-api-examples.md`](samples/backfill-api-examples.md)。
快速版：

```bash
docker compose up -d --build

# 造夹具：images/target(目标) images/golden(全) images/partial(缺一个层)
python3 scripts/seed_registry.py seed-backfill
# -> 打印 INDEX_DIGEST

# 模拟目标仓库层链接丢失 -> HEAD 层返回 404（默认 docker compose exec 删除）
python3 scripts/seed_registry.py break-layers

# 先看缺层：HTTP 404 + BLOB_UNKNOWN
curl -sS -I http://localhost:5000/v2/images/target/blobs/<LAYER_DIGEST>

# 回补：partial 缺的层会收到 202 会话，API 取消后改由 golden 挂载成功
python3 scripts/backfill.py \
  --target localhost:5000/images/target@<INDEX_DIGEST> \
  --source images/partial --source images/golden

# 立刻重试：3 layers, 3 already present, 0 mounted
python3 scripts/backfill.py --target localhost:5000/images/target@<INDEX_DIGEST> \
  --source images/partial --source images/golden
```

`break-layers` 通过删除仓库的 `_layers/sha256/<hex>/link` 复现“目标 404、
源仍可挂载”的可修复现场；这与真实仓库中层链接/引用损坏等价。若删除的是
全局 `blobs/.../data`，同一台 Registry 的所有源都会失去该 blob，挂载只会
得到 202 会话，最终报告该层仍 404——这正是“未补齐时列出原因”的场景。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ALLOWED_REGISTRIES` | `localhost:5000,registry:5000` | 允许解析的本地 registry host 列表，逗号分隔 |
| `REGISTRY_UPSTREAM` | `http://registry:5000` | webhook 访问仓库用的地址（可与 workload 里的 host 不同） |
| `REGISTRY_TIMEOUT` | `5` | 单次仓库 HTTP 请求超时（秒）；`/mutate` 与 `/backfill` 的 HEAD/挂载/取消会话共用 |

注意：补丁里保留镜像原始 host（如 `localhost:5000`），只把标签换成
`@sha256:...`；webhook 自身通过 `REGISTRY_UPSTREAM` 访问仓库。`/backfill`
同理：请求体里写 `localhost:5000/...`，实际连接走 `REGISTRY_UPSTREAM`。

## 测试

核心测试不需要 Docker 或第三方库，只用 Python 标准库：

```bash
python3 -I tests/run.py
# Ran 82 tests ... OK (skipped=8)
# 引用解析、平台选择、索引解析、请求内只解析一次、标签改指重新查询、
# 缺平台/缺子清单/摘要不一致拒绝、真实 HTTP 端到端；
# 回补：按摘要合并层、404 才挂载、202 取消并换源、超时后复核、
# 大小核对、失败清单、重试跳过、同层互斥/异层并行/失败不缓存。
```

装好 `requirements.txt` 后再跑会启用 8 个 FastAPI `TestClient` 接线测试
（`tests/test_backfill_api.py`，缺 fastapi/httpx 时自动 skip），此时
**82 个测试全部执行并通过**：

```bash
pip install -r requirements.txt
python3 tests/run.py     # Ran 82 tests ... OK
```

测试文件：

- `tests/test_http_e2e.py` 在本机临时端口起实现 Distribution v2 清单子集的
  HTTP 服务，走完整 URL/请求头/摘要头路径。
- `tests/test_backfill.py` 用脚本化内存传输覆盖回补状态机与并发锁。
- `tests/test_backfill_http.py` 用标准库 `ThreadingHTTPServer` 验证生产用的
  `UrllibBackfillTransport`：真实 socket 超时、202 会话 DELETE、
  `?mount=&from=` 编码、HEAD 405 → Range GET 回退。
- `tests/test_backfill_api.py` 验证 `/backfill` 路由、别名字段、状态码和
  “`/mutate` 仍在”。

### 真机实测（非 mock）

开发过程中还用 Debian 源里的 Distribution registry 2.8.3 二进制 + 真实
uvicorn 做了端到端核对：`seed` / `seed-backfill` 造夹具、删除仓库链接制造
404、调用真实 `POST /backfill`，确认 201/202+DELETE/换源、重试零挂载、
8 并发全部补齐且每层只挂载一次，以及 `/mutate` 原流程不变。响应样例就在
`samples/backfill-response-*.json`。

## 尚未完成 / 生产化差距

- **TLS 与鉴权**：当前为方便 Compose 演示使用明文 HTTP。Kubernetes 的
  `ValidatingWebhookConfiguration` / `MutatingWebhookConfiguration` 要求
  HTTPS（通常配合 cert-manager 注入 CA Bundle），部署清单与证书签发未包含。
- **没有内置 Registry 认证**：仅支持匿名本地仓库；生产环境需要 token /
  basic auth 与凭据管理。
- 架构选择器只识别 `nodeSelector` 中的 `kubernetes.io/arch`；
  `nodeAffinity`、多架构 `In` 取值、`OS/version/variant` 维度未处理。
- `/mutate` 仍只核对清单可达性，不核对 blob 层；层核对/回补只在显式调用
  `/backfill` 时发生，二者没有自动联动（不会在准入时自动修层）。
- `/backfill` 只展开索引的**直属**子清单，不接受嵌套索引。
- 复核只证明“按该摘要路径可取 + `Content-Length` 与清单 size 一致”，
  **没有下载层内容重算 sha256**（需求明确不下载内容）；需要密码学完整性
  保证时应另行离线校验。
- 并发去重锁是**单进程内**的（符合需求范围）。多副本/多 Pod 部署时，不同
  进程对同一层仍可能同时发起 mount；Registry 自身能保证链接最终一致，但
  极端情况下并发 HEAD 可能读到链接写入中间态而收到瞬时 5xx——本服务会把它
  作为该层当次失败原因列出，调用方重试即可（实测立即重试成功）。多副本
  强互斥需要引入 Redis/数据库锁或 leader 机制。
- 候选源仅限同一台、同一 `ALLOWED_REGISTRIES` 别名集合内的 Registry；不支持
  跨 registry 主机挂载，也不携带 source 侧凭据。
- 仓库查询失败目前在准入路径快速失败拒绝请求，没有重试；高并发下的限流/
  熔断未实现。`/backfill` 对单源错误会继续尝试后续源，但没有指数退避。
- Compose 面向演示；未提供在真实集群安装 webhook 所需的
  Service / MutatingWebhookConfiguration 清单，也未给 `/backfill` 配
  Ingress/RBAC/审计。
