# Webhook 保存动作持久投递记录 - 实施计划

说明：任务按依赖顺序排列；每个任务的 Test Requirements（TR）为任务局部验收依据，最终以 spec.md 的 AC-1～AC-13 为准。不依赖 PostgreSQL 的测试全部可在无数据库环境运行；SQL 层测试以 `DATABASE_URL` 缺失时 `t.Skip` 的方式提供。

## Task 1: 新增 Webhook 投递相关环境变量配置
- **Status**: `completed`
- **Completion Evidence**:
  - options.go 新增 7 个 WEBHOOK_SAVE_* 配置（10s/60s/3600s/2/10 次/120s/30 天）+ 访问器；3 个解析测试通过；`go test ./internal/config` 全绿。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 [options.go](file:///Users/ding/Documents/swe/09224/project-05/internal/config/options.go) 中按现有 key 风格新增配置（含校验器与 accessor）：
    - `WEBHOOK_SAVE_POLLING_FREQUENCY`（secondType，默认 10 秒，>=1）
    - `WEBHOOK_SAVE_INITIAL_BACKOFF`（secondType，默认 60 秒，>=1）
    - `WEBHOOK_SAVE_MAX_BACKOFF`（secondType，默认 3600 秒，>=1）
    - `WEBHOOK_SAVE_BACKOFF_MULTIPLIER`（intType，默认 2，>=1）
    - `WEBHOOK_SAVE_MAX_ATTEMPTS`（intType，默认 10，>=0；0 表示不限制）
    - `WEBHOOK_SAVE_CLAIM_LEASE`（secondType，默认 120 秒，>=1）
    - `WEBHOOK_SAVE_RETENTION_DAYS`（dayType，默认 30 天，>=1）
  - 在 `Options` 接口（config.go）与 configOptions 上增加对应访问方法。
  - 在 options_parsing_test.go 增加默认值与自定义值解析测试（含 0=不限）。
- **Acceptance Criteria Addressed**: AC-5, AC-8, AC-11, AC-12
- **Test Requirements**:
  - `rule` TR-1.1: 未设置环境时七个 accessor 返回默认值；证据：`go test ./internal/config`。
  - `rule` TR-1.2: 设置自定义值（含 `WEBHOOK_SAVE_MAX_ATTEMPTS=0`）解析正确，非法值（0 秒、负数）校验失败；证据：解析测试输出。
- **Notes**: 名称/默认值若实现中发现与现有 key 冲突，可在不改变语义前提下调整并在完成证据中说明。

## Task 2: 数据库迁移 v136 与投递记录模型
- **Status**: `completed`
- **Completion Evidence**:
  - migrations.go v135（本签出编号）创建 webhook_save_deliveries（六态 CHECK、活跃部分唯一索引、due/inflight/清理索引、双外键级联）；model/webhook_delivery.go 六态常量+DTO；全新 PG 迁移成功。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 在 [migrations.go](file:///Users/ding/Documents/swe/09224/project-05/internal/database/migrations.go) 末尾追加第 136 个迁移，创建 `webhook_save_deliveries` 表：
    - `id BIGSERIAL PK`、`event_id text NOT NULL UNIQUE`、`user_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE`、`entry_id bigint NOT NULL REFERENCES entries(id) ON DELETE CASCADE`、`webhook_url text NOT NULL`、`status text NOT NULL DEFAULT 'pending'`（CHECK 约束限定六态）、`attempts int NOT NULL DEFAULT 0`、`max_attempts int NOT NULL`、`last_http_status int NULL`、`last_error text NOT NULL DEFAULT ''`、`created_at/last_attempt_at/claimed_at/next_attempt_at/updated_at timestamptz`。
    - 部分唯一索引：`(user_id, entry_id) WHERE status IN ('pending','in_flight','retry_waiting')`（同一文章至多一条活跃投递）。
    - 认领索引：`(next_attempt_at) WHERE status IN ('pending','retry_waiting')`；接管索引：`(claimed_at) WHERE status='in_flight'`；清理索引：终态 `(updated_at)`。
  - 新建 `internal/model/webhook_delivery.go`：状态常量（pending/in_flight/retry_waiting/succeeded/failed/canceled）、`WebhookDelivery` 结构体（含扫描辅助）、面向 API 的只读 DTO（json tag `webhook_delivery,omitempty`）。
- **Acceptance Criteria Addressed**: AC-1, AC-3, AC-11, AC-12
- **Test Requirements**:
  - `rule` TR-2.1: 在全新 PostgreSQL 上 `Migrate` 成功至 v136，重复执行幂等；表、CHECK、四个索引、外键级联存在；证据：跳过式迁移测试（无 DATABASE_URL 时 skip）。
  - `rule` TR-2.2: 并发插入同一 `(user_id,entry_id)` 的两条活跃记录时第二条被部分唯一索引拒绝；终态+活跃可并存；证据：跳过式 DB 测试。
  - `rule` TR-2.3: 删除 user/entry 级联删除投递记录；证据：跳过式 DB 测试。

## Task 3: Storage 投递记录生命周期方法（创建、事务联动、取消、重试、查询、清理）
- **Status**: `completed`
- **Completion Evidence**:
  - storage/webhook_delivery.go：创建（ON CONFLICT 幂等，crypto/rand UUIDv4 无新依赖）、单事务星标+入队/撤销、failed 重置（保留 event_id）、单/批查询、超期清理；7 个 skip-gated PG 测试通过（含回滚、取消边界、清理范围）。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 新建 `internal/storage/webhook_delivery.go`，使用 `database/sql`：
    - 定义内部 executor 抽象（`*sql.DB`/`*sql.Tx` 共同满足）以复用同事务语句。
    - `CreateWebhookSaveDelivery(userID, entryID, webhookURL, maxAttempts)`：生成 UUIDv4 事件标识（crypto/rand，无新依赖），`INSERT ... ON CONFLICT`（部分唯一索引）`DO NOTHING ... RETURNING`，冲突时返回已有活跃记录。
    - `StarEntriesAndWebhookDeliveries(userID, entries, webhookURLByEntry, starred)`：单事务内批量更新 `entries.starred`；starred=true 时为每个条目幂等插入投递记录；starred=false 时把对应用户+条目下 `status='pending' AND attempts=0` 的记录条件更新为 `canceled`（updated_at 刷新）。
    - `CancelUnsentWebhookDeliveries(userID, entryIDs)`：仅取消从未发送记录（供需要独立调用的入口）。
    - `ResetFailedWebhookDelivery(userID, entryID)`：条件更新 `status='failed' → 'pending'`，attempts=0、清空 claimed_at/last_error、next_attempt_at=now()、保留 event_id；返回受影响行数与不存在（0 行）信号。
    - `WebhookDeliveryByUserEntry` / `WebhookDeliveriesByEntries(userID, entryIDs)`：返回每篇文章最近一条记录的 map（批量，供列表页无 N+1）。
    - `CleanOldWebhookDeliveries(before)`：删除超期终态记录，返回删除条数。
  - 新建 `internal/storage/webhook_delivery_test.go`：DATABASE_URL 缺失即 skip；覆盖创建幂等、取消边界、failed 重置保留 event_id、批量查询 map、清理范围。
- **Acceptance Criteria Addressed**: AC-1, AC-7, AC-9, AC-10, AC-11
- **Test Requirements**:
  - `rule` TR-3.1: 重复创建返回同一记录（id/event_id 不变）；canceled/succeeded 后再创建得到新 event_id；证据：跳过式 DB 测试。
  - `rule` TR-3.2: 取消只影响 pending+attempts=0；in_flight/retry_waiting/终态均不变化，且无任何 HTTP 行为耦合；证据：跳过式 DB 测试。
  - `rule` TR-3.3: 星标更新与投递记录插入/撤销在同一事务：人为制造语句失败时两者均回滚；证据：跳过式 DB 测试。
  - `rule` TR-3.4: failed→重置后 event_id 恒定、attempts=0、状态 pending；非 failed 调用影响行数 0；证据：跳过式 DB 测试。
  - `rule` TR-3.5: 批量查询为单条 SQL（无 N+1），按 entry_id 返回最近记录；证据：代码审查 + DB 测试断言 map 内容。

## Task 4: Storage 认领/租约接管/结果回写
- **Status**: `completed`
- **Completion Evidence**:
  - FOR UPDATE SKIP LOCKED 批量认领、租约接管、succeeded/retry/failed 三个条件回写；25 记录 8 协程并发认领零重复、接管 event_id 不变、终态条件保护，PG 测试通过。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 在 storage 层新增：
    - `ReclaimExpiredInFlightDeliveries(lease)`：把 `status='in_flight' AND claimed_at < now()-lease` 的记录条件更新回 `retry_waiting` 且 next_attempt_at=now()（event_id 不变），返回条数。
    - `ClaimDueWebhookDeliveries(batchSize)`：单事务 `SELECT ... WHERE status IN ('pending','retry_waiting') AND next_attempt_at<=now() ORDER BY next_attempt_at LIMIT n FOR UPDATE SKIP LOCKED`，随后条件 UPDATE 为 `in_flight`（claimed_at=now、attempts+1、last_attempt_at=now）并提交，返回带稳定快照的记录列表。
    - 结果回写三个条件方法（均以当前状态必须为 in_flight 为前提）：
      - `MarkWebhookDeliverySucceeded(id, httpStatus)`；
      - `MarkWebhookDeliveryRetry(id, httpStatus, errText, nextAttemptAt)`；
      - `MarkWebhookDeliveryFailed(id, httpStatus, errText)`。
    - 达到 max_attempts（>0）时由调用方在重试/失败之间裁决，storage 仅忠实执行；回写均刷新 updated_at。
  - 测试：并发认领（多 goroutine/多连接）去重；租约边界；结果回写的状态前提（非 in_flight 不被覆盖，成功终态不被改写）。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-8
- **Test Requirements**:
  - `rule` TR-4.1: N 个连接并发认领同一批到期记录，每条记录恰好被认领一次（attempts 只 +1）；证据：跳过式 DB 并发测试。
  - `rule` TR-4.2: 超租约 in_flight 被接管为可投递并可再次被认领，event_id 不变；未超租约不被接管；证据：跳过式 DB 测试。
  - `rule` TR-4.3: succeeded/failed/canceled/pending 上调用结果回写影响 0 行；in_flight→succeeded 后再次回写 0 行；证据：跳过式 DB 测试。

## Task 5: Webhook 客户端支持事件标识与失败分类
- **Status**: `completed`
- **Completion Evidence**:
  - webhook 客户端：X-Miniflux-Event-ID 头、AttemptResult/RequestError、IsPermanent/IsTransient、Retry-After 秒/HTTP 日期解析、1KiB 片段截断；9 个 httptest 测试通过，new_entries 签名零变化。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 改造 [webhook.go](file:///Users/ding/Documents/swe/09224/project-05/internal/integration/webhook/webhook.go)：
    - `SendSaveEntryWebhookEvent(entry, eventID)` 在请求头加入 `X-Miniflux-Event-ID`；`new_entries` 方法签名保持不变。
    - 返回结构化结果：HTTP 状态码、解析后的 Retry-After（秒/HTTP 日期）、错误；新增分类纯函数 `IsPermanentStatus(status)`（4xx 且非 408/429）与传输错误判定（一切无响应错误=临时）。
    - 永久失败时读取并截断响应体（上限约 1 KiB）作为原因片段；保持 <400 即成功的既有判定与 HMAC 签名算法。
  - 新建 webhook_test.go（httptest，无需 DB）：event_id 头存在且跨调用值由调用方控制、签名与手工 HMAC 一致、载荷 JSON 结构不变；200 成功；404 永久+响应体；500/408/429 临时且 429 Retry-After 解析正确；连接拒绝/超时为临时错误；空 URL 报错。
- **Acceptance Criteria Addressed**: AC-2, AC-5, AC-6, AC-12
- **Test Requirements**:
  - `rule` TR-5.1: 请求包含且仅包含一个 `X-Miniflux-Event-ID` 头，两次发送传入同一 ID 时头值相同；`X-Miniflux-Signature` 等于 `crypto.GenerateSHA256Hmac(secret, body)`；证据：httptest 单元测试。
  - `rule` TR-5.2: 分类函数对 200/400/401/403/404/408/409/410/422/429/500/502/503 分类正确；证据：表驱动单元测试。
  - `rule` TR-5.3: Retry-After（秒与 IMF-fixdate 两种形式）解析正确，缺失时为零值；证据：单元测试。
  - `rule` TR-5.4: `SendNewEntriesWebhookEvent` 签名与行为零变化；证据：编译期签名检查 + 现有代码调用点审查。

## Task 6: 退避算法与后台分发器（状态机）
- **Status**: `completed`
- **Completion Evidence**:
  - dispatcher.go：backoffDelay、六态推进、Retry-After 封顶、max_attempts 快照裁决、禁用暂停、崩溃接管、NotifyPending 即时唤醒、ctx 优雅停止；11 个假存储/假时钟测试在 -race 下通过。
- **Priority**: high
- **Depends On**: Task 4, Task 5, Task 1
- **Description**:
  - 在 webhook 包新增 `dispatcher.go`：
    - 纯函数 `backoffDelay(attempt int, initial, max time.Duration, multiplier int) time.Duration`（initial×multiplier^(attempt-1)，封顶 max）。
    - 定义仅包含分发所需方法的 `DeliveryStore` 端口接口（由 `*storage.Storage` 满足），便于假存储测试；定义 `Clock` 接口（默认 wall clock，测试注入假时钟）。
    - `Dispatcher`：`Run(ctx)` 循环——先 `ReclaimExpiredInFlightDeliveries`，再循环 `ClaimDue`（批量，批量大小取 `WORKER_POOL_SIZE`），批内以有界并发发送：重新加载 entry（EntryQueryBuilder，含 feed/category；行已被 entry 删除级联时跳过）、读取用户 Integration（webhook 关闭则本轮跳过、不改变记录）、用记录快照 URL 与当前 secret 构造 client、携带 event_id 发送；按结果与注入时钟回写 succeeded/retry_waiting（Retry-After 优先、其次退避；max_attempts 达限转 failed）/failed。
    - 进程内唤醒：包级非阻塞 `NotifyPending()`（dispatcher 注册带缓冲 channel），入队路径调用；ticker 兜底；优雅停止尊重 ctx。
    - 全链路 slog 结构化日志。
  - 单元测试（假存储 + httptest + 假时钟，无需 DB）：成功一次后不再发送；连接中断→退避序列→429 Retry-After→达限 failed；404 一次 failed 且原因含响应体；event_id 全程恒定；webhook 关闭时挂起不发；canceled/retry_waiting 在取消后不被掩盖的协作场景（假存储预置记录，断言请求序列）；崩溃接管（假存储返回过期 in_flight 后用同 ID 重发）；通知唤醒即时触发。
- **Acceptance Criteria Addressed**: AC-2, AC-3, AC-4, AC-5, AC-6, AC-7, AC-8, NFR-1, NFR-5
- **Test Requirements**:
  - `rule` TR-6.1: 远端请求计数：成功路径 1、临时失败重试 N 后成功 N+1、succeeded 后再跑若干轮仍为 N+1；证据：httptest 计数断言。
  - `rule` TR-6.2: 退避序列与 Retry-After 覆盖、max_attempts 达限终态、永久失败一次终态，均与假时钟推进一致；证据：单元测试。
  - `rule` TR-6.3: 所有发送请求的 event_id 等于记录 event_id，接管/手动重置场景也不变；证据：单元测试。
  - `rubric` TR-6.4: 状态机可维护性（端口小、分支清晰、无 goroutine 泄漏）；scale 1-5；anchors 1=逻辑与 SQL/HTTP 耦合无法测试；3=可测但有残留竞态；5=端口隔离、并发有界、ctx 取消干净；threshold >=4；证据：代码审查 + `go test -race`。

## Task 7: 四个保存入口接入发件箱，拆分 SendEntry，挂钩取消
- **Status**: `completed`
- **Completion Evidence**:
  - EnqueueSaveEntry/SyncStarredSaveEntries 接入 UI/API/Fever/GReader 四入口；SendEntry 移除 webhook 内联分支，PushEntries 不变；Fever unsaved/GReader unstar 走撤销路径；5 个编排器测试通过。
- **Priority**: high
- **Depends On**: Task 3, Task 6
- **Description**:
  - 在 integration 包新增编排入口：
    - `EnqueueSaveEntry(store, entry, settings)`：WebhookEnabled 时解析 URL（entry.Feed.WebhookURL 优先）→ `CreateWebhookSaveDelivery` → `webhook.NotifyPending()`；随后 `go SendEntry(entry, settings)` 负责其余集成。
    - `EnqueueStarredSaveEntries(store, entries, settings)` / `CancelStarredSaveEntries(...)`（命名实现时定）：GReader 批量路径在 storage 单事务方法内完成星标+投递记录；非 webhook 集成仍逐条 `go SendEntry`。
  - 从 `SendEntry` 移除 Webhook 分支（`PushEntries` 中 new_entries 的 webhook 分支原样保留）。
  - 改造入口：
    - UI [entry_save.go](file:///Users/ding/Documents/swe/09224/project-05/internal/ui/entry_save.go)：改为同步入队（替换 goroutine），响应附带投递摘要。
    - API [entry_handlers.go](file:///Users/ding/Documents/swe/09224/project-05/internal/api/entry_handlers.go#L255-L289)：同上，202 响应体携带投递状态。
    - Fever：`saved` 用单事务"置星标+入队"，随后 `go SendEntry`；`unsaved` 用单事务"取消星标+撤销未发送投递"，不发任何请求。
    - GReader edit-tag：starred 列表走"批量置星标+批量入队（按 entry.Feed 解析 URL）"后逐条 `go SendEntry`；unstarred 列表走"批量取消星标+撤销未发送投递"。
  - 更新/保留 integration_test.go（Linkwarden 日志用例不依赖 webhook 分支）。
- **Acceptance Criteria Addressed**: AC-1, AC-7, AC-10
- **Test Requirements**:
  - `rule` TR-7.1: webhook 关闭时 Enqueue 不写库、不报错；其他集成照常被 goroutine 调用；证据：单元测试（假 store/调用计数）+ 现有 integration_test.go 通过。
  - `rule` TR-7.2: webhook 开启时四个入口各产生恰好一条 pending 记录并触发 NotifyPending；HTTP 请求仅由分发器产生（处理器内无直接 webhook HTTP 调用）；证据：代码审查 + 假分发器/httptest 测试。
  - `rule` TR-7.3: Fever unsaved / GReader unstar 不产生任何外发请求；证据：代码审查 + httptest 计数测试。
  - `rule` TR-7.4: `PushEntries` 的 new_entries webhook 路径字节级行为不变；证据：代码审查（无修改）。

## Task 8: 守护进程接线（启动/关停分发器、清理任务）
- **Status**: `completed`
- **Completion Evidence**:
  - 分发器随 scheduler 角色启动，daemon SIGTERM 时 ctx 取消（在 HTTP/pool 关停间）；cleanup_tasks 增加终态记录清理；`go test -race ./...` 无泄漏。
- **Priority**: medium
- **Depends On**: Task 6, Task 3
- **Description**:
  - [scheduler.go](file:///Users/ding/Documents/swe/09224/project-05/internal/cli/scheduler.go)：在 `HasSchedulerService() && !HasMaintenanceMode()` 分支启动分发器（轮询间隔取配置），返回可 Stop 的句柄。
  - [daemon.go](file:///Users/ding/Documents/swe/09224/project-05/internal/cli/daemon.go)：关停流程中 cancel 分发器 ctx 并等待退出（在 worker pool 关停前后均可，需在 HTTP 服务器之后、进程退出之前）。
  - [cleanup_tasks.go](file:///Users/ding/Documents/swe/09224/project-05/internal/cli/cleanup_tasks.go)：增加 `CleanOldWebhookDeliveries(now-retention)` 与结构化日志。
- **Acceptance Criteria Addressed**: AC-8, AC-11, NFR-1
- **Test Requirements**:
  - `rule` TR-8.1: 维护模式/非 scheduler 角色不分发；证据：代码审查与启动分支检查。
  - `rule` TR-8.2: SIGTERM 后分发器在限定时间内退出，无 goroutine 泄漏（`go test -race` 相关包）；证据：分发器 ctx 取消单元测试。
  - `rule` TR-8.3: 清理任务仅删超期终态记录并返回正确计数；证据：storage 测试（Task 3）复用。

## Task 9: REST API 状态展示与手动重试端点
- **Status**: `completed`
- **Completion Evidence**:
  - 文章 JSON（单篇/列表/update）批量附加 webhook_delivery；POST /v1/entries/{id}/webhook-delivery/retry（200/409/404）；client SDK 新增 WebhookDelivery/SaveEntryWithDelivery/RetryWebhookDelivery；skip-gated API 测试通过。
- **Priority**: high
- **Depends On**: Task 7
- **Description**:
  - 文章响应附加状态：在单篇与列表响应构造处（getEntryFromBuilder、getEntriesHandler 等所有返回 entry JSON 的位置）按响应内 user+entryIDs 一次性批量加载投递记录并填充 DTO；无记录/未启用时字段缺省。
  - 保存接口 202 响应体返回 `{message?, webhook_delivery:{event_id,status,...}}`（UI 保存路径的 JSON 同步增加该字段）。
  - 新路由 `POST /v1/entries/{entryID}/webhook-delivery/retry`：无记录 404、非 failed 409、成功返回 200/202 与重置后的 DTO；记录成功后 NotifyPending。
  - client/ SDK：`Entry` 增加 WebhookDelivery 字段；新增 `RetryWebhookDelivery(entryID)`；`SaveEntry` 解析投递摘要（如响应体含）。
  - 处理器测试：DTO 填充（含批量去重）、404/409/成功分支。
- **Acceptance Criteria Addressed**: AC-6, AC-9
- **Test Requirements**:
  - `rule` TR-9.1: 列表接口 N 篇文章只触发一次批量状态查询且 DTO 正确关联；证据：处理器单元测试（假 store 调用计数）。
  - `rule` TR-9.2: failed 记录重试后响应 DTO 状态为 pending、event_id 不变；非 failed 返回 409；无记录 404；证据：API 处理器测试。
  - `rule` TR-9.3: 未启用 webhook 用户的 entry JSON 不含 `webhook_delivery` 键；证据：序列化断言。

## Task 10: Web UI 状态展示、重试操作与 i18n
- **Status**: `completed`
- **Completion Evidence**:
  - common/webhook_delivery.html 六态面板（原因+CSRF 重试按钮+下次尝试时间）接入 entry.html；UI 重试路由/处理器/JS/CSS；8 键同步全部 23 语言（zh_CN 完整翻译）；模板 8 子测试与 UI skip-gated 测试通过。
- **Priority**: medium
- **Depends On**: Task 9
- **Description**:
  - 文章页 handler（showXxxEntryPage 共用渲染路径）加载该篇文章投递记录并入模板数据；在 [entry.html](file:///Users/ding/Documents/swe/09224/project-05/internal/template/templates/views/entry.html) / item_meta.html 保存控件附近渲染状态徽标；failed 时显示截断原因与"重新投递"按钮；pending/in_flight/retry_waiting 显示待发送/投递中/等待重试（含下次尝试时间）；succeeded 显示已送达；canceled 显示已撤销。
  - 新 UI 路由 `POST /entry/save/{entryID}/webhook-retry`（CSRF 中间件覆盖范围内）与处理器：成功回 JSON 并触发 NotifyPending；冲突/不存在返回既有 JSON 错误风格。
  - 新增 i18n 键（状态、失败原因前缀、重试按钮/提示）：en_US 完整、zh_CN 完整翻译，其余语言文件用 `make add-string` 同等流程同步占位（英文兜底），确保 catalog 测试通过。
  - 保存成功 JSON（entry_save.go）被前端 JS 消费处保持兼容（仅增字段）。
- **Acceptance Criteria Addressed**: AC-6, AC-9
- **Test Requirements**:
  - `rule` TR-10.1: 六种状态在模板中均有对应文案与样式，failed 同时渲染原因与重试按钮（含 CSRF token）；证据：模板渲染测试（engine 测试或处理器测试）。
  - `rule` TR-10.2: 重试处理器成功/409/404 分支返回正确 JSON 状态；证据：UI 处理器测试。
  - `rule` TR-10.3: 全部语言 JSON 文件键集合一致且合法；证据：`go test ./internal/locale/...`。

## Task 11: 全量验证与静态检查
- **Status**: `completed`
- **Completion Evidence**:
  - gofmt 0 文件、go vet 通过、go build 通过；无 DB 下 50 个包全部通过 0 失败；skip-gated PG 测试（storage/api/ui）通过；-race 全包通过；另完成真实服务器端到端冒烟 18/18（保存→送达、事件标识/签名、404 永久失败原因、手动重试复用标识、409/404）。golangci-lint 二进制环境缺失，以 gofmt+vet+race 替代。
- **Priority**: high
- **Depends On**: Task 10
- **Description**:
  - 运行 `gofmt -l .`（须为空）、`go vet ./...`、`go build ./...`、`go test -race -count=1 ./...`（无 PostgreSQL 环境须全绿，DB 用例 skip）。
  - 环境允许时运行 `golangci-lint run`；若环境无 PG/lint 二进制，在完成证据中如实记录。
  - 人工走查 AC 清单，补充任何遗漏的边界（如 webhook URL 快照、secret 实时读取、entry 删除并发）。
- **Acceptance Criteria Addressed**: AC-12, AC-13
- **Test Requirements**:
  - `rule` TR-11.1: 上述命令实际输出全部符合预期，失败用例为零（skip 不算失败）；证据：完整命令输出摘录。
  - `rubric` TR-11.2: 总体测试说服力（状态机/并发/取消/展示分支均自动化覆盖）；scale 1-5；anchors 1=仅手工/happy path；3=核心分支覆盖但无并发；5=无外部依赖跑完全状态机且有 skip 式 PG 认领/接管/取消测试；threshold >=4；证据：测试文件清单与覆盖点矩阵。
