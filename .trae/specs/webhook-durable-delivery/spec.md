# Webhook 保存动作持久投递记录 - 产品需求文档

## Overview
- **Summary**：为"保存文章到用户配置的 Webhook"这一动作建立数据库持久化投递记录（transactional outbox），以稳定事件标识（幂等键）签名请求，由后台分发器在数据库事务提交后发送；对临时性失败按可配置指数退避重试，对永久性拒绝立即停止并向用户展示原因；取消保存时只撤销从未发出的投递，已发出但结果未知的投递保留原标识继续确认；通过 PostgreSQL 行级认领保证多实例不重复发送、重启后接管未确认记录。
- **Purpose**：当前实现（`go integration.SendEntry(...)`）是进程内 fire-and-forget：远端短暂不可用、请求途中进程重启都会导致保存事件静默丢失，用户无法分辨"未发送 / 重试中 / 已被远端接受"；请求也没有幂等标识，重发可能造成远端重复保存。
- **Target Users**：启用了 Webhook 集成的 Miniflux 用户（通过 Web UI、REST API、Fever API、Google Reader API 保存文章）；以及运行多个 Miniflux 实例的部署运维者。

## Goals
- 每次保存动作生成一条持久投递记录，状态可查询、可展示：未发送、投递中、等待重试、成功、永久失败、已撤销。
- 请求携带全程稳定的事件标识（同一投递的所有尝试相同），远端可据此去重；签名机制保持兼容。
- 投递仅在文章状态与投递记录的数据库事务提交之后发生。
- 临时故障自动退避重试（参数可配置），永久拒绝立即停止并把原因展示给用户，失败后支持手动重新投递。
- 取消保存：未开始的投递撤销；已发送/结果未知的投递不被相反事件掩盖，继续用原标识确认到终态。
- 多实例并发安全（同一条尝试不会被两个实例发送）；进程重启后自动接管未确认记录。
- 未启用 Webhook 的用户、其他第三方保存集成、`new_entries` 推送事件保持原有行为。

## Non-Goals
- 不改造其他保存集成（Pinboard、Wallabag、Instapaper 等约 20 种）的投递方式，它们仍为现有的进程内 fire-and-forget。
- 不改造订阅刷新时的 `new_entries` Webhook 事件，它仍为现有即时发送、仅记录日志。
- 不引入新的消息队列/外部依赖（Redis、Broker 等）；仅使用 PostgreSQL。
- 不改变现有 Webhook 请求的载荷结构与 `X-Miniflux-Signature`（请求体 HMAC）的计算方式；只新增请求头与可选响应字段。
- 不为 Web UI / REST API 新增"取消保存/取消收藏"端点（这两个入口当前不存在取消语义；取消仅发生在 Fever `unsaved` 与 Google Reader 取消星标路径）。
- 不做远端回调/回执 API；是否接受仍以同步 HTTP 响应为准。

## Background & Context
- 保存动作当前有 4 个入口：
  - Web UI：[entry_save.go](file:///Users/ding/Documents/swe/09224/project-05/internal/ui/entry_save.go) `POST /entry/save/{entryID}`，不改变文章状态，`go integration.SendEntry(...)` 后立即返回 `saved`。
  - REST API：[entry_handlers.go](file:///Users/ding/Documents/swe/09224/project-05/internal/api/entry_handlers.go#L255-L289) `POST /v1/entries/{entryID}/save`，返回 202。
  - Fever：[handler.go](file:///Users/ding/Documents/swe/09224/project-05/internal/fever/handler.go#L442-L470) `as=saved/unsaved`，先 `ToggleStarred` 再异步发送；`unsaved` 当前不触发任何集成。
  - Google Reader：[handler.go](file:///Users/ding/Documents/swe/09224/project-05/internal/googlereader/handler.go#L296-L317) 星标/取消星标批量改状态后，对新星标条目异步发送。
- Webhook 客户端：[webhook.go](file:///Users/ding/Documents/swe/09224/project-05/internal/integration/webhook/webhook.go)，POST JSON，带 `X-Miniflux-Signature`（body 的 HMAC-SHA256）与 `X-Miniflux-Event-Type`；HTTP 状态码 >= 400 即视为错误，不区分临时/永久。
- Webhook URL 解析：优先使用订阅级 `feed.WebhookURL`，否则用用户级 `WebhookURL`；密钥取 `integrations.WebhookSecret`。
- 数据库：PostgreSQL，迁移为追加式函数列表（当前 135 个），见 [migrations.go](file:///Users/ding/Documents/swe/09224/project-05/internal/database/migrations.go)；`database/sql` + `lib/pq`，支持 `FOR UPDATE SKIP LOCKED`。
- 后台：[scheduler.go](file:///Users/ding/Documents/swe/09224/project-05/internal/cli/scheduler.go) 中常驻 goroutine 由 `SCHEDULER_*` 角色开关控制；配置全部为环境变量（见 [options.go](file:///Users/ding/Documents/swe/09224/project-05/internal/config/options.go)）。
- 已确认的决策（用户选择）：
  1. 退避与重试参数通过**全局环境变量**配置，提供内置默认值。
  2. 投递状态与永久失败原因同时在 **REST API 与 Web UI** 展示。
  3. 临时失败达到最大尝试次数后**停止并标记失败、展示最后错误，支持手动重新投递**（复用原事件标识）。

## Functional Requirements

### FR-1：持久投递记录（事务发件箱）
- 启用 Webhook 的用户每发起一次保存动作，必须在**同一数据库事务**内：
  - Fever/Google Reader 路径：提交星标状态变更，并插入投递记录；
  - Web UI/REST 路径：插入投递记录（该路径无文章状态变更）。
- 记录至少包含：稳定事件标识（UUID，全局唯一）、user_id、entry_id、目标 Webhook URL 快照、状态、尝试次数、最大尝试次数、最近 HTTP 状态码、最近错误原因、创建时间、最近尝试时间、认领时间、下次可尝试时间。
- 同一 `(user_id, entry_id)` 至多存在一条非终态（`pending`/`in_flight`/`retry_waiting`）记录；重复保存返回已有记录而非新建，避免双击/重复请求产生重复投递。
- 事务提交前不得发出任何 HTTP 请求；事务回滚则既无状态变更也无请求。

### FR-2：后台分发与多实例认领
- 常驻分发器在 scheduler 角色启动；周期性认领到期记录并立即发送，同时提供进程内"有新记录"的即时唤醒（轮询作为兜底与跨实例手段）。
- 认领使用 `SELECT ... FOR UPDATE SKIP LOCKED`，在事务内把记录从 `pending`/`retry_waiting` 置为 `in_flight`、写认领时间并递增尝试次数；多个实例并发时同一条记录只会被一个实例认领。
- `in_flight` 超过可配置租约（默认 120 秒，须大于请求超时）的记录视为前次发送中途崩溃，重新置为可投递状态并用**同一事件标识**再次发送；重启后由此接管所有未确认记录。
- 每次发送时从数据库重新加载文章（含订阅、分类）以构造载荷；Webhook 密钥取用户当前配置；目标 URL 用记录中的快照。发送时若用户已关闭 Webhook 开关，跳过本轮（记录保留，重新启用后继续）。

### FR-3：稳定事件标识与签名
- 每次请求新增请求头 `X-Miniflux-Event-ID`，值为该投递记录的稳定 UUID；同一条记录的所有尝试（含崩溃后接管、手动重试）必须使用相同值。
- `X-Miniflux-Signature` 仍为请求体的 HMAC-SHA256，载荷结构保持不变；现有接收方不受影响。
- 成功（HTTP < 400）只确认一次：记录转为终态 `succeeded` 后不再发送；重复响应/重复尝试不得产生第二条成功记录或额外副作用。

### FR-4：失败分类、退避重试与永久拒绝
- 临时性失败（连接错误/超时/连接中断、HTTP 408、429、5xx）：记录转为 `retry_waiting`，按可配置指数退避（初始间隔 × 倍数^(尝试次数-1)，封顶最大间隔）设置 `next_attempt_at`；收到 429/503 且带 `Retry-After` 时遵从该等待时间（封顶不超过最大间隔）。
- 达到最大尝试次数（可配置，0 表示不限）后转为终态 `failed`，记录最后错误。
- 永久性拒绝（4xx 中除 408/429 外）立即转为终态 `failed`，不再自动重试；记录 HTTP 状态码与响应体片段（截断到合理长度）作为原因。
- `failed` 的原因（状态码/网络错误/响应片段）必须可通过 API 与 Web UI 被用户看到。
- `failed` 记录支持用户手动重新投递：复用原事件标识、重置尝试预算、回到 `pending`；非 `failed` 状态的手动重试请求必须被拒绝（冲突错误）。

### FR-5：取消保存语义
- Fever `unsaved` 与 Google Reader 取消星标在状态变更提交后，必须把对应用户+条目下**从未发送过**（`pending` 且 attempts=0）的投递记录置为终态 `canceled`，且不产生任何 Webhook 请求。
- 已发送但结果未知的记录（`in_flight`、`retry_waiting`，以及发送中途崩溃后被接管的记录）**不得**被取消：继续以原事件标识重试/确认到终态；系统**不得**发送任何"相反事件"（没有 unsave 事件类型，也不新增）。
- 已处于终态（`succeeded`/`failed`/`canceled`）的记录不受取消动作影响。
- 取消后再次保存：因旧记录已终态，应允许创建新的投递记录与新的事件标识。

### FR-6：状态展示
- REST API：文章 JSON 增加只读的 Webhook 投递状态对象（事件标识、状态、尝试次数/上限、最近 HTTP 状态码、最近错误、时间戳）；未启用 Webhook 或无记录时不出现该字段。保存接口的响应体包含新建/已有的投递状态。新增手动重试端点。文章列表场景必须批量加载状态，不得 N+1 查询。
- Web UI：文章页在保存控件附近展示投递状态（待发送/投递中/等待重试/已送达/失败/已撤销）；失败时展示原因并提供"重新投递"按钮（走 CSRF 保护的 UI 路由）。
- 状态文案进入 i18n 翻译体系（至少 en_US 完整；新增键同步到全部语言文件，zh_CN 提供完整翻译）。

### FR-7：生命周期清理
- 终态记录（`succeeded`/`failed`/`canceled`）超过可配置保留期（默认 30 天）后由现有清理任务周期删除。
- 用户或文章删除时通过外键级联删除其投递记录。

## Non-Functional Requirements
- **NFR-1 可靠性**：分发器任意时刻崩溃（含请求发送途中、结果写回途中）都不得造成记录丢失或永久卡死；恢复后凭同一事件标识继续，远端幂等去重。
- **NFR-2 并发正确性**：认领、状态推进、取消、手动重试全部以数据库条件更新（CAS 式 `WHERE status IN (...)`）为准，不依赖进程内锁；同一尝试不会被两个实例发送。
- **NFR-3 兼容性**：不新增第三方依赖；不改变现有请求体、签名算法、响应状态码语义；旧接收方无需改造。
- **NFR-4 性能**：保存接口仅增加一次小行写入（与星标更新同事务），不增加外部 HTTP 调用；文章列表通过一次批量查询附加投递状态；认领查询由部分索引支撑。
- **NFR-5 可观测性**：入队、认领、成功、重试（含下次尝试时间）、永久失败、撤销、崩溃接管、手动重试均有结构化日志（user_id、entry_id、event_id、webhook_url、尝试次数、状态码）。
- **NFR-6 可测试性**：退避计算、状态码分类、事件标识/签名、分发状态机可用单元测试（httptest + 内存假存储）覆盖，无需真实 PostgreSQL；SQL 状态推进另提供缺库自动跳过的数据库集成测试。
- **NFR-7 代码质量**：遵循仓库现有惯例（迁移追加、环境变量配置、storage 方法风格、slog 日志、license 头、gofmt/golangci-lint 通过）。

## Constraints
- **Technical**：Go、PostgreSQL（`database/sql` + `lib/pq`）；迁移只能在 `migrations` 数组末尾追加（成为 v136）；配置只能是环境变量；守护进程入口在 [daemon.go](file:///Users/ding/Documents/swe/09224/project-05/internal/cli/daemon.go)，调度在 [scheduler.go](file:///Users/ding/Documents/swe/09224/project-05/internal/cli/scheduler.go)。
- **Business**：对未启用 Webhook 的用户与所有其他保存集成零行为变化；对外契约（请求体、签名）保持兼容。
- **Dependencies**：仅依赖标准库与仓库现有依赖（UUID 生成优先使用仓库已有手段；如无则用 `crypto/rand` 生成，不引新依赖）。

## Assumptions
- Webhook 成功的判定沿用现有约定：HTTP 状态码 < 400 为成功。
- 永久/临时分类采用标准约定：4xx（除 408、429）永久；408、429、5xx、一切传输层错误临时；429/503 尊重 `Retry-After`。
- 用户在投递未完成期间关闭 Webhook 开关：记录保留且暂停发送，重新启用后继续；不自动判失败也不自动撤销（唯一撤销途径是取消保存，且仅对从未发送的记录生效）。
- Webhook URL 使用保存时刻快照（订阅级覆盖优先），即使用户事后修改 URL，在途投递仍发往原地址；密钥始终取当前配置。
- Fever 的 `saved/unsaved` 与 Google Reader 星标变更即为"保存/取消保存"信号；Web UI 与 REST API 不存在取消信号，保持现状。
- HTTP 请求超时沿用 HTTP 客户端现有默认值；认领租约默认 120 秒远大于该值。
- 分发器仅在 scheduler 角色实例启动，但行锁保证即使多个角色配置相同的实例同时运行也不会重复发送。

## Acceptance Criteria

### AC-1：保存后存在持久投递记录
- **Type**: `rule`
- **Given**：用户已启用 Webhook 集成（用户级或订阅级 URL），数据库为最新迁移版本。
- **When**：用户分别经 Web UI、REST API、Fever `saved`、Google Reader 加星标保存一篇文章。
- **Then**：`webhook_save_deliveries` 中各出现一条记录，含唯一稳定 `event_id`、正确 user_id/entry_id、解析后的 Webhook URL 快照、状态 `pending`、attempts=0、next_attempt_at 已到期；Fever/GReader 路径中星标状态与该记录在同一事务提交。
- **Pass Condition**：四个入口均能在数据库中查到上述记录；人为让事务回滚时记录与星标变更都不存在。
- **Evidence**：跳过式数据库集成测试输出 + 迁移 SQL 审查 + 四个入口处理器代码审查。

### AC-2：提交后发送且请求带稳定事件标识与兼容签名
- **Type**: `rule`
- **Given**：httptest 模拟远端；一条 `pending` 投递记录。
- **When**：分发器认领并发送该记录，且在制造首次临时失败后再次重试。
- **Then**：远端在记录对应的数据库事务提交后才收到请求；两次（及以后所有）请求的 `X-Miniflux-Event-ID` 相同且等于记录的 event_id；`X-Miniflux-Signature` 等于请求体的 HMAC-SHA256 且请求体结构与当前 `WebhookSaveEntryEvent` 一致。
- **Pass Condition**：单元测试断言提交前零请求、event_id 跨尝试恒定、签名与载荷与旧版一致。
- **Evidence**：`internal/integration/webhook` 单元测试（httptest）。

### AC-3：投递状态可分辨且持久化
- **Type**: `rule`
- **Given**：投递记录完整生命周期。
- **When**：依次发生待发送、被认领、临时失败等待重试、最终成功（或永久失败/撤销）。
- **Then**：记录状态在 `pending → in_flight → retry_waiting → in_flight → succeeded/failed/canceled` 之间按规则推进并落库；任何时刻查询 API/数据库都能区分"尚未发送 / 等待重试 / 投递中 / 已被接受 / 失败 / 已撤销"。
- **Pass Condition**：状态机单元/集成测试覆盖全部六种状态及合法/非法转换。
- **Evidence**：状态机单元测试 + 跳过式 DB 集成测试。

### AC-4：成功只确认一次
- **Type**: `rule`
- **Given**：投递记录已为 `succeeded` 终态。
- **When**：分发器轮询、崩溃接管流程、手动重试接口作用于该记录。
- **Then**：不会再为它发送任何 HTTP 请求；状态保持 `succeeded`；手动重试返回冲突错误。
- **Pass Condition**：测试中远端对该记录只收到 1 次 2xx 之前的请求序列，成功后计数不再增长。
- **Evidence**：分发器单元测试断言远端请求计数。

### AC-5：临时失败按可配置退避重试并遵守 Retry-After
- **Type**: `rule`
- **Given**：配置初始退避 10s、倍数 2、最大退避 80s、最大尝试 4 次；远端依次返回连接中断、503（Retry-After: 30）、429。
- **When**：分发器持续处理该记录。
- **Then**：每次失败后状态为 `retry_waiting`，attempts 递增，下次尝试时间依次为约 10s、30s（Retry-After 优先）、40s（10×2²）；第 4 次仍失败后转为 `failed` 终态并写入最后错误，之后不再发送。
- **Pass Condition**：退避计算函数与分发器推进测试断言时间序列（用注入时钟）、尝试上限与终态行为；0 表示不限制有解析测试。
- **Evidence**：退避/分发器单元测试；配置解析测试。

### AC-6：永久拒绝立即停止并向用户展示原因
- **Type**: `rule`
- **Given**：远端返回 404（或除 408/429 外的任意 4xx）并带响应体。
- **When**：分发器发送投递记录。
- **Then**：记录立即转为 `failed`（不再重试），保存 HTTP 状态码与截断后的响应体片段；该原因出现在文章 JSON 的 `webhook_delivery` 字段中，并出现在 Web UI 文章页上。
- **Pass Condition**：单元测试断言一次请求即终态；API 测试与模板渲染检查断言原因可见。
- **Evidence**：webhook 客户端单元测试 + API 测试/处理器测试 + 模板审查。

### AC-7：取消保存只撤销未发送的投递且不发相反事件
- **Type**: `rule`
- **Given**：同一篇文章分别存在 attempts=0 的 `pending` 记录、`retry_waiting`（已发送未确认）记录、`in_flight` 记录、各终态记录。
- **When**：Fever `unsaved` 或 Google Reader 取消星标提交。
- **Then**：仅 `pending`（attempts=0）记录转为 `canceled`；其余非终态记录保留原 event_id 继续推进至终态；终态记录不变；整个过程远端收到的请求数不因取消而增加（无任何"相反事件"请求）；取消后重新保存会产生新 event_id 的新记录。
- **Pass Condition**：取消逻辑单元/集成测试覆盖五种前置状态，并断言远端无新增请求。
- **Evidence**：storage 取消方法测试 + Fever/GReader 处理器代码审查 + 分发器请求计数测试。

### AC-8：多实例不重复发送、重启接管未确认记录
- **Type**: `rule`
- **Given**：两条并发运行的分发器（或两个数据库连接同时认领）；以及一条 `in_flight` 且 claimed_at 早于租约阈值的记录（模拟发送途中崩溃/重启）。
- **When**：两者同时认领到期记录；接管周期扫描僵死记录。
- **Then**：每条记录只被一个连接认领成功（另一个 SKIP LOCKED 取不到）；超租约的 `in_flight` 记录被重新置为可投递并以**同一 event_id** 再次发送；未超租约的 in_flight 不被抢占。
- **Pass Condition**：跳过式 PostgreSQL 集成测试中并发认领去重率 100%，接管复用同一 event_id；单元测试用假时钟验证租约边界。
- **Evidence**：DB 并发认领测试 + 租约接管测试。

### AC-9：手动重新投递失败记录
- **Type**: `rule`
- **Given**：一条 `failed` 投递记录（含事件标识 E）。
- **When**：用户调用 REST 重试端点或点击 Web UI 重试按钮。
- **Then**：记录回到 `pending`、attempts 归零、next_attempt_at 到期、event_id 保持 E，随后分发器用 E 再次发送；对非 `failed` 记录调用时 REST 返回 409、UI 给出错误提示；无记录时返回 404。
- **Pass Condition**：API/处理器测试断言状态重置、event_id 不变与冲突/不存在分支。
- **Evidence**：API 测试 + UI 处理器测试。

### AC-10：未启用用户与其他集成行为不变
- **Type**: `rule`
- **Given**：未启用 Webhook 的用户；以及仅启用其他保存集成（如 Pinboard）的用户；和一次订阅刷新。
- **When**：执行保存与刷新。
- **Then**：不产生任何 `webhook_save_deliveries` 行、不触发分发器逻辑；其他集成仍按原 fire-and-forget 方式调用；`new_entries` Webhook 事件路径代码与行为不变。
- **Evidence**：`rule`
- **Pass Condition**：代码审查确认 webhook 分支从旧 SendEntry 移除、新逻辑全部以 WebhookEnabled/记录存在为前提；现有 `integration_test.go` 与 API 集成测试通过。

### AC-11：终态记录周期清理与级联删除
- **Type**: `rule`
- **Given**：存在超过保留期与未超保留期的终态记录，以及任意非终态记录。
- **When**：清理任务执行；用户/文章被删除。
- **Then**：仅超期终态记录被删除，其余保留；用户/文章删除后相关记录由外键级联删除。
- **Pass Condition**：跳过式 DB 集成测试断言删除范围；迁移中外键与索引审查通过。
- **Evidence**：storage 清理测试 + 迁移审查。

### AC-12：实现质量与仓库惯例一致性
- **Type**: `rubric`
- **Dimension**：代码质量、最小侵入性与惯例一致性（配置、迁移、日志、包结构、license 头、命名）。
- **Scale**: 1-5
- **Anchors**: 1 = 引入新依赖或破坏性契约变更、风格与仓库明显不符；3 = 功能正确但存在不必要的侵入或重复代码；5 = 完全遵循 Miniflux 既有模式，改动边界清晰、无冗余。
- **Pass Threshold**: >= 4
- **Evidence**: `go vet ./...`、`gofmt -l` 为空、`golangci-lint run`（如环境可用）、`go build ./...` 通过；独立代码审查。

### AC-13：测试覆盖与回归防护
- **Type**: `rubric`
- **Dimension**：自动化测试对状态机、失败分类、退避、认领互斥、取消语义、展示链路的覆盖深度。
- **Scale**: 1-5
- **Anchors**: 1 = 仅 happy path 手动验证；3 = 关键分支有单元测试但缺少并发/崩溃接管验证；5 = 不依赖外部服务即可覆盖全部状态机分支，并有缺库自动跳过的 PostgreSQL 认领/接管/取消测试。
- **Pass Threshold**: >= 4
- **Evidence**: `go test ./...` 通过（无 DB 环境全绿），测试清单与覆盖点审查。

## Open Questions
- [x] 退避参数配置位置 → 全局环境变量（含默认值）。
- [x] 失败原因展示渠道 → REST API + Web UI。
- [x] 重试上限与手动重试 → 达到上限即 `failed` 终态，支持手动重新投递（复用事件标识）。
- [ ] 无其他待决问题；如评审中发现环境变量命名/默认值需要调整，在不改变语义前提下可在实现阶段确定。
