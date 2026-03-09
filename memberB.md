# M3 验收报告
## Member B：Transaction + Concurrency

> 目标：按“**结构/接口**”与“**功能/行为**”两类验收 Member B，并额外补充 **Table（table.py）完善度**与 **B 方案与 Table 的集成风险**。  


- 项目：L-Store / Milestone 3
- 被验收成员：Member B（Transaction + Concurrency）
- 对照文件：`lock_manager.py` / `transaction.py` / `transaction_worker.py` / `table.py`

---

## 0) 结论概览

### 0.1 结构/接口结论
- ✅ **无明显问题**：文件、类名、核心方法接口齐全，符合 M3 skeleton 预期。
- ⚠️ **debug需核对**：`Table.select/sum` 是否兼容 `txn=` 关键字参数（不兼容会导致无限重试/超时）。

### 0.2 功能/函数结论
- ✅ strict 2PL：只在 commit/abort 释放锁（方向正确）
- ✅ no-wait：冲突立刻 abort（方向正确）
- ✅ S/X 锁语义、S→X upgrade、insert 的 pk 锁（方向正确）
- ⚠️ undo 框架齐，但大量 try/except 可能“吞掉失败”，需要结合 Table/Index 实现验证（即：需要接下来几位成员内容一起鉴定）
- ❌ **Blocker：重复加锁计数与 release_all 不一致 → 可能锁泄漏 → worker 无限重试/测试超时**
- ⚠️ sum/scan 不加锁：如果 tester 强测 serializable 的聚合/范围读，有风险

### 0.3 Table 完善度结论（补充）
- ✅ Table 核心架构齐全：page_directory / page_ranges / index / merge / metadata persistence 均具备
- ✅ Table 提供了 Transaction(B) undo 所需的关键底层接口（read_physical_record / read_latest_user_columns / overwrite_base_* 等）
- ⚠️**集成级风险（debug时需验证）**：insert 事务锁资源为 `("PK", pk)`，而 update/delete 锁资源为 `base_rid` —— 两套锁粒度不一致，可能出现“未提交记录被并发 update/delete”的隔离性漏洞（取决于执行时序与 tester case）

---

## 1) 需求映射表（M3 PDF/Plan → 代码落点 → 判定）

> 注：这里以“需求关键词”映射到代码函数/关键逻辑点

| M3 需求点（关键词） | 期望行为 | 代码落点（文件:函数） | 判定 | 备注 |
|---|---|---|---:|---|
| strict 2PL | 锁只在 commit/abort 统一释放 | `transaction.py: commit()/abort()` -> `lm.release_all()` | ✅ | 中途不释放 |
| no-wait | 冲突不等待，直接失败/abort | `lock_manager.py: acquire_S/acquire_X` 冲突抛 `LockConflict`；`transaction.py: run()` 捕获后 abort | ✅ | 符合 no-wait |
| S 锁用于读 | select 点查/索引定位要拿 S | `transaction.py: _plan_locks(select)` -> read_res；`_acquire_locks_no_wait` 先 S 后 X | ✅ | 但 sum/scan 另说 |
| X 锁用于写 | update/delete/increment/insert 要拿 X | `_plan_locks(update/delete/increment)` -> baseRID；`insert` -> ("PK", pk) | ✅ | insert 用 pk 锁 |
| lock upgrade | S→X 升级仅当没有其他 S owner | `lock_manager.py: acquire_X` 中 S 模式升级判定 | ✅ | 冲突 no-wait |
| rollback / undo | abort 需撤销 insert/update/delete 的副作用 | `transaction.py: _capture_before_write / _apply_undo / abort()` | ✅/⚠️ | 依赖 Table/Index 接口是否完整且没被吞异常 |
| worker retry | abort 自动重试直到 commit | `transaction_worker.py: __run()` while True retry | ✅ | 有 backoff |
| serializable（若要求） | 范围读/聚合也要隔离 | `_plan_locks(sum/scan)` 返回空锁 | ⚠️ | 取决 tester |
| 可重入 / 重复加锁释放一致性 | 重复 acquire 要么幂等，要么释放同次数 | `lock_manager.py: s_count/x_count++` + `_txn_resources` 是 set；`release_all` 只释放一次 | ❌ | **Blocker：潜在锁泄漏/超时** |
| 参数结构兼容 | select/sum 调用不应 TypeError | `transaction.py: run()` 调 `op(*args, txn=self)` | ⚠️ | 需核对 table 接口（目前无需改动，需等待其余人承接） |

---

## 2) 结构层验收（接口/参数/文件结构）——勾选版

### 2.1 文件与模块（必须项）
- ✅ `lock_manager.py` 存在且可 import
- ✅ `transaction.py` 存在且可 import
- ✅ `transaction_worker.py` 存在且可 import

### 2.2 LockManager 接口（必须项）
- ✅ `class LockConflict(Exception)`
- ✅ `acquire_S(txn_id, rid)`
- ✅ `acquire_X(txn_id, rid)`
- ✅ `release_all(txn_id)`

### 2.3 Transaction 接口（必须项）
- ✅ `Transaction.add_query(query, table, *args)`
- ✅ `Transaction.run() -> bool`
- ✅ `Transaction.abort() -> bool`
- ✅ `Transaction.commit() -> bool`

> 结构备注：
> - [ ] ⚠️ `add_query()` 仅固定第一张 table：若 M3 测试多表事务则不足（handout未写是否为单表，若 tester 单表可忽略）

### 2.4 TransactionWorker 接口（必须项）
- ✅ `add_transaction()`
- ✅ `run()` / `join()`
- ✅ abort 自动重试直到 commit


### 2.5 参数兼容性（结构层高危项）
- [ ] ⚠️ `Table.select(..., txn=...)` 是否支持（无则 TypeError）
- [ ] ⚠️ `Table.sum(..., txn=...)` 是否支持（无则 TypeError）

**提示：此部分属于策子承接query时，需要进行对table的改动**
> 若不支持：将出现 abort→retry→abort 的循环，表现为测试卡住/超时。

---

## 3) 功能层验收（strict 2PL / no-wait / upgrade / undo）

### 3.1 Strict 2PL（必须项）
- ✅ 事务执行中不释放锁
- ✅ commit/abort 时统一释放全部锁

### 3.2 No-Wait（必须项）
- ✅ 冲突立刻失败（抛 LockConflict / 返回失败）
- ✅ 事务立刻 abort
- ✅ worker 负责重试

### 3.3 S/X 锁语义（必须项）
- ✅ select（点查/索引定位）拿 S
- ✅ update/delete/increment 拿 X
- ✅ insert 用 `("PK", pk)` 作为锁资源（避免并发同 pk insert）

### 3.4 锁升级（必须项）
- ✅ 支持 S→X upgrade
- ✅ upgrade 冲突 no-wait abort

### 3.5 undo / rollback（必须项）
- ✅ abort 逆序应用 undo
- ✅ insert/update/delete 均有对应 undo 条目
- [ ] ⚠️ 需结合 Table/Index 真正接口验证（避免 try/except 吞掉失败导致“undo 逻辑写了但没生效”）

---

## 4) Blocker 与风险点

### 4.1 【Blocker】重复加锁计数与 release_all 不一致（高概率超时/卡死）
**现状**
- 同一 txn 对同一 rid 重复 acquire：`s_count/x_count` 会递增
- 但 txn 持有资源集合 `_txn_resources[txn_id]` 是 set
- `release_all()` 对每个 rid 只释放一次 → count 只能减 1 次

**后果**
- 同一 txn 如果两次 select 同一 pk（或同 rid 被 touch 多次），commit/abort 后锁可能残留
- 后续事务永远冲突 → worker 无限重试 → 测试超时


**建议修复（明确可执行，三选一）**
1) 重复 acquire 视为幂等：若 txn 已持有该锁直接 return，不加 count  
2) `_txn_resources` 改成 multiset/list，确保 release 次数匹配 acquire 次数  
3) `release_all` 对每个 rid 循环释放直到 count 归零（while count>0）

---

### 4.2 【风险】sum/scan 不加锁（serializable 聚合/范围读可能不满足）
- 当前锁规划对 sum/scan 返回空锁集合（属于性能取舍）
- 若 tester 强测“聚合/范围读也要隔离”，可能扣分或挂测

**验收结论：⚠️ 取决于 M3 tester**

---

### 4.3 【风险】`txn=` 参数兼容性（不兼容就无限重试）
- `Transaction.run` 对 select/sum 使用 `txn=` 调用
- 若 `table.py` 的 select/sum 不支持该参数：会 TypeError → abort → retry

**验收结论：⚠️ 需你核对 Table 接口签名**

---

## 5) Table（table.py）完善度补充（结构 + 功能 + 对 B 的支撑）

> 说明：这部分不是“B 的代码”，但会决定 B 的事务/undo 是否能真正跑通、以及并发下是否存在集成漏洞。

### 5.1 Table 架构结构完整度（骨架层）
- ✅ page_directory：RID → 物理位置（RecordLocator）映射存在
- ✅ page_ranges：PageRange 列表存在，并能确保插入时扩展/选择 page range
- ✅ index：`self.index = Index(self)` 作为外部统一入口
- ✅ bufferpool 并发保护：存在 `_bp_lock`，PageRange 的 fetch/unpin 在锁下进行
- ✅ merge：存在 merge 触发与应用路径（apply_merge_if_ready / 后台 merge 逻辑）
- ✅ metadata persistence：提供 `to_metadata/from_metadata`，能恢复 table 核心状态

**结论：✅ Table “骨架 + 管理职责”完善度高，符合 M3 对 Table 的定位（管理 pages/page ranges/page_directory + merge + persistence）。**

---

### 5.2 Table 是否提供 Transaction(B) undo 所需关键接口
> 这点直接决定 B 的 abort/undo 是否能真正回滚成功。

- ✅ `read_physical_record(rid)`（读取 base/tail 物理记录）
- ✅ `read_latest_user_columns(base_rid)`（沿 tail chain 读取最新）
- ✅ `overwrite_base_indirection(base_rid, new_tail_rid)`（回滚 indirection）
- ✅ `overwrite_base_schema(base_rid, new_schema)`（回滚 schema）
- ✅ `key2rid` / `_deleted` / `index`（供锁规划与 undo 辅助使用）

**结论：✅ Table 接口层面足以支撑 B 的 undo 框架跑起来。**

---

### 5.3 Table 并发模型的现实点（
- Table 内部主要对 **bufferpool** 做了锁保护（bp_lock）
- 对 `page_directory/key2rid/_deleted/index` 等共享结构的并发一致性更依赖**事务层记录锁**（也就是说：B 的 record-level lock 覆盖必须完整）

**结论：⚠️ Table 自身不是全域线程安全，必须依赖事务锁把所有访问路径罩住。**

---

### 5.4 【集成级风险】insert 锁资源与 update/delete 锁资源不一致（建议写进报告）
**现状**
- insert：B 用 `("PK", pk)` 作为锁资源
- update/delete：B 用 `base_rid` 作为锁资源（由 `key2rid[pk]` 得到）

**潜在后果（隔离性漏洞）**
- insert 过程中 `key2rid[pk]` 可能已写入（即使事务未 commit）
- 并发 update 可能读到 `key2rid` 得到 base_rid，然后去锁 base_rid（而 insert 并没锁 base_rid）
- 从而出现“未提交记录被并发修改”的窗口（取决于时序/测试）

**建议修复方向**
- 统一锁资源：对同一逻辑记录，所有操作都使用同一锁 key（建议统一以 pk 锁，或 insert 完成后补锁 base_rid）
- 或者在 key2rid 可见性上做事务隔离（更复杂，通常作业不要求）

**结论：⚠️ 集成风险存在，建议至少在报告中注明并要求解释/修补策略。**

---


## 6) 最终判定

### 6.1 结构分
- ✅ 接口/文件结构基本满足
- ⚠️ txn= 参数兼容待核对（不兼容会直接挂）

**结构结论：✅通过（附注：需确认 txn= 签名兼容）**

### 6.2 功能分
- ✅ strict 2PL / no-wait / upgrade / retry 框架满足
- ❌ Blocker：重复 acquire 与 release_all 不一致（高概率超时/卡死）
- ⚠️ sum/scan 锁策略与 serializable 的覆盖取决于 tester
- ⚠️ insert(pk锁) vs update(baseRID锁) 的资源不一致存在集成风险

**功能结论：⚠️瑕疵 （建议修复 Blocker ）**

---

## 8) Member B 修复/补充（验收阻塞项与建议项）

### 8.1 阻塞项（高概率有问题）
1) 修复“重复加锁计数释放不一致”（见 4.1，Blocker）

### 8.2 建议项（可能影响 tester / 影响并发正确性）
2) 明确并保证 `select/sum` 接口支持 `txn=`（或在 Transaction 侧改为兼容调用）
3) 若 M3 强测聚合/范围读 serializable：补充 sum/scan 的锁策略或解释为什么 tester 不测
4) 解释并修补 insert 与 update/delete 锁资源不一致的隔离风险（统一锁 key 或补锁策略）

---

> 备注
- “结构符合 M3 skeleton，但存在并发锁释放一致性缺陷（Blocker），可能导致测试超时；”