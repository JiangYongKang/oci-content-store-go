# 本地镜像内容存储与回收（oci-content-store-go）

用本地目录模拟镜像仓库的内容存储（content-addressable store）。支持：

- 按 **sha256 内容摘要** 写入/读取，重复内容去重，只保留一份物理副本；
- **标签 → 清单 → 配置/层** 的引用关系维护；
- 标记-清除（mark-and-sweep）**垃圾回收**，仅回收不被任何标签间接引用的内容；
- 写/回收中断后**重启自恢复**，未完成内容不参与去重与引用统计；
- 基于 `flock(2)` 的**跨进程 + 跨 goroutine** 读写锁，保证并发下的一致性。

## 1. 磁盘布局与存储格式

```
<root>/
  blobs/sha256/<64位小写hex>   # 已提交、不可变的内容；文件名即摘要
  ingest/upload-<随机>         # 写入中的临时文件，任何情况下对外不可见
  state.json                   # 标签表：tag -> manifest digest
  lock                         # flock 锁文件（进程间互斥）
```

- **Blob 不可变**：文件名由内容 sha256 决定，同摘要即同内容，因此无需版本号。
- **清单以规范化 JSON 存储**：`PutManifest` 先解析并校验，再以稳定的紧凑形式
  `{"schemaVersion":2,"config":{"digest":"..."},"layers":[{"digest":"..."}...]}`
  重新序列化，blob 摘要按该规范形式计算。
- **state.json**：
  ```json
  {
    "version": 1,
    "tags": { "v1": "sha256:abcd…", "latest": "sha256:…" }
  }
  ```
  以“同目录临时文件 + `fsync` + `rename` + 目录 `fsync`”原子替换。
  `version` 为格式版本；更高版本的 state 会被旧程序拒绝，旧版本可直接读取，
  历史数据无需迁移或重建。

## 2. 引用规则

引用图为三层：

```
tag ──▶ manifest ──▶ config
                  └─▶ layer(0..n)
```

- 只有 **tag** 是根（GC root）。
- 一个 manifest 必须引用恰好一个 config 和零或多个 layer；
  层摘要不得重复、不得与 config 相同。
- 同一份 config/layer 可被多个 manifest 引用，共享一份存储（去重）。
- 标签只能指向**已存储且能解析为清单**的 blob，防止任意 blob 借标签“免疫回收”。

## 3. 回收（GC）触发条件与算法

触发：显式调用 `Store.GC(ctx)`。回收不会在写入路径上隐式发生，调用时机由
使用方决定（如推送高峰后、定时任务）。

算法（全程持有排他锁）：

1. **Mark**：从 `state.json` 的每个 tag 出发，读取其 manifest，标记 manifest
   本身及其 config、所有 layers 为可达；
2. **Sweep**：遍历 `blobs/sha256/`，删除所有“文件名是合法 sha256 摘要且不在
   可达集合中”的文件，随后 `fsync` 目录。非摘要命名的外来文件一律不动。

后果：

- 未打标签的 blob、未打标签的 manifest 立即可被回收；
- 删除/移动某 tag 后，其独占的 manifest/config/layer 变为垃圾；
  与其他 tag 共享的层会继续保留；
- GC 幂等，对稳定的可达集合重复执行删除 0 个。

## 4. 写入两阶段提交（原子性与不留残缺）

`PutBlob(digest, size, reader)`：

1. 在 `ingest/` 建临时文件，边写边计算 sha256；
2. 写完后依次校验 **声明长度** 与 **声明摘要**，并 `fsync`；
3. 提交阶段持排他锁：目标 blob 已存在则删除临时文件并**复用**（去重）；
   否则 `rename` 进 `blobs/sha256/` 并 `fsync` 目录。

任何失败（摘要不符、长度不符、流提前结束、上下文取消、IO 错误）都会删除
临时文件并返回，**绝不会**在 `blobs/` 留下可见、可被后续去重复用的残缺内容。

## 5. 并发语义

- 一把 `lock` 文件上的 `flock` 同时提供**跨进程**与**跨 goroutine** 的
  共享/排他锁；进程内用引用计数共享同一把 flock，用条件变量等待并配合
  “排他等待计数”防止读者饿死写者。
- **写操作**（PutBlob 提交、PutManifest、Tag/Untag、GC、Open 恢复）持排他锁；
  **读操作**（ResolveTag、Stats、GetManifest）持共享锁，多读者可并发。
- `GetBlob` 在 reader **打开期间**持有共享锁，直到调用方 `Close()`。
  因此 GC 与未关闭的 reader 互斥：**引用有效期间内容必定可读，绝不会被误回收**。
- 锁不可重入：所有公开方法最多加一次锁、不嵌套；`flock` 等待可能阻塞，
  传入的 `ctx` 仅在数据拷贝阶段被检查（等待锁时不响应取消）。
- 一个良性竞争窗口：blob 已上传但尚无 tag 引用时，它本就属于“未引用”，
  可能被并发 GC 回收；推送方应在一次装配中尽快完成“传层→传清单→打标签”，
  若在窗口内被回收（返回 `ErrReferencedBlobMissing`），重试即可。
  一旦 `Tag` 成功返回，整个闭包即受保护。

## 6. 失败分类

所有失败均可用 `errors.Is` 区分：

| 哨兵错误 | 触发条件 |
| --- | --- |
| `ErrInvalidDigest` | 摘要串格式非法或算法不受支持（仅支持小写 `sha256:` + 64 hex） |
| `ErrSizeMismatch` | 实际字节数 ≠ 声明长度，或输入流短于声明长度 |
| `ErrDigestMismatch` | 实际 sha256 ≠ 声明摘要 |
| `ErrInvalidContent` | 清单为空/非 JSON/含未知字段/摘要非法/层重复或与 config 相同；或 state.json 损坏、标签目标不是清单 |
| `ErrReferencedBlobMissing` | 清单引用的 config/layer 尚未存储（含被并发回收的良性竞争） |
| `ErrInvalidTag` | 标签为空、含 `/` 或控制字符、超过 128 字符等 |
| `ErrNotFound` | 读取未存储/已回收的 blob，或解析不存在的 tag |

失败信息都带具体上下文（实际 vs 声明值、出错层下标、缺失摘要等），
可解释、可区分。

## 7. 中断恢复（Open 时自动执行）

重新 `Open` 时持排他锁执行：

1. 清空 `ingest/`：上次崩溃遗留的半成品从不参与去重/引用统计；
2. 扫描 `state.json`：删除指向“缺失 manifest”或“摘要格式非法”的悬挂标签；
3. 其余历史数据原样保留，立即可读，不做重建。

GC 只做 unlink、不修改 state.json，因此在 GC 中途崩溃也是安全的：重启后
标签引用依旧成立，再跑一次 GC 即可完成剩余清除。

## 8. API 概览

```go
s, _ := contentstore.Open(ctx, "/var/lib/imagestore")
s.SetLogger(log.Default())                         // 可选：打印关键操作/判定

err := s.PutBlob(ctx, "sha256:…", size, reader)    // 校验+去重+原子提交
rc, err := s.GetBlob(ctx, "sha256:…")              // 打开期间受 GC 保护
defer rc.Close()

md, err := s.PutManifest(ctx, manifestJSON)        // 校验引用存在后存储
_, err = s.Tag(ctx, "v1", md.String())             // 打标签
d, m, err := s.GetManifest(ctx, "v1")              // 解析标签->清单
n, err := s.GC(ctx)                                // 标记-清除，返回删除数
st, _ := s.Stats(ctx)                              // blob/tag 计数
```

## 9. 验证方法

```bash
go test ./...                 # 全部用例
go test -race ./...           # 竞态检测
go test -race -count=N ./...  # 重复 N 次，验证稳定复现
go vet ./...
```

测试覆盖：

- `TestPutGetAndDedup`：重复写入复用同一物理副本；
- `TestPutFailuresLeaveNoResidue`：摘要不符/长度不符/流截断/非法摘要，
  失败后 `blobs/` 与 `ingest/` 均无残留、摘要不可读；
- `TestManifestValidation`：各类结构非法清单与缺失引用，错误分类正确；
- `TestGCReclaimsOnlyUnreferenced` / `TestGCIdempotent`：只回收未引用内容，
  共享层保留，幂等；
- `TestConcurrentDedup`：16 goroutine 并发写相同内容，最终恰为一份；
- `TestConcurrentInterleaved`：写者/读者/GC 持续交织（`-race`），
  校验“打开即受保护、读到内容摘要必匹配、标签只指向已提交清单”，
  并区分良性 NotFound 与真实损坏；
- `TestRecoverFromInterruptedIngest` / `TestRecoverPrunesDanglingTag` /
  `TestRecoverFromInterruptedGC` / `TestStateFileFormatStable`：
  各类中断恢复与格式兼容；
- `TestCrossProcessLock`：子进程持锁时，父进程 GC 被 `flock` 阻塞，
  直到子进程释放才完成。

测试日志通过 `Store.SetLogger` 接入 `t.Logf`，`go test -v` 可看到每次
提交、去重复用、拒绝原因、GC mark/sweep 判定依据等关键操作。
