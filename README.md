# data-qd-225 · 数据库迁移事务执行器

服务部署时提交完整、有序的 SQLite 迁移计划。执行器对照已登记的版本和校验值，拒绝历史漂移；将本次待执行的 schema/data SQL 与版本登记在同一事务提交。批次失败回退新增变更，原版本仍可用。只接受受信任的项目 SQL，每个元素为一条语句，应避开执行器保留的 `_migration_ledger`；不提供回滚脚本或多数据库协调。

Python 3.9+，仅标准库。示例使用本仓合成迁移和临时数据库。

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

错误批次输出 `after rollback: ['id']`，改用有效计划则显示 `upgrade: [2]` 和 `retry: []`。

## 在线唯一性发布

计划中可包含一个 `OnlineUnique(version, table)`，用于给已有表的
`route_key TEXT COLLATE BINARY` 列（`id INTEGER PRIMARY KEY`）在**不停写**、
**不重扫全表**的前提下发布唯一性。它不能由 `apply_plan` 直接执行，而是走三步：

```python
from migrations import (Migration, OnlineUnique, apply_plan,
                        prepare_online_unique, step_online_unique,
                        publish_online_unique)

plan = [Migration(1, (...)), OnlineUnique(2, "routes")]

prepare_online_unique(path, plan)          # 装触发器/进度表，不登记版本
step_online_unique(path, plan, 256)        # 每次至多 limit(1..256) 个 id，返回处理数
# … 反复 step 直到返回 0（发布前的新写入也会被计入） …
publish_online_unique(path, plan)          # ("published", None) 或冲突/未就绪
```

- `prepare_online_unique` 首次返回 `True`，同计划重做返回 `False` 且不清空进度。
  它在目标表上安装增量捕获触发器（普通连接无需任何改动），进度记录在
  `_online_unique_state` 中，尚不改变写入规则。
- `step_online_unique` 按 id 升序回填 capture 表，每步一个短事务，配额为
  `limit`。捕获触发器会同步合并期间的旧节点已提交 INSERT/UPDATE/DELETE，
  包括回填游标之前新插入的更小 id；回滚的写入不会留下痕迹。
- `publish_online_unique` 在同一事务中：校验无待处理工作、按 BINARY 顺序
  取最小重键见证、写入账本 checksum、开启写入期唯一性触发器。返回
  `("not_ready", None)` / `("conflict", (key, id1, id2))` /
  `("published", None)` / `("already_published", None)`。阻塞结果不会登记版本。

发布成功后，既有旧连接再插入或改键到已被占用的非 NULL 键会抛
`sqlite3.IntegrityError` 且不留变更；改成自身原键、删除后复用键、多行 NULL
正常。`apply_plan` 仍按原口径检查前缀：发布前不能越过待发布在线版本，也不能
在 prepare 与 publish 之间用别的普通迁移推进账本；发布后完整计划可重放，并
可继续原子应用之后不触碰该唯一性的普通迁移。所有入口只依赖数据库内可索引的
工作量，单行旧写入的额外成本与表大小、重复组大小无关。

## 维护窗口内回收发布制品

计划中可以加入普通阶段
`ReleaseArtifactReclaim(version, candidate_ids, max_releases)`。候选 ID 为
1–20 个不同的正 SQLite 整数（拒绝布尔值），`max_releases` 为 1–8；版本仍须
满足计划从 1 连续编号的规则。候选顺序不参与 checksum，checksum 绑定类型、
版本、排序后的候选集合和额度。

```python
from migrations import Migration, ReleaseArtifactReclaim, apply_plan

plan = [
    Migration(1, ("CREATE TABLE releases(...)", ...)),
    ReleaseArtifactReclaim(2, [5, 2, 4, 3], max_releases=2),
]
apply_plan(path, plan)
```

执行时只选择候选中实际存在且 `retired=1` 的发布；活跃或不存在的 ID 不占用
删除额度。一个制品只有在本阶段开始时至少有一个持有者，并且全部持有者都属于
最终选中的发布集合时才删除。未列入候选、仍活跃或未选中的持有者都会阻止释放；
历史无主制品不计收益也不删除。

选择算法把每个可由候选解释的制品持有者集合编码成位掩码，在有界临时表中完成
子集 zeta 汇总，然后精确比较所有不超过额度的集合：先最大化登记字节数，收益
相同则减少发布数，仍相同则选择数值 ID 升序序列中字典序最小者。目录扫描量只
与目录/关联行数相关，不随候选组合数倍增；Python 侧只流式读取评分行，最大
候选规模下新增 Python 峰值远低于 32 MiB，主要临时空间由 SQLite 管理。

该阶段与普通 SQL、账本写入处于同一个 `apply_plan` 事务；后续阶段失败会连同
回收删除和账本记录一起回滚。准确计划重放为 no-op；改变候选集合或额度会因
checksum 不同抛出 `HistoryMismatch`。它也可以位于已发布的 `OnlineUnique`
之后；未发布的在线版本仍按原规则阻止账本推进。
