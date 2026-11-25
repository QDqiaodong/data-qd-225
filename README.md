# data-qd-225 · 数据库迁移事务执行器

服务部署时提交完整、有序的 SQLite 迁移计划。执行器对照已登记的版本和校验值，拒绝历史漂移；将本次待执行的 schema/data SQL 与版本登记在同一事务提交。批次失败回退新增变更，原版本仍可用。只接受受信任的项目 SQL，每个元素为一条语句，应避开执行器保留的 `_migration_ledger`；不提供回滚脚本、在线分阶段迁移或多数据库协调。

Python 3.9+，仅标准库。示例使用本仓合成迁移和临时数据库。

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

错误批次输出 `after rollback: ['id']`，改用有效计划则显示 `upgrade: [2]` 和 `retry: []`。
