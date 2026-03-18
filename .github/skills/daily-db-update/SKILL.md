---
name: daily-db-update
description: "⚠️ 已合并到 db-manager skill。当用户需要更新数据库、刷新股票行情、更新可转债数据、运行每日快照、盘后数据更新时，请使用 db-manager skill。"
---

# ⚠️ 此 Skill 已合并到 `db-manager`

所有数据库操作（建库 + 每日/每周维护）已统一到：

```
.github/skills/db-manager/SKILL.md
.github/skills/db-manager/manage.py
```

## 快速对照

| 旧用法 | 新用法 |
|--------|--------|
| `python db/daily_update.py` | `python .github/skills/db-manager/manage.py daily` |
| `python db/daily_update.py --step bonds` | `python .github/skills/db-manager/manage.py daily --step bonds` |
| `python db/daily_update.py --dry-run` | `python .github/skills/db-manager/manage.py daily --dry-run` |
