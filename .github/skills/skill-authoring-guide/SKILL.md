---
name: skill-authoring-guide
description: Agent Skill编写规范指南。当用户询问如何创建skill、skill规范、SKILL.md格式、skill最佳实践时使用此技能。包含官方规范、模板和示例。
---

# Agent Skill 编写指南

本技能指导如何编写符合 [Agent Skills 开放标准](https://agentskills.io) 的技能文件。

## 官方规范

Agent Skills 是一个开放标准，支持 VS Code Copilot、GitHub Copilot CLI 和 GitHub Copilot coding agent。

### 目录结构

```
.github/skills/           # 项目级 skills（推荐）
~/.copilot/skills/        # 个人级 skills
```

每个 skill 是一个独立目录：

```
.github/skills/
└── my-skill/
    ├── SKILL.md          # 必需：技能定义文件
    ├── scripts/          # 可选：脚本文件
    ├── examples/         # 可选：示例文件
    └── references/       # 可选：参考资料
```

## SKILL.md 文件格式

### 必需：YAML Frontmatter

```yaml
---
name: skill-name
description: 技能描述，说明功能和触发条件
---
```

| 字段 | 必需 | 要求 |
|------|------|------|
| `name` | ✅ | 小写+连字符，最大64字符 |
| `description` | ✅ | 说明功能**和触发条件**，最大1024字符 |

### Description 编写要点

**必须包含两部分**：
1. **功能说明**：这个skill能做什么
2. **触发条件**：什么情况下应该使用

```yaml
# ✅ 好的 description
description: 筛选A股可转债下修策略标的。当用户询问可转债筛选、下修策略、转债推荐时使用此技能。

# ❌ 差的 description（缺少触发条件）
description: 筛选A股可转债下修策略标的。
```

### Body 内容结构

```markdown
# 技能标题

## 技能概述
简要说明技能的核心功能和适用场景。

## 触发条件
明确列出什么情况下使用此 skill：
- 条件1
- 条件2

## 命令用法

### 基本用法
\`\`\`bash
python script.py arg1 arg2
\`\`\`

### 参数说明
| 参数 | 说明 |
|------|------|
| `arg1` | 参数1说明 |

## 输出说明
描述脚本的输出格式和字段含义。

## 注意事项
列出使用时需要注意的问题。
```

## 模板

### 基础模板

```markdown
---
name: my-skill-name
description: 简要功能描述。当用户询问XXX、YYY时使用此技能。支持AAA、BBB功能。
---

# 技能标题

## 技能概述

本技能用于...

## 触发条件

当用户询问以下内容时使用此 skill：
- 条件1
- 条件2

## 命令用法

\`\`\`bash
cd /path/to/project && source .venv/bin/activate
python .github/skills/my-skill/scripts/main.py [参数]
\`\`\`

## 参数说明

| 参数 | 说明 |
|------|------|
| `--param1` | 参数1说明 |

## 注意事项

1. 注意事项1
2. 注意事项2
```

### 数据筛选类模板

```markdown
---
name: xxx-screener
description: 筛选A股XXX标的。当用户询问XXX筛选、XXX推荐时使用此技能。支持按YYY维度筛选。
---

# XXX筛选技能

## 技能概述

本技能用于筛选具有XXX特征的标的，帮助投资者发现：
- 特征1
- 特征2

## 核心投资逻辑

**策略假设**：条件A + 条件B → 预期结果

### 筛选条件

| 维度 | 条件 | 说明 |
|------|------|------|
| 维度1 | > X | 说明 |

## 命令用法

\`\`\`bash
python .github/skills/xxx-screener/scripts/screen.py [参数]
\`\`\`

## 输出字段

| 字段 | 说明 |
|------|------|
| `field1` | 字段1说明 |

## 风险提示

1. 风险1
2. 风险2
```

### 数据管理类模板

```markdown
---
name: xxx-manager
description: XXX数据的缓存管理工具。当用户询问XXX数据、更新XXX时使用此技能。支持从缓存读取和远端更新。
---

# XXX数据管理

## 技能概述

XXX数据的查询和管理工具，支持从 DuckDB 缓存读取和远端更新。

## 缓存策略

- 默认从缓存读取
- 用户明确要求时从远端更新
- [可选] 智能更新策略

## 命令用法

### 从缓存查询（默认）

\`\`\`bash
python .github/skills/xxx-manager/xxx_manager.py [代码]
\`\`\`

### 从远端更新

\`\`\`bash
python .github/skills/xxx-manager/xxx_manager.py update [代码]
\`\`\`

## Python API

\`\`\`python
from xxx_manager import (
    get_cached_latest,
    update_data,
)
\`\`\`
```

## 三级加载机制

Copilot 使用渐进式加载，只在需要时加载内容：

| 级别 | 加载内容 | 触发时机 |
|------|----------|----------|
| Level 1 | `name` + `description` | 始终加载（用于匹配） |
| Level 2 | SKILL.md body | 请求匹配 description 时 |
| Level 3 | 目录内其他文件 | Copilot 引用时 |

**优化建议**：
- description 要精准，便于 Level 1 匹配
- body 内容完整但精简
- 大量参考资料放 `references/` 或 `examples/`

## 命名规范

### Skill 名称

```
# 格式：功能-类型
xxx-screener      # 筛选类
xxx-manager       # 管理类
xxx-guide         # 指南类
xxx-notification  # 通知类
```

### 文件命名

```
SKILL.md          # 必需，大写
scripts/          # 脚本目录
  main.py         # 主脚本
  utils.py        # 工具函数
examples/         # 示例目录
references/       # 参考资料
```

## 最佳实践

### ✅ 推荐

1. **description 包含触发关键词** - 便于 Copilot 匹配
2. **提供完整命令示例** - 包含 cd 和 activate
3. **参数用表格说明** - 清晰易读
4. **列出输出字段** - 便于理解结果
5. **包含注意事项** - 避免常见错误

### ❌ 避免

1. **description 过于笼统** - 难以触发
2. **缺少触发条件说明** - Copilot 不知何时使用
3. **命令路径不完整** - 执行会失败
4. **缺少参数说明** - 使用困难

## 参考资源

- [Agent Skills 官方规范](https://agentskills.io)
- [VS Code Copilot Skills 文档](https://code.visualstudio.com/docs/copilot/customization/agent-skills)
- [anthropics/skills 示例仓库](https://github.com/anthropics/skills)
- [github/awesome-copilot](https://github.com/github/awesome-copilot)
