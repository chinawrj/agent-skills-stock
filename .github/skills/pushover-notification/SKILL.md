---
name: pushover-notification
description: 通过Pushover推送通知到手机。当用户询问推送消息、发送通知、Pushover、手机提醒时使用此技能。支持发送文本消息、从CSV生成报告、HTML格式化等功能。
---

# Pushover 推送通知技能

## 技能概述

本技能用于通过 Pushover 服务推送通知到手机/平板设备，适用于：
- 股票筛选结果推送
- 定时任务完成通知
- 自定义消息推送

## 配置说明

### 凭证文件

凭证存储在 `~/.pushover_credentials`：

```bash
export PUSHOVER_APP_TOKEN="your_app_token"
export PUSHOVER_USER_KEY="your_user_key"
```

### 自动加载

已配置在 `~/.zshrc` 中自动加载：

```bash
source ~/.pushover_credentials
```

## 使用方法

### 方式1：推送股票筛选结果（推荐）

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate

# 推送股东人数筛选结果
python .github/skills/pushover-notification/scripts/push_notification.py \
    --csv screened_shareholders_decrease_2periods.csv \
    --title "🔔 股东人数下降筛选" \
    --top 10

# 推送可转债筛选结果
python .github/skills/pushover-notification/scripts/push_notification.py \
    --csv screened_downrevise_bonds.csv \
    --title "📈 可转债下修筛选" \
    --top 8
```

### 方式2：发送自定义消息

```bash
# 纯文本消息
python .github/skills/pushover-notification/scripts/push_notification.py \
    -m "任务完成" \
    -t "通知标题"

# HTML 格式消息
python .github/skills/pushover-notification/scripts/push_notification.py \
    -m "<b>重要通知</b><br>任务已完成" \
    -t "📢 系统通知"
```

### 方式3：Python API 调用

```python
from push_notification import push_notification, format_shareholders_report

# 直接发送消息
result = push_notification(
    title="测试标题",
    message="<b>加粗文本</b> 普通文本",
    html=True
)

# 从 CSV 生成报告并发送
message = format_shareholders_report("screened_shareholders_decrease_2periods.csv", top_n=10)
push_notification(title="股东筛选", message=message)
```

## 命令行参数

| 参数 | 说明 | 示例 |
|------|------|------|
| `-t, --title` | 消息标题 | `--title "筛选结果"` |
| `-m, --message` | 消息内容 | `-m "测试消息"` |
| `-c, --csv` | 从 CSV 生成报告 | `--csv result.csv` |
| `-n, --top` | 显示前 N 条（默认10） | `--top 5` |
| `--no-html` | 禁用 HTML 格式 | `--no-html` |

## HTML 格式支持

Pushover 支持以下 HTML 标签：

| 标签 | 效果 | 示例 |
|------|------|------|
| `<b>` | **加粗** | `<b>重要</b>` |
| `<i>` | *斜体* | `<i>备注</i>` |
| `<u>` | 下划线 | `<u>链接</u>` |
| `<font color="">` | 颜色 | `<font color="#ff0000">红色</font>` |
| `<a href="">` | 链接 | `<a href="url">点击</a>` |

## 消息模板示例

### 股东人数筛选报告

```html
<b>📊 共筛选出 18 只股票</b>

<b>▼ 降幅前5名:</b>
<b>003041</b> 真爱美家 <font color='#ff6b6b'>-22.2%</font> (10,919人)
<b>600725</b> 云维股份 <font color='#ff6b6b'>-20.5%</font> (30,700人)

<b>📈 统计汇总:</b>
• 平均降幅: <font color='#ff6b6b'>-7.2%</font>
• 最大降幅: <font color='#ff6b6b'>-22.2%</font>
```

### 可转债筛选报告

```html
<b>📊 可转债下修策略筛选</b>

<b>▼ 重点关注:</b>
<b>127049</b> 希望转2 <font color='#4ecdc4'>120.7元</font> 下修1次
<b>113021</b> 林洋转债 <font color='#4ecdc4'>115.2元</font> 下修2次

<b>📈 筛选条件:</b>
• 价格 < 130元
• 有下修历史
```

## 脚本文件

| 脚本 | 用途 |
|------|------|
| [scripts/push_notification.py](scripts/push_notification.py) | 推送通知主脚本 |

## Pushover API 参考

- **API 端点**: `https://api.pushover.net/1/messages.json`
- **方法**: POST
- **必需参数**: `token`, `user`, `message`
- **可选参数**: `title`, `url`, `priority`, `sound`, `html`

### 优先级说明

| 值 | 说明 |
|----|------|
| -2 | 最低优先级，不产生通知 |
| -1 | 低优先级，静音通知 |
| 0 | 正常优先级（默认） |
| 1 | 高优先级，绕过静音 |
| 2 | 紧急，需用户确认 |

## 注意事项

1. **凭证安全**：`~/.pushover_credentials` 权限已设为 600
2. **消息限制**：单条消息最大 1024 字符
3. **频率限制**：每月 10,000 条免费消息
4. **HTML 转义**：特殊字符需要转义（`<` → `&lt;`）

## 获取凭证

1. 注册 Pushover 账号：https://pushover.net
2. 获取 User Key：https://pushover.net/dashboard
3. 创建应用获取 App Token：https://pushover.net/apps/build
