---
name: anti-detect-browser
description: 启动反检测浏览器并通过Patchright操控。当用户需要访问网页、浏览器自动化、爬取网页、防止被检测、访问Temu/1688等电商网站时使用此技能。支持启动Chrome、连接已有浏览器、模拟人类点击。
---

# 反检测浏览器技能

## 技能概述

使用 **Patchright** (反检测 Playwright) 启动无法被网站检测到的真实 Chrome 浏览器。单层架构，无需额外依赖。

核心优势：
- Patchright 自动 patch `navigator.webdriver`、移除自动化标记
- `launch_persistent_context` 持久化用户数据（cookie、登录状态、历史记录）
- 使用真实 Google Chrome (`channel="chrome"`)，非 Chromium
- 支持模拟人类鼠标轨迹和点击行为
- 同时开启 CDP 端口，其他脚本可通过 `connect_over_cdp` 连接操控

## 触发条件

当用户有以下需求时使用此 skill：
- 访问网页、浏览网站
- 浏览器自动化任务
- 爬取/抓取网页数据
- 访问有反爬检测的网站（Temu、1688、Amazon等）
- 需要防止被网站检测为机器人
- 模拟人类浏览行为
- 需要持久化登录状态的浏览操作

## 架构说明

```
┌──────────────────────────────────────────────┐
│  Patchright launch_persistent_context        │
│  - 启动真实 Google Chrome (channel="chrome") │
│  - 自动 patch navigator.webdriver=undefined  │
│  - 移除 --enable-automation 等标记           │
│  - 持久化 user-data-dir (~/.patchright-      │
│    userdata)                                 │
│  - 开启 CDP 端口 (默认 9222)                 │
│  - 浏览器保持后台运行                         │
└──────────────┬───────────────────────────────┘
               │ CDP 端口 (可选)
┌──────────────▼───────────────────────────────┐
│  其他脚本: connect_over_cdp()                 │
│  - web_proxy_hub.py (多站点代理)              │
│  - jisilu_cdp.py (集思录注入)                 │
│  - 仅连接、不启动新浏览器                     │
│  - browser.close() 只断开连接                 │
└──────────────────────────────────────────────┘
```

### 为什么能防检测

| 检测项 | Playwright直接启动 | Patchright |
|--------|-------------------|-----------|
| `navigator.webdriver` | `true` ❌ | `undefined` ✅ |
| `--enable-automation` | 有 ❌ | 自动移除 ✅ |
| Chrome DevTools标记 | 有 ❌ | 自动patch ✅ |
| 浏览器指纹 | 异常 ❌ | 真实Chrome ✅ |
| User-data-dir | 临时目录 ❌ | 持久化 ✅ |
| Canvas/WebGL | 可能异常 ❌ | 正常GPU渲染 ✅ |

## 前置依赖

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate
pip install patchright
```

## 命令用法

### 步骤1：启动浏览器

#### 方式A: Python 脚本（推荐）

写成独立 `.py` 文件，使用 `run_in_terminal(isBackground=true)` 在后台执行：

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate
python3 .github/skills/anti-detect-browser/scripts/launch_browser.py
# 启动并访问URL
python3 .github/skills/anti-detect-browser/scripts/launch_browser.py --url "https://www.temu.com/"
# 自定义CDP端口
python3 .github/skills/anti-detect-browser/scripts/launch_browser.py --port 9333
```

#### 方式B: Shell 脚本

使用 `open -na "Google Chrome"` 启动独立 Chrome 进程：

```bash
.github/skills/anti-detect-browser/scripts/start_browser.sh
.github/skills/anti-detect-browser/scripts/start_browser.sh https://www.temu.com/
.github/skills/anti-detect-browser/scripts/start_browser.sh --port 9333
```

两种方式 CDP 端口均默认为 `9222`。

### 步骤2：在脚本中连接已运行的浏览器

当浏览器已启动后，其他脚本可通过 CDP 连接操控：

```python
from patchright.sync_api import sync_playwright

pw = sync_playwright().start()
browser = pw.chromium.connect_over_cdp('http://127.0.0.1:9222')
ctx = browser.contexts[0]
page = ctx.pages[0]

# 执行操作...
title = page.title()
print(f"页面标题: {title}")

# 断开连接（不关闭浏览器）
pw.stop()
```

### 步骤3：模拟人类点击（重要！）

对于有追踪参数的网站（如Temu），必须模拟真实鼠标轨迹：

```python
import random, asyncio

async def human_click(page, x, y):
    """模拟人类鼠标移动和点击"""
    start_x = random.randint(100, 800)
    start_y = random.randint(100, 600)
    await page.mouse.move(start_x, start_y)
    await asyncio.sleep(random.uniform(0.1, 0.3))

    mid_x = (start_x + x) // 2 + random.randint(-50, 50)
    mid_y = (start_y + y) // 2 + random.randint(-30, 30)
    await page.mouse.move(mid_x, mid_y)
    await asyncio.sleep(random.uniform(0.1, 0.2))

    await page.mouse.move(x + random.randint(-3, 3), y + random.randint(-3, 3))
    await asyncio.sleep(random.uniform(0.4, 0.8))

    await page.mouse.click(x, y, delay=random.randint(70, 140))
```

## 参数说明

### launch_browser.py

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--url` | 启动后访问的URL | 无（打开空白页） |
| `--profile` | Chrome用户数据目录路径 | `~/.patchright-userdata` |
| `--port` | CDP 调试端口 | `9222` |

## 常用操作模板

### 检查浏览器是否运行

```bash
# 验证CDP连接
curl -s http://127.0.0.1:9222/json/version | python3 -m json.tool

# 检查使用 patchright-userdata 的 Chrome 进程
pgrep -f 'patchright-userdata' && echo "运行中" || echo "未运行"
```

### 提取页面文本

```python
text = await page.evaluate('() => document.body.innerText.substring(0, 5000)')
```

### 打开新标签页

```python
new_page = await ctx.new_page()
await new_page.goto('https://example.com', wait_until='domcontentloaded', timeout=30000)
```

### 列出所有标签页

```python
for i, pg in enumerate(ctx.pages):
    print(f"Tab {i}: {pg.url[:100]}")
```

## 注意事项

1. **持久化 Profile 很重要**：`~/.patchright-userdata` 保存了 cookie 和登录状态，不要删除。
2. **CDP 端口固定为 9222**：其他脚本（web_proxy_hub、jisilu_cdp）默认连接此端口。
3. **不要设置自定义 user_agent**：Patchright 已处理反检测，自定义 UA 反而增加风险。
4. **不要添加自定义 extra_http_headers**：同上。
5. **1688搜索用GBK编码**：1688的URL参数需要GBK编码，不能用UTF-8。
6. **模拟点击而非直接导航**：对于 Temu 等有JS追踪的网站，用 `mouse.click()` 而不是 `page.goto()` 访问商品详情。
7. **避免频繁操作**：在操作之间加入随机延迟（1-3秒），避免被行为分析检测。
8. **浏览器后台运行**：launch_browser.py 自动保持后台运行，使用 Ctrl+C 或关闭终端退出。
