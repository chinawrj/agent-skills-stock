---
name: anti-detect-browser
description: 启动反检测浏览器并通过Playwright操控。当用户需要访问网页、浏览器自动化、爬取网页、防止被检测、访问Temu/1688等电商网站时使用此技能。支持启动Chrome、连接已有浏览器、模拟人类点击。
---

# 反检测浏览器技能

## 技能概述

使用 **nodriver + Playwright CDP连接** 的两层架构，启动无法被网站检测到的真实Chrome浏览器，并通过Playwright进行自动化操控。

核心优势：
- 绕过所有主流反爬检测（Temu、Cloudflare、DataDome等）
- 持久化用户数据（cookie、登录状态、历史记录）
- 支持模拟人类鼠标轨迹和点击行为
- 键盘输入完全正常

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
┌─────────────────────────────────────────┐
│  第1层: nodriver (Python)                │
│  - 启动真实 Chrome（非自动化模式）         │
│  - 自动开启 CDP 调试端口                  │
│  - 持久化 user-data-dir                  │
│  - 隐藏所有自动化标记                     │
└──────────────┬──────────────────────────┘
               │ CDP 端口
┌──────────────▼──────────────────────────┐
│  第2层: Playwright connect_over_cdp()    │
│  - 仅连接已有浏览器（不启动新浏览器）      │
│  - 执行JS、读取DOM、模拟鼠标/键盘         │
│  - browser.close() 只断开连接不关闭浏览器  │
└─────────────────────────────────────────┘
```

### 为什么能防检测

| 检测项 | Playwright直接启动 | 本方案(nodriver+CDP) |
|--------|-------------------|---------------------|
| `navigator.webdriver` | `true` ❌ | `undefined` ✅ |
| `--enable-automation` | 有 ❌ | 无 ✅ |
| Chrome DevTools标记 | 有 ❌ | 被patch ✅ |
| 浏览器指纹 | 异常 ❌ | 真实Chrome ✅ |
| User-data-dir | 临时目录 ❌ | 持久化 ✅ |
| Canvas/WebGL | 可能异常 ❌ | 正常GPU渲染 ✅ |

## 前置依赖

```bash
# Playwright（用于CDP连接操控）
cd /Users/rjwang/fun/a-share && source .venv/bin/activate
pip install playwright
playwright install chromium

# nodriver（可选，备用启动方式）
pip install nodriver
```

## 命令用法

### 步骤1：启动浏览器（推荐方式 - shell脚本）

使用 `open -na "Google Chrome"` 启动，浏览器作为**完全独立进程**运行，和你手动双击Chrome一模一样。可以日常使用，积累真实浏览历史，让profile更逼真。

```bash
# 默认启动（CDP端口9222）
.github/skills/anti-detect-browser/scripts/start_browser.sh

# 启动并访问URL
.github/skills/anti-detect-browser/scripts/start_browser.sh https://www.temu.com/

# 自定义CDP端口
.github/skills/anti-detect-browser/scripts/start_browser.sh --port 9333 https://www.temu.com/
```

CDP端口固定为 `9222`（可通过 `--port` 修改），不再需要每次查找。

#### 备用方式：nodriver启动

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate
nohup python3 .github/skills/anti-detect-browser/scripts/launch_browser.py --url "https://www.temu.com/" > /tmp/nodriver.log 2>&1 &
# nodriver随机分配端口，需要查找：
ps aux | grep 'remote-debugging-port' | grep -v grep | head -1 | grep -oE 'remote-debugging-port=[0-9]+' | cut -d= -f2
```

### 步骤2：通过Playwright连接浏览器

```python
import asyncio
from playwright.async_api import async_playwright

CDP_PORT = 9222  # start_browser.sh 默认端口

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(f'http://127.0.0.1:{CDP_PORT}')
        ctx = browser.contexts[0]
        page = ctx.pages[0]

        # 执行操作...
        title = await page.title()
        print(f"页面标题: {title}")

        # 断开连接（不关闭浏览器）
        # 注意：不要用 await browser.close()，直接让上下文管理器退出即可

asyncio.run(main())
```

### 步骤3：模拟人类点击（重要！）

对于有追踪参数的网站（如Temu），必须模拟真实鼠标轨迹：

```python
import random, asyncio

async def human_click(page, x, y):
    """模拟人类鼠标移动和点击"""
    # 1. 从随机位置开始
    start_x = random.randint(100, 800)
    start_y = random.randint(100, 600)
    await page.mouse.move(start_x, start_y)
    await asyncio.sleep(random.uniform(0.1, 0.3))

    # 2. 移动到中间点（非线性轨迹）
    mid_x = (start_x + x) // 2 + random.randint(-50, 50)
    mid_y = (start_y + y) // 2 + random.randint(-30, 30)
    await page.mouse.move(mid_x, mid_y)
    await asyncio.sleep(random.uniform(0.1, 0.2))

    # 3. 移动到目标（加随机偏移）
    await page.mouse.move(x + random.randint(-3, 3), y + random.randint(-3, 3))
    await asyncio.sleep(random.uniform(0.4, 0.8))

    # 4. 点击（mousedown到mouseup有延迟）
    await page.mouse.click(x, y, delay=random.randint(70, 140))
```

## 参数说明

### launch_browser.py

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--url` | 启动后访问的URL | 无（打开空白页） |
| `--profile` | Chrome用户数据目录路径 | `data/nodriver-profile` |

## 常用操作模板

### 检查浏览器是否运行

```bash
# 检查使用反检测profile的Chrome是否在运行
pgrep -f 'nodriver-profile' && echo "运行中" || echo "未运行"

# 获取CDP端口
ps aux | grep 'nodriver-profile' | grep -v grep | grep -oE 'remote-debugging-port=[0-9]+' | head -1 | cut -d= -f2

# 验证CDP连接
curl -s http://127.0.0.1:9222/json/version | python3 -m json.tool
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

1. **不要用 `browser.close()`**：这会断开Playwright连接但不会关闭Chrome。如果调用了`await browser.close()`会报coroutine错误，让上下文管理器自然退出即可。
2. **CDP端口每次启动不同**：nodriver随机分配端口，每次需要通过`ps aux`查找。
3. **持久化Profile很重要**：`data/nodriver-profile/` 目录保存了cookie和登录状态，不要删除。
4. **1688搜索用GBK编码**：1688的URL参数需要GBK编码，不能用UTF-8。
5. **模拟点击而非直接导航**：对于Temu等有JS追踪的网站，用`mouse.click()`而不是`page.goto()`访问商品详情，这样会自动携带追踪参数，更像真人。
6. **避免频繁操作**：在操作之间加入`asyncio.sleep()`随机延迟（1-3秒），避免被行为分析检测。
7. **浏览器后台启动**：使用`nohup ... &`确保浏览器在终端关闭后继续运行。
