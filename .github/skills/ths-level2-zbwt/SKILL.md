---
name: ths-level2-zbwt
description: 从同花顺至尊版提取Level-2逐笔委托数据。当用户需要获取逐笔委托、Level-2委托明细、同花顺数据提取、逐笔成交数据、委托流水时使用此技能。支持任意A股标的全天数据提取，含断线自动续传和数据完整性校验。
---

# 同花顺 Level-2 逐笔委托提取

## 技能概述

从同花顺至尊版（Android）提取任意A股标的的 Level-2 逐笔委托全天数据。通过 Frida 注入 + ADB 自动化 + TCP socket 传输实现高效采集，支持断线自动续传和崩溃恢复。

## 触发条件

当用户询问以下内容时使用此 skill：
- 获取某只股票的逐笔委托数据
- 提取 Level-2 委托明细
- 同花顺数据提取/采集
- 逐笔成交、委托流水分析

## 前置条件

| 依赖项 | 要求 |
|--------|------|
| Android 设备 | 已 root（Magisk），USB 调试已开启 |
| 同花顺至尊版 | `com.hexin.plat.android.supremacy`，已登录且有 Level-2 权限 |
| frida-server | 已部署到 `/data/local/tmp/frida-server`，版本与 PC 端 frida CLI 一致 |
| frida CLI | `pip install frida-tools`（注意：当前环境在 python3.11 下安装） |
| adb | 已连接设备，`adb devices` 可见 |

### 设备参数（当前已验证）

- Pixel 8a, 屏幕 1080×2400, Android 16 (SDK 36)
- Frida v17.7.3（server + client 版本必须匹配）

## 命令用法

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate
python3 .github/skills/ths-level2-zbwt/scripts/extract_zbwt_tcp.py <股票代码> [日期YYYYMMDD]
```

### 示例

```bash
# 提取今天的兆新股份逐笔委托（日期自动取当天）
python3 .github/skills/ths-level2-zbwt/scripts/extract_zbwt_tcp.py 002256

# 指定日期
python3 .github/skills/ths-level2-zbwt/scripts/extract_zbwt_tcp.py 600519 20260407
```

### 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `stock_code` | ✅ | 6位股票代码，如 `002256` |
| `date` | ❌ | 日期 YYYYMMDD，默认今天 |

## 工作原理

### 架构

```
Python 编排器 (extract_zbwt_tcp.py)
  ├── ADB: 启动 app → 搜索导航到目标股票 → 逐笔委托页面
  ├── ADB: 启动 frida-server
  ├── Frida: 注入 _zbwt_full.js 到同花顺进程
  │     ├── Hook Level2ZbwtRecyclerView.onHistoryDataReceive
  │     ├── 通过 zlo.i(seq, oid) 获取历史分页数据
  │     └── TCP socket → Python (端口 19876)
  └── Python: 接收数据 → 去重排序 → 保存 CSV
```

### 数据流

1. **导航**：搜索栏输入股票代码 → 进入股票详情页 → Level-2 → 逐笔委托
2. **注入**：Frida hook `onHistoryDataReceive` 回调，拦截每页数据
3. **翻页**：从最新数据向历史逐页翻，每页通过 `zo.i(seq, oid)` 请求上一页
4. **传输**：数据通过 TCP socket (adb reverse) 实时传回 Python
5. **续传**：每次断线后从上次的 seq/oid 处恢复，直到到达 09:15:00

### 关键技术点

- **`--timeout inf`**：Frida CLI 必须使用此参数，否则长时间会话会超时退出
- **TCP socket**：替代 `console.log`，解决 Frida IPC 瓶颈导致的数据丢失
- **adb reverse**：`adb reverse tcp:19876 tcp:19876` 让设备端连接映射到 PC
- **StrictMode 禁用**：在主线程和回调线程都需要禁用，否则网络操作会抛异常

### SparseArray 字段映射

| Key | 含义 | 示例 |
|-----|------|------|
| 0 | 时间 | `09:30:01` |
| 10 | 价格 | `3.78` |
| 13 | 手数 | `100` |
| 12 | 方向 | `1`=买, 其他=卖 |
| 56 | 序列号 | `12345` |
| 1 | 委托号 | `67890` |

## 输出说明

### 输出文件

输出到项目根目录：`zbwt_<股票代码>_<日期>.csv`

### 输出字段

| 字段 | 说明 | 示例 |
|------|------|------|
| 时间 | HH:MM:SS | `09:30:01` |
| 价格 | 委托价格 | `3.78` |
| 手数 | 委托数量（手） | `100` |
| 方向 | 买/卖 | `买` |
| 序列号 | 交易所序列号 | `12345` |
| 委托号 | 委托单号 | `67890` |

### 完整性校验输出

提取完成后自动打印：
- 原始条数 vs 去重后条数、重复率
- 时间覆盖范围（是否覆盖 09:15:00 ~ 14:59:59）
- 序列号跳跃数量

## 进度恢复

脚本在每次会话后自动保存进度到 `.progress.json` 和 `.raw.csv` 文件。如果脚本崩溃或被中断，重新运行相同命令会自动从断点恢复。提取完成后进度文件自动清理。

## 文件清单

| 文件 | 说明 |
|------|------|
| `scripts/extract_zbwt_tcp.py` | Python 编排器（主入口） |
| `scripts/_zbwt_full.js` | Frida 注入脚本（stock-agnostic） |

## 注意事项

1. **盘后使用**：逐笔委托数据在盘后仍可提取（历史数据），无需在交易时间运行
2. **Level-2 权限**：同花顺账号必须有 Level-2 权限，否则逐笔委托页面无数据
3. **屏幕分辨率**：导航坐标基于 1080×2400 屏幕，其他分辨率需调整
4. **弹窗处理**：脚本会自动检测并关闭"开通夜盘交易"等弹窗，但新类型弹窗可能需要在 `dismiss_popups()` 的 `popup_keywords` 中添加关键词
5. **Frida 版本**：frida-server 和 frida CLI 版本必须完全匹配
6. **数据量**：一只活跃股票全天约 5~10 万条逐笔委托，提取通常需要 10~15 个会话
