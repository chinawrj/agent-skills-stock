# 东方财富 Android App — 协议逆向分析报告

## 概要

通过对 JADX 反编译的东方财富 Android APK 源码 (`data/apk/decompiled_real/sources/`) 与抓包数据的交叉分析，定位了三个 TCP 连接的协议实现。

---

## 1. 端口 1862 — XMPP 推送协议

### 配置来源

**文件**: `com/eastmoney/config/PushConfig.java` (line 103)
```java
this.defaultConfig = new HostPort("gate.push.eastmoney.com", 1862);
this.testConfig = new HostPort("61.129.249.32", 1862);
```

### 协议实现

- **协议类型**: XMPP (基于 `org.jivesoftware.smack.XMPPConnection`)
- **管理器**: `com/eastmoney/android/push/sdk/c/f.java` (XmppManager)
  - 构造函数接收 `host, port, userName, passWord`
  - 使用 Smack XMPP 客户端库建立连接
- **Service 入口**: `com/eastmoney/android/push/sdk/PushService.java`
  - 从 Intent extras 获取连接参数: `pushHost`, `pushPort`, `userName`, `passWord`
  - 注册了 JNI native 方法: `onCreate`, `onDestroy`, `onStartCommand`

### 服务器动态更新

**文件**: `com/eastmoney/filesum/c.java` (FileSumManager, line 460)
```java
ServerInfo serverInfo = (ServerInfo) listA.get(new Random().nextInt(listA.size()));
d.b("FileSumManager", "change push host : " + serverInfo.host + "  port : " + ((int) serverInfo.port));
PushConfig.hostAndPort.update(new PushConfig.HostPort(serverInfo.host, serverInfo.port));
```
Push 服务器地址从 `serverlist7.dat` 的 `[push]` section 随机选取。

### 与抓包数据的关联

抓包中 port 1862 的 `{` / `}` (0x7b/0x7d) 分隔符对应 XMPP XML 流的 `<stream:stream>` 等 XML 标签。XMPP 是基于 XML 的协议，`<` (0x3C) 和 `>` (0x3E) 是其自然分隔符。如果观察到 0x7b/0x7d 则可能是 XMPP extension 中的 JSON payload。

---

## 2. 端口 9696 — 行情 Socket 协议 (Linux Quote Server)

### 配置来源

Port 9696 **未硬编码**在 Java 源码中。行情服务器地址（含端口）由外部配置动态加载：

**文件**: `com/eastmoney/config/ServerListConfig.java` (line 119)
```java
this.defaultConfig = IniData.fromString(a.f(aw.b("serverlist7.dat")));
```

**文件**: `com/eastmoney/filesum/ServerListReader.java` (line 59)
- 从 `https://swdlcdn.eastmoney.com/sj/android/serverlist7.dat` 下载
- 下载后 **反转** (`reverse()` 按 20-byte chunk) 再 **zlib inflation** 解压
- 解析成 `IniData` (INI 格式)，包含 `[linux]`, `[windows]`, `[history]` 等 section
- 每个 section 中有 `host:port` 格式的服务器条目

### 服务器类型定义

**文件**: `com/eastmoney/android/sdk/net/socket/protocol/nature/Nature.java`
```java
public enum ServerType {
    WINDOWS, LINUX, HISTORY, LINUX_SUPER_L2, LINUX_EXTENSION, FUTURE, NULL
}
```

**文件**: `com/eastmoney/config/ManualServerListConfig.java`
- `manualLinuxServerList` → "Linux服务器手动配置"
- `manualWindowsServerList` → "Windows服务器手动配置"
- `manualFutureServerList` → "Future服务器手动配置"
- `manualHistoryServerList` → "History服务器手动配置"
- `manualLinuxSuperL2ServerList` → "超级L2 Linux服务器手动配置"
- `manualLinuxExtensionServerList` → "Linux扩展行情服务器配置"

`ManualServerInfo` 结构: `{ String host, short port, boolean isFee }`

### 协议编号系统 (Nature Annotation)

每个协议请求通过 `@Nature(a = ServerType, b = protocolNumber)` 注解声明:

| 协议号 | 类文件 | ServerType | 说明 |
|--------|--------|------------|------|
| 5055 | `protocol/k/a.java` (P5055) | LINUX | 基础股票数据 |
| 5215 | `protocol/ah/a.java` (P5215) | LINUX_SUPER_L2 | L2逐笔数据 |
| 5231 | `protocol/ar/a.java` (P5231) | LINUX_SUPER_L2 | L2扩展数据 |
| 7016 | `protocol/p7016/a.java` | FUTURE | 期权数据 |
| 7031 | `protocol/p7031/a.java` | FUTURE | 期货数据 |

### 协议字段类型 (Parser Types)

从 P5055 (`protocol/k/a.java`) 可见字段声明方式:
```java
public static final a<String, h> b = a.a("$code", h.a);           // 字符串 parser
public static final a<Short, sdk.e.a.h> c = a.a("$returnNumber", sdk.e.a.h.b); // Short parser
public static final a<Integer, d> d = a.a("$expiration", d.b);     // Integer parser
public static final a<Integer[], parser.a<Integer>> e = a.a("$expirations", parser.a.a(d.b)); // Integer数组 parser
```

Parser 类型映射:
- `lib.net.socket.parser.h` → String 类型
- `lib.net.socket.parser.a.e` → Byte 类型
- `lib.net.socket.parser.a.c` → Integer (32-bit) 类型
- `lib.net.socket.parser.a.g` → Long (64-bit) 类型
- `lib.net.socket.parser.b` → 固定长度 byte[] 类型 (如 `b.a(33)` = 33字节)
- `lib.net.socket.parser.a` → 数组类型 wrapper

### 连接管理

**文件**: `com/eastmoney/android/sdk/net/socket/EmSocketManager.java` — **未在反编译源码中** (属于预编译 lib)
- 被 `TradeModule`, `Berlin` DNS manager, `FileSumManager`, `ServerListReader` 广泛引用
- `EmSocketManager.a()` 是行情 socket 管理的单例
- `EmSocketManager.ServerListSource` 提供服务器列表来源管理

### 与抓包数据的关联

Port 9696 连接是 Linux 行情服务器的自定义二进制协议。抓包中的 3-byte length-prefix 帧格式对应 `lib.net.socket` 中的封包逻辑（该库未在反编译源中，属于预编译 AAR）。

---

## 3. 端口 5656 — IM/消息中心 HTTPS 协议

### 配置来源

**文件**: `com/eastmoney/config/IMBulletConfig.java` (line 71)
```java
IM_SOCKET_HOSTNAME default = "emimpcpf.eastmoney.com"
IM_SOCKET_HOSTNAME test    = "imcpftest.eastmoney.com"
IM_SOCKET_HOSTNAME grey    = "imcpftest2.eastmoney.com"
```

**文件**: `com/eastmoney/config/MsgCenterConfig.java` (line 187)
```java
"https://" + d.a().a("imcpf", IMBulletConfig.IM_SOCKET_HOSTNAME.get()) + ":5656"
// test 环境使用 port 16060
```

### 协议实现

这是 **HTTPS** 长连接，不是原始 TCP socket:
- URL 模式: `https://emimpcpf.eastmoney.com:5656/Msg/...`
- 用于 IM 弹幕、消息推送
- 抓包中观察到 TLS 连接 (SNI: `emimpcpf-hspre.eastmoney.com`) 符合 HTTPS 协议

---

## 4. 交易 Socket 协议 (Finance TCP)

### 配置来源

**文件**: `com/eastmoney/android/trade/finance/tcp/server/FinanceServerInfo.java`
- 从 JSON 配置解析: `"financetcpserver-1"`, `"financetcpserver-2"` 格式为 `"host:port"`
- 字段: `host` (String), `port` (short), `isMaster`, `isGM`, `isIPv6`
- 支持国密 (GM) 模式: `GMSSLSocketFactory`

### 连接管理

**文件**: `com/eastmoney/android/trade/finance/tcp/a.java` (FinanceSocketManager)
```java
socket.connect(new InetSocketAddress(str, s), iR);  // str=host, s=port
socket.setKeepAlive(true);
socket.setSoTimeout(30000);
```

### 握手流程:
1. 首包: RSA 公钥交换 (或 SM2 for 国密)
2. 建立 DES (或 SM4) 对称加密通道

### 心跳机制

**文件**: `com/eastmoney/android/trade/finance/tcp/protocol/function/FP_heartbeat.java`
```java
@a(a = "heartbeat", c = 15, d = false)
public class FP_heartbeat extends c<Request, Response>
```
- 协议功能 ID: **15**
- 空请求/空响应（纯 keepalive）

### 协议基类

**文件**: `com/eastmoney/android/trade/finance/tcp/protocol/function/c.java` (FinanceBaseProtocol)
```java
public abstract class c<REQ, RESP> extends com.eastmoney.android.lib.net.socket.a<e, byte[]>
```
- 所有 finance TCP 协议继承自此类
- `a(b, byte[])` = 解码响应 (decode)
- `a(b, e)` = 编码请求 (encode)
- **方法体均未反编译** (`return null / throw UnsupportedOperationException`)

---

## 5. 交易配置 Socket 协议 (PackageComposer) — 最完整的帧格式代码

### 完整帧格式

**文件**: `com/eastmoney/android/trade/configsocket/protocol/a.java` (PackageComposer)

这是反编译源码中**唯一完整可读的协议帧格式**:

#### 请求帧结构:
| 偏移 | 长度 | 字段 | Parser 类型 | 说明 |
|------|------|------|-------------|------|
| 0 | 33 | packageId | `parser.b(33)` | UUID (截取33字节) |
| 33 | 8 | desKey | `parser.b(8)` | 随机 DES 密钥 |
| 41 | 1 | type | `parser.a.e` | 固定 = 1 |
| 42 | 4 | bodyLength | `parser.a.c` | 加密体长度 |
| 46 | 1 | cipher | `parser.a.e` | 加密标志 = 1 |
| 47 | 1 | compress | `parser.a.e` | 压缩标志 = 0 |
| 48 | 8 | CRC32 | `parser.a.g` | 对 0-47 字节的 CRC32 |
| 56 | N | body | raw bytes | DES 加密的请求体 |
| 56+N | 4 | trailer | `parser.b(4)` | `{0x00, 0x00, 0x0D, 0x0A}` |

#### 响应帧结构:
| 偏移 | 长度 | 字段 | 说明 |
|------|------|------|------|
| 0 | 33 | packageId | |
| 33 | 8 | desKey | |
| 41 | 1 | type | |
| 42 | 4 | bodyLength | |
| 46 | 1 | cipher | |
| 47 | 1 | compress | |
| 48 | 4 | CRC32 | 校验 bytes[0:48] |
| 52 | N | body | DES 加密的响应体 |
| 52+N | 4 | trailer | |

**注意**: 请求的 CRC32 用 Long (8字节) 写入，响应解析时 body 从 offset 52 开始 (48 header + 4 bytes CRC32)，暗示响应 CRC32 实际只占 4 字节，或读取方式不同。

#### 加密:
```java
byte[] desKey = DesUtils.generateRandomPrintKey();     // 8 字节随机可打印密钥
byte[] encrypted = DesUtils.encrypt(body, desKey);     // DES CBC 加密
byte[] decrypted = DesUtils.decrypt(encrypted, desKey); // DES CBC 解密
```

### TradeConfigBaseProtocol
**文件**: `com/eastmoney/android/trade/configsocket/protocol/b.java`
- 另一种协议：请求/响应为 **JSON** 格式 (使用 Gson)
- 请求序列化: `Gson.toJson(request)` → `String.getBytes()`
- 响应反序列化: `new String(bytes)` → `Gson.fromJson(json, type)`

---

## 6. 关键缺失：协议帧解析基础库

### `com.eastmoney.android.lib.net.socket` — 不在反编译源中

这是整个协议栈的核心基础库，包含:
- `EmSocketManager` — 行情 socket 连接池管理
- `ServerInfo` — 服务器信息 (host/port/type)
- `a<IN, OUT>` — 协议基类 (encode/decode)
- `b` — Socket 会话/上下文
- `parser.*` — 二进制解析器族:
  - `parser.h` → String
  - `parser.b` → 固定长度 byte[]
  - `parser.a.e` → Byte
  - `parser.a.c` → Integer
  - `parser.a.g` → Long
  - `parser.a` → 数组 wrapper

此包在以下位置**均不存在**:
- `decompiled_real/sources/com/eastmoney/android/lib/net/` — 目录为空
- `eastmoney_decompiled/sources/com/eastmoney/android/lib/net/` — 目录不存在

这表明该库是一个 **独立的预编译 AAR/JAR**，未被包含在已 dump 的 DEX 文件中，或者被 VMP (Virtual Machine Protection, 如 `libdexvmp.so`) 保护。

### Native Libraries

可能包含协议处理代码的 native 库:
| 文件 | 可能用途 |
|------|---------|
| `libclientencode.so` | 客户端加密编码 |
| `libclientencode_wrap.so` | 加密编码 wrapper |
| `libEmse-lib.so` | Eastmoney SE 库 (可能含 socket 核心) |
| `libemutilc.so` | EM 工具 C 库 |
| `libcosign.so` | 签名库 |
| `libkeystore.so` | 密钥存储 |
| `libdexvmp.so` | DEX VMP 保护 |
| `libllvminteractive.so` | LLVM 交互式保护 |
| `libexec.so` / `libexecmain.so` | ijm 执行库 |

---

## 7. 服务器列表获取流程

**文件**: `com/eastmoney/filesum/ServerListReader.java`

```
下载 serverlist7.dat (https://swdlcdn.eastmoney.com/sj/android/)
    ↓
reverse(bytes, 20-byte chunks)  ← 按 (len%20+20) 为 chunk 做字节反转
    ↓
zlib.inflate(reversed)          ← zlib 解压
    ↓
IniData.fromString(text)        ← 解析 INI 格式
    ↓
sdk.net.socket.server.b.a(iniData)  ← 提取服务器列表
    ↓
EmSocketManager.a().a(serverList)   ← 更新行情连接池
PushConfig.hostAndPort.update(...)  ← 更新推送服务器
```

---

## 8. 总结表

| 端口 | 协议 | 域名 | Java 入口 | 帧格式 |
|------|------|------|-----------|--------|
| 1862 | XMPP over TCP | gate.push.eastmoney.com | `push/sdk/c/f.java` (XmppManager) | XML stream (Smack XMPP) |
| 9696 | 自定义二进制 | 从 serverlist7.dat 动态加载 | `lib.net.socket` (预编译，不可见) | Length-prefix (3 bytes?) |
| 5656 | HTTPS | emimpcpf.eastmoney.com | `config/MsgCenterConfig.java` | TLS + HTTP |
| 动态 | Finance TCP | 从 JSON 配置加载 | `trade/finance/tcp/a.java` | Binary + DES/SM4 加密 |
| 动态 | Config Socket | jyconfig server | `trade/configsocket/protocol/a.java` | 56-byte header + DES + CRC32 |

### 抓包 0x7b/0x7d 分析

- 0x7b = `{`, 0x7d = `}` — 在 XMPP 协议 (port 1862) 中，可能对应:
  1. XMPP extension 中的 JSON payload `{...}`
  2. 自定义 XMPP framing layer
- 在 config socket 协议中，trailer 为 `{0x00, 0x00, 0x0D, 0x0A}` (非 0x7b/0x7d)
- **0x8d 未在任何 Java 源码中找到** — 可能在 native 库或 XMPP Smack 库内部

### 下一步建议

1. **dump `lib.net.socket` 所在的 DEX**: 用 Frida 枚举所有已加载的 DEX，找到包含 `EmSocketManager` 的那个 → `frida_dump.py` 或 `dump_dex_v3.js`
2. **反编译 `libEmse-lib.so`**: 用 Ghidra/IDA 分析，搜索 0x7b/0x7d/0x8d 常量
3. **下载 serverlist7.dat**: 用 `ServerListReader.reverse()` + inflate 逻辑解码，获取实际行情服务器 IP:Port 列表
4. **Hook `lib.net.socket.a` 的 encode/decode**: 用 Frida 拦截协议基类方法，捕获 runtime 实际帧数据
