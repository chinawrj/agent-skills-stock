# 东方财富 serverlist7.ini：lvs → ServerType 映射分析

## 分析方法

本报告基于以下源码和数据的交叉分析：
- 解码后的 `serverlist7.ini`（168行）
- 内存dump的5个DEX文件反编译结果
- `Nature.java`（ServerType枚举定义）
- `ManualServerListConfig.java`（手动服务器配置类，含中文描述）
- `FileSumManager.java`（c.java, 服务器列表更新编排器）
- `PriorityServerListConfig.java`（新服务器列表响应，含ispType字段）
- 各协议包的 `@Nature` 注解

> **注意**：关键的 `com.eastmoney.android.sdk.net.socket.server.b` 类（将IniData解析为ServerInfo列表的核心类）在5个dump DEX中均未找到定义——该类存在于动态加载的加密模块中。因此，lvs→ServerType的映射是通过端口分析和代码交叉引证**推断**得出的，非直接代码确认。

---

## 1. lvs → ServerType 映射

### 端口指纹分析

| lvs键 | 主端口 | 备用端口 | 推断ServerType | 中文名称 | 依据 |
|--------|--------|----------|----------------|----------|------|
| **lvs1** | 1862 | 80 | **LINUX** | Linux行情服务器 | 端口1862为主行情端口；ManualServerListConfig定义"Linux服务器手动配置"；协议ID 50xx系列 |
| **lvs2** | 2860 | 80 | **WINDOWS** | Windows行情服务器 | 端口2860独特；ManualServerListConfig定义"Windows服务器手动配置"；协议ID 55xx系列 |
| **lvs3** | 80 | — | **HISTORY** | 历史数据服务器 | 纯HTTP端口80，跨ISP共享IP；ManualServerListConfig定义"History服务器手动配置"；协议ID 6xxx系列 |
| **lvs4** | 1861 | 80 | **FUTURE** | 期货行情服务器 | 端口1861独特；ManualServerListConfig定义"Future服务器手动配置"；发现使用阿里云/跨地域IP；协议ID 7xxx系列 |
| **lvs5** | 1862 | — | **Push Gateway** | 推送网关 | 使用域名`gate.push.eastmoney.com`而非IP；FileSumManager中用于更新`PushConfig.hostAndPort` |
| **lvs6** | 80 | — | **SSO/认证** | SSO认证服务器 | `[default]`段为`sso.eastmoney.com:80`；每ISP仅一个唯一IP；`[tt_f]`和`[jyw_f]`段只有lvs6 |
| **lvs7** | 1860 | — | **集中服务** | 集中式服务器 | 所有ISP段共享同一IP`180.163.69.219:1860`；端口1860唯一 |
| **lvs8** | 1863 | 80 | **LINUX_SUPER_L2** | 超级L2行情服务器 | 端口1863独特；ManualServerListConfig定义"超级L2 Linux服务器手动配置"；协议ID 52xx系列 |
| **lvs9** | 2880 | 80 | **付费L2（Windows协议）** | L2付费行情 | 仅出现在`_p`（付费）段，不在`_f`段；端口2880属于286x系列（Windows协议族） |
| **lvs10** | 1864 | 80 | **LINUX_EXTENSION** | Linux扩展行情服务器 | 端口1864独特；ManualServerListConfig定义"Linux扩展行情服务器配置"；协议ID 53xx系列 |
| **lvs11** | 1862 | — | **LINUX（CDN边缘节点）** | Linux行情CDN节点 | 端口1862同lvs1，但IP因ISP而异；海外段(hw)聚合了所有ISP的节点IP（12个地址）|

### 端口编号规律

```
186x 系列 = Linux协议族
  1860 = lvs7  (集中服务，性质待定)
  1861 = lvs4  (FUTURE 期货)
  1862 = lvs1  (LINUX 主行情) / lvs5 (推送) / lvs11 (CDN)
  1863 = lvs8  (LINUX_SUPER_L2)
  1864 = lvs10 (LINUX_EXTENSION)

286x 系列 = Windows协议族
  2860 = lvs2  (WINDOWS 行情)
  2880 = lvs9  (付费L2)

80 = HTTP回退端口（所有类型均提供80端口备选）/ 纯HTTP服务
```

### ServerType 枚举定义（来自 Nature.java）

```java
public enum ServerType {
    WINDOWS,          // Windows协议行情（端口286x）
    LINUX,            // Linux协议行情（端口1862）
    HISTORY,          // 历史数据（HTTP端口80）
    LINUX_SUPER_L2,   // 超级Level2行情（端口1863）
    LINUX_EXTENSION,  // 扩展行情（端口1864）
    FUTURE,           // 期货行情（端口1861）
    NULL              // 空/未知
}
```

### 协议ID分布（来自 @Nature 注解）

| ServerType | 协议ID范围 | 示例 |
|------------|-----------|------|
| LINUX | 50xx | 5008, 5010, 5014, 5015, 5016, 5023, 5051, 5055, 5061, 5074, 5093, 5120 |
| WINDOWS | 55xx | 5515, 5526, 5539 |
| HISTORY | 6xxx | 6136, 6137, 6999 |
| LINUX_SUPER_L2 | 52xx | 5203, 5226 |
| LINUX_EXTENSION | 53xx | 5305, 5306, 5311, 5332 |
| FUTURE | 7xxx | 7014, 7026, 7031, 7043, 7044, 7046 |

---

## 2. ISP段命名约定

### 段名前缀 → ISP运营商

| 前缀 | 中文名 | 运营商 | ISPType枚举值 |
|-------|--------|--------|---------------|
| **wt** | 网通 | 中国联通（原中国网通CNC） | *未在DEX中找到对应枚举值* |
| **dx** | 电信 | 中国电信 | `DIAN_XIN` |
| **yd** | 移动 | 中国移动 | `YI_DONG` |
| **hw** | 海外 | 海外/境外 | `HAI_WAI` |
| **wz** | 网杂 | 其他/混合网络 | *可能映射到 `UNKOWN`* |
| **tt** | 铁通 | 中国铁通（已并入中国移动） | *未在枚举中发现* |
| **jyw** | 教育网 | 中国教育和科研计算机网(CERNET) | *未在枚举中发现* |

### 段名后缀

| 后缀 | 含义 | lvs9 | 说明 |
|-------|------|------|------|
| **_f** | 免费 (Free) | ❌ 无lvs9 | 免费用户服务器列表 |
| **_p** | 付费 (Paid) | ✅ 有lvs9 | 付费用户服务器列表，包含L2行情服务器 |

### ISPType 枚举（从classes_0.dex字符串表提取）

已确认的枚举值：`DIAN_XIN`, `YI_DONG`, `HAI_WAI`, `UNKOWN`
带前缀变体：`ISP_DIAN_XIN`, `ISP_YI_DONG`

> 注意：`ISPType`枚举定义在 `com.eastmoney.android.sdk.net.socket.protocol.p2502.dto.ISPType`，但该类不在任何dump DEX中（p2502包为空）。完整枚举可能包含更多值如WANG_TONG、TIE_TONG等。

### 特殊段

| 段名 | 说明 |
|------|------|
| `[default]` | 仅含lvs6=sso.eastmoney.com:80（默认SSO） |
| `[tt_f]` / `[tt_p]` | 铁通，仅含lvs6（一个认证IP） |
| `[jyw_f]` / `[jyw_p]` | 教育网，仅含lvs6（一个认证IP） |
| `[test]` | 测试环境，lvs1-7（IP以202.104.236.x为主） |
| `[web_url]` | HTTP API地址配置（非服务器列表） |
| `[config]` | 应用配置参数 |

---

## 3. push2.eastmoney.com 连接入口

### URL配置来源

| 配置类 | URL | 用途 |
|--------|-----|------|
| `QuoteConfig.push2Url` | `https://push2.eastmoney.com` | 主行情HTTP推送 |
| `QuoteConfig.push2HistoryUrl` | `https://push2his.eastmoney.com` | 历史行情推送 |
| `QuoteConfig.wqt_domain` | `https://push2dycalc.eastmoney.com` | 动态计算行情 |
| `QuoteConfig.wqt_l2_domain` | `https://push2dycalcl2.eastmoney.com` | L2动态计算行情 |
| `WebQuotationPushConfig.defaultConfig` | `https://push2.eastmoney.com/` | Web行情默认 |
| `WebQuotationPushConfig.greyConfig` | `https://push2gray.eastmoney.com/` | 灰度测试 |
| `FinanceConfig.defaultConfig` | `https://push2.eastmoney.com` | 财务数据 |
| `FinanceConfig.testConfig` | `https://push2test.eastmoney.com` | 测试环境 |

### 推送服务器选择逻辑

`FileSumManager` (c.java) 中的代码显示，推送服务器从服务器列表中随机选取：

```java
// FileSumManager.a(server.b)
List listA = EmSocketManager.f.a(bVar.a());  // 过滤ServerInfo列表
ServerInfo serverInfo = (ServerInfo) listA.get(new Random().nextInt(listA.size()));
PushConfig.hostAndPort.update(new PushConfig.HostPort(serverInfo.host, serverInfo.port));
```

其中 `bVar.a()` 返回从serverlist7.ini解析的全部ServerInfo列表，`EmSocketManager.f.a()` 对其进行类型过滤（推测过滤出lvs5对应的push gateway）。

### serverlist7.ini 中的push2相关配置

```ini
[web_url]
url_exquote=https://push2.eastmoney.com    ; → WebQuotationPushConfig.baseUrl
```

lvs5 提供TCP长连接推送网关：
```ini
lvs5=gate.push.eastmoney.com:1862,gate.push.eastmoney.com:1862
```

---

## 4. 端口9696

**在所有反编译的Java源码中未找到端口9696的引用。**

搜索范围覆盖了全部5个dump DEX的jadx反编译输出。R.java中的整数数组匹配为误报（资源ID）。9696不属于东方财富的端口编号体系（186x/286x/80）。

---

## 5. 数据流架构总结

```
                            serverlist7.dat (远程)
                                   │
                            ┌──────┴──────┐
                            │  byte reverse │
                            │  + zlib inflate│
                            └──────┬──────┘
                                   │
                            serverlist7.ini (明文)
                                   │
                            ┌──────┴──────┐
                            │  IniData.fromString() │
                            └──────┬──────┘
                                   │
                   ┌───────────────┴───────────────┐
                   │  server.b.a(IniData)           │
                   │  (根据ISPType选择段，           │
                   │   解析lvs1-11为ServerInfo列表)  │
                   └───────────────┬───────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
      EmSocketManager        PushConfig           [web_url]段
    .a(ServerListSource)    .hostAndPort          → 各种HTTP URL配置
              │                    │
    按ServerType分发         随机选取push服务器
      到各协议处理器
```

### 服务器选择流程

1. `EmSocketManager.b(boolean isWifi)` → 返回 `ISPType`（检测当前网络ISP）
2. 根据ISPType选择INI段前缀（dx_, yd_, hw_, wt_, wz_等）
3. 根据用户订阅级别选择后缀（_f=免费, _p=付费）
4. 从选定段中解析lvs1-11，构建ServerInfo对象列表
5. 各连接按ServerType匹配对应的ServerInfo进行连接

---

## 附录A：serverlist7.ini 段结构摘要

| 段 | lvs1 | lvs2 | lvs3 | lvs4 | lvs5 | lvs6 | lvs7 | lvs8 | lvs9 | lvs10 | lvs11 |
|----|------|------|------|------|------|------|------|------|------|-------|-------|
| wt_f | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| wt_p | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| dx_f | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| dx_p | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| yd_f | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| yd_p | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| hw_f | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| hw_p | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| wz_f | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| wz_p | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| tt_f | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| tt_p | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| jyw_f | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| jyw_p | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| test | ✅ | ✅ | ✅ | ✅ | ✅(空) | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| default | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |

### 关键观察

- lvs9仅在`_p`段出现 → 确认为付费专属服务
- tt(铁通)和jyw(教育网)仅有lvs6 → 这些ISP的用户仅获得认证服务器，行情使用其他ISP的默认列表
- hw(海外)的lvs2和lvs11聚合了最多IP → 海外用户需要更多连接选择
- lvs1在所有主要ISP段中IP相同 → LINUX主行情使用共享CDN
- lvs6在每个ISP段有不同IP → SSO认证服务器按ISP就近部署

## 附录B：未解决问题

1. **lvs7确切作用**：端口1860，全ISP共享IP`180.163.69.219`，可能是码表服务器或中央调度服务器
2. **lvs9与lvs2的关系**：两者都使用Windows协议族端口(286x)，lvs9(2880)可能是lvs2(2860)的L2付费增强版
3. **lvs11与lvs1的区别**：同为端口1862/LINUX，但lvs11的IP因ISP不同而完全不同，推测lvs11是ISP就近部署的CDN边缘节点，而lvs1是共享的核心节点
4. **server.b解析逻辑**：该类在动态加载模块中，需通过Frida hook或运行时拦截获取完整映射关系
