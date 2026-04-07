#!/usr/bin/env python3
"""
通过 Frida CLI 注入同花顺App，直接从 Level2ZbwtRecyclerView 的 adapter 中
读取逐笔委托数据，并自动触发分页加载获取全天完整数据。

反编译追踪到的数据流：
  zlo.request() → MiddlewareProxy(4027) → zlo.receive(StuffTableStruct)
  → zlo.g() 解析 → onHistoryDataReceive(List<SparseArray<String>>, List<Integer>)
  → adapter.p() 追加到 f12987a

SparseArray keys: 0=时间(格式化), 10=价格, 12=买卖("1"=买), 13=手数, 56=序列号, 1=orderid
"""
import subprocess
import xml.etree.ElementTree as ET
import csv
import time
import sys
import os

STOCK_CODE = "002256"
STOCK_NAME = "兆新股份"
PKG = "com.hexin.plat.android.supremacy"
LAUNCHER = f"{PKG}/com.hexin.plat.android.LogoEmptyActivity"

OUTPUT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", f"zbwt_{STOCK_NAME}_20260407.csv")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# ─── ADB helpers ─────────────────────────────────────────────
def adb(args: list[str], timeout: int = 15) -> str:
    return subprocess.run(["adb"] + args, capture_output=True, text=True, timeout=timeout).stdout.strip()


def tap(x: int, y: int):
    adb(["shell", "input", "tap", str(x), str(y)])
    time.sleep(0.5)


def input_text(text: str):
    adb(["shell", "input", "text", text])
    time.sleep(0.5)


def dump_ui_texts() -> dict:
    """Dump UI and return dict mapping resource-id suffix → {text, bounds, clickable}"""
    adb(["shell", "uiautomator", "dump", "/sdcard/ui.xml"])
    xml_str = adb(["shell", "cat", "/sdcard/ui.xml"])
    result = {}
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return result
    for node in root.iter():
        rid = node.attrib.get("resource-id", "")
        text = node.attrib.get("text", "")
        bounds = node.attrib.get("bounds", "")
        clickable = node.attrib.get("clickable", "false")
        key = rid.split("/")[-1] if rid else ""
        if text or key:
            result[f"{key}:{text}"] = {"text": text, "bounds": bounds, "clickable": clickable, "rid": key}
        # Also store by text for easy lookup
        if text:
            result[f"text:{text}"] = {"text": text, "bounds": bounds, "clickable": clickable, "rid": key}
    return result


def parse_bounds(bounds_str: str) -> tuple:
    """Parse '[x1,y1][x2,y2]' → center (cx, cy)"""
    import re
    m = re.findall(r'\[(\d+),(\d+)\]', bounds_str)
    if len(m) >= 2:
        x1, y1 = int(m[0][0]), int(m[0][1])
        x2, y2 = int(m[1][0]), int(m[1][1])
        return (x1 + x2) // 2, (y1 + y2) // 2
    return 0, 0


def ensure_app_running() -> bool:
    """确保同花顺在运行，返回是否需要等待"""
    pid = adb(["shell", "pidof", PKG])
    if pid:
        return False
    print("  启动同花顺...")
    adb(["shell", "am", "start", "-n", LAUNCHER])
    time.sleep(10)
    pid = adb(["shell", "pidof", PKG])
    return bool(pid)


def navigate_to_zbwt() -> bool:
    """导航到兆新股份的逐笔委托页面。返回是否成功。"""
    print("  检查当前页面...")
    ui = dump_ui_texts()

    # Case 1: 已在逐笔委托页面
    if f"text:{STOCK_NAME}" in ui and "text:逐笔委托" in ui and "text:手数" in ui:
        # Check if zbwt data is visible
        if any("zbwt_single_item_time" in k for k in ui):
            print(f"  已在{STOCK_NAME}逐笔委托页面")
            return True

    # Case 2: 在股票详情页但需要切到 Level-2 逐笔委托
    if f"text:{STOCK_NAME}" in ui:
        # 可能在分时/K线等其他tab
        if "text:Level-2" in ui:
            lv2 = ui["text:Level-2"]
            cx, cy = parse_bounds(lv2["bounds"])
            print("  点击 Level-2 tab...")
            tap(cx, cy)
            time.sleep(2)
            ui = dump_ui_texts()

        if "text:逐笔委托" in ui:
            zbwt = ui["text:逐笔委托"]
            cx, cy = parse_bounds(zbwt["bounds"])
            print("  点击 逐笔委托...")
            tap(cx, cy)
            time.sleep(2)
            return True

    # Case 3: 需要搜索股票
    print(f"  搜索{STOCK_NAME}...")

    # 找搜索入口
    for key, val in ui.items():
        if "search" in val.get("rid", "").lower() and val.get("clickable") == "true":
            cx, cy = parse_bounds(val["bounds"])
            tap(cx, cy)
            time.sleep(2)
            break
    else:
        # 可能在主页，点击顶部搜索栏
        tap(540, 100)
        time.sleep(2)

    # 输入股票代码
    input_text(STOCK_CODE)
    time.sleep(2)

    # Dump UI 找搜索结果
    ui = dump_ui_texts()
    for key, val in ui.items():
        if STOCK_NAME in val.get("text", "") or STOCK_CODE in val.get("text", ""):
            cx, cy = parse_bounds(val["bounds"])
            tap(cx, cy)
            time.sleep(3)
            break

    # 切到 Level-2 → 逐笔委托
    ui = dump_ui_texts()
    if "text:Level-2" in ui:
        lv2 = ui["text:Level-2"]
        cx, cy = parse_bounds(lv2["bounds"])
        tap(cx, cy)
        time.sleep(2)

    ui = dump_ui_texts()
    if "text:逐笔委托" in ui:
        zbwt = ui["text:逐笔委托"]
        cx, cy = parse_bounds(zbwt["bounds"])
        tap(cx, cy)
        time.sleep(2)
        return True

    print("  无法导航到逐笔委托页面！")
    return False

FRIDA_JS = r"""
'use strict';

var found = false;
var pageCount = 0;
var totalCount = 0;

Java.perform(function() {
    var RV = Java.use("com.hexin.android.biz_hangqing.level2.zbwt.Level2ZbwtRecyclerView");
    var zloClass = Java.use("zlo");
    var SparseArray = Java.use("android.util.SparseArray");
    var JList = Java.use("java.util.List");

    function emitRow(sa) {
        var dir = (sa.get(12) + "") === "1" ? "买" : "卖";
        console.log("[ROW]" + sa.get(0) + "," + sa.get(10) + "," + sa.get(13) + "," + dir + "," + sa.get(56) + "," + sa.get(1));
        totalCount++;
    }

    var instanceCount = 0;

    // RecyclerView 无法直接 Java.choose 找到，通过 SingleList 容器获取
    Java.choose("com.hexin.android.biz_hangqing.level2.zbwt.Level2ZbwtSingleList", {
        onMatch: function(singleList) {
            // 字段 f 是 Level2ZbwtRecyclerView
            var rvField = singleList.getClass().getDeclaredField("f");
            rvField.setAccessible(true);
            var instance = rvField.get(singleList);
            if (!instance) return;

            instanceCount++;
            console.log("[INFO] 通过 SingleList 找到 RecyclerView #" + instanceCount);

            var adapterF = instance.getClass().getDeclaredField("b");
            adapterF.setAccessible(true);
            var adapter = adapterF.get(instance);

            var dataF = adapter.getClass().getDeclaredField("a");
            dataF.setAccessible(true);
            var rawList = dataF.get(adapter);
            if (!rawList) {
                console.log("[INFO] 实例#" + instanceCount + " adapter无数据，跳过");
                return;
            }
            var dataList = Java.cast(rawList, JList);

            var n = dataList.size();
            console.log("[INFO] 实例#" + instanceCount + " Adapter 缓存 " + n + " 条");

            // 检查 zlo 是否存在
            var zloF = instance.getClass().getDeclaredField("i");
            zloF.setAccessible(true);
            var zloInst = zloF.get(instance);
            if (!zloInst) {
                console.log("[INFO] 实例#" + instanceCount + " 无zlo连接，跳过");
                return;
            }

            if (found) {
                console.log("[INFO] 已有活跃实例，跳过实例#" + instanceCount);
                return;
            }
            found = true;

            for (var i = 0; i < n; i++) {
                emitRow(Java.cast(dataList.get(i), SparseArray));
            }
            console.log("[PROGRESS] 已输出 " + totalCount + " 条 (adapter缓存)");
            var zloObj = Java.cast(zloInst, zloClass);

            RV.onHistoryDataReceive.implementation = function(list, list2) {
                pageCount++;
                var cnt = list.size();

                var lastSeq = "", lastOid = "";
                for (var i = 0; i < cnt; i++) {
                    var sa = Java.cast(list.get(i), SparseArray);
                    emitRow(sa);
                    if (i === cnt - 1) {
                        lastSeq = sa.get(56) + "";
                        lastOid = sa.get(1) + "";
                    }
                }

                console.log("[PROGRESS] 第" + pageCount + "页 +" + cnt + "条, 累计 " + totalCount);

                if (cnt < 40) {
                    console.log("[DONE] " + totalCount);
                } else {
                    setTimeout(function() {
                        Java.perform(function() {
                            try {
                                zloObj.i(lastSeq, lastOid);
                            } catch(e) {
                                console.log("[ERROR] " + e);
                                console.log("[DONE] " + totalCount);
                            }
                        });
                    }, 200);
                }
            };

            // 触发第一次分页
            var lastSA = Java.cast(dataList.get(n - 1), SparseArray);
            var seq0 = lastSA.get(56) + "";
            var oid0 = lastSA.get(1) + "";
            console.log("[INFO] 开始分页加载: seq=" + seq0);
            zloObj.i(seq0, oid0);
        },
        onComplete: function() {}
    });
});

// Keep script alive until done
setInterval(function() {}, 1000);
"""


def make_js(start_seq: str = "0", start_oid: str = "0") -> str:
    """生成 Frida JS 脚本，支持从指定位置开始提取"""
    return FRIDA_JS.replace("__START_SEQ__", start_seq).replace("__START_OID__", start_oid)


def run_frida_batch(start_seq: str = "0", start_oid: str = "0", timeout: int = 120) -> tuple:
    """运行一批 Frida 提取，返回 (rows, last_seq, last_oid)"""
    js_code = make_js(start_seq, start_oid)
    js_path = os.path.join(SCRIPT_DIR, "_zbwt_extract.js")
    with open(js_path, "w") as f:
        f.write(js_code)

    proc = subprocess.Popen(
        ["frida", "-U", "-q", "同花顺至尊版", "-l", js_path, "--timeout", "30"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    rows = []
    last_seq = start_seq
    last_oid = start_oid
    start = time.time()

    while time.time() - start < timeout:
        line = proc.stdout.readline()
        if not line:
            break
        line = line.rstrip()

        if line.startswith("[ROW]"):
            parts = line[5:].split(",")
            rows.append(parts[:4])  # time, price, vol, dir
            if len(parts) >= 6:
                last_seq = parts[4]
                last_oid = parts[5]
        elif "[PROGRESS]" in line:
            sys.stdout.write(f"\r  {line}   ")
            sys.stdout.flush()
        elif "[INFO]" in line:
            print(f"  {line}")
        elif "[DONE]" in line:
            print(f"\n  {line}")
            break
        elif "[ERROR]" in line or "Error:" in line:
            print(f"\n  {line}")

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()

    try:
        os.remove(js_path)
    except OSError:
        pass

    return rows, last_seq, last_oid


def main():
    print("=" * 60)
    print(f"{STOCK_NAME}({STOCK_CODE}) 逐笔委托全天数据提取")
    print("=" * 60)

    # Step 1: 确保 app 在运行并导航到逐笔委托
    print("\n1. 准备页面...")
    ensure_app_running()
    if not navigate_to_zbwt():
        print("导航失败！请手动打开逐笔委托页面后重试")
        return

    # Step 2: 确保 frida-server 在运行
    print("\n2. 检查 frida-server...")
    fs_pid = adb(["shell", "su", "-c", "pidof frida-server"])
    if not fs_pid:
        print("  启动 frida-server...")
        subprocess.Popen(["adb", "shell", "su", "-c", "nohup /data/local/tmp/frida-server -D > /dev/null 2>&1 &"])
        time.sleep(3)

    # Step 3: 分批提取
    print("\n3. 开始提取数据...")
    all_rows = []
    batch = 0
    last_seq = "0"
    last_oid = "0"
    target_time = "09:15"

    while True:
        batch += 1
        print(f"\n--- 第{batch}批 (从seq={last_seq}) ---\n")

        rows, new_seq, new_oid = run_frida_batch(last_seq, last_oid)

        if not rows:
            if batch == 1:
                print("  首批无数据！检查 frida 连接...")
            else:
                print("  本批无数据，停止")
            break

        all_rows.extend(rows)
        earliest = rows[-1][0] if rows else "?"
        print(f"\n  本批 {len(rows)} 条, 累计 {len(all_rows)} 条, 最早: {earliest}")

        if new_seq == last_seq:
            print("  seq未变化，已到底")
            break

        last_seq = new_seq
        last_oid = new_oid

        if earliest <= target_time:
            print(f"  已到达 {target_time}")
            break

        # 等 app 恢复
        time.sleep(2)

    if not all_rows:
        print("\n未获取到数据！")
        return

    # 数据从新到旧，反转为时间正序
    all_rows.reverse()

    # Step 4: 保存 CSV
    print(f"\n4. 保存结果...")
    buy_count = sum(1 for r in all_rows if len(r) > 3 and r[3] == "买")
    sell_count = sum(1 for r in all_rows if len(r) > 3 and r[3] == "卖")
    print(f"   共 {len(all_rows)} 条 | 买: {buy_count} | 卖: {sell_count}")
    if all_rows:
        print(f"   时间范围: {all_rows[0][0]} ~ {all_rows[-1][0]}")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["时间", "价格", "手数", "方向"])
        writer.writerows(all_rows)
    print(f"   已保存到 {os.path.abspath(OUTPUT_CSV)}")


if __name__ == "__main__":
    main()
