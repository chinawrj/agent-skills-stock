#!/usr/bin/env python3
"""
分批提取兆新股份逐笔委托全天数据。
Frida 约每10-90页断连，用 Python 循环从断点自动续传。
"""
import subprocess, csv, time, os, sys, re

STOCK_NAME = "兆新股份"
STOCK_CODE = "002256"
PKG = "com.hexin.plat.android.supremacy"
LAUNCHER = f"{PKG}/com.hexin.plat.android.LogoEmptyActivity"
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", f"zbwt_{STOCK_NAME}_20260407.csv")


def adb(args, timeout=15):
    return subprocess.run(["adb"] + args, capture_output=True, text=True, timeout=timeout).stdout.strip()


def restart_app_and_navigate():
    """重启 app 并导航到兆新股份逐笔委托页面"""
    print("  重启 app...")
    adb(["shell", "am", "force-stop", PKG])
    time.sleep(2)

    # 重启 frida-server
    adb(["shell", "su", "-c", "killall frida-server"], timeout=5)
    time.sleep(1)
    subprocess.Popen(["adb", "shell", "su", "-c", "/data/local/tmp/frida-server -D &"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)

    adb(["shell", "am", "start", "-n", LAUNCHER])
    time.sleep(8)

    # 关闭可能的弹窗
    adb(["shell", "input", "keyevent", "BACK"])
    time.sleep(1)

    # 底部tab → 自选 (x=450, y=2295)
    print("  导航: 自选...")
    adb(["shell", "input", "tap", "450", "2295"])
    time.sleep(3)

    # 点击兆新股份 (第5行, y≈1280)
    print("  导航: 兆新股份...")
    adb(["shell", "input", "tap", "100", "1280"])
    time.sleep(3)

    # 点击逐笔委托
    print("  导航: 逐笔委托...")
    adb(["shell", "input", "tap", "540", "2150"])
    time.sleep(1)

    # 上滑展开数据区
    adb(["shell", "input", "swipe", "540", "2100", "540", "800", "300"])
    time.sleep(2)

    # 验证
    r = subprocess.run(["frida", "-U", "-q", "同花顺至尊版", "--timeout", "10", "-e",
                        'Java.perform(function(){var c=0;Java.choose("com.hexin.android.biz_hangqing.level2.zbwt.Level2ZbwtSingleList",{onMatch:function(){c++;},onComplete:function(){console.log("FOUND:"+c);}});});'],
                       capture_output=True, text=True, timeout=20)
    if "FOUND:0" in r.stdout or "FOUND:" not in r.stdout:
        print("  导航失败! 重试...")
        return False
    print("  导航成功")
    return True
JS_TEMPLATE = r"""
'use strict';
var done = false, pageCount = 0, totalCount = 0;
var START_SEQ = "__SEQ__", START_OID = "__OID__";
var IS_FIRST = __IS_FIRST__;

Java.perform(function() {
    var RV = Java.use("com.hexin.android.biz_hangqing.level2.zbwt.Level2ZbwtRecyclerView");
    var zloClass = Java.use("zlo");
    var SA = Java.use("android.util.SparseArray");
    var JL = Java.use("java.util.List");

    function emit(sa) {
        var d = (sa.get(12)+"") === "1" ? "B" : "S";
        console.log("[R]" + sa.get(0) + "," + sa.get(10) + "," + sa.get(13) + "," + d + "," + sa.get(56) + "," + sa.get(1));
        totalCount++;
    }

    var found = false;
    Java.choose("com.hexin.android.biz_hangqing.level2.zbwt.Level2ZbwtSingleList", {
        onMatch: function(sl) {
            if (found) return;
            var rvF = sl.getClass().getDeclaredField("f");
            rvF.setAccessible(true);
            var rv = rvF.get(sl);
            if (!rv) return;
            var adpF = rv.getClass().getDeclaredField("b");
            adpF.setAccessible(true);
            var adp = adpF.get(rv);
            var dataF = adp.getClass().getDeclaredField("a");
            dataF.setAccessible(true);
            var raw = dataF.get(adp);
            if (!raw) return;
            var dl = Java.cast(raw, JL);
            var n = dl.size();
            var zloF = rv.getClass().getDeclaredField("i");
            zloF.setAccessible(true);
            var zloInst = zloF.get(rv);
            if (!zloInst) return;
            found = true;
            var zo = Java.cast(zloInst, zloClass);

            if (IS_FIRST) {
                for (var i = 0; i < n; i++) emit(Java.cast(dl.get(i), SA));
                console.log("[P] cached " + n);
                var last = Java.cast(dl.get(n-1), SA);
                START_SEQ = last.get(56) + "";
                START_OID = last.get(1) + "";
            }

            RV.onHistoryDataReceive.implementation = function(list, list2) {
                if (done) return;
                pageCount++;
                var cnt = list.size();
                var ls = "", lo = "";
                for (var i = 0; i < cnt; i++) {
                    var sa = Java.cast(list.get(i), SA);
                    emit(sa);
                    if (i === cnt-1) { ls = sa.get(56)+""; lo = sa.get(1)+""; }
                }
                console.log("[P] p" + pageCount + " +" + cnt + " t=" + totalCount);
                if (cnt < 40) { done = true; console.log("[DONE] " + totalCount); return; }
                setTimeout(function() {
                    if (done) return;
                    Java.perform(function() {
                        try { zo.i(ls, lo); } catch(e) {
                            done = true; console.log("[DONE] " + totalCount);
                        }
                    });
                }, 150);
            };

            console.log("[P] start seq=" + START_SEQ);
            zo.i(START_SEQ, START_OID);
        },
        onComplete: function() { if (!found) console.log("[DONE] 0"); }
    });
});
setInterval(function(){}, 500);
"""


def run_batch(seq: str, oid: str, is_first: bool, timeout: int = 180):
    """Run one Frida extraction batch. Returns (rows, last_seq, last_oid)."""
    js = JS_TEMPLATE.replace("__SEQ__", seq).replace("__OID__", oid).replace("__IS_FIRST__", "true" if is_first else "false")
    js_path = "/tmp/_zbwt_batch.js"
    with open(js_path, "w") as f:
        f.write(js)

    proc = subprocess.Popen(
        ["frida", "-U", "-q", "同花顺至尊版", "-l", js_path, "--timeout", "inf"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    rows = []
    last_seq, last_oid = seq, oid
    start = time.time()

    while time.time() - start < timeout:
        line = proc.stdout.readline()
        if not line:
            break
        line = line.rstrip()
        if line.startswith("[R]"):
            parts = line[3:].split(",")
            if len(parts) >= 6:
                rows.append(parts)
                last_seq = parts[4]
                last_oid = parts[5]
        elif "[P]" in line:
            sys.stdout.write(f"\r  {line}   ")
            sys.stdout.flush()
        elif "[DONE]" in line:
            print(f"\n  {line}")
            break

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    return rows, last_seq, last_oid


def main():
    print(f"{'='*50}")
    print(f" {STOCK_NAME} 逐笔委托提取")
    print(f"{'='*50}")

    all_rows = []
    batch = 0
    last_seq = "0"
    last_oid = "0"
    target = "09:15"
    stale_count = 0
    need_restart = False

    while True:
        batch += 1
        is_first = (batch == 1)

        if need_restart or (batch > 1):
            ok = False
            for attempt in range(3):
                if restart_app_and_navigate():
                    ok = True
                    break
                time.sleep(3)
            if not ok:
                print("  导航失败3次，停止")
                break
            is_first = True  # 重启后需要重读 adapter cache
            need_restart = False

        print(f"\n--- 第{batch}批 seq={last_seq} ---")

        rows, new_seq, new_oid = run_batch(last_seq, last_oid, is_first)

        if not rows:
            print("  无数据")
            if batch == 1:
                print("  请确认在逐笔委托页面且 frida-server 在运行")
                return
            need_restart = True
            continue

        all_rows.extend(rows)
        earliest = rows[-1][0] if rows else "?"
        print(f"\n  本批 {len(rows)} 条, 累计 {len(all_rows)}, 最早: {earliest}")

        if new_seq == last_seq:
            stale_count += 1
            if stale_count >= 2:
                print("  seq未变化，已到底")
                break
        else:
            stale_count = 0

        last_seq = new_seq
        last_oid = new_oid

        if earliest <= target:
            print(f"  已到达 {target}")
            break

        time.sleep(1)

    if not all_rows:
        print("未获取到数据")
        return

    # Deduplicate by seq+oid
    seen = set()
    unique = []
    for r in all_rows:
        key = (r[4], r[5])  # seq, oid
        if key not in seen:
            seen.add(key)
            unique.append(r)

    # Sort by seq descending (newest first) then reverse for time-ascending
    unique.sort(key=lambda r: int(r[4]), reverse=True)
    unique.reverse()

    buy = sum(1 for r in unique if r[3] == "B")
    sell = len(unique) - buy
    print(f"\n{'='*50}")
    print(f" 共 {len(unique)} 条 (去重后) | 买: {buy} | 卖: {sell}")
    if unique:
        print(f" 时间: {unique[0][0]} ~ {unique[-1][0]}")

    with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["时间", "价格", "手数", "方向", "序列号", "委托号"])
        for r in unique:
            w.writerow([r[0], r[1], r[2], "买" if r[3] == "B" else "卖", r[4], r[5]])

    print(f" 已保存: {os.path.abspath(OUTPUT)}")


if __name__ == "__main__":
    main()
