#!/usr/bin/env python3
"""
通过 TCP socket + adb reverse 提取任意股票逐笔委托全天数据。
支持断线自动续传：每次断线后从上次 seq/oid 处恢复。

用法: python3 extract_zbwt_tcp.py <股票代码> [日期YYYYMMDD]
示例: python3 extract_zbwt_tcp.py 002256
      python3 extract_zbwt_tcp.py 002256 20260408
"""
import subprocess, csv, time, os, sys, socket, threading, tempfile, argparse, json, xml.etree.ElementTree as ET
from datetime import datetime

PKG = "com.hexin.plat.android.supremacy"
LAUNCHER = f"{PKG}/com.hexin.plat.android.LogoEmptyActivity"
PORT = 19876
JS_TEMPLATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_zbwt_full.js")
MAX_SESSIONS = 20


def parse_args():
    parser = argparse.ArgumentParser(description="提取同花顺至尊版逐笔委托数据")
    parser.add_argument("stock_code", help="股票代码，如 002256")
    parser.add_argument("date", nargs="?", default=datetime.now().strftime("%Y%m%d"),
                        help="日期 YYYYMMDD（默认今天）")
    args = parser.parse_args()
    if not args.stock_code.isdigit() or len(args.stock_code) != 6:
        parser.error(f"股票代码必须是6位数字，收到: {args.stock_code}")
    return args


ARGS = parse_args()
STOCK_CODE = ARGS.stock_code
DATE = ARGS.date
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", f"zbwt_{STOCK_CODE}_{DATE}.csv")
PROGRESS_FILE = OUTPUT.replace(".csv", ".progress.json")


def adb(args, timeout=15):
    return subprocess.run(["adb"] + args, capture_output=True, text=True, timeout=timeout).stdout.strip()


def ui_dump():
    """拉取 UI dump 返回所有含文字的元素列表 [{text, bounds, class}]."""
    adb(["shell", "uiautomator", "dump", "/sdcard/_ui.xml"], timeout=20)
    result = adb(["pull", "/sdcard/_ui.xml", "/tmp/_ui.xml"])
    try:
        tree = ET.parse("/tmp/_ui.xml")
        items = []
        for el in tree.iter():
            text = el.get("text", "").strip()
            if text:
                items.append({"text": text, "bounds": el.get("bounds", ""), "class": el.get("class", "")})
        return items
    except Exception:
        return []


def ui_has_text(items, target):
    return any(target in it["text"] for it in items)


def _parse_bounds_center(bounds_str):
    """解析 bounds 如 '[42,276][181,335]' 返回中心坐标 (x, y)."""
    nums = bounds_str.replace("][", ",").strip("[]").split(",")
    if len(nums) == 4:
        return (int(nums[0]) + int(nums[2])) // 2, (int(nums[1]) + int(nums[3])) // 2
    return None, None


def dismiss_popups(max_tries=3):
    """检测并关闭弹窗（如 '开通夜盘交易' 等），返回是否处理了弹窗。"""
    for _ in range(max_tries):
        items = ui_dump()
        popup_keywords = ["开通夜盘交易", "立即开通", "暂不开通", "我知道了", "以后再说"]
        found_popup = False
        for kw in popup_keywords:
            for it in items:
                if kw in it["text"]:
                    found_popup = True
                    break
            if found_popup:
                break
        if not found_popup:
            for it in items:
                if "扫货" in it["text"] or "加速回流" in it["text"]:
                    for it2 in items:
                        if it2["text"] == "×" or (it2["text"] == "" and "ImageButton" in it2.get("class", "")):
                            cx, cy = _parse_bounds_center(it2["bounds"])
                            if cx:
                                adb(["shell", "input", "tap", str(cx), str(cy)])
                                time.sleep(0.5)
                    break
            return False
        print(f"  弹窗检测: 尝试关闭...")
        adb(["shell", "input", "keyevent", "BACK"])
        time.sleep(1)
    return True


STOCK_NAME = ""  # 从导航过程中自动获取


def restart_and_navigate():
    global STOCK_NAME
    print("  重启 app + frida-server...")
    adb(["shell", "am", "force-stop", PKG])
    time.sleep(2)
    adb(["shell", "su", "-c", "killall frida-server"], timeout=5)
    time.sleep(1)
    subprocess.Popen(["adb", "shell", "su", "-c", "/data/local/tmp/frida-server -D &"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    adb(["shell", "am", "start", "-n", LAUNCHER])
    time.sleep(8)
    adb(["shell", "input", "keyevent", "BACK"])
    time.sleep(1)
    dismiss_popups()

    print(f"  导航: 搜索 {STOCK_CODE} → 逐笔委托...")
    # 点击首页tab
    adb(["shell", "input", "tap", "107", "2295"])
    time.sleep(2)
    # 点击搜索栏
    adb(["shell", "input", "tap", "540", "209"])
    time.sleep(2)
    dismiss_popups()
    # 点击搜索输入框并输入股票代码
    adb(["shell", "input", "tap", "472", "185"])
    time.sleep(1)
    adb(["shell", "input", "keyevent", "KEYCODE_MOVE_END"])
    adb(["shell", "input", "keyevent", "--longpress", "KEYCODE_DEL"])
    time.sleep(0.5)
    adb(["shell", "input", "text", STOCK_CODE])
    time.sleep(2)
    dismiss_popups()

    # 验证搜索结果包含股票代码
    items = ui_dump()
    if not ui_has_text(items, STOCK_CODE):
        print(f"  警告: 搜索未找到 {STOCK_CODE}, 重试输入...")
        adb(["shell", "input", "tap", "472", "185"])
        time.sleep(0.5)
        adb(["shell", "input", "keyevent", "KEYCODE_MOVE_END"])
        adb(["shell", "input", "keyevent", "--longpress", "KEYCODE_DEL"])
        time.sleep(0.5)
        adb(["shell", "input", "text", STOCK_CODE])
        time.sleep(2)

    # 从搜索结果提取股票名称（第一个非EditText的文本且不含股票代码的元素）
    for it in items:
        if "EditText" not in it["class"] and STOCK_CODE not in it["text"] and it["text"] not in ("搜索", "综合", "股票", "基金", "资讯", "用户", "选股", "搜索历史"):
            STOCK_NAME = it["text"]
            break

    # 点击第一个搜索结果
    adb(["shell", "input", "tap", "483", "308"])
    time.sleep(3)
    dismiss_popups()

    # 检查是否误入转债页面
    items = ui_dump()
    page_is_bond = any("转债" in it["text"] and it["text"].endswith("转债") for it in items
                       if it.get("bounds", "").startswith("[") and int(it["bounds"].replace("][", ",").strip("[]").split(",")[1]) < 200)
    if page_is_bond:
        print(f"  检测到转债页面，返回重新选择股票...")
        adb(["shell", "input", "keyevent", "BACK"])
        time.sleep(1)
        # 点击第二个搜索结果（位置更低）
        adb(["shell", "input", "tap", "483", "370"])
        time.sleep(3)
        dismiss_popups()

    # 验证进入了股票详情页
    items = ui_dump()
    if not (ui_has_text(items, "Level-2") or ui_has_text(items, "逐笔委托")):
        print(f"  警告: 未检测到股票详情页，尝试再次点击搜索结果...")
        adb(["shell", "input", "keyevent", "BACK"])
        time.sleep(1)
        adb(["shell", "input", "tap", "483", "308"])
        time.sleep(3)
        dismiss_popups()
        items = ui_dump()

    # 点击 Level-2 主标签（通过 UI dump 定位）
    clicked_level2 = False
    for it in items:
        if it["text"] == "Level-2":
            cx, cy = _parse_bounds_center(it["bounds"])
            if cx:
                adb(["shell", "input", "tap", str(cx), str(cy)])
                clicked_level2 = True
                break
    if not clicked_level2:
        adb(["shell", "input", "tap", "111", "305"])  # fallback
    time.sleep(2)

    # 点击逐笔委托 sub-tab（通过 UI dump 定位）
    items = ui_dump()
    clicked_zbwt = False
    for it in items:
        if "逐笔委托" in it["text"]:
            cx, cy = _parse_bounds_center(it["bounds"])
            if cx:
                adb(["shell", "input", "tap", str(cx), str(cy)])
                clicked_zbwt = True
                break
    if not clicked_zbwt:
        adb(["shell", "input", "tap", "540", "470"])  # fallback
    time.sleep(1)

    # 展开 Level-2 区域
    adb(["shell", "input", "swipe", "540", "2100", "540", "800", "300"])
    time.sleep(2)

    # 最终验证
    items = ui_dump()
    if ui_has_text(items, "逐笔委托"):
        print(f"  导航成功: {STOCK_NAME or STOCK_CODE} 逐笔委托页面")
    else:
        print(f"  警告: 最终页面未确认为逐笔委托")


def setup_adb_reverse():
    adb(["reverse", "--remove-all"])
    adb(["reverse", f"tcp:{PORT}", f"tcp:{PORT}"])
    print(f"  adb reverse tcp:{PORT} -> tcp:{PORT}")


def prepare_js(start_seq, start_oid, skip_cache):
    with open(JS_TEMPLATE, "r") as f:
        js = f.read()
    js = js.replace('var START_SEQ = "__START_SEQ__";', f'var START_SEQ = "{start_seq}";')
    js = js.replace('var START_OID = "__START_OID__";', f'var START_OID = "{start_oid}";')
    js = js.replace('var SKIP_CACHE = __SKIP_CACHE__;', f'var SKIP_CACHE = {"true" if skip_cache else "false"};')
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".js", delete=False, dir=os.path.dirname(JS_TEMPLATE))
    tmp.write(js)
    tmp.close()
    return tmp.name


def run_one_session(start_seq="", start_oid="", skip_cache=False):
    restart_and_navigate()
    setup_adb_reverse()

    js_file = prepare_js(start_seq, start_oid, skip_cache)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", PORT))
    srv.listen(1)
    srv.settimeout(30)

    print(f"  启动 Frida (seq={start_seq or 'HEAD'})...")
    frida_proc = subprocess.Popen(
        ["frida", "-U", "-q", "同花顺至尊版", "-l", js_file, "--timeout", "inf"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    def read_frida():
        try:
            for line in frida_proc.stdout:
                line = line.rstrip()
                if line:
                    print(f"  [frida] {line}")
        except:
            pass
    t_frida = threading.Thread(target=read_frida, daemon=True)
    t_frida.start()

    print(f"  等待设备连接...")
    try:
        conn, addr = srv.accept()
        print(f"  已连接: {addr}")
    except socket.timeout:
        print("  超时！设备未连接")
        frida_proc.terminate()
        srv.close()
        os.unlink(js_file)
        return [], start_seq, start_oid, False

    conn.settimeout(120)
    rows = []
    completed = False
    last_time = ""
    report_ts = time.time()
    buf = ""

    try:
        while True:
            data = conn.recv(262144)
            if not data:
                break
            buf += data.decode("utf-8", errors="replace")
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                if line.startswith("[DONE]"):
                    completed = True
                    break
                parts = line.split(",")
                if len(parts) == 6:
                    rows.append(parts)
                    last_time = parts[0]
            if completed:
                break
            now = time.time()
            if now - report_ts >= 5:
                report_ts = now
                print(f"  进度: {len(rows)} 条, 最早: {last_time}")
    except socket.timeout:
        print("  接收超时 (120s)")
    except Exception as e:
        print(f"  接收断开: {e}")
    finally:
        conn.close()
        srv.close()

    frida_proc.terminate()
    try:
        frida_proc.wait(timeout=5)
    except:
        frida_proc.kill()
    os.unlink(js_file)

    last_seq = rows[-1][4] if rows else start_seq
    last_oid = rows[-1][5] if rows else start_oid
    print(f"  本次: {len(rows)} 条, 最早: {last_time or 'N/A'}, seq={last_seq}")
    return rows, last_seq, last_oid, completed


def save_progress(all_rows, last_seq, last_oid, session_num):
    """保存进度到磁盘，用于崩溃恢复。"""
    progress = {
        "stock_code": STOCK_CODE,
        "date": DATE,
        "last_seq": last_seq,
        "last_oid": last_oid,
        "session_num": session_num,
        "row_count": len(all_rows),
    }
    # 保存进度JSON
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    # 增量保存数据到临时CSV（raw格式，不去重）
    raw_csv = OUTPUT.replace(".csv", ".raw.csv")
    with open(raw_csv, "w", newline="") as f:
        w = csv.writer(f)
        for r in all_rows:
            w.writerow(r)


def load_progress():
    """从磁盘加载进度，返回 (all_rows, last_seq, last_oid, session_num) 或 None。"""
    if not os.path.exists(PROGRESS_FILE):
        return None
    try:
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
        if progress.get("stock_code") != STOCK_CODE or progress.get("date") != DATE:
            return None
        raw_csv = OUTPUT.replace(".csv", ".raw.csv")
        all_rows = []
        if os.path.exists(raw_csv):
            with open(raw_csv, newline="") as f:
                all_rows = list(csv.reader(f))
        return all_rows, progress["last_seq"], progress["last_oid"], progress["session_num"]
    except Exception:
        return None


def cleanup_progress():
    """提取完成后清理进度文件。"""
    for f in [PROGRESS_FILE, OUTPUT.replace(".csv", ".raw.csv")]:
        if os.path.exists(f):
            os.remove(f)


def main():
    print(f"{'='*55}")
    print(f" {STOCK_CODE} 逐笔委托提取 (TCP + 自动续传)")
    print(f"{'='*55}")

    # 尝试从磁盘恢复进度
    restored = load_progress()
    if restored:
        all_rows, last_seq, last_oid, start_session = restored
        skip_cache = True
        start_session += 1  # 从下一个会话开始
        print(f"  从磁盘恢复: {len(all_rows)} 条, seq={last_seq}, 从第 {start_session} 次会话继续")
    else:
        all_rows = []
        last_seq, last_oid = "", ""
        skip_cache = False
        start_session = 1

    for session_num in range(start_session, MAX_SESSIONS + 1):
        print(f"\n{'_'*40}")
        print(f" 第 {session_num} 次会话")
        print(f"{'_'*40}")

        rows, last_seq, last_oid, completed = run_one_session(last_seq, last_oid, skip_cache)
        all_rows.extend(rows)
        skip_cache = True

        # 每次会话后保存进度
        save_progress(all_rows, last_seq, last_oid, session_num)

        if completed:
            print(f"\n  OK 数据已全部提取完成 (收到 [DONE])")
            break

        if not rows:
            print(f"  本次无数据，重试...")
            time.sleep(5)
            continue

        earliest = min(r[0] for r in rows)
        print(f"  累计: {len(all_rows)} 条, 本次最早: {earliest}")

        if earliest <= "09:15:00":
            print(f"\n  OK 已到达开盘前 ({earliest})，停止提取")
            break

        print(f"  断线续传，等 3 秒...")
        time.sleep(3)
    else:
        print(f"\n  已达最大会话数 ({MAX_SESSIONS})，停止")

    if not all_rows:
        print("\n未获取到数据！")
        return

    print(f"\n{'='*40}")
    print(f" 处理 & 保存数据")
    print(f"{'='*40}")
    seen = set()
    unique = []
    for r in all_rows:
        key = (r[4], r[5])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    unique.sort(key=lambda r: int(r[4]))

    buy = sum(1 for r in unique if r[3] == "B")
    sell = len(unique) - buy
    print(f"  共 {len(unique)} 条 (去重) | 买: {buy} | 卖: {sell}")
    if unique:
        print(f"  时间: {unique[0][0]} ~ {unique[-1][0]}")

    # === 数据完整性自检 ===
    dup_count = len(all_rows) - len(unique)
    dup_rate = dup_count / len(all_rows) * 100 if all_rows else 0
    print(f"\n  完整性检查:")
    print(f"    原始条数: {len(all_rows)} | 去重后: {len(unique)} | 重复率: {dup_rate:.1f}%")

    if unique:
        # 时间覆盖检查
        first_time, last_time_val = unique[0][0], unique[-1][0]
        covers_open = first_time <= "09:15:05"
        covers_close = last_time_val >= "14:59:55"
        print(f"    时间范围: {first_time} ~ {last_time_val}")
        print(f"    覆盖开盘: {'✓' if covers_open else '✗ (首条 > 09:15:05)'}")
        print(f"    覆盖收盘: {'✓' if covers_close else '✗ (末条 < 14:59:55)'}")

        # 序列号连续性检查
        seqs = [int(r[4]) for r in unique]
        gaps = 0
        for i in range(1, len(seqs)):
            if seqs[i] - seqs[i-1] > 1:
                gaps += 1
        print(f"    序列号跳跃: {gaps} 处{'(正常)' if gaps == 0 else ''}")

    with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["时间", "价格", "手数", "方向", "序列号", "委托号"])
        for r in unique:
            w.writerow([r[0], r[1], r[2], "买" if r[3] == "B" else "卖", r[4], r[5]])
    print(f"  已保存: {OUTPUT}")
    cleanup_progress()
    print(f"\n{'='*55}")
    print(f" 完成！")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
