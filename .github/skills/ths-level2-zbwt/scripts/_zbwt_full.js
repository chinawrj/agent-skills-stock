'use strict';
// TCP socket 传输 + 支持从指定 seq/oid 续传
// 参数通过全局变量注入: __START_SEQ__, __START_OID__, __SKIP_CACHE__
var done = false, pageCount = 0, totalCount = 0;
var sock = null, out = null;
var lastTime = "", lastReportMs = Date.now();
var START_SEQ = "__START_SEQ__";
var START_OID = "__START_OID__";
var SKIP_CACHE = __SKIP_CACHE__;

Java.perform(function() {
    var RV = Java.use("com.hexin.android.biz_hangqing.level2.zbwt.Level2ZbwtRecyclerView");
    var zloClass = Java.use("zlo");
    var SA = Java.use("android.util.SparseArray");
    var JL = Java.use("java.util.List");
    var Socket = Java.use("java.net.Socket");
    var PrintWriter = Java.use("java.io.PrintWriter");
    var BufferedOutputStream = Java.use("java.io.BufferedOutputStream");
    var OutputStreamWriter = Java.use("java.io.OutputStreamWriter");

    // 禁用 StrictMode
    try {
        var StrictMode = Java.use("android.os.StrictMode");
        var PB = Java.use("android.os.StrictMode$ThreadPolicy$Builder");
        StrictMode.setThreadPolicy(PB.$new().permitAll().build());
    } catch(e) {}

    // 连接 TCP
    try {
        sock = Socket.$new("127.0.0.1", 19876);
        sock.setTcpNoDelay(true);
        var bos = BufferedOutputStream.$new(sock.getOutputStream(), 65536);
        out = PrintWriter.$new(OutputStreamWriter.$new(bos, "UTF-8"), false);
        console.log("[INFO] TCP connected");
    } catch(e) {
        console.log("[ERROR] TCP connect: " + e);
        return;
    }

    function sendData(text) { out.print(text); }
    function flush() { out.flush(); }

    function emitPage(list) {
        var cnt = list.size();
        var sb = "";
        var ls = "", lo = "";
        for (var i = 0; i < cnt; i++) {
            var sa = Java.cast(list.get(i), SA);
            var d = (sa.get(12)+"") === "1" ? "B" : "S";
            sb += sa.get(0) + "," + sa.get(10) + "," + sa.get(13) + "," + d + "," + sa.get(56) + "," + sa.get(1) + "\n";
            totalCount++;
            if (i === 0) lastTime = sa.get(0) + "";
            if (i === cnt-1) { ls = sa.get(56)+""; lo = sa.get(1)+""; }
        }
        sendData(sb);
        flush();
        return {seq: ls, oid: lo, count: cnt};
    }

    var found = false;
    Java.choose("com.hexin.android.biz_hangqing.level2.zbwt.Level2ZbwtSingleList", {
        onMatch: function(sl) {
            if (found) return;
            try {
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

                // 首次运行: 输出 adapter 缓存; 续传: 跳过缓存
                if (!SKIP_CACHE) {
                    var r = emitPage(dl);
                    START_SEQ = r.seq;
                    START_OID = r.oid;
                    console.log("[P] cached " + n);
                }

                RV.onHistoryDataReceive.implementation = function(list, list2) {
                    if (done) return;
                    try {
                        // 禁 StrictMode on callback thread
                        var SM = Java.use("android.os.StrictMode");
                        var PB2 = Java.use("android.os.StrictMode$ThreadPolicy$Builder");
                        SM.setThreadPolicy(PB2.$new().permitAll().build());

                        pageCount++;
                        var r = emitPage(list);

                        var nowMs = Date.now();
                        if (nowMs - lastReportMs >= 5000) {
                            lastReportMs = nowMs;
                            console.log("[P] t=" + totalCount + " time=" + lastTime);
                        }
                        if (r.count < 40) {
                            done = true;
                            sendData("[DONE] " + totalCount + "\n");
                            flush();
                            console.log("[DONE] " + totalCount);
                            try { sock.close(); } catch(e2) {}
                            return;
                        }
                        setTimeout(function() {
                            if (done) return;
                            Java.perform(function() {
                                try { zo.i(r.seq, r.oid); } catch(e) {
                                    done = true;
                                    sendData("[DONE] " + totalCount + "\n");
                                    flush();
                                    console.log("[ERROR] " + e);
                                    try { sock.close(); } catch(e2) {}
                                }
                            });
                        }, 100);
                    } catch(e) {
                        done = true;
                        console.log("[ERROR] cb: " + e);
                        try { sendData("[DONE] " + totalCount + "\n"); flush(); sock.close(); } catch(e2) {}
                    }
                };

                console.log("[P] start seq=" + START_SEQ);
                zo.i(START_SEQ, START_OID);
            } catch(e) {
                console.log("[ERROR] init: " + e);
            }
        },
        onComplete: function() {
            if (!found) {
                console.log("[ERROR] not found");
                try { sendData("[DONE] 0\n"); flush(); sock.close(); } catch(e) {}
            }
        }
    });
});
setInterval(function(){}, 500);
