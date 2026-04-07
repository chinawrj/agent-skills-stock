'use strict';
// 诊断脚本：测试分页稳定性，添加详细错误信息
var done = false, pageCount = 0, totalCount = 0;
var startTime = Date.now();

Java.perform(function() {
    var RV = Java.use("com.hexin.android.biz_hangqing.level2.zbwt.Level2ZbwtRecyclerView");
    var zloClass = Java.use("zlo");
    var SA = Java.use("android.util.SparseArray");
    var JL = Java.use("java.util.List");

    // 批量输出减少 console.log 压力
    var rowBuf = [];
    function emit(sa) {
        var d = (sa.get(12)+"") === "1" ? "B" : "S";
        rowBuf.push(sa.get(0) + "," + sa.get(10) + "," + sa.get(13) + "," + d + "," + sa.get(56) + "," + sa.get(1));
        totalCount++;
    }
    function flushRows() {
        if (rowBuf.length === 0) return;
        // 每50行输出一次，减少IPC次数
        for (var i = 0; i < rowBuf.length; i += 50) {
            var chunk = rowBuf.slice(i, Math.min(i+50, rowBuf.length));
            console.log("[BATCH]" + chunk.join("|"));
        }
        rowBuf = [];
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

                // Read cached data
                for (var i = 0; i < n; i++) emit(Java.cast(dl.get(i), SA));
                flushRows();
                console.log("[P] cached " + n + " elapsed=" + (Date.now()-startTime) + "ms");

                var lastSA = Java.cast(dl.get(n-1), SA);
                var startSeq = lastSA.get(56) + "";
                var startOid = lastSA.get(1) + "";

                RV.onHistoryDataReceive.implementation = function(list, list2) {
                    if (done) return;
                    try {
                        pageCount++;
                        var cnt = list.size();
                        var ls = "", lo = "";
                        for (var i = 0; i < cnt; i++) {
                            var sa = Java.cast(list.get(i), SA);
                            emit(sa);
                            if (i === cnt-1) { ls = sa.get(56)+""; lo = sa.get(1)+""; }
                        }
                        flushRows();

                        var elapsed = Date.now() - startTime;
                        console.log("[P] p" + pageCount + " +" + cnt + " t=" + totalCount + " ms=" + elapsed);

                        if (cnt < 40) {
                            done = true;
                            console.log("[DONE] " + totalCount);
                            return;
                        }

                        // 增加延迟到500ms，减少对app的压力
                        setTimeout(function() {
                            if (done) return;
                            Java.perform(function() {
                                try {
                                    zo.i(ls, lo);
                                } catch(e) {
                                    console.log("[ERROR] zo.i failed: " + e);
                                    done = true;
                                    console.log("[DONE] " + totalCount);
                                }
                            });
                        }, 500);
                    } catch(e) {
                        console.log("[ERROR] onHistoryDataReceive: " + e);
                        done = true;
                        console.log("[DONE] " + totalCount);
                    }
                };

                console.log("[P] start seq=" + startSeq + " oid=" + startOid);
                zo.i(startSeq, startOid);
            } catch(e) {
                console.log("[ERROR] init: " + e);
            }
        },
        onComplete: function() {
            if (!found) console.log("[DONE] 0");
        }
    });
});
setInterval(function(){}, 500);
