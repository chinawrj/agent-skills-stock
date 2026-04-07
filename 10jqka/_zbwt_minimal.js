'use strict';
// 极简测试：只计数不输出行数据，排除 console.log 导致的断连
var done = false, pageCount = 0, totalCount = 0;

Java.perform(function() {
    var RV = Java.use("com.hexin.android.biz_hangqing.level2.zbwt.Level2ZbwtRecyclerView");
    var zloClass = Java.use("zlo");
    var SA = Java.use("android.util.SparseArray");
    var JL = Java.use("java.util.List");

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

                totalCount = n;
                var lastSA = Java.cast(dl.get(n-1), SA);
                var startSeq = lastSA.get(56) + "";
                var startOid = lastSA.get(1) + "";
                console.log("[INFO] cached=" + n + " seq=" + startSeq);

                RV.onHistoryDataReceive.implementation = function(list, list2) {
                    if (done) return;
                    try {
                        pageCount++;
                        var cnt = list.size();
                        totalCount += cnt;
                        var ls = "", lo = "";
                        if (cnt > 0) {
                            var last = Java.cast(list.get(cnt-1), SA);
                            ls = last.get(56) + "";
                            lo = last.get(1) + "";
                        }
                        // 只每10页输出一次
                        if (pageCount % 10 === 0) {
                            console.log("[P] p" + pageCount + " total=" + totalCount);
                        }
                        if (cnt < 40) {
                            done = true;
                            console.log("[DONE] pages=" + pageCount + " total=" + totalCount);
                            return;
                        }
                        setTimeout(function() {
                            if (done) return;
                            Java.perform(function() {
                                try { zo.i(ls, lo); } catch(e) {
                                    console.log("[ERROR] " + e);
                                    done = true;
                                    console.log("[DONE] pages=" + pageCount + " total=" + totalCount);
                                }
                            });
                        }, 500);
                    } catch(e) {
                        console.log("[ERROR] " + e);
                        done = true;
                    }
                };

                zo.i(startSeq, startOid);
            } catch(e) {
                console.log("[ERROR] init: " + e);
            }
        },
        onComplete: function() {
            if (!found) console.log("[DONE] notfound");
        }
    });
});
setInterval(function(){}, 500);
