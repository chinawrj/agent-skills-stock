#!/usr/bin/env python3
"""Explore jisilu stock page PE/PB charts for price/kline data."""
import json, time, os
from patchright.sync_api import sync_playwright

pw = sync_playwright().start()
browser = pw.chromium.connect_over_cdp('http://127.0.0.1:9222')
ctx = browser.contexts[0]

# Open fresh page
page = ctx.new_page()
page.goto('https://www.jisilu.cn/data/stock/002726', wait_until='networkidle', timeout=60000)
time.sleep(5)  # Wait for echarts to render

print(f'Page: {page.url}')

# Probe via CDP protocol directly (bypasses context isolation)
cdp = ctx.new_cdp_session(page)

# Execute in page context via CDP Runtime.evaluate
result = cdp.send('Runtime.evaluate', {
    'expression': '''
    (function() {
        var result = {};
        
        if (typeof echarts === 'undefined') {
            return JSON.stringify({ error: 'echarts not loaded' });
        }
        
        var pe_el = document.getElementById('chart_PE');
        if (pe_el) {
            var chart = echarts.getInstanceByDom(pe_el);
            if (chart) {
                var opt = chart.getOption();
                var pe = { series: [] };
                
                if (opt.xAxis && opt.xAxis[0] && opt.xAxis[0].data) {
                    var dates = opt.xAxis[0].data;
                    pe.total_dates = dates.length;
                    pe.first_date = dates[0];
                    pe.last_date = dates[dates.length - 1];
                }
                
                if (opt.yAxis) {
                    pe.yAxis = [];
                    for (var i = 0; i < opt.yAxis.length; i++) {
                        pe.yAxis.push(opt.yAxis[i].name || opt.yAxis[i].type || '');
                    }
                }
                
                if (opt.series) {
                    for (var j = 0; j < opt.series.length; j++) {
                        var s = opt.series[j];
                        var info = {
                            name: s.name || '',
                            type: s.type || '',
                            dataLen: s.data ? s.data.length : 0,
                            yAxisIndex: s.yAxisIndex || 0,
                        };
                        if (s.data && s.data.length > 0) {
                            info.first3 = s.data.slice(0, 3);
                            info.last3 = s.data.slice(-3);
                        }
                        pe.series.push(info);
                    }
                }
                result.PE = pe;
            } else {
                result.PE = { error: 'no echarts instance on PE' };
            }
        } else {
            result.PE = { error: 'no chart_PE element' };
        }
        
        var pb_el = document.getElementById('chart_PB');
        if (pb_el) {
            var chart2 = echarts.getInstanceByDom(pb_el);
            if (chart2) {
                var opt2 = chart2.getOption();
                var pb = { series: [] };
                
                if (opt2.xAxis && opt2.xAxis[0] && opt2.xAxis[0].data) {
                    var d2 = opt2.xAxis[0].data;
                    pb.total_dates = d2.length;
                    pb.first_date = d2[0];
                    pb.last_date = d2[d2.length - 1];
                }
                
                if (opt2.yAxis) {
                    pb.yAxis = [];
                    for (var k = 0; k < opt2.yAxis.length; k++) {
                        pb.yAxis.push(opt2.yAxis[k].name || opt2.yAxis[k].type || '');
                    }
                }
                
                if (opt2.series) {
                    for (var m = 0; m < opt2.series.length; m++) {
                        var s2 = opt2.series[m];
                        var info2 = {
                            name: s2.name || '',
                            type: s2.type || '',
                            dataLen: s2.data ? s2.data.length : 0,
                            yAxisIndex: s2.yAxisIndex || 0,
                        };
                        if (s2.data && s2.data.length > 0) {
                            info2.first3 = s2.data.slice(0, 3);
                            info2.last3 = s2.data.slice(-3);
                        }
                        pb.series.push(info2);
                    }
                }
                result.PB = pb;
            }
        }
        
        return JSON.stringify(result);
    })()
    ''',
    'returnByValue': True,
})

val = result.get('result', {}).get('value', '{}')
data = json.loads(val)
print(json.dumps(data, ensure_ascii=False, indent=2))

# Also check inline scripts for data source
print('\n=== Inline scripts with chart config ===')
result2 = cdp.send('Runtime.evaluate', {
    'expression': '''
    (function() {
        var scripts = document.querySelectorAll('script');
        var results = [];
        for (var i = 0; i < scripts.length; i++) {
            var s = scripts[i];
            if (!s.src && s.textContent.length > 50 && s.textContent.length < 20000) {
                var text = s.textContent;
                if (text.indexOf('chart_PE') >= 0 || text.indexOf('chart_PB') >= 0 || 
                    text.indexOf('setOption') >= 0 || text.indexOf('kline') >= 0 ||
                    text.indexOf('PE_data') >= 0 || text.indexOf('stock_report') >= 0) {
                    results.push(text.substring(0, 1000));
                }
            }
        }
        return JSON.stringify(results);
    })()
    ''',
    'returnByValue': True,
})
val2 = result2.get('result', {}).get('value', '[]')
scripts = json.loads(val2)
for i, s in enumerate(scripts):
    print(f'\n--- Script {i+1} ({len(s)} chars) ---')
    print(s)

# Check the report API
print('\n=== Report API ===')
result3 = cdp.send('Runtime.evaluate', {
    'expression': '''
    (function() {
        var xhr = new XMLHttpRequest();
        xhr.open('POST', '/data/stock/report/002726?___jsl=LST___t=' + Date.now(), false);
        xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
        xhr.send();
        return xhr.responseText.substring(0, 2000);
    })()
    ''',
    'returnByValue': True,
})
report = result3.get('result', {}).get('value', '')
print(report[:2000])

page.close()
pw.stop()
