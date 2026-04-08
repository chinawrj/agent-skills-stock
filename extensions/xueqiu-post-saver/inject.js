/**
 * 主世界注入脚本 — 运行在页面的 MAIN world 中
 * 可以直接访问 Vue 组件实例 (__vue__)，读取完整帖子内容 (item.text)
 * 通过 DOM 属性 data-xq-fulltext 桥接给隔离世界的 content.js
 */
(function () {
  'use strict';

  function extractVueData() {
    var articles = document.querySelectorAll('article.timeline__item');
    var count = 0;
    for (var i = 0; i < articles.length; i++) {
      var a = articles[i];
      if (a.hasAttribute('data-xq-fulltext')) { count++; continue; }
      var vm = a.__vue__;
      if (!vm || !vm.$props || !vm.$props.item) continue;
      var text = vm.$props.item.text || '';
      if (text) {
        a.setAttribute('data-xq-fulltext', text);
        count++;
      }
    }
    return count;
  }

  // 初次提取 + 延迟重试（Vue 可能尚未挂载）
  extractVueData();
  setTimeout(extractVueData, 500);
  setTimeout(extractVueData, 2000);

  // 响应 content.js 的按需刷新请求
  document.addEventListener('xq-request-vue-data', function () {
    extractVueData();
  });

  // 监听 DOM 变化，自动为新增 article 提取 Vue 数据
  var observer = new MutationObserver(function () {
    extractVueData();
  });
  observer.observe(document.body || document.documentElement, {
    childList: true,
    subtree: true
  });
})();
