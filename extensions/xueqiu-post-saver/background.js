/**
 * 雪球帖子保存器 - Background Service Worker
 *
 * 在 extension origin 维护一份 IndexedDB 副本，
 * 供 browser.html 等 extension 页面直接读取。
 */

const DB_NAME = 'XueqiuPostSaver';
const STORE_NAME = 'posts';
const DB_VERSION = 2;

function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      let store;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        store = db.createObjectStore(STORE_NAME, { keyPath: 'postId' });
        store.createIndex('authorId', 'authorId', { unique: false });
        store.createIndex('savedAt', 'savedAt', { unique: false });
        store.createIndex('postUrl', 'postUrl', { unique: false });
        store.createIndex('postTime', 'postTime', { unique: false });
      } else {
        store = event.target.transaction.objectStore(STORE_NAME);
        if (!store.indexNames.contains('postTime')) {
          store.createIndex('postTime', 'postTime', { unique: false });
        }
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

// 批量保存帖子（put = upsert）
async function savePosts(posts) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);
    let count = 0;
    for (const post of posts) {
      store.put(post);
      count++;
    }
    tx.oncomplete = () => { db.close(); resolve(count); };
    tx.onerror = () => { db.close(); reject(tx.error); };
  });
}

async function getAllPosts() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readonly');
    const req = tx.objectStore(STORE_NAME).getAll();
    req.onsuccess = () => { db.close(); resolve(req.result); };
    req.onerror = () => { db.close(); reject(req.error); };
  });
}

async function getStats() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readonly');
    const req = tx.objectStore(STORE_NAME).count();
    req.onsuccess = () => { db.close(); resolve({ count: req.result }); };
    req.onerror = () => { db.close(); reject(req.error); };
  });
}

async function deletePost(postId) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    tx.objectStore(STORE_NAME).delete(postId);
    tx.oncomplete = () => { db.close(); resolve(true); };
    tx.onerror = () => { db.close(); reject(tx.error); };
  });
}

async function clearAll() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    tx.objectStore(STORE_NAME).clear();
    tx.oncomplete = () => { db.close(); resolve(true); };
    tx.onerror = () => { db.close(); reject(tx.error); };
  });
}

// 消息处理
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.action === 'savePosts') {
    savePosts(msg.posts).then(count => sendResponse({ ok: true, count }))
      .catch(err => sendResponse({ ok: false, error: err.message }));
    return true; // async
  }
  if (msg.action === 'getAllPosts') {
    getAllPosts().then(posts => sendResponse({ ok: true, posts }))
      .catch(err => sendResponse({ ok: false, error: err.message }));
    return true;
  }
  if (msg.action === 'getStats') {
    getStats().then(stats => sendResponse({ ok: true, ...stats }))
      .catch(err => sendResponse({ ok: false, error: err.message }));
    return true;
  }
  if (msg.action === 'deletePost') {
    deletePost(msg.postId).then(() => sendResponse({ ok: true }))
      .catch(err => sendResponse({ ok: false, error: err.message }));
    return true;
  }
  if (msg.action === 'clearAll') {
    clearAll().then(() => sendResponse({ ok: true }))
      .catch(err => sendResponse({ ok: false, error: err.message }));
    return true;
  }
});
