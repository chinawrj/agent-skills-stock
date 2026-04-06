document.getElementById('btn-toggle').addEventListener('click', async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (tab) chrome.tabs.sendMessage(tab.id, { action: 'toggle' });
});

document.getElementById('btn-refresh').addEventListener('click', async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (tab) chrome.tabs.sendMessage(tab.id, { action: 'refresh' });
});

document.getElementById('btn-open').addEventListener('click', () => {
  chrome.tabs.create({ url: 'https://www.jisilu.cn/data/cbnew/#cb' });
});
