const { chromium } = require('playwright-extra');
const stealth = require('puppeteer-extra-plugin-stealth')();
chromium.use(stealth);

(async () => {
  const browser = await chromium.launch({
    headless: false,
    channel: 'chrome',
    args: ['--remote-debugging-port=9222']
  });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    locale: 'en-US',
  });
  const page = await context.newPage();
  await page.goto('about:blank');
  console.log('STEALTH_BROWSER_READY');
  console.log('CDP port: 9222');

  // Keep alive
  await new Promise(() => {});
})();
