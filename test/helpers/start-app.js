// test/helpers/start-app.js
import http from 'node:http';

const port = process.env.FDA_SERVER_PORT || '0';

// Simple ping loop to wait for health
function waitFor(url, timeoutMs = 30_000) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const tick = () => {
      const req = http.get(url, (res) => {
        res.resume();
        if (res.statusCode && res.statusCode < 500) return resolve();
        if (Date.now() - start > timeoutMs)
          return reject(new Error('timeout waiting app'));
        setTimeout(tick, 200);
      });
      req.on('error', () => {
        if (Date.now() - start > timeoutMs)
          return reject(new Error('timeout waiting app'));
        setTimeout(tick, 200);
      });
    };
    tick();
  });
}

// Nothing else here. This file is only used as the entrypoint for node child process.
console.log(`[CHILD] starting app... FDA_SERVER_PORT=${port}`);
// Your real entrypoint:
await import('../../index.js');

// If your app does not listen in NODE_ENV=test, force it by NOT using NODE_ENV=test in child.
// We'll handle that from the test (set NODE_ENV=integration).
// Add a health route in app? If you don't have one, we'll just rely on /fdas 400.
