import http from 'node:http';
import net from 'node:net';

const DEFAULT_TEST_SERVICE_PATH = '/public';

function withDefaultServicePath(headers = {}) {
  if (headers['Fiware-Service'] && !headers['Fiware-ServicePath']) {
    return {
      ...headers,
      'Fiware-ServicePath': DEFAULT_TEST_SERVICE_PATH,
    };
  }

  return headers;
}

export function httpReq({ method, url, headers, body }) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const finalHeaders = withDefaultServicePath(headers || {});
    const req = http.request(
      {
        method,
        hostname: u.hostname,
        port: u.port,
        path: u.pathname + u.search,
        headers: {
          ...finalHeaders,
          ...(body ? { 'Content-Type': 'application/json' } : {}),
        },
        timeout: 30_000,
      },
      (res) => {
        let data = '';
        res.setEncoding('utf8');
        res.on('data', (c) => (data += c));
        res.on('end', () => {
          resolve({
            status: res.statusCode,
            headers: res.headers,
            text: data,
            json: (() => {
              try {
                return JSON.parse(data);
              } catch {
                return null;
              }
            })(),
          });
        });
      },
    );
    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', reject);
    if (body) {
      req.write(JSON.stringify(body));
    }
    req.end();
  });
}

export function httpFormReq({ method, url, headers, form }) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const finalHeaders = withDefaultServicePath(headers || {});

    const body = new URLSearchParams(form).toString();

    const req = http.request(
      {
        method,
        hostname: u.hostname,
        port: u.port,
        path: u.pathname + u.search,
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(body),
          ...finalHeaders,
        },
        timeout: 30_000,
      },
      (res) => {
        let data = '';
        res.setEncoding('utf8');
        res.on('data', (c) => (data += c));
        res.on('end', () => {
          resolve({
            status: res.statusCode,
            headers: res.headers,
            text: data,
            json: (() => {
              try {
                return JSON.parse(data);
              } catch {
                return null;
              }
            })(),
          });
        });
      },
    );

    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

export function httpReqRaw({ method, url, headers, body, form }) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);

    let payload;
    const finalHeaders = withDefaultServicePath(headers || {});

    if (form) {
      payload = new URLSearchParams(form).toString();
      finalHeaders['Content-Type'] =
        finalHeaders['Content-Type'] || 'application/x-www-form-urlencoded';
      finalHeaders['Content-Length'] = Buffer.byteLength(payload);
    } else if (body) {
      payload = JSON.stringify(body);
      finalHeaders['Content-Type'] =
        finalHeaders['Content-Type'] || 'application/json';
      finalHeaders['Content-Length'] = Buffer.byteLength(payload);
    }

    const req = http.request(
      {
        method,
        hostname: u.hostname,
        port: u.port,
        path: u.pathname + u.search,
        headers: finalHeaders,
        timeout: 30_000,
      },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(Buffer.from(c)));
        res.on('end', () => {
          const buffer = Buffer.concat(chunks);
          const text = buffer.toString('utf8');
          resolve({
            status: res.statusCode,
            headers: res.headers,
            buffer,
            text,
            json: (() => {
              try {
                return JSON.parse(text);
              } catch {
                return null;
              }
            })(),
          });
        });
      },
    );

    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', reject);

    if (payload) {
      req.write(payload);
    }

    req.end();
  });
}

export function buildDaDataUrl(baseUrl, servicePath, fdaId, daId, query = {}) {
  const scope = (servicePath || '/private').replace(/^\//, '');
  const url = new URL(
    `${baseUrl}/${scope}/fdas/${encodeURIComponent(fdaId)}/das/${encodeURIComponent(daId)}/data`,
  );

  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined) {
      url.searchParams.set(key, String(value));
    }
  }

  return url.toString();
}

export function buildFdaDataUrl(baseUrl, servicePath, fdaId) {
  const scope = (servicePath || '/private').replace(/^\//, '');
  return `${baseUrl}/${scope}/fdas/${encodeURIComponent(fdaId)}/data`;
}

export function getFreePort() {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const port = srv.address().port;
      srv.close(() => resolve(port));
    });
  });
}

export async function connectWithRetry(client, attempts = 25, delayMs = 400) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      await client.connect();
      return;
    } catch (e) {
      lastErr = e;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw lastErr;
}

export async function waitUntilFDACompleted({
  baseUrl,
  service,
  fdaId,
  visibility = 'public',
  timeout = 10000,
  interval = 300,
}) {
  const start = Date.now();
  let lastSeen;

  while (Date.now() - start < timeout) {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/${visibility}/fdas/${encodeURIComponent(fdaId)}`,
      headers: { 'Fiware-Service': service },
    });

    if (res.status === 200 && res.json) {
      lastSeen = {
        status: res.json.status,
        progress: res.json.progress,
        error: res.json.error,
      };

      if (res.json.status === 'completed') {
        return res.json;
      }

      if (res.json.status === 'failed') {
        throw new Error(
          `FDA ${fdaId} reached failed state while waiting for completion (progress=${res.json.progress}, error=${res.json.error ?? 'n/a'})`,
        );
      }
    }

    await new Promise((r) => setTimeout(r, interval));
  }

  throw new Error(
    `Timeout waiting for FDA ${fdaId} to reach completed state (last status=${lastSeen?.status ?? 'unknown'}, progress=${lastSeen?.progress ?? 'unknown'}, error=${lastSeen?.error ?? 'n/a'})`,
  );
}
