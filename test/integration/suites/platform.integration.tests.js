import { test, expect } from '@jest/globals';

export function registerPlatformIntegrationTests({ getAppPort, httpReq }) {
  test('GET /health returns UP status', async () => {
    const appPort = getAppPort();
    const res = await httpReq({
      method: 'GET',
      url: `http://127.0.0.1:${appPort}/health`,
    });

    expect(res.status).toBe(200);
    expect(res.json.status).toBe('UP');
    expect(typeof res.json.uptimeSeconds).toBe('number');
  });

  test('GET /metrics returns text-format telemetry', async () => {
    const appPort = getAppPort();
    const res = await httpReq({
      method: 'GET',
      url: `http://127.0.0.1:${appPort}/metrics`,
    });

    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toContain('text/plain');
    expect(res.headers['content-type']).toContain('version=0.0.4');
    expect(res.headers['content-type']).toContain('charset=utf-8');
    expect(res.text).toContain('# TYPE fda_up gauge');
    expect(res.text).toContain('# EOF');
  });
}
