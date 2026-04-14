// Copyright 2025 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by Telefónica Soluciones
// de Informática y Comunicaciones de España, S.A.U (hereinafter TSOL) and is protected
// as copyright by the applicable legislation on intellectual property.
//
// It belongs to TSOL, and / or its licensors, the exclusive rights of reproduction,
// distribution, public communication and transformation, and any economic right on it,
// all without prejudice of the moral rights of the authors mentioned above. It is expressly
// forbidden to decompile, disassemble, reverse engineer, sublicense or otherwise transmit
// by any means, translate or create derivative works of the software and / or computer
// programs, and perform with respect to all or part of such programs, any type of exploitation.
//
// Any use of all or part of the software and / or computer program will require the
// express written consent of TSOL. In all cases, it will be necessary to make
// an express reference to TSOL ownership in the software and / or computer
// program.
//
// Non-fulfillment of the provisions set forth herein and, in general, any violation of
// the peaceful possession and ownership of these rights will be prosecuted by the means
// provided in both Spanish and international law. TSOL reserves any civil or
// criminal actions it may exercise to protect its rights.

import {
  jest,
  describe,
  beforeAll,
  afterAll,
  test,
  expect,
} from '@jest/globals';
import { GenericContainer, Wait } from 'testcontainers';
import {
  S3Client,
  CreateBucketCommand,
  HeadBucketCommand,
  ListBucketsCommand,
} from '@aws-sdk/client-s3';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import http from 'node:http';
import { spawn } from 'node:child_process';
import net from 'node:net';
import path from 'node:path';

const { Client } = pg;
const DEFAULT_TEST_SERVICE_PATH = '/public';

jest.setTimeout(240_000);

function withDefaultServicePath(headers = {}) {
  if (headers['Fiware-Service'] && !headers['Fiware-ServicePath']) {
    return {
      ...headers,
      'Fiware-ServicePath': DEFAULT_TEST_SERVICE_PATH,
    };
  }

  return headers;
}

function httpReq({ method, url, headers, body }) {
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

function httpFormReq({ method, url, headers, form }) {
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

function httpReqRaw({ method, url, headers, body, form }) {
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

function buildDaDataUrl(baseUrl, servicePath, fdaId, daId, query = {}) {
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

function getFreePort() {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const port = srv.address().port;
      srv.close(() => resolve(port));
    });
  });
}

async function connectWithRetry(client, attempts = 25, delayMs = 400) {
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

async function waitUntilFDACompleted({
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

export const EXECUTION_MODES = Object.freeze({
  COMBINED: 'combined',
  SEPARATED: 'separated',
});

export function runFDAIntegrationSuite({ mode, label }) {
  describe(`FDA API - integration (${label})`, () => {
    let minio;
    let mongo;
    let postgis;

    let minioHostPort;
    let minioUrl;
    let mongoUri;
    let pgHost;
    let pgPort;

    let appProc;
    let fetcherProc;
    let appPort;
    let baseUrl;

    const service = 'myservice';
    const servicePath = '/public';
    const visibility = 'public';
    const fdaId = 'fda1';
    const fdaId2 = 'fda2';
    const fdaId3 = 'fda3';
    const daId = 'da1';
    const daId2 = 'da2';

    beforeAll(async () => {
      // Containers
      minio = await new GenericContainer('minio/minio:latest')
        .withEnvironment({
          MINIO_ROOT_USER: 'admin',
          MINIO_ROOT_PASSWORD: 'admin123',
        })
        .withCommand(['server', '/data', '--console-address', ':9001'])
        .withExposedPorts(9000, 9001)
        .withWaitStrategy(Wait.forLogMessage(/API: http:\/\/.*:9000/))
        .start();
      minioHostPort = `${minio.getHost()}:${minio.getMappedPort(9000)}`;
      minioUrl = `http://${minioHostPort}`;

      mongo = await new GenericContainer('mongo:8.0')
        .withExposedPorts(27017)
        .withWaitStrategy(Wait.forLogMessage(/Waiting for connections/))
        .start();
      mongoUri = `mongodb://${mongo.getHost()}:${mongo.getMappedPort(27017)}/test-db`;

      postgis = await new GenericContainer('postgis/postgis:15-3.3')
        .withEnvironment({
          POSTGRES_USER: 'postgres',
          POSTGRES_PASSWORD: 'postgres',
          POSTGRES_DB: service,
        })
        .withExposedPorts(5432)
        .withWaitStrategy(Wait.forListeningPorts())
        .start();
      pgHost = '127.0.0.1';
      pgPort = postgis.getMappedPort(5432);

      console.log('[TEST] MinIO:', minioUrl);
      console.log('[TEST] Mongo:', mongoUri);
      console.log('[TEST] Postgres:', `${pgHost}:${pgPort}`);

      // Health Mongo
      {
        const mc = new MongoClient(mongoUri, {
          serverSelectionTimeoutMS: 10_000,
        });
        await mc.connect();
        await mc.db('admin').command({ ping: 1 });
        await mc.close();
        console.log('[TEST] Mongo OK');
      }

      // Health MinIO + bucket
      {
        const s3 = new S3Client({
          endpoint: minioUrl,
          region: 'us-east-1',
          credentials: { accessKeyId: 'admin', secretAccessKey: 'admin123' },
          forcePathStyle: true,
        });
        await s3.send(new ListBucketsCommand({}));
        try {
          await s3.send(new HeadBucketCommand({ Bucket: service }));
        } catch {
          await s3.send(new CreateBucketCommand({ Bucket: service }));
        }
        console.log('[TEST] MinIO OK');
      }

      // Health Postgres + seed
      {
        const pgClient = new Client({
          host: pgHost,
          port: pgPort,
          user: 'postgres',
          password: 'postgres',
          database: service,
          connectionTimeoutMillis: 10_000,
        });
        await connectWithRetry(pgClient);

        await pgClient.query(`DROP TABLE IF EXISTS public.users;`);
        await pgClient.query(`
        CREATE TABLE public.users (
          id INT PRIMARY KEY,
          name TEXT,
          age INT,
          timeinstant TIMESTAMP,
          authorized BOOLEAN
        );
      `);
        await pgClient.query(`
        INSERT INTO public.users (id, name, age, timeinstant, authorized)
        VALUES (1,'ana',30, '2020-08-17T18:25:28.332Z', true), (2,'bob',20, '2020-08-17T18:25:28.332Z', true), (3,'carlos',40, '2020-08-17T18:25:28.332Z', true);
      `);

        await pgClient.end();
        console.log('[TEST] Postgres OK');
      }

      await startApp();
    });

    afterAll(async () => {
      await stopApp();
      await Promise.allSettled([minio?.stop(), mongo?.stop(), postgis?.stop()]);
    });

    function wait(ms) {
      return new Promise((resolve) => setTimeout(resolve, ms));
    }

    function buildCommonEnv(overrides = {}) {
      return {
        ...process.env,
        NODE_ENV: 'integration',
        FDA_NODE_ENV: 'development',
        FDA_PG_USER: 'postgres',
        FDA_PG_PASSWORD: 'postgres',
        FDA_PG_HOST: pgHost,
        FDA_PG_PORT: String(pgPort),
        FDA_OBJSTG_USER: 'admin',
        FDA_OBJSTG_PASSWORD: 'admin123',
        FDA_OBJSTG_PROTOCOL: 'http',
        FDA_OBJSTG_ENDPOINT: minioHostPort,
        FDA_MONGO_URI: mongoUri,
        FDA_MAX_CONCURRENT_FRESH_QUERIES: '1',
        ...overrides,
      };
    }

    function attachProcLogs(proc, prefix) {
      proc.stdout.on('data', (d) =>
        console.log(`[${prefix}]`, d.toString().trim()),
      );
      proc.stderr.on('data', (d) =>
        console.error(`[${prefix}-ERR]`, d.toString().trim()),
      );
    }

    async function waitForApiReady() {
      const start = Date.now();
      while (Date.now() - start <= 30000) {
        try {
          const res = await httpReq({
            method: 'GET',
            url: `${baseUrl}/health`,
          });
          if (res.status === 200) {
            return;
          }
        } catch {
          // ignore, server not up yet
        }
        await wait(200);
      }

      throw new Error('Timeout waiting API to start');
    }

    async function startApp() {
      if (
        mode !== EXECUTION_MODES.COMBINED &&
        mode !== EXECUTION_MODES.SEPARATED
      ) {
        throw new Error(`Unknown integration runtime mode: ${mode}`);
      }

      appPort = await getFreePort();
      baseUrl = `http://127.0.0.1:${appPort}`;

      const entry = path.resolve('test/helpers/start-app.js');

      appProc = spawn(process.execPath, [entry], {
        stdio: ['ignore', 'pipe', 'pipe'],
        env: buildCommonEnv({
          FDA_SERVER_PORT: String(appPort),
          FDA_ROLE_APISERVER: 'true',
          FDA_ROLE_FETCHER:
            mode === EXECUTION_MODES.COMBINED ? 'true' : 'false',
          FDA_ROLE_SYNCQUERIES: 'true',
        }),
      });

      attachProcLogs(appProc, 'API');

      await waitForApiReady();
      console.log('[TEST] API OK at', baseUrl);

      if (mode === EXECUTION_MODES.SEPARATED) {
        fetcherProc = spawn(process.execPath, [entry], {
          stdio: ['ignore', 'pipe', 'pipe'],
          env: buildCommonEnv({
            FDA_ROLE_APISERVER: 'false',
            FDA_ROLE_FETCHER: 'true',
            FDA_ROLE_SYNCQUERIES: 'false',
          }),
        });

        attachProcLogs(fetcherProc, 'FETCHER');
        await wait(2000);
        console.log('[TEST] Fetcher OK');
      }
    }

    async function stopProcess(proc) {
      if (!proc) {
        return;
      }

      proc.kill('SIGTERM');
      await wait(500);
      if (!proc.killed) {
        proc.kill('SIGKILL');
      }
    }

    async function stopApp() {
      if (mode === EXECUTION_MODES.SEPARATED) {
        await stopProcess(fetcherProc);
        fetcherProc = undefined;
      }

      await stopProcess(appProc);
      appProc = undefined;
    }

    test('GET /health returns UP status', async () => {
      const res = await httpReq({
        method: 'GET',
        url: `http://127.0.0.1:${appPort}/health`,
      });
      expect(res.status).toBe(200);
      expect(res.json.status).toBe('UP');
      expect(typeof res.json.uptimeSeconds).toBe('number');
    });

    test('GET /metrics returns text-format telemetry', async () => {
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

    test('POST /fdas creates an FDA (uploads CSV then converts to Parquet)', async () => {
      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          // query base to extract from PG to CSV
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'users dataset',
        },
      });

      if (res.status >= 400) {
        console.error('POST /fdas failed:', res.status, res.json ?? res.text);
      }
      expect(res.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });
    });

    test('POST /fdas creates an FDA with various refresh policies', async () => {
      // Test with a window refresh policy (week) and partitioned by day, without compression, with a window size of 1 day
      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: 'fda_refresh',
          // query base to extract from PG to CSV
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'users dataset',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '0 0 * * 0',
              fetchSize: 'week',
              windowSize: 'day',
            },
          },
          timeColumn: 'timeinstant',
          objStgConf: {
            partition: 'week',
            compression: false,
          },
        },
      });

      if (res.status >= 400) {
        console.error('POST /fdas failed:', res.status, res.json ?? res.text);
      }
      expect(res.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: 'fda_refresh' });

      // Test with a window refresh policy (week) and partitioned by day, without objStgConf
      const yearfetchSize = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: 'fda_refresh_noobjStgConf',
          // query base to extract from PG to CSV
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'users dataset',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '0 0 * * 0',
              fetchSize: 'year',
              windowSize: 'day',
            },
          },
          timeColumn: 'timeinstant',
        },
      });

      if (yearfetchSize.status >= 400) {
        console.error(
          'POST /fdas failed:',
          yearfetchSize.status,
          yearfetchSize.json ?? yearfetchSize.text,
        );
      }
      expect(yearfetchSize.status).toBe(202);
      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: 'fda_refresh_noobjStgConf',
      });

      // Test with a fetchSize different from partition
      const diffFetchPartition = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: 'fda_refresh',
          // query base to extract from PG to CSV
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'users dataset',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '0 0 * * 0',
              fetchSize: 'week',
              windowSize: 'day',
            },
          },
          timeColumn: 'timeinstant',
          objStgConf: {
            partition: 'month',
            compression: false,
          },
        },
      });

      if (diffFetchPartition.status >= 400) {
        console.error(
          'POST /fdas failed as expected:',
          diffFetchPartition.status,
          diffFetchPartition.json ?? diffFetchPartition.text,
        );
      }
      //
      expect(diffFetchPartition.status).toBe(400);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: 'fda_refresh' });

      // Test with a window refresh policy (week) and partitioned by day, without compression, with a window size of 1 day
      const res2 = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: 'fda_refresh2',
          // query base to extract from PG to CSV
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'users dataset',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '0 0 * * 0',
              fetchSize: 'week',
              windowSize: 'week',
            },
          },
          timeColumn: 'timeinstant',
          objStgConf: {
            partition: 'week',
            compression: false,
          },
        },
      });

      if (res2.status >= 400) {
        console.error(
          'POST /fdas failed:',
          res2.status,
          res2.json ?? res2.text,
        );
      }
      expect(res2.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: 'fda_refresh2' });

      // Test with partition type but no time column, should return error as time column is required for partitioning
      const res3 = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: 'fda_refresh3',
          // query base to extract from PG to CSV
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'users dataset',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '0 0 1 * *',
              fetchSize: 'month',
              windowSize: 'month',
            },
          },
          objStgConf: {
            partition: 'month',
            compression: false,
          },
        },
      });

      if (res3.status >= 400) {
        console.error(
          'POST /fdas failed as expected:',
          res3.status,
          res3.json ?? res3.text,
        );
      }
      expect(res3.status).toBe(400);

      // Test with invalid partition type
      const res4 = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: 'fda_refresh4',
          // query base to extract from PG to CSV
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          description: 'users dataset',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '0 0 1 * *',
              fetchSize: 'month',
              windowSize: 'year',
            },
          },
          timeColumn: 'timeinstant',
          objStgConf: {
            partition: 'fakePartition',
            compression: false,
          },
        },
      });

      if (res4.status >= 400) {
        console.error(
          'POST /fdas failed:',
          res4.status,
          res4.json ?? res4.text,
        );
      }
      expect(res4.status).toBe(400);
    });

    test('POST /fdas tries to creates an FDA without id and is detected', async () => {
      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: null,
          // query base to extract from PG to CSV
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'users dataset',
        },
      });

      if (res.status >= 400) {
        console.error(
          'POST /fdas failed as expected:',
          res.status,
          res.json ?? res.text,
        );
      }
      expect(res.status).toBe(400);
    });
    test('POST /fdas try creates an FDA without body', async () => {
      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });
      if (res.status >= 400) {
        console.error(
          'POST /fdas failed as expected:',
          res.status,
          res.json ?? res.text,
        );
      }
      expect(res.status).toBe(400);
    });
    test('POST /fdas with duplicate id returns error', async () => {
      await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: { 'Fiware-Service': service },
        body: {
          id: fdaId3,
          query: 'SELECT id FROM public.users',
          description: 'duplicate test',
        },
      });

      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId3, // same id
          query: 'SELECT id FROM public.users',
          description: 'duplicate test',
        },
      });

      expect(res.status).toBe(409);
      expect(res.json.error).toBe('DuplicatedKey');

      await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaId3 });
    });

    test('POST /fdas allows same id in same service when servicePath differs', async () => {
      const scopedFdaId = 'fda_same_id_diff_servicepath';
      const otherServicePath = '/other-path';

      const firstCreate = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: scopedFdaId,
          query: 'SELECT id FROM public.users',
          description: 'scope one',
        },
      });

      expect(firstCreate.status).toBe(202);

      const secondCreate = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': otherServicePath,
        },
        body: {
          id: scopedFdaId,
          query: 'SELECT id FROM public.users',
          description: 'scope two',
        },
      });

      expect(secondCreate.status).toBe(202);

      const wrongScopeRead = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${scopedFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': '/unknown-path',
        },
      });

      expect(wrongScopeRead.status).toBe(403);
      expect(wrongScopeRead.json.error).toBe('ServicePathMismatch');

      const scopeOneRead = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${scopedFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
      });

      expect(scopeOneRead.status).toBe(200);

      const scopeTwoRead = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${scopedFdaId}`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': otherServicePath,
        },
      });

      expect(scopeTwoRead.status).toBe(200);

      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: scopedFdaId,
        servicePath,
      });
      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: scopedFdaId,
        servicePath: otherServicePath,
      });
    });

    test('POST /fdas pending allows DA creation but rejects GET /{visibility}/fdas/{fdaId}/das/{daId}/data until first completion', async () => {
      const pendingFdaId = 'fda_pending_first_fetch';
      const pendingDaId = 'da_pending_first_fetch';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: pendingFdaId,
          // Force a long first fetch to keep the FDA non-queryable for this test.
          query:
            'SELECT id, name, age FROM public.users, (SELECT pg_sleep(6)) AS delayed_fetch',
          description: 'pending fda test',
        },
      });

      expect(createFda.status).toBe(202);

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${pendingFdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: pendingDaId,
          description: 'pending da test',
          query: `
          SELECT id, name, age
          WHERE age > $minAge
          ORDER BY id
        `,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              required: true,
            },
          ],
        },
      });

      expect(createDa.status).toBe(201);

      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, pendingFdaId, pendingDaId, {
          minAge: 20,
        }),
        headers: { 'Fiware-Service': service },
      });

      if (queryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected while FDA is pending:',
          queryRes.status,
          queryRes.json ?? queryRes.text,
        );
      }

      expect(queryRes.status).toBe(409);
      expect(queryRes.json.error).toBe('FDAUnavailable');

      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: pendingFdaId,
        timeout: 30000,
      });

      const queryAfterCompletion = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, pendingFdaId, pendingDaId, {
          minAge: 20,
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(queryAfterCompletion.status).toBe(200);
      expect(queryAfterCompletion.json).toEqual([
        { id: '1', name: 'ana', age: '30' },
        { id: '3', name: 'carlos', age: '40' },
      ]);
    });

    test('GET /fdas returns list', async () => {
      const res = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: { 'Fiware-Service': service },
      });

      if (res.status >= 400) {
        console.error('GET /fdas failed:', res.status, res.json ?? res.text);
      }
      expect(res.status).toBe(200);
      expect(Array.isArray(res.json)).toBe(true);
      expect(res.json.some((x) => x.id === fdaId)).toBe(true);
    });

    test('POST /fdas/:fdaId/das + GET /{visibility}/fdas/{fdaId}/das/{daId}/data executes DuckDB against Parquet', async () => {
      // DuckDB reads parquet generated in  s3://<bucket>/<fdaID>.parquet
      const daQuery = `
      SELECT id, name, age
      WHERE age > $minAge
      ORDER BY id;
    `;

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daId,
          description: 'age filter',
          query: daQuery,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              required: true,
            },
          ],
        },
      });

      if (createDa.status >= 400) {
        console.error(
          'POST /das failed:',
          createDa.status,
          createDa.json ?? createDa.text,
        );
      }
      expect(createDa.status).toBe(201);

      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, {
          minAge: 25,
        }),
        headers: { 'Fiware-Service': service },
      });

      if (queryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed:',
          queryRes.status,
          queryRes.json ?? queryRes.text,
        );
      }
      expect(queryRes.status).toBe(200);
      expect(queryRes.json).toEqual([
        { id: '1', name: 'ana', age: '30' },
        { id: '3', name: 'carlos', age: '40' },
      ]);
    });

    test('GET /fdas/:fdaId/das and GET /fdas/:fdaId/das/:daId return stored DA and DaNotFound for unknown DA', async () => {
      const listRes = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
      });

      if (listRes.status >= 400) {
        console.error(
          'GET /fdas/:fdaId/das failed:',
          listRes.status,
          listRes.json ?? listRes.text,
        );
      }

      expect(listRes.status).toBe(200);
      expect(Array.isArray(listRes.json)).toBe(true);
      expect(listRes.json.some((x) => x.id === daId)).toBe(true);

      const getRes = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daId}`,
        headers: { 'Fiware-Service': service },
      });

      expect(getRes.status).toBe(200);
      expect(getRes.json).toMatchObject({
        id: daId,
        description: 'age filter',
      });

      const missingRes = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/da_does_not_exist`,
        headers: { 'Fiware-Service': service },
      });

      expect(missingRes.status).toBe(404);
      expect(missingRes.json.error).toBe('DaNotFound');
    });

    test('POST /fdas/:fdaId/das rejects query with FROM clause', async () => {
      const badQuery = `
      FROM read_parquet('s3://some/path')
      SELECT id
    `;

      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: 'da_bad',
          description: 'should fail',
          query: badQuery,
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidDAQuery');
    });

    test('PUT /fdas/:fdaId/das/:daId updates DA query and affects execution', async () => {
      const daIdToUpdate = 'da_update';

      // Create DA
      const createRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daIdToUpdate,
          description: 'initial filter',
          query: `
          SELECT id, name, age
          WHERE age > $minAge
          ORDER BY id
        `,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              required: true,
            },
          ],
        },
      });

      expect(createRes.status).toBe(201);

      // Execute with minAge=25 (should return 2 rows)
      const firstQuery = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daIdToUpdate, {
          minAge: 25,
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(firstQuery.status).toBe(200);
      expect(firstQuery.json.length).toBe(2);

      // Update DA (change filter logic)
      const updateRes = await httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daIdToUpdate}`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
        },
        body: {
          description: 'updated filter',
          query: `
          SELECT id, name, age
          WHERE age > $minAge
          AND age < 35
          ORDER BY id
        `,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              required: true,
            },
          ],
        },
      });

      if (updateRes.status >= 400) {
        console.error(
          'PUT /das failed:',
          updateRes.status,
          updateRes.json ?? updateRes.text,
        );
      }

      expect(updateRes.status).toBe(204);

      // Execute again → now should return only 1 row (ana, 30)
      const secondQuery = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daIdToUpdate, {
          minAge: 25,
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(secondQuery.status).toBe(200);
      expect(secondQuery.json).toEqual([{ id: '1', name: 'ana', age: '30' }]);
    });

    test('PUT /fdas/:fdaId/das/:daId updates DA with incorrect params', async () => {
      const daIdToUpdate = 'da_update';

      // Update DA (bad range param)
      const rangeUpdateRes = await httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daIdToUpdate}`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
        },
        body: {
          id: 'ignored_in_put',
          description: 'updated filter',
          params: [
            {
              name: 'activity',
              type: 'Number',
              range: ['badRange', 14],
            },
          ],
        },
      });

      if (rangeUpdateRes.status >= 400) {
        console.error(
          'PUT /das failed as expected:',
          rangeUpdateRes.status,
          rangeUpdateRes.json ?? rangeUpdateRes.text,
        );
      }

      expect(rangeUpdateRes.status).toBe(400);

      // Update DA (bad range enum)
      const enumUpdateRes = await httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daIdToUpdate}`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
        },
        body: {
          id: 'ignored_in_put',
          description: 'updated filter',
          params: [
            {
              name: 'activity',
              type: 'Number',
              enum: ['option', true],
            },
          ],
        },
      });

      if (enumUpdateRes.status >= 400) {
        console.error(
          'PUT /das failed as expected:',
          enumUpdateRes.status,
          enumUpdateRes.json ?? enumUpdateRes.text,
        );
      }

      expect(enumUpdateRes.status).toBe(400);
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns JSON array when Accept: application/json', async () => {
      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, {
          minAge: 25,
        }),
        headers: { 'Fiware-Service': service, Accept: 'application/json' },
      });

      if (res.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data (json) failed:',
          res.status,
          res.json ?? res.text,
        );
      }
      expect(res.status).toBe(200);
      expect(res.json).toEqual([
        { id: '1', name: 'ana', age: '30' },
        { id: '3', name: 'carlos', age: '40' },
      ]);
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data with fresh=true runs query against PostgreSQL source', async () => {
      const daFreshId = 'da_fresh_users';

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daFreshId,
          description: 'fresh query test',
          query: `
          SELECT id, name, age
          WHERE age > $minAge
          ORDER BY id
        `,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              required: true,
            },
          ],
        },
      });

      expect(createDa.status).toBe(201);

      const insertedId = 1001;
      const pgClient = new Client({
        host: pgHost,
        port: pgPort,
        user: 'postgres',
        password: 'postgres',
        database: service,
        connectionTimeoutMillis: 10_000,
      });

      await connectWithRetry(pgClient);

      try {
        await pgClient.query(
          `INSERT INTO public.users (id, name, age, timeinstant, authorized)
         VALUES ($1, $2, $3, $4, $5)`,
          [insertedId, 'diana', 35, '2020-08-17T18:25:28.332Z', true],
        );

        const cachedRes = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(baseUrl, servicePath, fdaId, daFreshId, {
            minAge: 25,
          }),
          headers: { 'Fiware-Service': service },
        });

        expect(cachedRes.status).toBe(200);
        expect(cachedRes.json.map((x) => x.name)).toEqual(['ana', 'carlos']);

        const freshRes = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(baseUrl, servicePath, fdaId, daFreshId, {
            minAge: 25,
            fresh: true,
          }),
          headers: { 'Fiware-Service': service },
        });

        expect(freshRes.status).toBe(200);
        expect(freshRes.json.map((x) => x.name)).toEqual([
          'ana',
          'carlos',
          'diana',
        ]);
      } finally {
        await pgClient.query('DELETE FROM public.users WHERE id = $1', [
          insertedId,
        ]);
        await pgClient.end();
      }
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data with fresh=true supports default Boolean and DateTime params', async () => {
      const fdaFreshDefaultsId = 'fda_fresh_defaults';
      const daFreshDefaultsId = 'da_fresh_defaults';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaFreshDefaultsId,
          description: 'fresh defaults test fda',
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: fdaFreshDefaultsId,
      });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaFreshDefaultsId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daFreshDefaultsId,
          description: 'fresh query defaults test',
          query: `
          SELECT id, name
          WHERE authorized = $authorized
          AND timeinstant >= $timeinstant
          ORDER BY id
        `,
          params: [
            {
              name: 'authorized',
              type: 'Boolean',
              default: true,
            },
            {
              name: 'timeinstant',
              type: 'DateTime',
              default: '2020-01-01T00:00:00.000Z',
            },
          ],
        },
      });

      expect(createDa.status).toBe(201);

      const freshRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          baseUrl,
          servicePath,
          fdaFreshDefaultsId,
          daFreshDefaultsId,
          {
            fresh: true,
          },
        ),
        headers: { 'Fiware-Service': service },
      });

      if (freshRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data fresh defaults failed:',
          freshRes.status,
          freshRes.json ?? freshRes.text,
        );
      }

      expect(freshRes.status).toBe(200);
      expect(freshRes.json).toEqual([
        { id: 1, name: 'ana' },
        { id: 2, name: 'bob' },
        { id: 3, name: 'carlos' },
      ]);
    });

    test('POST /fdas/:fdaId/das rejects incompatible default values', async () => {
      const fdaBadDefaultId = 'fda_bad_default';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaBadDefaultId,
          description: 'invalid default test fda',
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: fdaBadDefaultId,
      });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaBadDefaultId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: 'da_bad_default',
          description: 'should reject invalid default',
          query: `
          SELECT id, name
          WHERE authorized = $authorized
          ORDER BY id
        `,
          params: [
            {
              name: 'authorized',
              type: 'Boolean',
              default: 'notBool',
            },
          ],
        },
      });

      expect(createDa.status).toBe(400);
      expect(createDa.json.error).toBe('InvalidParam');
      expect(createDa.json.description).toContain(
        'Default value for param "authorized" not of valid type (Boolean).',
      );
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data with fresh=true returns 429 when max concurrent fresh queries is reached', async () => {
      const fdaFreshLimitId = 'fda_fresh_limit';
      const daFreshLimitId = 'da_fresh_limit';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaFreshLimitId,
          description: 'fresh query concurrency limit test fda',
          query:
            'SELECT id, name, age FROM public.users, (SELECT pg_sleep(0.8)) AS delayed_fetch',
        },
      });

      expect(createFda.status).toBe(202);

      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: fdaFreshLimitId,
        timeout: 20000,
        interval: 300,
      });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaFreshLimitId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daFreshLimitId,
          description: 'fresh query concurrency limit test',
          query: `
          SELECT id, name, age
          WHERE age > $minAge
          ORDER BY id
        `,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              required: true,
            },
          ],
        },
      });

      expect(createDa.status).toBe(201);

      const firstFreshRequest = httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          baseUrl,
          servicePath,
          fdaFreshLimitId,
          daFreshLimitId,
          {
            minAge: 0,
            fresh: true,
          },
        ),
        headers: { 'Fiware-Service': service },
      });

      await new Promise((r) => setTimeout(r, 100));

      const secondFreshRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(
          baseUrl,
          servicePath,
          fdaFreshLimitId,
          daFreshLimitId,
          {
            minAge: 0,
            fresh: true,
          },
        ),
        headers: { 'Fiware-Service': service },
      });

      expect(secondFreshRes.status).toBe(429);
      expect(secondFreshRes.json.error).toBe('TooManyFreshQueries');

      const firstFreshRes = await firstFreshRequest;
      expect(firstFreshRes.status).toBe(200);
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data with fresh=true streams NDJSON progressively from PostgreSQL (real streaming)', async () => {
      const fdaFreshStreamId = 'fda_fresh_stream_real';
      const daFreshStreamId = 'da_fresh_stream_real';

      const TOTAL_ROWS = 600; // Forces multiple batches

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaFreshStreamId,
          description: 'fresh ndjson real streaming test fda',
          query: 'SELECT id, name, age FROM public.users',
        },
      });

      expect(createFda.status).toBe(202);

      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: fdaFreshStreamId,
        timeout: 20000,
        interval: 300,
      });

      // Create DA for fresh NDJSON streaming
      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaFreshStreamId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daFreshStreamId,
          description: 'fresh ndjson real streaming test',
          query: `
          SELECT id, name, age
          ORDER BY id
        `,
        },
      });

      expect(createDa.status).toBe(201);

      // Insertar muchos datos
      const pgClient = new Client({
        host: pgHost,
        port: pgPort,
        user: 'postgres',
        password: 'postgres',
        database: service,
      });

      await connectWithRetry(pgClient);

      const extraIds = [];

      try {
        for (let i = 0; i < TOTAL_ROWS; i++) {
          const id = 1000 + i;
          extraIds.push(id);
          await pgClient.query(
            `INSERT INTO public.users (id, name, age, timeinstant, authorized)
          VALUES ($1, $2, $3, NOW(), true)`,
            [id, `user_${i}`, 20 + (i % 50)],
          );
        }

        // Real streaming request
        const url = new URL(
          buildDaDataUrl(
            baseUrl,
            servicePath,
            fdaFreshStreamId,
            daFreshStreamId,
            { fresh: true },
          ),
        );

        const chunks = [];
        let receivedChunks = 0;
        let firstChunkTime = null;

        await new Promise((resolve, reject) => {
          const req = http.request(
            {
              method: 'GET',
              hostname: url.hostname,
              port: url.port,
              path: url.pathname + url.search,
              headers: {
                'Fiware-Service': service,
                'Fiware-ServicePath': servicePath,
                Accept: 'application/x-ndjson',
              },
            },
            (res) => {
              expect(res.statusCode).toBe(200);
              expect(res.headers['content-type']).toContain(
                'application/x-ndjson',
              );

              res.setEncoding('utf8');

              res.on('data', (chunk) => {
                receivedChunks++;

                if (!firstChunkTime) {
                  firstChunkTime = Date.now();
                }

                chunks.push(chunk);
              });

              res.on('end', resolve);
            },
          );

          req.on('error', reject);
          req.end();
        });

        // Verify streaming
        expect(receivedChunks).toBeGreaterThan(1); // Multiple chunks
        expect(firstChunkTime).not.toBeNull(); // First chunk received

        // Parse and validate NDJSON
        const fullText = chunks.join('');
        const lines = fullText
          .trim()
          .split('\n')
          .map((l) => JSON.parse(l));

        expect(lines.length).toBeGreaterThanOrEqual(3 + TOTAL_ROWS);
        expect(lines[0]).toMatchObject({ id: 1, name: 'ana' });
        expect(lines[lines.length - 1].name).toMatch(/^user_/);
      } finally {
        await pgClient.query(`DELETE FROM public.users WHERE id >= 1000`);
        await pgClient.end();
      }
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data serializes dates consistently across cached and fresh JSON/NDJSON/CSV', async () => {
      const fixtureTable = 'format_serialization_fixture';
      const fdaSerializationId = 'fda_serialization_regression';
      const daSerializationId = 'da_serialization_regression';
      const expectedDates = [
        '2024-01-10T12:34:56.789Z',
        '2024-01-11T08:15:30.123Z',
      ];

      const pgClient = new Client({
        host: pgHost,
        port: pgPort,
        user: 'postgres',
        password: 'postgres',
        database: service,
      });

      await connectWithRetry(pgClient);

      try {
        await pgClient.query(`DROP TABLE IF EXISTS public.${fixtureTable}`);
        await pgClient.query(`
          CREATE TABLE public.${fixtureTable} (
            id INT PRIMARY KEY,
            observed_at TIMESTAMPTZ NOT NULL,
            total_bigint BIGINT NOT NULL,
            note TEXT
          )
        `);
        await pgClient.query(
          `
            INSERT INTO public.${fixtureTable} (id, observed_at, total_bigint, note)
            VALUES
              (1, $1::timestamptz, 42, 'alpha'),
              (2, $2::timestamptz, 84, 'with,comma')
          `,
          expectedDates,
        );

        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaSerializationId,
            description: 'issue 137 format matrix regression fda',
            query: `
              SELECT
                id,
                observed_at AS date,
                total_bigint AS total,
                note
              FROM public.${fixtureTable}
              ORDER BY id
            `,
          },
        });

        expect(createFda.status).toBe(202);

        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: fdaSerializationId,
        });

        const createDa = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/${fdaSerializationId}/das`,
          headers: { 'Fiware-Service': service },
          body: {
            id: daSerializationId,
            description: 'issue 137 format matrix regression da',
            query: `
              SELECT date, total, note
              ORDER BY date
            `,
          },
        });

        expect(createDa.status).toBe(201);

        const cachedJson = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            fdaSerializationId,
            daSerializationId,
          ),
          headers: {
            'Fiware-Service': service,
            Accept: 'application/json',
          },
        });

        expect(cachedJson.status).toBe(200);
        expect(cachedJson.json.map((row) => row.date)).toEqual(expectedDates);
        expect(
          cachedJson.json.every((row) =>
            ['string', 'number'].includes(typeof row.total),
          ),
        ).toBe(true);

        const cachedNdjson = await httpReqRaw({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            fdaSerializationId,
            daSerializationId,
          ),
          headers: {
            'Fiware-Service': service,
            Accept: 'application/x-ndjson',
          },
        });

        expect(cachedNdjson.status).toBe(200);
        const cachedNdjsonRows = cachedNdjson.text
          .split('\n')
          .filter((line) => line.trim())
          .map((line) => JSON.parse(line));
        expect(cachedNdjsonRows.map((row) => row.date)).toEqual(expectedDates);
        expect(
          cachedNdjsonRows.every((row) =>
            ['string', 'number'].includes(typeof row.total),
          ),
        ).toBe(true);

        const cachedCsv = await httpReqRaw({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            fdaSerializationId,
            daSerializationId,
          ),
          headers: {
            'Fiware-Service': service,
            Accept: 'text/csv',
          },
        });

        expect(cachedCsv.status).toBe(200);
        expect(cachedCsv.headers['content-type']).toContain('text/csv');
        expect(cachedCsv.text).toContain(expectedDates[0]);
        expect(cachedCsv.text).toContain(expectedDates[1]);
        expect(cachedCsv.text).not.toContain('[object Object]');

        const freshJson = await httpReq({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            fdaSerializationId,
            daSerializationId,
            { fresh: true },
          ),
          headers: {
            'Fiware-Service': service,
            Accept: 'application/json',
          },
        });

        expect(freshJson.status).toBe(200);
        expect(freshJson.json.map((row) => row.date)).toEqual(expectedDates);
        expect(freshJson.json.map((row) => row.total)).toEqual([42, 84]);

        const freshNdjson = await httpReqRaw({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            fdaSerializationId,
            daSerializationId,
            { fresh: true },
          ),
          headers: {
            'Fiware-Service': service,
            Accept: 'application/x-ndjson',
          },
        });

        expect(freshNdjson.status).toBe(200);
        expect(freshNdjson.headers['content-type']).toContain(
          'application/x-ndjson',
        );
        const freshNdjsonRows = freshNdjson.text
          .split('\n')
          .filter((line) => line.trim())
          .map((line) => JSON.parse(line));
        expect(freshNdjsonRows.map((row) => row.date)).toEqual(expectedDates);
        expect(freshNdjsonRows.map((row) => row.total)).toEqual([42, 84]);

        const freshCsv = await httpReqRaw({
          method: 'GET',
          url: buildDaDataUrl(
            baseUrl,
            servicePath,
            fdaSerializationId,
            daSerializationId,
            { fresh: true },
          ),
          headers: {
            'Fiware-Service': service,
            Accept: 'text/csv',
          },
        });

        expect(freshCsv.status).toBe(200);
        expect(freshCsv.headers['content-type']).toContain('text/csv');
        expect(freshCsv.text).toContain(expectedDates[0]);
        expect(freshCsv.text).toContain(expectedDates[1]);
        expect(freshCsv.text).not.toContain('[object Object]');
      } finally {
        await pgClient.query(`DROP TABLE IF EXISTS public.${fixtureTable}`);
        await pgClient.end();
      }
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data rejects invalid fresh query param', async () => {
      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, {
          minAge: 25,
          fresh: 'maybe',
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('BadRequest');
    });

    test('POST /fdas/:fdaId/das + GET /{visibility}/fdas/{fdaId}/das/{daId}/data using default params', async () => {
      // DuckDB reads parquet generated in  s3://<bucket>/<fdaID>.parquet
      const daQuery = `
      SELECT id, name, age
      WHERE age > $minAge AND name = $name AND timeinstant = $timeinstant AND authorized = $authorized
      ORDER BY id;
    `;

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daId2,
          description: 'get user',
          query: daQuery,
          params: [
            {
              name: 'name',
              type: 'Text',
              required: true,
              enum: ['ana', 'carlos'],
            },
            {
              name: 'minAge',
              type: 'Number',
              default: 25,
              range: [20, 50],
            },
            {
              name: 'timeinstant',
              type: 'DateTime',
              default: '2020-08-17T18:25:28.332Z',
            },
            {
              name: 'authorized',
              type: 'Boolean',
              default: true,
            },
          ],
        },
      });

      if (createDa.status >= 400) {
        console.error(
          'POST /das failed:',
          createDa.status,
          createDa.json ?? createDa.text,
        );
      }
      expect(createDa.status).toBe(201);

      // Create DA with params but no Type
      const noTypeDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: `${daId2}_noType`,
          description: 'get user',
          query: daQuery,
          params: [
            {
              name: 'minAge',
              default: 25,
              range: ['badRange', 50],
            },
          ],
        },
      });

      if (noTypeDa.status >= 400) {
        console.error(
          'POST /das failed as expected:',
          noTypeDa.status,
          noTypeDa.json ?? noTypeDa.text,
        );
      }
      expect(noTypeDa.status).toBe(400);

      // Create DA with bad params Type (invalid value)
      const badTypeDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: `${daId2}_badType`,
          description: 'get user',
          query: daQuery,
          params: [
            {
              name: 'minAge',
              type: 'FakeType',
              default: 25,
              range: ['badRange', 50],
            },
          ],
        },
      });

      if (badTypeDa.status >= 400) {
        console.error(
          'POST /das failed as expected:',
          badTypeDa.status,
          badTypeDa.json ?? badTypeDa.text,
        );
      }
      expect(badTypeDa.status).toBe(400);

      // Create DA with incompatible default value for declared type
      const badDefaultDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: `${daId2}_badDefault`,
          description: 'get user',
          query: daQuery,
          params: [
            {
              name: 'authorized',
              type: 'Boolean',
              default: 'notBool',
            },
          ],
        },
      });

      if (badDefaultDa.status >= 400) {
        console.error(
          'POST /das failed as expected:',
          badDefaultDa.status,
          badDefaultDa.json ?? badDefaultDa.text,
        );
      }
      expect(badDefaultDa.status).toBe(400);

      // Create DA with bad params Enum (non string or number values)
      const badEnumDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: `${daId2}_badType`,
          description: 'get user',
          query: daQuery,
          params: [
            {
              name: 'name',
              type: 'Text',
              default: 'carlos',
              enum: [true, false],
            },
          ],
        },
      });

      if (badEnumDa.status >= 400) {
        console.error(
          'POST /das failed as expected:',
          badEnumDa.status,
          badEnumDa.json ?? badEnumDa.text,
        );
      }
      expect(badEnumDa.status).toBe(400);

      // Create DA with bad params range
      const createBadDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: `${daId2}_badRange`,
          description: 'get user',
          query: daQuery,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              default: 25,
              range: ['badRange', 50],
            },
          ],
        },
      });

      if (createBadDa.status >= 400) {
        console.error(
          'POST /das failed as expected:',
          createBadDa.status,
          createBadDa.json ?? createBadDa.text,
        );
      }
      expect(createBadDa.status).toBe(400);

      // Create DA with bad params range (bad order)
      const createBadRangeDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: `${daId2}_badRangeOrder`,
          description: 'get user',
          query: daQuery,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              default: 25,
              range: [60, 50],
            },
          ],
        },
      });

      if (createBadRangeDa.status >= 400) {
        console.error(
          'POST /das failed as expected:',
          createBadRangeDa.status,
          createBadRangeDa.json ?? createBadRangeDa.text,
        );
      }
      expect(createBadRangeDa.status).toBe(400);

      // Create DA with bad params range (bad length)
      const badRangeLengthDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: `${daId2}_badRangeLength`,
          description: 'get user',
          query: daQuery,
          params: [
            {
              name: 'minAge',
              type: 'Number',
              default: 25,
              range: [40, 50, 60],
            },
          ],
        },
      });

      if (badRangeLengthDa.status >= 400) {
        console.error(
          'POST /das failed as expected:',
          badRangeLengthDa.status,
          badRangeLengthDa.json ?? badRangeLengthDa.text,
        );
      }
      expect(badRangeLengthDa.status).toBe(400);

      // Query with name outside of the enum
      const enumQueryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          minAge: 25,
          name: 'fakeNAme',
        }),
        headers: { 'Fiware-Service': service },
      });

      if (enumQueryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
          enumQueryRes.status,
          enumQueryRes.json ?? enumQueryRes.text,
        );
      }
      expect(enumQueryRes.status).toBe(400);

      // Query with age outside of the range
      const rangeQueryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          minAge: 60,
          name: 'ana',
        }),
        headers: { 'Fiware-Service': service },
      });

      if (rangeQueryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
          rangeQueryRes.status,
          rangeQueryRes.json ?? rangeQueryRes.text,
        );
      }
      expect(rangeQueryRes.status).toBe(400);

      // Good query using default values
      const defaultsQueryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          name: 'carlos',
        }),
        headers: { 'Fiware-Service': service },
      });

      if (defaultsQueryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed:',
          defaultsQueryRes.status,
          defaultsQueryRes.json ?? defaultsQueryRes.text,
        );
      }
      expect(defaultsQueryRes.status).toBe(200);
      expect(defaultsQueryRes.json).toEqual([
        { id: '3', name: 'carlos', age: '40' },
      ]);

      // Query without required value
      const requiredQueryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          minAge: 25,
        }),
        headers: { 'Fiware-Service': service },
      });

      if (requiredQueryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
          requiredQueryRes.status,
          requiredQueryRes.json ?? requiredQueryRes.text,
        );
      }
      expect(requiredQueryRes.status).toBe(400);

      // Query without proper type param
      const typeQueryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          minAge: 'text',
          name: 'carlos',
        }),
        headers: { 'Fiware-Service': service },
      });

      if (typeQueryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
          typeQueryRes.status,
          typeQueryRes.json ?? typeQueryRes.text,
        );
      }
      expect(typeQueryRes.status).toBe(400);

      // Query without proper date (ISO8601)
      const dateQueryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          name: 'carlos',
          timeinstant: '2020-08-17 18:25:28.332+01:00',
        }),
        headers: { 'Fiware-Service': service },
      });

      if (dateQueryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
          dateQueryRes.status,
          dateQueryRes.json ?? dateQueryRes.text,
        );
      }
      expect(dateQueryRes.status).toBe(400);

      // Query without proper date (ISO8601)
      const boolQueryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          name: 'carlos',
          authorized: 'notBool',
        }),
        headers: { 'Fiware-Service': service },
      });

      if (boolQueryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data failed as expected:',
          boolQueryRes.status,
          boolQueryRes.json ?? boolQueryRes.text,
        );
      }
      expect(boolQueryRes.status).toBe(400);

      // Query with proper type coercion: Number from string
      const numberCoercionRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          minAge: 30,
          name: 'carlos',
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(numberCoercionRes.status).toBe(200);
      expect(numberCoercionRes.json).toEqual([
        { id: '3', name: 'carlos', age: '40' },
      ]);

      // Query with proper type coercion: Boolean from string
      const booleanCoercionRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          name: 'ana',
          authorized: true,
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(booleanCoercionRes.status).toBe(200);
      expect(booleanCoercionRes.json).toEqual([
        { id: '1', name: 'ana', age: '30' },
      ]);

      // Query with proper type coercion: DateTime from string (ISO8601)
      const dateTimeCoercionRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId2, {
          name: 'carlos',
          timeinstant: '2020-08-17T18:25:28.332Z',
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(dateTimeCoercionRes.status).toBe(200);
      expect(dateTimeCoercionRes.json).toEqual([
        { id: '3', name: 'carlos', age: '40' },
      ]);
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns NDJSON when Accept: application/x-ndjson', async () => {
      const queryRes = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, {
          minAge: 25,
        }),
        headers: {
          'Fiware-Service': service,
          Accept: 'application/x-ndjson',
        },
      });

      if (queryRes.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data NDJSON failed:',
          queryRes.status,
          queryRes.text,
        );
      }
      expect(queryRes.status).toBe(200);
      expect(queryRes.text).toBeDefined();

      // Parse NDJSON: split by newline and parse each line as JSON
      const lines = queryRes.text.split('\n').filter((line) => line.trim());
      expect(lines.length).toBe(2);

      const row1 = JSON.parse(lines[0]);
      const row2 = JSON.parse(lines[1]);

      expect(row1).toEqual({ id: 1, name: 'ana', age: 30 });
      expect(row2).toEqual({ id: 3, name: 'carlos', age: 40 });
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns CSV when Accept: text/csv', async () => {
      const fdaCsvId = 'fda_accept_csv';
      const daCsvId = 'da_accept_csv';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaCsvId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'accept csv fixture',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaCsvId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaCsvId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daCsvId,
          description: 'accept csv da fixture',
          query: `
            SELECT id, name, age
            WHERE age > $minAge
            ORDER BY id
          `,
          params: [{ name: 'minAge', type: 'Number', required: true }],
        },
      });

      expect(createDa.status).toBe(201);

      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaCsvId, daCsvId, {
          minAge: 25,
        }),
        headers: {
          'Fiware-Service': service,
          Accept: 'text/csv',
        },
      });

      if (res.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data Accept=text/csv failed:',
          res.status,
          res.text,
        );
      }

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toContain('text/csv');
      expect(res.headers['content-disposition']).toContain('results.csv');

      const lines = res.text
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);
      expect(lines[0]).toBe('id,name,age');
      expect(lines[1]).toBe('1,ana,30');
      expect(lines[2]).toBe('3,carlos,40');
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns XLSX when Accept requests spreadsheet mime', async () => {
      const fdaXlsId = 'fda_accept_xls';
      const daXlsId = 'da_accept_xls';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaXlsId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'accept xls fixture',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaXlsId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaXlsId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daXlsId,
          description: 'accept xls da fixture',
          query: `
            SELECT id, name, age
            WHERE age > $minAge
            ORDER BY id
          `,
          params: [{ name: 'minAge', type: 'Number', required: true }],
        },
      });

      expect(createDa.status).toBe(201);

      const res = await httpReqRaw({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaXlsId, daXlsId, {
          minAge: 25,
        }),
        headers: {
          'Fiware-Service': service,
          Accept:
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        },
      });

      if (res.status >= 400) {
        console.error(
          'GET /{visibility}/fdas/{fdaId}/das/{daId}/data Accept=xlsx failed:',
          res.status,
          res.text,
        );
      }

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toContain(
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      );
      expect(res.headers['content-disposition']).toContain('results.xlsx');

      // XLSX is a ZIP-based format and starts with PK magic bytes.
      expect(res.buffer[0]).toBe(0x50);
      expect(res.buffer[1]).toBe(0x4b);
      expect(res.buffer.length).toBeGreaterThan(100);
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data rejects outputType query param', async () => {
      const fdaJsonId = 'fda_accept_json';
      const daJsonId = 'da_accept_json';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaJsonId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'accept json fixture',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaJsonId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${fdaJsonId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: daJsonId,
          description: 'accept json da fixture',
          query: `
            SELECT id, name, age
            WHERE age > $minAge
            ORDER BY id
          `,
          params: [{ name: 'minAge', type: 'Number', required: true }],
        },
      });

      expect(createDa.status).toBe(201);

      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaJsonId, daJsonId, {
          minAge: 25,
          outputType: 'html',
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('BadRequest');
      expect(res.json.description).toBe(
        'Invalid fields in request query, check your request',
      );
    });

    test('GET /{visibility}/fdas/{fdaId}/das/{daId}/data returns 406 for unsupported Accept header', async () => {
      const res = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, daId, {
          minAge: 25,
        }),
        headers: {
          'Fiware-Service': service,
          Accept: 'video/mpg4',
        },
      });

      expect(res.status).toBe(406);
      expect(res.json.error).toBe('NotAcceptable');
      expect(res.json.description).toContain(
        'Accept header must allow application/json',
      );
    });

    test('GET /{visibility}/... returns 400 for an invalid visibility value', async () => {
      const res = await httpReq({
        method: 'GET',
        url: `${baseUrl}/shared/fdas/${fdaId}/das/${daId}/data?minAge=25`,
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('InvalidVisibility');
    });

    test('GET /{visibility}/... returns 403 when visibility does not match the FDA visibility', async () => {
      // fdaId was created with Fiware-ServicePath: /public, so querying it
      // through /private/... must be rejected with 403 VisibilityMismatch.
      const res = await httpReq({
        method: 'GET',
        url: `${baseUrl}/private/fdas/${fdaId}/das/${daId}/data?minAge=25`,
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(403);
      expect(res.json.error).toBe('VisibilityMismatch');
    });

    test('POST /plugin/cda/api/doQuery behaves as CDA compatibility layer', async () => {
      const cdaFdaId = 'fda_da_cda';
      const cdaDaId = 'fda_da_cda';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: cdaFdaId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'users dataset for CDA',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId: cdaFdaId });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas/${cdaFdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: cdaDaId,
          description: 'CDA test DA',
          query: `
          SELECT id, name, age, COUNT(*) OVER() AS __total
          WHERE age >= $minAge
          ORDER BY id
          LIMIT $pageSize OFFSET $pageStart
        `,
          params: [
            { name: 'minAge', type: 'Number', default: 0 },
            { name: 'pageSize', type: 'Number', default: 10 },
            { name: 'pageStart', type: 'Number', default: 0 },
          ],
        },
      });

      expect(createDa.status).toBe(201);

      const res = await httpFormReq({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${cdaDaId}`,
          dataAccessId: cdaDaId,
          paramminAge: '0',
          pageSize: '2',
          pageStart: '0',
        },
      });

      if (res.status >= 400) {
        console.error('CDA doQuery failed:', res.status, res.json ?? res.text);
      }

      expect(res.status).toBe(200);

      expect(res.json).toHaveProperty('metadata');
      expect(res.json).toHaveProperty('resultset');
      expect(res.json).toHaveProperty('queryInfo');

      expect(Array.isArray(res.json.metadata)).toBe(true);
      expect(res.json.metadata[0]).toHaveProperty('colIndex');
      expect(res.json.metadata[0]).toHaveProperty('colName');

      expect(Array.isArray(res.json.resultset)).toBe(true);
      expect(Array.isArray(res.json.resultset[0])).toBe(true);
      expect(res.json.resultset.length).toBe(2);

      expect(res.json.queryInfo.pageStart).toBe(0);
      expect(res.json.queryInfo.pageSize).toBe(2);
      expect(res.json.queryInfo.totalRows).toBe(3);

      const ndjsonAttempt = await httpFormReq({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: {
          'Fiware-Service': service,
          Accept: 'application/x-ndjson',
        },
        form: {
          path: `/public/${service}/verticals/sql/${cdaDaId}`,
          dataAccessId: cdaDaId,
          pageSize: '2',
          pageStart: '0',
        },
      });

      expect(ndjsonAttempt.status).toBe(200);
      expect(ndjsonAttempt.text.includes('\n')).toBe(false);
      expect(ndjsonAttempt.json).toHaveProperty('resultset');
    });

    test('POST /plugin/cda/api/doQuery rejects scope mismatch', async () => {
      const privateFdaId = 'fda_cda_scope_private';
      const privateDaId = 'da_cda_scope_private';

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/private/fdas`,
        headers: { 'Fiware-Service': service },
        body: {
          id: privateFdaId,
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'users dataset for CDA private scope',
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: privateFdaId,
        visibility: 'private',
      });

      const createDa = await httpReq({
        method: 'POST',
        url: `${baseUrl}/private/fdas/${privateFdaId}/das`,
        headers: { 'Fiware-Service': service },
        body: {
          id: privateDaId,
          description: 'CDA scope mismatch test DA',
          query: `
          SELECT id, name, age
          WHERE age >= $minAge
          ORDER BY id
        `,
          params: [{ name: 'minAge', type: 'Number', default: 0 }],
        },
      });

      expect(createDa.status).toBe(201);

      const mismatchRes = await httpFormReq({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${privateDaId}`,
          cda: privateFdaId,
          dataAccessId: privateDaId,
          paramminAge: '0',
        },
      });

      expect(mismatchRes.status).toBe(403);
      expect(mismatchRes.json.error).toBe('VisibilityMismatch');
    });

    test('POST /plugin/cda/api/doQuery supports outputType=csv', async () => {
      const res = await httpReqRaw({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${daId}`,
          cda: fdaId,
          dataAccessId: daId,
          paramminAge: '25',
          outputType: 'csv',
        },
      });

      if (res.status >= 400) {
        console.error(
          'POST /plugin/cda/api/doQuery outputType=csv failed:',
          res.status,
          res.text,
        );
      }

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toContain('text/csv');
      expect(res.headers['content-disposition']).toContain('results.csv');

      const lines = res.text
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);
      expect(lines[0]).toBe('id,name,age');
      expect(lines[1]).toBe('1,ana,30');
      expect(lines[2]).toBe('3,carlos,40');
    });

    test('POST /plugin/cda/api/doQuery supports outputType=xls', async () => {
      const res = await httpReqRaw({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${daId}`,
          cda: fdaId,
          dataAccessId: daId,
          paramminAge: '25',
          outputType: 'xls',
        },
      });

      if (res.status >= 400) {
        console.error(
          'POST /plugin/cda/api/doQuery outputType=xls failed:',
          res.status,
          res.text,
        );
      }

      expect(res.status).toBe(200);
      expect(res.headers['content-type']).toContain(
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      );
      expect(res.headers['content-disposition']).toContain('results.xlsx');
      expect(res.buffer[0]).toBe(0x50);
      expect(res.buffer[1]).toBe(0x4b);
      expect(res.buffer.length).toBeGreaterThan(100);
    });

    test('POST /plugin/cda/api/doQuery serializes dates consistently for json/csv/xls', async () => {
      const fixtureTable = 'cda_format_serialization_fixture';
      const cdaFdaId = 'fda_cda_serialization_regression';
      const cdaDaId = 'da_cda_serialization_regression';
      const expectedDates = [
        '2024-01-10T12:34:56.789Z',
        '2024-01-11T08:15:30.123Z',
      ];

      const pgClient = new Client({
        host: pgHost,
        port: pgPort,
        user: 'postgres',
        password: 'postgres',
        database: service,
      });

      await connectWithRetry(pgClient);

      try {
        await pgClient.query(`DROP TABLE IF EXISTS public.${fixtureTable}`);
        await pgClient.query(`
          CREATE TABLE public.${fixtureTable} (
            id INT PRIMARY KEY,
            observed_at TIMESTAMPTZ NOT NULL,
            total_bigint BIGINT NOT NULL,
            note TEXT
          )
        `);
        await pgClient.query(
          `
            INSERT INTO public.${fixtureTable} (id, observed_at, total_bigint, note)
            VALUES
              (1, $1::timestamptz, 42, 'alpha'),
              (2, $2::timestamptz, 84, 'with,comma')
          `,
          expectedDates,
        );

        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: cdaFdaId,
            description: 'cda serialization regression fda',
            query: `
              SELECT
                id,
                observed_at AS date,
                total_bigint AS total,
                note
              FROM public.${fixtureTable}
              ORDER BY id
            `,
          },
        });

        expect(createFda.status).toBe(202);

        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: cdaFdaId,
        });

        const createDa = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/${cdaFdaId}/das`,
          headers: { 'Fiware-Service': service },
          body: {
            id: cdaDaId,
            description: 'cda serialization regression da',
            query: `
              SELECT date, total, note
              ORDER BY date
            `,
          },
        });

        expect(createDa.status).toBe(201);

        const jsonRes = await httpFormReq({
          method: 'POST',
          url: `${baseUrl}/plugin/cda/api/doQuery`,
          headers: { 'Fiware-Service': service },
          form: {
            path: `/public/${service}/verticals/sql/${cdaDaId}`,
            cda: cdaFdaId,
            dataAccessId: cdaDaId,
            pageSize: '10',
            pageStart: '0',
          },
        });

        expect(jsonRes.status).toBe(200);
        expect(Array.isArray(jsonRes.json.metadata)).toBe(true);
        expect(Array.isArray(jsonRes.json.resultset)).toBe(true);

        const dateColIndex = jsonRes.json.metadata.findIndex(
          (col) => col.colName === 'date',
        );
        const totalColIndex = jsonRes.json.metadata.findIndex(
          (col) => col.colName === 'total',
        );

        expect(dateColIndex).toBeGreaterThanOrEqual(0);
        expect(totalColIndex).toBeGreaterThanOrEqual(0);
        expect(jsonRes.json.resultset.map((row) => row[dateColIndex])).toEqual(
          expectedDates,
        );
        expect(
          jsonRes.json.resultset.every((row) =>
            ['string', 'number'].includes(typeof row[totalColIndex]),
          ),
        ).toBe(true);

        const csvRes = await httpReqRaw({
          method: 'POST',
          url: `${baseUrl}/plugin/cda/api/doQuery`,
          headers: { 'Fiware-Service': service },
          form: {
            path: `/public/${service}/verticals/sql/${cdaDaId}`,
            cda: cdaFdaId,
            dataAccessId: cdaDaId,
            outputType: 'csv',
          },
        });

        expect(csvRes.status).toBe(200);
        expect(csvRes.headers['content-type']).toContain('text/csv');
        expect(csvRes.text).toContain(expectedDates[0]);
        expect(csvRes.text).toContain(expectedDates[1]);
        expect(csvRes.text).not.toContain('[object Object]');

        const xlsRes = await httpReqRaw({
          method: 'POST',
          url: `${baseUrl}/plugin/cda/api/doQuery`,
          headers: { 'Fiware-Service': service },
          form: {
            path: `/public/${service}/verticals/sql/${cdaDaId}`,
            cda: cdaFdaId,
            dataAccessId: cdaDaId,
            outputType: 'xls',
          },
        });

        expect(xlsRes.status).toBe(200);
        expect(xlsRes.headers['content-type']).toContain(
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        );
        expect(xlsRes.headers['content-disposition']).toContain('results.xlsx');
        expect(xlsRes.buffer[0]).toBe(0x50);
        expect(xlsRes.buffer[1]).toBe(0x4b);
        expect(xlsRes.buffer.length).toBeGreaterThan(100);
      } finally {
        await pgClient.query(`DROP TABLE IF EXISTS public.${fixtureTable}`);
        await pgClient.end();
      }
    });

    test('POST /plugin/cda/api/doQuery rejects unsupported outputType', async () => {
      const res = await httpFormReq({
        method: 'POST',
        url: `${baseUrl}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/public/${service}/verticals/sql/${daId}`,
          cda: fdaId,
          dataAccessId: daId,
          outputType: 'xml',
        },
      });

      expect(res.status).toBe(400);
      expect(res.json.error).toBe('BadRequest');
      expect(res.json.description).toContain('Invalid outputType');
    });

    test('GET /fdas/:fdaId returns expected FDA', async () => {
      const res = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: { 'Fiware-Service': service },
      });

      if (res.status >= 400) {
        console.error(
          'GET /fdas/:fdaId failed:',
          res.status,
          res.json ?? res.text,
        );
      }
      expect(res.status).toBe(200);
      expect(Object.keys(res.json).length).toBeGreaterThan(0);
      expect(res.json.id).toBeUndefined();
      expect(res.json.fdaId).toBeUndefined();
      expect(res.json.service).toBeUndefined();
      expect(res.json.visibility).toBeUndefined();
      expect(res.json.servicePath).toBeUndefined();
    });

    test('PUT /fdas/:fdaID reuploads FDA', async () => {
      const res = await httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: { 'Fiware-Service': service },
      });

      if (res.status >= 400) {
        console.error(
          'PUT /fdas/:fdaId failed:',
          res.status,
          res.json ?? res.text,
        );
      }
      expect(res.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });
    });

    test('PUT /fdas/:fdaId triggers AlreadyFetching if concurrent', async () => {
      httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId3}`,
        headers: { 'Fiware-Service': service },
      });

      const put2 = await httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId3}`,
        headers: { 'Fiware-Service': service },
      });

      expect(put2.status).toBe(409);
      expect(put2.json.error).toBe('AlreadyFetching');

      await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaId3 });
    });

    test('PUT /fdas/:fdaId throws InvalidState if FDA in unexpected status', async () => {
      const client = new MongoClient(mongoUri);
      await client.connect();
      const collection = client.db().collection('fdas');

      await collection.updateOne(
        { fdaId: fdaId3, service },
        { $set: { status: 'transforming' } },
      );

      const res = await httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId3}`,
        headers: { 'Fiware-Service': service },
      });

      expect(res.status).toBe(409);
      expect(res.json.error).toBe('InvalidState');

      await collection.updateOne(
        { fdaId: fdaId3, service },
        { $set: { status: 'completed' } },
      );
      await client.close();
    });

    test('DELETE /fdas/:fdaId removes given FDA', async () => {
      const deleteFDA = await httpReq({
        method: 'DELETE',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: { 'Fiware-Service': service },
      });

      if (deleteFDA.status >= 400) {
        console.error(
          'DELETE /fdas/:fdaId failed:',
          deleteFDA.status,
          deleteFDA.json ?? deleteFDA.text,
        );
      }
      expect(deleteFDA.status).toBe(204);

      const getFDA = await httpReq({
        method: 'GET',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: { 'Fiware-Service': service },
      });

      expect(getFDA.status).toBe(404);
    });

    test('MongoDB integration: POST /fdas + get /fdas/:fdaId', async () => {
      const postFDA = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId2,
          // query base to extract from PG to CSV
          query: 'SELECT id, name, age FROM public.users ORDER BY id',
          description: 'users dataset',
          timeColumn: 'timeinstant',
          refreshPolicy: {
            type: 'interval',
            params: {
              refreshInterval: '1 hour',
            },
          },
        },
      });

      if (postFDA.status >= 400) {
        console.error(
          'POST /fdas failed:',
          postFDA.status,
          postFDA.json ?? postFDA.text,
        );
      }
      expect(postFDA.status).toBe(202);

      const completedFDA = await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: fdaId2,
      });

      expect(completedFDA).toMatchObject({
        query:
          'SELECT timeinstant, id, name, age FROM public.users ORDER BY id',
        description: 'users dataset',
        status: 'completed',
        progress: 100,
        refreshPolicy: {
          type: 'interval',
          params: {
            refreshInterval: '1 hour',
          },
        },
      });
      expect(completedFDA.fdaId).toBeUndefined();
      expect(completedFDA.service).toBeUndefined();
      expect(completedFDA.servicePath).toBeUndefined();
      expect(completedFDA.lastFetch).toBeDefined();
      expect(typeof completedFDA.lastFetch).toBe('string');
    });
  });
}
