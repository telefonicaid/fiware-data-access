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

jest.setTimeout(240_000);

function httpReq({ method, url, headers, body }) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const req = http.request(
      {
        method,
        hostname: u.hostname,
        port: u.port,
        path: u.pathname + u.search,
        headers: {
          ...(headers || {}),
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
  timeout = 10000,
  interval = 300,
}) {
  const start = Date.now();

  while (Date.now() - start < timeout) {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/fdas/${encodeURIComponent(fdaId)}`,
      headers: { 'Fiware-Service': service },
    });

    if (res.status === 200 && res.json?.status === 'completed') {
      return res.json;
    }

    await new Promise((r) => setTimeout(r, interval));
  }

  throw new Error(`Timeout waiting for FDA ${fdaId} to reach completed state`);
}

describe('FDA API - integration (run app as child process)', () => {
  let minio;
  let mongo;
  let postgis;

  let minioHostPort;
  let minioUrl;
  let mongoUri;
  let pgHost;
  let pgPort;

  let appProc;
  let appPort;
  let baseUrl;

  const service = 'myservice';
  const servicePath = '/public';
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
    mongoUri = `mongodb://${mongo.getHost()}:${mongo.getMappedPort(27017)}`;

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
          age INT
        );
      `);
      await pgClient.query(`
        INSERT INTO public.users (id, name, age)
        VALUES (1,'ana',30), (2,'bob',20), (3,'carlos',40);
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

  async function startApp() {
    // Start app as child process (NOT NODE_ENV=test)
    appPort = await getFreePort();
    baseUrl = `http://127.0.0.1:${appPort}`;

    const entry = path.resolve('test/helpers/start-app.js');

    appProc = spawn(process.execPath, [entry], {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,

        // IMPORTANT: allow app.listen
        NODE_ENV: 'integration',
        FDA_NODE_ENV: 'development',

        FDA_SERVER_PORT: String(appPort),

        FDA_PG_USER: 'postgres',
        FDA_PG_PASSWORD: 'postgres',
        FDA_PG_HOST: pgHost,
        FDA_PG_PORT: String(pgPort),

        FDA_OBJSTG_USER: 'admin',
        FDA_OBJSTG_PASSWORD: 'admin123',
        FDA_OBJSTG_PROTOCOL: 'http',
        FDA_OBJSTG_ENDPOINT: minioHostPort,

        FDA_MONGO_URI: mongoUri,
      },
    });

    appProc.stdout.on('data', (d) => console.log('[APP]', d.toString().trim()));
    appProc.stderr.on('data', (d) =>
      console.error('[APP-ERR]', d.toString().trim()),
    );

    // Wait until app responds: /fdas without header -> 400 when up
    const start = Date.now();
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        const res = await httpReq({ method: 'GET', url: `${baseUrl}/fdas` });
        if (res.status === 400) {
          break;
        }
        // eslint-disable-next-line no-empty
      } catch {}
      if (Date.now() - start > 30_000) {
        throw new Error('Timeout waiting app to start');
      }
      await new Promise((r) => setTimeout(r, 200));
    }

    console.log('[TEST] App OK at', baseUrl);
  }

  async function stopApp() {
    if (appProc) {
      appProc.kill('SIGTERM');
      await new Promise((r) => setTimeout(r, 500));
      if (!appProc.killed) {
        appProc.kill('SIGKILL');
      }
    }
  }

  test('GET /health returns UP status', async () => {
    const res = await httpReq({
      method: 'GET',
      url: `http://127.0.0.1:${appPort}/health`,
    });
    expect(res.status).toBe(200);
    expect(res.json.status).toBe('UP');
  });

  test('POST /fdas creates an FDA (uploads CSV then converts to Parquet)', async () => {
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/fdas`,
      headers: { 'Fiware-Service': service },
      body: {
        id: fdaId,
        // query base to extract from PG to CSV
        query: 'SELECT id, name, age FROM public.users ORDER BY id',
        description: 'users dataset',
      },
    });

    if (res.status >= 400) {
      console.error('POST /fdas failed:', res.status, res.json ?? res.text);
    }
    expect(res.status).toBe(202);
    await waitUntilFDACompleted({ baseUrl, service, fdaId });
  });

  test('POST /fdas tries to creates an FDA without id and is detected', async () => {
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/fdas`,
      headers: { 'Fiware-Service': service },
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
  test('POST /fdas with duplicate id returns error', async () => {
    await httpReq({
      method: 'POST',
      url: `${baseUrl}/fdas`,
      headers: { 'Fiware-Service': service },
      body: {
        id: fdaId3,
        query: 'SELECT id FROM public.users',
        description: 'duplicate test',
      },
    });

    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/fdas`,
      headers: { 'Fiware-Service': service },
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

  test('GET /fdas returns list', async () => {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/fdas`,
      headers: { 'Fiware-Service': service },
    });

    if (res.status >= 400) {
      console.error('GET /fdas failed:', res.status, res.json ?? res.text);
    }
    expect(res.status).toBe(200);
    expect(Array.isArray(res.json)).toBe(true);
    expect(res.json.some((x) => x.fdaId === fdaId)).toBe(true);
  });

  test('POST /fdas/:fdaId/das + GET /query executes DuckDB against Parquet', async () => {
    // DuckDB reads parquet generated in  s3://<bucket>/<fdaID>.parquet
    const daQuery = `
      SELECT id, name, age
      WHERE age > $minAge
      ORDER BY id;
    `;

    const createDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: daId,
        description: 'age filter',
        query: daQuery,
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
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daId)}&minAge=25`,
      headers: { 'Fiware-Service': service },
    });

    if (queryRes.status >= 400) {
      console.error(
        'GET /query failed:',
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

  test('POST /fdas/:fdaId/das rejects query with FROM clause', async () => {
    const badQuery = `
      FROM read_parquet('s3://some/path')
      SELECT id
    `;

    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/fdas/${fdaId}/das`,
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
      url: `${baseUrl}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: daIdToUpdate,
        description: 'initial filter',
        query: `
          SELECT id, name, age
          WHERE age > $minAge
          ORDER BY id
        `,
      },
    });

    expect(createRes.status).toBe(201);

    // Execute with minAge=25 (should return 2 rows)
    const firstQuery = await httpReq({
      method: 'GET',
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daIdToUpdate)}&minAge=25`,
      headers: { 'Fiware-Service': service },
    });

    expect(firstQuery.status).toBe(200);
    expect(firstQuery.json.length).toBe(2);

    // Update DA (change filter logic)
    const updateRes = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/fdas/${fdaId}/das/${daIdToUpdate}`,
      headers: {
        'Content-Type': 'application/json',
        'Fiware-Service': service,
      },
      body: {
        id: 'ignored_in_put',
        description: 'updated filter',
        query: `
          SELECT id, name, age
          WHERE age > $minAge
          AND age < 35
          ORDER BY id
        `,
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
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daIdToUpdate)}&minAge=25`,
      headers: { 'Fiware-Service': service },
    });

    expect(secondQuery.status).toBe(200);
    expect(secondQuery.json).toEqual([{ id: '1', name: 'ana', age: '30' }]);
  });

  test('GET /query returns JSON array when Accept: application/json', async () => {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daId)}&minAge=25`,
      headers: { 'Fiware-Service': service, Accept: 'application/json' },
    });

    if (res.status >= 400) {
      console.error(
        'GET /query (json) failed:',
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

  test('POST /fdas/:fdaId/das + GET /query using default params', async () => {
    // DuckDB reads parquet generated in  s3://<bucket>/<fdaID>.parquet
    const daQuery = `
      SELECT id, name, age
      WHERE age > $minAge AND name = $name
      ORDER BY id;
    `;

    const createDa = await httpReq({
      method: 'POST',
      url: `${baseUrl}/fdas/${fdaId}/das`,
      headers: { 'Fiware-Service': service },
      body: {
        id: daId2,
        description: 'get user',
        query: daQuery,
        params: [
          {
            name: 'name',
            type: 'String',
            required: true,
            enum: ['ana', 'carlos'],
          },
          {
            name: 'minAge',
            type: 'Numeric',
            default: 25,
            range: [20, 50],
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

    // Query with name outside of the enum
    const enumQueryRes = await httpReq({
      method: 'GET',
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daId2)}&minAge=25&name=fakeNAme`,
      headers: { 'Fiware-Service': service },
    });

    if (enumQueryRes.status >= 400) {
      console.error(
        'GET /query failed:',
        enumQueryRes.status,
        enumQueryRes.json ?? enumQueryRes.text,
      );
    }
    expect(enumQueryRes.status).toBe(400);

    // Query with age outside of the range
    const rangeQueryRes = await httpReq({
      method: 'GET',
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daId2)}&minAge=60&name=ana`,
      headers: { 'Fiware-Service': service },
    });

    if (rangeQueryRes.status >= 400) {
      console.error(
        'GET /query failed:',
        rangeQueryRes.status,
        rangeQueryRes.json ?? rangeQueryRes.text,
      );
    }
    expect(rangeQueryRes.status).toBe(400);

    // Good query using default values
    const defaultsQueryRes = await httpReq({
      method: 'GET',
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daId2)}&name=carlos`,
      headers: { 'Fiware-Service': service },
    });

    if (defaultsQueryRes.status >= 400) {
      console.error(
        'GET /query failed:',
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
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daId2)}&minAge=25`,
      headers: { 'Fiware-Service': service },
    });

    if (requiredQueryRes.status >= 400) {
      console.error(
        'GET /query failed:',
        requiredQueryRes.status,
        requiredQueryRes.json ?? requiredQueryRes.text,
      );
    }
    expect(requiredQueryRes.status).toBe(400);

    // Query without proper type param
    const typeQueryRes = await httpReq({
      method: 'GET',
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daId2)}&minAge=text&name=carlos`,
      headers: { 'Fiware-Service': service },
    });

    if (typeQueryRes.status >= 400) {
      console.error(
        'GET /query failed:',
        typeQueryRes.status,
        typeQueryRes.json ?? typeQueryRes.text,
      );
    }
    expect(typeQueryRes.status).toBe(400);
  });

  test('GET /query returns NDJSON when Accept: application/x-ndjson', async () => {
    const queryRes = await httpReq({
      method: 'GET',
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daId)}&minAge=25`,
      headers: {
        'Fiware-Service': service,
        Accept: 'application/x-ndjson',
      },
    });

    if (queryRes.status >= 400) {
      console.error(
        'GET /query NDJSON failed:',
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

  test('GET /doQuery returns JSON array (legacy) when Accept: application/json', async () => {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/doQuery?path=/fdas/${encodeURIComponent(
        fdaId,
      )}&dataAccessId=${encodeURIComponent(daId)}&minAge=15`,
      headers: { 'Fiware-Service': service, Accept: 'application/json' },
    });

    if (res.status >= 400) {
      console.error('GET /doQuery failed:', res.status, res.json ?? res.text);
    }
    expect(res.status).toBe(200);
    expect(res.json).toEqual([
      { id: '1', name: 'ana', age: '30' },
      { id: '2', name: 'bob', age: '20' },
      { id: '3', name: 'carlos', age: '40' },
    ]);
  });

  test('GET /doQuery returns NDJSON when Accept: application/x-ndjson', async () => {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/doQuery?path=/fdas/${encodeURIComponent(
        fdaId,
      )}&dataAccessId=${encodeURIComponent(daId)}&minAge=15`,
      headers: { 'Fiware-Service': service, Accept: 'application/x-ndjson' },
    });

    if (res.status >= 400) {
      console.error('GET /doQuery NDJSON failed:', res.status, res.text);
    }
    expect(res.status).toBe(200);
    expect(res.text).toBeDefined();

    const lines = res.text.split('\n').filter((l) => l.trim());
    expect(lines.length).toBe(3);

    const a = JSON.parse(lines[0]);
    const b = JSON.parse(lines[1]);
    const c = JSON.parse(lines[2]);

    expect(a).toEqual({ id: 1, name: 'ana', age: 30 });
    expect(b).toEqual({ id: 2, name: 'bob', age: 20 });
    expect(c).toEqual({ id: 3, name: 'carlos', age: 40 });
  });

  test('GET /query works correctly after app restart', async () => {
    await stopApp();
    await startApp();

    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/query?fdaId=${encodeURIComponent(
        fdaId,
      )}&daId=${encodeURIComponent(daId)}&minAge=25`,
      headers: { 'Fiware-Service': service, Accept: 'application/json' },
    });

    if (res.status >= 400) {
      console.error(
        'GET /query (json) failed:',
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

  test('GET /fdas/:fdaId returns expected FDA', async () => {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/fdas/${fdaId}`,
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
    expect(res.json.fdaId === fdaId).toBe(true);
  });

  test('PUT /fdas/:fdaID reuploads FDA', async () => {
    const res = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/fdas/${fdaId}`,
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
      url: `${baseUrl}/fdas/${fdaId3}`,
      headers: { 'Fiware-Service': service },
    });

    const put2 = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/fdas/${fdaId3}`,
      headers: { 'Fiware-Service': service },
    });

    expect(put2.status).toBe(409);
    expect(put2.json.error).toBe('AlreadyFetching');

    await waitUntilFDACompleted({ baseUrl, service, fdaId: fdaId3 });
  });

  test('PUT /fdas/:fdaId throws InvalidState if FDA in unexpected status', async () => {
    const client = new MongoClient(mongoUri);
    await client.connect();
    const collection = client.db('fiware-data-access').collection('fdas');

    await collection.updateOne(
      { fdaId: fdaId3, service },
      { $set: { status: 'transforming' } },
    );

    const res = await httpReq({
      method: 'PUT',
      url: `${baseUrl}/fdas/${fdaId3}`,
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
      url: `${baseUrl}/fdas/${fdaId}`,
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
      url: `${baseUrl}/fdas/${fdaId}`,
      headers: { 'Fiware-Service': service },
    });

    expect(getFDA.status).toBe(404);
  });

  test('MongoDB integration: POST /fdas + get /fdas/:fdaId', async () => {
    const postFDA = await httpReq({
      method: 'POST',
      url: `${baseUrl}/fdas`,
      headers: { 'Fiware-Service': service, 'Fiware-ServicePath': servicePath },
      body: {
        id: fdaId2,
        // query base to extract from PG to CSV
        query: 'SELECT id, name, age FROM public.users ORDER BY id',
        description: 'users dataset',
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
      fdaId: fdaId2,
      query: 'SELECT id, name, age FROM public.users ORDER BY id',
      description: 'users dataset',
      service,
      servicePath,
      status: 'completed',
      progress: 100,
    });
    expect(completedFDA.lastFetch).toBeDefined();
    expect(typeof completedFDA.lastFetch).toBe('string');
  });
});
