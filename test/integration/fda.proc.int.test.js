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
        timeout: 20_000,
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
      }
    );
    req.on('timeout', () => req.destroy(new Error('timeout')));
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function getFreePort() {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const port = srv.address().port;
      srv.close(() => resolve(port));
    });
  });
}

async function connectWithRetry(client, attempts = 20, delayMs = 500) {
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

describe('FDA API - integration (run app as child process)', () => {
  let minio, mongo, postgis;
  let minioHostPort, minioUrl, mongoUri, pgHost, pgPort;

  let appProc;
  let appPort;
  let baseUrl;

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
        POSTGRES_DB: 'postgres',
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
      const bucket = 'myservice';
      try {
        await s3.send(new HeadBucketCommand({ Bucket: bucket }));
      } catch {
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
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
        database: 'postgres',
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
      console.error('[APP-ERR]', d.toString().trim())
    );

    // Wait until app responds (we can hit /fdas without header -> 400)
    const start = Date.now();
    while (true) {
      try {
        const res = await httpReq({ method: 'GET', url: `${baseUrl}/fdas` });
        if (res.status === 400) break; // app is up
      } catch {}
      if (Date.now() - start > 30_000)
        throw new Error('Timeout waiting app to start');
      await new Promise((r) => setTimeout(r, 200));
    }

    console.log('[TEST] App OK at', baseUrl);
  });

  afterAll(async () => {
    if (appProc) {
      appProc.kill('SIGTERM');
      await new Promise((r) => setTimeout(r, 500));
      if (!appProc.killed) appProc.kill('SIGKILL');
    }
    await Promise.allSettled([minio?.stop(), mongo?.stop(), postgis?.stop()]);
  });

  test('POST /fdas creates an FDA', async () => {
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/fdas`,
      headers: { 'Fiware-Service': 'myservice' },
      body: {
        id: 'fda1',
        database: 'postgres',
        query: 'SELECT * FROM public.users',
        path: '/datasets/users',
        description: 'users dataset',
      },
    });

    if (res.status >= 400)
      console.error('POST /fdas failed:', res.status, res.json ?? res.text);
    expect(res.status).toBe(201);
  });

  test('GET /fdas returns list', async () => {
    const res = await httpReq({
      method: 'GET',
      url: `${baseUrl}/fdas`,
      headers: { 'Fiware-Service': 'myservice' },
    });

    if (res.status >= 400)
      console.error('GET /fdas failed:', res.status, res.json ?? res.text);
    expect(res.status).toBe(200);
    expect(Array.isArray(res.json)).toBe(true);
    expect(res.json.some((x) => x.fdaId === 'fda1')).toBe(true);
  });
});
