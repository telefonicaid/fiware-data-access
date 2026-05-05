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
import path from 'node:path';
import { registerPlatformIntegrationTests } from './suites/platform.integration.tests.js';
import { registerSlidingWindowsIntegrationTests } from './suites/slidingWindows.integration.tests.js';
import { registerFdaCreationIntegrationTests } from './suites/fdaCreation.integration.tests.js';
import { registerDefaultDataAccessIntegrationTests } from './suites/defaultDataAccess.integration.tests.js';
import { registerDaDataQueriesIntegrationTests } from './suites/daDataQueries.integration.tests.js';
import { registerDaParamsIntegrationTests } from './suites/daParams.integration.tests.js';
import {
  httpReq,
  httpFormReq,
  httpReqRaw,
  buildDaDataUrl,
  buildFdaDataUrl,
  getFreePort,
  connectWithRetry,
  waitUntilFDACompleted,
} from './utils/integrationTestUtils.js';

const { Client } = pg;

jest.setTimeout(240_000);

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

    registerPlatformIntegrationTests({
      getAppPort: () => appPort,
      httpReq,
    });

    registerFdaCreationIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      fdaId,
      fdaId3,
      httpReq,
      waitUntilFDACompleted,
      buildDaDataUrl,
    });

    registerSlidingWindowsIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      httpReq,
      waitUntilFDACompleted,
      buildDaDataUrl,
      getPgHost: () => pgHost,
      getPgPort: () => pgPort,
    });

    registerDefaultDataAccessIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      httpReq,
      waitUntilFDACompleted,
      buildDaDataUrl,
    });

    registerDaDataQueriesIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      fdaId,
      daId,
      httpReq,
      httpReqRaw,
      waitUntilFDACompleted,
      buildDaDataUrl,
    });

    registerDaParamsIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      fdaId,
      daId2,
      httpReq,
      waitUntilFDACompleted,
      buildDaDataUrl,
    });

    test('POST /fdas?defaultDataAccess=false creates FDA without default DA', async () => {
      const disabledFdaId = 'fda_default_da_disabled';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas?defaultDataAccess=false`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: disabledFdaId,
            query:
              'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
            description: 'default DA disabled test',
          },
        });

        expect(createFda.status).toBe(202);
        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId: disabledFdaId,
        });

        expect(completedFDA?.das || {}).toEqual({});
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${disabledFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with cached=false creates an only-fresh FDA without DAs', async () => {
      const onlyFreshFdaId = 'fda_only_fresh';

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: onlyFreshFdaId,
            query:
              'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
            description: 'only fresh FDA',
            cached: false,
          },
        });

        expect(createFda.status).toBe(202);

        const storedFda = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${onlyFreshFdaId}`,
          headers: { 'Fiware-Service': service },
        });

        expect(storedFda.status).toBe(200);
        expect(storedFda.json.cached).toBe(false);
        expect(storedFda.json.das || {}).toEqual({});

        const directQueryRes = await httpReq({
          method: 'GET',
          url: buildFdaDataUrl(baseUrl, servicePath, onlyFreshFdaId),
          headers: { 'Fiware-Service': service },
        });

        expect(directQueryRes.status).toBe(200);
        expect(directQueryRes.json.map((row) => row.name)).toEqual([
          'ana',
          'bob',
          'carlos',
        ]);

        const createDa = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas/${onlyFreshFdaId}/das`,
          headers: { 'Fiware-Service': service },
          body: {
            id: 'da_should_fail',
            description: 'must fail',
            query: 'SELECT id ORDER BY id',
          },
        });

        expect(createDa.status).toBe(409);
        expect(createDa.json.error).toBe('FDAOnlyFresh');
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${onlyFreshFdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
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

    test('GET /{visibility}/fdas/{fdaId}/data runs FDA query directly against PostgreSQL source', async () => {
      const daFreshId = 'da_fresh_users';
      const freshFdaId = 'fda_fresh_users_direct';

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

      const createFreshFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: freshFdaId,
          description: 'direct fresh query test fda',
          query:
            'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
          cached: false,
        },
      });

      expect(createFreshFda.status).toBe(202);

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
          url: buildFdaDataUrl(baseUrl, servicePath, freshFdaId),
          headers: { 'Fiware-Service': service },
        });

        expect(freshRes.status).toBe(200);
        expect(freshRes.json.map((x) => x.name)).toEqual([
          'ana',
          'bob',
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

    test('GET /{visibility}/fdas/{fdaId}/data rejects query params', async () => {
      const fdaFreshDefaultsId = 'fda_fresh_defaults';

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

      const freshRes = await httpReq({
        method: 'GET',
        url: `${buildFdaDataUrl(baseUrl, servicePath, fdaFreshDefaultsId)}?limit=1`,
        headers: { 'Fiware-Service': service },
      });

      expect(freshRes.status).toBe(400);
      expect(freshRes.json.error).toBe('BadRequest');
    });

    test('GET /{visibility}/fdas/{fdaId}/data returns 429 when max concurrent fresh queries is reached', async () => {
      const fdaFreshLimitId = 'fda_fresh_limit';

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
          cached: false,
        },
      });

      expect(createFda.status).toBe(202);

      const firstFreshRequest = httpReq({
        method: 'GET',
        url: buildFdaDataUrl(baseUrl, servicePath, fdaFreshLimitId),
        headers: { 'Fiware-Service': service },
      });

      await new Promise((r) => setTimeout(r, 100));

      const secondFreshRes = await httpReq({
        method: 'GET',
        url: buildFdaDataUrl(baseUrl, servicePath, fdaFreshLimitId),
        headers: { 'Fiware-Service': service },
      });

      expect(secondFreshRes.status).toBe(429);
      expect(secondFreshRes.json.error).toBe('TooManyFreshQueries');

      const firstFreshRes = await firstFreshRequest;
      expect(firstFreshRes.status).toBe(200);
    });

    test('GET /{visibility}/fdas/{fdaId}/data streams NDJSON progressively from PostgreSQL (real streaming)', async () => {
      const fdaFreshStreamId = 'fda_fresh_stream_real';

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
          cached: false,
        },
      });

      expect(createFda.status).toBe(202);

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
          buildFdaDataUrl(baseUrl, servicePath, fdaFreshStreamId),
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

    test('GET /{visibility}/fdas/{fdaId}/data serializes dates consistently across cached and fresh JSON/NDJSON/CSV', async () => {
      const fixtureTable = 'format_serialization_fixture';
      const fdaSerializationId = 'fda_serialization_regression';
      const fdaFreshSerializationId = 'fda_serialization_regression_fresh';
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

        const createFreshFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaFreshSerializationId,
            description: 'issue 137 format matrix regression fresh fda',
            query: `
              SELECT
                id,
                observed_at AS date,
                total_bigint AS total,
                note
              FROM public.${fixtureTable}
              ORDER BY id
            `,
            cached: false,
          },
        });

        expect(createFreshFda.status).toBe(202);

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
          url: buildFdaDataUrl(baseUrl, servicePath, fdaFreshSerializationId),
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
          url: buildFdaDataUrl(baseUrl, servicePath, fdaFreshSerializationId),
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
          url: buildFdaDataUrl(baseUrl, servicePath, fdaFreshSerializationId),
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
      const cdaFdaId = 'fdaID_da_cda';
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
          path: `/public/${service}/verticals/sql/${cdaFdaId}`,
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
          path: `/public/${service}/verticals/sql/${cdaFdaId}`,
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
          path: `/public/${service}/verticals/sql/${privateFdaId}`,
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
          path: `/public/${service}/verticals/sql/${fdaId}`,
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
          path: `/public/${service}/verticals/sql/${fdaId}`,
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
            path: `/public/${service}/verticals/sql/${cdaFdaId}`,
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
            path: `/public/${service}/verticals/sql/${cdaFdaId}`,
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
            path: `/public/${service}/verticals/sql/${cdaFdaId}`,
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
          path: `/public/${service}/verticals/sql/${fdaId}`,
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
