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

import { jest, describe, beforeAll, afterAll } from '@jest/globals';
import { GenericContainer, Wait } from 'testcontainers';
import {
  S3Client,
  CreateBucketCommand,
  HeadBucketCommand,
  ListBucketsCommand,
} from '@aws-sdk/client-s3';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { registerDatasourcesIntegrationTests } from './suites/datasources.integration.tests.js';
import { registerPlatformIntegrationTests } from './suites/platform.integration.tests.js';
import { registerSlidingWindowsIntegrationTests } from './suites/slidingWindows.integration.tests.js';
import { registerFdaCreationIntegrationTests } from './suites/fdaCreation.integration.tests.js';
import { registerDefaultDataAccessIntegrationTests } from './suites/defaultDataAccess.integration.tests.js';
import { registerDaDataQueriesIntegrationTests } from './suites/daDataQueries.integration.tests.js';
import { registerDaParamsIntegrationTests } from './suites/daParams.integration.tests.js';
import { registerFdaVariantsIntegrationTests } from './suites/fdaVariants.integration.tests.js';
import { registerDaCrudIntegrationTests } from './suites/daCrud.integration.tests.js';
import { registerFreshQueriesIntegrationTests } from './suites/freshQueries.integration.tests.js';
import { registerQueryStyleDataIntegrationTests } from './suites/queryStyleData.integration.tests.js';
import { registerVisibilityConstraintsIntegrationTests } from './suites/visibilityConstraints.integration.tests.js';
import { registerCdaCompatibilityIntegrationTests } from './suites/cdaCompatibility.integration.tests.js';
import { registerFdaLifecycleIntegrationTests } from './suites/fdaLifecycle.integration.tests.js';
import { registerMongoFdasIntegrationTests } from './suites/mongoFdas.integration.tests.js';
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
    const fdaId3 = 'fda3';
    const daId = 'da1';

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

    registerDatasourcesIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      getPgHost: () => pgHost,
      getPgPort: () => pgPort,
      httpReq,
      waitUntilFDACompleted,
    });

    registerPlatformIntegrationTests({
      getAppPort: () => appPort,
      httpReq,
    });

    registerFdaCreationIntegrationTests({
      getBaseUrl: () => baseUrl,
      getMongoUri: () => mongoUri,
      getPgHost: () => pgHost,
      getPgPort: () => pgPort,
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

    registerDaParamsIntegrationTests({
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
      httpReq,
      httpReqRaw,
      waitUntilFDACompleted,
      buildDaDataUrl,
    });

    registerQueryStyleDataIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      getPgHost: () => pgHost,
      getPgPort: () => pgPort,
      httpReq,
      httpReqRaw,
      buildDaDataUrl,
      buildFdaDataUrl,
      waitUntilFDACompleted,
    });

    registerFdaVariantsIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      httpReq,
      waitUntilFDACompleted,
      buildFdaDataUrl,
    });

    registerFdaTimeColumnIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      httpReq,
      waitUntilFDACompleted,
      buildFdaDataUrl,
    });

    registerDaCrudIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      httpReq,
      buildDaDataUrl,
      fdaId,
      daId,
    });

    registerFreshQueriesIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      httpReq,
      httpReqRaw,
      waitUntilFDACompleted,
      buildDaDataUrl,
      buildFdaDataUrl,
      fdaId,
      getPgHost: () => pgHost,
      getPgPort: () => pgPort,
    });

    registerVisibilityConstraintsIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      httpReq,
      fdaId,
      daId,
    });

    registerCdaCompatibilityIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      httpReq,
      httpFormReq,
      httpReqRaw,
      waitUntilFDACompleted,
      fdaId,
      daId,
      getPgHost: () => pgHost,
      getPgPort: () => pgPort,
    });

    registerFdaLifecycleIntegrationTests({
      getBaseUrl: () => baseUrl,
      service,
      servicePath,
      visibility,
      fdaId,
      fdaId2: 'fda2',
      fdaId3,
      httpReq,
      waitUntilFDACompleted,
      getMongoUri: () => mongoUri,
    });

    registerMongoFdasIntegrationTests({
      getBaseUrl: () => baseUrl,
      getMongoUri: () => mongoUri,
      service,
      servicePath,
      visibility,
      httpReq,
      httpReqRaw,
      waitUntilFDACompleted,
      buildDaDataUrl,
    });
  });
}
