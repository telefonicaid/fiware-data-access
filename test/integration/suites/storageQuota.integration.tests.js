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

import { describe, beforeAll, afterAll, test, expect } from '@jest/globals';
import pg from 'pg';
import { GenericContainer, Wait } from 'testcontainers';
import {
  S3Client,
  CreateBucketCommand,
  ListObjectsV2Command,
  HeadObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';

const { Client } = pg;

export function registerStorageQuotaIntegrationTests({
  getBaseUrl,
  getMinioUrl,
  getPgHost,
  getPgPort,
  service: databaseName,
  visibility,
  httpReq,
}) {
  const quotaService = 'quotaservice';
  const servicePath = '/quota';
  const storagePrefix = 'quota/';
  const goodRows = 20;
  const oversizedRows = 40_000;
  const headers = {
    'Fiware-Service': quotaService,
    'Fiware-ServicePath': servicePath,
  };

  let s3;
  let mcContainer;

  function wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function listStoredKeys(prefix) {
    const page = await s3.send(
      new ListObjectsV2Command({ Bucket: quotaService, Prefix: prefix }),
    );
    return (page.Contents ?? []).map(({ Key }) => Key);
  }

  async function snapshotStoredObjects(prefix) {
    const page = await s3.send(
      new ListObjectsV2Command({ Bucket: quotaService, Prefix: prefix }),
    );
    return Object.fromEntries(
      (page.Contents ?? []).map(({ Key, ETag }) => [Key, ETag]),
    );
  }

  async function waitUntilFDAFinished(fdaId, timeout = 120_000) {
    const start = Date.now();
    let fda;

    while (Date.now() - start < timeout) {
      const res = await httpReq({
        method: 'GET',
        url: `${getBaseUrl()}/${visibility}/fdas/${fdaId}`,
        headers,
      });
      fda = res.json;
      if (fda?.status === 'completed' || fda?.status === 'failed') {
        return fda;
      }
      await wait(300);
    }

    throw new Error(
      `Timeout waiting for FDA ${fdaId}: ${JSON.stringify(fda)?.slice(0, 500)}`,
    );
  }

  function createFDA(body) {
    return httpReq({
      method: 'POST',
      url: `${getBaseUrl()}/${visibility}/fdas`,
      headers,
      body,
    });
  }

  function refreshFDA(fdaId) {
    return httpReq({
      method: 'PUT',
      url: `${getBaseUrl()}/${visibility}/fdas/${fdaId}`,
      headers,
    });
  }

  function readDefaultDataAccess(fdaId) {
    return httpReq({
      method: 'GET',
      url: `${getBaseUrl()}/${visibility}/fdas/${fdaId}/das/defaultDataAccess/data`,
      headers,
    });
  }

  async function versionsOf(fdaId) {
    const res = await readDefaultDataAccess(fdaId);
    expect(res.status).toBe(200);
    return [...new Set(res.json.map(({ version }) => version))].sort();
  }

  async function setBucketQuota(size) {
    mcContainer = await new GenericContainer('pgsty/mc:latest')
      .withNetworkMode('host')
      .withEntrypoint(['/bin/sh', '-c'])
      .withCommand([
        `mc alias set local ${getMinioUrl()} admin admin123 && mc quota set local/${quotaService} --size ${size} && echo QUOTA_READY && sleep 3600`,
      ])
      .withWaitStrategy(Wait.forLogMessage(/QUOTA_READY/))
      .start();
  }

  describe('Bucket quota exceeded', () => {
    beforeAll(async () => {
      const datasourceRes = await httpReq({
        method: 'POST',
        url: `${getBaseUrl()}/datasources`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': quotaService,
        },
        body: {
          datasourceId: 'default',
          type: 'postgres',
          config: {
            username: 'postgres',
            password: 'postgres',
            host: getPgHost(),
            port: getPgPort(),
            database: databaseName,
          },
        },
      });
      expect([204, 409]).toContain(datasourceRes.status);

      s3 = new S3Client({
        endpoint: getMinioUrl(),
        region: 'us-east-1',
        credentials: { accessKeyId: 'admin', secretAccessKey: 'admin123' },
        forcePathStyle: true,
      });
      await s3.send(new CreateBucketCommand({ Bucket: quotaService }));
      await setBucketQuota('1MiB');

      const pgClient = new Client({
        host: getPgHost(),
        port: getPgPort(),
        user: 'postgres',
        password: 'postgres',
        database: databaseName,
      });
      await pgClient.connect();
      await pgClient.query('DROP TABLE IF EXISTS public.quota_events');
      await pgClient.query(
        'CREATE TABLE public.quota_events (id INT, payload TEXT)',
      );
      await pgClient.query(
        `INSERT INTO public.quota_events
         SELECT g, md5(g::text) FROM generate_series(1, ${goodRows}) g`,
      );
      await pgClient.query('DROP TABLE IF EXISTS public.quota_mixed');
      await pgClient.query(
        'CREATE TABLE public.quota_mixed (id INT, observed_at TIMESTAMP, version TEXT)',
      );
      await pgClient.query(
        `INSERT INTO public.quota_mixed
         SELECT g, TIMESTAMP '2026-01-01' + (g % 3 || ' days')::interval, 'v1'
         FROM generate_series(1, 9) g`,
      );
      await pgClient.query('DROP TABLE IF EXISTS public.quota_partitioned');
      await pgClient.query(
        'CREATE TABLE public.quota_partitioned (id INT, observed_at TIMESTAMP, payload TEXT)',
      );
      await pgClient.query(
        `INSERT INTO public.quota_partitioned
         SELECT g, TIMESTAMP '2026-01-01' + (g % 3 || ' days')::interval, md5(g::text)
         FROM generate_series(1, ${goodRows}) g`,
      );
      await pgClient.end();
    }, 120_000);

    afterAll(async () => {
      s3?.destroy();
      await mcContainer?.stop();
    });

    test('a refresh that exceeds the bucket quota fails explicitly, leaves no staging objects and keeps the last good data', async () => {
      const res = await createFDA({
        id: 'quota_refresh',
        query: 'SELECT id, payload FROM public.quota_events',
      });
      expect(res.status).toBe(202);

      const created = await waitUntilFDAFinished('quota_refresh');
      expect(created.status).toBe('completed');

      const parquetKey = `${storagePrefix}quota_refresh.parquet`;
      const before = await s3.send(
        new HeadObjectCommand({ Bucket: quotaService, Key: parquetKey }),
      );
      const dataBefore = await readDefaultDataAccess('quota_refresh');
      expect(dataBefore.status).toBe(200);
      expect(dataBefore.json).toHaveLength(goodRows);

      const pgClient = new Client({
        host: getPgHost(),
        port: getPgPort(),
        user: 'postgres',
        password: 'postgres',
        database: databaseName,
      });
      await pgClient.connect();
      await pgClient.query(
        `INSERT INTO public.quota_events
         SELECT g, repeat(md5(g::text), 6) FROM generate_series(${goodRows + 1}, ${oversizedRows}) g`,
      );
      await pgClient.end();

      const refreshRes = await refreshFDA('quota_refresh');
      expect(refreshRes.status).toBeLessThan(300);

      const failed = await waitUntilFDAFinished('quota_refresh');
      expect(failed.status).toBe('failed');
      expect(failed.error).toContain('Storage quota exceeded');
      expect(failed.error).toContain(quotaService);

      await expect(listStoredKeys(storagePrefix)).resolves.toEqual([
        parquetKey,
      ]);
      await expect(listStoredKeys('tmp/')).resolves.toEqual([]);

      const after = await s3.send(
        new HeadObjectCommand({ Bucket: quotaService, Key: parquetKey }),
      );
      expect(after.ETag).toBe(before.ETag);

      const dataAfter = await readDefaultDataAccess('quota_refresh');
      expect(dataAfter.status).toBe(200);
      expect(dataAfter.json).toHaveLength(goodRows);
    }, 240_000);

    test('a partitioned refresh that exceeds the bucket quota fails explicitly and leaves the published partitions untouched', async () => {
      const res = await createFDA({
        id: 'quota_partitioned',
        query: 'SELECT id, observed_at, payload FROM public.quota_partitioned',
        timeColumn: 'observed_at',
        objStgConf: { partition: 'day' },
      });
      expect(res.status).toBe(202);

      const created = await waitUntilFDAFinished('quota_partitioned');
      expect(created.status).toBe('completed');

      const partitionPrefix = `${storagePrefix}quota_partitioned.parquet/`;
      const before = await snapshotStoredObjects(partitionPrefix);
      expect(Object.keys(before).length).toBeGreaterThanOrEqual(3);

      const pgClient = new Client({
        host: getPgHost(),
        port: getPgPort(),
        user: 'postgres',
        password: 'postgres',
        database: databaseName,
      });
      await pgClient.connect();
      await pgClient.query(
        `INSERT INTO public.quota_partitioned
         SELECT g, TIMESTAMP '2026-01-05', repeat(md5(g::text), 6)
         FROM generate_series(${goodRows + 1}, ${oversizedRows}) g`,
      );
      await pgClient.end();

      const refreshRes = await refreshFDA('quota_partitioned');
      expect(refreshRes.status).toBeLessThan(300);

      const failed = await waitUntilFDAFinished('quota_partitioned');
      expect(failed.status).toBe('failed');
      expect(failed.error).toContain('Storage quota exceeded');

      await expect(snapshotStoredObjects(partitionPrefix)).resolves.toEqual(
        before,
      );
      await expect(listStoredKeys('tmp/')).resolves.toEqual([]);
      await expect(
        listStoredKeys(`${storagePrefix}quota_partitioned.csv`),
      ).resolves.toEqual([]);
    }, 240_000);

    test('the next refresh repairs a partitioned FDA left with partitions of two different fetches', async () => {
      const res = await createFDA({
        id: 'quota_mixed',
        query: 'SELECT id, observed_at, version FROM public.quota_mixed',
        timeColumn: 'observed_at',
        objStgConf: { partition: 'day' },
      });
      expect(res.status).toBe(202);
      expect((await waitUntilFDAFinished('quota_mixed')).status).toBe(
        'completed',
      );

      const firstDayKey = `${storagePrefix}quota_mixed.parquet/year=2026/month=1/day=1/data_0.parquet`;
      const firstDayV1 = await s3
        .send(new GetObjectCommand({ Bucket: quotaService, Key: firstDayKey }))
        .then(({ Body }) => Body.transformToByteArray());

      const pgClient = new Client({
        host: getPgHost(),
        port: getPgPort(),
        user: 'postgres',
        password: 'postgres',
        database: databaseName,
      });
      await pgClient.connect();
      await pgClient.query("UPDATE public.quota_mixed SET version = 'v2'");
      await pgClient.end();

      await refreshFDA('quota_mixed');
      expect((await waitUntilFDAFinished('quota_mixed')).status).toBe(
        'completed',
      );
      await expect(versionsOf('quota_mixed')).resolves.toEqual(['v2']);

      await s3.send(
        new PutObjectCommand({
          Bucket: quotaService,
          Key: firstDayKey,
          Body: firstDayV1,
        }),
      );
      await expect(versionsOf('quota_mixed')).resolves.toEqual(['v1', 'v2']);

      await refreshFDA('quota_mixed');
      expect((await waitUntilFDAFinished('quota_mixed')).status).toBe(
        'completed',
      );
      await expect(versionsOf('quota_mixed')).resolves.toEqual(['v2']);
    }, 240_000);
  });
}
