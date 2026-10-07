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

import { describe, beforeAll, afterAll, test, expect } from '@jest/globals';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import {
  S3Client,
  ListObjectsV2Command,
  CopyObjectCommand,
} from '@aws-sdk/client-s3';
import { ensureDefaultDatasource } from '../utils/integrationTestUtils.js';

const { Client } = pg;

export function registerFdaAccoutingIntegrationTests({
  getBaseUrl,
  getMongoUri,
  getMinioUrl,
  getPgHost,
  getPgPort,
  service,
  visibility,
  httpReq,
}) {
  const servicePath = '/accounting';
  const storagePrefix = 'accounting/';
  const headers = {
    'Fiware-Service': service,
    'Fiware-ServicePath': servicePath,
  };
  const partitionedDays = 1100;

  let mongoClient;
  let quotas;
  let s3;

  function wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function listStoredObjects(prefix) {
    const objects = [];
    let continuationToken;

    do {
      const page = await s3.send(
        new ListObjectsV2Command({
          Bucket: service,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        }),
      );
      objects.push(...(page.Contents ?? []));
      continuationToken = page.IsTruncated
        ? page.NextContinuationToken
        : undefined;
    } while (continuationToken);

    return objects;
  }

  async function getFDA(fdaId) {
    const res = await httpReq({
      method: 'GET',
      url: `${getBaseUrl()}/${visibility}/fdas/${fdaId}`,
      headers,
    });
    return res.json;
  }

  async function waitForFDA(fdaId, isReady, timeout = 120_000) {
    const start = Date.now();
    let fda;

    while (Date.now() - start < timeout) {
      fda = await getFDA(fdaId);
      if (fda && isReady(fda)) {
        return fda;
      }
      await wait(300);
    }

    throw new Error(
      `Timeout waiting for FDA ${fdaId}: ${JSON.stringify(fda)?.slice(0, 500)}`,
    );
  }

  function hasFinished(fda) {
    return fda.status === 'completed' || fda.status === 'failed';
  }

  function createFDA(body) {
    return httpReq({
      method: 'POST',
      url: `${getBaseUrl()}/${visibility}/fdas`,
      headers,
      body,
    });
  }

  function getUsage() {
    return httpReq({
      method: 'GET',
      url: `${getBaseUrl()}/usage`,
      headers,
    });
  }

  describe('Resource accounting', () => {
    beforeAll(async () => {
      await ensureDefaultDatasource({
        httpReq,
        baseUrl: getBaseUrl(),
        service,
        getPgHost,
        getPgPort,
      });

      mongoClient = new MongoClient(getMongoUri());
      await mongoClient.connect();
      quotas = mongoClient.db().collection('quotas');
      await quotas.deleteMany({ service });

      s3 = new S3Client({
        endpoint: getMinioUrl(),
        region: 'us-east-1',
        credentials: { accessKeyId: 'admin', secretAccessKey: 'admin123' },
        forcePathStyle: true,
      });

      const pgClient = new Client({
        host: getPgHost(),
        port: getPgPort(),
        user: 'postgres',
        password: 'postgres',
        database: service,
      });
      await pgClient.connect();
      await pgClient.query('DROP TABLE IF EXISTS public.accounting_events');
      await pgClient.query(
        'CREATE TABLE public.accounting_events (id INT, observed_at TIMESTAMP, value INT)',
      );
      await pgClient.query(
        `INSERT INTO public.accounting_events
         SELECT g, TIMESTAMP '2021-01-01' + (g || ' days')::interval, g
         FROM generate_series(0, ${partitionedDays - 1}) g`,
      );
      await pgClient.query('DROP TABLE IF EXISTS public.accounting_recent');
      await pgClient.query(
        'CREATE TABLE public.accounting_recent (id INT, observed_at TIMESTAMP)',
      );
      await pgClient.query(
        `INSERT INTO public.accounting_recent
         VALUES (1, NOW() - INTERVAL '1 minute'), (2, NOW() - INTERVAL '2 minutes')`,
      );
      await pgClient.end();
    });

    afterAll(async () => {
      await quotas?.deleteMany({ service });
      await mongoClient?.close();
      s3?.destroy();
    });

    test('measures the storage of a small FDA and exposes it in the FDA and in /usage', async () => {
      const res = await createFDA({
        id: 'account_small',
        query: 'SELECT id, observed_at, value FROM public.accounting_events',
      });
      expect(res.status).toBe(202);

      const fda = await waitForFDA('account_small', hasFinished);
      expect(fda.status).toBe('completed');
      expect(fda.createdAt).toEqual(expect.any(String));

      const stored = await listStoredObjects(`${storagePrefix}account_small`);
      expect(stored.map(({ Key }) => Key)).toEqual([
        'accounting/account_small.parquet',
      ]);
      expect(fda.storage).toMatchObject({
        bytes: stored[0].Size,
        objects: 1,
        partitions: 0,
      });

      const usage = await getUsage();
      expect(usage.status).toBe(200);
      expect(usage.json.servicePath.used).toEqual({
        fdas: 1,
        bytes: stored[0].Size,
      });
      expect(usage.json.servicePath.available).toEqual({
        fdas: null,
        bytes: null,
      });
      expect(usage.json.service.used.fdas).toBeGreaterThanOrEqual(1);
    });

    test('counts every DA query on the FDA and on the DA', async () => {
      const daRes = await httpReq({
        method: 'POST',
        url: `${getBaseUrl()}/${visibility}/fdas/account_small/das`,
        headers,
        body: {
          id: 'account_da',
          description: 'first rows',
          query: 'SELECT id, value ORDER BY id LIMIT 5',
        },
      });
      expect(daRes.status).toBe(204);

      for (let i = 0; i < 3; i++) {
        const dataRes = await httpReq({
          method: 'GET',
          url: `${getBaseUrl()}/${visibility}/fdas/account_small/das/account_da/data`,
          headers,
        });
        expect(dataRes.status).toBe(200);
      }

      const fda = await waitForFDA(
        'account_small',
        (candidate) => candidate.access?.count >= 3,
        15_000,
      );

      expect(fda.access.count).toBe(3);
      expect(fda.access.lastAccessAt).toEqual(expect.any(String));
      expect(fda.das.account_da.access.count).toBe(3);

      const metrics = await httpReq({
        method: 'GET',
        url: `${getBaseUrl()}/metrics`,
      });
      expect(metrics.text).toContain(
        'fda_da_queries_total{fda="account_small",fiware_service="myservice",fiware_service_path="/accounting"} 3',
      );
    });

    test('measures and fully deletes a partitioned FDA with more than 1000 objects', async () => {
      const res = await createFDA({
        id: 'account_partitioned',
        query: 'SELECT id, observed_at, value FROM public.accounting_events',
        timeColumn: 'observed_at',
        objStgConf: { partition: 'day' },
      });
      expect(res.status).toBe(202);

      const fda = await waitForFDA('account_partitioned', hasFinished, 200_000);
      expect(fda.status).toBe('completed');

      const stored = await listStoredObjects(
        `${storagePrefix}account_partitioned.parquet/`,
      );
      expect(stored.length).toBe(partitionedDays);
      expect(fda.storage).toMatchObject({
        bytes: stored.reduce((total, { Size }) => total + Size, 0),
        objects: partitionedDays,
        partitions: partitionedDays,
      });

      const deleteRes = await httpReq({
        method: 'DELETE',
        url: `${getBaseUrl()}/${visibility}/fdas/account_partitioned`,
        headers,
      });
      expect(deleteRes.status).toBe(204);

      await expect(
        listStoredObjects(`${storagePrefix}account_partitioned`),
      ).resolves.toEqual([]);
      await expect(
        listStoredObjects(`tmp/${storagePrefix}account_partitioned`),
      ).resolves.toEqual([]);
    });

    test('rejects a new FDA when the servicePath reached its FDA count quota', async () => {
      await quotas.insertOne({ service, servicePath, maxFDAs: 1 });

      const usage = await getUsage();
      expect(usage.json.servicePath).toMatchObject({
        limits: { maxFDAs: 1, maxBytes: null },
        used: { fdas: 1 },
        available: { fdas: 0, bytes: null },
      });

      const res = await createFDA({
        id: 'account_rejected',
        query: 'SELECT id FROM public.accounting_events',
      });

      expect(res.status).toBe(403);
      expect(res.json.error).toBe('QuotaExceeded');
      expect(await getFDA('account_rejected')).toMatchObject({
        error: 'FDANotFound',
      });

      await quotas.deleteMany({ service, servicePath });
    });

    test('rejects a new FDA when the servicePath storage quota is full', async () => {
      await quotas.insertOne({ service, servicePath, maxBytes: 1 });

      const res = await createFDA({
        id: 'account_rejected_bytes',
        query: 'SELECT id FROM public.accounting_events',
      });

      expect(res.status).toBe(403);
      expect(res.json.error).toBe('QuotaExceeded');

      await quotas.deleteMany({ service, servicePath });
    });

    test('aborts a fetch that exceeds the per-fetch byte limit and leaves no staging CSV', async () => {
      await quotas.insertOne({ service, servicePath, maxFetchBytes: 200 });

      const res = await createFDA({
        id: 'account_too_big',
        query: 'SELECT id, observed_at, value FROM public.accounting_events',
      });
      expect(res.status).toBe(202);

      const fda = await waitForFDA('account_too_big', hasFinished);
      expect(fda.status).toBe('failed');
      expect(fda.error).toContain('200 bytes allowed per fetch');

      const stored = await listStoredObjects(`${storagePrefix}account_too_big`);
      expect(stored.filter(({ Key }) => Key.endsWith('.csv'))).toEqual([]);

      await quotas.deleteMany({ service, servicePath });
    });

    test('fails and purges an FDA whose stored size exceeds the per-FDA limit', async () => {
      await quotas.insertOne({ service, servicePath, maxBytesPerFDA: 100 });

      const res = await createFDA({
        id: 'account_over_limit',
        query: 'SELECT id, observed_at, value FROM public.accounting_events',
      });
      expect(res.status).toBe(202);

      const fda = await waitForFDA('account_over_limit', hasFinished);
      expect(fda.status).toBe('failed');
      expect(fda.error).toContain('above the 100 allowed per FDA');
      expect(fda.storage).toMatchObject({ bytes: 0, objects: 0 });
      expect(fda.lastFetch).toBeNull();
      await expect(
        listStoredObjects(`${storagePrefix}account_over_limit`),
      ).resolves.toEqual([]);

      const dataRes = await httpReq({
        method: 'GET',
        url: `${getBaseUrl()}/${visibility}/fdas/account_over_limit/das/defaultDataAccess/data`,
        headers,
      });
      expect(dataRes.status).toBe(409);

      await quotas.deleteMany({ service, servicePath });
    });

    test('clean-partition removes partitions older than the window', async () => {
      const res = await createFDA({
        id: 'account_window',
        query: 'SELECT id, observed_at FROM public.accounting_recent',
        timeColumn: 'observed_at',
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '0 0 * * *',
            fetchSize: 'day',
            windowSize: 'week',
          },
        },
        objStgConf: { partition: 'day' },
      });
      expect(res.status).toBe(202);

      const fda = await waitForFDA(
        'account_window',
        (candidate) => hasFinished(candidate) && candidate.storage?.objects > 0,
      );
      expect(fda.status).toBe('completed');

      const partitionPrefix = `${storagePrefix}account_window.parquet/`;
      const [livePartition] = await listStoredObjects(partitionPrefix);
      const expiredKey = `${partitionPrefix}year=2020/month=1/day=1/data_0.parquet`;
      await s3.send(
        new CopyObjectCommand({
          Bucket: service,
          CopySource: `${service}/${livePartition.Key}`,
          Key: expiredKey,
        }),
      );

      const refreshRes = await httpReq({
        method: 'PUT',
        url: `${getBaseUrl()}/${visibility}/fdas/account_window`,
        headers,
      });
      expect(refreshRes.status).toBeLessThan(300);

      const start = Date.now();
      let keys = [];
      while (Date.now() - start < 30_000) {
        keys = (await listStoredObjects(partitionPrefix)).map(({ Key }) => Key);
        if (!keys.includes(expiredKey)) {
          break;
        }
        await wait(500);
      }

      expect(keys).not.toContain(expiredKey);
      expect(keys).toContain(livePartition.Key);
    });
  });
}
