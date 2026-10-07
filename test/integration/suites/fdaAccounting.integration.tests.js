// Copyright 2025 Telef�nica Soluciones de Inform�tica y Comunicaciones de Espa�a, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by Telef�nica Soluciones
// de Inform�tica y Comunicaciones de Espa�a, S.A.U (hereinafter TSOL) and is protected
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
import {
  S3Client,
  ListObjectsV2Command,
  CopyObjectCommand,
} from '@aws-sdk/client-s3';
import { ensureDefaultDatasource } from '../utils/integrationTestUtils.js';

const { Client } = pg;

export function registerFdaAccoutingIntegrationTests({
  getBaseUrl,
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

  async function getFDA(fdaId, scopedHeaders = headers) {
    const res = await httpReq({
      method: 'GET',
      url: `${getBaseUrl()}/${visibility}/fdas/${fdaId}`,
      headers: scopedHeaders,
    });
    return res.json;
  }

  async function waitForFDA(
    fdaId,
    isReady,
    timeout = 120_000,
    scopedHeaders = headers,
  ) {
    const start = Date.now();
    let fda;

    while (Date.now() - start < timeout) {
      fda = await getFDA(fdaId, scopedHeaders);
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

  function createFDA(body, scopedHeaders = headers) {
    return httpReq({
      method: 'POST',
      url: `${getBaseUrl()}/${visibility}/fdas`,
      headers: scopedHeaders,
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
      for (const fdaId of [
        'account_small',
        'account_cda',
        'account_partitioned',
        'account_window',
      ]) {
        const scopedHeaders =
          fdaId === 'account_cda'
            ? { 'Fiware-Service': service, 'Fiware-ServicePath': '/public' }
            : headers;
        await httpReq({
          method: 'DELETE',
          url: `${getBaseUrl()}/${visibility}/fdas/${fdaId}`,
          headers: scopedHeaders,
        });
      }
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
      expect(usage.json.servicePath).toMatchObject({
        fdas: 1,
        bytes: stored[0].Size,
        objects: 1,
        partitions: 0,
      });
      expect(usage.json.service.fdas).toBeGreaterThanOrEqual(1);
      expect(fda.lastRefresh.durationMs).toBeGreaterThanOrEqual(0);
      expect(fda.lastRefresh.bytesFetched).toBeGreaterThan(0);
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
          headers: {
            ...headers,
            Accept: i === 2 ? 'text/csv' : 'application/json',
          },
        });
        expect(dataRes.status).toBe(200);
      }

      const fda = await waitForFDA(
        'account_small',
        (candidate) => candidate.access?.count >= 3,
        15_000,
      );

      expect(fda.access.count).toBe(3);
      expect(fda.access.sinceLastFetch).toBe(3);
      expect(fda.access.lastAccessAt).toEqual(expect.any(String));
      expect(fda.das.account_da.access.count).toBe(3);
      expect(fda.das.account_da.access.sinceLastFetch).toBe(3);

      const storageMetric = `fda_usage_storage_bytes{fiware_service="${service}",fiware_service_path="/accounting"} ${fda.storage.bytes}`;
      const startedAt = Date.now();
      let metrics;
      do {
        metrics = await httpReq({
          method: 'GET',
          url: `${getBaseUrl()}/metrics`,
        });
        if (metrics.text.includes(storageMetric)) {
          break;
        }
        await wait(300);
      } while (Date.now() - startedAt < 15000);
      expect(metrics.text).toContain(
        'fda_da_queries_total{fda="account_small",fiware_service="myservice",fiware_service_path="/accounting"} 3',
      );
      expect(metrics.text).toContain(storageMetric);
    });

    test('resets queries since refresh while preserving FDA and DA lifetime counts', async () => {
      const previous = await getFDA('account_small');
      const res = await httpReq({
        method: 'PUT',
        url: `${getBaseUrl()}/${visibility}/fdas/account_small`,
        headers,
      });
      expect(res.status).toBeLessThan(300);
      const fda = await waitForFDA(
        'account_small',
        (candidate) =>
          candidate.status === 'completed' &&
          candidate.lastFetch !== previous.lastFetch,
      );
      expect(fda.access).toMatchObject({ count: 3, sinceLastFetch: 0 });
      expect(fda.das.account_da.access).toMatchObject({
        count: 3,
        sinceLastFetch: 0,
      });
      expect(fda.lastRefresh.bytesFetched).toBeGreaterThan(0);
    });

    test('counts cached CDA reads through the shared query tracker', async () => {
      const cdaHeaders = {
        'Fiware-Service': service,
        'Fiware-ServicePath': '/public',
      };
      const res = await createFDA(
        {
          id: 'account_cda',
          query: 'SELECT id, value FROM public.accounting_events',
        },
        cdaHeaders,
      );
      expect(res.status).toBe(202);
      const created = await waitForFDA(
        'account_cda',
        hasFinished,
        120000,
        cdaHeaders,
      );
      expect(created.status).toBe('completed');
      const url = new URL(`${getBaseUrl()}/plugin/cda/api/doQuery`);
      url.searchParams.set(
        'path',
        `/public/${service}/verticals/sql/account_cda`,
      );
      url.searchParams.set('dataAccessId', 'defaultDataAccess');
      const data = await httpReq({ method: 'GET', url: url.toString() });
      expect(data.status).toBe(200);
      const fda = await waitForFDA(
        'account_cda',
        (candidate) => candidate.access?.count === 1,
        15000,
        cdaHeaders,
      );
      expect(fda.access.sinceLastFetch).toBe(1);
      expect(fda.das.defaultDataAccess.access).toMatchObject({
        count: 1,
        sinceLastFetch: 1,
      });
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
      const measured = await waitForFDA(
        'account_window',
        (candidate) =>
          candidate.storage?.measuredAt !== fda.storage.measuredAt &&
          candidate.storage?.objects === keys.length,
      );
      const stored = await listStoredObjects(partitionPrefix);
      expect(measured.storage.bytes).toBe(
        stored.reduce((total, { Size }) => total + Size, 0),
      );
    });
  });
}
