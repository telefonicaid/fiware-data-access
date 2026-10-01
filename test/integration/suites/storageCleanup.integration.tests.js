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
import {
  S3Client,
  ListObjectsV2Command,
  CopyObjectCommand,
} from '@aws-sdk/client-s3';
import { ensureDefaultDatasource } from '../utils/integrationTestUtils.js';

const { Client } = pg;

export function registerStorageCleanupIntegrationTests({
  getBaseUrl,
  getMinioUrl,
  getPgHost,
  getPgPort,
  service,
  visibility,
  httpReq,
}) {
  const servicePath = '/cleanup';
  const storagePrefix = 'cleanup/';
  const headers = {
    'Fiware-Service': service,
    'Fiware-ServicePath': servicePath,
  };
  const partitionedDays = 1100;

  let s3;

  function wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function listStoredKeys(prefix) {
    const keys = [];
    let continuationToken;

    do {
      const page = await s3.send(
        new ListObjectsV2Command({
          Bucket: service,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        }),
      );
      keys.push(...(page.Contents ?? []).map(({ Key }) => Key));
      continuationToken = page.IsTruncated
        ? page.NextContinuationToken
        : undefined;
    } while (continuationToken);

    return keys;
  }

  async function waitUntilFDAFinished(fdaId, timeout = 200_000) {
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

  describe('Object storage cleanup', () => {
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
      await pgClient.query('DROP TABLE IF EXISTS public.cleanup_events');
      await pgClient.query(
        'CREATE TABLE public.cleanup_events (id INT, observed_at TIMESTAMP)',
      );
      await pgClient.query(
        `INSERT INTO public.cleanup_events
         SELECT g, TIMESTAMP '2021-01-01' + (g || ' days')::interval
         FROM generate_series(0, ${partitionedDays - 1}) g`,
      );
      await pgClient.query('DROP TABLE IF EXISTS public.cleanup_recent');
      await pgClient.query(
        'CREATE TABLE public.cleanup_recent (id INT, observed_at TIMESTAMP)',
      );
      await pgClient.query(
        `INSERT INTO public.cleanup_recent
         VALUES (1, NOW() - INTERVAL '1 minute'), (2, NOW() - INTERVAL '2 minutes')`,
      );
      await pgClient.end();
    });

    afterAll(() => {
      s3?.destroy();
    });

    test('DELETE /fdas/:fdaId removes every object of an FDA with more than 1000 partitions', async () => {
      const res = await createFDA({
        id: 'cleanup_partitioned',
        query: 'SELECT id, observed_at FROM public.cleanup_events',
        timeColumn: 'observed_at',
        objStgConf: { partition: 'day' },
      });
      expect(res.status).toBe(202);

      const fda = await waitUntilFDAFinished('cleanup_partitioned');
      expect(fda.status).toBe('completed');
      await expect(
        listStoredKeys(`${storagePrefix}cleanup_partitioned.parquet/`),
      ).resolves.toHaveLength(partitionedDays);

      const deleteRes = await httpReq({
        method: 'DELETE',
        url: `${getBaseUrl()}/${visibility}/fdas/cleanup_partitioned`,
        headers,
      });
      expect(deleteRes.status).toBe(204);

      await expect(
        listStoredKeys(`${storagePrefix}cleanup_partitioned`),
      ).resolves.toEqual([]);
      await expect(
        listStoredKeys(`tmp/${storagePrefix}cleanup_partitioned`),
      ).resolves.toEqual([]);
    });

    test('clean-partition removes the partitions older than windowSize', async () => {
      const res = await createFDA({
        id: 'cleanup_window',
        query: 'SELECT id, observed_at FROM public.cleanup_recent',
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

      const fda = await waitUntilFDAFinished('cleanup_window');
      expect(fda.status).toBe('completed');

      const partitionPrefix = `${storagePrefix}cleanup_window.parquet/`;
      const [livePartitionKey] = await listStoredKeys(partitionPrefix);
      expect(livePartitionKey).toEqual(expect.any(String));

      const expiredKey = `${partitionPrefix}year=2020/month=1/day=1/data_0.parquet`;
      await s3.send(
        new CopyObjectCommand({
          Bucket: service,
          CopySource: `${service}/${livePartitionKey}`,
          Key: expiredKey,
        }),
      );

      const refreshRes = await httpReq({
        method: 'PUT',
        url: `${getBaseUrl()}/${visibility}/fdas/cleanup_window`,
        headers,
      });
      expect(refreshRes.status).toBeLessThan(300);

      const start = Date.now();
      let keys = [];
      while (Date.now() - start < 30_000) {
        keys = await listStoredKeys(partitionPrefix);
        if (!keys.includes(expiredKey)) {
          break;
        }
        await wait(500);
      }

      expect(keys).not.toContain(expiredKey);
      expect(keys).toContain(livePartitionKey);
    });
  });
}
