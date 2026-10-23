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

import { describe, test, expect, beforeAll } from '@jest/globals';
import { MongoClient } from 'mongodb';
import { ensureDefaultDatasource } from '../utils/integrationTestUtils.js';

export function registerFdaUpdateIntegrationTests({
  getBaseUrl,
  getMongoUri,
  getPgHost,
  getPgPort,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
  buildDaDataUrl,
}) {
  const headers = {
    'Fiware-Service': service,
    'Fiware-ServicePath': servicePath,
  };
  const usersQuery =
    'SELECT id, name, age, timeinstant, authorized FROM public.users';

  const fdaUrl = (fdaId) => `${getBaseUrl()}/${visibility}/fdas/${fdaId}`;

  async function createFda(fdaId, extraBody = {}) {
    const res = await httpReq({
      method: 'POST',
      url: `${getBaseUrl()}/${visibility}/fdas`,
      headers,
      body: { id: fdaId, query: usersQuery, ...extraBody },
    });
    expect(res.status).toBe(202);
    return waitUntilCompleted(fdaId);
  }

  async function waitUntilCompleted(fdaId) {
    const fda = await waitUntilFDACompleted({
      baseUrl: getBaseUrl(),
      service,
      fdaId,
    });
    expect(fda.status).toBe('completed');
    return fda;
  }

  function putFda(fdaId, body) {
    return httpReq({ method: 'PUT', url: fdaUrl(fdaId), headers, body });
  }

  async function putFdaAndWait(fdaId, body) {
    const res = await putFda(fdaId, body);
    if (res.status >= 400) {
      console.error(
        'PUT /fdas/:fdaId failed:',
        res.status,
        res.json ?? res.text,
      );
    }
    expect(res.status).toBe(202);
    return waitUntilCompleted(fdaId);
  }

  async function getFda(fdaId) {
    const res = await httpReq({ method: 'GET', url: fdaUrl(fdaId), headers });
    expect(res.status).toBe(200);
    return res.json;
  }

  async function deleteFda(fdaId) {
    await httpReq({ method: 'DELETE', url: fdaUrl(fdaId), headers });
  }

  function readDefaultDa(fdaId) {
    return httpReq({
      method: 'GET',
      url: buildDaDataUrl(
        getBaseUrl(),
        servicePath,
        fdaId,
        'defaultDataAccess',
      ),
      headers: { 'Fiware-Service': service },
    });
  }

  async function countDefaultDaRows(fdaId) {
    const res = await readDefaultDa(fdaId);
    expect(res.status).toBe(200);
    return res.json.length;
  }

  async function withAgendaJobs(fn) {
    const client = new MongoClient(getMongoUri());
    await client.connect();
    try {
      return await fn(client.db().collection('agendaJobs'));
    } finally {
      await client.close();
    }
  }

  function findRecurringJob(fdaId) {
    return withAgendaJobs((agendaJobs) =>
      agendaJobs.findOne({
        name: 'refresh-fda-recurring',
        'data.fdaId': fdaId,
      }),
    );
  }

  describe('PUT /fdas/:fdaId with a body', () => {
    beforeAll(async () => {
      await ensureDefaultDatasource({
        httpReq,
        baseUrl: getBaseUrl(),
        service,
        getPgHost,
        getPgPort,
      });
    });

    test('without a body or with an empty body only regenerates the FDA', async () => {
      const fdaId = 'fda_put_no_body';

      try {
        const created = await createFda(fdaId, { timeColumn: 'timeinstant' });

        for (const body of [undefined, {}]) {
          const before = await getFda(fdaId);
          const after = await putFdaAndWait(fdaId, body);

          expect(Date.parse(after.lastFetch)).toBeGreaterThanOrEqual(
            Date.parse(before.lastFetch),
          );
          expect(after.description).toBe(created.description);
          expect(after.timeColumn).toBe('timeinstant');
          expect(after.refreshPolicy).toEqual(created.refreshPolicy);
          expect(after.objStgConf).toEqual(created.objStgConf);
        }
      } finally {
        await deleteFda(fdaId);
      }
    });

    test('updates the description, validationMode and timeColumn without scheduling refresh jobs', async () => {
      const fdaId = 'fda_put_simple_fields';

      try {
        const created = await createFda(fdaId, { description: 'original' });
        expect(created.timeColumn ?? null).toBeNull();

        const updated = await putFdaAndWait(fdaId, {
          description: 'updated',
          validationMode: 'unchecked',
          timeColumn: 'timeinstant',
        });

        expect(updated).toMatchObject({
          description: 'updated',
          validationMode: 'unchecked',
          timeColumn: 'timeinstant',
        });
        expect(await findRecurringJob(fdaId)).toBeNull();
      } finally {
        await deleteFda(fdaId);
      }
    });

    test('recreates the recurring jobs when the refreshPolicy changes', async () => {
      const fdaId = 'fda_put_refresh_policy';
      const refreshPolicy = {
        type: 'interval',
        params: { refreshInterval: '1 hour' },
      };

      try {
        await createFda(fdaId);
        expect(await findRecurringJob(fdaId)).toBeNull();

        const updated = await putFdaAndWait(fdaId, { refreshPolicy });
        expect(updated.refreshPolicy).toEqual(refreshPolicy);
        expect((await findRecurringJob(fdaId)).data.refreshPolicy).toEqual(
          refreshPolicy,
        );

        const disabled = await putFdaAndWait(fdaId, {
          refreshPolicy: { type: 'none' },
        });
        expect(disabled.refreshPolicy).toEqual({ type: 'none' });
        expect(await findRecurringJob(fdaId)).toBeNull();
      } finally {
        await deleteFda(fdaId);
      }
    });

    test('rebuilds the stored data when the partition changes and merges compression', async () => {
      const fdaId = 'fda_put_partition';

      try {
        await createFda(fdaId, { timeColumn: 'timeinstant' });
        const rowsBefore = await countDefaultDaRows(fdaId);
        expect(rowsBefore).toBeGreaterThan(0);

        const partitioned = await putFdaAndWait(fdaId, {
          objStgConf: { partition: 'month' },
        });
        expect(partitioned.objStgConf).toEqual({ partition: 'month' });
        expect(await countDefaultDaRows(fdaId)).toBe(rowsBefore);

        const compressed = await putFdaAndWait(fdaId, {
          objStgConf: { compression: true },
        });
        expect(compressed.objStgConf).toEqual({
          partition: 'month',
          compression: true,
        });
        expect(await countDefaultDaRows(fdaId)).toBe(rowsBefore);

        const unpartitioned = await putFdaAndWait(fdaId, {
          objStgConf: { partition: 'none' },
        });
        expect(unpartitioned.objStgConf).toEqual({ compression: true });
        expect(await countDefaultDaRows(fdaId)).toBe(rowsBefore);
      } finally {
        await deleteFda(fdaId);
      }
    });

    test('rejected updates leave the FDA untouched', async () => {
      const fdaId = 'fda_put_rejected';

      try {
        const created = await createFda(fdaId, {
          timeColumn: 'timeinstant',
          refreshPolicy: {
            type: 'interval',
            params: { refreshInterval: '1 hour' },
          },
        });

        const notUpdatable = await putFda(fdaId, {
          query: 'SELECT id FROM public.users',
        });
        expect(notUpdatable.status).toBe(400);
        expect(notUpdatable.json.error).toBe('BadRequest');

        // The stored interval policy has no fetchSize to match the partition
        const invalid = await putFda(fdaId, {
          objStgConf: { partition: 'month' },
        });
        expect(invalid.status).toBe(400);
        expect(invalid.json.error).toBe('InvalidUpdateBody');
        expect(invalid.json.description).toContain(
          'must be equal to partition size',
        );

        expect(await getFda(fdaId)).toMatchObject({
          query: created.query,
          objStgConf: created.objStgConf,
          refreshPolicy: created.refreshPolicy,
          status: 'completed',
        });
      } finally {
        await deleteFda(fdaId);
      }
    });

    describe('download restrictions', () => {
      test('raising maxDownloads re-enables an exhausted FDA without resetting the downloads used', async () => {
        const fdaId = 'fda_put_download_limit';
        const downloadFda = () => readDefaultDa(fdaId);

        try {
          await createFda(fdaId, { maxDownloads: 1 });

          expect((await downloadFda()).status).toBe(200);
          const exhaustedRes = await downloadFda();
          expect(exhaustedRes.status).toBe(410);
          expect(exhaustedRes.json.error).toBe('DownloadLimitReached');

          await putFdaAndWait(fdaId, { maxDownloads: 3 });

          expect(await getFda(fdaId)).toMatchObject({
            maxDownloads: 3,
            downloadsUsed: 1,
            remainingDownloads: 2,
            downloadStatus: 'available',
          });
          expect((await downloadFda()).status).toBe(200);
        } finally {
          await deleteFda(fdaId);
        }
      });

      test('updates and removes the download limits independently', async () => {
        const fdaId = 'fda_put_download_policy';

        try {
          await createFda(fdaId, {
            maxDownloads: 5,
            expirationDate: '2100-01-01T00:00:00Z',
          });

          const beforePut = Date.now();
          await putFdaAndWait(fdaId, { validityPeriod: '7 days' });

          const withValidity = await getFda(fdaId);
          const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
          expect(withValidity.maxDownloads).toBe(5);
          expect(
            Date.parse(withValidity.expirationDate),
          ).toBeGreaterThanOrEqual(beforePut + sevenDaysMs);
          expect(Date.parse(withValidity.expirationDate)).toBeLessThanOrEqual(
            Date.now() + sevenDaysMs,
          );

          await putFdaAndWait(fdaId, { maxDownloads: null });

          const withoutLimit = await getFda(fdaId);
          expect(withoutLimit.maxDownloads).toBeNull();
          expect(withoutLimit.remainingDownloads).toBeNull();
          expect(withoutLimit.expirationDate).toBe(withValidity.expirationDate);

          await putFdaAndWait(fdaId, { expirationDate: null });

          const unrestricted = await getFda(fdaId);
          expect(unrestricted.maxDownloads).toBeUndefined();
          expect(unrestricted.expirationDate).toBeUndefined();
          expect(unrestricted.downloadStatus).toBeUndefined();
        } finally {
          await deleteFda(fdaId);
        }
      });
    });
  });
}
