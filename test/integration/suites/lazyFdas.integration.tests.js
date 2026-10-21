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
  ensureDefaultDatasource,
  buildDaDataUrl,
  httpFormReq,
} from '../utils/integrationTestUtils.js';

const { Client } = pg;

export function registerLazyFdasIntegrationTests({
  getBaseUrl,
  getMongoUri,
  getPgHost,
  getPgPort,
  service,
  visibility,
  httpReq,
  onDemandWaitTimeoutMs,
}) {
  const servicePath = '/public';
  const headers = {
    'Fiware-Service': service,
    'Fiware-ServicePath': servicePath,
  };

  let pgClient;
  let mongoClient;
  let agendaJobs;

  function wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function createFDA(body) {
    return httpReq({
      method: 'POST',
      url: `${getBaseUrl()}/${visibility}/fdas`,
      headers,
      body,
    });
  }

  function getFDA(fdaId) {
    return httpReq({
      method: 'GET',
      url: `${getBaseUrl()}/${visibility}/fdas/${fdaId}`,
      headers,
    });
  }

  function withoutTotal(row) {
    const copy = { ...row };
    delete copy.__total;
    return copy;
  }

  async function queryFDA(fdaId) {
    const res = await httpReq({
      method: 'GET',
      url: buildDaDataUrl(
        getBaseUrl(),
        `/${visibility}`,
        fdaId,
        'defaultDataAccess',
      ),
      headers,
    });

    return {
      ...res,
      rows: Array.isArray(res.json) ? res.json.map(withoutTotal) : undefined,
    };
  }

  async function fetchCount(sequence) {
    const { rows } = await pgClient.query(
      `SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS fetches FROM public.${sequence}`,
    );
    return Number(rows[0].fetches);
  }

  async function createSequence(sequence) {
    await pgClient.query(`DROP SEQUENCE IF EXISTS public.${sequence}`);
    await pgClient.query(`CREATE SEQUENCE public.${sequence}`);
  }

  function countingQuery(sequence) {
    return `SELECT id, nextval('public.${sequence}')::int AS fetch_number FROM public.lazy_rows`;
  }

  function idsAndValues(rows) {
    return rows
      .map(({ id, value }) => ({ id, value }))
      .sort((a, b) => a.id - b.id);
  }

  async function waitUntilCompleted(fdaId) {
    let fda;
    for (let attempt = 0; attempt < 100; attempt++) {
      fda = (await getFDA(fdaId)).json;
      if (fda.status === 'completed' || fda.status === 'failed') {
        return fda;
      }
      await wait(200);
    }
    throw new Error(`Timeout waiting for FDA ${fdaId}: ${JSON.stringify(fda)}`);
  }

  function jobsOf(fdaId) {
    return agendaJobs.find({ 'data.fdaId': fdaId }).toArray();
  }

  describe('Lazy and onDemand FDAs', () => {
    beforeAll(async () => {
      await ensureDefaultDatasource({
        httpReq,
        baseUrl: getBaseUrl(),
        service,
        getPgHost,
        getPgPort,
      });

      pgClient = new Client({
        host: getPgHost(),
        port: getPgPort(),
        user: 'postgres',
        password: 'postgres',
        database: service,
      });
      await pgClient.connect();
      await pgClient.query('DROP TABLE IF EXISTS public.lazy_rows');
      await pgClient.query('CREATE TABLE public.lazy_rows (id INT)');
      await pgClient.query('INSERT INTO public.lazy_rows VALUES (1)');
      await pgClient.query('DROP TABLE IF EXISTS public.lazy_events');
      await pgClient.query(
        'CREATE TABLE public.lazy_events (id INT, value TEXT, observed_at TIMESTAMP)',
      );
      await pgClient.query(
        `INSERT INTO public.lazy_events VALUES
           (1, 'old', NOW() - INTERVAL '3 days'),
           (2, 'outside', NOW() - INTERVAL '10 days')`,
      );
      await pgClient.query('DROP TABLE IF EXISTS public.lazy_broken');
      await pgClient.query('CREATE TABLE public.lazy_broken (id INT)');

      mongoClient = new MongoClient(getMongoUri());
      await mongoClient.connect();
      agendaJobs = mongoClient.db().collection('agendaJobs');
    });

    afterAll(async () => {
      await pgClient?.end();
      await mongoClient?.close();
    });

    test('a lazy FDA is created idle, without jobs, and materialized by its first query', async () => {
      await createSequence('lazy_interval_fetches');

      const res = await createFDA({
        id: 'lazy_interval',
        query: countingQuery('lazy_interval_fetches'),
        refreshPolicy: {
          type: 'interval',
          params: { refreshInterval: '1 hour' },
        },
        lazy: true,
      });
      expect(res.status).toBe(202);
      expect(res.json).toEqual({ id: 'lazy_interval', status: 'idle' });

      const created = await getFDA('lazy_interval');
      expect(created.json).toMatchObject({
        status: 'idle',
        lazy: true,
        lastFetch: null,
      });
      await expect(jobsOf('lazy_interval')).resolves.toEqual([]);
      await expect(fetchCount('lazy_interval_fetches')).resolves.toBe(0);

      const data = await queryFDA('lazy_interval');
      expect(data.status).toBe(200);
      expect(data.rows).toEqual([{ id: 1, fetch_number: 1 }]);

      const materialized = await getFDA('lazy_interval');
      expect(materialized.json.status).toBe('completed');
      const jobs = await jobsOf('lazy_interval');
      expect(jobs.map(({ name }) => name)).toContain('refresh-fda-recurring');
    });

    test('concurrent first queries trigger a single fetch', async () => {
      await createSequence('lazy_concurrent_fetches');

      const res = await createFDA({
        id: 'lazy_concurrent',
        query: `SELECT id, nextval('public.lazy_concurrent_fetches')::int AS fetch_number
                FROM public.lazy_rows, (SELECT pg_sleep(2)) AS slow`,
        refreshPolicy: { type: 'onDemand', params: { maxAge: '1 hour' } },
      });
      expect(res.status).toBe(202);

      const responses = await Promise.all(
        Array.from({ length: 5 }, () => queryFDA('lazy_concurrent')),
      );

      for (const response of responses) {
        expect(response.status).toBe(200);
        expect(response.rows).toEqual([{ id: 1, fetch_number: 1 }]);
      }
      await expect(fetchCount('lazy_concurrent_fetches')).resolves.toBe(1);
    });

    test('an onDemand FDA is only refreshed when it is queried after maxAge', async () => {
      await createSequence('lazy_ondemand_fetches');

      const res = await createFDA({
        id: 'lazy_ondemand',
        query: countingQuery('lazy_ondemand_fetches'),
        refreshPolicy: { type: 'onDemand', params: { maxAge: '3 seconds' } },
      });
      expect(res.status).toBe(202);
      await expect(jobsOf('lazy_ondemand')).resolves.toEqual([]);

      const first = await queryFDA('lazy_ondemand');
      expect(first.rows).toEqual([{ id: 1, fetch_number: 1 }]);

      const cached = await queryFDA('lazy_ondemand');
      expect(cached.rows).toEqual([{ id: 1, fetch_number: 1 }]);

      await wait(4000);
      await expect(fetchCount('lazy_ondemand_fetches')).resolves.toBe(1);

      const refreshed = await queryFDA('lazy_ondemand');
      expect(refreshed.status).toBe(200);
      expect(refreshed.rows).toEqual([{ id: 1, fetch_number: 2 }]);
      await expect(jobsOf('lazy_ondemand')).resolves.toEqual([]);
    });

    test('a first query returns 409 when the materialization takes longer than the wait timeout', async () => {
      const sleepSeconds = Math.ceil(onDemandWaitTimeoutMs / 1000) + 3;

      const res = await createFDA({
        id: 'lazy_slow',
        query: `SELECT id FROM public.lazy_rows, (SELECT pg_sleep(${sleepSeconds})) AS slow`,
        refreshPolicy: { type: 'onDemand', params: { maxAge: '1 hour' } },
      });
      expect(res.status).toBe(202);

      const timedOut = await queryFDA('lazy_slow');
      expect(timedOut.status).toBe(409);
      expect(timedOut.json.error).toBe('FDAUnavailable');

      const later = await queryFDA('lazy_slow');
      expect(later.status).toBe(200);
      expect(later.rows).toEqual([{ id: 1 }]);
    });

    test('a failed materialization returns 503 and is not retried during the backoff', async () => {
      const res = await createFDA({
        id: 'lazy_broken',
        query: 'SELECT id FROM public.lazy_broken',
        refreshPolicy: { type: 'onDemand', params: { maxAge: '1 hour' } },
      });
      expect(res.status).toBe(202);
      await pgClient.query('DROP TABLE public.lazy_broken');

      const failed = await queryFDA('lazy_broken');
      expect(failed.status).toBe(503);
      expect(failed.json.error).toBe('FDAMaterializationFailed');

      const { initFetch } = (await getFDA('lazy_broken')).json;

      const retried = await queryFDA('lazy_broken');
      expect(retried.status).toBe(503);
      expect((await getFDA('lazy_broken')).json.initFetch).toBe(initFetch);
    });

    test('the CDA legacy endpoint waits for the first materialization', async () => {
      const res = await createFDA({
        id: 'lazy_cda',
        query: 'SELECT id FROM public.lazy_rows',
        refreshPolicy: { type: 'onDemand', params: { maxAge: '1 hour' } },
      });
      expect(res.status).toBe(202);

      const cda = await httpFormReq({
        method: 'POST',
        url: `${getBaseUrl()}/plugin/cda/api/doQuery`,
        headers: { 'Fiware-Service': service },
        form: {
          path: `/${visibility}/${service}/verticals/sql/lazy_cda`,
          dataAccessId: 'defaultDataAccess',
          outputType: 'json',
        },
      });

      expect(cda.status).toBe(200);
      expect(cda.json.resultset).toHaveLength(1);
      expect(cda.json.resultset[0][0]).toBe(1);
    });

    test('a lazy window FDA only fetches its window and schedules its jobs on the first query', async () => {
      const res = await createFDA({
        id: 'lazy_window',
        query: 'SELECT id, value, observed_at FROM public.lazy_events',
        timeColumn: 'observed_at',
        objStgConf: { partition: 'day' },
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '1 day',
            fetchSize: 'day',
            windowSize: '1 week',
          },
        },
        lazy: true,
      });
      expect(res.status).toBe(202);
      expect(res.json.status).toBe('idle');
      await expect(jobsOf('lazy_window')).resolves.toEqual([]);

      const data = await queryFDA('lazy_window');
      expect(data.status).toBe(200);
      expect(idsAndValues(data.rows)).toEqual([{ id: 1, value: 'old' }]);

      const jobs = (await jobsOf('lazy_window')).map(({ name }) => name);
      expect(jobs).toEqual(
        expect.arrayContaining([
          'refresh-fda-recurring',
          'clean-partition-recurring',
        ]),
      );
    });

    test('an expired partitioned onDemand FDA only refetches from the partition of its last fetch', async () => {
      const res = await createFDA({
        id: 'lazy_incremental',
        query: 'SELECT id, value, observed_at FROM public.lazy_events',
        timeColumn: 'observed_at',
        objStgConf: { partition: 'day' },
        refreshPolicy: {
          type: 'onDemand',
          params: { maxAge: '3 seconds', windowSize: '1 week' },
        },
      });
      expect(res.status).toBe(202);

      const first = await queryFDA('lazy_incremental');
      expect(first.status).toBe(200);
      expect(idsAndValues(first.rows)).toEqual([{ id: 1, value: 'old' }]);

      await pgClient.query(
        "UPDATE public.lazy_events SET value = 'changed' WHERE id = 1",
      );
      await pgClient.query(
        "INSERT INTO public.lazy_events VALUES (3, 'new', NOW() - INTERVAL '1 second')",
      );
      await wait(4000);

      const incremental = await queryFDA('lazy_incremental');
      expect(incremental.status).toBe(200);
      expect(idsAndValues(incremental.rows)).toEqual([
        { id: 1, value: 'old' },
        { id: 3, value: 'new' },
      ]);
      await expect(jobsOf('lazy_incremental')).resolves.toEqual([]);

      const rebuild = await httpReq({
        method: 'PUT',
        url: `${getBaseUrl()}/${visibility}/fdas/lazy_incremental`,
        headers,
      });
      expect(rebuild.status).toBeLessThan(300);
      await waitUntilCompleted('lazy_incremental');

      const rebuilt = await queryFDA('lazy_incremental');
      expect(idsAndValues(rebuilt.rows)).toEqual([
        { id: 1, value: 'changed' },
        { id: 3, value: 'new' },
      ]);

      await pgClient.query('DELETE FROM public.lazy_events WHERE id = 3');
      await pgClient.query(
        "UPDATE public.lazy_events SET value = 'old' WHERE id = 1",
      );
    });

    test('an idle FDA can be deleted', async () => {
      const res = await createFDA({
        id: 'lazy_deleted',
        query: 'SELECT id FROM public.lazy_rows',
        lazy: true,
      });
      expect(res.status).toBe(202);

      const deleted = await httpReq({
        method: 'DELETE',
        url: `${getBaseUrl()}/${visibility}/fdas/lazy_deleted`,
        headers,
      });
      expect(deleted.status).toBe(204);
      expect((await getFDA('lazy_deleted')).status).toBe(404);
    });

    test('a manual refresh materializes an idle FDA', async () => {
      const res = await createFDA({
        id: 'lazy_manual',
        query: 'SELECT id FROM public.lazy_rows',
        lazy: true,
      });
      expect(res.status).toBe(202);

      const refresh = await httpReq({
        method: 'PUT',
        url: `${getBaseUrl()}/${visibility}/fdas/lazy_manual`,
        headers,
      });
      expect(refresh.status).toBeLessThan(300);

      const fda = await waitUntilCompleted('lazy_manual');
      expect(fda.status).toBe('completed');
      expect(fda.lastFetch).not.toBeNull();
    });
  });
}
