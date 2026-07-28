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

import { describe, test, expect } from '@jest/globals';
import { Client } from 'pg';
import http from 'node:http';
import { connectWithRetry } from '../utils/integrationTestUtils.js';

export function registerFreshQueriesIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  httpReqRaw,
  waitUntilFDACompleted,
  buildDaDataUrl,
  buildFdaDataUrl,
  fdaId,
  getPgHost,
  getPgPort,
}) {
  describe('Fresh FDA data queries', () => {
    test('GET /{visibility}/fdas/{fdaId}/data runs FDA query directly against PostgreSQL source', async () => {
      const baseUrl = getBaseUrl();
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

      expect(createDa.status).toBe(204);

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
        host: getPgHost(),
        port: getPgPort(),
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
      const baseUrl = getBaseUrl();
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
      const baseUrl = getBaseUrl();
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
      const baseUrl = getBaseUrl();
      const fdaFreshStreamId = 'fda_fresh_stream_real';
      const TOTAL_ROWS = 600;

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

      const pgClient = new Client({
        host: getPgHost(),
        port: getPgPort(),
        user: 'postgres',
        password: 'postgres',
        database: service,
      });

      await connectWithRetry(pgClient);

      try {
        for (let i = 0; i < TOTAL_ROWS; i++) {
          const id = 1000 + i;
          await pgClient.query(
            `INSERT INTO public.users (id, name, age, timeinstant, authorized)
          VALUES ($1, $2, $3, NOW(), true)`,
            [id, `user_${i}`, 20 + (i % 50)],
          );
        }

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

        expect(receivedChunks).toBeGreaterThan(1);
        expect(firstChunkTime).not.toBeNull();

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
      const baseUrl = getBaseUrl();
      const fixtureTable = 'format_serialization_fixture';
      const fdaSerializationId = 'fda_serialization_regression';
      const fdaFreshSerializationId = 'fda_serialization_regression_fresh';
      const daSerializationId = 'da_serialization_regression';
      const expectedDates = [
        '2024-01-10T12:34:56.789Z',
        '2024-01-11T08:15:30.123Z',
      ];

      const pgClient = new Client({
        host: getPgHost(),
        port: getPgPort(),
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

        expect(createDa.status).toBe(204);

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
  });
}
