// Copyright 2025 Telefonica Soluciones de Informatica y Comunicaciones de Espana, S.A.U.
// PROJECT: fiware-data-access
//
// This software and / or computer program has been developed by Telefonica Soluciones
// de Informatica y Comunicaciones de Espana, S.A.U (hereinafter TSOL) and is protected
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

import { test, expect } from '@jest/globals';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import { httpReq } from '../../integration/utils/integrationTestUtils.js';
import { waitUntilFDAStatus } from '../utils/performanceTestUtils.js';

export function registerWindowMemoryPerformanceTests({
  getBaseUrl,
  service,
  getPgHost,
  getPgPort,
  getMongoUri,
  rows,
  timeout,
  memoryLimit,
  expectWindowOom = false,
}) {
  test.each([
    {
      partition: 'month',
      outcome: 'monthly control completes successfully',
    },
    {
      partition: 'day',
      outcome: expectWindowOom
        ? 'creation fails with expected Out of Memory Error'
        : 'creation and two daily refreshes complete successfully',
    },
  ])(
    'Window memory: $partition partitions, $outcome',
    async ({ partition }) => {
      const suffix = `${partition}_${Date.now()}`;
      const fdaId = `traffic_memory_${suffix}`;
      const table = `traffic_memory_${suffix}`;
      const baseUrl = getBaseUrl();
      const servicePath = '/fdas';
      const headers = {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      };
      const pgClient = new pg.Client({
        host: getPgHost(),
        port: getPgPort(),
        user: 'postgres',
        password: 'postgres',
        database: service,
      });
      const mongoClient = new MongoClient(getMongoUri());
      await pgClient.connect();
      try {
        await mongoClient.connect();
        const jobs = mongoClient.db('test-db').collection('agendaJobs');
        // Keep per-row strings distinct to avoid an unrealistically tiny Parquet output.
        await pgClient.query(`
          CREATE TABLE public.${table} AS
          SELECT i AS idtramo,
            (SELECT string_agg(md5(i::text || ':' || n::text), '')
             FROM generate_series(1, 8) n) AS description,
            date_trunc('day', NOW()) - INTERVAL '1 minute'
              - ((i % 32) || ' days')::interval AS fechahora,
            0 AS estadoforzado
          FROM generate_series(1, ${rows}) i
        `);
        const query = `SELECT description AS direccion, idtramo,
          fechahora AS fecha_medicion, estadoforzado AS estado
          FROM public.${table}`;
        const expected = await pgClient.query(`
          SELECT COUNT(*)::int AS total,
            COUNT(DISTINCT fecha_medicion::date)::int AS days
          FROM (${query}) source
          WHERE fecha_medicion >= date_trunc('day', NOW() - INTERVAL '1 month')
            AND fecha_medicion < NOW()
        `);
        expect(expected.rows[0].total).toBeGreaterThan(0);
        expect(expected.rows[0].total).toBeLessThan(rows);
        expect(expected.rows[0].days).toBeGreaterThanOrEqual(28);
        console.log('[Window memory] fixture', {
          partition,
          sourceRows: rows,
          ...expected.rows[0],
          memoryLimit,
        });
        const created = await httpReq({
          method: 'POST',
          url: `${baseUrl}/private/fdas`,
          headers,
          body: {
            id: fdaId,
            query,
            description: 'Traffic window memory regression',
            datasourceId: 'default',
            timeColumn: 'fecha_medicion',
            refreshPolicy: {
              type: 'window',
              params: {
                refreshInterval: '0 16 * * *',
                fetchSize: partition,
                windowSize: 'month',
              },
            },
            cached: true,
            objStgConf: { partition },
          },
        });
        expect({ status: created.status, body: created.json }).toMatchObject({
          status: 202,
        });

        const assertCompleted = async () => {
          await waitUntilFDAStatus({
            baseUrl,
            service,
            servicePath,
            fdaId,
            visibility: 'private',
            status: 'completed',
            timeout,
            httpReq,
          });
        };

        if (partition === 'day' && expectWindowOom) {
          await expect(assertCompleted()).rejects.toThrow(
            /FDA .* failed: Out of Memory Error:/,
          );
          return;
        }
        await assertCompleted();

        const da = await httpReq({
          method: 'POST',
          url: `${baseUrl}/private/fdas/${fdaId}/das`,
          headers,
          body: {
            id: 'counts',
            description: 'Verify complete window and refreshed values',
            query: 'SELECT COUNT(*) AS total, SUM(estado) AS checksum',
          },
        });
        expect(da.status).toBe(204);

        const assertData = async (checksum) => {
          const data = await httpReq({
            method: 'GET',
            url: `${baseUrl}/private/fdas/${fdaId}/das/counts/data`,
            headers,
          });
          expect(data.status).toBe(200);
          expect(Number(data.json[0].total)).toBe(expected.rows[0].total);
          expect(Number(data.json[0].checksum)).toBe(checksum);
        };

        await assertData(0);
        if (partition === 'day') {
          for (const cycle of [1, 2]) {
            const changed = await pgClient.query(`
              UPDATE public.${table} SET estadoforzado = ${cycle}
              WHERE fechahora >= date_trunc('day', NOW()) - INTERVAL '1 day'
              RETURNING idtramo
            `);
            expect(changed.rowCount).toBeGreaterThan(0);
            const filter = {
              name: 'refresh-fda-recurring',
              'data.fdaId': fdaId,
              'data.servicePath': servicePath,
            };
            const previous = await jobs.findOne(filter);
            expect(previous).not.toBeNull();
            await jobs.updateOne(filter, { $set: { nextRunAt: new Date() } });
            const start = Date.now();
            let finished = false;
            while (Date.now() - start < timeout) {
              const job = await jobs.findOne(filter);
              if (
                job.lastFinishedAt &&
                job.lastFinishedAt.getTime() !==
                  previous.lastFinishedAt?.getTime()
              ) {
                finished = true;
                break;
              }
              await new Promise((resolve) => setTimeout(resolve, 300));
            }
            expect(finished).toBe(true);
            await assertCompleted();
            await assertData(changed.rowCount * cycle);
          }
        }
      } finally {
        try {
          const deleted = await httpReq({
            method: 'DELETE',
            url: `${baseUrl}/private/fdas/${fdaId}`,
            headers,
          });
          expect([204, 404]).toContain(deleted.status);
        } finally {
          try {
            await pgClient.query(`DROP TABLE IF EXISTS public.${table}`);
          } finally {
            await Promise.all([pgClient.end(), mongoClient.close()]);
          }
        }
      }
    },
    timeout,
  );
}
