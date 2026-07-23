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
import pg from 'pg';
import { connectWithRetry } from '../utils/integrationTestUtils.js';

const { Client } = pg;

export function registerComplexCasesIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
  getPgHost,
  getPgPort,
}) {
  describe('Complex FDA queries', () => {
    async function ensureDefaultDatasource(baseUrl) {
      const createRes = await httpReq({
        method: 'POST',
        url: `${baseUrl}/datasources`,
        headers: {
          'Content-Type': 'application/json',
          'Fiware-Service': service,
        },
        body: {
          datasourceId: 'default',
          type: 'postgres',
          config: {
            user: 'postgres',
            password: 'postgres',
            host: getPgHost(),
            port: getPgPort(),
            database: service,
          },
        },
      });

      if (createRes.status !== 204 && createRes.status !== 409) {
        throw new Error(
          `Failed to ensure default datasource: ${createRes.status} ${JSON.stringify(createRes.json)}`,
        );
      }
    }

    beforeAll(async () => {
      const baseUrl = getBaseUrl();

      await ensureDefaultDatasource(baseUrl);

      // Health Postgres + seed
      {
        const pgClient = new Client({
          host: getPgHost(),
          port: getPgPort(),
          user: 'postgres',
          password: 'postgres',
          database: service,
          connectionTimeoutMillis: 10_000,
        });
        await connectWithRetry(pgClient);

        await pgClient.query(`
        CREATE TABLE IF NOT EXISTS public.smartbuildings_building_lastdata (
          entityid TEXT PRIMARY KEY,
          category TEXT,
          name TEXT,
          timeinstant TIMESTAMP
        );
      `);

        await pgClient.query(`
        CREATE TABLE IF NOT EXISTS public.smartbuildings_bldwatermeter_lastdata (
          entityid TEXT PRIMARY KEY,
          refpointofinterest TEXT,
          timeinstant TIMESTAMP,
          vol DECIMAL
        );
      `);

        await pgClient.query(`
        CREATE TABLE IF NOT EXISTS public.smartbuildings_bldwatermeter (
          entityid TEXT,
          timeinstant TIMESTAMP,
          vol DECIMAL,
          refpointofinterest TEXT
        );
      `);

        await pgClient.query(`
        INSERT INTO public.smartbuildings_building_lastdata (entityid, category, name, timeinstant)
        VALUES 
          ('school_1', 'school', 'Colegio San José', NOW()),
          ('school_2', 'school', 'school Pública La Paz', NOW()),
          ('school_3', 'school', 'Instituto Tecnológico', NOW()),
          ('school_4', 'school', 'Colegio Santa María', NOW());
      `);

        await pgClient.query(`
        INSERT INTO public.smartbuildings_bldwatermeter_lastdata (entityid, refpointofinterest, timeinstant, vol)
        VALUES 
          ('meter_1', 'school_1', NOW(), 100.5),
          ('meter_2', 'school_2', NOW(), 75.2),
          ('meter_3', 'school_3', NOW(), 150.8),
          ('meter_4', 'school_4', NOW(), 200.3);
      `);

        for (let month = 0; month < 12; month++) {
          const date = new Date();
          date.setMonth(date.getMonth() - month);

          const baseVol = 50 + Math.random() * 200;

          await pgClient.query(
            `
          INSERT INTO public.smartbuildings_bldwatermeter (entityid, timeinstant, vol, refpointofinterest)
          VALUES 
            ('meter_1', $1, ${baseVol + 10}, 'school_1'),
            ('meter_2', $1, ${baseVol + 5}, 'school_2'),
            ('meter_3', $1, ${baseVol + 20}, 'school_3'),
            ('meter_4', $1, ${baseVol + 15}, 'school_4')
        `,
            [date],
          );
        }

        await pgClient.end();
        console.log('[TEST] Postgres OK');
      }
    });

    test('POST /fdas with complex query should return expected results', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'water_consumption_fda';

      const complexQuery = `
        WITH params AS (
          SELECT ((EXTRACT(YEAR FROM CURRENT_DATE) - 
            CASE WHEN EXTRACT(MONTH FROM CURRENT_DATE) < 9 THEN 1 ELSE 0 END))::int AS current_semester
        ),
        bounds AS (
          SELECT 
            make_date(p.current_semester, 9, 1)::date AS course_start_date,
            make_date(p.current_semester + 1, 9, 1)::date AS course_end_date
          FROM params p
        ),
        devices AS (
          SELECT 
            l.refpointofinterest, 
            l.entityid AS entityid_measurement 
          FROM public.smartbuildings_bldwatermeter_lastdata l
          JOIN public.smartbuildings_building_lastdata b 
            ON l.refpointofinterest = b.entityid
          WHERE b.category = 'school'
        ),
        monthly_totals AS (
          SELECT 
            d.refpointofinterest,
            date_trunc('month', t.timeinstant)::date AS month_date,
            ROUND(SUM(t.vol)::numeric, 2) AS total_vol
          FROM public.smartbuildings_bldwatermeter t
          JOIN devices d ON t.entityid = d.entityid_measurement
          CROSS JOIN bounds b
          WHERE t.timeinstant >= b.course_start_date 
            AND t.timeinstant < b.course_end_date
            AND t.vol IS NOT NULL 
            AND t.vol >= 0 
            AND t.vol < 1000000
          GROUP BY d.refpointofinterest, date_trunc('month', t.timeinstant)::date
        ),
        months AS (
          SELECT DISTINCT month_date FROM monthly_totals
        ),
        building_months AS (
          SELECT d.refpointofinterest, m.month_date
          FROM (SELECT DISTINCT refpointofinterest FROM devices) d
          CROSS JOIN months m
        ),
        building_monthly AS (
          SELECT 
            bm.refpointofinterest,
            bm.month_date,
            COALESCE(mt.total_vol, 0) AS building_consumption
          FROM building_months bm
          LEFT JOIN monthly_totals mt 
            ON mt.refpointofinterest = bm.refpointofinterest 
            AND mt.month_date = bm.month_date
        ),
        stats AS (
          SELECT 
            month_date,
            ROUND(MAX(building_consumption)::numeric, 2) AS max_consumption_total,
            ROUND(MIN(building_consumption)::numeric, 2) AS min_consumption_total,
            ROUND(AVG(building_consumption)::numeric, 2) AS avg_consumption_total
          FROM building_monthly
          GROUP BY month_date
        )
        SELECT 
          to_char(bm.month_date, 'YYYY-MM') AS mes,
          bm.refpointofinterest,
          bm.building_consumption,
          s.max_consumption_total,
          s.min_consumption_total,
          s.avg_consumption_total
        FROM building_monthly bm
        JOIN stats s ON s.month_date = bm.month_date
        ORDER BY bm.month_date, bm.refpointofinterest
      `;

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query: complexQuery,
            description: 'Water consumption analysis by school buildings',
            validationMode: 'strict',
            cached: true,
          },
        });

        expect(createFda.status).toBe(202);

        const completedFDA = await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
          timeout: 30000,
        });

        expect(completedFDA).toBeDefined();
        expect(completedFDA.status).toBe('completed');
        expect(Array.isArray(completedFDA.schema)).toBe(true);
        expect(completedFDA.schema.length).toBeGreaterThan(0);

        const daId = 'defaultDataAccess';

        const dataRes = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daId}/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(dataRes.status).toBe(200);
        expect(Array.isArray(dataRes.json)).toBe(true);

        if (dataRes.json.length > 0) {
          const firstRow = dataRes.json[0];
          expect(firstRow).toHaveProperty('mes');
          expect(firstRow).toHaveProperty('refpointofinterest');
          expect(firstRow).toHaveProperty('building_consumption');
          expect(firstRow).toHaveProperty('max_consumption_total');
          expect(firstRow).toHaveProperty('min_consumption_total');
          expect(firstRow).toHaveProperty('avg_consumption_total');

          expect(typeof firstRow.mes).toBe('string');
          expect(firstRow.mes).toMatch(/^\d{4}-\d{2}$/);
          expect(typeof firstRow.building_consumption).toBe('number');
          expect(typeof firstRow.max_consumption_total).toBe('number');
          expect(typeof firstRow.min_consumption_total).toBe('number');
          expect(typeof firstRow.avg_consumption_total).toBe('number');
        }

        if (dataRes.json.length > 0) {
          const groupedData = dataRes.json.reduce((acc, row) => {
            if (!acc[row.mes]) {
              acc[row.mes] = [];
            }
            acc[row.mes].push(row);
            return acc;
          }, {});

          for (const mes in groupedData) {
            const rows = groupedData[mes];
            const expectedMax = Math.max(
              ...rows.map((r) => r.building_consumption),
            );
            const expectedMin = Math.min(
              ...rows.map((r) => r.building_consumption),
            );
            const expectedAvg =
              rows.reduce((sum, r) => sum + r.building_consumption, 0) /
              rows.length;

            expect(rows[0].max_consumption_total).toBeCloseTo(expectedMax, 1);
            expect(rows[0].min_consumption_total).toBeCloseTo(expectedMin, 1);
            expect(rows[0].avg_consumption_total).toBeCloseTo(expectedAvg, 1);
          }
        }

        const dataResNoParams = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daId}/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          query: {
            pageSize: 10,
            pageStart: 0,
          },
        });

        expect(dataResNoParams.status).toBe(200);
        expect(Array.isArray(dataResNoParams.json)).toBe(true);
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });

    test('POST /fdas with water consumption query should handle empty results gracefully', async () => {
      const baseUrl = getBaseUrl();
      const fdaId = 'water_consumption_empty_fda';

      const emptyQuery = `
        WITH params AS (
          SELECT 2026 AS current_semester
        ),
        bounds AS (
          SELECT 
            make_date(p.current_semester, 9, 1)::date AS course_start_date,
            make_date(p.current_semester + 1, 9, 1)::date AS course_end_date
          FROM params p
        ),
        devices AS (
          SELECT 
            l.refpointofinterest, 
            l.entityid AS entityid_measurement 
          FROM public.smartbuildings_bldwatermeter_lastdata l
          JOIN public.smartbuildings_building_lastdata b 
            ON l.refpointofinterest = b.entityid
          WHERE b.category = 'school'
        ),
        monthly_totals AS (
          SELECT 
            d.refpointofinterest,
            date_trunc('month', t.timeinstant)::date AS month_date,
            ROUND(SUM(t.vol)::numeric, 2) AS total_vol
          FROM public.smartbuildings_bldwatermeter t
          JOIN devices d ON t.entityid = d.entityid_measurement
          CROSS JOIN bounds b
          WHERE t.timeinstant >= b.course_start_date 
            AND t.timeinstant < b.course_end_date
            AND t.vol IS NOT NULL 
            AND t.vol >= 0 
            AND t.vol < 1000000
          GROUP BY d.refpointofinterest, date_trunc('month', t.timeinstant)::date
        )
        SELECT 
          to_char(month_date, 'YYYY-MM') AS mes,
          refpointofinterest,
          total_vol AS building_consumption
        FROM monthly_totals
        ORDER BY month_date, refpointofinterest
      `;

      try {
        const createFda = await httpReq({
          method: 'POST',
          url: `${baseUrl}/${visibility}/fdas`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          body: {
            id: fdaId,
            query: emptyQuery,
            description: 'Water consumption test with potential empty results',
            validationMode: 'strict',
            cached: true,
          },
        });

        expect(createFda.status).toBe(202);

        await waitUntilFDACompleted({
          baseUrl,
          service,
          fdaId,
          timeout: 30000, // 30 segundos
        });

        const daId = 'defaultDataAccess';

        const dataRes = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daId}/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });

        expect(dataRes.status).toBe(200);
        expect(Array.isArray(dataRes.json)).toBe(true);

        if (dataRes.json.length > 0) {
          const firstRow = dataRes.json[0];
          expect(firstRow).toHaveProperty('mes');
          expect(firstRow).toHaveProperty('refpointofinterest');
          expect(firstRow).toHaveProperty('building_consumption');
        }

        const dataResPaginated = await httpReq({
          method: 'GET',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}/das/${daId}/data`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
          query: {
            pageSize: 5,
            pageStart: 0,
          },
        });

        expect(dataResPaginated.status).toBe(200);
        expect(Array.isArray(dataResPaginated.json)).toBe(true);
      } finally {
        await httpReq({
          method: 'DELETE',
          url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
          headers: {
            'Fiware-Service': service,
            'Fiware-ServicePath': servicePath,
          },
        });
      }
    });
  });
}
