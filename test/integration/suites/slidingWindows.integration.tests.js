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

import { test, expect } from '@jest/globals';
import pg from 'pg';

const { Client } = pg;

export function registerSlidingWindowsIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
  buildDaDataUrl,
  getPgHost,
  getPgPort,
}) {
  test('POST /fdas creates an FDA with various refresh policies', async () => {
    const baseUrl = getBaseUrl();

    // Test with a window refresh policy (week) and partitioned by day, without compression, with a window size of 1 day
    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: 'fda_refresh',
        // query base to extract from PG to CSV
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'users dataset',
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '0 0 * * 0',
            fetchSize: 'week',
            windowSize: 'day',
          },
        },
        timeColumn: 'timeinstant',
        objStgConf: {
          partition: 'week',
          compression: false,
        },
      },
    });

    if (res.status >= 400) {
      console.error('POST /fdas failed:', res.status, res.json ?? res.text);
    }
    expect(res.status).toBe(202);
    await waitUntilFDACompleted({ baseUrl, service, fdaId: 'fda_refresh' });

    // Test with a window refresh policy (week) and partitioned by day, without objStgConf
    const yearfetchSize = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: 'fda_refresh_noobjStgConf',
        // query base to extract from PG to CSV
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'users dataset',
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '0 0 * * 0',
            fetchSize: 'year',
            windowSize: 'day',
          },
        },
        timeColumn: 'timeinstant',
      },
    });

    if (yearfetchSize.status >= 400) {
      console.error(
        'POST /fdas failed:',
        yearfetchSize.status,
        yearfetchSize.json ?? yearfetchSize.text,
      );
    }
    expect(yearfetchSize.status).toBe(202);
    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: 'fda_refresh_noobjStgConf',
    });

    // Test with a fetchSize different from partition
    const diffFetchPartition = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: 'fda_refresh',
        // query base to extract from PG to CSV
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'users dataset',
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '0 0 * * 0',
            fetchSize: 'week',
            windowSize: 'day',
          },
        },
        timeColumn: 'timeinstant',
        objStgConf: {
          partition: 'month',
          compression: false,
        },
      },
    });

    if (diffFetchPartition.status >= 400) {
      console.error(
        'POST /fdas failed as expected:',
        diffFetchPartition.status,
        diffFetchPartition.json ?? diffFetchPartition.text,
      );
    }

    expect(diffFetchPartition.status).toBe(400);
    await waitUntilFDACompleted({ baseUrl, service, fdaId: 'fda_refresh' });

    // Test with a window refresh policy (week) and partitioned by day, without compression, with a window size of 1 day
    const res2 = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: 'fda_refresh2',
        // query base to extract from PG to CSV
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'users dataset',
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '0 0 * * 0',
            fetchSize: 'week',
            windowSize: 'week',
          },
        },
        timeColumn: 'timeinstant',
        objStgConf: {
          partition: 'week',
          compression: false,
        },
      },
    });

    if (res2.status >= 400) {
      console.error('POST /fdas failed:', res2.status, res2.json ?? res2.text);
    }
    expect(res2.status).toBe(202);
    await waitUntilFDACompleted({ baseUrl, service, fdaId: 'fda_refresh2' });

    // Test with partition type but no time column, should return error as time column is required for partitioning
    const res3 = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: 'fda_refresh3',
        // query base to extract from PG to CSV
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'users dataset',
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '0 0 1 * *',
            fetchSize: 'month',
            windowSize: 'month',
          },
        },
        objStgConf: {
          partition: 'month',
          compression: false,
        },
      },
    });

    if (res3.status >= 400) {
      console.error(
        'POST /fdas failed as expected:',
        res3.status,
        res3.json ?? res3.text,
      );
    }
    expect(res3.status).toBe(400);

    // Test with invalid partition type
    const res4 = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: 'fda_refresh4',
        // query base to extract from PG to CSV
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'users dataset',
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '0 0 1 * *',
            fetchSize: 'month',
            windowSize: 'year',
          },
        },
        timeColumn: 'timeinstant',
        objStgConf: {
          partition: 'fakePartition',
          compression: false,
        },
      },
    });

    if (res4.status >= 400) {
      console.error('POST /fdas failed:', res4.status, res4.json ?? res4.text);
    }
    expect(res4.status).toBe(400);
  });

  test('POST /fdas accepts sliding window with day partition (realistic daily setup)', async () => {
    const baseUrl = getBaseUrl();
    const fdaId = 'fda_sw_daily_partition';

    const createRes = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: fdaId,
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'daily sliding window partition test',
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '1 day',
            fetchSize: 'day',
            windowSize: 'week',
          },
        },
        timeColumn: 'timeinstant',
        objStgConf: {
          partition: 'day',
          compression: false,
        },
      },
    });

    expect(createRes.status).toBe(202);
    await waitUntilFDACompleted({ baseUrl, service, fdaId });
  });

  test('POST /fdas accepts refresh intervals smaller than partition size', async () => {
    const baseUrl = getBaseUrl();
    const fdaId = 'fda_sw_small_interval_week_partition';

    const createRes = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: fdaId,
        query:
          'SELECT id, name, age, timeinstant, authorized FROM public.users ORDER BY id',
        description: 'small interval vs partition test',
        refreshPolicy: {
          type: 'window',
          params: {
            refreshInterval: '12 hours',
            fetchSize: 'week',
            windowSize: 'month',
          },
        },
        timeColumn: 'timeinstant',
        objStgConf: {
          partition: 'week',
          compression: false,
        },
      },
    });

    if (createRes.status >= 400) {
      console.error(
        'POST /fdas failed unexpectedly for small refresh interval:',
        createRes.status,
        createRes.json ?? createRes.text,
      );
    }

    expect(createRes.status).toBe(202);
    await waitUntilFDACompleted({ baseUrl, service, fdaId });
  });

  test('PUT /fdas/:fdaId keeps only current window rows after manual update', async () => {
    const baseUrl = getBaseUrl();
    const suffix = `${Date.now()}`;
    const fixtureTable = `sw_update_fixture_${suffix}`;
    const fdaId = `fda_sw_update_${suffix}`;

    const pgClient = new Client({
      host: getPgHost(),
      port: getPgPort(),
      user: 'postgres',
      password: 'postgres',
      database: service,
      connectionTimeoutMillis: 10_000,
    });

    await pgClient.connect();

    try {
      await pgClient.query(`DROP TABLE IF EXISTS public.${fixtureTable}`);
      await pgClient.query(`
        CREATE TABLE public.${fixtureTable} (
          id INT PRIMARY KEY,
          label TEXT NOT NULL,
          observed_at TIMESTAMPTZ NOT NULL
        )
      `);

      await pgClient.query(
        `
          INSERT INTO public.${fixtureTable} (id, label, observed_at)
          VALUES
            (1, 'old_before_create', NOW() - INTERVAL '10 days'),
            (2, 'recent_before_create', NOW() - INTERVAL '2 hours')
        `,
      );

      const createFda = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          query: `
            SELECT id, label, observed_at
            FROM public.${fixtureTable}
            ORDER BY id
          `,
          description: 'sliding window update test',
          refreshPolicy: {
            type: 'window',
            params: {
              refreshInterval: '6 hours',
              fetchSize: 'day',
              windowSize: 'week',
            },
          },
          timeColumn: 'observed_at',
          objStgConf: {
            partition: 'day',
            compression: false,
          },
        },
      });

      expect(createFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });

      const firstRead = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, 'defaultDataAccess', {
          pageSize: 100,
          pageStart: 0,
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(firstRead.status).toBe(200);
      expect(firstRead.json.map((row) => row.label)).toEqual([
        'recent_before_create',
      ]);

      await pgClient.query(
        `
          INSERT INTO public.${fixtureTable} (id, label, observed_at)
          VALUES
            (3, 'recent_before_update', NOW() - INTERVAL '1 hour'),
            (4, 'old_before_update', NOW() - INTERVAL '9 days')
        `,
      );

      const updateFda = await httpReq({
        method: 'PUT',
        url: `${baseUrl}/${visibility}/fdas/${fdaId}`,
        headers: { 'Fiware-Service': service },
      });

      expect(updateFda.status).toBe(202);
      await waitUntilFDACompleted({ baseUrl, service, fdaId });

      const secondRead = await httpReq({
        method: 'GET',
        url: buildDaDataUrl(baseUrl, servicePath, fdaId, 'defaultDataAccess', {
          pageSize: 100,
          pageStart: 0,
        }),
        headers: { 'Fiware-Service': service },
      });

      expect(secondRead.status).toBe(200);
      expect(secondRead.json.map((row) => row.label)).toEqual([
        'recent_before_create',
        'recent_before_update',
      ]);
    } finally {
      await pgClient.query(`DROP TABLE IF EXISTS public.${fixtureTable}`);
      await pgClient.end();
    }
  });
}
