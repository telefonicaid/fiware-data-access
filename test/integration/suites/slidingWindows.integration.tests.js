import { test, expect } from '@jest/globals';

export function registerSlidingWindowsIntegrationTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
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
}
