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
import { performance } from 'node:perf_hooks';
import { waitUntilFDAStatus } from '../utils/performanceTestUtils';

export function registerFdaCreationPerformanceTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  fdaId,
  httpReq,
  waitUntilFDACompleted,
  maxWaitMs,
}) {
  const query =
    'SELECT id, name, age, timeinstant, authorized, country, score FROM public.users ORDER BY id';
  test(
    'Create basic FDA',
    async () => {
      const baseUrl = getBaseUrl();

      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          query,
          description: 'Performance test: CSV to Parquet conversion',
        },
      });

      if (res.status >= 400) {
        console.error('POST /fdas failed:', res.status, res.json ?? res.text);
      }
      expect(res.status).toBe(202);

      performance.mark('fda-create-start');
      await waitUntilFDAStatus({
        baseUrl,
        service,
        fdaId,
        visibility,
        timeout: maxWaitMs(),
        status: 'fetching',
        progress: 20,
        httpReq,
      });
      performance.mark('fda-fetch-start');

      await waitUntilFDAStatus({
        baseUrl,
        service,
        fdaId,
        visibility,
        timeout: maxWaitMs(),
        status: 'transforming',
        progress: 60,
        httpReq,
      });
      performance.mark('fda-fetch-end');

      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId,
        visibility,
        timeout: maxWaitMs(),
      });
      performance.mark('fda-create-end');

      performance.measure(
        'Basic FDA creation',
        'fda-create-start',
        'fda-create-end',
      );
      performance.measure('Fetch time', 'fda-fetch-start', 'fda-fetch-end');

      const creationTime =
        performance.getEntriesByName('Basic FDA creation')[0];
      const fetchTime = performance.getEntriesByName('Fetch time')[0];

      console.log(
        `[PERF] Basic FDA creation took ${creationTime.duration.toFixed(2)}ms (fetch step: ${fetchTime.duration.toFixed(2)}ms)`,
      );
    },
    maxWaitMs(),
  );

  test(
    'Create compressed FDA',
    async () => {
      const baseUrl = getBaseUrl();
      const uniqueFdaId = `${fdaId}-compressed`;

      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: uniqueFdaId,
          query,
          description: 'Performance test: CSV to Parquet conversion',
          objStgConf: {
            compression: true,
          },
        },
      });

      if (res.status >= 400) {
        console.error('POST /fdas failed:', res.status, res.json ?? res.text);
      }
      expect(res.status).toBe(202);

      performance.mark('fda-create-start');
      await waitUntilFDAStatus({
        baseUrl,
        service,
        fdaId: uniqueFdaId,
        visibility,
        timeout: maxWaitMs(),
        status: 'transforming',
        progress: 60,
        httpReq,
      });
      performance.mark('fda-compression-start');

      await waitUntilFDAStatus({
        baseUrl,
        service,
        fdaId: uniqueFdaId,
        visibility,
        timeout: maxWaitMs(),
        status: 'uploading',
        progress: 80,
        httpReq,
      });
      performance.mark('fda-compression-end');

      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: uniqueFdaId,
        visibility,
        timeout: maxWaitMs(),
      });
      performance.mark('fda-create-end');

      performance.measure(
        'Compression time',
        'fda-compression-start',
        'fda-compression-end',
      );
      performance.measure(
        'Compressed FDA creation',
        'fda-create-start',
        'fda-create-end',
      );

      const creationTime = performance.getEntriesByName(
        'Compressed FDA creation',
      )[0];
      const compressionTime =
        performance.getEntriesByName('Compression time')[0];

      console.log(
        `[PERF] Compressed FDA creation took ${creationTime.duration.toFixed(2)}ms (compression step: ${compressionTime.duration.toFixed(2)}ms)`,
      );
    },
    maxWaitMs(),
  );

  test(
    'Create partitioned FDA',
    async () => {
      const baseUrl = getBaseUrl();
      const uniqueFdaId = `${fdaId}-partitioned`;

      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: uniqueFdaId,
          query,
          description: 'Performance test: CSV to Parquet conversion',
          timeColumn: 'timeinstant',
          objStgConf: {
            partition: 'day',
          },
        },
      });

      if (res.status >= 400) {
        console.error('POST /fdas failed:', res.status, res.json ?? res.text);
      }
      expect(res.status).toBe(202);

      performance.mark('fda-create-start');
      await waitUntilFDAStatus({
        baseUrl,
        service,
        fdaId: uniqueFdaId,
        visibility,
        timeout: maxWaitMs(),
        status: 'transforming',
        progress: 60,
        httpReq,
      });
      performance.mark('fda-partition-start');

      await waitUntilFDAStatus({
        baseUrl,
        service,
        fdaId: uniqueFdaId,
        visibility,
        timeout: maxWaitMs(),
        status: 'uploading',
        progress: 80,
        httpReq,
      });

      performance.mark('fda-partition-end');
      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId: uniqueFdaId,
        visibility,
        timeout: maxWaitMs(),
      });

      performance.mark('fda-create-end');

      performance.measure(
        'Partition time',
        'fda-partition-start',
        'fda-partition-end',
      );
      performance.measure(
        'Partitioned FDA creation',
        'fda-create-start',
        'fda-create-end',
      );

      const creationTime = performance.getEntriesByName(
        'Partitioned FDA creation',
      )[0];
      const partitionTime = performance.getEntriesByName('Partition time')[0];

      console.log(
        `[PERF] Partitioned FDA creation took ${creationTime.duration.toFixed(2)}ms (partition step: ${partitionTime.duration.toFixed(2)}ms)`,
      );
    },
    maxWaitMs(),
  );

  test('Create fresh FDA', async () => {
    const baseUrl = getBaseUrl();
    const uniqueFdaId = `${fdaId}-fresh`;

    const res = await httpReq({
      method: 'POST',
      url: `${baseUrl}/${visibility}/fdas`,
      headers: {
        'Fiware-Service': service,
        'Fiware-ServicePath': servicePath,
      },
      body: {
        id: uniqueFdaId,
        query,
        description: 'only fresh FDA',
        cached: false,
      },
    });

    if (res.status >= 400) {
      console.error('POST /fdas failed:', res.status, res.json ?? res.text);
    }
    expect(res.status).toBe(202);

    performance.mark('fda-create-start');
    await waitUntilFDACompleted({
      baseUrl,
      service,
      fdaId: uniqueFdaId,
      visibility,
      timeout: maxWaitMs(),
    });
    performance.mark('fda-create-end');

    performance.measure(
      'Fresh FDA creation',
      'fda-create-start',
      'fda-create-end',
    );

    const creationTime = performance.getEntriesByName('Fresh FDA creation')[0];

    console.log(
      `[PERF] Fresh FDA creation took ${creationTime.duration.toFixed(2)}ms`,
    );
  });
}
