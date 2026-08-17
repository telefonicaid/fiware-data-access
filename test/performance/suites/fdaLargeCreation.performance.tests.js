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

export function registerLargeFdaPerformanceTests({
  getBaseUrl,
  service,
  servicePath,
  visibility,
  httpReq,
  waitUntilFDACompleted,
  maxWaitMs,
}) {
  test(
    'Create large air quality FDA',
    async () => {
      const baseUrl = getBaseUrl();
      //const fdaId = 'fda-air-quality-large';
      const fdaId = `fda-air-quality-large-${Date.now()}`;

      const res = await httpReq({
        method: 'POST',
        url: `${baseUrl}/${visibility}/fdas`,
        headers: {
          'Fiware-Service': service,
          'Fiware-ServicePath': servicePath,
        },
        body: {
          id: fdaId,
          query: 'SELECT * FROM public.air_quality_test',
          description: 'Performance test: large air quality dataset',
          timeColumn: 'timeinstant',
          objStgConf: {
            partition: 'year',
            compression: false,
          },
          datasourceId: 'default',
          cached: false,
        },
      });

      if (res.status >= 400) {
        console.error('POST /fdas failed:', res.status, res.json ?? res.text);
      }

      expect(res.status).toBe(202);

      performance.mark('large-fda-start');

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

      performance.mark('large-fda-fetch-start');

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

      performance.mark('large-fda-fetch-end');

      await waitUntilFDAStatus({
        baseUrl,
        service,
        fdaId,
        visibility,
        timeout: maxWaitMs(),
        status: 'uploading',
        progress: 80,
        httpReq,
      });

      performance.mark('large-fda-upload-start');

      await waitUntilFDACompleted({
        baseUrl,
        service,
        fdaId,
        visibility,
        timeout: maxWaitMs(),
      });

      performance.mark('large-fda-end');

      performance.measure(
        'Large FDA creation',
        'large-fda-start',
        'large-fda-end',
      );

      performance.measure(
        'Large FDA fetch',
        'large-fda-fetch-start',
        'large-fda-fetch-end',
      );

      performance.measure(
        'Large FDA parquet+upload',
        'large-fda-fetch-end',
        'large-fda-upload-start',
      );

      const creationTime =
        performance.getEntriesByName('Large FDA creation')[0];

      const fetchTime = performance.getEntriesByName('Large FDA fetch')[0];

      const parquetTime = performance.getEntriesByName(
        'Large FDA parquet+upload',
      )[0];

      console.log(
        `[PERF] Large FDA creation took ${creationTime.duration.toFixed(
          2,
        )}ms (fetch: ${fetchTime.duration.toFixed(
          2,
        )}ms) (transform/upload: ${parquetTime.duration.toFixed(2)}ms)`,
      );
    },
    maxWaitMs(),
  );
}
